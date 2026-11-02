wit_bindgen::generate!({
    path: "../../vigilo/wit/evaluator/v1.0.0/evaluator.wit",
    world: "evaluator-world",
});

use exports::vigilo::evaluator::evaluator::{
    Guest,
    Input,
    Output,
};
use serde_json::json;
use vigilo::evaluator::{
    executor,
    types::{
        DiagnosticFinding,
        EvaluatorError,
        EvaluatorIdentity,
        EvaluatorOutcome,
        Measurement,
        Severity,
    },
};

struct Evaluator;

const POSITIVE_TERMS: &[&str] = &[
    "good",
    "great",
    "excellent",
    "amazing",
    "love",
    "helpful",
    "fast",
    "happy",
    "success",
    "pleasant",
];

const NEGATIVE_TERMS: &[&str] = &[
    "bad", "terrible", "awful", "hate", "slow", "angry", "sad", "bug", "failure", "poor",
];

fn extract_text(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("context.db must not be empty".to_string());
    }

    // Accept either plain text input or a JSON object containing a text payload.
    if trimmed.starts_with('{') {
        let parsed: serde_json::Value = serde_json::from_str(trimmed)
            .map_err(|err| format!("invalid JSON in context.db: {err}"))?;

        let obj = parsed
            .as_object()
            .ok_or_else(|| "context.db JSON must be an object".to_string())?;

        if let Some(text) = obj.get("text").and_then(|value| value.as_str()) {
            let text = text.trim();
            if text.is_empty() {
                return Err("context.db.text must not be empty".to_string());
            }
            return Ok(text.to_string());
        }

        return Err("context.db JSON must contain a non-empty string field 'text'".to_string());
    }

    Ok(trimmed.to_string())
}

fn extract_text_from_input(input: &Input) -> Result<String, String> {
    if let Some(text) = input.actual.text.as_deref()
        && let Ok(parsed) = extract_text(text)
    {
        return Ok(parsed);
    }

    let input_json: serde_json::Value = serde_json::from_str(&input.test_case.input_json)
        .map_err(|err| format!("invalid test-case.input-json: {err}"))?;

    if let Some(user_message) = input_json
        .get("user_message")
        .and_then(serde_json::Value::as_str)
    {
        return extract_text(user_message);
    }

    Err("input must include actual.text or test-case.input-json.user_message".to_string())
}

fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(|token| token.to_ascii_lowercase())
        .collect()
}

fn score_text(text: &str) -> (i32, usize, usize) {
    let mut score = 0;
    let mut positive_matches = 0;
    let mut negative_matches = 0;

    for token in tokenize(text) {
        if POSITIVE_TERMS.contains(&token.as_str()) {
            score += 1;
            positive_matches += 1;
        }

        if NEGATIVE_TERMS.contains(&token.as_str()) {
            score -= 1;
            negative_matches += 1;
        }
    }

    (score, positive_matches, negative_matches)
}

fn label_from_score(score: i32) -> &'static str {
    if score > 0 {
        "positive"
    } else if score < 0 {
        "negative"
    } else {
        "neutral"
    }
}

impl Guest for Evaluator {
    fn requirements() -> vigilo::evaluator::types::EvidenceRequirements {
        vigilo::evaluator::types::EvidenceRequirements {
            original_body: false,
            mapped_items: false,
            content_media_types: Vec::new(),
            tool_calls: false,
            tool_results: false,
        }
    }

    fn evaluate(input: Input) -> Result<Output, EvaluatorError> {
        executor::trace("sentiment evaluator started");
        executor::debug(&format!(
            "input ids run={} execution={} attempt={}",
            input.run_id, input.execution_id, input.attempt_id
        ));

        let text = extract_text_from_input(&input).map_err(|message| EvaluatorError {
            code: "invalid_input".to_string(),
            message,
        })?;
        let (score, positive_matches, negative_matches) = score_text(&text);
        let label = label_from_score(score);

        let evidence_json = json!({
            "label": label,
            "score": score,
            "positive_matches": positive_matches,
            "negative_matches": negative_matches,
            "matched_terms": positive_matches + negative_matches,
            "text": text,
        })
        .to_string();

        let severity = if label == "negative" {
            Severity::Medium
        } else {
            Severity::None
        };

        Ok(Output {
            evaluator: EvaluatorIdentity {
                namespace: "vigilo".to_string(),
                name: "sentiment-basic-en".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                content_hash: None,
                interface_version: Some("1.0.0".to_string()),
            },
            outcome: EvaluatorOutcome::Completed(Measurement::Ordinal(label.to_string())),
            diagnostics: vec![DiagnosticFinding {
                severity,
                category: "sentiment_classification".to_string(),
                reason: Some(format!(
                    "basic English-only lexicon sentiment evaluator labeled input as {label}"
                )),
                evidence_json,
                tags: vec![
                    "sentiment".to_string(),
                    "basic".to_string(),
                    "english-only".to_string(),
                    label.to_string(),
                ],
            }],
            metadata_json: json!({
                "approach": "lexicon",
                "language_scope": "en",
                "language_scope_enforcement": "advisory",
                "maturity": "basic",
                "intended_use": "example_only",
                "configuration": {
                    "max_evidence_length": "not_configurable_yet"
                }
            })
            .to_string(),
        })
    }
}

export!(Evaluator);

#[cfg(test)]
mod tests {
    use super::{
        extract_text,
        extract_text_from_input,
        label_from_score,
        score_text,
        tokenize,
    };
    use crate::vigilo::evaluator::types::{
        AgentOutput,
        Input,
        TestCase,
    };

    #[test]
    fn tokenize_normalizes_and_strips_symbols() {
        let tokens = tokenize("Great service, fast response!");
        assert_eq!(tokens, vec!["great", "service", "fast", "response"]);
    }

    #[test]
    fn score_text_detects_positive_sentiment() {
        let (score, positives, negatives) = score_text("Great and helpful support");
        assert_eq!((score, positives, negatives), (2, 2, 0));
        assert_eq!(label_from_score(score), "positive");
    }

    #[test]
    fn score_text_detects_negative_sentiment() {
        let (score, positives, negatives) = score_text("Awful, slow, bad experience");
        assert_eq!((score, positives, negatives), (-3, 0, 3));
        assert_eq!(label_from_score(score), "negative");
    }

    #[test]
    fn score_text_returns_neutral_for_mixed_or_unknown() {
        let (score, positives, negatives) = score_text("Great but slow");
        assert_eq!((score, positives, negatives), (0, 1, 1));
        assert_eq!(label_from_score(score), "neutral");
    }

    #[test]
    fn extract_text_accepts_plain_text() {
        let text = extract_text("  plain text input  ").unwrap();
        assert_eq!(text, "plain text input");
    }

    #[test]
    fn extract_text_accepts_json_payload() {
        let text = extract_text(r#"{"text":"Excellent response"}"#).unwrap();
        assert_eq!(text, "Excellent response");
    }

    #[test]
    fn extract_text_rejects_invalid_or_empty_input() {
        assert!(extract_text("   ").is_err());
        assert!(extract_text(r#"{"message":"missing text"}"#).is_err());
    }

    #[test]
    fn extract_text_from_input_prefers_actual_text() {
        let input = Input {
            run_id: "run-1".to_string(),
            execution_id: "exec-1".to_string(),
            attempt_id: "attempt-1".to_string(),
            test_case: TestCase {
                id: "case-1".to_string(),
                task_type: "classification".to_string(),
                case_group: None,
                input_json: r#"{"user_message":"fallback"}"#.to_string(),
                expected_json: None,
                context_json: None,
                tags: vec![],
                metadata_json: "{}".to_string(),
            },
            response_evidence: None,
            actual: AgentOutput {
                text: Some("great experience".to_string()),
                structured_json: None,
                tool_calls: vec![],
                trace: vec![],
                raw_json: "{}".to_string(),
                metadata_json: "{}".to_string(),
            },
            evaluator_config_json: "{}".to_string(),
        };

        assert_eq!(extract_text_from_input(&input).unwrap(), "great experience");
    }

    #[test]
    fn extract_text_from_input_falls_back_when_actual_text_is_empty() {
        let input = Input {
            run_id: "run-1".to_string(),
            execution_id: "exec-1".to_string(),
            attempt_id: "attempt-1".to_string(),
            test_case: TestCase {
                id: "case-1".to_string(),
                task_type: "classification".to_string(),
                case_group: None,
                input_json: r#"{"user_message":"fallback message"}"#.to_string(),
                expected_json: None,
                context_json: None,
                tags: vec![],
                metadata_json: "{}".to_string(),
            },
            response_evidence: None,
            actual: AgentOutput {
                text: Some("   ".to_string()),
                structured_json: None,
                tool_calls: vec![],
                trace: vec![],
                raw_json: "{}".to_string(),
                metadata_json: "{}".to_string(),
            },
            evaluator_config_json: "{}".to_string(),
        };

        assert_eq!(extract_text_from_input(&input).unwrap(), "fallback message");
    }
}
