//! English lexicon sentiment over explicitly mapped text message content.
//! The evaluator never substitutes the case prompt, guesses JSON fields, or scores
//! tool arguments. Resource-backed text is read through bounded host capabilities.
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

const MAX_TEXT_BYTES: usize = 4 * 1024 * 1024;

/// Read exactly the declared range, rejecting short reads, invalid UTF-8, or
/// exhausted budgets before any score is produced.
fn read_text(handle: u64, offset: u64, length: u64, remaining: usize) -> Result<String, String> {
    if length > remaining as u64 || offset.checked_add(length).is_none() {
        return Err("mapped text exceeds evaluator byte limit".into());
    }
    let mut bytes = Vec::with_capacity(length as usize);
    while bytes.len() < length as usize {
        let count = (length as usize - bytes.len()).min(64 * 1024) as u32;
        let chunk = executor::read_resource(handle, offset + bytes.len() as u64, count)
            .map_err(|_| "mapped text resource is unavailable".to_string())?;
        if chunk.is_empty() || chunk.len() > count as usize {
            return Err("mapped text resource returned an invalid range".into());
        }
        bytes.extend(chunk);
    }
    String::from_utf8(bytes).map_err(|_| "mapped text is not UTF-8".into())
}

/// Score only text/plain message parts, in declared order. Each part is separated
/// by a newline so tokens cannot merge across independent content boundaries.
fn extract_text_from_input(input: &Input) -> Result<String, String> {
    use vigilo::evaluator::types::{
        ContentSource,
        OutputItem,
    };
    let evidence = input
        .response_evidence
        .as_ref()
        .ok_or("mapped response evidence is required")?;
    let items = evidence
        .items
        .as_ref()
        .ok_or("mapped response items are required")?;
    let mut text = String::new();
    for item in items {
        let OutputItem::Message(message) = item else {
            continue;
        };
        for part in &message.content {
            if part.media_type != "text/plain" {
                continue;
            }
            if !text.is_empty() {
                text.push('\n');
            }
            let remaining = MAX_TEXT_BYTES
                .checked_sub(text.len())
                .ok_or("mapped text exceeds evaluator byte limit")?;
            let value = match &part.source {
                ContentSource::InlineText(value) => {
                    if value.len() > remaining {
                        return Err("mapped text exceeds evaluator byte limit".into());
                    }
                    value.clone()
                }
                ContentSource::BodyRange(range) => read_text(
                    evidence.body.resource,
                    range.offset,
                    range.byte_length,
                    remaining,
                )?,
                ContentSource::Resource(resource) => {
                    read_text(resource.handle, 0, resource.byte_length, remaining)?
                }
            };
            text.push_str(&value);
        }
    }
    if text.trim().is_empty() {
        return Err("nonempty mapped text/plain message content is required".into());
    }
    Ok(text)
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
            mapped_items: true,
            content_media_types: vec!["text/plain".into()],
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

    fn input() -> Input {
        Input {
            run_id: "run".into(),
            execution_id: "execution".into(),
            attempt_id: "attempt".into(),
            test_case: TestCase {
                id: "case".into(),
                task_type: "classification".into(),
                case_group: None,
                input_json: r#"{"user_message":"great prompt"}"#.into(),
                expected_json: None,
                context_json: None,
                tags: vec![],
                metadata_json: "{}".into(),
            },
            response_evidence: None,
            actual: AgentOutput {
                text: Some("great legacy text".into()),
                structured_json: None,
                tool_calls: vec![],
                trace: vec![],
                raw_json: "{}".into(),
                metadata_json: "{}".into(),
            },
            evaluator_config_json: "{}".into(),
        }
    }
    fn mapped(parts: &[(&str, &str)]) -> Input {
        use crate::vigilo::evaluator::types::*;
        let mut input = input();
        input.response_evidence = Some(ResponseEvidence {
            schema_version: "1.0.0".into(),
            body: BodyMetadata {
                resource: 1,
                byte_length: 0,
                content_type: None,
            },
            items: Some(vec![OutputItem::Message(Message {
                id: None,
                role: None,
                content: parts
                    .iter()
                    .map(|(media, text)| ContentPart {
                        media_type: (*media).into(),
                        source: ContentSource::InlineText((*text).into()),
                    })
                    .collect(),
            })]),
            status: None,
            stop_reason: None,
        });
        input
    }
    #[test]
    fn missing_mapping_never_falls_back_to_prompt_or_actual_text() {
        assert!(extract_text_from_input(&input()).is_err());
        let mut value = mapped(&[]);
        assert!(extract_text_from_input(&value).is_err());
        value.response_evidence.as_mut().unwrap().items = None;
        assert!(extract_text_from_input(&value).is_err());
    }
    #[test]
    fn mapped_text_preserves_order_and_does_not_parse_json_or_score_other_media() {
        let value = mapped(&[
            ("text/plain", "bad"),
            ("image/png", "great"),
            ("text/plain", "slow"),
        ]);
        let text = extract_text_from_input(&value).unwrap();
        assert_eq!(text, "bad\nslow");
        assert_eq!(label_from_score(score_text(&text).0), "negative");
        let literal = r#"{"text":"good"}"#;
        assert_eq!(
            extract_text_from_input(&mapped(&[("text/plain", literal)])).unwrap(),
            literal
        );
    }
    #[test]
    fn mapped_text_rejects_empty_wrong_media_and_oversize_content() {
        for parts in [
            vec![],
            vec![("text/plain", "  ")],
            vec![("application/json", "good")],
        ] {
            assert!(extract_text_from_input(&mapped(&parts)).is_err());
        }
        assert!(
            extract_text_from_input(&mapped(&[(
                "text/plain",
                &"x".repeat(super::MAX_TEXT_BYTES + 1)
            )]))
            .is_err()
        );
    }
    #[test]
    fn requirements_are_explicit() {
        use super::{
            Evaluator,
            Guest,
        };
        let requirements = Evaluator::requirements();
        assert!(requirements.mapped_items);
        assert_eq!(requirements.content_media_types, ["text/plain"]);
    }
}
