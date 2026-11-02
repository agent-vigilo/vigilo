//! Worker-side client for invoking the configured agent endpoint.
//!
//! This module owns request-format adaptation, HTTP transport, and response
//! capture without inferring response semantics. It returns bounded
//! operational diagnostics separately so the workflow can retain HTTP outcomes
//! even when later evaluator processing fails, without additional database calls.
//!
//! Supported request formats:
//!
//! - `vigilo_case`: the default Agent Vigilo request envelope for first-party
//!   or custom agent endpoints. It includes run/execution/attempt ids, agent
//!   identity, the case `input`, and non-oracle case metadata. It deliberately
//!   omits the expected output so agent endpoints cannot see evaluator answers.
//! - `openai_compatible_chat_completions`: an adapter for OpenAI-compatible
//!   `/v1/chat/completions` servers such as llama.cpp. It sends `model`,
//!   `messages`, `stream: false`, and selected generation options from
//!   `agent.config`.

#[path = "agent_client/failure.rs"]
pub(crate) mod failure;
use failure::{
    Failure,
    Kind,
};

#[path = "agent_client/limits.rs"]
pub(crate) mod limits;

#[path = "agent_client/credentials.rs"]
pub(crate) mod credentials;

#[path = "agent_client/diagnostics.rs"]
pub(crate) mod diagnostics;

/// Successful output plus bounded evidence retained independently of evaluator results.
#[derive(Debug)]
pub(crate) struct Invocation {
    pub(crate) output: AgentOutput,
    pub(crate) diagnostics: diagnostics::Diagnostics,
}

/// Process services used for one invocation; contains no resolved secrets.
pub(crate) struct Services<'a> {
    pub(crate) client: &'a reqwest::Client,
    pub(crate) limits: limits::Limits,
    pub(crate) credentials: &'a credentials::Provider,
}

#[path = "agent_client/capture.rs"]
pub(crate) mod capture;

use std::time::Duration;

use serde_json::{
    Value,
    json,
};
use uuid::Uuid;

use crate::contracts::{
    evaluator::{
        AgentOutput,
        TestCase,
    },
    run::RunProfile,
};

const REQUEST_FORMAT_VIGILO_CASE: &str = "vigilo_case";
const REQUEST_FORMAT_OPENAI_COMPATIBLE_CHAT_COMPLETIONS: &str =
    "openai_compatible_chat_completions";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentRequestFormat {
    /// Agent Vigilo's native request envelope for custom agent endpoints.
    ///
    /// This is the default when `agent.config.request_format` is absent. The
    /// payload carries Vigilo execution metadata and case input, but not the
    /// case's expected output.
    VigiloCase,

    /// OpenAI-compatible Chat Completions payload.
    ///
    /// This is intended for endpoints that implement the OpenAI
    /// `/v1/chat/completions` wire shape, including local llama.cpp server
    /// instances. The provider is still the configured agent; this name only
    /// describes the request body format.
    OpenAiCompatibleChatCompletions,
}

impl AgentRequestFormat {
    fn from_profile(run_profile: &RunProfile) -> anyhow::Result<Self> {
        let Some(format_value) = run_profile.agent.config.get("request_format") else {
            return Ok(Self::VigiloCase);
        };
        let Some(format) = format_value.as_str() else {
            anyhow::bail!("agent.config.request_format must be a string");
        };

        match format {
            REQUEST_FORMAT_VIGILO_CASE => Ok(Self::VigiloCase),
            REQUEST_FORMAT_OPENAI_COMPATIBLE_CHAT_COMPLETIONS => {
                Ok(Self::OpenAiCompatibleChatCompletions)
            }
            other => anyhow::bail!(
                "unsupported agent.config.request_format '{}'; expected '{}' or '{}'",
                other,
                REQUEST_FORMAT_VIGILO_CASE,
                REQUEST_FORMAT_OPENAI_COMPATIBLE_CHAT_COMPLETIONS
            ),
        }
    }
}

pub(crate) fn validate_request_format(run_profile: &RunProfile) -> anyhow::Result<()> {
    if let Some(response) = &run_profile.agent.response
        && let vigilo::response::Interpretation::Json { mapping } = &response.interpretation
    {
        mapping.validate().map_err(anyhow::Error::msg)?;
    }
    AgentRequestFormat::from_profile(run_profile).map(|_| ())
}

fn build_request_body(
    run_id: Uuid,
    execution_id: Uuid,
    attempt_id: Uuid,
    run_profile: &RunProfile,
    test_case: &TestCase,
) -> anyhow::Result<Value> {
    match AgentRequestFormat::from_profile(run_profile)? {
        AgentRequestFormat::VigiloCase => Ok(build_vigilo_case_request_body(
            run_id,
            execution_id,
            attempt_id,
            run_profile,
            test_case,
        )),
        AgentRequestFormat::OpenAiCompatibleChatCompletions => {
            build_openai_compatible_chat_completions_request_body(run_profile, test_case)
        }
    }
}

fn build_vigilo_case_request_body(
    run_id: Uuid,
    execution_id: Uuid,
    attempt_id: Uuid,
    run_profile: &RunProfile,
    test_case: &TestCase,
) -> Value {
    // Keep the default request format evaluator-safe: the case envelope mirrors
    // useful identifying metadata but excludes `expected`.
    json!({
        "run_id": run_id,
        "execution_id": execution_id,
        "attempt_id": attempt_id,
        "agent": {
            "provider": &run_profile.agent.provider,
            "name": &run_profile.agent.name,
            "version": &run_profile.agent.version,
            "model": &run_profile.agent.model,
            "config": &run_profile.agent.config,
        },
        "input": &test_case.input,
        "case": {
            "id": &test_case.id,
            "task_type": &test_case.task_type,
            "case_group": &test_case.case_group,
            "input": &test_case.input,
            "context": &test_case.context,
            "tags": &test_case.tags,
            "metadata": &test_case.metadata,
        },
    })
}

fn input_text_for_chat(test_case: &TestCase) -> anyhow::Result<String> {
    if let Some(message) = test_case.input.get("user_message").and_then(Value::as_str) {
        return Ok(message.to_string());
    }

    if let Some(prompt) = test_case.input.get("prompt").and_then(Value::as_str) {
        return Ok(prompt.to_string());
    }

    serde_json::to_string(&test_case.input)
        .map_err(|err| anyhow::anyhow!("failed to serialize case input for chat request: {}", err))
}

fn build_openai_compatible_chat_messages(
    run_profile: &RunProfile,
    test_case: &TestCase,
) -> anyhow::Result<Value> {
    let mut messages = Vec::new();
    if let Some(system_prompt) = run_profile
        .agent
        .config
        .get("system_prompt")
        .and_then(Value::as_str)
        && !system_prompt.trim().is_empty()
    {
        messages.push(json!({
            "role": "system",
            "content": system_prompt,
        }));
    }

    if let Some(input_messages) = test_case.input.get("messages").and_then(Value::as_array) {
        for message in input_messages {
            let role = message
                .get("role")
                .and_then(Value::as_str)
                .unwrap_or("user");
            let content = message
                .get("content")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    anyhow::anyhow!("input.messages entries must include string content")
                })?;
            messages.push(json!({
                "role": role,
                "content": content,
            }));
        }
    } else {
        messages.push(json!({
            "role": "user",
            "content": input_text_for_chat(test_case)?,
        }));
    }

    Ok(Value::Array(messages))
}

fn copy_chat_completion_option(
    body: &mut serde_json::Map<String, Value>,
    config: &Value,
    key: &str,
) {
    if let Some(value) = config.get(key) {
        body.insert(key.to_string(), value.clone());
    }
}

fn build_openai_compatible_chat_completions_request_body(
    run_profile: &RunProfile,
    test_case: &TestCase,
) -> anyhow::Result<Value> {
    // This adapter intentionally builds only the OpenAI-compatible chat
    // completions body. Vigilo execution ids and oracle fields are omitted
    // because generic model servers should not need Vigilo-specific context.
    let model = run_profile
        .agent
        .model
        .as_deref()
        .unwrap_or(&run_profile.agent.name);
    let mut body = serde_json::Map::new();
    body.insert("model".to_string(), Value::String(model.to_string()));
    body.insert(
        "messages".to_string(),
        build_openai_compatible_chat_messages(run_profile, test_case)?,
    );
    body.insert("stream".to_string(), Value::Bool(false));

    let config = &run_profile.agent.config;
    for key in [
        "temperature",
        "top_p",
        "min_p",
        "max_tokens",
        "stop",
        "seed",
        "response_format",
        "presence_penalty",
        "frequency_penalty",
        "repeat_penalty",
    ] {
        copy_chat_completion_option(&mut body, config, key);
    }

    if !body.contains_key("temperature") {
        body.insert("temperature".to_string(), json!(0.0));
    } else if body.get("temperature").and_then(Value::as_f64).is_none() {
        anyhow::bail!("agent.config.temperature must be numeric when provided");
    }

    Ok(Value::Object(body))
}

fn response_body_excerpt(body: &str) -> String {
    const MAX_ERROR_BODY_CHARS: usize = 512;
    body.chars().take(MAX_ERROR_BODY_CHARS).collect()
}

/// Preserve complete response bytes without guessing a provider envelope.
fn captured_output(body: vigilo::response::CapturedBody) -> AgentOutput {
    AgentOutput {
        captured_body: Some(body),
        mapping: None,
        text: None,
        structured: None,
        tool_calls: Vec::new(),
        trace: Vec::new(),
        raw: json!({}),
        metadata: json!({}),
    }
}

/// Attach bounded evidence to every typed outcome without changing retry policy.
pub(crate) async fn invoke(
    services: Services<'_>,
    run_id: Uuid,
    execution_id: Uuid,
    attempt_id: Uuid,
    run_profile: &RunProfile,
    test_case: &TestCase,
) -> anyhow::Result<Invocation> {
    let mut observation = diagnostics::Observation::new(
        run_id,
        execution_id,
        attempt_id,
        &run_profile.agent.http.url,
    );
    let result = invoke_inner(
        services,
        run_id,
        execution_id,
        attempt_id,
        run_profile,
        test_case,
        &mut observation,
    )
    .await;
    match result {
        Ok(output) => Ok(Invocation {
            output,
            diagnostics: observation.finish(None),
        }),
        Err(error) => match error.downcast::<Failure>() {
            Ok(mut failure) => {
                failure.invocation = Some(Box::new(observation.finish(Some(failure.kind))));
                Err(failure.into())
            }
            Err(error) => Err(error),
        },
    }
}

async fn invoke_inner(
    services: Services<'_>,
    run_id: Uuid,
    execution_id: Uuid,
    attempt_id: Uuid,
    run_profile: &RunProfile,
    test_case: &TestCase,
    observation: &mut diagnostics::Observation,
) -> anyhow::Result<AgentOutput> {
    validate_request_format(run_profile)
        .map_err(|err| Failure::terminal(Kind::Configuration, err))?;
    let http = &run_profile.agent.http;
    if http
        .headers
        .keys()
        .any(|name| name.eq_ignore_ascii_case(diagnostics::REQUEST_ID_HEADER))
    {
        return Err(Failure::terminal(
            Kind::Configuration,
            "profile overrides the host attempt correlation header",
        )
        .into());
    }
    let limits = services
        .limits
        .resolve(http)
        .map_err(|err| Failure::terminal(Kind::Configuration, err))?;
    let max_retry_after = http
        .max_retry_after_secs
        .unwrap_or(failure::DEFAULT_MAX_RETRY_AFTER_SECS);
    if max_retry_after == 0 || max_retry_after > failure::MAX_RETRY_AFTER_SECS {
        return Err(Failure::terminal(
            Kind::Configuration,
            "agent.http.max_retry_after_secs must be between 1 and 86400",
        )
        .into());
    }
    let timeout_secs = http
        .timeout_secs
        .unwrap_or(run_profile.defaults.request_timeout_secs);
    if timeout_secs == 0 {
        return Err(Failure::terminal(
            Kind::Configuration,
            "agent HTTP timeout_secs must be greater than zero",
        )
        .into());
    }

    let method = http.method.parse::<reqwest::Method>().map_err(|err| {
        Failure::terminal(
            Kind::Configuration,
            format!("invalid agent HTTP method: {err}"),
        )
    })?;

    let request_body = build_request_body(run_id, execution_id, attempt_id, run_profile, test_case)
        .map_err(|err| Failure::terminal(Kind::Configuration, err))?;
    // Resolve only after the request is serialized and bounded. Credentials are
    // added to transport headers, never the profile or evaluator payload.
    let request_body = limits::serialize(&request_body, limits.max_request_bytes)
        .map_err(|err| Failure::terminal(Kind::BodyLimit, err))?;
    let resolved = match http.credential.as_deref() {
        Some(reference) => Some(
            services
                .credentials
                .resolve(
                    reference,
                    &http.url,
                    &http.headers,
                    credentials::Execution {
                        run_id,
                        execution_id,
                        attempt_id,
                    },
                )
                .await?,
        ),
        None => None,
    };
    let client = if resolved.is_some() {
        services.credentials.client().await?
    } else {
        services.client
    };
    let mut request = client
        .request(method, &http.url)
        .timeout(Duration::from_secs(u64::from(timeout_secs)))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(request_body);
    for (name, value) in &http.headers {
        request = request.header(name, value);
    }

    if let Some(resolved) = resolved {
        resolved.check_validity()?;
        if resolved.name.as_str() == diagnostics::REQUEST_ID_HEADER {
            return Err(Failure::terminal(
                Kind::Configuration,
                "credential binding overrides the host attempt correlation header",
            )
            .into());
        }
        request = request.header(resolved.name, resolved.value);
    }
    request = request.header(diagnostics::REQUEST_ID_HEADER, attempt_id.to_string());
    let request = request
        .build()
        .map_err(|err| Failure::transport(err, "invalid agent HTTP request", None))?;
    observation.start_http();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(u64::from(timeout_secs));
    let response = client
        .execute(request)
        .await
        .map_err(|err| Failure::transport(err, "agent HTTP request failed", None))?;
    let status = response.status();
    observation.data.http_status = Some(status.as_u16());
    let http_failure = (!status.is_success()).then(|| {
        Failure::http(
            status.as_u16(),
            response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
            chrono::Utc::now(),
            max_retry_after,
        )
    });
    let captured = capture::collect(response, limits.max_response_bytes, deadline)
        .await
        .map_err(|error| {
            if error.kind == Kind::BodyLimit {
                error
            } else {
                http_failure.clone().unwrap_or(error)
            }
        })?;
    let bytes = captured.bytes();
    observation.finish_http();
    // Match reqwest text decoding: strip a UTF-8 BOM and replace invalid UTF-8.
    let bytes = bytes.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(bytes);
    let body = String::from_utf8_lossy(bytes);

    if let Some(failure) = http_failure {
        return Err(if http.credential.is_some() {
            // Authenticated endpoints may echo credentials in error bodies.
            failure
        } else {
            failure.with_excerpt(&response_body_excerpt(&body))
        }
        .into());
    }

    // Usage is operational data for the explicitly selected request adapter;
    // parsing failure never invalidates successfully delivered behavior.
    if AgentRequestFormat::from_profile(run_profile)?
        == AgentRequestFormat::OpenAiCompatibleChatCompletions
        && let Ok(value) = serde_json::from_slice::<Value>(captured.bytes())
    {
        observation.data.usage = diagnostics::chat_usage(&value);
    }
    Ok(captured_output(captured))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn captured_output_does_not_infer_or_decode_response_shapes() {
        for bytes in [
            b"{invalid".as_slice(),
            b"",
            &[0xff, 0],
            br#"{"text":"secret","choices":[]}"#,
        ] {
            let output = captured_output(vigilo::response::CapturedBody::new(bytes.to_vec(), None));
            assert_eq!(output.captured_body.unwrap().bytes(), bytes);
            assert!(output.text.is_none());
            assert!(output.structured.is_none());
            assert!(output.mapping.is_none());
            assert_eq!(output.raw, json!({}));
            assert_eq!(output.metadata, json!({}));
        }
    }

    #[test]
    fn validate_request_format_rejects_unknown_value() {
        let profile: RunProfile = serde_yaml::from_str(
            r#"
profile_id: p
profile_version: 1
description: d
defaults:
  max_attempts: 1
  request_timeout_secs: 30
  fail_on_any_blocking_failure: true
  min_execution_score: 0.5
persistence:
  mode: full
  persist_raw_outputs: all
  persist_evaluator_evidence: true
agent:
  provider: example
  name: agent
  http:
    url: http://agent_vigilo_agent:8080/v1/chat/completions
  config:
    request_format: unsupported
case_groups: []
"#,
        )
        .unwrap();

        let err = validate_request_format(&profile).unwrap_err();
        assert!(
            err.to_string()
                .contains("expected 'vigilo_case' or 'openai_compatible_chat_completions'")
        );
    }

    #[test]
    fn validate_request_format_rejects_non_string_value() {
        let profile: RunProfile = serde_yaml::from_str(
            r#"
profile_id: p
profile_version: 1
description: d
defaults:
  max_attempts: 1
  request_timeout_secs: 30
  fail_on_any_blocking_failure: true
  min_execution_score: 0.5
persistence:
  mode: full
  persist_raw_outputs: all
  persist_evaluator_evidence: true
agent:
  provider: example
  name: agent
  http:
    url: http://agent_vigilo_agent:8080/v1/chat/completions
  config:
    request_format: 12
case_groups: []
"#,
        )
        .unwrap();

        let err = validate_request_format(&profile).unwrap_err();
        assert_eq!(
            err.to_string(),
            "agent.config.request_format must be a string"
        );
    }

    #[test]
    fn openai_compatible_chat_request_uses_model_and_input_message() {
        let profile: RunProfile = serde_yaml::from_str(
            r#"
profile_id: p
profile_version: 1
description: d
defaults:
  max_attempts: 1
  request_timeout_secs: 30
  fail_on_any_blocking_failure: true
  min_execution_score: 0.5
persistence:
  mode: full
  persist_raw_outputs: all
  persist_evaluator_evidence: true
agent:
  provider: llama.cpp
  name: qwen
  model: qwen2.5-0.5b-instruct-q4_k_m.gguf
  http:
    url: http://agent_vigilo_agent:8080/v1/chat/completions
  config:
    request_format: openai_compatible_chat_completions
    system_prompt: classify sentiment
    max_tokens: 64
case_groups: []
"#,
        )
        .unwrap();
        let test_case = TestCase {
            id: "case-1".to_string(),
            task_type: "classification".to_string(),
            case_group: None,
            input: json!({"user_message": "I love this product."}),
            expected: Some(json!({"label": "positive"})),
            context: None,
            tags: Vec::new(),
            metadata: Default::default(),
        };

        let body = build_request_body(
            Uuid::now_v7(),
            Uuid::now_v7(),
            Uuid::now_v7(),
            &profile,
            &test_case,
        )
        .unwrap();

        assert_eq!(
            body.get("model").and_then(Value::as_str),
            Some("qwen2.5-0.5b-instruct-q4_k_m.gguf")
        );
        assert_eq!(
            body.pointer("/messages/1/content").and_then(Value::as_str),
            Some("I love this product.")
        );
        assert!(body.get("case").is_none());
        assert_eq!(body.get("stream").and_then(Value::as_bool), Some(false));
    }
}
