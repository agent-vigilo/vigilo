//! Bounded operational evidence for one agent invocation.
//!
//! Runtime IDs and an opaque endpoint fingerprint identify calls without copying
//! profile configuration, URLs, headers or response bodies into attempt summaries.
//! Timing separates preparation/credential resolution from HTTP send/body receipt.
//! Known adapter usage is optional evidence, never an inferred zero or a score.

use std::time::Instant;

use serde::Serialize;
use uuid::Uuid;

use super::failure::Kind;

pub(crate) const REQUEST_ID_HEADER: &str = "x-vigilo-attempt-id";

#[derive(Debug, Clone, Serialize)]
pub(crate) struct Diagnostics {
    pub(crate) run_id: Uuid,
    pub(crate) execution_id: Uuid,
    pub(crate) attempt_id: Uuid,
    pub(crate) endpoint_ref: String,
    pub(crate) elapsed_ms: u64,
    /// End-to-end HTTP send/body latency, absent when HTTP never started.
    pub(crate) latency_ms: Option<u64>,
    pub(crate) http_status: Option<u16>,
    pub(crate) failure_category: Option<Kind>,
    pub(crate) usage: Option<Usage>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct Usage {
    pub(crate) prompt_tokens: Option<u64>,
    pub(crate) completion_tokens: Option<u64>,
    pub(crate) total_tokens: Option<u64>,
}

/// Capture only understood integer counters; reject malformed counters as a group.
/// Omitted counters remain absent. Unknown provider fields are never copied.
pub(crate) fn chat_usage(value: &serde_json::Value) -> Option<Usage> {
    let usage = value.get("usage")?.as_object()?;
    let count = |name| match usage.get(name) {
        None | Some(serde_json::Value::Null) => Some(None),
        Some(value) => value.as_u64().map(Some),
    };
    let result = Usage {
        prompt_tokens: count("prompt_tokens")?,
        completion_tokens: count("completion_tokens")?,
        total_tokens: count("total_tokens")?,
    };
    (result.prompt_tokens.is_some()
        || result.completion_tokens.is_some()
        || result.total_tokens.is_some())
    .then_some(result)
}

pub(crate) struct Observation {
    started: Instant,
    http_started: Option<Instant>,
    pub(crate) data: Diagnostics,
}

impl Observation {
    pub(crate) fn new(run_id: Uuid, execution_id: Uuid, attempt_id: Uuid, endpoint: &str) -> Self {
        Self {
            started: Instant::now(),
            http_started: None,
            data: Diagnostics {
                run_id,
                execution_id,
                attempt_id,
                endpoint_ref: format!("blake3:{}", blake3::hash(endpoint.as_bytes()).to_hex()),
                elapsed_ms: 0,
                latency_ms: None,
                http_status: None,
                failure_category: None,
                usage: None,
            },
        }
    }
    pub(crate) fn start_http(&mut self) {
        self.http_started = Some(Instant::now());
    }
    pub(crate) fn finish_http(&mut self) {
        if self.data.latency_ms.is_none() {
            self.data.latency_ms = self.http_started.map(milliseconds);
        }
    }
    pub(crate) fn finish(mut self, failure: Option<Kind>) -> Diagnostics {
        self.finish_http();
        self.data.elapsed_ms = milliseconds(self.started);
        self.data.failure_category = failure;
        self.data
    }
}
fn milliseconds(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    #[test]
    fn usage_preserves_absence_zero_and_rejects_malformed_values() {
        for value in [
            json!({}),
            json!({"usage":{}}),
            json!({"usage":{"prompt_tokens":-1}}),
            json!({"usage":{"total_tokens":"12"}}),
            json!({"usage":{"total_tokens":1.5}}),
        ] {
            assert!(chat_usage(&value).is_none());
        }
        let usage = chat_usage(&json!({"usage":{"prompt_tokens":0,"secret":"canary"}})).unwrap();
        assert_eq!(usage.prompt_tokens, Some(0));
        assert_eq!(usage.completion_tokens, None);
        assert!(!serde_json::to_string(&usage).unwrap().contains("canary"));
    }
    #[test]
    fn preflight_diagnostics_are_bounded_and_do_not_expose_endpoint_values() {
        let observation = Observation::new(
            Uuid::nil(),
            Uuid::nil(),
            Uuid::nil(),
            "https://user:canary@example.com/path?token=canary",
        );
        let data = observation.finish(Some(Kind::Configuration));
        assert!(data.latency_ms.is_none());
        assert!(data.http_status.is_none());
        assert_eq!(data.endpoint_ref.len(), 71);
        let json = serde_json::to_string(&data).unwrap();
        assert!(!json.contains("canary"));
        assert!(!json.contains("example.com"));
        assert!(json.len() < 1024);
    }
}
