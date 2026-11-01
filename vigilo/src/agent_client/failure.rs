//! Typed agent invocation failures and bounded server-directed retry scheduling.
//!
//! Classification belongs at the HTTP boundary. The durable execution scheduler
//! consumes retryability and an absolute not-before time, never message text.
//! Non-agent execution failures retain their existing policy.

use chrono::{
    DateTime,
    Utc,
};
use serde::Serialize;

pub(crate) const DEFAULT_MAX_RETRY_AFTER_SECS: u32 = 600;
pub(crate) const MAX_RETRY_AFTER_SECS: u32 = 86_400;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Kind {
    Configuration,
    CredentialUnavailable,
    Authentication,
    RateLimited,
    HttpStatus,
    Timeout,
    Transport,
    BodyLimit,
    InvalidResponse,
    RetryDelayExceeded,
}

/// Safe diagnostic data retained in the attempt summary alongside retry policy.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct Failure {
    pub(crate) kind: Kind,
    pub(crate) http_status: Option<u16>,
    pub(crate) retryable: bool,
    pub(crate) retry_not_before: Option<DateTime<Utc>>,
    /// Delta-seconds advice is also applied against database time at persistence.
    pub(crate) retry_delay_secs: Option<u32>,
    /// Carried to the workflow; persisted separately as uniform attempt evidence.
    #[serde(skip)]
    pub(crate) invocation: Option<Box<super::diagnostics::Diagnostics>>,
    message: String,
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.message.fmt(f)
    }
}
impl std::error::Error for Failure {}

impl Failure {
    /// Transient resolver failures reuse durable attempt retries, with no remote
    /// diagnostic body or credential material in persisted error details.
    pub(crate) fn credential_unavailable() -> Self {
        let mut failure = Self::terminal(
            Kind::CredentialUnavailable,
            "credential provider unavailable",
        );
        failure.retryable = true;
        failure
    }

    pub(crate) fn terminal(kind: Kind, message: impl ToString) -> Self {
        Self {
            kind,
            invocation: None,
            message: message.to_string(),
            http_status: None,
            retryable: false,
            retry_not_before: None,
            retry_delay_secs: None,
        }
    }

    pub(crate) fn transport(error: reqwest::Error, phase: &str, status: Option<u16>) -> Self {
        let kind = if error.is_timeout() {
            Kind::Timeout
        } else if error.is_builder() || error.is_redirect() {
            Kind::Configuration
        } else {
            Kind::Transport
        };
        Self {
            kind,
            http_status: status,
            retryable: matches!(kind, Kind::Timeout | Kind::Transport),
            retry_not_before: None,
            retry_delay_secs: None,
            invocation: None,
            message: format!("{phase}: {}", error.without_url()),
        }
    }

    /// Interpret HTTP policy at response receipt time. Invalid advice falls back
    /// to local backoff; valid advice beyond the configured horizon is terminal.
    pub(crate) fn http(
        status: u16,
        retry_after: Option<&str>,
        now: DateTime<Utc>,
        max_delay: u32,
    ) -> Self {
        let kind = match status {
            401 | 403 => Kind::Authentication,
            429 => Kind::RateLimited,
            _ => Kind::HttpStatus,
        };
        let mut result = Self {
            kind,
            http_status: Some(status),
            retryable: matches!(status, 408 | 425 | 429 | 500 | 502 | 503 | 504),
            retry_not_before: None,
            retry_delay_secs: None,
            invocation: None,
            message: format!("agent HTTP request failed with status {status}"),
        };
        if !result.retryable {
            return result;
        }
        if let Some(value) = retry_after {
            match not_before(value, now, max_delay) {
                Ok(time) => {
                    result.retry_not_before = time;
                    if time.is_some() {
                        result.retry_delay_secs = value.trim().parse::<u32>().ok();
                    }
                }
                Err(()) => {
                    result.kind = Kind::RetryDelayExceeded;
                    result.retryable = false;
                    result.message = format!(
                        "agent HTTP status {status}: Retry-After exceeds configured {max_delay} second limit"
                    );
                }
            }
        }
        result
    }

    pub(crate) fn with_excerpt(mut self, excerpt: &str) -> Self {
        if !excerpt.is_empty() {
            self.message.push_str(": ");
            self.message.push_str(excerpt);
        }
        self
    }

    pub(crate) fn with_status(mut self, status: u16) -> Self {
        self.http_status = Some(status);
        self
    }
}

fn not_before(
    value: &str,
    now: DateTime<Utc>,
    max_delay: u32,
) -> Result<Option<DateTime<Utc>>, ()> {
    let value = value.trim();
    if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) {
        let seconds: u64 = value.parse().map_err(|_| ())?;
        if seconds > u64::from(max_delay) {
            return Err(());
        }
        return Ok(Some(now + chrono::Duration::seconds(seconds as i64)));
    }
    if value.len() > 128 {
        return Ok(None);
    }
    let Ok(date) = httpdate::parse_http_date(value) else {
        return Ok(None);
    };
    let date = DateTime::<Utc>::from(date).max(now);
    if date > now + chrono::Duration::seconds(i64::from(max_delay)) {
        return Err(());
    }
    Ok(Some(date))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-10-31T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn statuses_have_explicit_retry_semantics() {
        for status in [408, 425, 429, 500, 502, 503, 504] {
            assert!(Failure::http(status, None, now(), 600).retryable);
        }
        for status in [400, 401, 403, 404, 409, 413, 422, 501, 505] {
            assert!(!Failure::http(status, None, now(), 600).retryable);
        }
        assert_eq!(
            Failure::http(401, Some("60"), now(), 600).kind,
            Kind::Authentication
        );
        assert_eq!(Failure::http(429, None, now(), 600).kind, Kind::RateLimited);
    }

    #[test]
    fn server_delay_preserves_exact_boundary_dates_and_rejects_excess() {
        let date = httpdate::fmt_http_date((now() + chrono::Duration::seconds(600)).into());
        for value in ["600", date.as_str()] {
            let failure = Failure::http(503, Some(value), now(), 600);
            assert!(failure.retryable);
            assert_eq!(
                failure.retry_not_before,
                Some(now() + chrono::Duration::seconds(600))
            );
        }
        for value in ["601", "99999999999999999999999999999999"] {
            let failure = Failure::http(429, Some(value), now(), 600);
            assert!(!failure.retryable);
            assert_eq!(failure.kind, Kind::RetryDelayExceeded);
        }
        let distant = httpdate::fmt_http_date((now() + chrono::Duration::days(1)).into());
        assert!(!Failure::http(503, Some(&distant), now(), 600).retryable);
    }

    #[test]
    fn malformed_and_past_delays_do_not_remove_local_backoff() {
        for value in ["", "garbage", "-1", "1.5"] {
            let failure = Failure::http(429, Some(value), now(), 600);
            assert!(failure.retryable);
            assert!(failure.retry_not_before.is_none());
        }
        let past = httpdate::fmt_http_date((now() - chrono::Duration::days(1)).into());
        assert_eq!(
            Failure::http(503, Some(&past), now(), 600).retry_not_before,
            Some(now())
        );
    }
}
