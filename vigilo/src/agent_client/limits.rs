//! Byte budgets for agent HTTP bodies. Worker ceilings bound optional profile
//! limits; serialization and incremental reads reject overflow before retaining
//! excess bytes. These budgets do not bound the complete worker working set.

use std::io::Write;

use crate::contracts::run::AgentHttpConfig;

pub(crate) const DEFAULT_BODY_BYTES: u64 = 4 * 1024 * 1024;

/// Worker ceilings, or effective limits after profile validation.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    pub(crate) max_request_bytes: u64,
    pub(crate) max_response_bytes: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_request_bytes: DEFAULT_BODY_BYTES,
            max_response_bytes: DEFAULT_BODY_BYTES,
        }
    }
}

impl Limits {
    /// Reject invalid or excessive overrides before doing network I/O.
    pub(crate) fn resolve(self, profile: &AgentHttpConfig) -> anyhow::Result<Self> {
        fn resolve(name: &str, requested: Option<u64>, ceiling: u64) -> anyhow::Result<u64> {
            let value = requested.unwrap_or(ceiling);
            anyhow::ensure!(
                value > 0 && value <= ceiling,
                "agent.http.{name} must be between 1 and worker ceiling {ceiling}"
            );
            Ok(value)
        }
        Ok(Self {
            max_request_bytes: resolve(
                "max_request_bytes",
                profile.max_request_bytes,
                self.max_request_bytes,
            )?,
            max_response_bytes: resolve(
                "max_response_bytes",
                profile.max_response_bytes,
                self.max_response_bytes,
            )?,
        })
    }
}

/// Collect bytes only while they fit; never append a partial oversized chunk.
pub(crate) struct Body {
    bytes: Vec<u8>,
    limit: u64,
}

impl Body {
    pub(crate) fn new(limit: u64) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
        }
    }

    pub(crate) fn append(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        if bytes.len() as u64 > self.limit.saturating_sub(self.bytes.len() as u64) {
            return Err(std::io::Error::other(format!(
                "agent HTTP body exceeds {} byte limit",
                self.limit
            )));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    pub(crate) fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for Body {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.append(bytes)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Serialize directly into a bounded buffer rather than allocating then checking.
pub(crate) fn serialize(value: &serde_json::Value, limit: u64) -> anyhow::Result<Vec<u8>> {
    let mut body = Body::new(limit);
    serde_json::to_writer(&mut body, value)
        .map_err(|err| anyhow::anyhow!("agent HTTP request serialization failed: {err}"))?;
    Ok(body.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_limit_counts_encoded_bytes_and_preserves_json() {
        let value = serde_json::json!({"text":"é\n\""});
        let expected = serde_json::to_vec(&value).unwrap();
        assert_eq!(serialize(&value, expected.len() as u64).unwrap(), expected);
        assert!(serialize(&value, expected.len() as u64 - 1).is_err());
    }

    #[test]
    fn chunked_body_rejects_overflow_without_appending_and_counts_utf8() {
        let mut body = Body::new(3);
        body.append(b"a").unwrap();
        body.append("é".as_bytes()).unwrap();
        body.append(b"").unwrap();
        assert!(body.append(b"x").is_err());
        assert_eq!(body.finish(), "aé".as_bytes());
        assert!(Body::new(1).append(b"oversized first chunk").is_err());
    }

    #[test]
    fn profile_limits_default_lower_and_reject_excess_or_zero() {
        let mut profile: AgentHttpConfig =
            serde_json::from_value(serde_json::json!({"url":"http://localhost"})).unwrap();
        let ceilings = Limits {
            max_request_bytes: 10,
            max_response_bytes: 20,
        };
        assert_eq!(ceilings.resolve(&profile).unwrap().max_response_bytes, 20);
        profile.max_request_bytes = Some(10);
        profile.max_response_bytes = Some(5);
        assert_eq!(ceilings.resolve(&profile).unwrap().max_response_bytes, 5);
        profile.max_response_bytes = Some(21);
        assert!(ceilings.resolve(&profile).is_err());
        profile.max_response_bytes = Some(0);
        assert!(ceilings.resolve(&profile).is_err());
        profile.max_response_bytes = None;
        profile.max_request_bytes = Some(11);
        assert!(ceilings.resolve(&profile).is_err());
        profile.max_request_bytes = Some(0);
        assert!(ceilings.resolve(&profile).is_err());
    }
}
