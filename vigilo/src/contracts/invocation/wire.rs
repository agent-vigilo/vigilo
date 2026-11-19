//! The native harness protocol. Decode admitted JSON bodies with exact version,
//! shape, identity and byte limits before handing outcomes to execution code.
//! Authentication and durable invocation eligibility are execution boundaries.

use anyhow::{
    Result,
    ensure,
};
use base64::{
    Engine,
    engine::general_purpose::STANDARD,
};
use chrono::{
    DateTime,
    FixedOffset,
};
use serde::{
    Deserialize,
    Serialize,
    de::DeserializeOwned,
};
use uuid::Uuid;

use super::profile::{
    AgentIdentity,
    endpoint_url,
};
use crate::contracts::case_storage::strict_json;

pub const SCHEMA_VERSION: &str = "1.0.0";
pub const MAX_EVIDENCE_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_OUTCOME_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_RECEIPT_BYTES: usize = 8 * 1024;
pub const MAX_REFERENCE_BYTES: usize = 256;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Submission {
    pub schema_version: String,
    pub invocation_id: Uuid,
    pub requested_agent: AgentIdentity,
    pub deadline_at: DateTime<FixedOffset>,
    pub case: CaseInput,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub callback: Option<Callback>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaseInput {
    pub input: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<serde_json::Value>,
}

/// Host-provided delivery address and invocation-scoped secret.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Callback {
    pub url: String,
    pub token: String,
}

impl Submission {
    /// Validates the native submission and its selected delivery shape.
    pub fn validate(&self, callback: bool) -> Result<()> {
        check_identity(&self.schema_version, self.invocation_id, self.invocation_id)?;
        self.requested_agent.validate()?;
        ensure!(
            callback == self.callback.is_some(),
            "unexpected callback configuration"
        );
        if let Some(callback) = &self.callback {
            ensure!(
                endpoint_url(&callback.url)?.scheme() == "https",
                "callback requires HTTPS"
            );
            ensure!(
                !callback.token.is_empty()
                    && callback.token.bytes().all(|byte| byte.is_ascii_graphic()),
                "invalid callback credential"
            );
        }
        Ok(())
    }

    /// Parses a submission using the caller's configured HTTP request limit.
    pub fn decode(body: &[u8], limit: usize, callback: bool) -> Result<Self> {
        let message: Self = decode(body, limit)?;
        message.validate(callback)?;
        Ok(message)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    schema_version: String,
    invocation_id: Uuid,
    status: Accepted,
    pub provider_reference: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Accepted {
    Accepted,
}

impl Receipt {
    /// Validates an acceptance receipt against the submitted invocation.
    pub fn decode(body: &[u8], invocation_id: Uuid, polling: bool) -> Result<Self> {
        let receipt: Self = decode(body, MAX_RECEIPT_BYTES)?;
        check_identity(
            &receipt.schema_version,
            receipt.invocation_id,
            invocation_id,
        )?;
        let Accepted::Accepted = receipt.status;
        ensure!(
            !polling || receipt.provider_reference.is_some(),
            "polling requires a provider reference"
        );
        check_reference(receipt.provider_reference.as_deref())?;
        Ok(receipt)
    }
}

/// A final outcome on the wire. `decode` validates its version, shape and
/// correlation. The caller owns authentication and durable state checks.
#[derive(Debug, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum Outcome {
    Completed {
        schema_version: String,
        invocation_id: Uuid,
        executed_agent: Option<AgentIdentity>,
        evidence: Evidence,
    },
    Failed {
        schema_version: String,
        invocation_id: Uuid,
        error: Failure,
    },
}

/// Polling can return a pending acknowledgment or the common final outcome.
#[derive(Debug)]
pub enum PollResponse {
    Pending,
    Final(Outcome),
}

#[derive(Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum PendingResponse {
    Pending {
        schema_version: String,
        invocation_id: Uuid,
    },
}

impl PollResponse {
    /// Validates a poll reply. A mixed reply stream is admitted under the
    /// outcome limit and pending replies must also fit the smaller budget.
    pub fn decode(
        body: &[u8],
        invocation_id: Uuid,
        agent: &AgentIdentity,
        require_executed_agent: bool,
    ) -> Result<Self> {
        let value = parse(body, MAX_OUTCOME_BYTES)?;
        if value.get("status").and_then(serde_json::Value::as_str) == Some("pending") {
            ensure!(
                body.len() <= MAX_RECEIPT_BYTES,
                "pending response exceeds byte limit"
            );
            let PendingResponse::Pending {
                schema_version,
                invocation_id: reported,
            } = from_value(value)?;
            check_identity(&schema_version, reported, invocation_id)?;
            return Ok(Self::Pending);
        }
        let outcome: Outcome = from_value(value)?;
        outcome.validate(invocation_id, agent, require_executed_agent)?;
        Ok(Self::Final(outcome))
    }
}

impl Outcome {
    /// Validates a final outcome and binds it to the requested identity.
    pub fn decode(
        body: &[u8],
        invocation_id: Uuid,
        agent: &AgentIdentity,
        require_executed_agent: bool,
    ) -> Result<Self> {
        let outcome: Self = decode(body, MAX_OUTCOME_BYTES)?;
        outcome.validate(invocation_id, agent, require_executed_agent)?;
        Ok(outcome)
    }

    fn validate(
        &self,
        expected: Uuid,
        agent: &AgentIdentity,
        require_executed_agent: bool,
    ) -> Result<()> {
        match self {
            Self::Completed {
                schema_version,
                invocation_id,
                executed_agent,
                ..
            } => {
                check_identity(schema_version, *invocation_id, expected)?;
                ensure!(
                    !require_executed_agent || executed_agent.is_some(),
                    "executed agent is required"
                );
                ensure!(
                    executed_agent
                        .as_ref()
                        .is_none_or(|reported| reported == agent),
                    "executed agent mismatch"
                );
            }
            Self::Failed {
                schema_version,
                invocation_id,
                error,
            } => {
                check_identity(schema_version, *invocation_id, expected)?;
                ensure!(
                    !error.code.is_empty()
                        && error.code.len() <= 64
                        && error.code.bytes().all(|byte| byte.is_ascii_graphic()),
                    "invalid failure code"
                );
                ensure!(
                    error
                        .message
                        .as_ref()
                        .is_none_or(|message| message.len() <= 512),
                    "failure message exceeds byte limit"
                );
            }
        }
        Ok(())
    }
}

/// Raw evidence recovered from the explicitly encoded body.
#[derive(Debug)]
pub struct Evidence {
    pub content_type: String,
    pub bytes: Vec<u8>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EncodedEvidence {
    content_type: String,
    encoding: Encoding,
    body: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Encoding {
    Utf8,
    Base64,
}

impl<'de> Deserialize<'de> for Evidence {
    fn deserialize<D: serde::Deserializer<'de>>(input: D) -> std::result::Result<Self, D::Error> {
        use serde::de::Error as _;
        let EncodedEvidence {
            content_type,
            encoding,
            body,
        } = EncodedEvidence::deserialize(input)?;
        if content_type.trim().is_empty() || content_type.chars().any(char::is_control) {
            return Err(D::Error::custom("invalid evidence content type"));
        }
        let bytes = match encoding {
            Encoding::Utf8 => body.into_bytes(),
            Encoding::Base64 => {
                if body.len() > MAX_EVIDENCE_BYTES.div_ceil(3) * 4 {
                    return Err(D::Error::custom("evidence exceeds decoded byte limit"));
                }
                STANDARD
                    .decode(body)
                    .map_err(|_| D::Error::custom("invalid base64 evidence"))?
            }
        };
        if bytes.len() > MAX_EVIDENCE_BYTES {
            return Err(D::Error::custom("evidence exceeds decoded byte limit"));
        }
        Ok(Self {
            content_type,
            bytes,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Failure {
    pub code: String,
    pub message: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cancellation {
    pub schema_version: String,
    pub invocation_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_reference: Option<String>,
}

impl Cancellation {
    /// Parses the body of a cancellation request within the receiver's limit.
    pub fn decode(body: &[u8], limit: usize) -> Result<Self> {
        let message: Self = decode(body, limit)?;
        message.validate()?;
        Ok(message)
    }

    /// Validates protocol identity and the optional opaque reference.
    pub fn validate(&self) -> Result<()> {
        check_identity(&self.schema_version, self.invocation_id, self.invocation_id)?;
        check_reference(self.provider_reference.as_deref())
    }
}

fn check_reference(reference: Option<&str>) -> Result<()> {
    ensure!(
        reference.is_none_or(|value| !value.is_empty() && value.len() <= MAX_REFERENCE_BYTES),
        "invalid provider reference"
    );
    Ok(())
}

fn check_identity(version: &str, reported: Uuid, expected: Uuid) -> Result<()> {
    ensure!(
        version == SCHEMA_VERSION,
        "unsupported invocation schema version"
    );
    ensure!(
        !reported.is_nil() && reported == expected,
        "invocation identity mismatch"
    );
    Ok(())
}

fn parse(body: &[u8], limit: usize) -> Result<serde_json::Value> {
    ensure!(body.len() <= limit, "invocation message exceeds byte limit");
    strict_json(body).map_err(|_| anyhow::anyhow!("invalid invocation JSON"))
}

fn from_value<T: DeserializeOwned>(value: serde_json::Value) -> Result<T> {
    // JSON fields can include callback credentials or arbitrary remote text.
    serde_json::from_value(value)
        .map_err(|_| anyhow::anyhow!("invalid invocation message shape or payload"))
}

fn decode<T: DeserializeOwned>(body: &[u8], limit: usize) -> Result<T> {
    from_value(parse(body, limit)?)
}
