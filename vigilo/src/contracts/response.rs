//! Explicit agent response configuration and evaluator evidence contracts.
//!
//! Configuration describes interpretation, not execution: selectors never guess
//! meaning from field names. Schema identities are independent of the application
//! release. Ordered evidence preserves message boundaries and tool relationships;
//! absent evidence differs from an explicitly empty sequence. Resource descriptors
//! identify invocation-scoped host data, never storage credentials or remote URLs.
//! Parsing these contracts does not execute mappings, fetch content, or authorize
//! resource access. The host owns those operations and their budgets.

#[path = "response/configuration.rs"]
mod configuration;
pub use configuration::*;
use serde::{
    Deserialize,
    Serialize,
};

/// Exact supported evidence schema identity; unknown versions fail deserialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvidenceVersion {
    #[serde(rename = "1.0.0")]
    V1,
}

/// Evidence requirements declared by an evaluator, not inferred from its code.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceRequirements {
    pub original_body: bool,
    pub mapped_items: bool,
    pub content_media_types: Vec<String>,
    pub tool_calls: bool,
    pub tool_results: bool,
}

/// Ordered mapped output; runtime validation enforces the producer's budgets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResponseEvidence {
    pub schema_version: EvidenceVersion,
    pub body: BodyMetadata,
    pub items: Option<Vec<OutputItem>>,
    pub status: Option<CompletionStatus>,
    pub stop_reason: Option<StopReason>,
}

/// Descriptor for original bytes after decompression, before interpretation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BodyMetadata {
    pub resource: ResourceHandle,
    pub byte_length: u64,
    pub content_type: Option<String>,
}

/// Opaque invocation-scoped identity. A numeric value alone grants no access.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ResourceHandle(pub u64);

/// One observed item. Vector order is semantic; IDs never determine chronology.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum OutputItem {
    Message {
        id: Option<String>,
        role: Option<String>,
        content: Vec<ContentPart>,
    },
    ToolCall {
        call_id: Option<String>,
        name: String,
        arguments: ContentPart,
    },
    ToolResult {
        call_id: Option<String>,
        content: Vec<ContentPart>,
        is_error: Option<bool>,
    },
}

/// Explicit content interpretation; media type is not guessed from data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentPart {
    pub media_type: String,
    pub source: ContentSource,
}

/// Resource bounds and ownership are checked by the host on every read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContentSource {
    InlineText {
        text: String,
    },
    BodyRange {
        offset: u64,
        byte_length: u64,
    },
    Resource {
        handle: ResourceHandle,
        byte_length: u64,
    },
}

/// Agent-reported completion, independent of transport or evaluator success.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionStatus {
    Completed,
    Incomplete,
    Failed,
}

/// Explicitly mapped reason; a refusal is not itself an evaluation failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopReason {
    pub category: StopCategory,
    pub provider_value: Option<String>,
}

/// Stable vocabulary for generation termination, not instructions to run tools.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopCategory {
    EndTurn,
    ToolCall,
    Limit,
    Refusal,
    Other,
}

/// Requested read extent. EOF is an empty successful read; offset beyond EOF is
/// out of bounds. Successful reads return at most `max_bytes`, with no cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceRead {
    pub handle: ResourceHandle,
    pub offset: u64,
    pub max_bytes: u32,
}

/// Stable resource-access failures; errors must not expose storage internals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceReadError {
    Unavailable,
    InvalidHandle,
    OutOfBounds,
    LimitExceeded,
}

#[cfg(test)]
#[path = "response/tests.rs"]
mod tests;
