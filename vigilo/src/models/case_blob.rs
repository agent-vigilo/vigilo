//! Test-only normalized case fixtures for deterministic identity checks.
//!
//! These logical fields let golden tests independently reproduce case hashes;
//! application payloads are encoded as immutable case records in object storage.

use serde::{
    Deserialize,
    Serialize,
};

/// Normalized case payload used by identity fixtures.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CaseBlobDraft {
    /// Stable hash of the normalized case content.
    pub(crate) case_hash: String,
    /// Task type used to match the case to compatible evaluator groups.
    pub(crate) task_type: String,
    /// Optional explicit profile case-group routing override.
    pub(crate) case_group: Option<String>,
    /// Model input payload provided to the agent under evaluation.
    pub(crate) input_payload: serde_json::Value,
    /// Expected output or reference answer used by evaluators.
    pub(crate) expected_output: serde_json::Value,
    /// Supplemental context supplied with the case.
    pub(crate) context_payload: serde_json::Value,
    /// Case tags stored as JSON for flexible filtering.
    pub(crate) tags: serde_json::Value,
    /// Additional case metadata stored without schema-specific columns.
    pub(crate) metadata: serde_json::Value,
}
