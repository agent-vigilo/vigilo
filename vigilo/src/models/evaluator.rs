//! Evaluator catalog persistence models.
//!
//! Evaluators are versioned WebAssembly components published into the registry.
//! These models describe catalog rows and update payloads; runtime execution
//! inputs and outputs stay in `contracts`.

use chrono::{
    DateTime,
    Utc,
};
use serde::{
    Deserialize,
    Serialize,
};
use uuid::Uuid;

/// Lifecycle state for a published evaluator version.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type, PartialEq, Eq, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
#[sqlx(type_name = "evaluator_state", rename_all = "lowercase")]
pub(crate) enum EvaluatorState {
    /// Available for new evaluation profiles and execution.
    Active,
    /// Withdrawn after publication, usually due to a known issue.
    Yanked,
    /// Still loadable but superseded by a newer evaluator.
    Deprecated,
    /// Temporarily unavailable for execution.
    Disabled,
    /// Removed from normal discovery and use.
    Removed,
}

impl std::str::FromStr for EvaluatorState {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "active" => Ok(Self::Active),
            "yanked" => Ok(Self::Yanked),
            "deprecated" => Ok(Self::Deprecated),
            "disabled" => Ok(Self::Disabled),
            "removed" => Ok(Self::Removed),
            _ => anyhow::bail!(
                "invalid evaluator state '{}'; expected one of: active, yanked, deprecated, disabled, removed",
                s
            ),
        }
    }
}

/// Insert payload for a published evaluator version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EvaluatorDraft {
    /// Registry namespace that owns the evaluator.
    pub(crate) namespace: String,
    /// Evaluator name within the namespace.
    pub(crate) name: String,
    /// Semver or project-defined version string.
    pub(crate) version: String,
    /// Hash of the evaluator content used for integrity and dedupe checks.
    pub(crate) content_hash: String,
    /// Serialized WebAssembly component bytes.
    pub(crate) wasm_bytes: Vec<u8>,
    /// Verified WIT interface name implemented by the component.
    pub(crate) interface_name: String,
    /// Verified WIT interface version implemented by the component.
    pub(crate) interface_version: String,
    /// Verified WIT world used to instantiate the component.
    pub(crate) wit_world: String,
    /// Hash of the immutable WIT contract verified by the host.
    pub(crate) abi_contract_hash: String,
    /// Versioned host adapter selected for this contract.
    pub(crate) abi_adapter: String,
    /// Runtime family used to execute the component.
    pub(crate) runtime: String,
    /// Runtime version used when preparing the component.
    pub(crate) runtime_version: String,
    /// Fingerprint of runtime settings that affect component compatibility.
    pub(crate) runtime_fingerprint: String,
    /// Human-readable evaluator description.
    pub(crate) description: Option<String>,
    /// JSON tag array or object used for discovery.
    pub(crate) tags: serde_json::Value,
    /// Additional registry metadata carried without schema-specific columns.
    pub(crate) metadata: serde_json::Value,
}

/// Mutable evaluator lifecycle fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EvaluatorPatch {
    /// New evaluator lifecycle state.
    pub(crate) state: EvaluatorState,
    /// Optional operator-supplied reason for the state change.
    pub(crate) state_reason: Option<String>,
}

/// Persisted evaluator catalog row.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub(crate) struct Evaluator {
    /// Database id for this evaluator version.
    pub(crate) id: Uuid,
    /// Registry namespace that owns the evaluator.
    pub(crate) namespace: String,
    /// Evaluator name within the namespace.
    pub(crate) name: String,
    /// Semver or project-defined version string.
    pub(crate) version: String,
    /// Hash of the evaluator content used for integrity and dedupe checks.
    pub(crate) content_hash: String,
    /// Serialized WebAssembly component bytes.
    pub(crate) wasm_bytes: Vec<u8>,
    /// Size of `wasm_bytes` recorded for listing and audit queries.
    pub(crate) wasm_size_bytes: i64,
    /// Verified WIT interface name implemented by the component.
    pub(crate) interface_name: String,
    /// Verified WIT interface version implemented by the component.
    pub(crate) interface_version: String,
    /// Verified WIT world used to instantiate the component.
    pub(crate) wit_world: String,
    /// Hash of the immutable WIT contract verified by the host.
    pub(crate) abi_contract_hash: String,
    /// Versioned host adapter selected for this contract.
    pub(crate) abi_adapter: String,
    /// Runtime family used to execute the component.
    pub(crate) runtime: String,
    /// Runtime version used when preparing the component.
    pub(crate) runtime_version: String,
    /// Fingerprint of runtime settings that affect component compatibility.
    pub(crate) runtime_fingerprint: String,
    /// Human-readable evaluator description.
    pub(crate) description: Option<String>,
    /// JSON tag array or object used for discovery.
    pub(crate) tags: serde_json::Value,
    /// Additional registry metadata carried without schema-specific columns.
    pub(crate) metadata: serde_json::Value,
    /// Current evaluator lifecycle state.
    pub(crate) state: EvaluatorState,
    /// Optional operator-supplied reason for the current state.
    pub(crate) state_reason: Option<String>,
    /// Time this evaluator version was inserted.
    pub(crate) created_at: DateTime<Utc>,
    /// Time this evaluator row was last updated.
    pub(crate) updated_at: DateTime<Utc>,
}
