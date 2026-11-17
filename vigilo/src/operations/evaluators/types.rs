//! Public registry values and sealed identifiers; no persistence or CLI types.
use chrono::{
    DateTime,
    Utc,
};
use serde::{
    Deserialize,
    Serialize,
};
use uuid::Uuid;

use super::super::Error;
pub use crate::contracts::evaluator::{
    Abstention,
    DiagnosticFinding,
    EvaluatorIdentity,
    EvaluatorOutcome,
    EvaluatorOutput,
    Measurement,
    Severity,
};

/// Canonical registry address, independent of a database row UUID.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvaluatorId {
    pub(super) namespace: String,
    pub(super) name: String,
    pub(super) version: String,
}
impl EvaluatorId {
    pub fn parse(value: &str) -> Result<Self, Error> {
        if value.len() > 4096 {
            return Err(Error::InvalidInput("evaluator identifier exceeds limit"));
        }
        let id = crate::contracts::evaluator_ref::parse_fully_qualified_evaluator(value)
            .map_err(|_| Error::InvalidInput("expected <namespace>/<name>:<version>"))?;
        Ok(Self {
            namespace: id.namespace,
            name: id.name,
            version: id.version,
        })
    }
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn version(&self) -> &str {
        &self.version
    }
}
impl std::fmt::Display for EvaluatorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}:{}", self.namespace, self.name, self.version)
    }
}
/// Lifecycle state is separate from artifact identity and access policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum State {
    Active,
    Yanked,
    Deprecated,
    Disabled,
    Removed,
}
impl From<crate::models::evaluator::EvaluatorState> for State {
    fn from(s: crate::models::evaluator::EvaluatorState) -> Self {
        use crate::models::evaluator::EvaluatorState as S;
        match s {
            S::Active => Self::Active,
            S::Yanked => Self::Yanked,
            S::Deprecated => Self::Deprecated,
            S::Disabled => Self::Disabled,
            S::Removed => Self::Removed,
        }
    }
}
impl From<State> for crate::models::evaluator::EvaluatorState {
    fn from(s: State) -> Self {
        match s {
            State::Active => Self::Active,
            State::Yanked => Self::Yanked,
            State::Deprecated => Self::Deprecated,
            State::Disabled => Self::Disabled,
            State::Removed => Self::Removed,
        }
    }
}
/// Verified registry metadata; artifact bytes are loaded only for execution.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Descriptor {
    pub id: Uuid,
    pub namespace: String,
    pub name: String,
    pub version: String,
    pub content_hash: String,
    pub wasm_size_bytes: i64,
    pub interface_name: String,
    pub interface_version: String,
    pub wit_world: String,
    pub abi_contract_hash: String,
    pub abi_adapter: String,
    pub runtime: String,
    pub runtime_version: String,
    pub runtime_fingerprint: String,
    pub description: Option<String>,
    pub tags: serde_json::Value,
    pub metadata: serde_json::Value,
    pub state: State,
    pub state_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
impl From<crate::models::evaluator::Evaluator> for Descriptor {
    fn from(e: crate::models::evaluator::Evaluator) -> Self {
        Self {
            id: e.id,
            namespace: e.namespace,
            name: e.name,
            version: e.version,
            content_hash: e.content_hash,
            wasm_size_bytes: e.wasm_size_bytes,
            interface_name: e.interface_name,
            interface_version: e.interface_version,
            wit_world: e.wit_world,
            abi_contract_hash: e.abi_contract_hash,
            abi_adapter: e.abi_adapter,
            runtime: e.runtime,
            runtime_version: e.runtime_version,
            runtime_fingerprint: e.runtime_fingerprint,
            description: e.description,
            tags: e.tags,
            metadata: e.metadata,
            state: e.state.into(),
            state_reason: e.state_reason,
            created_at: e.created_at,
            updated_at: e.updated_at,
        }
    }
}

/// Publication preserves exact bytes; replay does not reactivate a withdrawn version.
#[derive(Clone, Debug, Serialize)]
pub struct Publication {
    pub evaluator: Descriptor,
    pub reused: bool,
}
/// Native evaluator output; normalization, aggregation and gates are not applied.
#[derive(Clone, Debug, Serialize)]
pub struct TestResult {
    pub namespace: String,
    pub name: String,
    pub version: String,
    pub state: State,
    pub output: EvaluatorOutput,
}
/// A bounded discovery page; continue under current caller authority.
#[derive(Clone, Debug, Serialize)]
pub struct Page {
    pub items: Vec<Descriptor>,
    pub next: Option<Cursor>,
}
/// Opaque logical position, never an authorization grant or a database handle.
#[derive(Clone, Debug, Serialize)]
#[serde(transparent)]
pub struct Cursor(pub(super) String);
impl Cursor {
    /// Validate an encoded logical position before accepting it from an interface.
    pub fn parse(value: &str) -> Result<Self, Error> {
        if value.len() > 16 * 1024 {
            return Err(Error::InvalidInput("evaluator cursor exceeds limit"));
        }
        Self::from_owned(value.into())
    }
    fn from_owned(value: String) -> Result<Self, Error> {
        let value = Self(value);
        value.parts()?;
        Ok(value)
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    fn parts(&self) -> Result<(String, String, String), Error> {
        if self.0.len() > 16 * 1024 {
            return Err(Error::InvalidInput("evaluator cursor exceeds limit"));
        }
        let value: (String, String, String) = serde_json::from_str(&self.0)
            .map_err(|_| Error::InvalidInput("invalid evaluator cursor"))?;
        if value.0.len() != 64
            || !value.0.bytes().all(|b| b.is_ascii_hexdigit())
            || value.1.is_empty()
            || value.1.len() > 4096
            || value.2.is_empty()
            || value.2.len() > 4096
        {
            return Err(Error::InvalidInput("invalid evaluator cursor"));
        }
        Ok(value)
    }
    pub(super) fn position(
        &self,
        namespace: &str,
        query: Option<&str>,
    ) -> Result<(String, String), Error> {
        let (bound, name, version) = self.parts()?;
        if bound != filter_hash(namespace, query) {
            return Err(Error::InvalidInput("evaluator cursor filter changed"));
        }
        Ok((name, version))
    }
}
impl<'de> Deserialize<'de> for Cursor {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::from_owned(value).map_err(serde::de::Error::custom)
    }
}
pub(super) fn filter_hash(namespace: &str, query: Option<&str>) -> String {
    let query = query.map(str::trim).filter(|s| !s.is_empty());
    blake3::hash(&serde_json::to_vec(&(namespace, query)).expect("strings serialize"))
        .to_hex()
        .to_string()
}
/// Limits apply independently to rows and encoded descriptor bytes.
#[derive(Clone, Copy, Debug)]
pub struct PageLimits {
    pub(super) rows: u32,
    pub(super) bytes: u32,
}
impl Default for PageLimits {
    fn default() -> Self {
        Self {
            rows: 10,
            bytes: 8 * 1024 * 1024,
        }
    }
}
impl PageLimits {
    pub fn new(rows: u32, bytes: u32) -> Result<Self, Error> {
        if !(1..=20).contains(&rows) || !(1024..=8 * 1024 * 1024).contains(&bytes) {
            return Err(Error::InvalidInput("invalid evaluator page limits"));
        }
        Ok(Self { rows, bytes })
    }
}
