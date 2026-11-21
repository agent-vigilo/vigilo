//! Curated export records, independent of persistence and interface formatting.
//!
//! Ordinary metadata excludes free-form content and infrastructure identity.
//! Sensitive records preserve evidence without truncation. Parent references join
//! independently paginated collections; they do not convey authorization.
use chrono::{
    DateTime,
    Utc,
};
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::Value;
use uuid::Uuid;

/// Explicit ExecutionMetadata projection; access is checked for every page.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionMetadata {
    pub id: Uuid,
    pub run_id: Uuid,
    pub run_shard: i16,
    pub chunk_id: Uuid,
    pub case_id: Uuid,
    pub task_type: String,
    pub evaluation_profile_id: String,
    pub evaluation_profile_version: String,
    pub expected_evaluator_count: i32,
    pub status: String,
    pub current_attempt_no: i32,
    pub current_attempt_id: Option<Uuid>,
    pub retry_after: Option<DateTime<Utc>>,
    pub retry_count: i32,
    pub last_attempt_completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

/// Explicit AttemptMetadata projection; access is checked for every page.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptMetadata {
    pub id: Uuid,
    pub run_id: Uuid,
    pub run_shard: i16,
    pub execution_id: Uuid,
    pub attempt_no: i32,
    pub status: String,
    pub agent_latency_ms: Option<i64>,
    pub evaluator_latency_ms: Option<i64>,
    pub total_latency_ms: Option<i64>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub evidence_state: String,
}

/// Explicit AggregateMetadata projection; access is checked for every page.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateMetadata {
    pub run_id: Uuid,
    pub run_shard: i16,
    pub execution_id: Uuid,
    pub attempt_id: Uuid,
    pub overall_status: String,
    pub aggregate_score: Option<f64>,
    pub evaluator_result_count: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub authoritative: bool,
}

/// Explicit EvaluatorResultMetadata projection; access is checked for every page.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluatorResultMetadata {
    pub id: Uuid,
    pub run_id: Uuid,
    pub run_shard: i16,
    pub execution_id: Uuid,
    pub attempt_id: Uuid,
    pub binding_id: String,
    pub evaluator_id: Uuid,
    pub evaluator_version: String,
    pub evaluator_profile_id: String,
    pub evaluator_profile_version: String,
    pub evaluator_interface_version: Option<String>,
    pub evaluator_runtime_version: Option<String>,
    pub dimension: String,
    pub outcome: String,
    pub judgment: Option<String>,
    pub blocking: bool,
    pub measurement_kind: Option<String>,
    pub raw_boolean: Option<bool>,
    pub raw_numeric: Option<f64>,
    pub normalized_score: Option<f64>,
    pub normalization_policy_hash: String,
    pub pass_threshold: f64,
    pub weight: f64,
    pub created_at: DateTime<Utc>,
    pub evidence_state: String,
}

/// Explicit ExecutionContent projection; access is checked for every page.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionContent {
    pub id: Uuid,
    pub run_id: Uuid,
    pub run_shard: i16,
    pub tags: Value,
    pub input_payload: Value,
    pub expected_output: Value,
    pub case_metadata: Value,
    pub last_error_message: Option<String>,
}

/// Explicit AttemptContent projection; access is checked for every page.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptContent {
    pub id: Uuid,
    pub run_id: Uuid,
    pub run_shard: i16,
    pub execution_id: Uuid,
    pub token_usage: Value,
    pub outcome_summary: Value,
    pub error_message: Option<String>,
}

/// Explicit AggregateContent projection; access is checked for every page.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateContent {
    pub run_id: Uuid,
    pub run_shard: i16,
    pub execution_id: Uuid,
    pub attempt_id: Uuid,
    pub dimension_scores: Value,
    pub blocking_failures: Value,
    pub summary: Value,
}

/// Explicit EvaluatorResultContent projection; access is checked for every page.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluatorResultContent {
    pub id: Uuid,
    pub run_id: Uuid,
    pub run_shard: i16,
    pub execution_id: Uuid,
    pub attempt_id: Uuid,
    pub raw_ordinal: Option<String>,
    pub raw_unit: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub abstention_category: Option<String>,
    pub abstention_reason: Option<String>,
    pub raw_evaluator_output: Value,
}

/// Explicit ExportDiagnostic projection; access is checked for every page.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExportDiagnostic {
    pub id: Uuid,
    pub run_id: Uuid,
    pub run_shard: i16,
    pub evaluator_result_id: Uuid,
    pub diagnostic_index: i32,
    pub severity: String,
    pub category: String,
    pub reason: Option<String>,
    pub evidence: Value,
    pub tags: Vec<String>,
    pub created_at: DateTime<Utc>,
}

/// One member of a homogeneous bounded export page.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum ExportRecord {
    ExecutionMetadata(ExecutionMetadata),
    AttemptMetadata(AttemptMetadata),
    AggregateMetadata(AggregateMetadata),
    EvaluatorResultMetadata(EvaluatorResultMetadata),
    ExecutionContent(ExecutionContent),
    AttemptContent(AttemptContent),
    AggregateContent(AggregateContent),
    EvaluatorResultContent(EvaluatorResultContent),
    ExportDiagnostic(ExportDiagnostic),
}
