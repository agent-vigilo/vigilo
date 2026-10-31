//! Contracts used at the run-configuration and dataset input boundary.
//!
//! These types define the canonical profile and dataset payloads consumed by
//! `vigilo run validate`. They are intentionally transport-focused contracts used
//! for parsing and validation before orchestration/runtime execution logic.

use std::collections::BTreeMap;

use serde::{
    Deserialize,
    Serialize,
};
use serde_json::Value;
use uuid::Uuid;

fn default_json_object() -> Value {
    Value::Object(Default::default())
}

fn default_post_method() -> String {
    "POST".to_string()
}

fn default_true() -> bool {
    true
}

/// Run profile used by `vigilo run validate`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RunProfile {
    /// Stable profile identifier (for example, `mixed_agent_release`).
    ///
    /// This is intended to be human-meaningful and stable across revisions.
    pub(crate) profile_id: String,

    /// Profile document version (independent from evaluator versions).
    ///
    /// Allows evolving profile behavior/configuration over time.
    pub(crate) profile_version: String,

    /// Human-readable summary of profile purpose and scope.
    pub(crate) description: String,

    /// Default runtime policy values applied during run orchestration.
    pub(crate) defaults: RunDefaults,

    /// Persistence behavior controls for run/evaluation artifacts.
    pub(crate) persistence: PersistenceSettings,

    /// Run-level scorecard and gate policy applied after every shard completes.
    #[serde(default)]
    pub(crate) scorecard: ScorecardSettings,

    /// Agent target invoked by workers before evaluator execution.
    pub(crate) agent: AgentProfile,

    /// Case-group specific evaluator bindings and aggregation policies.
    ///
    /// Empty by default to allow incremental authoring/validation.
    #[serde(default)]
    pub(crate) case_groups: Vec<CaseGroupProfile>,
}

/// Run-level rules evaluated from bounded shard-local rollups.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScorecardSettings {
    /// Gates evaluated after shard scorecards are merged.
    #[serde(default)]
    pub(crate) gates: Vec<ScorecardGate>,
}

/// One run-level gate over a dimension across all matching executions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScorecardGate {
    /// Stable gate identifier within the profile.
    pub(crate) id: String,
    /// Dimension to evaluate.
    pub(crate) dimension: String,
    /// Optional evaluator binding target within the dimension.
    #[serde(default)]
    pub(crate) binding_id: Option<String>,
    /// Optional exact case-group selector.
    #[serde(default)]
    pub(crate) case_group: Option<String>,
    /// Optional tag selector; a case matches when it has every configured tag.
    #[serde(default)]
    pub(crate) tags_all: Vec<String>,
    /// Minimum mean normalized score across scored matching executions.
    #[serde(default)]
    pub(crate) min_mean_score: Option<f64>,
    /// Score at or above which a matching execution counts as passing.
    #[serde(default)]
    pub(crate) score_threshold: Option<f64>,
    /// Minimum fraction of scored matching executions meeting `score_threshold`.
    #[serde(default)]
    pub(crate) min_pass_rate: Option<f64>,
    /// Minimum scored-execution coverage over expected matching executions.
    #[serde(default)]
    pub(crate) min_coverage: Option<f64>,
    /// Maximum execution error rate over expected matching executions.
    #[serde(default)]
    pub(crate) max_error_rate: Option<f64>,
    /// Maximum execution abstention rate over expected matching executions.
    #[serde(default)]
    pub(crate) max_abstention_rate: Option<f64>,
}

/// Agent target configuration attached to this run profile.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AgentProfile {
    /// Provider/platform for the evaluated target.
    pub(crate) provider: String,

    /// Logical agent or model name under evaluation.
    pub(crate) name: String,

    /// Optional version/deployment identifier for the evaluated target.
    #[serde(default)]
    pub(crate) version: Option<String>,

    /// Optional provider-specific model identifier used by the target.
    #[serde(default)]
    pub(crate) model: Option<String>,

    /// Prompt/config identity associated with the agent call.
    #[serde(default)]
    pub(crate) prompt_config_id: Option<String>,

    /// Prompt/config version associated with the agent call.
    #[serde(default)]
    pub(crate) prompt_config_version: Option<String>,

    /// HTTP invocation configuration used by workers.
    pub(crate) http: AgentHttpConfig,

    /// Agent-specific unstructured configuration passed in invocation metadata.
    #[serde(default = "default_json_object")]
    pub(crate) config: Value,
}

/// HTTP settings workers use to invoke the configured agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AgentHttpConfig {
    /// Absolute URI for the agent invocation endpoint.
    pub(crate) url: String,

    /// HTTP method used for invocation. Defaults to `POST`.
    #[serde(default = "default_post_method")]
    pub(crate) method: String,

    /// Opaque name resolved through trusted worker credential configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) credential: Option<String>,

    /// Static nonsecret headers to attach to every invocation.
    #[serde(default)]
    pub(crate) headers: BTreeMap<String, String>,

    /// Optional per-request timeout override. Falls back to profile defaults.
    #[serde(default)]
    pub(crate) timeout_secs: Option<u32>,

    /// Optional serialized request-body limit, bounded by the worker ceiling.
    #[serde(default)]
    pub(crate) max_request_bytes: Option<u64>,

    /// Optional response-body limit, bounded by the worker ceiling.
    #[serde(default)]
    pub(crate) max_response_bytes: Option<u64>,

    /// Maximum accepted Retry-After delay in seconds (default 600, maximum 86400).
    #[serde(default)]
    pub(crate) max_retry_after_secs: Option<u32>,
}

/// Default execution policy for run processing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RunDefaults {
    /// Maximum number of attempts per execution before terminal failure.
    pub(crate) max_attempts: u32,

    /// Request timeout budget, in seconds, for invocation/evaluation phases.
    pub(crate) request_timeout_secs: u32,

    /// Whether any blocking evaluator failure should fail the full execution.
    pub(crate) fail_on_any_blocking_failure: bool,

    /// Minimum aggregate execution score required for passing policy.
    pub(crate) min_execution_score: f64,
}

/// Persistence policy controls for run output and evidence retention.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PersistenceSettings {
    /// High-level persistence strategy (`full` vs `summary`).
    pub(crate) mode: PersistenceMode,

    /// Raw output retention policy (`all`, `failures_only`, `none`).
    pub(crate) persist_raw_outputs: PersistRawOutputsMode,

    /// Whether evaluator evidence blobs should be retained.
    pub(crate) persist_evaluator_evidence: bool,
}

/// Persistence breadth mode for run artifacts.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PersistenceMode {
    /// Persist complete run/evaluator detail.
    Full,

    /// Persist reduced summary-level data only.
    Summary,
}

/// Raw-output retention strategy.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PersistRawOutputsMode {
    /// Persist raw outputs for every case/execution.
    All,

    /// Persist raw outputs only for failed/blocking outcomes.
    FailuresOnly,

    /// Do not persist raw outputs.
    None,
}

/// Profile block that binds case selection, evaluators, and aggregation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CaseGroupProfile {
    /// Stable identifier for this case group.
    pub(crate) id: String,

    /// Human-readable case-group description.
    pub(crate) description: String,

    /// Case-matching selector for applying this group.
    pub(crate) applies_to: AppliesTo,

    /// Evaluator bindings used when a case matches `applies_to`.
    #[serde(default)]
    pub(crate) evaluators: Vec<EvaluatorBinding>,

    /// Dimension-level aggregation strategy for this group.
    pub(crate) aggregation: AggregationSettings,
}

/// Selector used to determine which dataset cases this profile group applies to.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AppliesTo {
    /// Primary task type discriminator (for example, `classification`).
    pub(crate) task_type: String,

    /// Optional tag OR-filter; at least one tag should match when provided.
    #[serde(default)]
    pub(crate) tags_any: Vec<String>,

    /// Optional tag AND-filter; all listed tags should match when provided.
    #[serde(default)]
    pub(crate) tags_all: Vec<String>,
}

/// Evaluator binding configuration for one case-group entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EvaluatorBinding {
    /// Stable identifier for this binding within the profile.
    pub(crate) id: String,

    /// Evaluator reference string as declared in profile payload.
    ///
    /// This is currently treated as an opaque identifier at parse time.
    #[serde(rename = "ref")]
    pub(crate) evaluator_ref: String,

    /// Whether this evaluator must produce a valid score for aggregation.
    /// Optional bindings are diagnostic-only and cannot affect score or policy.
    #[serde(default = "default_true")]
    pub(crate) required: bool,

    /// Aggregation/reporting dimension this evaluator contributes to.
    pub(crate) dimension: String,

    /// Whether this evaluator can act as a hard gate.
    pub(crate) blocking: bool,

    /// Relative weighting for this evaluator within its dimension.
    pub(crate) weight: f64,

    /// Host-owned interpretation of the evaluator's measurement type.
    pub(crate) normalization: NormalizationPolicy,

    /// Minimum normalized score that yields a host `passed` judgment.
    pub(crate) pass_threshold: f64,

    /// Evaluator-specific unstructured configuration payload.
    #[serde(default = "default_json_object")]
    pub(crate) config: Value,
}

/// Host-owned conversion from an evaluator measurement to a normalized score.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum NormalizationPolicy {
    /// Maps both possible binary observations explicitly.
    Binary { false_score: f64, true_score: f64 },
    /// Maps a raw numeric observation through a declarative utility function.
    Numeric {
        #[serde(default)]
        unit: Option<String>,
        mapping: NumericMapping,
    },
    /// Maps evaluator-provided labels to explicit utility values.
    Ordinal { values: BTreeMap<String, f64> },
}

/// Host-owned mapping from a numeric measurement to zero-through-one utility.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum NumericMapping {
    /// Linearly maps a fixed raw domain in the configured direction.
    Linear {
        min: f64,
        max: f64,
        direction: ScoreDirection,
    },
    /// Linearly interpolates between ordered raw-value utility points.
    PiecewiseLinear { points: Vec<UtilityPoint> },
    /// Assigns one score to each interval formed by ordered cut points.
    Thresholds {
        min: f64,
        max: f64,
        cutpoints: Vec<f64>,
        scores: Vec<f64>,
    },
}

/// One raw-value to normalized-score point in a piecewise-linear mapping.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UtilityPoint {
    pub(crate) value: f64,
    pub(crate) score: f64,
}

/// Direction used by linear numeric normalization.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ScoreDirection {
    HigherIsBetter,
    LowerIsBetter,
}

/// Aggregation policy for a case-group, keyed by dimension name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AggregationSettings {
    /// Dimension aggregation rules (`format`, `correctness`, `safety`, etc.).
    #[serde(default)]
    pub(crate) dimensions: BTreeMap<String, DimensionAggregation>,
}

/// Aggregation strategy for one dimension.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct DimensionAggregation {
    /// Scoring method used to combine evaluator outputs.
    pub(crate) method: AggregationMethod,

    /// Whether this dimension can fail execution/run gating.
    pub(crate) blocking: bool,

    /// Relative dimension contribution to overall score.
    pub(crate) weight: f64,
}

/// Supported dimension aggregation methods.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AggregationMethod {
    /// Use the minimum score among contributors.
    MinScore,

    /// Use a weighted arithmetic mean.
    WeightedMean,
}

/// Dataset envelope used by `vigilo run validate`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RunDataset {
    /// Stable dataset identifier.
    pub(crate) dataset_id: Uuid,

    /// Optional dataset version string.
    #[serde(default)]
    pub(crate) dataset_version: Option<String>,

    /// Dataset cases included in this payload.
    #[serde(default)]
    pub(crate) cases: Vec<DatasetCase>,
}

/// Single dataset case consumed by run/profile matching.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DatasetCase {
    /// Stable case identifier in dataset.
    pub(crate) id: Uuid,

    /// Task type used for profile `applies_to` matching.
    pub(crate) task_type: String,

    /// Optional pre-assigned case group label.
    #[serde(default)]
    pub(crate) case_group: Option<String>,

    /// Input payload sent to target system/agent.
    pub(crate) input: Value,

    /// Optional expected/oracle payload used by evaluators.
    #[serde(default)]
    pub(crate) expected: Option<Value>,

    /// Optional supporting context payload.
    #[serde(default)]
    pub(crate) context: Option<Value>,

    /// Optional tags for filtering/routing/reporting.
    #[serde(default)]
    pub(crate) tags: Vec<String>,

    /// Optional arbitrary bookkeeping metadata.
    #[serde(default)]
    pub(crate) metadata: BTreeMap<String, Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_profile_draft_yaml() {
        let raw = r#"
profile_id: mixed_agent_release
profile_version: 1.0.0
description: Release-grade evaluation profile for mixed generative AI agent tasks.
defaults:
  max_attempts: 2
  request_timeout_secs: 60
  fail_on_any_blocking_failure: true
  min_execution_score: 0.85
persistence:
  mode: full
  persist_raw_outputs: failures_only
  persist_evaluator_evidence: true
agent:
  provider: example
  name: sentiment-demo-agent
  version: 1.0.0
  model: demo-classifier-v1
  prompt_config_id: sentiment-demo
  prompt_config_version: 1.0.0
  http:
    url: http://127.0.0.1:8787/v1/agent/invoke
    headers:
      x-vigilo-example: sentiment
case_groups:
  - id: classification
    description: Evaluates classification-style cases.
    applies_to:
      task_type: classification
    evaluators:
      - id: json_schema
        ref: core/json-schema:1.0.0
        dimension: format
        blocking: true
        weight: 1.0
        normalization:
          method: binary
          false_score: 0.0
          true_score: 1.0
        pass_threshold: 1.0
        config:
          schema:
            type: object
    aggregation:
      dimensions:
        format:
          method: min_score
          blocking: true
          weight: 0.0
"#;

        let profile: RunProfile = serde_yaml::from_str(raw).unwrap();
        assert_eq!(profile.profile_id, "mixed_agent_release");
        assert_eq!(profile.agent.provider, "example");
        assert_eq!(profile.agent.name, "sentiment-demo-agent");
        assert_eq!(profile.agent.model.as_deref(), Some("demo-classifier-v1"));
        assert_eq!(profile.agent.http.method, "POST");
        assert_eq!(profile.case_groups.len(), 1);
        assert_eq!(
            profile.case_groups[0].evaluators[0].evaluator_ref,
            "core/json-schema:1.0.0"
        );
        assert!(profile.case_groups[0].evaluators[0].required);
    }

    #[test]
    fn parse_every_normalization_policy_shape() {
        let raw = r#"
- method: binary
  false_score: 0.0
  true_score: 1.0
- method: numeric
  unit: milliseconds
  mapping:
    type: linear
    min: 0.0
    max: 1000.0
    direction: lower_is_better
- method: numeric
  mapping:
    type: piecewise_linear
    points:
      - { value: 0.0, score: 0.0 }
      - { value: 10.0, score: 1.0 }
- method: numeric
  mapping:
    type: thresholds
    min: 0.0
    max: 100.0
    cutpoints: [50.0, 80.0]
    scores: [0.0, 0.5, 1.0]
- method: ordinal
  values:
    preferred: 1.0
    tie: 0.4
    not_preferred: 0.0
"#;

        let policies: Vec<NormalizationPolicy> = serde_yaml::from_str(raw).unwrap();

        assert_eq!(policies.len(), 5);
        for policy in policies {
            crate::contracts::normalization::validate_policy(&policy).unwrap();
        }
    }

    #[test]
    fn parse_profile_requires_agent() {
        let raw = r#"
profile_id: mixed_agent_release
profile_version: 1.0.0
description: Release-grade evaluation profile for mixed generative AI agent tasks.
defaults:
  max_attempts: 2
  request_timeout_secs: 60
  fail_on_any_blocking_failure: true
  min_execution_score: 0.85
persistence:
  mode: full
  persist_raw_outputs: failures_only
  persist_evaluator_evidence: true
case_groups: []
"#;

        let err = serde_yaml::from_str::<RunProfile>(raw).unwrap_err();
        assert!(err.to_string().contains("missing field `agent`"));
    }

    #[test]
    fn parse_profile_requires_agent_http() {
        let raw = r#"
profile_id: mixed_agent_release
profile_version: 1.0.0
description: Release-grade evaluation profile for mixed generative AI agent tasks.
defaults:
  max_attempts: 2
  request_timeout_secs: 60
  fail_on_any_blocking_failure: true
  min_execution_score: 0.85
persistence:
  mode: full
  persist_raw_outputs: failures_only
  persist_evaluator_evidence: true
agent:
  provider: example
  name: sentiment-demo-agent
case_groups: []
"#;

        let err = serde_yaml::from_str::<RunProfile>(raw).unwrap_err();
        assert!(err.to_string().contains("missing field `http`"));
    }

    #[test]
    fn parse_dataset_yaml() {
        let raw = r#"
dataset_id: 018f1111-1111-7111-8111-111111111111
dataset_version: 1.0.0
cases:
  - id: 018f1111-1111-7111-8111-111111111101
    task_type: classification
    case_group: classification
    input:
      user_message: "I love this product"
    expected:
      label: positive
    tags: [smoke]
    metadata:
      source: synthetic
"#;

        let dataset: RunDataset = serde_yaml::from_str(raw).unwrap();
        assert_eq!(
            dataset.dataset_id,
            Uuid::parse_str("018f1111-1111-7111-8111-111111111111").unwrap()
        );
        assert_eq!(dataset.cases.len(), 1);
        assert_eq!(
            dataset.cases[0].id,
            Uuid::parse_str("018f1111-1111-7111-8111-111111111101").unwrap()
        );
        assert_eq!(dataset.cases[0].task_type, "classification");
    }
}
