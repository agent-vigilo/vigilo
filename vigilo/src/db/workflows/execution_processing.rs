//! Execution processing workflow.
//!
//! This module owns the worker-side path from a leased dataset case to persisted
//! evaluator results, execution aggregate, and terminal execution transition.
//! It also owns evaluator runtime lookup and single-flight component loading so
//! concurrent workers do not repeatedly compile the same evaluator artifact.

use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    time::Instant,
};

use chrono::{
    DateTime,
    Utc,
};
use futures_util::{
    StreamExt,
    stream::FuturesUnordered,
};
use serde_json::json;
use sqlx::{
    PgPool,
    Postgres,
    QueryBuilder,
    Transaction,
};
use tokio::task::{
    self,
    JoinSet,
};
use tracing::debug;
use uuid::Uuid;

use super::chunk_processing;
use crate::{
    agent_client,
    context::Context,
    contracts::{
        aggregation::{
            AggregationBinding,
            AggregationResult,
            aggregate_results,
            evaluation_status_key,
        },
        evaluator::{
            EvaluationStatus,
            EvaluatorInput,
            EvaluatorOutcome,
            EvaluatorOutput,
            EvaluatorReportedError,
            Measurement,
            Severity,
            TestCase,
        },
        evaluator_abi::{
            EvaluatorAbiIdentity,
            ResolvedEvaluator,
        },
        evaluator_ref::parse_fully_qualified_evaluator,
        normalization,
        run::{
            AggregationSettings,
            CaseGroupProfile,
            EvaluatorBinding,
            NormalizationPolicy,
            PersistRawOutputsMode,
            PersistenceMode,
            PersistenceSettings,
            RunProfile,
        },
    },
    db::tables::{
        evaluator_results,
        evaluators,
    },
    models::run_chunk::RunChunk,
};

mod queries;

use queries::{
    allocate_execution_attempts_for_cases,
    heartbeat_running_attempts_for_chunk_query,
    persist_completed_execution_results_batch,
};
pub(crate) use queries::{
    finalize_execution_terminal_transitions,
    summarize_chunk_execution_state,
};

fn jsonb_text(value: &serde_json::Value) -> String {
    serde_json::to_string(value).expect("serializing serde_json::Value should not fail")
}

#[derive(Debug)]
struct EvaluatorExecutionRecord {
    binding_id: String,
    evaluator_id: Uuid,
    status: EvaluationStatus,
    binding_dimension: String,
    normalized_score: Option<f64>,
    blocking: bool,
    binding_weight: f64,
    failure_category: Option<String>,
    reason: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct CaseEvaluationPlan {
    pub(super) profile_group_id: String,
    pub(super) evaluator_bindings: Vec<EvaluatorBinding>,
    aggregation: AggregationSettings,
}

fn redacted_payload(reason: &str) -> serde_json::Value {
    json!({
        "redacted": true,
        "reason": reason,
    })
}

fn should_persist_raw_evaluator_output(
    persistence: &PersistenceSettings,
    status: &EvaluationStatus,
) -> bool {
    match &persistence.persist_raw_outputs {
        PersistRawOutputsMode::All => true,
        PersistRawOutputsMode::FailuresOnly => {
            matches!(status, EvaluationStatus::Failed | EvaluationStatus::Error)
        }
        PersistRawOutputsMode::None => false,
    }
}

fn persisted_evaluator_evidence(
    persistence: &PersistenceSettings,
    evidence: serde_json::Value,
) -> serde_json::Value {
    if persistence.persist_evaluator_evidence {
        evidence
    } else {
        redacted_payload("persistence.persist_evaluator_evidence=false")
    }
}

fn persisted_raw_evaluator_output(
    persistence: &PersistenceSettings,
    status: &EvaluationStatus,
    raw_output: serde_json::Value,
) -> serde_json::Value {
    if !persistence.persist_evaluator_evidence {
        return redacted_payload("persistence.persist_evaluator_evidence=false");
    }

    if should_persist_raw_evaluator_output(persistence, status) {
        raw_output
    } else {
        redacted_payload(match &persistence.persist_raw_outputs {
            PersistRawOutputsMode::All => "persistence.persist_raw_outputs=all",
            PersistRawOutputsMode::FailuresOnly => "persistence.persist_raw_outputs=failures_only",
            PersistRawOutputsMode::None => "persistence.persist_raw_outputs=none",
        })
    }
}

fn persisted_case_input_payload(
    profile: &RunProfile,
    case: &chunk_processing::WorkerCaseBatchItem,
) -> serde_json::Value {
    match &profile.persistence.mode {
        PersistenceMode::Full => case.input_payload.clone(),
        PersistenceMode::Summary => json!({
            "redacted": true,
            "reason": "persistence.mode=summary",
            "case_hash": case.case_hash.clone(),
            "field": "input_payload",
        }),
    }
}

fn persisted_case_expected_output(
    profile: &RunProfile,
    case: &chunk_processing::WorkerCaseBatchItem,
) -> serde_json::Value {
    match &profile.persistence.mode {
        PersistenceMode::Full => case.expected_output.clone(),
        PersistenceMode::Summary => json!({
            "redacted": true,
            "reason": "persistence.mode=summary",
            "case_hash": case.case_hash.clone(),
            "field": "expected_output",
        }),
    }
}

fn persisted_case_metadata(
    profile: &RunProfile,
    case: &chunk_processing::WorkerCaseBatchItem,
) -> serde_json::Value {
    match &profile.persistence.mode {
        PersistenceMode::Full => case.metadata.clone(),
        PersistenceMode::Summary => json!({
            "redacted": true,
            "reason": "persistence.mode=summary",
            "case_hash": case.case_hash.clone(),
            "field": "case_metadata",
        }),
    }
}

fn persisted_case_tags(profile: &RunProfile, tags: &serde_json::Value) -> serde_json::Value {
    match &profile.persistence.mode {
        PersistenceMode::Full => tags.clone(),
        PersistenceMode::Summary => {
            let required = profile
                .scorecard
                .gates
                .iter()
                .flat_map(|gate| gate.tags_all.iter().map(String::as_str))
                .collect::<std::collections::BTreeSet<_>>();
            serde_json::Value::Array(
                tags.as_array()
                    .into_iter()
                    .flatten()
                    .filter(|tag| tag.as_str().is_some_and(|tag| required.contains(tag)))
                    .cloned()
                    .collect(),
            )
        }
    }
}

fn persisted_evaluator_manifest(
    profile: &RunProfile,
    evaluator_bindings: &[EvaluatorBinding],
) -> anyhow::Result<serde_json::Value> {
    match &profile.persistence.mode {
        PersistenceMode::Full => Ok(serde_json::to_value(evaluator_bindings)?),
        PersistenceMode::Summary => Ok(serde_json::Value::Array(
            evaluator_bindings
                .iter()
                .map(|binding| {
                    json!({
                        "id": binding.id.clone(),
                        "ref": binding.evaluator_ref.clone(),
                        "required": binding.required,
                        "dimension": binding.dimension.clone(),
                        "blocking": binding.blocking,
                        "weight": binding.weight,
                        "normalization": binding.normalization.clone(),
                        "pass_threshold": binding.pass_threshold,
                        "config": redacted_payload("persistence.mode=summary"),
                    })
                })
                .collect(),
        )),
    }
}

/// Runtime metadata needed to execute one evaluator binding for a run.
#[derive(Debug, Clone)]
pub(crate) struct RunEvaluatorCatalogEntry {
    pub(crate) evaluator_id: Uuid,
    pub(crate) evaluator_version: String,
    pub(crate) content_hash: String,
    pub(crate) abi: EvaluatorAbiIdentity,
    pub(crate) evaluator_runtime: String,
    pub(crate) evaluator_runtime_version: String,
    pub(crate) evaluator_runtime_fingerprint: String,
}

/// Lookup table keyed by fully qualified evaluator ref.
pub(crate) type RunEvaluatorCatalog = BTreeMap<String, RunEvaluatorCatalogEntry>;

fn evaluator_output_identity_error(
    output: &EvaluatorOutput,
    evaluator_ref: &str,
    evaluator: &RunEvaluatorCatalogEntry,
) -> Option<String> {
    if output.evaluator_identifier() != evaluator_ref {
        return Some(format!(
            "evaluator reported identity '{}' but binding expected '{}'",
            output.evaluator_identifier(),
            evaluator_ref,
        ));
    }
    if output
        .evaluator
        .interface_version
        .as_deref()
        .is_some_and(|version| version != evaluator.abi.version)
    {
        return Some(format!(
            "evaluator '{}' reported interface version '{}' but run pinned '{}'",
            evaluator_ref,
            output
                .evaluator
                .interface_version
                .as_deref()
                .unwrap_or_default(),
            evaluator.abi.version,
        ));
    }
    if output
        .evaluator
        .content_hash
        .as_deref()
        .is_some_and(|hash| hash != evaluator.content_hash)
    {
        return Some(format!(
            "evaluator '{}' reported content hash '{}' but run pinned '{}'",
            evaluator_ref,
            output.evaluator.content_hash.as_deref().unwrap_or_default(),
            evaluator.content_hash,
        ));
    }
    None
}

/// Builds a worker catalog only from the immutable run execution plan.
pub(crate) fn build_pinned_evaluator_catalog(
    resolved: Vec<ResolvedEvaluator>,
) -> anyhow::Result<RunEvaluatorCatalog> {
    let mut catalog = BTreeMap::new();
    for evaluator in resolved {
        crate::evaluator_abi::resolve_identity(&evaluator.abi)?;
        let identity = parse_fully_qualified_evaluator(&evaluator.evaluator_ref)?;
        let entry = RunEvaluatorCatalogEntry {
            evaluator_id: evaluator.evaluator_id,
            evaluator_version: identity.version,
            content_hash: evaluator.content_hash,
            abi: evaluator.abi,
            evaluator_runtime: evaluator.runtime,
            evaluator_runtime_version: evaluator.runtime_version,
            evaluator_runtime_fingerprint: evaluator.runtime_fingerprint,
        };
        if catalog
            .insert(evaluator.evaluator_ref.clone(), entry)
            .is_some()
        {
            anyhow::bail!(
                "run execution plan repeats evaluator '{}'",
                evaluator.evaluator_ref
            );
        }
    }
    Ok(catalog)
}

const CASE_EXECUTION_PARALLELISM: usize = 8;
const EVALUATOR_EXECUTION_PARALLELISM: usize = 8;
const EXECUTION_RETRY_BASE_SECONDS: i32 = 5;
const EXECUTION_RETRY_MAX_SECONDS: i32 = 600;

/// Terminal state transition to apply after evaluator processing finishes.
///
/// The attempt id and attempt number are authority tokens. A transition only
/// applies if the execution still points at the same current attempt and, for
/// worker-owned transitions, the worker still owns a live attempt lease.
#[derive(Debug, Clone)]
pub(crate) struct ExecutionTerminalTransition {
    pub(crate) execution_id: Uuid,
    pub(crate) attempt_id: Uuid,
    pub(crate) attempt_no: i32,
    pub(crate) completed: bool,
    pub(crate) error_message: Option<String>,
    pub(crate) requires_worker_lease: bool,
}

/// Worker ownership attached to newly allocated execution attempts.
#[derive(Debug, Clone)]
pub(crate) struct AttemptLeaseContext {
    pub(crate) worker_id: Uuid,
    pub(crate) worker_host: Option<String>,
    pub(crate) queue_message_id: Uuid,
    pub(crate) broker_message_id: Option<String>,
    pub(crate) lease_seconds: i32,
}

fn validate_attempt_lease_seconds(lease_seconds: i32) -> anyhow::Result<()> {
    if lease_seconds <= 0 {
        anyhow::bail!("attempt lease_seconds must be greater than zero");
    }
    Ok(())
}

fn validate_attempt_allocation_batch(
    case_count: usize,
    plan_count: usize,
    max_attempts: u32,
    lease_seconds: i32,
) -> anyhow::Result<Option<i32>> {
    if case_count == 0 {
        return Ok(None);
    }
    if max_attempts == 0 {
        anyhow::bail!("run profile defaults.max_attempts must be greater than zero");
    }
    validate_attempt_lease_seconds(lease_seconds)?;
    if case_count != plan_count {
        anyhow::bail!(
            "case batch has {} cases but {} evaluation plans",
            case_count,
            plan_count
        );
    }
    Ok(Some(i32::try_from(max_attempts)?))
}

pub(crate) async fn heartbeat_running_attempts_for_chunk(
    db: &PgPool,
    run_id: Uuid,
    run_shard: i16,
    chunk_id: Uuid,
    worker_id: Uuid,
    lease_seconds: i32,
) -> anyhow::Result<u64> {
    validate_attempt_lease_seconds(lease_seconds)?;
    heartbeat_running_attempts_for_chunk_query(
        db,
        run_id,
        run_shard,
        chunk_id,
        worker_id,
        lease_seconds,
    )
    .await
}

/// Result of processing a single case execution.
#[derive(Debug)]
pub(crate) struct ProcessedExecution {
    pub(crate) execution_id: Uuid,
    pub(crate) attempt_id: Uuid,
    pub(crate) result_count: usize,
    pub(crate) terminal_transition: ExecutionTerminalTransition,
}

#[derive(Debug, sqlx::FromRow)]
struct AttemptAllocation {
    case_id: Uuid,
    execution_id: Uuid,
    attempt_id: Option<Uuid>,
    attempt_no: i32,
    should_process: bool,
    already_terminal: bool,
    retry_not_due: bool,
    max_attempts_exhausted: bool,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct ChunkExecutionState {
    pub(crate) open_execution_count: i64,
    pub(crate) retry_scheduled_count: i64,
    pub(crate) next_retry_after: Option<DateTime<Utc>>,
}

#[derive(Debug)]
struct CompletedExecutionPersistence {
    execution_id: Uuid,
    attempt_id: Uuid,
    attempt_no: i32,
    result_rows: Vec<evaluator_results::EvaluatorResultInsertRow>,
    overall_status: String,
    aggregate_score: Option<f64>,
    evaluator_result_count: i32,
    dimension_scores: serde_json::Value,
    blocking_failures: serde_json::Value,
    summary: serde_json::Value,
}

#[derive(Debug)]
struct CaseExecutionOutcome {
    processed: Option<ProcessedExecution>,
    persistence: Option<CompletedExecutionPersistence>,
}

#[derive(Debug, Default)]
struct BatchPersistenceStats {
    evaluator_results_attempted: usize,
    evaluator_results_inserted: u64,
    evaluator_result_conflicts: usize,
}

fn processed_terminal_failure(
    execution_id: Uuid,
    attempt_id: Uuid,
    attempt_no: i32,
    error_message: String,
) -> ProcessedExecution {
    ProcessedExecution {
        execution_id,
        attempt_id,
        result_count: 0,
        terminal_transition: ExecutionTerminalTransition {
            execution_id,
            attempt_id,
            attempt_no,
            completed: false,
            error_message: Some(error_message),
            requires_worker_lease: true,
        },
    }
}

fn processed_terminal_failure_without_worker_lease(
    execution_id: Uuid,
    attempt_id: Uuid,
    attempt_no: i32,
    error_message: String,
) -> ProcessedExecution {
    ProcessedExecution {
        execution_id,
        attempt_id,
        result_count: 0,
        terminal_transition: ExecutionTerminalTransition {
            execution_id,
            attempt_id,
            attempt_no,
            completed: false,
            error_message: Some(error_message),
            requires_worker_lease: false,
        },
    }
}

/// Extracts unique evaluator refs from a run profile.
///
/// Database behavior: none. This is a deterministic profile scan used before
/// catalog lookup so each evaluator identity is fetched once per run context.
pub(crate) fn evaluator_refs_from_profile(profile: &RunProfile) -> anyhow::Result<Vec<String>> {
    let mut unique_refs = BTreeSet::new();
    for group in &profile.case_groups {
        for binding in &group.evaluators {
            unique_refs.insert(binding.evaluator_ref.clone());
        }
    }

    Ok(unique_refs.into_iter().collect())
}

/// Loads or returns the exact component pinned by a run execution plan.
///
/// Component compilation is single-flight through the registry cache, so
/// concurrent requests for the same artifact and ABI share one load operation.
///
/// On a cache miss, this fetches the evaluator by immutable database id,
/// verifies every pinned artifact, ABI, and runtime field, typed-links the
/// selected adapter, and caches the compiled component for the process.
pub(crate) async fn get_or_load_component(
    context: &Context,
    evaluator_ref: &str,
    evaluator: &RunEvaluatorCatalogEntry,
) -> anyhow::Result<crate::evaluator_abi::PreparedEvaluator> {
    let context = context.clone();
    let evaluator_ref_owned = evaluator_ref.to_string();
    let evaluator_ref_for_closure = evaluator_ref_owned.clone();
    let evaluator = evaluator.clone();
    let cache_key = evaluator.abi.cache_key(&evaluator.content_hash);
    let cache = context.reg().await?.clone();
    let component = cache
        .try_get_with(cache_key, async move {
            let db = context.dbr().await?.control().await?;
            let evaluator_record = evaluators::select_evaluator_by_id(db, evaluator.evaluator_id)
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "pinned evaluator '{}' ({}) was not found during worker execution",
                        evaluator_ref_for_closure,
                        evaluator.evaluator_id,
                    )
                })?;
            let actual_interface = evaluator_record.interface_name.rsplit_once('/');
            let matches_plan = evaluator_record.content_hash == evaluator.content_hash
                && actual_interface.is_some_and(|(package, interface)| {
                    package == evaluator.abi.package && interface == evaluator.abi.interface
                })
                && evaluator_record.wit_world == evaluator.abi.world
                && evaluator_record.interface_version == evaluator.abi.version
                && evaluator_record.abi_contract_hash == evaluator.abi.contract_hash
                && evaluator_record.abi_adapter == evaluator.abi.adapter
                && evaluator_record.runtime == evaluator.evaluator_runtime
                && evaluator_record.runtime_version == evaluator.evaluator_runtime_version
                && evaluator_record.runtime_fingerprint == evaluator.evaluator_runtime_fingerprint;
            if !matches_plan {
                anyhow::bail!(
                    "pinned evaluator '{}' no longer matches the run execution plan",
                    evaluator_ref_for_closure
                );
            }

            let wasm = context.wasm().await?;
            wasm.compile_evaluator(&evaluator_record.wasm_bytes, &evaluator.abi)
        })
        .await
        .map_err(|err| {
            anyhow::anyhow!(
                "single-flight component load failed for evaluator '{}': {}",
                evaluator_ref_owned,
                err
            )
        })?;

    Ok(component)
}

fn tags_from_case_row(tags: &serde_json::Value) -> Vec<String> {
    tags.as_array()
        .into_iter()
        .flatten()
        .filter_map(|value| value.as_str().map(ToOwned::to_owned))
        .collect()
}

fn metadata_from_case_row(
    metadata: &serde_json::Value,
) -> anyhow::Result<BTreeMap<String, serde_json::Value>> {
    match metadata {
        serde_json::Value::Object(map) => Ok(map
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()),
        _ => anyhow::bail!("case metadata must be a JSON object"),
    }
}

fn matching_groups_for_case<'a>(
    profile: &'a RunProfile,
    case: &chunk_processing::WorkerCaseBatchItem,
) -> Vec<&'a CaseGroupProfile> {
    if let Some(group_id) = case.case_group.as_deref() {
        return profile
            .case_groups
            .iter()
            .filter(|group| group.id == group_id)
            .collect();
    }

    let case_tags = tags_from_case_row(&case.tags);

    profile
        .case_groups
        .iter()
        .filter(|group| {
            if group.applies_to.task_type != case.task_type {
                return false;
            }

            if !group.applies_to.tags_any.is_empty()
                && !group
                    .applies_to
                    .tags_any
                    .iter()
                    .any(|tag| case_tags.iter().any(|case_tag| case_tag == tag))
            {
                return false;
            }

            if !group
                .applies_to
                .tags_all
                .iter()
                .all(|tag| case_tags.iter().any(|case_tag| case_tag == tag))
            {
                return false;
            }

            true
        })
        .collect()
}

fn merge_aggregation_settings(
    case_id: Uuid,
    groups: &[&CaseGroupProfile],
) -> anyhow::Result<AggregationSettings> {
    let mut merged = AggregationSettings {
        dimensions: BTreeMap::new(),
    };

    for group in groups {
        for (dimension, policy) in &group.aggregation.dimensions {
            if let Some(existing) = merged.dimensions.get(dimension) {
                if existing != policy {
                    anyhow::bail!(
                        "case '{}' matched conflicting aggregation policy for dimension '{}' across case_groups",
                        case_id,
                        dimension
                    );
                }
                continue;
            }

            merged.dimensions.insert(dimension.clone(), policy.clone());
        }
    }

    Ok(merged)
}

fn equivalent_evaluator_binding(left: &EvaluatorBinding, right: &EvaluatorBinding) -> bool {
    left.evaluator_ref == right.evaluator_ref
        && left.required == right.required
        && left.dimension == right.dimension
        && left.blocking == right.blocking
        && left.weight == right.weight
        && left.normalization == right.normalization
        && left.pass_threshold == right.pass_threshold
        && left.config == right.config
}

/// Resolves evaluator bindings and aggregation policy for one dataset case.
///
/// Database behavior: none. Runtime aggregation is driven by the profile
/// case-group policies that selected the evaluator bindings. When multiple
/// groups match the same case, non-overlapping or identical dimension policies
/// are merged; conflicting policies are rejected rather than guessed.
pub(super) fn evaluation_plan_for_case(
    profile: &RunProfile,
    case: &chunk_processing::WorkerCaseBatchItem,
) -> anyhow::Result<CaseEvaluationPlan> {
    let matching_groups = matching_groups_for_case(profile, case);
    if matching_groups.is_empty() {
        anyhow::bail!(
            "case '{}' did not match any evaluator bindings in run profile",
            case.case_id
        );
    }

    let profile_group_id = matching_groups
        .iter()
        .map(|group| group.id.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let aggregation = merge_aggregation_settings(case.case_id, &matching_groups)?;
    let mut unique = BTreeMap::new();
    for group in matching_groups {
        for binding in &group.evaluators {
            if let Some(existing) = unique.get(&binding.id) {
                if !equivalent_evaluator_binding(existing, binding) {
                    anyhow::bail!(
                        "case '{}' matched conflicting evaluator binding id '{}' across case_groups",
                        case.case_id,
                        binding.id
                    );
                }
                continue;
            }

            unique.insert(binding.id.clone(), binding.clone());
        }
    }

    let evaluator_bindings = unique.into_values().collect::<Vec<_>>();
    if evaluator_bindings.is_empty() {
        anyhow::bail!(
            "case '{}' matched run profile groups with no evaluator bindings",
            case.case_id
        );
    }

    Ok(CaseEvaluationPlan {
        profile_group_id,
        evaluator_bindings,
        aggregation,
    })
}

/// Converts a persisted chunk case row into the evaluator contract shape.
///
/// Database behavior: none. This is the boundary where stored payloads become
/// WIT-facing `input`, `expected`, `context`, tag, and metadata fields.
fn make_test_case(case: &chunk_processing::WorkerCaseBatchItem) -> anyhow::Result<TestCase> {
    Ok(TestCase {
        id: case.case_id.to_string(),
        task_type: case.task_type.clone(),
        case_group: case.case_group.clone(),
        input: case.input_payload.clone(),
        expected: Some(case.expected_output.clone()),
        context: Some(case.context_payload.clone()),
        tags: tags_from_case_row(&case.tags),
        metadata: metadata_from_case_row(&case.metadata)?,
    })
}

fn map_evaluation_status(status: &EvaluationStatus) -> &'static str {
    evaluation_status_key(status)
}

fn map_severity(severity: &Severity) -> &'static str {
    match severity {
        Severity::None => "none",
        Severity::Low => "low",
        Severity::Medium => "medium",
        Severity::High => "high",
        Severity::Critical => "critical",
    }
}

fn normalize_measurement(
    policy: &NormalizationPolicy,
    measurement: &Measurement,
) -> anyhow::Result<f64> {
    normalization::normalize_measurement(policy, measurement).map_err(anyhow::Error::msg)
}

fn normalization_policy_hash(policy: &NormalizationPolicy) -> anyhow::Result<String> {
    Ok(blake3::hash(&serde_json::to_vec(policy)?)
        .to_hex()
        .to_string())
}

fn set_raw_measurement(
    row: &mut evaluator_results::EvaluatorResultInsertRow,
    measurement: &Measurement,
) {
    match measurement {
        Measurement::Binary { value } => {
            row.measurement_kind = Some(measurement.kind().to_string());
            row.raw_boolean = Some(*value);
        }
        Measurement::Numeric { value, unit } if value.is_finite() => {
            row.measurement_kind = Some(measurement.kind().to_string());
            row.raw_numeric = Some(*value);
            row.raw_unit.clone_from(unit);
        }
        Measurement::Ordinal { value } if !value.trim().is_empty() => {
            row.measurement_kind = Some(measurement.kind().to_string());
            row.raw_ordinal = Some(value.clone());
        }
        Measurement::Numeric { .. } | Measurement::Ordinal { .. } => {}
    }
}

fn interpret_measurement(
    policy: &NormalizationPolicy,
    pass_threshold: f64,
    measurement: &Measurement,
) -> anyhow::Result<(f64, EvaluationStatus)> {
    let score = normalize_measurement(policy, measurement)?;
    let status = if score >= pass_threshold {
        EvaluationStatus::Passed
    } else {
        EvaluationStatus::Failed
    };
    Ok((score, status))
}

#[allow(clippy::too_many_arguments)]
async fn evaluate_case_execution(
    context: &Context,
    run_id: Uuid,
    run_shard: i16,
    run_profile: &RunProfile,
    evaluator_catalog: &RunEvaluatorCatalog,
    case: &chunk_processing::WorkerCaseBatchItem,
    evaluator_bindings: &[EvaluatorBinding],
    aggregation: &AggregationSettings,
    allocation: &AttemptAllocation,
) -> CaseExecutionOutcome {
    let runtime_started = Instant::now();
    let execution_id = allocation.execution_id;
    let Some(attempt_id) = allocation.attempt_id else {
        let failure_message = format!(
            "case '{}' was selected for processing without an allocated attempt",
            case.case_id
        );
        return CaseExecutionOutcome {
            processed: Some(processed_terminal_failure(
                execution_id,
                Uuid::nil(),
                allocation.attempt_no,
                failure_message,
            )),
            persistence: None,
        };
    };
    let attempt_no = allocation.attempt_no;

    let evaluation_result: anyhow::Result<(
        Vec<AggregationBinding>,
        Vec<EvaluatorExecutionRecord>,
        Vec<evaluator_results::EvaluatorResultInsertRow>,
    )> = async {
        let test_case = make_test_case(case)?;
        let http = context.http().await?;
        let agent_output = agent_client::invoke(
            http,
            context.agent_http_limits(),
            run_id,
            execution_id,
            attempt_id,
            run_profile,
            &test_case,
        )
        .await
        .map_err(|err| anyhow::anyhow!("agent invocation failed: {}", err))?;

        let aggregation_bindings = evaluator_bindings
            .iter()
            .map(|binding| {
                if !evaluator_catalog.contains_key(&binding.evaluator_ref) {
                    return Err(anyhow::anyhow!(
                        "evaluator '{}' is missing from run evaluator catalog",
                        binding.evaluator_ref
                    ));
                }
                Ok(AggregationBinding {
                    binding_id: binding.id.clone(),
                    required: binding.required,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        let mut ordered_records = BTreeMap::new();
        let mut ordered_rows = BTreeMap::new();
        let parallelism = EVALUATOR_EXECUTION_PARALLELISM.min(evaluator_bindings.len().max(1));
        let profile_id = run_profile.profile_id.clone();
        let profile_version = run_profile.profile_version.clone();
        let mut next_index = 0usize;
        let mut tasks = JoinSet::<
            anyhow::Result<(
                usize,
                Vec<EvaluatorExecutionRecord>,
                Vec<evaluator_results::EvaluatorResultInsertRow>,
            )>,
        >::new();

        while next_index < evaluator_bindings.len() || !tasks.is_empty() {
            while next_index < evaluator_bindings.len() && tasks.len() < parallelism {
                let index = next_index;
                next_index += 1;

                let binding = evaluator_bindings[index].clone();
                let evaluator_entry = evaluator_catalog
                    .get(&binding.evaluator_ref)
                    .cloned()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "evaluator '{}' is missing from run evaluator catalog",
                            binding.evaluator_ref
                        )
                    })?;
                let dimension_policy_blocking = aggregation
                    .dimensions
                    .get(&binding.dimension)
                    .map(|policy| policy.blocking)
                    .unwrap_or(false);
                let context = context.clone();
                let test_case = test_case.clone();
                let agent_output = agent_output.clone();
                let profile_id = profile_id.clone();
                let profile_version = profile_version.clone();
                let persistence = run_profile.persistence.clone();
                tasks.spawn(async move {
                    let policy_hash = normalization_policy_hash(&binding.normalization)?;
                    let wasm = context.wasm().await?.clone();
                    let component =
                        get_or_load_component(&context, &binding.evaluator_ref, &evaluator_entry)
                            .await?;
                    let wasm_permit = wasm.acquire_evaluation_permit().await?;

                    let evaluator_input = EvaluatorInput {
                        run_id: run_id.to_string(),
                        execution_id: execution_id.to_string(),
                        attempt_id: attempt_id.to_string(),
                        case: test_case,
                        actual: agent_output,
                        evaluator_config: binding.config.clone(),
                    };
                    let output = task::spawn_blocking(move || {
                        let _wasm_permit = wasm_permit;
                        wasm.test_evaluator_component(&component, evaluator_input)
                    })
                    .await
                    .map_err(|err| {
                        anyhow::anyhow!("evaluator blocking task join failed: {}", err)
                    })?;

                    let blocking = binding.blocking || dimension_policy_blocking;
                    let base_row =
                        |status: &EvaluationStatus| evaluator_results::EvaluatorResultInsertRow {
                            run_id,
                            run_shard,
                            execution_id,
                            attempt_id,
                            binding_id: binding.id.clone(),
                            evaluator_id: evaluator_entry.evaluator_id,
                            evaluator_version: evaluator_entry.evaluator_version.clone(),
                            evaluator_profile_id: profile_id.clone(),
                            evaluator_profile_version: profile_version.clone(),
                            evaluator_interface_version: Some(evaluator_entry.abi.version.clone()),
                            evaluator_runtime_version: Some(
                                evaluator_entry.evaluator_runtime_version.clone(),
                            ),
                            dimension: binding.dimension.clone(),
                            outcome: "error".to_string(),
                            judgment: None,
                            blocking,
                            measurement_kind: None,
                            raw_boolean: None,
                            raw_numeric: None,
                            raw_ordinal: None,
                            raw_unit: None,
                            normalized_score: None,
                            normalization_policy_hash: policy_hash.clone(),
                            pass_threshold: binding.pass_threshold,
                            weight: binding.weight,
                            error_code: None,
                            error_message: None,
                            abstention_category: None,
                            abstention_reason: None,
                            raw_evaluator_output: persisted_raw_evaluator_output(
                                &persistence,
                                status,
                                json!({}),
                            ),
                            diagnostics: Vec::new(),
                        };

                    let (record, row) = match output {
                        Ok(evaluator_output) => {
                            let serialized_output = serde_json::to_value(&evaluator_output)?;
                            if let Some(reason) = evaluator_output_identity_error(
                                &evaluator_output,
                                &binding.evaluator_ref,
                                &evaluator_entry,
                            ) {
                                let status = EvaluationStatus::Error;
                                let mut row = base_row(&status);
                                row.error_code = Some("identity_mismatch".to_string());
                                row.error_message = Some(reason.clone());
                                row.raw_evaluator_output = persisted_raw_evaluator_output(
                                    &persistence,
                                    &status,
                                    serialized_output,
                                );
                                (
                                    EvaluatorExecutionRecord {
                                        binding_id: binding.id.clone(),
                                        evaluator_id: evaluator_entry.evaluator_id,
                                        status,
                                        binding_dimension: binding.dimension.clone(),
                                        normalized_score: None,
                                        blocking,
                                        binding_weight: binding.weight,
                                        failure_category: Some("identity_mismatch".to_string()),
                                        reason: Some(reason),
                                    },
                                    row,
                                )
                            } else {
                                let diagnostics = evaluator_output
                                    .diagnostics
                                    .iter()
                                    .enumerate()
                                    .map(|(index, diagnostic)| {
                                        Ok(evaluator_results::EvaluatorDiagnosticInsertRow {
                                            diagnostic_index: i32::try_from(index)?,
                                            severity: map_severity(&diagnostic.severity)
                                                .to_string(),
                                            category: diagnostic.category.clone(),
                                            reason: diagnostic.reason.clone(),
                                            evidence: persisted_evaluator_evidence(
                                                &persistence,
                                                diagnostic.evidence.clone(),
                                            ),
                                            tags: diagnostic.tags.clone(),
                                        })
                                    })
                                    .collect::<anyhow::Result<Vec<_>>>()?;

                                match &evaluator_output.outcome {
                                    EvaluatorOutcome::Completed(measurement) => {
                                        match interpret_measurement(
                                            &binding.normalization,
                                            binding.pass_threshold,
                                            measurement,
                                        ) {
                                            Ok((score, status)) => {
                                                let mut row = base_row(&status);
                                                row.outcome = "completed".to_string();
                                                row.judgment = Some(
                                                    map_evaluation_status(&status).to_string(),
                                                );
                                                set_raw_measurement(&mut row, measurement);
                                                row.normalized_score = Some(score);
                                                row.raw_evaluator_output =
                                                    persisted_raw_evaluator_output(
                                                        &persistence,
                                                        &status,
                                                        serialized_output,
                                                    );
                                                row.diagnostics = diagnostics;
                                                (
                                                    EvaluatorExecutionRecord {
                                                        binding_id: binding.id.clone(),
                                                        evaluator_id: evaluator_entry.evaluator_id,
                                                        status,
                                                        binding_dimension: binding
                                                            .dimension
                                                            .clone(),
                                                        normalized_score: Some(score),
                                                        blocking,
                                                        binding_weight: binding.weight,
                                                        failure_category: None,
                                                        reason: None,
                                                    },
                                                    row,
                                                )
                                            }
                                            Err(err) => {
                                                let status = EvaluationStatus::Error;
                                                let reason = err.to_string();
                                                let mut row = base_row(&status);
                                                set_raw_measurement(&mut row, measurement);
                                                row.error_code =
                                                    Some("invalid_measurement".to_string());
                                                row.error_message = Some(reason.clone());
                                                row.raw_evaluator_output =
                                                    persisted_raw_evaluator_output(
                                                        &persistence,
                                                        &status,
                                                        serialized_output,
                                                    );
                                                row.diagnostics = diagnostics;
                                                (
                                                    EvaluatorExecutionRecord {
                                                        binding_id: binding.id.clone(),
                                                        evaluator_id: evaluator_entry.evaluator_id,
                                                        status,
                                                        binding_dimension: binding
                                                            .dimension
                                                            .clone(),
                                                        normalized_score: None,
                                                        blocking,
                                                        binding_weight: binding.weight,
                                                        failure_category: Some(
                                                            "invalid_measurement".to_string(),
                                                        ),
                                                        reason: Some(reason),
                                                    },
                                                    row,
                                                )
                                            }
                                        }
                                    }
                                    EvaluatorOutcome::Abstained(abstention) => {
                                        let status = EvaluationStatus::Abstained;
                                        let mut row = base_row(&status);
                                        row.outcome = "abstained".to_string();
                                        row.abstention_category = Some(abstention.category.clone());
                                        row.abstention_reason = abstention.reason.clone();
                                        row.raw_evaluator_output = persisted_raw_evaluator_output(
                                            &persistence,
                                            &status,
                                            serialized_output,
                                        );
                                        row.diagnostics = diagnostics;
                                        (
                                            EvaluatorExecutionRecord {
                                                binding_id: binding.id.clone(),
                                                evaluator_id: evaluator_entry.evaluator_id,
                                                status,
                                                binding_dimension: binding.dimension.clone(),
                                                normalized_score: None,
                                                blocking,
                                                binding_weight: binding.weight,
                                                failure_category: Some(abstention.category.clone()),
                                                reason: abstention.reason.clone(),
                                            },
                                            row,
                                        )
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            let status = EvaluationStatus::Error;
                            let reported = err.downcast_ref::<EvaluatorReportedError>();
                            let error_code = reported
                                .map(|error| error.code.clone())
                                .unwrap_or_else(|| "evaluator_runtime_error".to_string());
                            let failure_category = if reported.is_some() {
                                "evaluator_error"
                            } else {
                                "evaluator_runtime_error"
                            };
                            let reason = reported
                                .map(|error| error.message.clone())
                                .unwrap_or_else(|| err.to_string());
                            let mut row = base_row(&status);
                            row.error_code = Some(error_code.clone());
                            row.error_message = Some(reason.clone());
                            row.raw_evaluator_output = persisted_raw_evaluator_output(
                                &persistence,
                                &status,
                                json!({ "code": error_code, "error": reason }),
                            );
                            (
                                EvaluatorExecutionRecord {
                                    binding_id: binding.id.clone(),
                                    evaluator_id: evaluator_entry.evaluator_id,
                                    status,
                                    binding_dimension: binding.dimension.clone(),
                                    normalized_score: None,
                                    blocking,
                                    binding_weight: binding.weight,
                                    failure_category: Some(failure_category.to_string()),
                                    reason: Some(reason),
                                },
                                row,
                            )
                        }
                    };

                    Ok((index, vec![record], vec![row]))
                });
            }

            let Some(task_result) = tasks.join_next().await else {
                continue;
            };

            let (index, record, row) = task_result
                .map_err(|err| anyhow::anyhow!("evaluator worker task join failed: {}", err))??;
            ordered_records.insert(index, record);
            ordered_rows.insert(index, row);
        }

        Ok((
            aggregation_bindings,
            ordered_records.into_values().flatten().collect(),
            ordered_rows.into_values().flatten().collect(),
        ))
    }
    .await;

    match evaluation_result {
        Ok((aggregation_bindings, records, result_rows)) => {
            let runtime_ms = runtime_started.elapsed().as_millis() as u64;
            let aggregation_results = records
                .iter()
                .map(|record| AggregationResult {
                    binding_id: record.binding_id.clone(),
                    evaluator_id: record.evaluator_id,
                    binding_dimension: record.binding_dimension.clone(),
                    status: record.status.clone(),
                    normalized_score: record.normalized_score,
                    blocking: record.blocking,
                    binding_weight: record.binding_weight,
                    failure_category: record.failure_category.clone(),
                    reason: record.reason.clone(),
                })
                .collect::<Vec<_>>();
            let aggregate = aggregate_results(
                &run_profile.defaults,
                aggregation,
                attempt_id,
                &aggregation_bindings,
                &aggregation_results,
            );

            let evaluator_result_count = match i32::try_from(records.len()) {
                Ok(count) => count,
                Err(err) => {
                    let failure_message = format!("case execution result count overflow: {}", err);
                    return CaseExecutionOutcome {
                        processed: Some(processed_terminal_failure(
                            execution_id,
                            attempt_id,
                            attempt_no,
                            failure_message,
                        )),
                        persistence: None,
                    };
                }
            };

            debug!(
                run_id = %run_id,
                case_id = %case.case_id,
                execution_id = %execution_id,
                attempt_id = %attempt_id,
                evaluator_results_attempted = result_rows.len(),
                runtime_ms,
                "completed case evaluator execution"
            );

            let processed = ProcessedExecution {
                execution_id,
                attempt_id,
                result_count: records.len(),
                terminal_transition: ExecutionTerminalTransition {
                    execution_id,
                    attempt_id,
                    attempt_no,
                    completed: true,
                    error_message: None,
                    requires_worker_lease: true,
                },
            };

            CaseExecutionOutcome {
                processed: Some(processed),
                persistence: Some(CompletedExecutionPersistence {
                    execution_id,
                    attempt_id,
                    attempt_no,
                    result_rows,
                    overall_status: aggregate.overall_status,
                    aggregate_score: aggregate.aggregate_score,
                    evaluator_result_count,
                    dimension_scores: aggregate.dimension_scores,
                    blocking_failures: aggregate.blocking_failures,
                    summary: aggregate.summary,
                }),
            }
        }
        Err(err) => {
            let error_message = err.to_string();
            let failure_message = if error_message.starts_with("agent invocation failed:") {
                error_message
            } else {
                format!("case execution processing failed: {}", error_message)
            };
            CaseExecutionOutcome {
                processed: Some(processed_terminal_failure(
                    execution_id,
                    attempt_id,
                    attempt_no,
                    failure_message,
                )),
                persistence: None,
            }
        }
    }
}

/// Processes a chunk-sized batch of dataset cases.
///
/// The function allocates authoritative attempts in one transaction, runs cases
/// with bounded chunk-local parallelism, runs each case's evaluators with
/// bounded per-case parallelism, and persists all completed case results in one
/// chunk-level transaction. Terminal execution transitions are returned for the
/// caller to batch-apply after persistence.
///
/// Workflow behavior:
/// - Resolve evaluator bindings from the run profile for each case.
/// - Ask the database which executions are due, waiting, terminal, or exhausted.
/// - Run only due cases; waiting and terminal cases are skipped for this pass.
/// - Convert exhausted open cases into failed terminal transitions so retry
///   loops cannot continue indefinitely.
/// - Persist successful evaluator evidence before returning terminal
///   transitions for authority-checked status updates.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_case_batch_execution(
    context: &Context,
    db: &PgPool,
    database_alias: &str,
    chunk: &RunChunk,
    lease: &AttemptLeaseContext,
    run_profile: &RunProfile,
    evaluator_catalog: &RunEvaluatorCatalog,
    cases: &[chunk_processing::WorkerCaseBatchItem],
) -> anyhow::Result<Vec<ProcessedExecution>> {
    if cases.is_empty() {
        return Ok(Vec::new());
    }

    let run_id = chunk.run_id;
    let run_shard = chunk.run_shard;
    let setup_started = Instant::now();
    let evaluation_plans_by_case = cases
        .iter()
        .map(|case| evaluation_plan_for_case(run_profile, case))
        .collect::<anyhow::Result<Vec<_>>>()?;

    let database_router = context.dbr().await?;
    let allocations = database_router
        .run_database_operation(
            database_alias,
            "worker_attempt_allocation",
            allocate_execution_attempts_for_cases(
                db,
                chunk,
                lease,
                run_profile,
                cases,
                &evaluation_plans_by_case,
            ),
        )
        .await?;

    debug!(
        run_id = %run_id,
        case_count = cases.len(),
        setup_ms = setup_started.elapsed().as_millis() as u64,
        "initialized execution attempts for case batch"
    );

    let parallelism = CASE_EXECUTION_PARALLELISM.min(cases.len().max(1));
    let mut next_index = 0usize;
    let mut pending = FuturesUnordered::new();
    let mut ordered_outcomes = Vec::with_capacity(cases.len());

    while next_index < cases.len() || !pending.is_empty() {
        while next_index < cases.len() && pending.len() < parallelism {
            let index = next_index;
            next_index += 1;

            let case = &cases[index];
            let evaluation_plan = &evaluation_plans_by_case[index];
            let allocation = &allocations[index];
            pending.push(async move {
                if allocation.case_id != case.case_id {
                    anyhow::bail!(
                        "attempt allocation returned case '{}' while processing case '{}'",
                        allocation.case_id,
                        case.case_id
                    );
                }

                let outcome = if allocation.retry_not_due || allocation.already_terminal {
                    CaseExecutionOutcome {
                        processed: None,
                        persistence: None,
                    }
                } else if allocation.max_attempts_exhausted {
                    let attempt_id = allocation.attempt_id.ok_or_else(|| {
                        anyhow::anyhow!(
                            "case '{}' exhausted attempts without a current attempt id",
                            case.case_id
                        )
                    })?;
                    CaseExecutionOutcome {
                        processed: Some(processed_terminal_failure_without_worker_lease(
                            allocation.execution_id,
                            attempt_id,
                            allocation.attempt_no,
                            format!(
                                "execution retry budget exhausted after {} attempts",
                                allocation.attempt_no
                            ),
                        )),
                        persistence: None,
                    }
                } else if allocation.should_process {
                    evaluate_case_execution(
                        context,
                        run_id,
                        run_shard,
                        run_profile,
                        evaluator_catalog,
                        case,
                        &evaluation_plan.evaluator_bindings,
                        &evaluation_plan.aggregation,
                        allocation,
                    )
                    .await
                } else {
                    CaseExecutionOutcome {
                        processed: None,
                        persistence: None,
                    }
                };

                Ok::<_, anyhow::Error>((index, outcome))
            });
        }

        let Some(result) = pending.next().await else {
            continue;
        };
        ordered_outcomes.push(result?);
    }

    ordered_outcomes.sort_by_key(|(index, _)| *index);

    let mut processed = Vec::with_capacity(cases.len());
    let mut completed = Vec::new();
    for (_, outcome) in ordered_outcomes {
        if let Some(persistence) = outcome.persistence {
            completed.push(persistence);
        }
        if let Some(outcome) = outcome.processed {
            processed.push(outcome);
        }
    }

    let persistence_started = Instant::now();
    let persistence_stats = database_router
        .run_database_operation(
            database_alias,
            "worker_result_persistence",
            persist_completed_execution_results_batch(db, chunk, lease.worker_id, &completed),
        )
        .await?;
    debug!(
        run_id = %run_id,
        case_count = cases.len(),
        case_parallelism = parallelism,
        completed_case_count = completed.len(),
        evaluator_results_attempted = persistence_stats.evaluator_results_attempted,
        evaluator_results_inserted = persistence_stats.evaluator_results_inserted,
        evaluator_result_conflicts = persistence_stats.evaluator_result_conflicts,
        persistence_ms = persistence_started.elapsed().as_millis() as u64,
        "persisted completed case results for batch"
    );

    Ok(processed)
}

#[cfg(test)]
#[path = "execution_processing/postgres_tests.rs"]
mod postgres_tests;
#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::{
        RunEvaluatorCatalogEntry,
        evaluation_plan_for_case,
        evaluator_output_identity_error,
        interpret_measurement,
        make_test_case,
        map_evaluation_status,
        map_severity,
        matching_groups_for_case,
        metadata_from_case_row,
        normalization_policy_hash,
        normalize_measurement,
        persisted_case_expected_output,
        persisted_case_input_payload,
        persisted_case_metadata,
        persisted_case_tags,
        persisted_evaluator_evidence,
        persisted_evaluator_manifest,
        persisted_raw_evaluator_output,
        validate_attempt_allocation_batch,
    };
    use crate::{
        contracts::{
            evaluator::{
                EvaluationStatus,
                EvaluatorIdentity,
                EvaluatorOutcome,
                EvaluatorOutput,
                Measurement,
                Severity,
            },
            run::{
                AgentHttpConfig,
                AgentProfile,
                AggregationMethod,
                AggregationSettings,
                AppliesTo,
                CaseGroupProfile,
                DimensionAggregation,
                EvaluatorBinding,
                NormalizationPolicy,
                NumericMapping,
                PersistRawOutputsMode,
                PersistenceMode,
                PersistenceSettings,
                RunDefaults,
                RunProfile,
                ScorecardGate,
            },
        },
        db::workflows::chunk_processing::WorkerCaseBatchItem,
    };

    fn evaluator_output(
        interface_version: Option<&str>,
        content_hash: Option<&str>,
    ) -> EvaluatorOutput {
        EvaluatorOutput {
            evaluator: EvaluatorIdentity {
                namespace: "vigilo".to_string(),
                name: "quality".to_string(),
                version: "1.0.0".to_string(),
                content_hash: content_hash.map(ToOwned::to_owned),
                interface_version: interface_version.map(ToOwned::to_owned),
            },
            outcome: EvaluatorOutcome::Completed(Measurement::Binary { value: true }),
            diagnostics: Vec::new(),
            metadata: json!({}),
        }
    }

    fn evaluator_catalog_entry() -> RunEvaluatorCatalogEntry {
        RunEvaluatorCatalogEntry {
            evaluator_id: Uuid::nil(),
            evaluator_version: "1.0.0".to_string(),
            content_hash: "artifact-hash".to_string(),
            abi: crate::evaluator_abi::current_identity(),
            evaluator_runtime: "wasmtime".to_string(),
            evaluator_runtime_version: "44.0.0".to_string(),
            evaluator_runtime_fingerprint: "runtime-fingerprint".to_string(),
        }
    }

    #[test]
    fn attempt_allocation_batch_accepts_valid_and_empty_inputs() {
        assert_eq!(
            validate_attempt_allocation_batch(2, 2, 3, 30).unwrap(),
            Some(3)
        );
        assert_eq!(validate_attempt_allocation_batch(0, 0, 0, 0).unwrap(), None);
    }

    #[test]
    fn attempt_allocation_batch_rejects_invalid_policy_and_shape() {
        for error in [
            validate_attempt_allocation_batch(1, 1, 0, 30).unwrap_err(),
            validate_attempt_allocation_batch(1, 1, 1, 0).unwrap_err(),
            validate_attempt_allocation_batch(2, 1, 1, 30).unwrap_err(),
        ] {
            assert!(!error.to_string().is_empty());
        }
    }

    #[test]
    fn evaluator_output_identity_must_match_the_pinned_artifact_and_abi() {
        let entry = evaluator_catalog_entry();
        let evaluator_ref = "vigilo/quality:1.0.0";

        assert!(
            evaluator_output_identity_error(
                &evaluator_output(Some("1.0.0"), Some("artifact-hash")),
                evaluator_ref,
                &entry,
            )
            .is_none()
        );
        assert!(
            evaluator_output_identity_error(
                &evaluator_output(Some("0.1.0"), Some("artifact-hash")),
                evaluator_ref,
                &entry,
            )
            .unwrap()
            .contains("interface version")
        );
        assert!(
            evaluator_output_identity_error(
                &evaluator_output(Some("1.0.0"), Some("different")),
                evaluator_ref,
                &entry,
            )
            .unwrap()
            .contains("content hash")
        );
    }

    fn evaluator_binding(evaluator_ref: &str, dimension: &str) -> EvaluatorBinding {
        EvaluatorBinding {
            id: evaluator_ref.replace(['/', ':'], "_"),
            evaluator_ref: evaluator_ref.to_string(),
            required: true,
            dimension: dimension.to_string(),
            blocking: false,
            weight: 1.0,
            normalization: NormalizationPolicy::Numeric {
                unit: None,
                mapping: NumericMapping::Linear {
                    min: 0.0,
                    max: 1.0,
                    direction: crate::contracts::run::ScoreDirection::HigherIsBetter,
                },
            },
            pass_threshold: 0.8,
            config: json!({"threshold": 0.8}),
        }
    }

    fn aggregation(method: AggregationMethod) -> AggregationSettings {
        AggregationSettings {
            dimensions: [(
                "quality".to_string(),
                DimensionAggregation {
                    method,
                    blocking: false,
                    weight: 1.0,
                },
            )]
            .into_iter()
            .collect(),
        }
    }

    pub(super) fn case_group(
        id: &str,
        task_type: &str,
        tags_any: Vec<&str>,
        evaluator_ref: &str,
        method: AggregationMethod,
    ) -> CaseGroupProfile {
        CaseGroupProfile {
            id: id.to_string(),
            description: format!("{id} group"),
            applies_to: AppliesTo {
                task_type: task_type.to_string(),
                tags_any: tags_any.into_iter().map(ToOwned::to_owned).collect(),
                tags_all: vec![],
            },
            evaluators: vec![evaluator_binding(evaluator_ref, "quality")],
            aggregation: aggregation(method),
        }
    }

    pub(super) fn run_profile(groups: Vec<CaseGroupProfile>) -> RunProfile {
        RunProfile {
            profile_id: "profile".to_string(),
            profile_version: "1.0.0".to_string(),
            description: "test profile".to_string(),
            defaults: RunDefaults {
                max_attempts: 1,
                request_timeout_secs: 30,
                fail_on_any_blocking_failure: true,
                min_execution_score: 0.8,
            },
            persistence: PersistenceSettings {
                mode: PersistenceMode::Full,
                persist_raw_outputs: PersistRawOutputsMode::All,
                persist_evaluator_evidence: true,
            },
            scorecard: Default::default(),
            agent: AgentProfile {
                provider: "example".to_string(),
                name: "agent".to_string(),
                version: None,
                model: None,
                prompt_config_id: None,
                prompt_config_version: None,
                http: AgentHttpConfig {
                    url: "http://127.0.0.1:8787/v1/agent/invoke".to_string(),
                    method: "POST".to_string(),
                    headers: Default::default(),
                    timeout_secs: None,
                    max_request_bytes: None,
                    max_response_bytes: None,
                },
                config: json!({}),
            },
            case_groups: groups,
        }
    }

    pub(super) fn worker_case(case_group: Option<&str>) -> WorkerCaseBatchItem {
        WorkerCaseBatchItem {
            case_id: Uuid::parse_str("018f1111-1111-7111-8111-111111111101").unwrap(),
            case_hash: "case-hash".to_string(),
            case_ordinal: 0,
            task_type: "classification".to_string(),
            case_group: case_group.map(ToOwned::to_owned),
            input_payload: json!({"user_message": "I love this product."}),
            expected_output: json!({"label": "positive"}),
            context_payload: serde_json::Value::Null,
            tags: json!(["sentiment"]),
            metadata: json!({}),
        }
    }

    fn routing_profile() -> RunProfile {
        run_profile(vec![
            case_group(
                "sentiment_classification",
                "classification",
                vec!["sentiment"],
                "vigilo/sentiment-basic-en:0.1.0",
                AggregationMethod::WeightedMean,
            ),
            case_group(
                "json_contract",
                "json_contract",
                vec![],
                "core/json-schema:1.0.0",
                AggregationMethod::MinScore,
            ),
        ])
    }

    #[test]
    fn explicit_case_group_bypasses_task_and_tag_matching() {
        let profile = routing_profile();
        let case = worker_case(Some("json_contract"));

        let groups = matching_groups_for_case(&profile, &case);
        let plan = evaluation_plan_for_case(&profile, &case).unwrap();

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].id, "json_contract");
        assert_eq!(plan.profile_group_id, "json_contract");
        assert_eq!(plan.evaluator_bindings.len(), 1);
        assert_eq!(
            plan.evaluator_bindings[0].evaluator_ref,
            "core/json-schema:1.0.0"
        );
    }

    #[test]
    fn omitted_case_group_uses_task_and_tag_matching() {
        let profile = routing_profile();
        let case = worker_case(None);

        let groups = matching_groups_for_case(&profile, &case);
        let plan = evaluation_plan_for_case(&profile, &case).unwrap();

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].id, "sentiment_classification");
        assert_eq!(plan.profile_group_id, "sentiment_classification");
        assert_eq!(
            plan.evaluator_bindings[0].evaluator_ref,
            "vigilo/sentiment-basic-en:0.1.0"
        );
    }

    #[test]
    fn unknown_explicit_case_group_does_not_fall_back_to_task_tags() {
        let profile = routing_profile();
        let case = worker_case(Some("missing_group"));

        let groups = matching_groups_for_case(&profile, &case);
        let err = evaluation_plan_for_case(&profile, &case).unwrap_err();

        assert!(groups.is_empty());
        assert!(
            err.to_string()
                .contains("did not match any evaluator bindings")
        );
    }

    #[test]
    fn automatic_matching_rejects_conflicting_case_group_policies() {
        let profile = run_profile(vec![
            case_group(
                "quality_weighted",
                "classification",
                vec![],
                "core/quality-a:1.0.0",
                AggregationMethod::WeightedMean,
            ),
            case_group(
                "quality_min",
                "classification",
                vec![],
                "core/quality-b:1.0.0",
                AggregationMethod::MinScore,
            ),
        ]);
        let case = worker_case(None);

        let err = evaluation_plan_for_case(&profile, &case).unwrap_err();

        assert!(err.to_string().contains("conflicting aggregation policy"));
    }

    #[test]
    fn automatic_matching_merges_compatible_groups_without_duplicate_evaluators() {
        let first = case_group(
            "first",
            "classification",
            vec![],
            "core/quality:1.0.0",
            AggregationMethod::WeightedMean,
        );
        let mut second = first.clone();
        second.id = "second".to_string();
        let profile = run_profile(vec![first, second]);

        let plan = evaluation_plan_for_case(&profile, &worker_case(None)).unwrap();

        assert_eq!(plan.profile_group_id, "first,second");
        assert_eq!(plan.evaluator_bindings.len(), 1);
        assert_eq!(plan.aggregation.dimensions.len(), 1);
    }

    #[test]
    fn automatic_matching_rejects_conflicting_duplicate_evaluator_bindings() {
        let first = case_group(
            "first",
            "classification",
            vec![],
            "core/quality:1.0.0",
            AggregationMethod::WeightedMean,
        );
        let mut second = first.clone();
        second.id = "second".to_string();
        second.evaluators[0].blocking = true;
        let profile = run_profile(vec![first, second]);

        let error = evaluation_plan_for_case(&profile, &worker_case(None)).unwrap_err();

        assert!(error.to_string().contains("conflicting evaluator binding"));
    }

    #[test]
    fn make_test_case_preserves_case_group_for_evaluator_input() {
        let case = worker_case(Some("sentiment_classification"));

        let test_case = make_test_case(&case).unwrap();

        assert_eq!(
            test_case.case_group.as_deref(),
            Some("sentiment_classification")
        );
    }

    #[test]
    fn summary_mode_redacts_execution_case_snapshots() {
        let mut profile = routing_profile();
        profile.persistence.mode = PersistenceMode::Summary;
        let case = worker_case(Some("sentiment_classification"));

        let input = persisted_case_input_payload(&profile, &case);
        let expected = persisted_case_expected_output(&profile, &case);
        let metadata = persisted_case_metadata(&profile, &case);
        let tags = persisted_case_tags(&profile, &case.tags);

        assert_eq!(input["field"], json!("input_payload"));
        assert_eq!(expected["field"], json!("expected_output"));
        assert_eq!(metadata["field"], json!("case_metadata"));
        assert_eq!(tags, json!([]));
        for value in [&input, &expected, &metadata] {
            assert_eq!(value["redacted"], json!(true));
            assert_eq!(value["case_hash"], json!("case-hash"));
        }
    }

    #[test]
    fn summary_mode_retains_only_scorecard_tags() {
        let mut profile = routing_profile();
        profile.persistence.mode = PersistenceMode::Summary;
        profile.scorecard.gates.push(ScorecardGate {
            id: "release".to_string(),
            dimension: "quality".to_string(),
            binding_id: None,
            case_group: None,
            tags_all: vec!["release".to_string()],
            min_mean_score: Some(0.8),
            score_threshold: None,
            min_pass_rate: None,
            min_coverage: None,
            max_error_rate: None,
            max_abstention_rate: None,
        });

        assert_eq!(
            persisted_case_tags(&profile, &json!(["private", "release"])),
            json!(["release"])
        );
    }

    #[test]
    fn full_mode_preserves_execution_case_snapshots() {
        let profile = routing_profile();
        let case = worker_case(Some("sentiment_classification"));

        assert_eq!(
            persisted_case_input_payload(&profile, &case),
            case.input_payload
        );
        assert_eq!(
            persisted_case_expected_output(&profile, &case),
            case.expected_output
        );
        assert_eq!(persisted_case_metadata(&profile, &case), case.metadata);
        assert_eq!(persisted_case_tags(&profile, &case.tags), case.tags);
    }

    #[test]
    fn summary_mode_redacts_evaluator_binding_config_from_manifest() {
        let mut profile = routing_profile();
        profile.persistence.mode = PersistenceMode::Summary;
        let plan = evaluation_plan_for_case(&profile, &worker_case(None)).unwrap();

        let manifest = persisted_evaluator_manifest(&profile, &plan.evaluator_bindings).unwrap();

        assert_eq!(manifest[0]["ref"], json!("vigilo/sentiment-basic-en:0.1.0"));
        assert_eq!(manifest[0]["id"], json!("vigilo_sentiment-basic-en_0.1.0"));
        assert_eq!(manifest[0]["required"], json!(true));
        assert_eq!(manifest[0]["pass_threshold"], json!(0.8));
        assert_eq!(manifest[0]["normalization"]["method"], json!("numeric"));
        assert_eq!(
            manifest[0]["normalization"]["mapping"]["type"],
            json!("linear")
        );
        assert_eq!(manifest[0]["config"]["redacted"], json!(true));
    }

    #[test]
    fn raw_output_policy_keeps_only_failed_or_error_invocations_for_failures_only() {
        let mut profile = routing_profile();
        profile.persistence.persist_raw_outputs = PersistRawOutputsMode::FailuresOnly;

        let passed = persisted_raw_evaluator_output(
            &profile.persistence,
            &EvaluationStatus::Passed,
            json!({"raw": "kept"}),
        );
        let failed = persisted_raw_evaluator_output(
            &profile.persistence,
            &EvaluationStatus::Failed,
            json!({"raw": "kept"}),
        );
        let error = persisted_raw_evaluator_output(
            &profile.persistence,
            &EvaluationStatus::Error,
            json!({"raw": "kept"}),
        );

        assert_eq!(passed["redacted"], json!(true));
        assert_eq!(failed, json!({"raw": "kept"}));
        assert_eq!(error, json!({"raw": "kept"}));
    }

    #[test]
    fn raw_output_none_policy_redacts_every_status() {
        let mut profile = routing_profile();
        profile.persistence.persist_raw_outputs = PersistRawOutputsMode::None;

        for status in [
            EvaluationStatus::Passed,
            EvaluationStatus::Failed,
            EvaluationStatus::Error,
            EvaluationStatus::Abstained,
        ] {
            let raw = persisted_raw_evaluator_output(
                &profile.persistence,
                &status,
                json!({"raw": "sensitive"}),
            );
            assert_eq!(raw["redacted"], json!(true));
        }
    }

    #[test]
    fn evidence_policy_redacts_finding_evidence_when_disabled() {
        let mut profile = routing_profile();
        profile.persistence.persist_evaluator_evidence = false;

        let evidence =
            persisted_evaluator_evidence(&profile.persistence, json!({"span": "sensitive"}));

        assert_eq!(evidence["redacted"], json!(true));
        assert_eq!(
            evidence["reason"],
            json!("persistence.persist_evaluator_evidence=false")
        );
    }

    #[test]
    fn evidence_policy_redacts_raw_output_to_avoid_embedded_evidence_leaks() {
        let mut profile = routing_profile();
        profile.persistence.persist_raw_outputs = PersistRawOutputsMode::All;
        profile.persistence.persist_evaluator_evidence = false;

        let raw = persisted_raw_evaluator_output(
            &profile.persistence,
            &EvaluationStatus::Failed,
            json!({"evidence": {"span": "sensitive"}}),
        );

        assert_eq!(raw["redacted"], json!(true));
        assert_eq!(
            raw["reason"],
            json!("persistence.persist_evaluator_evidence=false")
        );
    }

    #[test]
    fn evaluator_case_metadata_must_be_an_object() {
        assert!(metadata_from_case_row(&json!({"source": "fixture"})).is_ok());
        assert!(metadata_from_case_row(&json!(["invalid"])).is_err());
    }

    #[test]
    fn evaluator_persistence_mappings_cover_contract_variants() {
        for (status, expected) in [
            (EvaluationStatus::Passed, "passed"),
            (EvaluationStatus::Failed, "failed"),
            (EvaluationStatus::Error, "error"),
            (EvaluationStatus::Abstained, "abstained"),
        ] {
            assert_eq!(map_evaluation_status(&status), expected);
        }

        for (severity, expected) in [
            (Severity::None, "none"),
            (Severity::Low, "low"),
            (Severity::Medium, "medium"),
            (Severity::High, "high"),
            (Severity::Critical, "critical"),
        ] {
            assert_eq!(map_severity(&severity), expected);
        }
    }

    #[test]
    fn normalization_rejects_measurements_that_do_not_match_host_policy() {
        let error = normalize_measurement(
            &NormalizationPolicy::Binary {
                false_score: 0.0,
                true_score: 1.0,
            },
            &Measurement::Numeric {
                value: 1.0,
                unit: None,
            },
        )
        .unwrap_err();

        assert!(error.to_string().contains("incompatible"));
    }

    #[test]
    fn normalization_rejects_out_of_domain_values_instead_of_clamping() {
        let error = normalize_measurement(
            &NormalizationPolicy::Numeric {
                unit: None,
                mapping: NumericMapping::Linear {
                    min: 0.0,
                    max: 1.0,
                    direction: crate::contracts::run::ScoreDirection::HigherIsBetter,
                },
            },
            &Measurement::Numeric {
                value: 2.0,
                unit: None,
            },
        )
        .unwrap_err();

        assert!(error.to_string().contains("outside the configured domain"));
    }

    #[test]
    fn host_threshold_derives_judgment_from_measurement() {
        let measurement = Measurement::Numeric {
            value: 0.8,
            unit: None,
        };
        let policy = NormalizationPolicy::Numeric {
            unit: None,
            mapping: NumericMapping::Linear {
                min: 0.0,
                max: 1.0,
                direction: crate::contracts::run::ScoreDirection::HigherIsBetter,
            },
        };

        assert_eq!(
            interpret_measurement(&policy, 0.8, &measurement).unwrap(),
            (0.8, EvaluationStatus::Passed)
        );
        assert_eq!(
            interpret_measurement(&policy, 0.81, &measurement).unwrap(),
            (0.8, EvaluationStatus::Failed)
        );
    }

    #[test]
    fn normalization_policy_hash_is_stable_and_policy_specific() {
        let mut policy = NormalizationPolicy::Binary {
            false_score: 0.0,
            true_score: 1.0,
        };
        let original = normalization_policy_hash(&policy).unwrap();

        assert_eq!(normalization_policy_hash(&policy).unwrap(), original);
        policy = NormalizationPolicy::Binary {
            false_score: 0.2,
            true_score: 1.0,
        };
        assert_ne!(normalization_policy_hash(&policy).unwrap(), original);
    }
}
