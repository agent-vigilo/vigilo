//! Loading, validation, and resolution of workload registries and profiles.
//!
//! Registry entries define semantic workload contracts; profiles select exact
//! registered tuples and execution budgets. Resolution is strict: unknown,
//! out-of-profile, or unavailable selections fail rather than being skipped.

use std::{
    collections::BTreeSet,
    fs,
    path::Path,
};

use anyhow::{
    Context,
    Result,
    bail,
};

use super::model::{
    BUDGET_SCHEMA,
    BudgetPolicy,
    PROFILE_SCHEMA,
    Profile,
    ProfileWorkload,
    REGISTRY_SCHEMA,
    ScalingKind,
    Workload,
    WorkloadRegistry,
};

/// A profile selection paired with its resolved registry contract.
pub struct SelectedWorkload<'a> {
    /// Profile-specific tuple, block count, and timing policy.
    pub profile: &'a ProfileWorkload,
    /// Registry-owned workload definition and execution requirements.
    pub workload: &'a Workload,
}

/// Loads and validates the workspace's versioned MVP workload registry.
pub fn load_registry(root: &Path) -> Result<WorkloadRegistry> {
    let path = root.join("performance/registry/workloads-v1.toml");
    let registry: WorkloadRegistry = toml::from_str(
        &fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?,
    )
    .with_context(|| format!("parse {}", path.display()))?;
    validate_registry(&registry)?;
    Ok(registry)
}

/// Loads and validates a named profile from `performance/profiles`.
pub fn load_profile(root: &Path, id: &str) -> Result<Profile> {
    validate_file_id(id, "profile")?;
    let path = root
        .join(super::default_profile_dir())
        .join(format!("{id}.toml"));
    let profile: Profile = toml::from_str(
        &fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?,
    )
    .with_context(|| format!("parse {}", path.display()))?;
    if profile.id != id {
        bail!("profile file identity {} does not match {id}", profile.id);
    }
    validate_profile(&profile)?;
    Ok(profile)
}

/// Loads and validates one reviewed performance-budget policy.
pub fn load_budget_policy(root: &Path, id: &str) -> Result<BudgetPolicy> {
    validate_file_id(id, "budget")?;
    let path = root.join("performance/budgets").join(format!("{id}.toml"));
    let policy: BudgetPolicy = toml::from_str(
        &fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?,
    )
    .with_context(|| format!("parse {}", path.display()))?;
    if policy.id != id {
        bail!("budget file identity {} does not match {id}", policy.id);
    }
    validate_budget_policy(&policy)?;
    Ok(policy)
}

/// Validates registry schema, uniqueness, and required execution metadata.
pub fn validate_registry(registry: &WorkloadRegistry) -> Result<()> {
    if registry.schema_id != REGISTRY_SCHEMA {
        bail!("unsupported registry schema: {}", registry.schema_id);
    }
    if registry.revision == 0 {
        bail!("registry revision must be positive");
    }
    if registry.extra.keys().any(String::is_empty) {
        bail!("registry extension keys cannot be empty");
    }
    let mut ids = BTreeSet::new();
    for workload in &registry.workloads {
        if !ids.insert(&workload.id) {
            bail!("duplicate workload ID: {}", workload.id);
        }
        if workload.tuples.is_empty()
            || workload.watchdog_ms == 0
            || workload.planning_duration_ms == 0
        {
            bail!(
                "workload {} has an incomplete execution contract",
                workload.id
            );
        }
        if workload.owner.is_empty()
            || workload.capability.is_empty()
            || workload.fixture.is_empty()
            || workload.unit.is_empty()
            || workload.oracle.is_empty()
            || workload.required_metrics.is_empty()
        {
            bail!(
                "workload {} has incomplete ownership or measurement metadata",
                workload.id
            );
        }
        let tuples = workload.tuples.iter().collect::<BTreeSet<_>>();
        if tuples.len() != workload.tuples.len() {
            bail!("workload {} contains a duplicate tuple", workload.id);
        }
        for (tuple, ceiling) in &workload.statement_budgets {
            if *ceiling == 0
                || !workload.tuples.contains(tuple)
                || !workload.required_metrics.iter().any(|m| m == "sql")
            {
                bail!(
                    "workload {} has an invalid statement budget for {tuple}",
                    workload.id
                );
            }
        }
        let Some((unit, oracle)) = driver_contract(&workload.id) else {
            bail!("workload {} has no registered harness driver", workload.id);
        };
        if workload.unit != unit || workload.oracle != oracle {
            bail!(
                "workload {} unit or oracle does not match its harness driver",
                workload.id
            );
        }
        let mut metrics = BTreeSet::new();
        for metric in &workload.required_metrics {
            if !metrics.insert(metric) {
                bail!(
                    "workload {} contains a duplicate required metric",
                    workload.id
                );
            }
            if !is_supported_metric(metric) {
                bail!(
                    "workload {} requires unsupported metric {metric}",
                    workload.id
                );
            }
        }
        if workload.extra.keys().any(String::is_empty) {
            bail!("workload {} has an empty extension key", workload.id);
        }
        if workload.scaling_model.is_some() {
            validate_scaling_model(workload)?;
        }
        if let Some(contract) = &workload.reliability {
            validate_reliability_contract(workload, contract)?;
        }
    }
    Ok(())
}

fn validate_reliability_contract(
    workload: &Workload,
    contract: &super::model::ReliabilityContract,
) -> Result<()> {
    if contract.duration_secs == 0
        || contract.observation_interval_secs == 0
        || contract.observation_interval_secs > contract.duration_secs
        || contract.recovery_deadline_secs == 0
        || contract.max_process_rss_bytes == 0
        || !(contract.min_throughput_retention.is_finite()
            && contract.min_throughput_retention > 0.0
            && contract.min_throughput_retention <= 1.0)
        || !(contract.max_attempts_per_case.is_finite() && contract.max_attempts_per_case >= 1.0)
        || !(contract.max_deliveries_per_chunk.is_finite()
            && contract.max_deliveries_per_chunk >= 1.0)
    {
        bail!(
            "workload {} has an invalid reliability contract",
            workload.id
        );
    }
    Ok(())
}

/// Validates that a scaling model is identifiable and covers its workload exactly.
pub fn validate_scaling_model(workload: &Workload) -> Result<()> {
    let model = workload
        .scaling_model
        .as_ref()
        .context("scaling-model validation requires a model")?;
    if model.input_dimension.is_empty()
        || !(model.max_residual_fraction.is_finite()
            && model.max_residual_fraction > 0.0
            && model.max_residual_fraction <= 1.0)
    {
        bail!("workload {} has invalid scaling metadata", workload.id);
    }
    let minimum_points = match model.kind {
        ScalingKind::FixedPlusSlope => 3,
        ScalingKind::Stepped => 2,
    };
    if model.points.len() < minimum_points {
        bail!(
            "workload {} scaling model needs at least {minimum_points} points",
            workload.id
        );
    }
    if model.kind == ScalingKind::Stepped && model.discontinuities.is_empty() {
        bail!(
            "workload {} stepped model has no discontinuity",
            workload.id
        );
    }
    if model.kind == ScalingKind::FixedPlusSlope && !model.discontinuities.is_empty() {
        bail!(
            "workload {} fixed-plus-slope model cannot declare discontinuities",
            workload.id
        );
    }
    let tuples = workload.tuples.iter().collect::<BTreeSet<_>>();
    let point_tuples = model
        .points
        .iter()
        .map(|point| &point.tuple)
        .collect::<BTreeSet<_>>();
    let inputs = model
        .points
        .iter()
        .map(|point| point.input)
        .collect::<BTreeSet<_>>();
    if tuples != point_tuples || point_tuples.len() != model.points.len() {
        bail!(
            "workload {} model must cover every tuple exactly once",
            workload.id
        );
    }
    if inputs.len() != model.points.len()
        || model
            .points
            .iter()
            .any(|point| point.input == 0 || point.exact.is_empty())
    {
        bail!(
            "workload {} has duplicate or incomplete scaling points",
            workload.id
        );
    }
    if model
        .points
        .iter()
        .flat_map(|point| point.exact.keys())
        .any(String::is_empty)
    {
        bail!(
            "workload {} has an empty exact-observation key",
            workload.id
        );
    }
    if model
        .discontinuities
        .iter()
        .any(|boundary| !inputs.contains(boundary))
    {
        bail!(
            "workload {} discontinuities must name measured points",
            workload.id
        );
    }
    Ok(())
}

/// Validates profile schema, limits, block balance, and timing policies.
pub fn validate_profile(profile: &Profile) -> Result<()> {
    if profile.schema_id != PROFILE_SCHEMA {
        bail!("unsupported profile schema: {}", profile.schema_id);
    }
    if profile.id.is_empty()
        || profile.description.is_empty()
        || profile.workloads.is_empty()
        || profile.campaign_cap_secs == 0
        || profile.max_artifact_bytes == 0
        || profile.max_stdout_bytes == 0
        || profile.max_stderr_bytes == 0
    {
        bail!(
            "profile {} has an incomplete execution contract",
            profile.id
        );
    }
    if profile.extra.keys().any(String::is_empty) {
        bail!("profile {} has an empty extension key", profile.id);
    }
    if profile
        .max_residual_orientation_effect
        .is_some_and(|value| !value.is_finite() || value <= 0.0)
    {
        bail!("profile {} has an invalid orientation limit", profile.id);
    }
    let mut selections = BTreeSet::new();
    let mut timing_modes = BTreeSet::new();
    for workload in &profile.workloads {
        if !selections.insert((&workload.id, &workload.tuple)) {
            bail!(
                "profile {} contains duplicate workload tuple {}:{}",
                profile.id,
                workload.id,
                workload.tuple
            );
        }
        timing_modes.insert(workload.timing.as_str());
        let reliability = matches!(workload.timing.as_str(), "soak" | "recovery");
        if workload.blocks == 0 || (!reliability && workload.blocks % 2 != 0) {
            bail!(
                "profile {} workload {} must declare a positive even block count",
                profile.id,
                workload.id
            );
        }
        if !matches!(
            workload.timing.as_str(),
            "informative" | "calibration" | "capacity" | "gating" | "soak" | "recovery"
        ) {
            bail!("unsupported timing policy: {}", workload.timing);
        }
    }
    if timing_modes.len() != 1 {
        bail!("profile {} cannot mix timing modes", profile.id);
    }
    let gating = profile
        .workloads
        .iter()
        .filter(|workload| workload.timing == "gating")
        .count();
    if gating > 0 && (gating != profile.workloads.len() || profile.budget_reference.is_none()) {
        bail!("gating profiles require one budget policy and cannot mix timing modes");
    }
    if gating == 0 && profile.budget_reference.is_some() {
        bail!("only gating profiles may reference a budget policy");
    }
    let reliability = profile
        .workloads
        .iter()
        .filter(|workload| matches!(workload.timing.as_str(), "soak" | "recovery"))
        .count();
    if reliability > 0
        && (reliability != profile.workloads.len()
            || profile
                .workloads
                .iter()
                .any(|workload| workload.blocks != 1))
    {
        bail!(
            "reliability profiles require exactly one observation per workload and cannot mix timing modes"
        );
    }
    if let Some(reference) = profile.budget_reference.as_deref() {
        validate_file_id(reference, "budget")?;
    }
    Ok(())
}

/// Validates reviewed budget identity, uniqueness, and finite positive gates.
pub fn validate_budget_policy(policy: &BudgetPolicy) -> Result<()> {
    if policy.schema_id != BUDGET_SCHEMA
        || policy.id.is_empty()
        || policy.environment_id.is_empty()
        || policy.calibration_id.is_empty()
        || policy.approved_at.is_empty()
        || policy.approved_by.is_empty()
        || policy.entries.is_empty()
    {
        bail!("budget policy has an incompatible or incomplete contract");
    }
    validate_file_id(&policy.id, "budget")?;
    let mut entries = BTreeSet::new();
    for entry in &policy.entries {
        if entry.workload_id.is_empty()
            || entry.tuple_id.is_empty()
            || entry.metric.is_empty()
            || !(entry.practical_budget.is_finite() && entry.practical_budget > 0.0)
            || entry.minimum_blocks == 0
            || !entry.minimum_blocks.is_multiple_of(2)
            || !(entry.max_residual_orientation_effect.is_finite()
                && entry.max_residual_orientation_effect > 0.0)
        {
            bail!("budget policy contains an invalid entry");
        }
        if !entries.insert((&entry.workload_id, &entry.tuple_id, &entry.metric)) {
            bail!("budget policy contains a duplicate workload metric");
        }
    }
    Ok(())
}

fn validate_file_id(id: &str, kind: &str) -> Result<()> {
    if id.is_empty()
        || !id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        bail!("invalid {kind} ID: {id}");
    }
    Ok(())
}

/// Proves that every profile selection names a registered workload tuple.
pub fn validate_profile_registry(profile: &Profile, registry: &WorkloadRegistry) -> Result<()> {
    for profile_workload in &profile.workloads {
        let workload = registry
            .workloads
            .iter()
            .find(|workload| workload.id == profile_workload.id)
            .with_context(|| {
                format!(
                    "profile {} references unknown workload {}",
                    profile.id, profile_workload.id
                )
            })?;
        if !workload.tuples.contains(&profile_workload.tuple) {
            bail!(
                "profile {} references unknown tuple {} for {}",
                profile.id,
                profile_workload.tuple,
                workload.id
            );
        }
    }
    Ok(())
}

/// Resolves optional workload filters against an already validated profile.
pub fn select_workloads<'a>(
    profile: &'a Profile,
    registry: &'a WorkloadRegistry,
    requested: &[String],
) -> Result<Vec<SelectedWorkload<'a>>> {
    validate_profile_registry(profile, registry)?;
    if profile.requires_workload_selection && requested.is_empty() {
        bail!("profile {} requires at least one --workload", profile.id);
    }
    let requested: BTreeSet<_> = requested.iter().collect();
    for id in &requested {
        if !profile
            .workloads
            .iter()
            .any(|workload| workload.id == id.as_str())
        {
            bail!("workload {id} is not in profile {}", profile.id);
        }
    }
    let mut selected = Vec::new();
    for profile_workload in &profile.workloads {
        if !requested.is_empty() && !requested.contains(&profile_workload.id) {
            continue;
        }
        let workload = registry
            .workloads
            .iter()
            .find(|workload| workload.id == profile_workload.id)
            .expect("profile registry validation must resolve workload");
        selected.push(SelectedWorkload {
            profile: profile_workload,
            workload,
        });
    }
    Ok(selected)
}

/// Estimates comparison campaign time from declared planning durations.
pub fn estimated_compare_millis(selected: &[SelectedWorkload<'_>]) -> Result<u64> {
    estimate_millis(selected, 4, 2, false)
}

/// Estimates single-binary campaign time from declared planning durations.
pub fn estimated_single_millis(selected: &[SelectedWorkload<'_>]) -> Result<u64> {
    estimate_millis(selected, 2, 1, true)
}

fn estimate_millis(
    selected: &[SelectedWorkload<'_>],
    executions_per_block: u64,
    precondition_executions: u64,
    reliability_is_single: bool,
) -> Result<u64> {
    selected.iter().try_fold(0_u64, |total, selected| {
        let executions = if reliability_is_single && is_reliability_timing(&selected.profile.timing)
        {
            1
        } else {
            u64::from(selected.profile.blocks)
                .checked_mul(executions_per_block)
                .context("performance execution-count estimate overflowed")?
        };
        let measured = executions
            .checked_mul(selected.workload.planning_duration_ms)
            .context("performance measured-duration estimate overflowed")?;
        let precondition =
            if selected.workload.preconditioning == super::model::Preconditioning::OnePerBinary {
                precondition_executions
                    .checked_mul(selected.workload.planning_duration_ms)
                    .context("performance precondition-duration estimate overflowed")?
            } else {
                0
            };
        total
            .checked_add(measured)
            .and_then(|total| total.checked_add(precondition))
            .context("performance campaign-duration estimate overflowed")
    })
}

/// Returns the semantic unit and exact oracle implemented by a workload driver.
fn driver_contract(workload_id: &str) -> Option<(&'static str, &'static str)> {
    Some(match workload_id {
        "startup.cli-help.v1" => ("process_start", "exit_0_and_help_signature"),
        "run.create.v1" | "run.create-scaling.v1" | "run.create-ingestion.v1" => {
            ("case_and_run", "exact_control_and_execution_rows")
        }
        "coordinator.dispatch.v1" => ("cycle_and_chunk", "exact_dispatch_and_queue_state"),
        "worker.execute-wasm.v1" => (
            "useful_case_and_evaluation",
            "exact_attempt_result_and_chunk_state",
        ),
        "system.lifecycle.v1" => (
            "useful_case_chunk_and_run",
            "exact_completed_run_and_drained_work",
        ),
        "system.capacity.v1" => (
            "useful_case_per_second",
            "exact_completed_run_and_drained_work",
        ),
        "system.soak.v1" => (
            "useful_case_interval",
            "exact_terminal_work_drained_queue_and_bounded_resources",
        ),
        "system.recovery.v1" => (
            "recovered_dependency_and_useful_case",
            "exact_pre_and_post_fault_work_with_bounded_reconnect",
        ),
        "coordinator.dispatch-scaling.v1" => ("chunk", "exact_dispatch_and_queue_state"),
        "agent.http-scaling.v1" | "agent.http-variants.v1" => {
            ("agent_request", "exact_agent_requests_and_terminal_results")
        }
        "evaluator.wasm-scaling.v1" => {
            ("evaluation", "exact_evaluator_results_and_terminal_attempt")
        }
        "worker.persistence-scaling.v1" => ("case_result", "exact_attempt_result_and_chunk_state"),
        "coordinator.outbox-scaling.v1" => (
            "published_delivery",
            "exact_outbox_delivery_and_queue_state",
        ),
        "database.route-cache.v1" => ("route_lookup", "exact_status_projection"),
        "coordinator.recovery.v1" => ("recovered_lease", "exact_recovery_attempt_and_redelivery"),
        "coordinator.finalization.v1" => ("finalized_run", "exact_terminal_summary_and_event"),
        "run.cancel-scaling.v1" => (
            "cancelled_execution_and_route",
            "exact_cancelled_run_rows_and_idempotent_replay",
        ),
        "run.read.v1" => (
            "returned_execution_summary",
            "exact_terminal_status_and_result_counts",
        ),
        "run.export.v1" => (
            "serialized_execution",
            "exact_export_record_types_and_counts",
        ),
        "shard.move.v1" => (
            "copied_row_page_and_byte",
            "exact_route_switch_checksums_and_page_distribution",
        ),
        "shard.rebalance.v1" => (
            "moved_and_verified_shard",
            "exact_completed_rebalance_items_and_routes",
        ),
        "coordinator.placement-scaling.v1" => {
            ("logical_placement", "exact_bounded_cycle_and_dispatch")
        }
        "run.create-boundaries.v1" => (
            "case_and_creation_page",
            "exact_creation_progress_at_every_page_boundary",
        ),
        _ => return None,
    })
}

fn is_supported_metric(metric: &str) -> bool {
    matches!(
        metric,
        "wall_time"
            | "child_cpu"
            | "process_cpu"
            | "peak_rss"
            | "executable_bytes"
            | "service_cpu"
            | "sql"
            | "wal"
            | "http"
            | "queue"
            | "throughput"
            | "file_descriptors"
            | "recovery_time"
    )
}

/// Returns whether a timing policy represents one bounded reliability observation.
pub fn is_reliability_timing(timing: &str) -> bool {
    matches!(timing, "soak" | "recovery")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn odd_block_count_is_rejected() {
        let profile: Profile = toml::from_str(
            r#"
schema_id = "profile/v1"
id = "bad-v1"
description = "bad"
requires_workload_selection = false
campaign_cap_secs = 1
schedule_seed = 1
max_artifact_bytes = 1
max_stdout_bytes = 1
max_stderr_bytes = 1
[[workloads]]
id = "startup.cli-help.v1"
tuple = "cold-help"
blocks = 3
timing = "informative"
"#,
        )
        .unwrap();
        assert!(validate_profile(&profile).is_err());
    }

    #[test]
    fn repository_profiles_resolve_strict_selections_and_estimates() {
        let root = crate::perf::artifact::workspace_root().unwrap();
        let registry = load_registry(&root).unwrap();
        let developer = load_profile(&root, "developer-v1").unwrap();
        assert!(load_profile(&root, "../escape").is_err());
        assert!(select_workloads(&developer, &registry, &[]).is_err());
        assert!(select_workloads(&developer, &registry, &["run.create.v1".into()]).is_err());

        let selected =
            select_workloads(&developer, &registry, &["startup.cli-help.v1".into()]).unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(estimated_single_millis(&selected).unwrap(), 5_000);
        assert_eq!(estimated_compare_millis(&selected).unwrap(), 10_000);

        let pr = load_profile(&root, "pr-v1").unwrap();
        assert_eq!(select_workloads(&pr, &registry, &[]).unwrap().len(), 4);

        let capacity = load_profile(&root, "capacity-v1").unwrap();
        assert_eq!(
            select_workloads(&capacity, &registry, &[]).unwrap().len(),
            10
        );

        let soak = load_profile(&root, "soak-v1").unwrap();
        let selected = select_workloads(&soak, &registry, &[]).unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(estimated_single_millis(&selected).unwrap(), 1_800_000);
        assert!(is_reliability_timing(&selected[0].profile.timing));
    }

    #[test]
    fn registry_and_profile_validation_reject_contract_drift() {
        let root = crate::perf::artifact::workspace_root().unwrap();
        let registry = load_registry(&root).unwrap();
        let profile = load_profile(&root, "pr-v1").unwrap();

        let mut invalid = registry.clone();
        invalid.schema_id = "workload-registry/v2".into();
        assert!(validate_registry(&invalid).is_err());
        let mut invalid = registry.clone();
        invalid.revision = 0;
        assert!(validate_registry(&invalid).is_err());
        let mut invalid = registry.clone();
        invalid.workloads.push(invalid.workloads[0].clone());
        assert!(validate_registry(&invalid).is_err());
        let mut invalid = registry.clone();
        invalid.workloads[0].tuples.clear();
        assert!(validate_registry(&invalid).is_err());
        let mut invalid = registry.clone();
        invalid.workloads[0].owner.clear();
        assert!(validate_registry(&invalid).is_err());
        let mut invalid = registry.clone();
        let duplicate = invalid.workloads[0].tuples[0].clone();
        invalid.workloads[0].tuples.push(duplicate);
        assert!(validate_registry(&invalid).is_err());
        let mut invalid = registry.clone();
        invalid.workloads[0].oracle = "misspelled_oracle".into();
        assert!(validate_registry(&invalid).is_err());
        let mut invalid = registry.clone();
        invalid.workloads[0].required_metrics.push("unknown".into());
        assert!(validate_registry(&invalid).is_err());
        let mut invalid = registry.clone();
        let duplicate = invalid.workloads[0].required_metrics[0].clone();
        invalid.workloads[0].required_metrics.push(duplicate);
        assert!(validate_registry(&invalid).is_err());

        let mut invalid = profile.clone();
        invalid.schema_id = "profile/v2".into();
        assert!(validate_profile(&invalid).is_err());
        let mut invalid = profile.clone();
        invalid.max_stdout_bytes = 0;
        assert!(validate_profile(&invalid).is_err());
        let mut invalid = profile.clone();
        invalid.workloads[0].timing = "unknown".into();
        assert!(validate_profile(&invalid).is_err());
        let mut invalid = profile.clone();
        invalid.workloads.push(invalid.workloads[0].clone());
        assert!(validate_profile(&invalid).is_err());
        let mut invalid = profile.clone();
        invalid.workloads[0].id = "unknown.v1".into();
        assert!(validate_profile_registry(&invalid, &registry).is_err());
        let mut invalid = profile;
        invalid.workloads[0].tuple = "unknown".into();
        assert!(validate_profile_registry(&invalid, &registry).is_err());

        let mut invalid = load_profile(&root, "soak-v1").unwrap();
        invalid.workloads[0].blocks = 2;
        assert!(validate_profile(&invalid).is_err());

        let mut invalid = load_profile(&root, "capacity-v1").unwrap();
        invalid.workloads[0].timing = "informative".into();
        assert!(validate_profile(&invalid).is_err());

        let mut invalid = registry.clone();
        invalid
            .workloads
            .iter_mut()
            .find(|workload| workload.id == "system.soak.v1")
            .unwrap()
            .reliability
            .as_mut()
            .unwrap()
            .duration_secs = 0;
        assert!(validate_registry(&invalid).is_err());
    }

    #[test]
    fn profile_file_identity_must_match_requested_id() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join(super::super::default_profile_dir());
        fs::create_dir_all(&directory).unwrap();
        fs::write(
            directory.join("requested-v1.toml"),
            r#"
schema_id = "profile/v1"
id = "different-v1"
description = "identity mismatch"
requires_workload_selection = false
campaign_cap_secs = 1
schedule_seed = 1
max_artifact_bytes = 1
max_stdout_bytes = 1
max_stderr_bytes = 1
[[workloads]]
id = "startup.cli-help.v1"
tuple = "cold-help"
blocks = 2
timing = "informative"
"#,
        )
        .unwrap();

        assert!(load_profile(root.path(), "requested-v1").is_err());
    }

    #[test]
    fn duration_estimates_reject_arithmetic_overflow() {
        let root = crate::perf::artifact::workspace_root().unwrap();
        let mut registry = load_registry(&root).unwrap();
        let profile = load_profile(&root, "developer-v1").unwrap();
        registry.workloads[0].planning_duration_ms = u64::MAX;
        let selected =
            select_workloads(&profile, &registry, &["startup.cli-help.v1".into()]).unwrap();

        assert!(estimated_single_millis(&selected).is_err());
        assert!(estimated_compare_millis(&selected).is_err());
    }

    #[test]
    fn gating_profiles_and_budget_policies_fail_closed() {
        let root = crate::perf::artifact::workspace_root().unwrap();
        let mut profile = load_profile(&root, "developer-v1").unwrap();
        profile.workloads[0].timing = "gating".into();
        assert!(validate_profile(&profile).is_err());
        profile.budget_reference = Some("reference-v2-budgets".into());
        assert!(validate_profile(&profile).is_ok());

        let mut policy: super::super::model::BudgetPolicy = toml::from_str(
            r#"
schema_id = "performance-budget/v1"
id = "reference-v2-budgets"
environment_id = "aws-m6i-2xlarge-al2023-v1"
calibration_id = "calibration-1"
approved_at = "2026-08-21T00:00:00Z"
approved_by = "reviewer"
[[entries]]
workload_id = "startup.cli-help.v1"
tuple_id = "cold-help"
metric = "wall_time"
practical_budget = 0.05
minimum_blocks = 6
max_residual_orientation_effect = 0.02
"#,
        )
        .unwrap();
        assert!(validate_budget_policy(&policy).is_ok());
        policy.entries.push(policy.entries[0].clone());
        assert!(validate_budget_policy(&policy).is_err());
        policy.entries.pop();
        policy.entries[0].practical_budget = 0.0;
        assert!(validate_budget_policy(&policy).is_err());
    }

    #[test]
    fn scalable_models_cover_every_tuple_and_exact_observation() {
        let root = crate::perf::artifact::workspace_root().unwrap();
        let registry = load_registry(&root).unwrap();
        let scalable = registry
            .workloads
            .iter()
            .filter(|workload| workload.scaling_model.is_some())
            .collect::<Vec<_>>();
        assert!(!scalable.is_empty());
        for workload in scalable {
            validate_scaling_model(workload).unwrap();
            let model = workload.scaling_model.as_ref().unwrap();
            assert_eq!(model.points.len(), workload.tuples.len());
            assert!(model.points.iter().all(|point| !point.exact.is_empty()));
        }
    }

    #[test]
    fn scalable_models_reject_missing_points_and_weak_fits() {
        let root = crate::perf::artifact::workspace_root().unwrap();
        let registry = load_registry(&root).unwrap();
        let mut workload = registry
            .workloads
            .iter()
            .find(|workload| workload.scaling_model.is_some())
            .unwrap()
            .clone();

        workload.scaling_model.as_mut().unwrap().points.pop();
        assert!(validate_scaling_model(&workload).is_err());

        let model = workload.scaling_model.as_mut().unwrap();
        model.points = model.points.iter().take(2).cloned().collect();
        workload.tuples = model
            .points
            .iter()
            .map(|point| point.tuple.clone())
            .collect();
        assert!(validate_scaling_model(&workload).is_err());
    }

    #[test]
    fn repository_schedules_admin_and_large_data_contracts_as_informative() {
        let root = crate::perf::artifact::workspace_root().unwrap();
        let registry = load_registry(&root).unwrap();
        let profile = load_profile(&root, "admin-nightly-v1").unwrap();
        let expected = [
            "run.cancel-scaling.v1",
            "run.read.v1",
            "run.export.v1",
            "shard.move.v1",
            "shard.rebalance.v1",
            "coordinator.placement-scaling.v1",
            "run.create-boundaries.v1",
        ];

        for id in expected {
            let workload = registry
                .workloads
                .iter()
                .find(|workload| workload.id == id)
                .unwrap_or_else(|| panic!("missing registered workload {id}"));
            let selected = profile
                .workloads
                .iter()
                .filter(|selection| selection.id == id)
                .collect::<Vec<_>>();
            assert_eq!(
                selected.len(),
                workload.tuples.len(),
                "profile coverage for {id}"
            );
            assert!(
                selected
                    .iter()
                    .all(|selection| selection.timing == "informative")
            );
        }

        validate_profile_registry(&profile, &registry).unwrap();
    }
}
