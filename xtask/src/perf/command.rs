//! Single-binary and baseline/candidate campaign orchestration.
//!
//! Commands resolve an explicit profile, reject unavailable workloads before
//! provisioning, verify immutable build manifests and common capabilities, and
//! execute bounded schedules. They checkpoint raw samples and block validity
//! before deriving comparison and report documents.

use std::{
    fs,
    path::{
        Component,
        Path,
        PathBuf,
    },
    time::{
        Duration,
        Instant,
    },
};

use anyhow::{
    Context,
    Result,
    bail,
};
use chrono::Utc;
use clap::Args;
use serde::{
    Deserialize,
    Serialize,
};

use super::{
    EXIT_INVALID,
    EXIT_PASS,
    EXIT_REGRESSION,
    artifact::{
        CampaignLease,
        atomic_json,
        atomic_jsonl,
        atomic_text,
        create_run_dir,
        digest_file,
        digest_tree,
        directory_bytes,
        no_extra,
        run_id,
        workspace_root,
    },
    build::setup_asset_digest,
    config::{
        SelectedWorkload,
        estimated_compare_millis,
        estimated_single_millis,
        is_reliability_timing,
        load_budget_policy,
        load_profile,
        load_registry,
        select_workloads,
    },
    model::{
        BUILD_SCHEMA,
        BinaryRole,
        BlockRecord,
        BudgetEntry,
        BudgetPolicy,
        BuildManifest,
        CampaignManifest,
        ComparisonDocument,
        EnvironmentManifest,
        ExternalMeasurements,
        ImplementationStatus,
        Orientation,
        Preconditioning,
        ProcessMeasurement,
        REPORT_SCHEMA,
        ReportDocument,
        SAMPLE_SCHEMA,
        Sample,
        SampleState,
        Validation,
        Verdict,
    },
    process::{
        CapturedOutput,
        ProcessOutcome,
        ProcessSpec,
        executable_path,
        execute,
    },
    provenance::{
        self,
        FrozenPaths,
    },
    report,
    schedule,
    stats::compare_wall_time,
    workload::{
        ExecutionLimits,
        ProductFailure,
        WorkloadRequest,
        WorkloadRunner,
    },
};

/// CLI arguments for measuring one immutable release binary.
#[derive(Debug, Args)]
pub struct RunArgs {
    /// Checked-in profile selecting workload tuples, repetitions, and limits.
    #[arg(long)]
    profile: String,
    /// Restrict the profile to this workload ID; repeat to select multiple workloads.
    #[arg(long = "workload")]
    workloads: Vec<String>,
    /// Release executable from the build snapshot being measured.
    #[arg(long = "bin")]
    binary: PathBuf,
    /// `build-manifest.json` from the same snapshot as `--bin`.
    #[arg(long = "build-manifest")]
    build_manifest: PathBuf,
    /// New run directory under `target/perf/runs`; defaults to a generated run ID.
    #[arg(long)]
    output: Option<PathBuf>,
    /// Override the profile's seed when a reproducible independent schedule is needed.
    #[arg(long)]
    schedule_seed: Option<u64>,
}

/// CLI arguments for a counterbalanced baseline/candidate comparison.
#[derive(Debug, Args)]
pub struct CompareArgs {
    /// Checked-in comparison profile; numerical gating requires a published budget policy.
    #[arg(long)]
    profile: String,
    /// Restrict the profile to this workload ID; repeat to select multiple workloads.
    #[arg(long = "workload")]
    workloads: Vec<String>,
    /// Release executable for the known reference revision.
    #[arg(long = "baseline-bin")]
    baseline_binary: PathBuf,
    /// `build-manifest.json` from the same snapshot as `--baseline-bin`.
    #[arg(long = "baseline-build-manifest")]
    baseline_manifest: PathBuf,
    /// Release executable for the revision being evaluated.
    #[arg(long = "candidate-bin")]
    candidate_binary: PathBuf,
    /// `build-manifest.json` from the same snapshot as `--candidate-bin`.
    #[arg(long = "candidate-build-manifest")]
    candidate_manifest: PathBuf,
    /// New comparison directory under `target/perf/runs`; defaults to a generated run ID.
    #[arg(long)]
    output: Option<PathBuf>,
    /// Override the profile's seed for a reproducible counterbalanced schedule.
    #[arg(long)]
    schedule_seed: Option<u64>,
    /// Prior matching over-budget comparison to confirm before declaring a regression.
    #[arg(long = "confirmation-of")]
    confirmation_of: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReadinessEvent {
    schema_id: String,
    run_id: String,
    workload_id: Option<String>,
    role: Option<BinaryRole>,
    kind: String,
    valid: bool,
    measured: bool,
    at: String,
    message: String,
}

struct ExecutedSample {
    sample: Sample,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

struct ConfirmationEvidence {
    report: ReportDocument,
    campaign: CampaignManifest,
}

/// Executes an informative single-binary campaign for the selected profile.
pub fn run_single(args: RunArgs) -> Result<u8> {
    let root = workspace_root()?;
    run_single_at(&root, args)
}

/// Runs a single-binary campaign against an explicit workspace root.
///
/// Keeping root discovery outside this function lets tests exercise the full
/// campaign pipeline with isolated registries, manifests, and artifact trees.
fn run_single_at(root: &Path, args: RunArgs) -> Result<u8> {
    let registry = load_registry(root)?;
    let profile = load_profile(root, &args.profile)?;
    let selected = select_workloads(&profile, &registry, &args.workloads)?;
    require_implemented(&selected)?;
    let binary = executable_path(&args.binary)?;
    let manifest = load_and_verify_manifest(&args.build_manifest, &binary)?;
    require_capabilities(&selected, &[&manifest])?;
    let planned_millis = estimated_single_millis(&selected)?;
    validate_campaign_budget(&profile, &selected, planned_millis, false)?;
    let environment = environment_manifest();
    validate_timing_contract(&selected, &environment, &manifest, &manifest, false)?;

    let _lease = CampaignLease::acquire(root)?;
    let run_dir = create_run_dir(root, args.output.as_deref(), "run")?;
    let id = run_id();
    let mut workload_runner = WorkloadRunner::new(root, &run_dir, &id);
    let seed = args.schedule_seed.unwrap_or(profile.schedule_seed);
    atomic_json(&run_dir.join("environment.json"), &environment)?;
    let resolved_profile = resolved_profile(&profile, &selected);
    let frozen = provenance::freeze(
        &run_dir,
        &registry,
        &resolved_profile,
        None,
        None,
        &manifest,
    )?;
    let mut campaign = campaign_manifest(&id, "run", &profile, seed, &selected, &frozen, false);
    atomic_json(&run_dir.join("campaign.json"), &campaign)?;
    print_budget(&selected, planned_millis, false);

    let deadline = Instant::now()
        .checked_add(Duration::from_secs(profile.campaign_cap_secs))
        .context("campaign deadline exceeds the platform clock range")?;
    let mut readiness = vec![ReadinessEvent {
        schema_id: REPORT_SCHEMA.into(),
        run_id: id.clone(),
        workload_id: None,
        role: None,
        kind: "campaign_readiness".into(),
        valid: true,
        measured: false,
        at: Utc::now().to_rfc3339(),
        message: "binary, manifest, profile, artifact path, and service-free workload validated"
            .into(),
    }];
    let mut samples = Vec::new();
    let mut blocks = Vec::new();
    let mut failures = Vec::new();
    let mut exit = EXIT_PASS;

    for selected_workload in &selected {
        if selected_workload.workload.preconditioning == Preconditioning::OnePerBinary {
            let precondition = execute_workload(
                &mut workload_runner,
                ExecutionRequest {
                    run_id: &id,
                    profile_id: &profile.id,
                    selected: selected_workload,
                    binary: &binary,
                    build_manifest: &args.build_manifest,
                    manifest: &manifest,
                    role: BinaryRole::Single,
                    orientation: Orientation::Single,
                    block_id: 0,
                    pair_id: 0,
                    position: 0,
                    measured: false,
                    stdout_limit: profile.max_stdout_bytes,
                    stderr_limit: profile.max_stderr_bytes,
                },
            )?;
            let valid = precondition.sample.validation.state == SampleState::Valid;
            readiness.push(ReadinessEvent {
                schema_id: REPORT_SCHEMA.into(),
                run_id: id.clone(),
                workload_id: Some(selected_workload.workload.id.clone()),
                role: Some(BinaryRole::Single),
                kind: "one_per_binary_preconditioning".into(),
                valid,
                measured: false,
                at: Utc::now().to_rfc3339(),
                message: precondition.sample.validation.message.clone(),
            });
            if !valid {
                failures.push(format!(
                    "{} preconditioning failed: {}",
                    selected_workload.workload.id, precondition.sample.validation.message
                ));
                exit = classification_exit(&precondition.sample);
                write_failure_output(&run_dir, &precondition, "preconditioning")?;
                break;
            }
        }

        for block_id in 0..selected_workload.profile.blocks {
            ensure_before_deadline(deadline)?;
            verify_executable_unchanged(&binary, &manifest)?;
            let orientation = schedule::orientation(
                seed,
                &selected_workload.workload.id,
                &selected_workload.profile.tuple,
                block_id,
            );
            let scheduled = schedule::executions(orientation);
            let mut block_samples = Vec::new();
            let executions = scheduled
                .iter()
                .filter(|execution| execution.role == BinaryRole::Candidate)
                .take(
                    if is_reliability_timing(&selected_workload.profile.timing) {
                        1
                    } else {
                        usize::MAX
                    },
                );
            for execution in executions {
                let executed = execute_workload(
                    &mut workload_runner,
                    ExecutionRequest {
                        run_id: &id,
                        profile_id: &profile.id,
                        selected: selected_workload,
                        binary: &binary,
                        build_manifest: &args.build_manifest,
                        manifest: &manifest,
                        role: BinaryRole::Single,
                        orientation,
                        block_id,
                        pair_id: execution.pair_id,
                        position: execution.position,
                        measured: true,
                        stdout_limit: profile.max_stdout_bytes,
                        stderr_limit: profile.max_stderr_bytes,
                    },
                )?;
                if executed.sample.validation.state != SampleState::Valid {
                    exit = classification_exit(&executed.sample);
                    failures.push(format!(
                        "{} block {} position {}: {}",
                        selected_workload.workload.id,
                        block_id,
                        execution.position,
                        executed.sample.validation.message
                    ));
                    write_failure_output(
                        &run_dir,
                        &executed,
                        &format!("block-{block_id}-position-{}", execution.position),
                    )?;
                }
                block_samples.push(executed.sample);
                if exit != EXIT_PASS {
                    break;
                }
            }
            let valid = block_samples
                .iter()
                .all(|sample| sample.validation.state == SampleState::Valid);
            samples.extend(block_samples);
            blocks.push(BlockRecord {
                schema_id: SAMPLE_SCHEMA.into(),
                run_id: id.clone(),
                workload_id: selected_workload.workload.id.clone(),
                tuple_id: selected_workload.profile.tuple.clone(),
                block_id,
                orientation_set_id: block_id / 2,
                orientation,
                complete: valid,
                valid,
                sample_count: if is_reliability_timing(&selected_workload.profile.timing) {
                    1
                } else {
                    2
                },
            });
            checkpoint(&run_dir, &samples, &blocks, &readiness)?;
            enforce_artifact_limit(&run_dir, profile.max_artifact_bytes)?;
            if exit != EXIT_PASS {
                break;
            }
        }
        if exit != EXIT_PASS {
            break;
        }
    }

    let status = if exit == EXIT_PASS {
        "pass"
    } else if exit == EXIT_REGRESSION {
        "failure"
    } else {
        "invalid"
    };
    campaign.status = status.into();
    campaign.completed_at = Some(Utc::now().to_rfc3339());
    campaign.failure = failures.first().cloned();
    atomic_json(&run_dir.join("campaign.json"), &campaign)?;
    let report = ReportDocument {
        schema_id: REPORT_SCHEMA.into(),
        run_id: id,
        kind: "run".into(),
        status: status.into(),
        profile_id: profile.id.clone(),
        generated_at: Utc::now().to_rfc3339(),
        comparisons: Vec::new(),
        failures,
        artifact_files: artifact_files(
            false,
            selected
                .iter()
                .any(|selected| is_reliability_timing(&selected.profile.timing)),
        ),
        extra: no_extra(),
    };
    report::write(&run_dir, &report)?;
    Ok(exit)
}

/// Executes a counterbalanced baseline/candidate campaign and writes verdicts.
pub fn compare(args: CompareArgs) -> Result<u8> {
    let root = workspace_root()?;
    compare_at(&root, args)
}

/// Runs a baseline/candidate campaign against an explicit workspace root.
fn compare_at(root: &Path, args: CompareArgs) -> Result<u8> {
    let registry = load_registry(root)?;
    let profile = load_profile(root, &args.profile)?;
    let budget_policy = profile
        .budget_reference
        .as_deref()
        .map(|id| load_budget_policy(root, id))
        .transpose()?;
    let selected = select_workloads(&profile, &registry, &args.workloads)?;
    require_implemented(&selected)?;
    let baseline_binary = executable_path(&args.baseline_binary)?;
    let candidate_binary = executable_path(&args.candidate_binary)?;
    let baseline_manifest = load_and_verify_manifest(&args.baseline_manifest, &baseline_binary)?;
    let candidate_manifest = load_and_verify_manifest(&args.candidate_manifest, &candidate_binary)?;
    if baseline_manifest.target != candidate_manifest.target {
        bail!(
            "build targets differ: {} versus {}",
            baseline_manifest.target,
            candidate_manifest.target
        );
    }
    require_capabilities(&selected, &[&baseline_manifest, &candidate_manifest])?;
    let planned_millis = estimated_compare_millis(&selected)?;
    validate_campaign_budget(&profile, &selected, planned_millis, true)?;
    let environment = environment_manifest();
    validate_timing_contract(
        &selected,
        &environment,
        &baseline_manifest,
        &candidate_manifest,
        true,
    )?;
    for selected_workload in &selected {
        resolve_budget_entry(
            selected_workload,
            &environment.environment_id,
            budget_policy.as_ref(),
        )?;
    }
    let confirmation = args
        .confirmation_of
        .as_deref()
        .map(|run_dir| {
            load_confirmation(
                run_dir,
                &registry,
                &profile,
                &selected,
                &environment,
                &baseline_manifest,
                &candidate_manifest,
                budget_policy.as_ref(),
            )
        })
        .transpose()?;
    let seed = match (&confirmation, args.schedule_seed) {
        (Some(evidence), None) => evidence.campaign.schedule_seed ^ 0xa5a5_5a5a_d3c4_b2e1,
        (_, Some(seed)) => seed,
        (None, None) => profile.schedule_seed,
    };
    if confirmation
        .as_ref()
        .is_some_and(|evidence| evidence.campaign.schedule_seed == seed)
    {
        bail!("confirmation run must use an independent schedule seed");
    }

    let _lease = CampaignLease::acquire(root)?;
    let run_dir = create_run_dir(root, args.output.as_deref(), "compare")?;
    let id = run_id();
    let mut workload_runner = WorkloadRunner::new(root, &run_dir, &id);
    atomic_json(&run_dir.join("environment.json"), &environment)?;
    let resolved_profile = resolved_profile(&profile, &selected);
    let frozen = provenance::freeze(
        &run_dir,
        &registry,
        &resolved_profile,
        budget_policy.as_ref(),
        Some(&baseline_manifest),
        &candidate_manifest,
    )?;
    let mut campaign = campaign_manifest(&id, "compare", &profile, seed, &selected, &frozen, true);
    atomic_json(&run_dir.join("campaign.json"), &campaign)?;
    print_budget(&selected, planned_millis, true);

    let deadline = Instant::now()
        .checked_add(Duration::from_secs(profile.campaign_cap_secs))
        .context("campaign deadline exceeds the platform clock range")?;
    let mut readiness = vec![ReadinessEvent {
        schema_id: REPORT_SCHEMA.into(),
        run_id: id.clone(),
        workload_id: None,
        role: None,
        kind: "campaign_readiness".into(),
        valid: true,
        measured: false,
        at: Utc::now().to_rfc3339(),
        message: "both immutable manifests, executable digests, common capabilities, and output limits validated".into(),
    }];
    let mut samples = Vec::new();
    let mut blocks = Vec::new();
    let mut comparisons = Vec::new();
    let mut failures = Vec::new();
    let mut exit = EXIT_PASS;

    for selected_workload in &selected {
        if selected_workload.workload.preconditioning == Preconditioning::OnePerBinary {
            let first_orientation = schedule::orientation(
                seed,
                &selected_workload.workload.id,
                &selected_workload.profile.tuple,
                0,
            );
            for role in schedule::preconditioning_order(first_orientation) {
                let binary = binary_for(role, &baseline_binary, &candidate_binary);
                let (build_manifest, manifest) = match role {
                    BinaryRole::Baseline => (&args.baseline_manifest, &baseline_manifest),
                    BinaryRole::Candidate => (&args.candidate_manifest, &candidate_manifest),
                    BinaryRole::Single => unreachable!(),
                };
                let precondition = execute_workload(
                    &mut workload_runner,
                    ExecutionRequest {
                        run_id: &id,
                        profile_id: &profile.id,
                        selected: selected_workload,
                        binary,
                        build_manifest,
                        manifest,
                        role,
                        orientation: first_orientation,
                        block_id: 0,
                        pair_id: 0,
                        position: 0,
                        measured: false,
                        stdout_limit: profile.max_stdout_bytes,
                        stderr_limit: profile.max_stderr_bytes,
                    },
                )?;
                let valid = precondition.sample.validation.state == SampleState::Valid;
                readiness.push(ReadinessEvent {
                    schema_id: REPORT_SCHEMA.into(),
                    run_id: id.clone(),
                    workload_id: Some(selected_workload.workload.id.clone()),
                    role: Some(role),
                    kind: "one_per_binary_preconditioning".into(),
                    valid,
                    measured: false,
                    at: Utc::now().to_rfc3339(),
                    message: precondition.sample.validation.message.clone(),
                });
                if !valid {
                    exit = classification_exit(&precondition.sample);
                    failures.push(format!(
                        "{} {role:?} preconditioning failed: {}",
                        selected_workload.workload.id, precondition.sample.validation.message
                    ));
                    write_failure_output(
                        &run_dir,
                        &precondition,
                        &format!("preconditioning-{role:?}"),
                    )?;
                    break;
                }
            }
            checkpoint(&run_dir, &samples, &blocks, &readiness)?;
            if exit != EXIT_PASS {
                break;
            }
        }

        let workload_sample_start = samples.len();
        for block_id in 0..selected_workload.profile.blocks {
            ensure_before_deadline(deadline)?;
            let orientation = schedule::orientation(
                seed,
                &selected_workload.workload.id,
                &selected_workload.profile.tuple,
                block_id,
            );
            for role in schedule::preconditioning_order(orientation) {
                match role {
                    BinaryRole::Baseline => {
                        verify_executable_unchanged(&baseline_binary, &baseline_manifest)?;
                    }
                    BinaryRole::Candidate => {
                        verify_executable_unchanged(&candidate_binary, &candidate_manifest)?;
                    }
                    BinaryRole::Single => unreachable!(),
                }
            }
            let mut block_samples = Vec::new();
            for execution in schedule::executions(orientation) {
                let binary = binary_for(execution.role, &baseline_binary, &candidate_binary);
                let (build_manifest, manifest) = match execution.role {
                    BinaryRole::Baseline => (&args.baseline_manifest, &baseline_manifest),
                    BinaryRole::Candidate => (&args.candidate_manifest, &candidate_manifest),
                    BinaryRole::Single => unreachable!(),
                };
                let executed = execute_workload(
                    &mut workload_runner,
                    ExecutionRequest {
                        run_id: &id,
                        profile_id: &profile.id,
                        selected: selected_workload,
                        binary,
                        build_manifest,
                        manifest,
                        role: execution.role,
                        orientation,
                        block_id,
                        pair_id: execution.pair_id,
                        position: execution.position,
                        measured: true,
                        stdout_limit: profile.max_stdout_bytes,
                        stderr_limit: profile.max_stderr_bytes,
                    },
                )?;
                if executed.sample.validation.state != SampleState::Valid {
                    exit = classification_exit(&executed.sample);
                    failures.push(format!(
                        "{} block {} position {} ({:?}): {}",
                        selected_workload.workload.id,
                        block_id,
                        execution.position,
                        execution.role,
                        executed.sample.validation.message
                    ));
                    write_failure_output(
                        &run_dir,
                        &executed,
                        &format!("block-{block_id}-position-{}", execution.position),
                    )?;
                }
                block_samples.push(executed.sample);
                if exit != EXIT_PASS {
                    break;
                }
            }
            let complete = block_samples.len() == 4;
            let valid = complete
                && block_samples
                    .iter()
                    .all(|sample| sample.validation.state == SampleState::Valid);
            let count = block_samples.len();
            samples.extend(block_samples);
            blocks.push(BlockRecord {
                schema_id: SAMPLE_SCHEMA.into(),
                run_id: id.clone(),
                workload_id: selected_workload.workload.id.clone(),
                tuple_id: selected_workload.profile.tuple.clone(),
                block_id,
                orientation_set_id: block_id / 2,
                orientation,
                complete,
                valid,
                sample_count: count,
            });
            checkpoint(&run_dir, &samples, &blocks, &readiness)?;
            enforce_artifact_limit(&run_dir, profile.max_artifact_bytes)?;
            if exit != EXIT_PASS {
                break;
            }
        }

        if exit == EXIT_PASS {
            let workload_samples = &samples[workload_sample_start..];
            let budget = resolve_budget_entry(
                selected_workload,
                &environment.environment_id,
                budget_policy.as_ref(),
            )?;
            let wall_time = compare_wall_time(
                workload_samples,
                seed ^ u64::from(selected_workload.profile.blocks),
                budget.map(|entry| entry.practical_budget),
                budget
                    .map(|entry| entry.max_residual_orientation_effect)
                    .or(profile.max_residual_orientation_effect),
                budget.is_some_and(|entry| {
                    confirmation.as_ref().is_some_and(|evidence| {
                        confirms_prior_signal(
                            evidence,
                            &selected_workload.workload.id,
                            &selected_workload.profile.tuple,
                            entry,
                        )
                    })
                }),
            )?;
            let comparison_verdict = wall_time.verdict.clone();
            match comparison_verdict {
                Verdict::Regression => {
                    exit = EXIT_REGRESSION;
                    failures.push(format!(
                        "{} confirmed wall-time regression {:.2}% exceeds {:.2}% budget",
                        selected_workload.workload.id,
                        wall_time.harmful_effect * 100.0,
                        wall_time.practical_budget.unwrap_or_default() * 100.0
                    ));
                }
                Verdict::Invalid => {
                    exit = EXIT_INVALID;
                    failures.push(format!(
                        "{} retained residual orientation effect {:.2}%",
                        selected_workload.workload.id,
                        wall_time.residual_orientation_effect * 100.0
                    ));
                }
                Verdict::Inconclusive => {
                    exit = EXIT_INVALID;
                    failures.push(format!(
                        "{} wall-time result is inconclusive and requires independent confirmation",
                        selected_workload.workload.id
                    ));
                }
                Verdict::Pass | Verdict::Improvement | Verdict::Informative => {}
            }
            let comparison = ComparisonDocument {
                schema_id: super::model::COMPARISON_SCHEMA.into(),
                run_id: id.clone(),
                profile_id: profile.id.clone(),
                workload_id: selected_workload.workload.id.clone(),
                tuple_id: selected_workload.profile.tuple.clone(),
                baseline_digest: baseline_manifest.executable_digest.clone(),
                candidate_digest: candidate_manifest.executable_digest.clone(),
                metrics: vec![wall_time],
                verdict: comparison_verdict,
                extra: no_extra(),
            };
            let name = artifact_name(&comparison.workload_id, &comparison.tuple_id);
            atomic_json(
                &run_dir.join("comparisons").join(format!("{name}.json")),
                &comparison,
            )?;
            comparisons.push(comparison);
        }
        if exit != EXIT_PASS {
            break;
        }
    }

    let status = if exit == EXIT_PASS {
        "pass"
    } else if exit == EXIT_REGRESSION {
        "failure"
    } else {
        "invalid"
    };
    campaign.status = status.into();
    campaign.completed_at = Some(Utc::now().to_rfc3339());
    campaign.failure = failures.first().cloned();
    atomic_json(&run_dir.join("campaign.json"), &campaign)?;
    atomic_jsonl(&run_dir.join("comparisons.jsonl"), &comparisons)?;
    let report = ReportDocument {
        schema_id: REPORT_SCHEMA.into(),
        run_id: id,
        kind: "compare".into(),
        status: status.into(),
        profile_id: profile.id,
        generated_at: Utc::now().to_rfc3339(),
        comparisons,
        failures,
        artifact_files: artifact_files(true, false),
        extra: no_extra(),
    };
    report::write(&run_dir, &report)?;
    enforce_artifact_limit(&run_dir, profile.max_artifact_bytes)?;
    Ok(exit)
}

/// Loads a manifest and verifies its schema, executable identity, and setup assets.
fn load_and_verify_manifest(path: &Path, binary: &Path) -> Result<BuildManifest> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let manifest: BuildManifest =
        serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))?;
    if manifest.schema_id != BUILD_SCHEMA {
        bail!("unsupported build manifest schema: {}", manifest.schema_id);
    }
    let digest = digest_file(binary)?;
    if digest != manifest.executable_digest {
        bail!(
            "executable digest mismatch for {}: manifest {}, actual {}",
            binary.display(),
            manifest.executable_digest,
            digest
        );
    }
    if binary.metadata()?.len() != manifest.executable_bytes {
        bail!("executable size does not match build manifest");
    }
    let manifest_dir = path.parent().context("build manifest has no parent")?;
    for asset in &manifest.setup_assets {
        let relative = Path::new(&asset.relative_path);
        if relative.is_absolute()
            || relative
                .components()
                .any(|component| component == Component::ParentDir)
        {
            bail!("unsafe setup asset path: {}", asset.relative_path);
        }
        let actual = setup_asset_digest(&asset.name, &manifest_dir.join(relative))?;
        if actual != asset.digest {
            // Manifests written before evaluator metadata-cache exclusion used
            // the full tree digest. Accept them only while that exact legacy
            // tree remains unchanged; new manifests use the reusable digest.
            let legacy = digest_tree(&manifest_dir.join(relative))?;
            if legacy != asset.digest {
                bail!("setup asset digest mismatch: {}", asset.name);
            }
        }
    }
    Ok(manifest)
}

/// Detects executable mutation between measurement blocks.
fn verify_executable_unchanged(binary: &Path, manifest: &BuildManifest) -> Result<()> {
    let digest = digest_file(binary)?;
    if digest != manifest.executable_digest {
        bail!(
            "executable changed during the campaign: {} expected {}, found {}",
            binary.display(),
            manifest.executable_digest,
            digest
        );
    }
    Ok(())
}

/// Rejects selected workload contracts that do not yet have executable drivers.
fn require_implemented(selected: &[SelectedWorkload<'_>]) -> Result<()> {
    let planned: Vec<_> = selected
        .iter()
        .filter(|selected| selected.workload.status != ImplementationStatus::Implemented)
        .map(|selected| selected.workload.id.as_str())
        .collect();
    if !planned.is_empty() {
        bail!(
            "profile selection includes workloads that are not implemented: {}",
            planned.join(", ")
        );
    }
    Ok(())
}

/// Requires every measured build to advertise every selected workload capability.
fn require_capabilities(
    selected: &[SelectedWorkload<'_>],
    manifests: &[&BuildManifest],
) -> Result<()> {
    for selected in selected {
        for manifest in manifests {
            if !has_capability(&manifest.capabilities, &selected.workload.capability) {
                bail!(
                    "build {} lacks capability {}",
                    manifest.executable_digest,
                    selected.workload.capability
                );
            }
        }
    }
    Ok(())
}

fn has_capability(capabilities: &[String], required: &str) -> bool {
    capabilities.iter().any(|capability| capability == required)
}

/// Enforces timing-mode boundaries before creating campaign resources.
fn validate_timing_contract(
    selected: &[SelectedWorkload<'_>],
    environment: &EnvironmentManifest,
    baseline: &BuildManifest,
    candidate: &BuildManifest,
    comparison: bool,
) -> Result<()> {
    let timings = selected
        .iter()
        .map(|selected| selected.profile.timing.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    if timings.len() != 1 {
        bail!("one campaign cannot mix fixed-load, noise, and capacity timing modes");
    }
    match timings.first().copied().unwrap_or("informative") {
        "calibration" => {
            if !comparison {
                bail!("noise calibration requires a same-build comparison campaign");
            }
            if baseline.executable_digest != candidate.executable_digest {
                bail!("noise calibration must compare one identical immutable build");
            }
            if selected.iter().any(|selected| selected.profile.blocks < 30) {
                bail!("noise calibration requires at least 30 blocks per tuple");
            }
        }
        "capacity" => {
            if comparison {
                bail!("capacity calibration must remain separate from A/B comparisons");
            }
        }
        "soak" | "recovery" => {
            if comparison {
                bail!("reliability workloads are single-build operational checks");
            }
            if selected
                .iter()
                .any(|selected| selected.workload.reliability.is_none())
            {
                bail!("reliability workload is missing its bounded acceptance contract");
            }
        }
        "gating" => {
            if !comparison {
                bail!("calibrated gating profiles require an A/B comparison");
            }
            if !environment.canonical {
                bail!("calibrated gating requires its canonical environment");
            }
        }
        "informative" => {}
        timing => bail!("unsupported timing policy: {timing}"),
    }
    Ok(())
}

/// Resolves one exact calibrated wall-time gate for a selected workload tuple.
fn resolve_budget_entry<'a>(
    selected: &SelectedWorkload<'_>,
    environment_id: &str,
    policy: Option<&'a BudgetPolicy>,
) -> Result<Option<&'a BudgetEntry>> {
    let Some(policy) = policy else {
        return Ok(None);
    };
    if policy.environment_id != environment_id {
        bail!(
            "budget {} belongs to environment {}, not {}",
            policy.id,
            policy.environment_id,
            environment_id
        );
    }
    let entry = policy
        .entries
        .iter()
        .find(|entry| {
            entry.workload_id == selected.workload.id
                && entry.tuple_id == selected.profile.tuple
                && entry.metric == "wall_time"
        })
        .with_context(|| {
            format!(
                "budget {} has no wall_time gate for {}:{}",
                policy.id, selected.workload.id, selected.profile.tuple
            )
        })?;
    if selected.profile.blocks < entry.minimum_blocks {
        bail!(
            "profile provides {} blocks for {}:{}, but budget {} requires {}",
            selected.profile.blocks,
            selected.workload.id,
            selected.profile.tuple,
            policy.id,
            entry.minimum_blocks
        );
    }
    Ok(Some(entry))
}

#[allow(clippy::too_many_arguments)]
/// Loads and validates the first over-budget run used for confirmation.
fn load_confirmation(
    run_dir: &Path,
    registry: &super::model::WorkloadRegistry,
    profile: &super::model::Profile,
    selected: &[SelectedWorkload<'_>],
    environment: &EnvironmentManifest,
    baseline: &BuildManifest,
    candidate: &BuildManifest,
    policy: Option<&BudgetPolicy>,
) -> Result<ConfirmationEvidence> {
    let report_path = run_dir.join("report.json");
    let campaign_path = run_dir.join("campaign.json");
    let environment_path = run_dir.join("environment.json");
    let report: ReportDocument = serde_json::from_slice(
        &fs::read(&report_path).with_context(|| format!("read {}", report_path.display()))?,
    )?;
    let campaign: CampaignManifest = serde_json::from_slice(
        &fs::read(&campaign_path).with_context(|| format!("read {}", campaign_path.display()))?,
    )?;
    let frozen =
        provenance::load(run_dir, &campaign).context("load confirmation campaign provenance")?;
    let prior_environment: EnvironmentManifest = serde_json::from_slice(
        &fs::read(&environment_path)
            .with_context(|| format!("read {}", environment_path.display()))?,
    )?;
    let expected = selected
        .iter()
        .map(|selected| format!("{}:{}", selected.workload.id, selected.profile.tuple))
        .collect::<Vec<_>>();
    if report.schema_id != REPORT_SCHEMA
        || campaign.schema_id != REPORT_SCHEMA
        || prior_environment.schema_id != super::model::ENVIRONMENT_SCHEMA
        || report.run_id != campaign.run_id
        || report.kind != "compare"
        || campaign.kind != "compare"
        || report.profile_id != profile.id
        || campaign.profile_id != profile.id
        || campaign.selected_workloads != expected
        || report.status != "invalid"
        || campaign.status != "invalid"
        || !prior_environment.canonical
        || prior_environment.environment_id != environment.environment_id
    {
        bail!("confirmation source is not a compatible inconclusive canonical comparison");
    }
    let expected_profile = resolved_profile(profile, selected);
    if serde_json::to_value(&frozen.registry)? != serde_json::to_value(registry)?
        || serde_json::to_value(&frozen.profile)? != serde_json::to_value(&expected_profile)?
        || serde_json::to_value(&frozen.budget_policy)? != serde_json::to_value(policy)?
    {
        bail!("confirmation source frozen registry, profile, or budget policy does not match");
    }
    let prior_baseline = frozen
        .baseline_manifest
        .as_ref()
        .context("confirmation source omitted its frozen baseline manifest")?;
    if prior_baseline.executable_digest != baseline.executable_digest
        || frozen.candidate_manifest.executable_digest != candidate.executable_digest
    {
        bail!("confirmation source frozen build digests do not match");
    }
    for comparison in &report.comparisons {
        if comparison.baseline_digest != baseline.executable_digest
            || comparison.candidate_digest != candidate.executable_digest
        {
            bail!("confirmation source build digests do not match");
        }
    }
    let evidence = ConfirmationEvidence { report, campaign };
    let has_signal = policy.is_some_and(|policy| {
        policy.entries.iter().any(|entry| {
            confirms_prior_signal(&evidence, &entry.workload_id, &entry.tuple_id, entry)
        })
    });
    if !has_signal {
        bail!("confirmation source has no independently confirmable over-budget signal");
    }
    Ok(evidence)
}

fn confirms_prior_signal(
    evidence: &ConfirmationEvidence,
    workload_id: &str,
    tuple_id: &str,
    budget: &BudgetEntry,
) -> bool {
    evidence.report.comparisons.iter().any(|comparison| {
        comparison.workload_id == workload_id
            && comparison.tuple_id == tuple_id
            && comparison.metrics.iter().any(|metric| {
                metric.name == budget.metric
                    && metric.verdict == Verdict::Inconclusive
                    && metric.confidence_lower > budget.practical_budget
                    && metric.practical_budget.is_some_and(|value| {
                        (value - budget.practical_budget).abs() <= f64::EPSILON
                    })
            })
    })
}

/// Proves planned duration and worst-case retained output fit the profile caps.
fn validate_campaign_budget(
    profile: &super::model::Profile,
    selected: &[SelectedWorkload<'_>],
    planned_millis: u64,
    comparison: bool,
) -> Result<()> {
    let campaign_cap_millis = profile
        .campaign_cap_secs
        .checked_mul(1_000)
        .context("profile campaign cap overflows milliseconds")?;
    if planned_millis > campaign_cap_millis {
        bail!(
            "planned duration {}s exceeds profile cap {}s",
            planned_millis.div_ceil(1000),
            profile.campaign_cap_secs
        );
    }
    let process_count: u64 = selected
        .iter()
        .map(|selected| {
            let measured = if !comparison && is_reliability_timing(&selected.profile.timing) {
                1
            } else {
                u64::from(selected.profile.blocks) * if comparison { 4 } else { 2 }
            };
            let precondition = if selected.workload.preconditioning == Preconditioning::OnePerBinary
            {
                if comparison { 2 } else { 1 }
            } else {
                0
            };
            measured + precondition
        })
        .sum();
    let output_per_process = u64::try_from(profile.max_stdout_bytes)
        .ok()
        .and_then(|stdout| {
            u64::try_from(profile.max_stderr_bytes)
                .ok()
                .and_then(|stderr| stdout.checked_add(stderr))
        })
        .context("profile output limits overflow the artifact estimate")?;
    let worst_case_output = process_count
        .checked_mul(output_per_process)
        .context("profile output estimate overflowed")?;
    if worst_case_output > profile.max_artifact_bytes {
        bail!(
            "worst-case captured output {} bytes exceeds artifact cap {} bytes",
            worst_case_output,
            profile.max_artifact_bytes
        );
    }
    Ok(())
}

struct ExecutionRequest<'request, 'config> {
    run_id: &'request str,
    profile_id: &'request str,
    selected: &'request SelectedWorkload<'config>,
    binary: &'request Path,
    build_manifest: &'request Path,
    manifest: &'request BuildManifest,
    role: BinaryRole,
    orientation: Orientation,
    block_id: u32,
    pair_id: u8,
    position: u8,
    measured: bool,
    stdout_limit: usize,
    stderr_limit: usize,
}

/// Executes one scheduled position and converts observations into a raw sample.
///
/// Startup is launched directly; service-backed workloads delegate to the lazy
/// workload runner. This function records measurements but does not decide the
/// campaign-level exit code.
fn execute_workload(
    runner: &mut WorkloadRunner,
    request: ExecutionRequest<'_, '_>,
) -> Result<ExecutedSample> {
    let started_at = Utc::now().to_rfc3339();
    let (outcome, external, workload_failure) =
        if request.selected.workload.id == "startup.cli-help.v1" {
            (
                execute(&ProcessSpec {
                    program: request.binary,
                    args: &request.selected.workload.command,
                    current_dir: None,
                    env: &[],
                    timeout: Duration::from_millis(request.selected.workload.watchdog_ms),
                    stdout_limit: request.stdout_limit,
                    stderr_limit: request.stderr_limit,
                })?,
                Default::default(),
                None,
            )
        } else {
            let empty_exact = std::collections::BTreeMap::new();
            let exact = request
                .selected
                .workload
                .scaling_model
                .as_ref()
                .and_then(|model| {
                    model
                        .points
                        .iter()
                        .find(|point| point.tuple == request.selected.profile.tuple)
                })
                .map_or(&empty_exact, |point| &point.exact);
            let executed = runner.execute(WorkloadRequest {
                workload_id: &request.selected.workload.id,
                tuple: &request.selected.profile.tuple,
                fixture_id: &request.selected.workload.fixture,
                binary: request.binary,
                manifest_path: request.build_manifest,
                manifest: request.manifest,
                exact,
                reliability: request.selected.workload.reliability.as_ref(),
                limits: ExecutionLimits {
                    watchdog: Duration::from_millis(request.selected.workload.watchdog_ms),
                    stdout: request.stdout_limit,
                    stderr: request.stderr_limit,
                },
            });
            match executed {
                Ok(executed) => (executed.process, executed.external, None),
                Err(error) => {
                    let Some(failure) = error.downcast_ref::<ProductFailure>() else {
                        return Err(error);
                    };
                    (
                        failed_workload_outcome(),
                        ExternalMeasurements::default(),
                        Some(failure.message().to_owned()),
                    )
                }
            }
        };
    let mut sample_validation = workload_failure.map_or_else(
        || validate_outcome(&outcome, request.selected, request.role),
        |message| workload_failure_validation(request.role, &message),
    );
    if sample_validation.state == SampleState::Valid
        && let Some(metric) =
            missing_required_metric(request.selected, request.manifest, &outcome, &external)
    {
        sample_validation = validation(
            SampleState::Invalid,
            "missing_required_metric",
            &format!("required metric {metric:?} was not observed"),
        );
    }
    let measurement = ProcessMeasurement {
        wall_time_ns: outcome.wall_time.as_nanos().min(u128::from(u64::MAX)) as u64,
        cpu_time_ns: outcome.cpu_time_ns,
        peak_rss_bytes: outcome.peak_rss_bytes,
        stdout_first_byte_ns: outcome
            .stdout
            .first_byte_time
            .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64),
        stdout_last_byte_ns: outcome
            .stdout
            .last_byte_time
            .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64),
        resource_source: outcome.resource_source.into(),
        exit_code: outcome.exit_code,
        timed_out: outcome.timed_out,
        stdout_bytes: outcome.stdout.bytes_seen,
        stderr_bytes: outcome.stderr.bytes_seen,
        stdout_truncated: outcome.stdout.truncated,
        stderr_truncated: outcome.stderr.truncated,
    };
    Ok(ExecutedSample {
        sample: Sample {
            schema_id: SAMPLE_SCHEMA.into(),
            run_id: request.run_id.into(),
            profile_id: request.profile_id.into(),
            workload_id: request.selected.workload.id.clone(),
            tuple_id: request.selected.profile.tuple.clone(),
            block_id: request.block_id,
            orientation_set_id: request.block_id / 2,
            orientation: request.orientation,
            pair_id: request.pair_id,
            position: request.position,
            role: request.role,
            measured: request.measured,
            started_at,
            process: measurement,
            external,
            validation: sample_validation,
            extra: no_extra(),
        },
        stdout: outcome.stdout.data,
        stderr: outcome.stderr.data,
    })
}

fn failed_workload_outcome() -> ProcessOutcome {
    ProcessOutcome {
        wall_time: Duration::ZERO,
        cpu_time_ns: None,
        peak_rss_bytes: None,
        resource_source: "workload_oracle",
        exit_code: None,
        timed_out: false,
        stdout: CapturedOutput {
            bytes_seen: 0,
            truncated: false,
            data: Vec::new(),
            first_byte_time: None,
            last_byte_time: None,
        },
        stderr: CapturedOutput {
            bytes_seen: 0,
            truncated: false,
            data: Vec::new(),
            first_byte_time: None,
            last_byte_time: None,
        },
    }
}

fn workload_failure_validation(role: BinaryRole, message: &str) -> Validation {
    validation(
        if role == BinaryRole::Baseline {
            SampleState::Invalid
        } else {
            SampleState::ProductFailure
        },
        "workload_oracle",
        message,
    )
}

/// Classifies process and startup-oracle failures according to the measured role.
///
/// Baseline failures invalidate a comparison because no trustworthy reference
/// remains. Candidate and single-binary failures are product failures.
fn validate_outcome(
    outcome: &ProcessOutcome,
    selected: &SelectedWorkload<'_>,
    role: BinaryRole,
) -> Validation {
    let failure_state = if role == BinaryRole::Baseline {
        SampleState::Invalid
    } else {
        SampleState::ProductFailure
    };
    if outcome.timed_out {
        return validation(
            failure_state,
            "process_timeout",
            "workload exceeded its watchdog",
        );
    }
    if outcome.exit_code != Some(0) {
        return validation(
            failure_state,
            "process_exit",
            &format!("workload exited with {:?}", outcome.exit_code),
        );
    }
    if outcome.stdout.truncated || outcome.stderr.truncated {
        return validation(
            failure_state,
            "output_truncated",
            "workload output exceeded its byte limit",
        );
    }
    let stdout = outcome.stdout.text();
    if let Some(signature) = missing_signature(&stdout, &selected.workload.help_signatures) {
        return validation(
            failure_state,
            "fixture_mismatch",
            &format!("missing expected output signature {signature:?}"),
        );
    }
    validation(SampleState::Valid, "ok", "exact startup oracle passed")
}

fn missing_signature<'a>(output: &str, signatures: &'a [String]) -> Option<&'a str> {
    signatures
        .iter()
        .find(|signature| !output.contains(signature.as_str()))
        .map(String::as_str)
}

/// Returns the first declared metric absent from a successful raw observation.
fn missing_required_metric<'a>(
    selected: &'a SelectedWorkload<'_>,
    manifest: &BuildManifest,
    outcome: &ProcessOutcome,
    external: &ExternalMeasurements,
) -> Option<&'a str> {
    selected
        .workload
        .required_metrics
        .iter()
        .find(|metric| {
            !match metric.as_str() {
                "wall_time" => !outcome.wall_time.is_zero(),
                "child_cpu" | "process_cpu" => outcome.cpu_time_ns.is_some(),
                "peak_rss" => outcome.peak_rss_bytes.is_some(),
                "executable_bytes" => manifest.executable_bytes > 0,
                "service_cpu" => external.service_cpu_percent.is_some(),
                "sql" => {
                    external.sql_calls.is_some()
                        && external.sql_time_ms.is_some()
                        && external.sql_rows.is_some()
                }
                "wal" => external.wal_bytes.is_some(),
                "http" => {
                    external.http_requests.is_some()
                        && external.http_bytes.is_some()
                        && external.http_peak_concurrency.is_some()
                }
                "queue" => external.queue_ready.is_some() && external.queue_unacked.is_some(),
                "throughput" => !outcome.wall_time.is_zero() && !external.durable_counts.is_empty(),
                // Successful reliability drivers have already required these
                // observations through their exact reliability artifact.
                "file_descriptors" => matches!(
                    selected.workload.id.as_str(),
                    "system.soak.v1" | "system.recovery.v1"
                ),
                "recovery_time" => selected.workload.id == "system.recovery.v1",
                _ => false,
            }
        })
        .map(String::as_str)
}

/// Constructs the stable validation payload persisted with a sample.
fn validation(state: SampleState, code: &str, message: &str) -> Validation {
    Validation {
        state,
        code: code.into(),
        message: message.into(),
    }
}

/// Maps sample validation state to the public `cargo perf` exit-code contract.
fn classification_exit(sample: &Sample) -> u8 {
    match sample.validation.state {
        SampleState::Valid => EXIT_PASS,
        SampleState::ProductFailure => EXIT_REGRESSION,
        SampleState::Invalid => EXIT_INVALID,
    }
}

/// Atomically rewrites resumable raw sample, block, and readiness journals.
fn checkpoint(
    run_dir: &Path,
    samples: &[Sample],
    blocks: &[BlockRecord],
    readiness: &[ReadinessEvent],
) -> Result<()> {
    atomic_jsonl(&run_dir.join("samples.jsonl"), samples)?;
    atomic_jsonl(&run_dir.join("blocks.jsonl"), blocks)?;
    atomic_jsonl(&run_dir.join("readiness.jsonl"), readiness)
}

/// Persists the failed sample and its bounded output for diagnosis.
fn write_failure_output(run_dir: &Path, executed: &ExecutedSample, name: &str) -> Result<()> {
    let directory = run_dir
        .join("failures")
        .join(artifact_name(name, "failure"));
    atomic_json(&directory.join("sample.json"), &executed.sample)?;
    atomic_text(
        &directory.join("stdout.log"),
        &String::from_utf8_lossy(&executed.stdout),
    )?;
    atomic_text(
        &directory.join("stderr.log"),
        &String::from_utf8_lossy(&executed.stderr),
    )
}

fn binary_for<'a>(role: BinaryRole, baseline: &'a Path, candidate: &'a Path) -> &'a Path {
    match role {
        BinaryRole::Baseline => baseline,
        BinaryRole::Candidate | BinaryRole::Single => candidate,
    }
}

/// Captures host identity and whether it qualifies for canonical comparisons.
fn environment_manifest() -> EnvironmentManifest {
    let environment_id = std::env::var("VIGILO_PERF_ENVIRONMENT_ID").unwrap_or_else(|_| {
        format!(
            "local-{}-{}-development-v1",
            std::env::consts::OS,
            std::env::consts::ARCH
        )
    });
    let canonical = environment_id == "aws-m6i-2xlarge-al2023-v1"
        && cfg!(target_os = "linux")
        && std::env::var("VIGILO_PERF_CANONICAL_VALIDATED").as_deref() == Ok("1");
    EnvironmentManifest {
        schema_id: super::model::ENVIRONMENT_SCHEMA.into(),
        created_at: Utc::now().to_rfc3339(),
        canonical,
        environment_id,
        os: std::env::consts::OS.into(),
        architecture: std::env::consts::ARCH.into(),
        logical_cpus: std::thread::available_parallelism().map_or(1, usize::from),
        hostname: std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("COMPUTERNAME"))
            .ok(),
        collector: if cfg!(windows) {
            "windows-job-object-v1"
        } else {
            "counterbalanced-wall-time-v1"
        }
        .into(),
        validity: vec![
            if canonical {
                "canonical_environment_declared"
            } else {
                "noncanonical_development_mode"
            }
            .into(),
        ],
        extra: no_extra(),
    }
}

#[allow(clippy::too_many_arguments)]
/// Creates the initial campaign contract and its planned execution counts.
fn campaign_manifest(
    id: &str,
    kind: &str,
    profile: &super::model::Profile,
    seed: u64,
    selected: &[SelectedWorkload<'_>],
    frozen: &FrozenPaths,
    comparison: bool,
) -> CampaignManifest {
    let measured = selected
        .iter()
        .map(|selected| {
            if !comparison && is_reliability_timing(&selected.profile.timing) {
                1
            } else {
                u64::from(selected.profile.blocks) * if comparison { 4 } else { 2 }
            }
        })
        .sum();
    let precondition = selected
        .iter()
        .filter(|selected| selected.workload.preconditioning == Preconditioning::OnePerBinary)
        .count() as u64
        * if comparison { 2 } else { 1 };
    CampaignManifest {
        schema_id: REPORT_SCHEMA.into(),
        run_id: id.into(),
        kind: kind.into(),
        status: "running".into(),
        created_at: Utc::now().to_rfc3339(),
        completed_at: None,
        profile_id: profile.id.clone(),
        schedule_seed: seed,
        selected_workloads: selected
            .iter()
            .map(|selected| format!("{}:{}", selected.workload.id, selected.profile.tuple))
            .collect(),
        planned_measured_executions: measured,
        planned_preconditioning_executions: precondition,
        artifact_limit_bytes: profile.max_artifact_bytes,
        environment_file: "environment.json".into(),
        registry_file: frozen.registry_file.clone(),
        profile_file: frozen.profile_file.clone(),
        budget_policy_file: frozen.budget_policy_file.clone(),
        baseline_manifest: frozen.baseline_manifest.clone(),
        candidate_manifest: frozen.candidate_manifest.clone(),
        failure: None,
        extra: no_extra(),
    }
}

/// Materializes the exact profile subset selected for one campaign.
fn resolved_profile(
    profile: &super::model::Profile,
    selected: &[SelectedWorkload<'_>],
) -> super::model::Profile {
    let mut resolved = profile.clone();
    resolved.workloads = selected
        .iter()
        .map(|selected| selected.profile.clone())
        .collect();
    resolved
}

/// Prevents a new block from starting after the campaign wall-time cap.
fn ensure_before_deadline(deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        bail!("campaign wall-time cap expired before the next block");
    }
    Ok(())
}

/// Rejects a campaign whose persisted artifact tree exceeds its profile cap.
fn enforce_artifact_limit(run_dir: &Path, limit: u64) -> Result<()> {
    let bytes = directory_bytes(run_dir)?;
    if bytes > limit {
        bail!("campaign artifact limit exceeded: {bytes} > {limit} bytes");
    }
    Ok(())
}

fn print_budget(selected: &[SelectedWorkload<'_>], planned_millis: u64, comparison: bool) {
    println!("Execution budget:");
    for selected in selected {
        let measured = if !comparison && is_reliability_timing(&selected.profile.timing) {
            1
        } else {
            selected.profile.blocks * if comparison { 4 } else { 2 }
        };
        let discarded = if selected.workload.preconditioning == Preconditioning::OnePerBinary {
            if comparison { 2 } else { 1 }
        } else {
            0
        };
        println!(
            "  {}:{}  blocks={} measured={} preconditioning={}",
            selected.workload.id,
            selected.profile.tuple,
            selected.profile.blocks,
            measured,
            discarded
        );
    }
    println!(
        "  planned campaign duration: {:.1}s",
        planned_millis as f64 / 1000.0
    );
}

fn artifact_name(left: &str, right: &str) -> String {
    format!("{left}-{right}")
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' {
                character
            } else {
                '-'
            }
        })
        .collect()
}

fn artifact_files(comparison: bool, reliability: bool) -> Vec<String> {
    let mut files = vec![
        "campaign.json".into(),
        "environment.json".into(),
        "readiness.jsonl".into(),
        "samples.jsonl".into(),
        "blocks.jsonl".into(),
        "report.json".into(),
        "summary.md".into(),
        "provenance/".into(),
    ];
    if comparison {
        files.insert(5, "comparisons.jsonl".into());
    }
    if reliability {
        files.push("reliability/".into());
        files.push("reliability.md".into());
    }
    files
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::perf::{
        artifact::{
            digest_file,
            digest_tree,
        },
        model::SetupAsset,
        process::CapturedOutput,
    };

    fn process_outcome(stdout: &str) -> ProcessOutcome {
        ProcessOutcome {
            wall_time: Duration::from_millis(1),
            cpu_time_ns: Some(1),
            peak_rss_bytes: Some(1),
            resource_source: "test",
            exit_code: Some(0),
            timed_out: false,
            stdout: CapturedOutput {
                bytes_seen: stdout.len() as u64,
                truncated: false,
                data: stdout.as_bytes().to_vec(),
                first_byte_time: None,
                last_byte_time: None,
            },
            stderr: CapturedOutput {
                bytes_seen: 0,
                truncated: false,
                data: Vec::new(),
                first_byte_time: None,
                last_byte_time: None,
            },
        }
    }

    fn manifest(binary: &Path, asset: &Path) -> BuildManifest {
        BuildManifest {
            schema_id: BUILD_SCHEMA.into(),
            created_at: "2026-08-18T00:00:00Z".into(),
            executable_name: binary.file_name().unwrap().to_string_lossy().into_owned(),
            executable_digest: digest_file(binary).unwrap(),
            executable_bytes: binary.metadata().unwrap().len(),
            source_commit: None,
            source_dirty: false,
            source_label: "test".into(),
            cargo_lock_digest: "lock".into(),
            dependency_tree_digest: "tree".into(),
            migrations_digest: "migrations".into(),
            evaluator_abi_digest: "abi".into(),
            rustc: "rustc test".into(),
            cargo: "cargo test".into(),
            target: "test".into(),
            profile: "release".into(),
            capabilities: vec!["startup.cli-help.v1".into()],
            setup_assets: vec![SetupAsset {
                name: "fixture".into(),
                relative_path: asset.file_name().unwrap().to_string_lossy().into_owned(),
                digest: digest_tree(asset).unwrap(),
            }],
            extra: BTreeMap::new(),
        }
    }

    /// Synthetic help stays alive long enough for post-exec RSS sampling. The
    /// campaign tests exercise orchestration, not the speed of libtest's help.
    #[test]
    fn startup_help_fixture() {
        std::thread::sleep(Duration::from_millis(100));
        println!("Usage: Commands:");
    }

    fn startup_campaign(command: &str) -> (tempfile::TempDir, PathBuf, PathBuf, PathBuf) {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let repository = workspace_root().unwrap();
        let registry_dir = root.join("performance/registry");
        let profile_dir = root.join("performance/profiles");
        fs::create_dir_all(&registry_dir).unwrap();
        fs::create_dir_all(&profile_dir).unwrap();

        let command = if command == "--help" {
            vec![
                "--exact",
                "perf::command::tests::startup_help_fixture",
                "--nocapture",
            ]
        } else {
            vec![command]
        };
        let registry =
            fs::read_to_string(repository.join("performance/registry/workloads-v1.toml"))
                .unwrap()
                .replace(
                    "help_signatures = [\"Usage:\", \"Commands:\"]",
                    "help_signatures = [\"Usage:\"]",
                )
                .replace("command = [\"--help\"]", &format!("command = {command:?}"));
        fs::write(registry_dir.join("workloads-v1.toml"), registry).unwrap();
        fs::copy(
            repository.join("performance/profiles/developer-v1.toml"),
            profile_dir.join("developer-v1.toml"),
        )
        .unwrap();

        let binary = std::env::current_exe().unwrap();
        let build_dir = root.join("target/perf/builds/test");
        let asset = build_dir.join("assets");
        fs::create_dir_all(&asset).unwrap();
        fs::write(asset.join("fixture"), "data").unwrap();
        let manifest_path = build_dir.join("build-manifest.json");
        atomic_json(&manifest_path, &manifest(&binary, &asset)).unwrap();
        let run_dir = root.join("target/perf/runs/test");
        (directory, binary, manifest_path, run_dir)
    }

    #[test]
    fn candidate_failures_are_product_failures_but_baseline_failures_are_invalid() {
        let base = Sample {
            schema_id: SAMPLE_SCHEMA.into(),
            run_id: "run".into(),
            profile_id: "profile".into(),
            workload_id: "workload".into(),
            tuple_id: "tuple".into(),
            block_id: 0,
            orientation_set_id: 0,
            orientation: Orientation::Abba,
            pair_id: 0,
            position: 1,
            role: BinaryRole::Baseline,
            measured: true,
            started_at: "now".into(),
            process: ProcessMeasurement {
                wall_time_ns: 1,
                cpu_time_ns: None,
                peak_rss_bytes: None,
                stdout_first_byte_ns: None,
                stdout_last_byte_ns: None,
                resource_source: "test".into(),
                exit_code: Some(1),
                timed_out: false,
                stdout_bytes: 0,
                stderr_bytes: 0,
                stdout_truncated: false,
                stderr_truncated: false,
            },
            external: Default::default(),
            validation: Validation {
                state: SampleState::Invalid,
                code: "process_exit".into(),
                message: "failed".into(),
            },
            extra: BTreeMap::new(),
        };
        assert_eq!(classification_exit(&base), EXIT_INVALID);
        let mut candidate = base.clone();
        candidate.role = BinaryRole::Candidate;
        candidate.validation.state = SampleState::ProductFailure;
        assert_eq!(classification_exit(&candidate), EXIT_REGRESSION);

        let baseline = workload_failure_validation(BinaryRole::Baseline, "oracle differed");
        assert_eq!(baseline.state, SampleState::Invalid);
        assert_eq!(baseline.code, "workload_oracle");
        let candidate = workload_failure_validation(BinaryRole::Candidate, "oracle differed");
        assert_eq!(candidate.state, SampleState::ProductFailure);
    }

    #[test]
    fn artifact_names_do_not_create_paths() {
        assert_eq!(artifact_name("a/b", "c:d"), "a-b-c-d");
    }

    #[test]
    fn capability_mismatch_is_detected_symmetrically() {
        let baseline = vec!["startup.cli-help.v1".to_owned()];
        let candidate = vec!["run.create.v1".to_owned()];
        assert!(has_capability(&baseline, "startup.cli-help.v1"));
        assert!(!has_capability(&candidate, "startup.cli-help.v1"));
    }

    #[test]
    fn fixture_signature_mismatch_is_detected() {
        let signatures = vec!["Usage:".to_owned(), "Commands:".to_owned()];
        assert_eq!(
            missing_signature("Usage: vigilo", &signatures),
            Some("Commands:")
        );
        assert_eq!(
            missing_signature("Usage: vigilo\nCommands:", &signatures),
            None
        );
    }

    #[test]
    fn manifest_verification_rejects_changed_or_escaping_inputs() {
        let directory = tempfile::tempdir().unwrap();
        let binary = directory.path().join("vigilo-test");
        let asset = directory.path().join("assets");
        let manifest_path = directory.path().join("build-manifest.json");
        fs::write(&binary, "binary").unwrap();
        fs::create_dir(&asset).unwrap();
        fs::write(asset.join("fixture"), "data").unwrap();
        let valid = manifest(&binary, &asset);
        atomic_json(&manifest_path, &valid).unwrap();
        assert_eq!(
            load_and_verify_manifest(&manifest_path, &binary)
                .unwrap()
                .executable_digest,
            valid.executable_digest
        );

        fs::write(&binary, "changed").unwrap();
        assert!(verify_executable_unchanged(&binary, &valid).is_err());
        fs::write(&binary, "binary").unwrap();
        let mut unsafe_manifest = valid.clone();
        unsafe_manifest.setup_assets[0].relative_path = "../outside".into();
        atomic_json(&manifest_path, &unsafe_manifest).unwrap();
        assert!(load_and_verify_manifest(&manifest_path, &binary).is_err());

        let mut wrong_schema = valid;
        wrong_schema.schema_id = "build-manifest/v2".into();
        atomic_json(&manifest_path, &wrong_schema).unwrap();
        assert!(load_and_verify_manifest(&manifest_path, &binary).is_err());
    }

    #[test]
    fn evaluator_manifest_ignores_only_the_mutable_rustc_cache() {
        let directory = tempfile::tempdir().unwrap();
        let binary = directory.path().join("vigilo-test");
        let asset = directory.path().join("evaluator");
        let manifest_path = directory.path().join("build-manifest.json");
        fs::write(&binary, "binary").unwrap();
        fs::create_dir_all(asset.join("target")).unwrap();
        fs::write(asset.join("fixture.wasm"), "wasm").unwrap();
        fs::write(asset.join("target/.rustc_info.json"), "first cache").unwrap();

        let mut legacy = manifest(&binary, &asset);
        legacy.setup_assets[0].name = "evaluator-fixture".into();
        atomic_json(&manifest_path, &legacy).unwrap();
        assert!(load_and_verify_manifest(&manifest_path, &binary).is_ok());
        fs::write(asset.join("target/.rustc_info.json"), "second cache").unwrap();
        assert!(load_and_verify_manifest(&manifest_path, &binary).is_err());

        legacy.setup_assets[0].digest = setup_asset_digest("evaluator-fixture", &asset).unwrap();
        atomic_json(&manifest_path, &legacy).unwrap();
        assert!(load_and_verify_manifest(&manifest_path, &binary).is_ok());
        fs::write(asset.join("target/.rustc_info.json"), "third cache").unwrap();
        assert!(load_and_verify_manifest(&manifest_path, &binary).is_ok());
        fs::write(asset.join("fixture.wasm"), "changed wasm").unwrap();
        assert!(load_and_verify_manifest(&manifest_path, &binary).is_err());
    }

    #[test]
    fn selected_workloads_require_implementation_capability_and_budget() {
        let root = workspace_root().unwrap();
        let registry = load_registry(&root).unwrap();
        let mut profile = load_profile(&root, "developer-v1").unwrap();
        let requested = vec!["startup.cli-help.v1".into()];
        let selected = select_workloads(&profile, &registry, &requested).unwrap();
        assert!(require_implemented(&selected).is_ok());
        assert!(validate_campaign_budget(&profile, &selected, 1_000, false).is_ok());

        let directory = tempfile::tempdir().unwrap();
        let binary = directory.path().join("binary");
        let asset = directory.path().join("assets");
        fs::write(&binary, "binary").unwrap();
        fs::create_dir(&asset).unwrap();
        let manifest = manifest(&binary, &asset);
        assert!(require_capabilities(&selected, &[&manifest]).is_ok());
        let mut missing = manifest.clone();
        missing.capabilities.clear();
        assert!(require_capabilities(&selected, &[&missing]).is_err());

        profile.campaign_cap_secs = 0;
        let selected = select_workloads(&profile, &registry, &requested).unwrap();
        assert!(validate_campaign_budget(&profile, &selected, 1, false).is_err());
        profile.campaign_cap_secs = 10;
        profile.max_artifact_bytes = 1;
        let selected = select_workloads(&profile, &registry, &requested).unwrap();
        assert!(validate_campaign_budget(&profile, &selected, 1, false).is_err());

        let mut planned_registry = registry.clone();
        planned_registry.workloads[0].status = ImplementationStatus::Planned;
        let selected = select_workloads(&profile, &planned_registry, &requested).unwrap();
        assert!(require_implemented(&selected).is_err());
    }

    #[test]
    fn calibration_timing_modes_fail_closed_before_execution() {
        let root = workspace_root().unwrap();
        let registry = load_registry(&root).unwrap();
        let calibration = load_profile(&root, "calibration-v1").unwrap();
        let selected = select_workloads(&calibration, &registry, &[]).unwrap();
        let mut environment = environment_manifest();
        let directory = tempfile::tempdir().unwrap();
        let binary = directory.path().join("binary");
        let assets = directory.path().join("assets");
        fs::write(&binary, "binary").unwrap();
        fs::create_dir(&assets).unwrap();
        let baseline = manifest(&binary, &assets);
        let mut candidate = baseline.clone();

        assert!(
            validate_timing_contract(&selected, &environment, &baseline, &candidate, true,).is_ok()
        );
        environment.canonical = true;
        environment.environment_id = "aws-m6i-2xlarge-al2023-v1".into();
        assert!(
            validate_timing_contract(&selected, &environment, &baseline, &candidate, true,).is_ok()
        );
        candidate.executable_digest = "different".into();
        assert!(
            validate_timing_contract(&selected, &environment, &baseline, &candidate, true,)
                .is_err()
        );

        let capacity = load_profile(&root, "capacity-v1").unwrap();
        let selected = select_workloads(&capacity, &registry, &[]).unwrap();
        assert!(
            validate_timing_contract(&selected, &environment, &baseline, &baseline, false,).is_ok()
        );
        assert!(
            validate_timing_contract(&selected, &environment, &baseline, &baseline, true,).is_err()
        );
    }

    #[test]
    fn gating_budget_resolution_requires_exact_tuple_and_environment() {
        use crate::perf::model::{
            BudgetEntry,
            BudgetPolicy,
        };

        let root = workspace_root().unwrap();
        let registry = load_registry(&root).unwrap();
        let profile = load_profile(&root, "developer-v1").unwrap();
        let selected =
            select_workloads(&profile, &registry, &["startup.cli-help.v1".into()]).unwrap();
        let policy = BudgetPolicy {
            schema_id: super::super::model::BUDGET_SCHEMA.into(),
            id: "reference-v2-budgets".into(),
            environment_id: "canonical-v1".into(),
            calibration_id: "calibration-1".into(),
            approved_at: "2026-08-21T00:00:00Z".into(),
            approved_by: "reviewer".into(),
            entries: vec![BudgetEntry {
                workload_id: "startup.cli-help.v1".into(),
                tuple_id: "cold-help".into(),
                metric: "wall_time".into(),
                practical_budget: 0.05,
                minimum_blocks: 2,
                max_residual_orientation_effect: 0.02,
            }],
        };
        let entry = resolve_budget_entry(&selected[0], "canonical-v1", Some(&policy)).unwrap();
        assert_eq!(entry.unwrap().practical_budget, 0.05);
        assert!(resolve_budget_entry(&selected[0], "other", Some(&policy)).is_err());

        let mut missing = policy;
        missing.entries[0].tuple_id = "other".into();
        assert!(resolve_budget_entry(&selected[0], "canonical-v1", Some(&missing)).is_err());
        assert!(
            resolve_budget_entry(&selected[0], "canonical-v1", None)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn confirmation_requires_a_matching_inconclusive_over_budget_signal() {
        use crate::perf::model::{
            BudgetEntry,
            MetricComparison,
        };

        let budget = BudgetEntry {
            workload_id: "startup.cli-help.v1".into(),
            tuple_id: "cold-help".into(),
            metric: "wall_time".into(),
            practical_budget: 0.05,
            minimum_blocks: 2,
            max_residual_orientation_effect: 0.02,
        };
        let metric = MetricComparison {
            name: "wall_time".into(),
            unit: "nanoseconds".into(),
            direction: "positive_is_harmful".into(),
            baseline_median: 100.0,
            candidate_median: 120.0,
            raw_candidate_delta: 0.2,
            harmful_effect: 0.2,
            confidence_lower: 0.10,
            confidence_upper: 0.30,
            practical_budget: Some(0.05),
            verdict: Verdict::Inconclusive,
            valid_abba_blocks: 3,
            valid_baab_blocks: 3,
            unmatched_blocks: 0,
            residual_orientation_effect: 0.0,
            orientation_medians: BTreeMap::new(),
            position_medians: BTreeMap::new(),
            estimator: "test".into(),
            bootstrap_seed: 1,
        };
        let mut evidence = ConfirmationEvidence {
            report: ReportDocument {
                schema_id: REPORT_SCHEMA.into(),
                run_id: "prior".into(),
                kind: "compare".into(),
                status: "invalid".into(),
                profile_id: "reference-v2".into(),
                generated_at: "2026-08-21T00:00:00Z".into(),
                comparisons: vec![ComparisonDocument {
                    schema_id: super::super::model::COMPARISON_SCHEMA.into(),
                    run_id: "prior".into(),
                    profile_id: "reference-v2".into(),
                    workload_id: budget.workload_id.clone(),
                    tuple_id: budget.tuple_id.clone(),
                    baseline_digest: "base".into(),
                    candidate_digest: "candidate".into(),
                    metrics: vec![metric],
                    verdict: Verdict::Inconclusive,
                    extra: BTreeMap::new(),
                }],
                failures: vec!["confirmation required".into()],
                artifact_files: Vec::new(),
                extra: BTreeMap::new(),
            },
            campaign: CampaignManifest {
                schema_id: REPORT_SCHEMA.into(),
                run_id: "prior".into(),
                kind: "compare".into(),
                status: "invalid".into(),
                created_at: "2026-08-21T00:00:00Z".into(),
                completed_at: Some("2026-08-21T00:01:00Z".into()),
                profile_id: "reference-v2".into(),
                schedule_seed: 1,
                selected_workloads: vec!["startup.cli-help.v1:cold-help".into()],
                planned_measured_executions: 24,
                planned_preconditioning_executions: 2,
                artifact_limit_bytes: 1,
                environment_file: "environment.json".into(),
                registry_file: "provenance/workload-registry.json".into(),
                profile_file: "provenance/profile.json".into(),
                budget_policy_file: Some("provenance/budget-policy.json".into()),
                baseline_manifest: Some("base.json".into()),
                candidate_manifest: "candidate.json".into(),
                failure: Some("confirmation required".into()),
                extra: BTreeMap::new(),
            },
        };
        assert!(confirms_prior_signal(
            &evidence,
            &budget.workload_id,
            &budget.tuple_id,
            &budget
        ));

        evidence.report.comparisons[0].metrics[0].verdict = Verdict::Pass;
        assert!(!confirms_prior_signal(
            &evidence,
            &budget.workload_id,
            &budget.tuple_id,
            &budget
        ));
        evidence.report.comparisons[0].metrics[0].verdict = Verdict::Inconclusive;
        evidence.report.comparisons[0].metrics[0].confidence_lower = 0.01;
        assert!(!confirms_prior_signal(
            &evidence,
            &budget.workload_id,
            &budget.tuple_id,
            &budget
        ));
    }

    #[test]
    fn startup_oracle_classifies_every_process_failure_mode() {
        let root = workspace_root().unwrap();
        let registry = load_registry(&root).unwrap();
        let profile = load_profile(&root, "developer-v1").unwrap();
        let selected =
            select_workloads(&profile, &registry, &["startup.cli-help.v1".into()]).unwrap();
        let selected = &selected[0];

        let valid = process_outcome("Usage: vigilo\nCommands:");
        assert_eq!(
            validate_outcome(&valid, selected, BinaryRole::Single).state,
            SampleState::Valid
        );
        let mut timeout = process_outcome("");
        timeout.timed_out = true;
        assert_eq!(
            validate_outcome(&timeout, selected, BinaryRole::Baseline).code,
            "process_timeout"
        );
        assert_eq!(
            validate_outcome(&timeout, selected, BinaryRole::Baseline).state,
            SampleState::Invalid
        );
        let mut exit = process_outcome("");
        exit.exit_code = Some(2);
        assert_eq!(
            validate_outcome(&exit, selected, BinaryRole::Candidate).code,
            "process_exit"
        );
        let mut truncated = process_outcome("");
        truncated.stdout.truncated = true;
        assert_eq!(
            validate_outcome(&truncated, selected, BinaryRole::Single).code,
            "output_truncated"
        );
        assert_eq!(
            validate_outcome(&process_outcome("Usage:"), selected, BinaryRole::Single).code,
            "fixture_mismatch"
        );
    }

    #[test]
    fn required_metrics_must_be_present_before_a_sample_is_valid() {
        let root = workspace_root().unwrap();
        let registry = load_registry(&root).unwrap();
        let profile = load_profile(&root, "developer-v1").unwrap();
        let selected =
            select_workloads(&profile, &registry, &["startup.cli-help.v1".into()]).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let binary = directory.path().join("binary");
        let assets = directory.path().join("assets");
        fs::write(&binary, "binary").unwrap();
        fs::create_dir(&assets).unwrap();
        let manifest = manifest(&binary, &assets);
        let mut outcome = process_outcome("Usage: vigilo\nCommands:");

        assert!(
            missing_required_metric(
                &selected[0],
                &manifest,
                &outcome,
                &ExternalMeasurements::default(),
            )
            .is_none()
        );
        outcome.peak_rss_bytes = None;
        assert_eq!(
            missing_required_metric(
                &selected[0],
                &manifest,
                &outcome,
                &ExternalMeasurements::default(),
            ),
            Some("peak_rss")
        );

        let mut registry = registry;
        let workload = registry
            .workloads
            .iter_mut()
            .find(|workload| workload.id == "run.create.v1")
            .unwrap();
        workload.required_metrics = vec!["sql".into()];
        let profile_workload = super::super::model::ProfileWorkload {
            id: workload.id.clone(),
            tuple: workload.tuples[0].clone(),
            blocks: 2,
            timing: "informative".into(),
        };
        let selected = SelectedWorkload {
            profile: &profile_workload,
            workload,
        };
        assert_eq!(
            missing_required_metric(
                &selected,
                &manifest,
                &process_outcome(""),
                &ExternalMeasurements::default(),
            ),
            Some("sql")
        );
    }

    #[test]
    fn campaign_helpers_write_bounded_artifacts() {
        let directory = tempfile::tempdir().unwrap();
        checkpoint(directory.path(), &[], &[], &[]).unwrap();
        assert!(directory.path().join("samples.jsonl").is_file());
        assert_eq!(artifact_files(false, false).len(), 8);
        assert_eq!(artifact_files(true, false).len(), 9);
        assert_eq!(artifact_files(false, true).len(), 10);
        assert!(ensure_before_deadline(Instant::now() + Duration::from_secs(1)).is_ok());
        assert!(ensure_before_deadline(Instant::now()).is_err());
        assert!(enforce_artifact_limit(directory.path(), u64::MAX).is_ok());
        fs::write(directory.path().join("nonempty"), "x").unwrap();
        assert!(enforce_artifact_limit(directory.path(), 0).is_err());
        assert_eq!(
            binary_for(BinaryRole::Baseline, Path::new("a"), Path::new("b")),
            Path::new("a")
        );
        assert_eq!(
            binary_for(BinaryRole::Single, Path::new("a"), Path::new("b")),
            Path::new("b")
        );
        assert_eq!(
            environment_manifest().schema_id,
            super::super::model::ENVIRONMENT_SCHEMA
        );
    }

    #[test]
    fn startup_single_campaign_records_pass_and_product_failure() {
        let (workspace, binary, build_manifest, run_dir) = startup_campaign("--help");
        let exit = run_single_at(
            workspace.path(),
            RunArgs {
                profile: "developer-v1".into(),
                workloads: vec!["startup.cli-help.v1".into()],
                binary: binary.clone(),
                build_manifest: build_manifest.clone(),
                output: Some(run_dir.clone()),
                schedule_seed: Some(7),
            },
        )
        .unwrap();
        assert_eq!(exit, EXIT_PASS);
        assert_eq!(
            fs::read_to_string(run_dir.join("samples.jsonl"))
                .unwrap()
                .lines()
                .count(),
            4
        );
        assert!(
            fs::read_to_string(run_dir.join("report.json"))
                .unwrap()
                .contains("\"status\": \"pass\"")
        );

        let (workspace, binary, build_manifest, run_dir) = startup_campaign("--definitely-invalid");
        let exit = run_single_at(
            workspace.path(),
            RunArgs {
                profile: "developer-v1".into(),
                workloads: vec!["startup.cli-help.v1".into()],
                binary,
                build_manifest,
                output: Some(run_dir.clone()),
                schedule_seed: None,
            },
        )
        .unwrap();
        assert_eq!(exit, EXIT_REGRESSION);
        assert!(
            run_dir
                .join("failures/preconditioning-failure/sample.json")
                .is_file()
        );
        assert!(
            fs::read_to_string(run_dir.join("report.json"))
                .unwrap()
                .contains("\"status\": \"failure\"")
        );
    }

    #[test]
    fn startup_comparison_records_balanced_informative_result() {
        let (workspace, binary, build_manifest, run_dir) = startup_campaign("--help");
        let exit = compare_at(
            workspace.path(),
            CompareArgs {
                profile: "developer-v1".into(),
                workloads: vec!["startup.cli-help.v1".into()],
                baseline_binary: binary.clone(),
                baseline_manifest: build_manifest.clone(),
                candidate_binary: binary,
                candidate_manifest: build_manifest,
                output: Some(run_dir.clone()),
                schedule_seed: Some(11),
                confirmation_of: None,
            },
        )
        .unwrap();

        assert_eq!(exit, EXIT_PASS);
        assert_eq!(
            fs::read_to_string(run_dir.join("samples.jsonl"))
                .unwrap()
                .lines()
                .count(),
            8
        );
        assert_eq!(
            fs::read_to_string(run_dir.join("blocks.jsonl"))
                .unwrap()
                .lines()
                .count(),
            2
        );
        assert_eq!(
            fs::read_to_string(run_dir.join("comparisons.jsonl"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        assert!(
            fs::read_to_string(run_dir.join("report.json"))
                .unwrap()
                .contains("informative")
        );
    }
}
