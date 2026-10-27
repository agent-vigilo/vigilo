//! Deployment capacity projections derived from bounded performance evidence.
//!
//! A projection combines one versioned deployment workload mix with a completed
//! capacity run. The calculator exposes its equations and limit provenance,
//! bootstraps the derived model as one unit, and refuses supported labels when
//! evidence is nonlinear, saturated, incompatible, or operationally unbounded.

use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    fs,
    path::{
        Path,
        PathBuf,
    },
};

use anyhow::{
    Context,
    Result,
    bail,
};
use chrono::{
    NaiveDate,
    Utc,
};
use clap::Args;
use rand::{
    Rng,
    SeedableRng,
    rngs::StdRng,
};
use serde::{
    Deserialize,
    Serialize,
    de::DeserializeOwned,
};

use super::{
    EXIT_INVALID,
    EXIT_PASS,
    artifact::{
        atomic_json,
        atomic_text,
        require_artifact_path,
        workspace_root,
    },
    fixture,
    model::{
        CAPACITY_SCHEMA,
        CampaignManifest,
        CapacityDocument,
        CapacityPoint,
        ENVIRONMENT_SCHEMA,
        EnvironmentManifest,
        REPORT_SCHEMA,
        ReportDocument,
        SAMPLE_SCHEMA,
        Sample,
        SampleState,
        WorkloadRegistry,
    },
    provenance,
    workload::capacity_tuple,
};

const DEPLOYMENT_SCHEMA: &str = "deployment/v1";
const PROJECTION_SCHEMA: &str = "projections/v1";
const CAPACITY_WORKLOAD: &str = "system.capacity.v1";
const REQUIRED_PROFILE: &str = "capacity-v1";
const MINIMUM_VALID_BOOTSTRAP_FRACTION: f64 = 0.90;
const REQUIRED_BOUNDED_PATH: &str = "coordinator_broker_publish_confirm";

/// Arguments for projecting a named deployment from a completed capacity run.
#[derive(Debug, Args)]
pub struct ProjectArgs {
    /// Completed `capacity-v1` run with raw samples and analyzed capacity evidence.
    #[arg(long = "run", alias = "run-dir")]
    run_dir: PathBuf,
    /// Named workload and infrastructure assumptions under `performance/deployments`.
    #[arg(long)]
    deployment: PathBuf,
}

/// Named workload and infrastructure assumptions for one deployment estimate.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeploymentInput {
    schema_id: String,
    id: String,
    description: String,
    target_utilization: f64,
    bootstrap_resamples: usize,
    bootstrap_seed: u64,
    capacity_fixture: String,
    workload: WorkloadMix,
    agent: AgentInput,
    amplification: AmplificationInput,
    configuration: DeploymentConfiguration,
    #[serde(default)]
    limits: Vec<LimitInput>,
    boundedness: Vec<BoundedPath>,
    staging: Option<StagingInput>,
}

/// Traffic shape whose resource demand is being projected.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkloadMix {
    peak_case_rate: f64,
    duty_cycle: f64,
    concurrent_runs_peak: u32,
    run_classes: Vec<RunClass>,
    payload_classes: Vec<PayloadClass>,
    evaluator_mix: Vec<EvaluatorClass>,
    result_bytes_per_case: u64,
    diagnostic_bytes_per_case: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunClass {
    name: String,
    cases: u64,
    fraction: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PayloadClass {
    bytes: u64,
    fraction: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvaluatorClass {
    name: String,
    evaluators_per_case: u32,
    fraction: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentInput {
    mean_latency_ms: f64,
    p95_latency_ms: f64,
    connection_policy: String,
}

/// Retry and delivery multipliers kept separate by protocol meaning.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AmplificationInput {
    attempts_per_useful_case: f64,
    successful_agent_attempts_per_useful_case: f64,
    chunk_deliveries_per_useful_chunk: f64,
    durable_events_per_useful_chunk: f64,
    publish_attempts_per_durable_event: f64,
    acknowledgements_per_delivery: f64,
    retry_publishes_per_useful_chunk: f64,
    quarantine_publishes_per_useful_chunk: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeploymentConfiguration {
    cases_per_chunk: u32,
    worker_inflight_chunks: u32,
    wasm_concurrency_per_worker: u32,
    database_connections_per_target: u32,
    coordinator_tick_ms: u64,
    placements: Vec<PlacementInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlacementInput {
    alias: String,
    fraction: f64,
}

/// Capacity limit with enough provenance to audit how it was obtained.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LimitInput {
    resource: Resource,
    capacity: f64,
    usable_fraction: f64,
    provenance: LimitProvenance,
    source: String,
    observed_on: String,
    hardware: String,
    configuration: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LimitProvenance {
    Measured,
    ProviderDocumented,
    OperatorDeclared,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BoundedPath {
    path: String,
    bounded: bool,
    provenance: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StagingInput {
    source_run_id: String,
    workers: u32,
    observed_case_rate: f64,
    maximum_relative_error: f64,
}

/// Resources whose demand and usable limit are shown independently.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum Resource {
    HostCpuCores,
    HostMemoryBytes,
    PostgresqlStatementsPerSecond,
    PostgresqlWalBytesPerSecond,
    RabbitmqPublishesPerSecond,
    RabbitmqDeliveriesPerSecond,
    HttpRequestsPerSecond,
    HttpConcurrency,
    WasmEvaluationsPerSecond,
    StorageBytesPerDay,
    CoordinatorEventsPerSecond,
}

const REQUIRED_RESOURCES: [Resource; 11] = [
    Resource::HostCpuCores,
    Resource::HostMemoryBytes,
    Resource::PostgresqlStatementsPerSecond,
    Resource::PostgresqlWalBytesPerSecond,
    Resource::RabbitmqPublishesPerSecond,
    Resource::RabbitmqDeliveriesPerSecond,
    Resource::HttpRequestsPerSecond,
    Resource::HttpConcurrency,
    Resource::WasmEvaluationsPerSecond,
    Resource::StorageBytesPerDay,
    Resource::CoordinatorEventsPerSecond,
];

/// Strength of the projection evidence, ordered from unusable to direct match.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ProjectionConfidence {
    Invalid,
    Directional,
    Planning,
    Calibrated,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Interval {
    lower: f64,
    estimate: f64,
    upper: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkerProjection {
    required: u32,
    interval: Interval,
    usable_case_rate_per_worker: Interval,
    formula: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DemandEstimate {
    resource: Resource,
    unit: String,
    demand: Interval,
    formula: String,
    limit: Option<LimitInput>,
    usable_limit: Option<f64>,
    utilization: Option<f64>,
    supported_peak_case_rate: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StagingValidation {
    source_run_id: String,
    workers: u32,
    projected_case_rate: f64,
    observed_case_rate: f64,
    relative_error: f64,
    maximum_relative_error: f64,
    accepted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompletionEstimate {
    representative_cases: f64,
    seconds: Interval,
    formula: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AmplificationDemand {
    attempt_rate_peak: f64,
    successful_agent_rate_peak: f64,
    useful_chunk_rate_peak: f64,
    durable_event_rate_peak: f64,
    publish_attempt_rate_peak: f64,
    delivery_rate_peak: f64,
    acknowledgement_rate_peak: f64,
    retry_publish_rate_peak: f64,
    quarantine_publish_rate_peak: f64,
}

/// Versioned projection artifact used by terminal and Markdown renderers.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProjectionDocument {
    schema_id: String,
    source_run_id: String,
    source_build_digest: String,
    environment_id: String,
    deployment_id: String,
    resolved_deployment: DeploymentInput,
    generated_at: String,
    confidence: ProjectionConfidence,
    target_peak_case_rate: f64,
    average_case_rate: f64,
    r1_knee: Option<Interval>,
    r2_knee: Option<Interval>,
    scale_efficiency_2: Option<Interval>,
    workers: WorkerProjection,
    amplification_demand: AmplificationDemand,
    demands: Vec<DemandEstimate>,
    bottleneck: Option<Resource>,
    overall_capacity_case_rate: Option<f64>,
    target_within_declared_limits: Option<bool>,
    completion: Option<CompletionEstimate>,
    staging_validation: Option<StagingValidation>,
    warnings: Vec<String>,
    failures: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct CapacityTargets {
    capacity: CapacityPolicy,
}

#[derive(Debug, Clone, Deserialize)]
struct CapacityPolicy {
    minimum_throughput_gain: f64,
    latency_growth: f64,
    maximum_worker_cpu_percent: f64,
    maximum_service_cpu_percent: f64,
    minimum_samples_per_point: usize,
    minimum_scale_efficiency: f64,
}

#[derive(Debug, Clone)]
struct EvidenceDraw {
    r1_knee: f64,
    r2_knee: f64,
    scale_efficiency_2: f64,
    cpu_seconds_per_case: f64,
    memory_bytes_per_worker: f64,
    sql_statements_per_case: f64,
    wal_bytes_per_case: f64,
    http_requests_per_case: f64,
    database_bytes_per_case: f64,
}

#[derive(Debug, Clone)]
struct ProjectionMath {
    workers: u32,
    usable_rate_per_worker: f64,
    demands: BTreeMap<Resource, f64>,
    completion_seconds: f64,
}

#[derive(Debug, Clone, Copy)]
struct ProjectionContext {
    canonical_environment: bool,
    exact_fixture_match: bool,
}

/// Generates a versioned capacity projection and human-readable views.
pub fn execute(args: ProjectArgs) -> Result<u8> {
    let root = workspace_root()?;
    let run_dir = require_artifact_path(&root, &args.run_dir)?;
    let deployment_path = resolve_deployment_path(&root, &args.deployment)?;
    let deployment: DeploymentInput = read_toml(&deployment_path)?;
    validate_deployment(&deployment)?;
    let capacity: CapacityDocument = read_json(&run_dir.join("capacity.json"))?;
    let samples: Vec<Sample> = read_jsonl(&run_dir.join("samples.jsonl"))?;
    let report: ReportDocument = read_json(&run_dir.join("report.json"))?;
    let campaign: CampaignManifest = read_json(&run_dir.join("campaign.json"))?;
    let environment: EnvironmentManifest = read_json(&run_dir.join("environment.json"))?;
    validate_sources(&capacity, &report, &campaign, &environment)?;
    let frozen = provenance::load(&run_dir, &campaign)
        .context("load projection source campaign provenance")?;
    if capacity.build_digest != frozen.candidate_manifest.executable_digest {
        bail!("capacity evidence build digest differs from frozen campaign provenance");
    }
    let policy = load_capacity_policy(&root)?;
    let context = ProjectionContext {
        canonical_environment: environment.canonical,
        exact_fixture_match: exact_fixture_match(&root, &deployment, &capacity, &frozen.registry)?,
    };
    let document = project(&deployment, &capacity, &samples, &policy, context)?;
    atomic_json(&run_dir.join("projections.json"), &document)?;
    atomic_text(&run_dir.join("projection.md"), &markdown(&document))?;
    print_terminal(&document, &run_dir);
    Ok(
        if matches!(
            document.confidence,
            ProjectionConfidence::Planning | ProjectionConfidence::Calibrated
        ) {
            EXIT_PASS
        } else {
            EXIT_INVALID
        },
    )
}

/// Recreates the human-readable projection view without changing its JSON source.
pub(super) fn rerender(run_dir: &Path) -> Result<bool> {
    let path = run_dir.join("projections.json");
    if !path.is_file() {
        return Ok(false);
    }
    let document: ProjectionDocument = read_json(&path)?;
    if document.schema_id != PROJECTION_SCHEMA {
        bail!("unsupported projection schema: {}", document.schema_id);
    }
    atomic_text(&run_dir.join("projection.md"), &markdown(&document))?;
    print_terminal(&document, run_dir);
    Ok(true)
}

/// Validates checked-in deployment inputs without requiring performance evidence.
pub(super) fn validate_repository_contract(root: &Path) -> Result<usize> {
    let directory = root.join("performance/deployments");
    let mut paths = fs::read_dir(&directory)
        .with_context(|| format!("read {}", directory.display()))?
        .map(|entry| Ok(entry?.path()))
        .collect::<Result<Vec<_>>>()?;
    paths.sort();
    if paths.is_empty() {
        bail!("at least one named deployment input is required");
    }
    let mut ids = BTreeSet::new();
    for path in &paths {
        if path.extension().and_then(|value| value.to_str()) != Some("toml") {
            bail!(
                "deployment directory contains a non-TOML input: {}",
                path.display()
            );
        }
        let deployment: DeploymentInput = read_toml(path)?;
        validate_deployment(&deployment)?;
        if !ids.insert(deployment.id.clone()) {
            bail!("duplicate deployment ID: {}", deployment.id);
        }
        let broker = deployment
            .boundedness
            .iter()
            .find(|path| path.path == REQUIRED_BOUNDED_PATH)
            .context("checked-in deployment omitted the coordinator broker boundedness contract")?;
        if broker.bounded {
            bail!(
                "checked-in deployment contradicts the current unbounded coordinator broker path"
            );
        }
        fixture::load(root, &deployment.capacity_fixture)?;
    }
    Ok(paths.len())
}

fn project(
    deployment: &DeploymentInput,
    capacity: &CapacityDocument,
    samples: &[Sample],
    policy: &CapacityPolicy,
    context: ProjectionContext,
) -> Result<ProjectionDocument> {
    let generated_at = Utc::now().to_rfc3339();
    let mut failures = validate_evidence(capacity, samples, policy);
    let evidence_valid = failures.is_empty();
    for path in &deployment.boundedness {
        if !path.bounded {
            failures.push(format!(
                "{} is operationally unbounded ({})",
                path.path, path.provenance
            ));
        }
    }

    let limits = deployment
        .limits
        .iter()
        .map(|limit| (limit.resource, limit.clone()))
        .collect::<BTreeMap<_, _>>();
    let missing_limits = REQUIRED_RESOURCES
        .iter()
        .filter(|resource| !limits.contains_key(resource))
        .copied()
        .collect::<Vec<_>>();

    let base = if evidence_valid {
        match derive_evidence(samples.iter().collect(), policy) {
            Ok(draw) => Some(draw),
            Err(error) => {
                failures.push(format!("{error:#}"));
                None
            }
        }
    } else {
        None
    };
    let bootstrap = if base.is_some() {
        match bootstrap_evidence(samples, policy, deployment) {
            Ok(draws) => draws,
            Err(error) => {
                failures.push(format!("{error:#}"));
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    let invalid_workers = WorkerProjection {
        required: 0,
        interval: Interval {
            lower: 0.0,
            estimate: 0.0,
            upper: 0.0,
        },
        usable_case_rate_per_worker: Interval {
            lower: 0.0,
            estimate: 0.0,
            upper: 0.0,
        },
        formula: "ceil(R_peak / (lower(R1_knee) * scale_efficiency_2 * target_utilization))".into(),
    };
    let Some(base) = base else {
        return Ok(invalid_document(
            deployment,
            capacity,
            generated_at,
            invalid_workers,
            missing_limits,
            failures,
        ));
    };
    if bootstrap.is_empty() {
        return Ok(invalid_document(
            deployment,
            capacity,
            generated_at,
            invalid_workers,
            missing_limits,
            failures,
        ));
    }

    let base_math = calculate_math(&base, deployment);
    let draw_math = bootstrap
        .iter()
        .map(|draw| calculate_math(draw, deployment))
        .collect::<Vec<_>>();
    let worker_rates = draw_math
        .iter()
        .map(|math| math.usable_rate_per_worker)
        .collect::<Vec<_>>();
    let worker_counts = draw_math
        .iter()
        .map(|math| f64::from(math.workers))
        .collect::<Vec<_>>();
    let conservative_rate = percentile(&worker_rates, 0.025);
    let conservative_workers = ceil_u32(deployment.workload.peak_case_rate / conservative_rate);
    let workers = WorkerProjection {
        required: conservative_workers,
        interval: interval(&worker_counts, f64::from(base_math.workers)),
        usable_case_rate_per_worker: interval(&worker_rates, base_math.usable_rate_per_worker),
        formula: "ceil(R_peak / (lower(R1_knee) * scale_efficiency_2 * target_utilization))".into(),
    };

    let demands = REQUIRED_RESOURCES
        .iter()
        .map(|resource| {
            let values = draw_math
                .iter()
                .map(|math| math.demands[resource])
                .collect::<Vec<_>>();
            let demand = interval(&values, base_math.demands[resource]);
            let limit = limits.get(resource).cloned();
            let usable_limit = limit
                .as_ref()
                .map(|limit| limit.capacity * limit.usable_fraction);
            let utilization = usable_limit.map(|limit| demand.upper / limit);
            let supported_peak_case_rate = usable_limit.and_then(|limit| {
                supported_peak_case_rate(
                    *resource,
                    limit,
                    demand.estimate,
                    deployment.workload.peak_case_rate,
                    &base,
                    deployment,
                )
            });
            DemandEstimate {
                resource: *resource,
                unit: resource_unit(*resource).into(),
                demand,
                formula: resource_formula(*resource).into(),
                limit,
                usable_limit,
                utilization,
                supported_peak_case_rate,
            }
        })
        .collect::<Vec<_>>();
    let bottleneck = demands
        .iter()
        .filter_map(|demand| demand.utilization.map(|value| (demand.resource, value)))
        .max_by(|left, right| left.1.total_cmp(&right.1))
        .map(|(resource, _)| resource);
    let overall_capacity_case_rate = if missing_limits.is_empty() {
        demands
            .iter()
            .map(|demand| demand.supported_peak_case_rate)
            .collect::<Option<Vec<_>>>()
            .and_then(|rates| rates.into_iter().min_by(f64::total_cmp))
    } else {
        None
    };
    let target_within_declared_limits = if missing_limits.is_empty() {
        Some(demands.iter().all(|demand| {
            demand
                .utilization
                .is_some_and(|utilization| utilization <= 1.0)
        }))
    } else {
        None
    };

    let staging_validation = deployment.staging.as_ref().map(|staging| {
        let projected = base.r1_knee * f64::from(staging.workers) * base.scale_efficiency_2;
        let relative_error =
            (projected - staging.observed_case_rate).abs() / staging.observed_case_rate;
        StagingValidation {
            source_run_id: staging.source_run_id.clone(),
            workers: staging.workers,
            projected_case_rate: projected,
            observed_case_rate: staging.observed_case_rate,
            relative_error,
            maximum_relative_error: staging.maximum_relative_error,
            accepted: relative_error <= staging.maximum_relative_error,
        }
    });
    if staging_validation
        .as_ref()
        .is_some_and(|validation| !validation.accepted)
    {
        failures.push("staging observation exceeds the declared projection-error tolerance".into());
    }

    let mut warnings = missing_limit_warnings(&missing_limits);
    if conservative_workers > 2 {
        warnings.push(
            "worker count exceeds the measured one/two-worker range; scenarios are directional"
                .into(),
        );
    }
    if !context.exact_fixture_match {
        warnings.push("deployment workload mix does not exactly match the capacity fixture".into());
    }
    if !context.canonical_environment {
        warnings.push("capacity evidence was not collected on the canonical environment".into());
    }
    if target_within_declared_limits == Some(false) {
        warnings.push("target demand exceeds at least one declared usable dependency limit".into());
    }

    let confidence = if !failures.is_empty() {
        ProjectionConfidence::Invalid
    } else if !missing_limits.is_empty() || conservative_workers > 2 || !context.exact_fixture_match
    {
        ProjectionConfidence::Directional
    } else if context.exact_fixture_match
        && context.canonical_environment
        && staging_validation.as_ref().is_some_and(|validation| {
            validation.accepted && validation.workers == conservative_workers
        })
    {
        ProjectionConfidence::Calibrated
    } else {
        ProjectionConfidence::Planning
    };
    let efficiencies = bootstrap
        .iter()
        .map(|draw| draw.scale_efficiency_2)
        .collect::<Vec<_>>();
    let completion_values = draw_math
        .iter()
        .map(|math| math.completion_seconds)
        .collect::<Vec<_>>();
    let representative_cases = weighted_cases_per_run(&deployment.workload);
    Ok(ProjectionDocument {
        schema_id: PROJECTION_SCHEMA.into(),
        source_run_id: capacity.source_run_id.clone(),
        source_build_digest: capacity.build_digest.clone(),
        environment_id: capacity.environment_id.clone(),
        deployment_id: deployment.id.clone(),
        resolved_deployment: deployment.clone(),
        generated_at,
        confidence,
        target_peak_case_rate: deployment.workload.peak_case_rate,
        average_case_rate: deployment.workload.peak_case_rate * deployment.workload.duty_cycle,
        r1_knee: Some(interval(
            &bootstrap
                .iter()
                .map(|draw| draw.r1_knee)
                .collect::<Vec<_>>(),
            base.r1_knee,
        )),
        r2_knee: Some(interval(
            &bootstrap
                .iter()
                .map(|draw| draw.r2_knee)
                .collect::<Vec<_>>(),
            base.r2_knee,
        )),
        scale_efficiency_2: Some(interval(&efficiencies, base.scale_efficiency_2)),
        workers,
        amplification_demand: amplification_demand(deployment),
        demands,
        bottleneck,
        overall_capacity_case_rate,
        target_within_declared_limits,
        completion: Some(CompletionEstimate {
            representative_cases,
            seconds: interval(&completion_values, base_math.completion_seconds),
            formula:
                "coordinator_tick + representative_cases * concurrent_runs / target_peak_case_rate"
                    .into(),
        }),
        staging_validation,
        warnings,
        failures,
    })
}

fn invalid_document(
    deployment: &DeploymentInput,
    capacity: &CapacityDocument,
    generated_at: String,
    workers: WorkerProjection,
    missing_limits: Vec<Resource>,
    failures: Vec<String>,
) -> ProjectionDocument {
    ProjectionDocument {
        schema_id: PROJECTION_SCHEMA.into(),
        source_run_id: capacity.source_run_id.clone(),
        source_build_digest: capacity.build_digest.clone(),
        environment_id: capacity.environment_id.clone(),
        deployment_id: deployment.id.clone(),
        resolved_deployment: deployment.clone(),
        generated_at,
        confidence: ProjectionConfidence::Invalid,
        target_peak_case_rate: deployment.workload.peak_case_rate,
        average_case_rate: deployment.workload.peak_case_rate * deployment.workload.duty_cycle,
        r1_knee: None,
        r2_knee: None,
        scale_efficiency_2: None,
        workers,
        amplification_demand: amplification_demand(deployment),
        demands: Vec::new(),
        bottleneck: None,
        overall_capacity_case_rate: None,
        target_within_declared_limits: None,
        completion: None,
        staging_validation: None,
        warnings: missing_limit_warnings(&missing_limits),
        failures,
    }
}

fn validate_deployment(deployment: &DeploymentInput) -> Result<()> {
    if deployment.schema_id != DEPLOYMENT_SCHEMA {
        bail!("unsupported deployment schema: {}", deployment.schema_id);
    }
    validate_id(&deployment.id, "deployment")?;
    validate_id(&deployment.capacity_fixture, "capacity fixture")?;
    if deployment.description.trim().is_empty()
        || !(deployment.target_utilization.is_finite()
            && deployment.target_utilization > 0.0
            && deployment.target_utilization <= 1.0)
        || !(100..=10_000).contains(&deployment.bootstrap_resamples)
    {
        bail!("deployment has invalid description, utilization, or bootstrap count");
    }
    let workload = &deployment.workload;
    if !(workload.peak_case_rate.is_finite()
        && workload.peak_case_rate > 0.0
        && workload.duty_cycle.is_finite()
        && workload.duty_cycle > 0.0
        && workload.duty_cycle <= 1.0)
        || workload.concurrent_runs_peak == 0
        || workload.result_bytes_per_case == 0
    {
        bail!("deployment workload rates, concurrency, and result size must be positive");
    }
    validate_distribution(
        workload
            .run_classes
            .iter()
            .map(|item| (item.name.as_str(), item.cases, item.fraction)),
        "run classes",
    )?;
    validate_distribution(
        workload
            .payload_classes
            .iter()
            .map(|item| ("payload", item.bytes, item.fraction)),
        "payload classes",
    )?;
    validate_distribution(
        workload.evaluator_mix.iter().map(|item| {
            (
                item.name.as_str(),
                u64::from(item.evaluators_per_case),
                item.fraction,
            )
        }),
        "evaluator mix",
    )?;
    if !(deployment.agent.mean_latency_ms.is_finite()
        && deployment.agent.mean_latency_ms > 0.0
        && deployment.agent.p95_latency_ms.is_finite()
        && deployment.agent.p95_latency_ms >= deployment.agent.mean_latency_ms)
        || deployment.agent.connection_policy.trim().is_empty()
    {
        bail!("agent latency and connection policy are invalid");
    }
    let amplification = &deployment.amplification;
    let nonnegative = [
        amplification.retry_publishes_per_useful_chunk,
        amplification.quarantine_publishes_per_useful_chunk,
    ];
    let at_least_one = [
        amplification.attempts_per_useful_case,
        amplification.successful_agent_attempts_per_useful_case,
        amplification.chunk_deliveries_per_useful_chunk,
        amplification.durable_events_per_useful_chunk,
        amplification.publish_attempts_per_durable_event,
        amplification.acknowledgements_per_delivery,
    ];
    if nonnegative
        .iter()
        .any(|value| !value.is_finite() || *value < 0.0)
        || at_least_one
            .iter()
            .any(|value| !value.is_finite() || *value < 1.0)
        || amplification.successful_agent_attempts_per_useful_case
            > amplification.attempts_per_useful_case
    {
        bail!("retry and protocol amplification values are inconsistent");
    }
    let configuration = &deployment.configuration;
    if [
        configuration.cases_per_chunk,
        configuration.worker_inflight_chunks,
        configuration.wasm_concurrency_per_worker,
        configuration.database_connections_per_target,
    ]
    .contains(&0)
        || configuration.coordinator_tick_ms == 0
    {
        bail!("deployment concurrency and batching configuration must be positive");
    }
    validate_distribution(
        configuration
            .placements
            .iter()
            .map(|item| (item.alias.as_str(), 1, item.fraction)),
        "placement distribution",
    )?;
    let mut resources = BTreeSet::new();
    for limit in &deployment.limits {
        if !resources.insert(limit.resource) {
            bail!("duplicate capacity limit for {:?}", limit.resource);
        }
        if !(limit.capacity.is_finite()
            && limit.capacity > 0.0
            && limit.usable_fraction.is_finite()
            && limit.usable_fraction > 0.0
            && limit.usable_fraction <= 1.0)
            || [
                limit.source.as_str(),
                limit.hardware.as_str(),
                limit.configuration.as_str(),
            ]
            .iter()
            .any(|value| value.trim().is_empty())
            || NaiveDate::parse_from_str(&limit.observed_on, "%Y-%m-%d").is_err()
        {
            bail!(
                "capacity limit {:?} has invalid value or provenance",
                limit.resource
            );
        }
    }
    if deployment.boundedness.is_empty() {
        bail!("deployment must declare boundedness of required operational paths");
    }
    let mut paths = BTreeSet::new();
    for path in &deployment.boundedness {
        if path.path.trim().is_empty()
            || path.provenance.trim().is_empty()
            || !paths.insert(path.path.as_str())
        {
            bail!("boundedness paths require unique names and provenance");
        }
    }
    if !paths.contains(REQUIRED_BOUNDED_PATH) {
        bail!("deployment must declare boundedness for {REQUIRED_BOUNDED_PATH}");
    }
    if let Some(staging) = &deployment.staging
        && (staging.source_run_id.trim().is_empty()
            || !matches!(staging.workers, 1 | 2 | 4)
            || !(staging.observed_case_rate.is_finite() && staging.observed_case_rate > 0.0)
            || !(staging.maximum_relative_error.is_finite()
                && staging.maximum_relative_error > 0.0
                && staging.maximum_relative_error <= 1.0))
    {
        bail!("staging validation input is invalid");
    }
    Ok(())
}

fn validate_distribution<'a>(
    values: impl Iterator<Item = (&'a str, u64, f64)>,
    name: &str,
) -> Result<()> {
    let values = values.collect::<Vec<_>>();
    if values.is_empty()
        || values.iter().any(|(id, count, fraction)| {
            id.trim().is_empty() || *count == 0 || !fraction.is_finite() || *fraction <= 0.0
        })
        || (values.iter().map(|(_, _, value)| value).sum::<f64>() - 1.0).abs() > 1e-6
    {
        bail!("{name} must be nonempty, positive, and sum to 1.0");
    }
    Ok(())
}

fn validate_id(value: &str, name: &str) -> Result<()> {
    if value.is_empty()
        || !value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_')
        })
    {
        bail!("{name} ID contains unsupported characters: {value}");
    }
    Ok(())
}

fn validate_sources(
    capacity: &CapacityDocument,
    report: &ReportDocument,
    campaign: &CampaignManifest,
    environment: &EnvironmentManifest,
) -> Result<()> {
    if capacity.schema_id != CAPACITY_SCHEMA {
        bail!("unsupported capacity schema: {}", capacity.schema_id);
    }
    if report.schema_id != REPORT_SCHEMA
        || campaign.schema_id != REPORT_SCHEMA
        || report.run_id != campaign.run_id
        || report.run_id != capacity.source_run_id
        || report.kind != "run"
        || campaign.kind != "run"
        || report.status != "pass"
        || campaign.status != "pass"
        || report.profile_id != REQUIRED_PROFILE
        || campaign.profile_id != REQUIRED_PROFILE
    {
        bail!("projection source is not one completed capacity-v1 campaign");
    }
    if environment.schema_id != ENVIRONMENT_SCHEMA
        || capacity.environment_id != environment.environment_id
        || capacity.canonical != environment.canonical
    {
        bail!("capacity and environment identities do not match");
    }
    Ok(())
}

fn validate_evidence(
    capacity: &CapacityDocument,
    samples: &[Sample],
    policy: &CapacityPolicy,
) -> Vec<String> {
    let mut failures = capacity.failures.clone();
    if capacity.schema_id != CAPACITY_SCHEMA {
        failures.push(format!(
            "unsupported capacity schema: {}",
            capacity.schema_id
        ));
    }
    if !capacity.supports_linear_projection {
        failures.push("capacity evidence does not support linear worker projection".into());
    }
    if capacity
        .scale_efficiency_2
        .is_none_or(|value| !value.is_finite() || value < policy.minimum_scale_efficiency)
    {
        failures.push("one/two-worker scale efficiency is missing or below policy".into());
    }
    if samples.is_empty() {
        failures.push("capacity run contains no raw samples".into());
    }
    for sample in samples {
        if sample.schema_id != SAMPLE_SCHEMA
            || sample.run_id != capacity.source_run_id
            || sample.profile_id != REQUIRED_PROFILE
            || sample.workload_id != CAPACITY_WORKLOAD
            || sample.validation.state != SampleState::Valid
            || !sample.measured
        {
            failures.push("capacity input contains an invalid or unrelated measured sample".into());
            break;
        }
        if sample
            .external
            .service_cpu_percent
            .is_some_and(|cpu| cpu >= policy.maximum_service_cpu_percent)
        {
            failures.push("shared-service CPU saturation invalidates projection evidence".into());
            break;
        }
    }
    if let Err(error) = validate_capacity_curve(&capacity.points) {
        failures.push(format!("{error:#}"));
    }
    if let Err(error) = validate_capacity_reduction(capacity, samples, policy) {
        failures.push(format!("{error:#}"));
    }
    failures.sort();
    failures.dedup();
    failures
}

fn validate_capacity_reduction(
    capacity: &CapacityDocument,
    samples: &[Sample],
    policy: &CapacityPolicy,
) -> Result<()> {
    let references = samples.iter().collect::<Vec<_>>();
    let actual = aggregate_points(&references)?;
    if actual.len() != capacity.points.len() {
        bail!("capacity artifact point count does not match its raw samples");
    }
    for (actual, recorded) in actual.iter().zip(&capacity.points) {
        if (
            actual.workers,
            actual.load_step,
            actual.cases,
            actual.samples,
        ) != (
            recorded.workers,
            recorded.load_step,
            recorded.cases,
            recorded.samples,
        ) || !approximately_equal(actual.throughput_per_second, recorded.throughput_per_second)
            || !approximately_equal(actual.p95_latency_ms, recorded.p95_latency_ms)
            || !approximately_equal(
                actual.process_cpu_percent_per_worker,
                recorded.process_cpu_percent_per_worker,
            )
            || !optional_approximately_equal(
                actual.service_cpu_percent,
                recorded.service_cpu_percent,
            )
        {
            bail!("capacity artifact does not reproduce from its raw samples");
        }
    }
    let draw = derive_evidence(references, policy)?;
    if capacity
        .scale_efficiency_2
        .is_none_or(|value| !approximately_equal(value, draw.scale_efficiency_2))
    {
        bail!("capacity scale efficiency does not reproduce from raw samples");
    }
    for (workers, expected) in [(1, draw.r1_knee), (2, draw.r2_knee)] {
        let knee = capacity
            .knees
            .iter()
            .find(|knee| knee.workers == workers)
            .with_context(|| format!("capacity artifact has no {workers}-worker knee"))?;
        if knee
            .knee_throughput_per_second
            .is_none_or(|value| !approximately_equal(value, expected))
            || knee.knee_step.is_none()
            || knee.observed_rate_lower_bound.is_some()
        {
            bail!("capacity knee does not reproduce from raw samples");
        }
    }
    Ok(())
}

fn approximately_equal(left: f64, right: f64) -> bool {
    left.is_finite()
        && right.is_finite()
        && (left - right).abs() <= left.abs().max(right.abs()).max(1.0) * 1e-9
}

fn optional_approximately_equal(left: Option<f64>, right: Option<f64>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => approximately_equal(left, right),
        (None, None) => true,
        _ => false,
    }
}

fn validate_capacity_curve(points: &[CapacityPoint]) -> Result<()> {
    for workers in [1, 2] {
        let mut selected = points
            .iter()
            .filter(|point| point.workers == workers)
            .collect::<Vec<_>>();
        selected.sort_by_key(|point| point.load_step);
        if selected.len() < 3 {
            bail!("capacity curve requires at least three points for {workers} worker(s)");
        }
        for pair in selected.windows(2) {
            if pair[1].throughput_per_second < pair[0].throughput_per_second * 0.90 {
                bail!(
                    "capacity curve is nonlinear: throughput falls materially for {workers} worker(s)"
                );
            }
        }
    }
    Ok(())
}

fn derive_evidence(samples: Vec<&Sample>, policy: &CapacityPolicy) -> Result<EvidenceDraw> {
    let points = aggregate_points(&samples)?;
    validate_capacity_curve(&points)?;
    let r1_knee = find_knee_rate(1, &points, policy)?;
    let r2_knee = find_knee_rate(2, &points, policy)?;
    let scale_efficiency_2 = (r2_knee / (2.0 * r1_knee)).min(1.0);
    if scale_efficiency_2 < policy.minimum_scale_efficiency {
        bail!(
            "scale efficiency {:.3} is below policy {:.3}",
            scale_efficiency_2,
            policy.minimum_scale_efficiency
        );
    }
    let ratios = samples
        .iter()
        .map(|sample| sample_ratios(sample))
        .collect::<Result<Vec<_>>>()?;
    Ok(EvidenceDraw {
        r1_knee,
        r2_knee,
        scale_efficiency_2,
        cpu_seconds_per_case: median(&ratios.iter().map(|value| value[0]).collect::<Vec<_>>()),
        memory_bytes_per_worker: median(&ratios.iter().map(|value| value[1]).collect::<Vec<_>>()),
        sql_statements_per_case: median(&ratios.iter().map(|value| value[2]).collect::<Vec<_>>()),
        wal_bytes_per_case: median(&ratios.iter().map(|value| value[3]).collect::<Vec<_>>()),
        http_requests_per_case: median(&ratios.iter().map(|value| value[4]).collect::<Vec<_>>()),
        database_bytes_per_case: median(&ratios.iter().map(|value| value[5]).collect::<Vec<_>>()),
    })
}

fn sample_ratios(sample: &Sample) -> Result<[f64; 6]> {
    let cases = exact_cases(sample)?;
    let (workers, _) = capacity_tuple(&sample.tuple_id)?;
    if cases == 0 || sample.process.wall_time_ns == 0 {
        bail!("capacity sample has zero useful work or elapsed time");
    }
    let per_case = |value: u64| value as f64 / cases as f64;
    Ok([
        sample
            .process
            .cpu_time_ns
            .map(|value| value as f64 / 1_000_000_000.0 / cases as f64)
            .context("capacity sample has no process CPU measurement")?,
        sample
            .process
            .peak_rss_bytes
            .map(|value| value as f64 / workers as f64)
            .context("capacity sample has no peak RSS measurement")?,
        per_case(
            sample
                .external
                .sql_calls
                .context("capacity sample has no SQL measurement")?,
        ),
        per_case(
            sample
                .external
                .wal_bytes
                .context("capacity sample has no WAL measurement")?,
        ),
        per_case(
            sample
                .external
                .http_requests
                .context("capacity sample has no HTTP measurement")?,
        ),
        sample
            .external
            .database_bytes_delta
            .map(|value| value.max(0) as f64 / cases as f64)
            .context("capacity sample has no database-size measurement")?,
    ])
}

fn aggregate_points(samples: &[&Sample]) -> Result<Vec<CapacityPoint>> {
    let mut grouped = BTreeMap::<&str, Vec<&Sample>>::new();
    for sample in samples {
        grouped.entry(&sample.tuple_id).or_default().push(sample);
    }
    let mut points = Vec::new();
    for (tuple, samples) in grouped {
        let (workers, load_step) = capacity_tuple(tuple)?;
        let cases = exact_cases(samples[0])?;
        if samples
            .iter()
            .map(|sample| exact_cases(sample))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .any(|actual| actual != cases)
        {
            bail!("capacity tuple {tuple} has inconsistent exact case counts");
        }
        let throughputs = samples
            .iter()
            .map(|sample| cases as f64 * 1_000_000_000.0 / sample.process.wall_time_ns as f64)
            .collect::<Vec<_>>();
        let latencies = samples
            .iter()
            .map(|sample| sample.process.wall_time_ns as f64 / 1_000_000.0)
            .collect::<Vec<_>>();
        let process_cpu = samples
            .iter()
            .map(|sample| {
                sample
                    .process
                    .cpu_time_ns
                    .map(|cpu| {
                        cpu as f64 * 100.0 / sample.process.wall_time_ns as f64 / workers as f64
                    })
                    .context("capacity sample has no process CPU measurement")
            })
            .collect::<Result<Vec<_>>>()?;
        let service_cpu = samples
            .iter()
            .map(|sample| {
                sample
                    .external
                    .service_cpu_percent
                    .context("capacity sample has no shared-service CPU measurement")
            })
            .collect::<Result<Vec<_>>>()?;
        points.push(CapacityPoint {
            workers: u32::try_from(workers)?,
            load_step: u32::try_from(load_step)?,
            cases,
            samples: samples.len(),
            throughput_per_second: median(&throughputs),
            p95_latency_ms: percentile(&latencies, 0.95),
            process_cpu_percent_per_worker: process_cpu
                .into_iter()
                .max_by(f64::total_cmp)
                .unwrap_or_default(),
            service_cpu_percent: service_cpu.into_iter().max_by(f64::total_cmp),
        });
    }
    points.sort_by_key(|point| (point.workers, point.load_step));
    Ok(points)
}

fn exact_cases(sample: &Sample) -> Result<u64> {
    let cases = sample
        .external
        .durable_counts
        .get("executions")
        .copied()
        .context("capacity sample has no exact execution count")?;
    Ok(u64::try_from(cases)?)
}

fn find_knee_rate(workers: u32, points: &[CapacityPoint], policy: &CapacityPolicy) -> Result<f64> {
    let mut selected = points
        .iter()
        .filter(|point| point.workers == workers)
        .collect::<Vec<_>>();
    selected.sort_by_key(|point| point.load_step);
    if selected.len() < 3 {
        bail!("capacity evidence has fewer than three points for {workers} worker(s)");
    }
    for point in &selected {
        if point.samples < policy.minimum_samples_per_point {
            bail!("capacity point has fewer than the required samples");
        }
    }
    super::calibration::find_knee(
        workers,
        points,
        policy.minimum_throughput_gain,
        policy.latency_growth,
        policy.maximum_worker_cpu_percent,
        policy.maximum_service_cpu_percent,
    )?
    .knee_throughput_per_second
    .with_context(|| format!("bounded staircase found no sustainable knee for {workers} worker(s)"))
}

fn bootstrap_evidence(
    samples: &[Sample],
    policy: &CapacityPolicy,
    deployment: &DeploymentInput,
) -> Result<Vec<EvidenceDraw>> {
    let mut grouped = BTreeMap::<&str, Vec<&Sample>>::new();
    for sample in samples {
        grouped.entry(&sample.tuple_id).or_default().push(sample);
    }
    let mut rng = StdRng::seed_from_u64(deployment.bootstrap_seed);
    let mut draws = Vec::with_capacity(deployment.bootstrap_resamples);
    for _ in 0..deployment.bootstrap_resamples {
        let mut resampled = Vec::with_capacity(samples.len());
        for group in grouped.values() {
            for _ in 0..group.len() {
                resampled.push(group[rng.random_range(0..group.len())]);
            }
        }
        if let Ok(draw) = derive_evidence(resampled, policy) {
            draws.push(draw);
        }
    }
    let minimum =
        (deployment.bootstrap_resamples as f64 * MINIMUM_VALID_BOOTSTRAP_FRACTION).ceil() as usize;
    if draws.len() < minimum {
        bail!(
            "only {} of {} whole-model bootstrap draws were valid; {minimum} required",
            draws.len(),
            deployment.bootstrap_resamples
        );
    }
    Ok(draws)
}

fn calculate_math(evidence: &EvidenceDraw, deployment: &DeploymentInput) -> ProjectionMath {
    let workload = &deployment.workload;
    let peak = workload.peak_case_rate;
    let average = peak * workload.duty_cycle;
    let usable_rate_per_worker =
        evidence.r1_knee * evidence.scale_efficiency_2 * deployment.target_utilization;
    let workers = ceil_u32(peak / usable_rate_per_worker);
    let cases_per_run = weighted_cases_per_run(workload);
    let run_rate = peak / cases_per_run;
    let amplification_demand = amplification_demand(deployment);
    let evaluators = workload
        .evaluator_mix
        .iter()
        .map(|item| f64::from(item.evaluators_per_case) * item.fraction)
        .sum::<f64>();
    let amplification = &deployment.amplification;
    let attempt_rate = amplification_demand.attempt_rate_peak;
    let http_rate = (attempt_rate * evidence.http_requests_per_case).max(attempt_rate);
    let storage_per_case = evidence.database_bytes_per_case.max(
        workload
            .result_bytes_per_case
            .saturating_add(workload.diagnostic_bytes_per_case) as f64,
    );
    let demands = BTreeMap::from([
        (Resource::HostCpuCores, peak * evidence.cpu_seconds_per_case),
        (
            Resource::HostMemoryBytes,
            f64::from(workers) * evidence.memory_bytes_per_worker,
        ),
        (
            Resource::PostgresqlStatementsPerSecond,
            peak * evidence.sql_statements_per_case,
        ),
        (
            Resource::PostgresqlWalBytesPerSecond,
            peak * evidence.wal_bytes_per_case,
        ),
        (
            Resource::RabbitmqPublishesPerSecond,
            amplification_demand.publish_attempt_rate_peak
                + amplification_demand.retry_publish_rate_peak
                + amplification_demand.quarantine_publish_rate_peak,
        ),
        (
            Resource::RabbitmqDeliveriesPerSecond,
            amplification_demand.delivery_rate_peak,
        ),
        (Resource::HttpRequestsPerSecond, http_rate),
        (
            Resource::HttpConcurrency,
            http_rate * deployment.agent.mean_latency_ms / 1_000.0,
        ),
        (
            Resource::WasmEvaluationsPerSecond,
            peak * amplification.successful_agent_attempts_per_useful_case * evaluators,
        ),
        (
            Resource::StorageBytesPerDay,
            average * storage_per_case * 86_400.0,
        ),
        (
            Resource::CoordinatorEventsPerSecond,
            run_rate + amplification_demand.durable_event_rate_peak,
        ),
    ]);
    ProjectionMath {
        workers,
        usable_rate_per_worker,
        demands,
        completion_seconds: deployment.configuration.coordinator_tick_ms as f64 / 1_000.0
            + cases_per_run * f64::from(workload.concurrent_runs_peak) / peak,
    }
}

fn amplification_demand(deployment: &DeploymentInput) -> AmplificationDemand {
    let peak = deployment.workload.peak_case_rate;
    let run_rate = peak / weighted_cases_per_run(&deployment.workload);
    let useful_chunk_rate_peak = run_rate
        * deployment
            .workload
            .run_classes
            .iter()
            .map(|class| {
                class.fraction
                    * (class.cases as f64 / f64::from(deployment.configuration.cases_per_chunk))
                        .ceil()
            })
            .sum::<f64>();
    let values = &deployment.amplification;
    let durable_event_rate_peak = useful_chunk_rate_peak * values.durable_events_per_useful_chunk;
    let delivery_rate_peak = useful_chunk_rate_peak * values.chunk_deliveries_per_useful_chunk;
    AmplificationDemand {
        attempt_rate_peak: peak * values.attempts_per_useful_case,
        successful_agent_rate_peak: peak * values.successful_agent_attempts_per_useful_case,
        useful_chunk_rate_peak,
        durable_event_rate_peak,
        publish_attempt_rate_peak: durable_event_rate_peak
            * values.publish_attempts_per_durable_event,
        delivery_rate_peak,
        acknowledgement_rate_peak: delivery_rate_peak * values.acknowledgements_per_delivery,
        retry_publish_rate_peak: useful_chunk_rate_peak * values.retry_publishes_per_useful_chunk,
        quarantine_publish_rate_peak: useful_chunk_rate_peak
            * values.quarantine_publishes_per_useful_chunk,
    }
}

fn supported_peak_case_rate(
    resource: Resource,
    usable_limit: f64,
    demand: f64,
    target_peak: f64,
    evidence: &EvidenceDraw,
    deployment: &DeploymentInput,
) -> Option<f64> {
    if !(usable_limit.is_finite() && usable_limit > 0.0 && demand.is_finite() && demand > 0.0) {
        return None;
    }
    if resource == Resource::HostMemoryBytes {
        let workers = (usable_limit / evidence.memory_bytes_per_worker).floor();
        return Some(
            workers
                * evidence.r1_knee
                * evidence.scale_efficiency_2
                * deployment.target_utilization,
        );
    }
    Some(target_peak * usable_limit / demand)
}

fn weighted_cases_per_run(workload: &WorkloadMix) -> f64 {
    workload
        .run_classes
        .iter()
        .map(|class| class.cases as f64 * class.fraction)
        .sum()
}

fn interval(values: &[f64], estimate: f64) -> Interval {
    Interval {
        lower: percentile(values, 0.025),
        estimate,
        upper: percentile(values, 0.975),
    }
}

fn median(values: &[f64]) -> f64 {
    percentile(values, 0.5)
}

fn percentile(values: &[f64], quantile: f64) -> f64 {
    let mut values = values.to_vec();
    values.sort_by(f64::total_cmp);
    let index = ((values.len() as f64 * quantile).ceil() as usize)
        .saturating_sub(1)
        .min(values.len().saturating_sub(1));
    values.get(index).copied().unwrap_or_default()
}

fn ceil_u32(value: f64) -> u32 {
    value.ceil().clamp(1.0, f64::from(u32::MAX)) as u32
}

fn missing_limit_warnings(missing: &[Resource]) -> Vec<String> {
    missing
        .iter()
        .map(|resource| format!("missing usable limit for {}", resource_name(*resource)))
        .collect()
}

fn resource_name(resource: Resource) -> &'static str {
    match resource {
        Resource::HostCpuCores => "host_cpu_cores",
        Resource::HostMemoryBytes => "host_memory_bytes",
        Resource::PostgresqlStatementsPerSecond => "postgresql_statements_per_second",
        Resource::PostgresqlWalBytesPerSecond => "postgresql_wal_bytes_per_second",
        Resource::RabbitmqPublishesPerSecond => "rabbitmq_publishes_per_second",
        Resource::RabbitmqDeliveriesPerSecond => "rabbitmq_deliveries_per_second",
        Resource::HttpRequestsPerSecond => "http_requests_per_second",
        Resource::HttpConcurrency => "http_concurrency",
        Resource::WasmEvaluationsPerSecond => "wasm_evaluations_per_second",
        Resource::StorageBytesPerDay => "storage_bytes_per_day",
        Resource::CoordinatorEventsPerSecond => "coordinator_events_per_second",
    }
}

fn resource_unit(resource: Resource) -> &'static str {
    match resource {
        Resource::HostCpuCores => "cores",
        Resource::HostMemoryBytes => "bytes",
        Resource::PostgresqlStatementsPerSecond => "statements/s",
        Resource::PostgresqlWalBytesPerSecond => "bytes/s",
        Resource::RabbitmqPublishesPerSecond => "publishes/s",
        Resource::RabbitmqDeliveriesPerSecond => "deliveries/s",
        Resource::HttpRequestsPerSecond => "requests/s",
        Resource::HttpConcurrency => "requests",
        Resource::WasmEvaluationsPerSecond => "evaluations/s",
        Resource::StorageBytesPerDay => "bytes/day",
        Resource::CoordinatorEventsPerSecond => "events/s",
    }
}

fn resource_formula(resource: Resource) -> &'static str {
    match resource {
        Resource::HostCpuCores => "R_peak * measured_cpu_seconds_per_case",
        Resource::HostMemoryBytes => "workers_required * measured_peak_rss_per_worker",
        Resource::PostgresqlStatementsPerSecond => "R_peak * measured_sql_statements_per_case",
        Resource::PostgresqlWalBytesPerSecond => "R_peak * measured_wal_bytes_per_case",
        Resource::RabbitmqPublishesPerSecond => {
            "chunk_rate * (durable_events * publish_attempts + retry_publishes + quarantine_publishes)"
        }
        Resource::RabbitmqDeliveriesPerSecond => "chunk_rate * deliveries_per_useful_chunk",
        Resource::HttpRequestsPerSecond => "R_peak * attempts_per_useful_case",
        Resource::HttpConcurrency => "http_request_rate * agent_mean_latency_seconds",
        Resource::WasmEvaluationsPerSecond => {
            "R_peak * successful_agent_attempts * weighted_evaluators_per_case"
        }
        Resource::StorageBytesPerDay => {
            "R_average * max(measured_database_bytes, declared_result_and_diagnostics) * 86400"
        }
        Resource::CoordinatorEventsPerSecond => "run_arrival_rate + durable_chunk_event_rate",
    }
}

fn resolve_deployment_path(root: &Path, path: &Path) -> Result<PathBuf> {
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        bail!("deployment path cannot contain '..': {}", path.display());
    }
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };
    let allowed = root.join("performance/deployments");
    if !absolute.starts_with(&allowed) {
        bail!("deployment input must be under {}", allowed.display());
    }
    Ok(absolute)
}

fn exact_fixture_match(
    root: &Path,
    deployment: &DeploymentInput,
    capacity: &CapacityDocument,
    registry: &WorkloadRegistry,
) -> Result<bool> {
    let fixture = fixture::load(root, &deployment.capacity_fixture)?;
    let fixture_cases = u64::try_from(fixture.lifecycle.cases)?;
    let knee_cases = capacity
        .knees
        .iter()
        .filter_map(|knee| knee.knee_step)
        .map(|step| u64::from(step).saturating_mul(fixture_cases))
        .collect::<BTreeSet<_>>();
    let run_match = knee_cases.len() == 1
        && deployment.workload.run_classes.len() == 1
        && knee_cases.contains(&deployment.workload.run_classes[0].cases);
    let payload_match = deployment.workload.payload_classes.len() == 1
        && deployment.workload.payload_classes[0].bytes
            == u64::try_from(fixture.agent_payload_bytes)?;
    let evaluator_match = deployment.workload.evaluator_mix.len() == 1
        && deployment.workload.evaluator_mix[0].evaluators_per_case == 1;
    Ok(run_match
        && payload_match
        && evaluator_match
        && deployment.configuration.cases_per_chunk == registry.constants.run_chunk_size
        && deployment.configuration.worker_inflight_chunks
            == registry.constants.worker_default_inflight_chunks
        && deployment.configuration.wasm_concurrency_per_worker
            == registry.constants.wasm_concurrency
        && deployment.configuration.database_connections_per_target
            == registry.constants.database_connections_per_target
        && deployment.configuration.coordinator_tick_ms == registry.constants.coordinator_tick_ms
        && deployment.configuration.placements.len() == 1)
}

fn load_capacity_policy(root: &Path) -> Result<CapacityPolicy> {
    let path = root.join("performance/budgets/review-targets-v1.toml");
    let targets: CapacityTargets = read_toml(&path)?;
    Ok(targets.capacity)
}

fn read_toml<T: DeserializeOwned>(path: &Path) -> Result<T> {
    toml::from_str(&fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?)
        .with_context(|| format!("parse {}", path.display()))
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    serde_json::from_slice(&fs::read(path).with_context(|| format!("read {}", path.display()))?)
        .with_context(|| format!("parse {}", path.display()))
}

fn read_jsonl<T: DeserializeOwned>(path: &Path) -> Result<Vec<T>> {
    fs::read_to_string(path)
        .with_context(|| format!("read {}", path.display()))?
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(index, line)| {
            serde_json::from_str(line)
                .with_context(|| format!("parse {} line {}", path.display(), index + 1))
        })
        .collect()
}

fn markdown(document: &ProjectionDocument) -> String {
    let mut output = format!(
        "# Capacity projection\n\n- Deployment: `{}`\n- Source run: `{}`\n- Confidence: **{:?}**\n- Target: {:.2} cases/s peak, {:.2} cases/s average\n- Workers: {} (bootstrap [{:.0}, {:.0}])\n\n",
        document.deployment_id,
        document.source_run_id,
        document.confidence,
        document.target_peak_case_rate,
        document.average_case_rate,
        document.workers.required,
        document.workers.interval.lower,
        document.workers.interval.upper,
    );
    output.push_str(
        "| Resource | Demand | 95% interval | Usable limit | Utilization | Provenance |\n",
    );
    output.push_str("| --- | ---: | ---: | ---: | ---: | --- |\n");
    for demand in &document.demands {
        let usable = demand
            .usable_limit
            .map(|value| format!("{value:.2}"))
            .unwrap_or_else(|| "unknown".into());
        let utilization = demand
            .utilization
            .map(|value| format!("{:.1}%", value * 100.0))
            .unwrap_or_else(|| "unknown".into());
        let provenance = demand
            .limit
            .as_ref()
            .map(|limit| {
                format!(
                    "{:?}: {} ({})",
                    limit.provenance, limit.source, limit.observed_on
                )
            })
            .unwrap_or_else(|| "missing".into());
        output.push_str(&format!(
            "| `{}` | {:.2} {} | [{:.2}, {:.2}] | {} | {} | {} |\n",
            resource_name(demand.resource),
            demand.demand.estimate,
            demand.unit,
            demand.demand.lower,
            demand.demand.upper,
            usable,
            utilization,
            provenance,
        ));
    }
    if let Ok(resolved) = toml::to_string_pretty(&document.resolved_deployment) {
        output.push_str("\n## Resolved deployment input\n\n```toml\n");
        output.push_str(&resolved);
        output.push_str("```\n");
    }
    let amplification = &document.amplification_demand;
    output.push_str("\n## Retry and message amplification\n\n");
    output.push_str("| Flow | Peak rate |\n| --- | ---: |\n");
    for (name, value) in [
        ("case attempts", amplification.attempt_rate_peak),
        (
            "successful agent attempts",
            amplification.successful_agent_rate_peak,
        ),
        ("useful chunks", amplification.useful_chunk_rate_peak),
        ("durable events", amplification.durable_event_rate_peak),
        ("publish attempts", amplification.publish_attempt_rate_peak),
        ("deliveries", amplification.delivery_rate_peak),
        ("acknowledgements", amplification.acknowledgement_rate_peak),
        ("retry publishes", amplification.retry_publish_rate_peak),
        (
            "quarantine publishes",
            amplification.quarantine_publish_rate_peak,
        ),
    ] {
        output.push_str(&format!("| {name} | {value:.2}/s |\n"));
    }
    output.push_str("\n## Equations\n\n");
    output.push_str(&format!("- Workers: `{}`\n", document.workers.formula));
    for demand in &document.demands {
        output.push_str(&format!(
            "- `{}`: `{}`\n",
            resource_name(demand.resource),
            demand.formula
        ));
    }
    if let Some(validation) = &document.staging_validation {
        output.push_str(&format!(
            "\n## Staging validation\n\nProjected {:.2} cases/s versus observed {:.2} cases/s; error {:.1}% (limit {:.1}%): **{}**.\n",
            validation.projected_case_rate,
            validation.observed_case_rate,
            validation.relative_error * 100.0,
            validation.maximum_relative_error * 100.0,
            if validation.accepted { "accepted" } else { "rejected" },
        ));
    }
    if !document.warnings.is_empty() {
        output.push_str("\n## Warnings\n\n");
        for warning in &document.warnings {
            output.push_str(&format!("- {warning}\n"));
        }
    }
    if !document.failures.is_empty() {
        output.push_str("\n## Failures\n\n");
        for failure in &document.failures {
            output.push_str(&format!("- {failure}\n"));
        }
    }
    output
}

fn print_terminal(document: &ProjectionDocument, run_dir: &Path) {
    println!();
    println!("Capacity projection: {:?}", document.confidence);
    println!("Deployment: {}", document.deployment_id);
    println!("Target:     {:.2} cases/s", document.target_peak_case_rate);
    println!("Workers:    {}", document.workers.required);
    println!(
        "Bottleneck: {}",
        document.bottleneck.map(resource_name).unwrap_or("unknown")
    );
    for warning in &document.warnings {
        println!("Warning: {warning}");
    }
    for failure in &document.failures {
        println!("Failure: {failure}");
    }
    println!("Artifacts:  {}", run_dir.display());
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tempfile::Builder;

    use super::*;
    use crate::perf::{
        artifact::no_extra,
        model::{
            BUILD_SCHEMA,
            BinaryRole,
            BuildManifest,
            CapacityKnee,
            ExternalMeasurements,
            Orientation,
            ProcessMeasurement,
            SetupAsset,
            Validation,
        },
    };

    #[derive(Debug, Clone, Copy)]
    enum FixtureKind {
        Linear,
        Nonlinear,
        Saturated,
    }

    #[derive(Debug, Clone, Copy)]
    enum LimitSet {
        Complete,
        MissingStorage,
    }

    struct StagingFixture {
        observed_case_rate: f64,
        maximum_relative_error: f64,
    }

    #[test]
    fn complete_linear_evidence_produces_visible_capacity_math() {
        let result = project_fixture(FixtureKind::Linear, LimitSet::Complete, None).unwrap();

        assert_eq!(result.confidence, ProjectionConfidence::Planning);
        assert!(result.workers.required >= 1);
        assert!(result.workers.formula.contains("R1_knee"));
        assert!(result.demands.iter().all(|demand| demand.limit.is_some()));
        assert!(result.bottleneck.is_some());
        assert!(result.failures.is_empty());
    }

    #[test]
    fn missing_limit_is_directional_and_never_invents_capacity() {
        let result = project_fixture(FixtureKind::Linear, LimitSet::MissingStorage, None).unwrap();

        assert_eq!(result.confidence, ProjectionConfidence::Directional);
        let storage = result
            .demands
            .iter()
            .find(|demand| demand.resource == Resource::StorageBytesPerDay)
            .unwrap();
        assert!(storage.limit.is_none());
        assert!(result.overall_capacity_case_rate.is_none());
    }

    #[test]
    fn nonlinear_and_saturated_evidence_refuse_worker_projection() {
        for fixture in [FixtureKind::Nonlinear, FixtureKind::Saturated] {
            let result = project_fixture(fixture, LimitSet::Complete, None).unwrap();

            assert_eq!(result.confidence, ProjectionConfidence::Invalid);
            assert_eq!(result.workers.required, 0);
            assert!(!result.failures.is_empty());
        }
    }

    #[test]
    fn staging_error_is_recorded_and_mismatch_invalidates_support() {
        let staging = StagingFixture {
            observed_case_rate: 20.0,
            maximum_relative_error: 0.10,
        };
        let result =
            project_fixture(FixtureKind::Linear, LimitSet::Complete, Some(staging)).unwrap();

        let validation = result.staging_validation.unwrap();
        assert!(validation.relative_error > validation.maximum_relative_error);
        assert!(!validation.accepted);
        assert_eq!(result.confidence, ProjectionConfidence::Invalid);
    }

    #[test]
    fn exact_canonical_evidence_and_matching_staging_point_are_calibrated() {
        let deployment = deployment(
            LimitSet::Complete,
            Some(StagingFixture {
                observed_case_rate: 49.0,
                maximum_relative_error: 0.10,
            }),
        );
        let result = project_with_context(
            FixtureKind::Linear,
            deployment,
            ProjectionContext {
                canonical_environment: true,
                exact_fixture_match: true,
            },
        )
        .unwrap();

        assert_eq!(result.confidence, ProjectionConfidence::Calibrated);
        assert!(result.staging_validation.unwrap().accepted);
    }

    #[test]
    fn deployment_parser_rejects_bad_weights_and_duplicate_limits() {
        let mut bad_weights = deployment(LimitSet::Complete, None);
        bad_weights.workload.run_classes[0].fraction = 0.5;
        assert!(validate_deployment(&bad_weights).is_err());

        let mut deployment = deployment(LimitSet::Complete, None);
        deployment.limits.push(deployment.limits[0].clone());
        assert!(validate_deployment(&deployment).is_err());
    }

    #[test]
    fn projection_uses_the_earliest_capacity_knee_rule() {
        let points = vec![
            CapacityPoint {
                workers: 1,
                load_step: 1,
                cases: 100,
                samples: 8,
                throughput_per_second: 100.0,
                p95_latency_ms: 100.0,
                process_cpu_percent_per_worker: 50.0,
                service_cpu_percent: Some(30.0),
            },
            CapacityPoint {
                workers: 1,
                load_step: 2,
                cases: 200,
                samples: 8,
                throughput_per_second: 105.0,
                p95_latency_ms: 130.0,
                process_cpu_percent_per_worker: 60.0,
                service_cpu_percent: Some(40.0),
            },
            CapacityPoint {
                workers: 1,
                load_step: 4,
                cases: 400,
                samples: 8,
                throughput_per_second: 110.0,
                p95_latency_ms: 140.0,
                process_cpu_percent_per_worker: 95.0,
                service_cpu_percent: Some(50.0),
            },
        ];

        assert_eq!(find_knee_rate(1, &points, &policy()).unwrap(), 105.0);
    }

    #[test]
    fn command_records_current_unbounded_path_and_regenerates_its_view() {
        let root = workspace_root().unwrap();
        fs::create_dir_all(root.join("target/perf/runs")).unwrap();
        let run = Builder::new()
            .prefix("projection-cli-")
            .tempdir_in(root.join("target/perf/runs"))
            .unwrap();
        let samples = samples(FixtureKind::Linear);
        let policy = policy();
        let base = derive_evidence(samples.iter().collect(), &policy).unwrap();
        let points = aggregate_points(&samples.iter().collect::<Vec<_>>()).unwrap();
        let capacity = capacity_document(points, Some(&base));
        write_json(run.path(), "capacity.json", &capacity);
        let sample_lines = samples
            .iter()
            .map(|sample| serde_json::to_string(sample).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(
            run.path().join("samples.jsonl"),
            format!("{sample_lines}\n"),
        )
        .unwrap();
        write_json(
            run.path(),
            "report.json",
            &ReportDocument {
                schema_id: REPORT_SCHEMA.into(),
                run_id: "capacity-run".into(),
                kind: "run".into(),
                status: "pass".into(),
                profile_id: REQUIRED_PROFILE.into(),
                generated_at: "2026-08-24T00:00:00Z".into(),
                comparisons: Vec::new(),
                failures: Vec::new(),
                artifact_files: Vec::new(),
                extra: no_extra(),
            },
        );
        let registry = super::super::config::load_registry(&root).unwrap();
        let profile = super::super::config::load_profile(&root, REQUIRED_PROFILE).unwrap();
        let manifest = build_manifest("build");
        let frozen =
            provenance::freeze(run.path(), &registry, &profile, None, None, &manifest).unwrap();
        write_json(
            run.path(),
            "campaign.json",
            &CampaignManifest {
                schema_id: REPORT_SCHEMA.into(),
                run_id: "capacity-run".into(),
                kind: "run".into(),
                status: "pass".into(),
                created_at: "2026-08-24T00:00:00Z".into(),
                completed_at: Some("2026-08-24T00:01:00Z".into()),
                profile_id: REQUIRED_PROFILE.into(),
                schedule_seed: 1,
                selected_workloads: profile
                    .workloads
                    .iter()
                    .map(|workload| format!("{}:{}", workload.id, workload.tuple))
                    .collect(),
                planned_measured_executions: samples.len() as u64,
                planned_preconditioning_executions: 0,
                artifact_limit_bytes: 1_000_000,
                environment_file: "environment.json".into(),
                registry_file: frozen.registry_file,
                profile_file: frozen.profile_file,
                budget_policy_file: frozen.budget_policy_file,
                baseline_manifest: frozen.baseline_manifest,
                candidate_manifest: frozen.candidate_manifest,
                failure: None,
                extra: no_extra(),
            },
        );
        write_json(
            run.path(),
            "environment.json",
            &EnvironmentManifest {
                schema_id: ENVIRONMENT_SCHEMA.into(),
                created_at: "2026-08-24T00:00:00Z".into(),
                environment_id: "test".into(),
                canonical: false,
                os: "test".into(),
                architecture: "test".into(),
                logical_cpus: 2,
                hostname: None,
                collector: "fixture".into(),
                validity: Vec::new(),
                extra: no_extra(),
            },
        );
        let code = execute(ProjectArgs {
            run_dir: run.path().to_path_buf(),
            deployment: root.join("performance/deployments/planning-example-v1.toml"),
        })
        .unwrap();

        assert_eq!(code, EXIT_INVALID);
        let document: ProjectionDocument = read_json(&run.path().join("projections.json")).unwrap();
        assert_eq!(document.schema_id, PROJECTION_SCHEMA);
        assert_eq!(document.confidence, ProjectionConfidence::Invalid);
        assert_eq!(document.resolved_deployment.id, "planning-example-v1");
        assert!(!document.demands.is_empty());
        assert!(
            document
                .failures
                .iter()
                .any(|failure| failure.contains("unbounded"))
        );
        fs::remove_file(run.path().join("projection.md")).unwrap();
        assert!(rerender(run.path()).unwrap());
        assert!(run.path().join("projection.md").is_file());
    }

    fn project_fixture(
        kind: FixtureKind,
        limits: LimitSet,
        staging: Option<StagingFixture>,
    ) -> Result<ProjectionDocument> {
        project_with_context(
            kind,
            deployment(limits, staging),
            ProjectionContext {
                canonical_environment: false,
                exact_fixture_match: true,
            },
        )
    }

    fn project_with_context(
        kind: FixtureKind,
        deployment: DeploymentInput,
        context: ProjectionContext,
    ) -> Result<ProjectionDocument> {
        validate_deployment(&deployment)?;
        let samples = samples(kind);
        let policy = policy();
        let base = derive_evidence(samples.iter().collect(), &policy).ok();
        let points = aggregate_points(&samples.iter().collect::<Vec<_>>())?;
        let capacity = capacity_document(points, base.as_ref());
        project(&deployment, &capacity, &samples, &policy, context)
    }

    fn capacity_document(
        points: Vec<CapacityPoint>,
        base: Option<&EvidenceDraw>,
    ) -> CapacityDocument {
        let knees = base
            .map(|draw| {
                vec![
                    CapacityKnee {
                        workers: 1,
                        knee_step: Some(8),
                        knee_throughput_per_second: Some(draw.r1_knee),
                        observed_rate_lower_bound: None,
                    },
                    CapacityKnee {
                        workers: 2,
                        knee_step: Some(8),
                        knee_throughput_per_second: Some(draw.r2_knee),
                        observed_rate_lower_bound: None,
                    },
                ]
            })
            .unwrap_or_default();
        CapacityDocument {
            schema_id: CAPACITY_SCHEMA.into(),
            source_run_id: "capacity-run".into(),
            build_digest: "build".into(),
            environment_id: "test".into(),
            canonical: false,
            points,
            knees,
            scale_efficiency_2: base.map(|draw| draw.scale_efficiency_2).or(Some(0.95)),
            supports_linear_projection: true,
            failures: Vec::new(),
        }
    }

    fn build_manifest(digest: &str) -> BuildManifest {
        BuildManifest {
            schema_id: BUILD_SCHEMA.into(),
            created_at: "2026-08-24T00:00:00Z".into(),
            executable_name: "vigilo".into(),
            executable_digest: digest.into(),
            executable_bytes: 1,
            source_commit: Some("commit".into()),
            source_dirty: false,
            source_label: "test".into(),
            cargo_lock_digest: "lock".into(),
            dependency_tree_digest: "dependencies".into(),
            migrations_digest: "migrations".into(),
            evaluator_abi_digest: "abi".into(),
            rustc: "rustc test".into(),
            cargo: "cargo test".into(),
            target: "test-target".into(),
            profile: "release".into(),
            capabilities: Vec::new(),
            setup_assets: Vec::<SetupAsset>::new(),
            extra: no_extra(),
        }
    }

    fn write_json<T: Serialize>(directory: &Path, name: &str, value: &T) {
        fs::write(
            directory.join(name),
            serde_json::to_vec_pretty(value).unwrap(),
        )
        .unwrap();
    }

    fn deployment(limits: LimitSet, staging: Option<StagingFixture>) -> DeploymentInput {
        let mut resources = REQUIRED_RESOURCES.to_vec();
        if matches!(limits, LimitSet::MissingStorage) {
            resources.retain(|resource| *resource != Resource::StorageBytesPerDay);
        }
        DeploymentInput {
            schema_id: DEPLOYMENT_SCHEMA.into(),
            id: "test-v1".into(),
            description: "test deployment".into(),
            target_utilization: 0.70,
            bootstrap_resamples: 100,
            bootstrap_seed: 7,
            capacity_fixture: "mvp-v1".into(),
            workload: WorkloadMix {
                peak_case_rate: 20.0,
                duty_cycle: 0.5,
                concurrent_runs_peak: 2,
                run_classes: vec![RunClass {
                    name: "default".into(),
                    cases: 100,
                    fraction: 1.0,
                }],
                payload_classes: vec![PayloadClass {
                    bytes: 1024,
                    fraction: 1.0,
                }],
                evaluator_mix: vec![EvaluatorClass {
                    name: "sentiment".into(),
                    evaluators_per_case: 1,
                    fraction: 1.0,
                }],
                result_bytes_per_case: 512,
                diagnostic_bytes_per_case: 64,
            },
            agent: AgentInput {
                mean_latency_ms: 10.0,
                p95_latency_ms: 20.0,
                connection_policy: "keepalive".into(),
            },
            amplification: AmplificationInput {
                attempts_per_useful_case: 1.0,
                successful_agent_attempts_per_useful_case: 1.0,
                chunk_deliveries_per_useful_chunk: 1.0,
                durable_events_per_useful_chunk: 1.0,
                publish_attempts_per_durable_event: 1.0,
                acknowledgements_per_delivery: 1.0,
                retry_publishes_per_useful_chunk: 0.0,
                quarantine_publishes_per_useful_chunk: 0.0,
            },
            configuration: DeploymentConfiguration {
                cases_per_chunk: 100,
                worker_inflight_chunks: 8,
                wasm_concurrency_per_worker: 8,
                database_connections_per_target: 8,
                coordinator_tick_ms: 1_000,
                placements: vec![PlacementInput {
                    alias: "primary".into(),
                    fraction: 1.0,
                }],
            },
            limits: resources
                .into_iter()
                .map(|resource| LimitInput {
                    resource,
                    capacity: 1_000_000_000_000.0,
                    usable_fraction: 0.70,
                    provenance: LimitProvenance::Measured,
                    source: "fixture".into(),
                    observed_on: "2026-08-24".into(),
                    hardware: "fixture".into(),
                    configuration: "fixture".into(),
                })
                .collect(),
            boundedness: vec![BoundedPath {
                path: REQUIRED_BOUNDED_PATH.into(),
                bounded: true,
                provenance: "fixture".into(),
            }],
            staging: staging.map(|staging| StagingInput {
                source_run_id: "staging".into(),
                workers: 2,
                observed_case_rate: staging.observed_case_rate,
                maximum_relative_error: staging.maximum_relative_error,
            }),
        }
    }

    fn policy() -> CapacityPolicy {
        CapacityPolicy {
            minimum_throughput_gain: 0.10,
            latency_growth: 0.25,
            maximum_worker_cpu_percent: 90.0,
            maximum_service_cpu_percent: 85.0,
            minimum_samples_per_point: 8,
            minimum_scale_efficiency: 0.85,
        }
    }

    fn samples(kind: FixtureKind) -> Vec<Sample> {
        let one = match kind {
            FixtureKind::Linear | FixtureKind::Saturated => [10.0, 18.0, 24.0, 25.0, 25.5],
            FixtureKind::Nonlinear => [10.0, 18.0, 12.0, 25.0, 25.5],
        };
        let two = [20.0, 36.0, 48.0, 49.0, 49.5];
        let mut samples = Vec::new();
        for (workers, rates) in [(1, one), (2, two)] {
            for (index, load) in [1, 2, 4, 8, 16].into_iter().enumerate() {
                for repetition in 0..8 {
                    let cases = 100_u64 * load;
                    let rate = rates[index] * (1.0 + f64::from(repetition) / 1_000.0);
                    let wall_time_ns = (cases as f64 / rate * 1_000_000_000.0) as u64;
                    let service_cpu = if matches!(kind, FixtureKind::Saturated)
                        && workers == 1
                        && load == 4
                        && repetition == 0
                    {
                        90.0
                    } else {
                        30.0
                    };
                    samples.push(sample(workers, load, cases, wall_time_ns, service_cpu));
                }
            }
        }
        samples
    }

    fn sample(workers: u32, load: u64, cases: u64, wall_time_ns: u64, service_cpu: f64) -> Sample {
        Sample {
            schema_id: SAMPLE_SCHEMA.into(),
            run_id: "capacity-run".into(),
            profile_id: REQUIRED_PROFILE.into(),
            workload_id: CAPACITY_WORKLOAD.into(),
            tuple_id: format!("workers-{workers}-load-{load}"),
            block_id: 0,
            orientation_set_id: 0,
            orientation: Orientation::Single,
            pair_id: 0,
            position: 1,
            role: BinaryRole::Single,
            measured: true,
            started_at: "2026-08-24T00:00:00Z".into(),
            process: ProcessMeasurement {
                wall_time_ns,
                cpu_time_ns: Some(wall_time_ns * u64::from(workers) / 2),
                peak_rss_bytes: Some(100_000_000 * u64::from(workers)),
                stdout_first_byte_ns: None,
                stdout_last_byte_ns: None,
                resource_source: "fixture".into(),
                exit_code: Some(0),
                timed_out: false,
                stdout_bytes: 0,
                stderr_bytes: 0,
                stdout_truncated: false,
                stderr_truncated: false,
            },
            validation: Validation {
                state: SampleState::Valid,
                code: "valid".into(),
                message: "fixture".into(),
            },
            external: ExternalMeasurements {
                storage_counters: BTreeMap::new(),
                sql_databases: Vec::new(),
                table_writes: BTreeMap::new(),
                sql_calls: Some(cases * 4),
                sql_time_ms: Some(1.0),
                sql_rows: Some(cases),
                wal_bytes: Some(cases * 1_000),
                database_bytes_delta: Some((cases * 800) as i64),
                http_requests: Some(cases),
                http_bytes: Some(cases * 2_048),
                http_peak_concurrency: Some(8),
                queue_ready: Some(0),
                queue_unacked: Some(0),
                service_memory_bytes: Some(100_000_000),
                service_cpu_percent: Some(service_cpu),
                durable_counts: BTreeMap::from([("executions".into(), cases as i64)]),
                query_diagnostics: Vec::new(),
            },
            extra: no_extra(),
        }
    }
}
