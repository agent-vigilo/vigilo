//! Service-backed workload preparation, execution, and exact oracles.
//!
//! Every measured sample clones a binary-specific prepared PostgreSQL database
//! and receives a fresh RabbitMQ vhost and namespace. Preparation is outside
//! the measurement region; collectors are reset only after all prerequisite
//! work has settled.
//! Administrative and routed-data workloads may clone additional databases;
//! their SQL and storage observations are aggregated while cluster-wide WAL is
//! counted once.
//!
//! A workload oracle is the exact correctness postcondition for a measured
//! operation: durable row counts, lifecycle state, outbox events, and queue or
//! HTTP observations. It is not an evaluator from the evaluator ABI and it is
//! not a timing threshold. A sample is usable for performance analysis only
//! after its oracle passes. Timing policy belongs to `stats`, while evaluator
//! input/output behavior remains owned by the versioned evaluator ABI.

use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    error::Error,
    fmt,
    path::{
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
    ensure,
};
use postgres::{
    Client,
    NoTls,
};
use serde_json::Value;

use super::{
    fixture::{
        FixtureCatalog,
        RunInputs,
        load,
        write_run_inputs,
        write_run_inputs_with_payload,
    },
    model::{
        BuildManifest,
        ExternalMeasurements,
        ReliabilityContract,
    },
    process::{
        CapturedOutput,
        ProcessOutcome,
        ProcessSpec,
        RunningProcess,
        execute,
    },
    reliability::{
        RELIABILITY_SCHEMA,
        ReliabilityArtifact,
        ReliabilityTotals,
        StabilityObservation,
        evaluate_recovery,
        evaluate_soak,
        require_pass,
    },
    service::{
        DurableCounts,
        SampleScope,
        ServiceHarness,
    },
};

#[derive(Clone)]
struct PreparedDatabase {
    url: String,
    run_id: Option<String>,
    cases: usize,
    evaluators: usize,
    placements: Vec<PreparedPlacement>,
}

#[derive(Clone)]
struct PreparedPlacement {
    env_key: String,
    url: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkloadShape {
    cases: usize,
    evaluators: usize,
    create_run: bool,
}

/// Result of one service-backed workload invocation.
pub struct WorkloadOutcome {
    /// Aggregate process observations for the measured region.
    pub process: ProcessOutcome,
    /// Scoped external measurements and exact durable counts.
    pub external: ExternalMeasurements,
}

/// A measured product or exact-oracle failure, distinct from harness invalidity.
#[derive(Debug)]
pub(super) struct ProductFailure {
    message: String,
}

impl ProductFailure {
    /// Human-readable product failure retained in the campaign sample.
    pub(super) fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ProductFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for ProductFailure {}

fn product<T>(result: Result<T>) -> Result<T> {
    result.map_err(|error| {
        ProductFailure {
            message: format!("{error:#}"),
        }
        .into()
    })
}

fn product_failure(message: impl Into<String>) -> anyhow::Error {
    ProductFailure {
        message: message.into(),
    }
    .into()
}

pub(super) struct WorkloadRequest<'a> {
    pub(super) workload_id: &'a str,
    pub(super) tuple: &'a str,
    pub(super) fixture_id: &'a str,
    pub(super) binary: &'a Path,
    pub(super) manifest_path: &'a Path,
    pub(super) manifest: &'a BuildManifest,
    pub(super) exact: &'a BTreeMap<String, u64>,
    pub(super) reliability: Option<&'a ReliabilityContract>,
    pub(super) limits: ExecutionLimits,
}

#[derive(Clone, Copy)]
pub(super) struct ExecutionLimits {
    pub(super) watchdog: Duration,
    pub(super) stdout: usize,
    pub(super) stderr: usize,
}

/// Campaign-owned service topology and reusable prepared database snapshots.
pub struct WorkloadRunner {
    root: PathBuf,
    run_dir: PathBuf,
    run_id: String,
    service: Option<ServiceHarness>,
    fixture: Option<FixtureCatalog>,
    prepared: BTreeMap<String, PreparedDatabase>,
}

impl WorkloadRunner {
    /// Creates a lazy runner; startup-only campaigns never provision services.
    pub fn new(root: &Path, run_dir: &Path, run_id: &str) -> Self {
        Self {
            root: root.to_path_buf(),
            run_dir: run_dir.to_path_buf(),
            run_id: run_id.to_owned(),
            service: None,
            fixture: None,
            prepared: BTreeMap::new(),
        }
    }

    /// Executes one non-startup workload from a fresh isolated sample scope.
    pub fn execute(&mut self, request: WorkloadRequest<'_>) -> Result<WorkloadOutcome> {
        self.ensure_service(request.fixture_id)?;
        let key = format!(
            "{}:{}:{}",
            request.manifest.executable_digest, request.workload_id, request.tuple
        );
        if !self.prepared.contains_key(&key) {
            let prepared = self.prepare(
                request.workload_id,
                request.tuple,
                request.binary,
                request.manifest_path,
                request.limits,
            )?;
            self.prepared.insert(key.clone(), prepared);
        }
        let prepared = self
            .prepared
            .get(&key)
            .context("prepared workload disappeared")?
            .clone();
        let service = self
            .service
            .as_mut()
            .context("service topology was not started")?;
        let template = service.owned_database_name(&prepared.url)?;
        let database_url = service.clone_database(&template, request.workload_id)?;
        let mut placement_urls = Vec::with_capacity(prepared.placements.len());
        let mut route_env = service.storage_env();
        for placement in &prepared.placements {
            let template = service.owned_database_name(&placement.url)?;
            let url = service.clone_database(&template, request.workload_id)?;
            route_env.push((placement.env_key.clone(), url.clone()));
            placement_urls.push(url);
        }
        let scope = service.create_sample_scope(database_url, request.workload_id)?;

        let result = self.execute_scope(
            request.workload_id,
            request.tuple,
            request.binary,
            request.manifest_path,
            prepared.run_id.as_deref(),
            prepared.cases,
            prepared.evaluators,
            request.exact,
            request.reliability,
            &scope,
            &placement_urls,
            &route_env,
            request.limits.watchdog,
            request.limits.stdout,
            request.limits.stderr,
        );
        let release = self
            .service
            .as_mut()
            .context("service topology was not started")?
            .release_scope(scope);
        let placement_release = self
            .service
            .as_mut()
            .context("service topology was not started")?
            .release_databases(&placement_urls);
        combine_sample_cleanup(result, release, placement_release)
    }

    /// Lazily starts one topology and prevents a campaign from mixing fixture catalogs.
    fn ensure_service(&mut self, fixture_id: &str) -> Result<()> {
        if let Some(fixture) = &self.fixture {
            if fixture.id != fixture_id {
                bail!("one campaign cannot mix performance fixture catalogs");
            }
            return Ok(());
        }
        let fixture = load(&self.root, fixture_id)?;
        let service = ServiceHarness::start(
            &self.root,
            &self.run_dir,
            &self.run_id,
            &fixture.agent_response_text,
            fixture.agent_payload_bytes,
        )?;
        self.fixture = Some(fixture);
        self.service = Some(service);
        Ok(())
    }

    /// Builds a reusable, binary-specific database template outside measurement.
    ///
    /// Preparation applies migrations, publishes the frozen evaluator fixture,
    /// creates any prerequisite run, and verifies structural counts before the
    /// database can be cloned for samples.
    fn prepare(
        &mut self,
        workload_id: &str,
        tuple: &str,
        binary: &Path,
        manifest_path: &Path,
        limits: ExecutionLimits,
    ) -> Result<PreparedDatabase> {
        let database_url = self
            .service
            .as_mut()
            .context("service topology was not started")?
            .create_database(&format!("template-{}", self.prepared.len()))?;
        let migrations = setup_asset(manifest_path, "setup-assets/migrations")?;
        let evaluator = setup_asset(manifest_path, "setup-assets/evaluators/sentiment-basic-en")?;
        setup_database(binary, &database_url, &migrations, limits)?;
        require_success(
            invoke(
                binary,
                base_args(
                    &database_url,
                    [
                        "evaluator".into(),
                        "publish".into(),
                        path_arg(&evaluator)?,
                        "--release".into(),
                    ],
                ),
                &[],
                Duration::from_secs(120),
                limits.stdout,
                limits.stderr,
            )?,
            "publish frozen evaluator fixture",
        )?;

        let fixture = self
            .fixture
            .as_ref()
            .context("fixture was not loaded")?
            .clone();
        let shape = workload_shape(&fixture, workload_id, tuple)?;
        let route_count = required_routes(workload_id, tuple)?;
        let mut placements = Vec::with_capacity(route_count.saturating_sub(1));
        let mut route_env = self
            .service
            .as_ref()
            .context("service topology was not started")?
            .storage_env();
        for index in 1..route_count {
            let alias = format!("perf_{index:02}");
            let env_key = format!("VIGILO_PERF_DATABASE_URL_{index:02}");
            let url = self
                .service
                .as_mut()
                .context("service topology was not started")?
                .create_database(&format!("route-{index}"))?;
            setup_database(binary, &url, &migrations, limits)?;
            route_env.push((env_key.clone(), url.clone()));
            require_success(
                invoke(
                    binary,
                    database_register_args(&database_url, &alias, &env_key),
                    &route_env,
                    Duration::from_secs(120),
                    limits.stdout,
                    limits.stderr,
                )?,
                "register routed database fixture",
            )?;
            require_success(
                invoke(
                    binary,
                    database_activate_args(&database_url, &alias),
                    &route_env,
                    Duration::from_secs(120),
                    limits.stdout,
                    limits.stderr,
                )?,
                "activate routed database fixture",
            )?;
            placements.push(PreparedPlacement { env_key, url });
        }
        if workload_id == "run.create-ingestion.v1" && route_count > 1 {
            Client::connect(&database_url, NoTls)?.execute(
                "UPDATE database_placements SET role='control' WHERE alias='primary'",
                &[],
            )?;
        }
        let run_id = if shape.create_run {
            let identity = format!("{workload_id}:{tuple}");
            let inputs = if workload_id == "run.create-ingestion.v1" {
                self.inputs_with_payload(
                    &identity,
                    shape.cases,
                    shape.evaluators,
                    parse_ingestion_tuple(tuple)?.payload_bytes,
                )?
            } else if workload_id == "shard.move.v1" {
                self.inputs_with_payload(
                    &identity,
                    shape.cases,
                    shape.evaluators,
                    parse_move_tuple(tuple)?.payload_bytes,
                )?
            } else {
                self.inputs(&identity, shape.cases, shape.evaluators)?
            };
            if workload_id == "run.create-ingestion.v1" {
                super::fixture::set_gates(&inputs, parse_ingestion_tuple(tuple)?.gates)?;
            }
            Some(
                if workload_id == "run.create-ingestion.v1" && route_count > 1 {
                    let mut args = ingestion_args(&database_url, &inputs, None)?;
                    args.extend([
                        "--default-shard-database-alias".into(),
                        "perf_01".into(),
                        "--shard-assignment-policy".into(),
                        "spread-active".into(),
                    ]);
                    let process = invoke(
                        binary,
                        args,
                        &route_env,
                        Duration::from_secs(600),
                        limits.stdout,
                        limits.stderr,
                    )?;
                    require_success_ref(&process, "prepare routed ingestion fixture")?;
                    parse_run_id(&process.stdout.text())?
                } else {
                    create_run(
                        binary,
                        &database_url,
                        &inputs,
                        &route_env,
                        route_count > 1
                            && !matches!(workload_id, "shard.move.v1" | "shard.rebalance.v1"),
                        Duration::from_secs(600),
                        limits.stdout,
                        limits.stderr,
                    )?
                },
            )
        } else {
            None
        };
        let route_urls = std::iter::once(database_url.clone())
            .chain(placements.iter().map(|placement| placement.url.clone()))
            .collect::<Vec<_>>();
        if let Some(run_id) = run_id.as_deref() {
            match workload_id {
                "run.cancel-scaling.v1" => {
                    ensure_execution_routes(
                        binary,
                        &database_url,
                        run_id,
                        route_count,
                        &route_env,
                        limits,
                    )?;
                    seed_open_executions(
                        self.service
                            .as_ref()
                            .context("service topology was not started")?,
                        &route_urls,
                        run_id,
                    )?;
                }
                "run.read.v1" | "run.export.v1" => prepare_terminal_run(
                    self.service
                        .as_mut()
                        .context("service topology was not started")?,
                    binary,
                    &database_url,
                    run_id,
                    &route_urls,
                    &route_env,
                    shape.cases,
                    fixture.lifecycle.worker_pass_limit,
                    limits,
                )?,
                "shard.move.v1" => consolidate_shard_rows(&database_url, run_id)?,
                _ => {}
            }
        }
        let actual = structural_counts(&database_url, run_id.as_deref())?;
        verify_prepared(workload_id, &actual)?;
        Ok(PreparedDatabase {
            url: database_url,
            run_id,
            cases: shape.cases,
            evaluators: shape.evaluators,
            placements,
        })
    }

    #[allow(clippy::too_many_arguments)]
    /// Executes the measured region and exact oracle for one isolated sample scope.
    ///
    /// Setup commands required only to make the sample runnable occur before
    /// `begin_measurement`; each match arm defines the workload-specific measured
    /// boundary and postcondition.
    fn execute_scope(
        &mut self,
        workload_id: &str,
        tuple: &str,
        binary: &Path,
        _manifest_path: &Path,
        prepared_run_id: Option<&str>,
        cases: usize,
        evaluators: usize,
        exact: &BTreeMap<String, u64>,
        reliability: Option<&ReliabilityContract>,
        scope: &SampleScope,
        placement_urls: &[String],
        route_env: &[(String, String)],
        watchdog: Duration,
        stdout_limit: usize,
        stderr_limit: usize,
    ) -> Result<WorkloadOutcome> {
        let mut env = route_env.to_vec();
        env.push(("VIGILO_MQ_NAMESPACE".into(), scope.mq_namespace.clone()));
        let service = self
            .service
            .as_ref()
            .context("service topology was not started")?;
        let fixture = self.fixture.as_ref().context("fixture was not loaded")?;
        let (agent_payload, agent_delay_ms) = if workload_id.starts_with("agent.http-") {
            let (_, payload, delay) = parse_http_tuple(tuple)?;
            (payload, delay)
        } else {
            (fixture.agent_payload_bytes, 0)
        };
        service.configure_agent(
            &fixture.agent_response_text,
            agent_payload,
            Duration::from_millis(agent_delay_ms),
        )?;
        match workload_id {
            "run.create-ingestion.v1" => {
                let shape = parse_ingestion_tuple(tuple)?;
                let inputs = self.inputs_with_payload(
                    &format!("{workload_id}:{tuple}"),
                    cases,
                    evaluators,
                    shape.payload_bytes,
                )?;
                let upload = if shape.reference {
                    let mut db = Client::connect(&scope.database_url, NoTls)?;
                    Some(db.query_one("SELECT dataset_upload_id::text FROM run_plans WHERE run_id=$1::text::uuid", &[&prepared_run_id.context("reference fixture has no seed run")?])?.get::<_, String>(0))
                } else {
                    None
                };
                super::fixture::set_gates(&inputs, shape.gates)?;
                let mut args = ingestion_args(&scope.database_url, &inputs, upload.as_deref())?;
                if shape.placements > 0 {
                    args.extend([
                        "--default-shard-database-alias".into(),
                        "perf_01".into(),
                        "--shard-assignment-policy".into(),
                        "spread-active".into(),
                    ]);
                }
                let baseline = service.begin_measurement_with_databases(scope, placement_urls)?;
                let started = Instant::now();
                let outcomes = invoke_creators(
                    shape.creators,
                    binary,
                    args,
                    &env,
                    watchdog,
                    stdout_limit,
                    stderr_limit,
                )?;
                let elapsed = started.elapsed();
                // Freeze counters before any oracle reads.
                let mut external =
                    service.finish_measurement(scope, baseline, DurableCounts::new())?;
                let mut run_ids = std::collections::BTreeSet::new();
                for process in &outcomes {
                    product(require_success_ref(process, "measured ingestion"))?;
                    let run_id = product(parse_run_id(&process.stdout.text()))?;
                    if !run_ids.insert(run_id.clone()) {
                        return Err(product_failure("creators returned duplicate run IDs"));
                    }
                    let counts = product(oracle_ingestion(
                        service,
                        &scope.database_url,
                        placement_urls,
                        &run_id,
                        shape,
                        cases.div_ceil(fixture.coordinator.cases_per_chunk) as i64,
                    ))?;
                    for (name, count) in counts {
                        if matches!(
                            name.as_str(),
                            "total_runs"
                                | "dataset_versions"
                                | "dataset_catalogs"
                                | "referenced_datasets"
                                | "case_payload_tables"
                        ) {
                            external.durable_counts.insert(name, count);
                        } else {
                            *external.durable_counts.entry(name).or_default() += count;
                        }
                    }
                }
                external
                    .durable_counts
                    .insert("created_runs".into(), shape.creators as i64);
                let process = if shape.creators == 1 {
                    outcomes
                        .into_iter()
                        .next()
                        .context("missing creator result")?
                } else {
                    aggregate(outcomes, elapsed)
                };
                product(verify_exact_observations(&external, exact))?;
                if external.http_requests != Some(0)
                    || external.queue_ready != Some(0)
                    || external.queue_unacked != Some(0)
                {
                    return Err(product_failure(
                        "ingestion unexpectedly executed or dispatched work",
                    ));
                }
                Ok(WorkloadOutcome { process, external })
            }
            "run.create.v1" | "run.create-scaling.v1" | "run.create-boundaries.v1" => {
                let inputs = self.inputs(&format!("{workload_id}:{tuple}"), cases, evaluators)?;
                let baseline = service.begin_measurement_with_databases(scope, placement_urls)?;
                let process = invoke(
                    binary,
                    create_args(&scope.database_url, &inputs)?,
                    &env,
                    watchdog,
                    stdout_limit,
                    stderr_limit,
                )?;
                let run_id = product(parse_run_id(&process.stdout.text()))?;
                let chunks = cases.div_ceil(fixture.coordinator.cases_per_chunk) as i64;
                let counts = if workload_id == "run.create-boundaries.v1" {
                    product(oracle_create_boundary(
                        &scope.database_url,
                        &run_id,
                        cases,
                        chunks,
                    ))?
                } else {
                    product(oracle_create(&scope.database_url, &run_id, cases, chunks))?
                };
                finish(
                    service,
                    scope,
                    baseline,
                    process,
                    counts,
                    ExternalOracle::new(0, 0, exact),
                )
            }
            "run.cancel-scaling.v1" => {
                let run_id = prepared_run_id.context("cancellation fixture has no run")?;
                let baseline = service.begin_measurement_with_databases(scope, placement_urls)?;
                let started = Instant::now();
                let first = invoke(
                    binary,
                    run_admin_args(&scope.database_url, "cancel", run_id),
                    &env,
                    watchdog,
                    stdout_limit,
                    stderr_limit,
                )?;
                product(validate_cancel_output(&first.stdout.text(), cases, false))?;
                let replay = invoke(
                    binary,
                    run_admin_args(&scope.database_url, "cancel", run_id),
                    &env,
                    watchdog,
                    stdout_limit,
                    stderr_limit,
                )?;
                product(validate_cancel_output(&replay.stdout.text(), 0, true))?;
                let process = aggregate(vec![first, replay], started.elapsed());
                let counts = product(cancellation_counts(
                    &scope.database_url,
                    placement_urls,
                    run_id,
                    cases,
                ))?;
                finish(
                    service,
                    scope,
                    baseline,
                    process,
                    counts,
                    ExternalOracle::new(0, 0, exact),
                )
            }
            "run.read.v1" | "run.export.v1" => {
                let run_id = prepared_run_id.context("read/export fixture has no run")?;
                let shape = parse_read_tuple(tuple)?;
                let baseline = service.begin_measurement_with_databases(scope, placement_urls)?;
                let process = invoke(
                    binary,
                    read_args(&scope.database_url, run_id, shape),
                    &env,
                    watchdog,
                    stdout_limit,
                    stderr_limit,
                )?;
                if process.stdout.truncated {
                    return Err(product_failure(
                        "read/export output exceeded the profile retention contract",
                    ));
                }
                product(validate_read_output(shape, &process.stdout.text(), run_id))?;
                product(validate_read_resources(shape, &process))?;
                let counts = product(read_counts(
                    &scope.database_url,
                    placement_urls,
                    run_id,
                    cases,
                ))?;
                finish(
                    service,
                    scope,
                    baseline,
                    process,
                    counts,
                    ExternalOracle::new(0, 0, exact),
                )
            }
            "shard.move.v1" => {
                let run_id = prepared_run_id.context("shard move fixture has no run")?;
                let baseline = service.begin_measurement_with_databases(scope, placement_urls)?;
                let started = Instant::now();
                let first = invoke(
                    binary,
                    shard_move_args(&scope.database_url, run_id, "perf_01"),
                    &env,
                    watchdog,
                    stdout_limit,
                    stderr_limit,
                )?;
                product(validate_move_output(&first.stdout.text(), "perf_01", true))?;
                let replay = invoke(
                    binary,
                    shard_move_args(&scope.database_url, run_id, "perf_01"),
                    &env,
                    watchdog,
                    stdout_limit,
                    stderr_limit,
                )?;
                product(validate_move_output(
                    &replay.stdout.text(),
                    "perf_01",
                    false,
                ))?;
                let process = aggregate(vec![first, replay], started.elapsed());
                let counts = product(shard_move_counts(&scope.database_url, run_id))?;
                finish(
                    service,
                    scope,
                    baseline,
                    process,
                    counts,
                    ExternalOracle::new(0, 0, exact),
                )
            }
            "shard.rebalance.v1" => {
                let run_id = prepared_run_id.context("rebalance fixture has no run")?;
                let shards = parse_rebalance_tuple(tuple)?;
                let plan = invoke(
                    binary,
                    rebalance_plan_args(&scope.database_url, shards),
                    &env,
                    Duration::from_secs(180),
                    stdout_limit,
                    stderr_limit,
                )?;
                require_success_ref(&plan, "prepare rebalance plan")?;
                let operation_id = parse_rebalance_operation_id(&plan.stdout.text())?;
                let baseline = service.begin_measurement_with_databases(scope, placement_urls)?;
                let started = Instant::now();
                let mut outcomes = Vec::with_capacity(shards.saturating_add(1));
                for _ in 0..shards {
                    outcomes.push(invoke(
                        binary,
                        rebalance_apply_args(&scope.database_url, &operation_id, 1),
                        &env,
                        watchdog,
                        stdout_limit,
                        stderr_limit,
                    )?);
                }
                let verify = invoke(
                    binary,
                    rebalance_verify_args(&scope.database_url, &operation_id, shards),
                    &env,
                    watchdog,
                    stdout_limit,
                    stderr_limit,
                )?;
                product(validate_rebalance_output(&verify.stdout.text(), shards))?;
                outcomes.push(verify);
                let process = aggregate(outcomes, started.elapsed());
                let counts = product(rebalance_counts(
                    &scope.database_url,
                    &operation_id,
                    run_id,
                    shards,
                ))?;
                finish(
                    service,
                    scope,
                    baseline,
                    process,
                    counts,
                    ExternalOracle::new(0, 0, exact),
                )
            }
            "coordinator.placement-scaling.v1" => {
                let run_id = prepared_run_id.context("placement fixture has no run")?;
                let aliases = parse_placement_tuple(tuple)?;
                let baseline = service.begin_measurement_with_databases(scope, placement_urls)?;
                let started = Instant::now();
                let process = aggregate(
                    vec![
                        invoke(
                            binary,
                            coordinator_args(&scope.database_url, &scope.messaging_url),
                            &env,
                            watchdog,
                            stdout_limit,
                            stderr_limit,
                        )?,
                        invoke(
                            binary,
                            coordinator_args(&scope.database_url, &scope.messaging_url),
                            &env,
                            watchdog,
                            stdout_limit,
                            stderr_limit,
                        )?,
                    ],
                    started.elapsed(),
                );
                let counts = product(routed_coordinator_counts(
                    &scope.database_url,
                    placement_urls,
                    run_id,
                    aliases,
                ))?;
                finish(
                    service,
                    scope,
                    baseline,
                    process,
                    counts,
                    ExternalOracle::new(aliases as u64, 0, exact),
                )
            }
            "coordinator.dispatch.v1" | "coordinator.dispatch-scaling.v1" => {
                let run_id = prepared_run_id
                    .context("coordinator fixture has no run")?
                    .to_owned();
                let baseline = service.begin_measurement(scope)?;
                let started = Instant::now();
                let process = aggregate(
                    vec![
                        invoke(
                            binary,
                            coordinator_args(&scope.database_url, &scope.messaging_url),
                            &env,
                            watchdog,
                            stdout_limit,
                            stderr_limit,
                        )?,
                        invoke(
                            binary,
                            coordinator_args(&scope.database_url, &scope.messaging_url),
                            &env,
                            watchdog,
                            stdout_limit,
                            stderr_limit,
                        )?,
                    ],
                    started.elapsed(),
                );
                let chunks = cases.div_ceil(fixture.coordinator.cases_per_chunk);
                let counts = product(oracle_coordinator(&scope.database_url, &run_id, chunks))?;
                finish(
                    service,
                    scope,
                    baseline,
                    process,
                    counts,
                    ExternalOracle::new(chunks as u64, 0, exact),
                )
            }
            "coordinator.outbox-scaling.v1" => {
                let run_id = prepared_run_id
                    .context("outbox fixture has no run")?
                    .to_owned();
                let shape = parse_outbox_tuple(tuple)?;
                require_success(
                    invoke(
                        binary,
                        coordinator_args_with_outbox(
                            &scope.database_url,
                            &scope.messaging_url,
                            1,
                            1,
                        ),
                        &env,
                        Duration::from_secs(180),
                        stdout_limit,
                        stderr_limit,
                    )?,
                    "prepare pending outbox events",
                )?;
                require_success(
                    invoke(
                        binary,
                        coordinator_args_with_outbox(
                            &scope.database_url,
                            &scope.messaging_url,
                            1,
                            1,
                        ),
                        &env,
                        Duration::from_secs(180),
                        stdout_limit,
                        stderr_limit,
                    )?,
                    "dispatch pending outbox events",
                )?;
                service.purge_scope_queues(scope)?;
                let baseline = service.begin_measurement(scope)?;
                let process = invoke(
                    binary,
                    coordinator_args_with_outbox(
                        &scope.database_url,
                        &scope.messaging_url,
                        shape.batch,
                        shape.parallel,
                    ),
                    &env,
                    watchdog,
                    stdout_limit,
                    stderr_limit,
                )?;
                let counts = product(oracle_coordinator(
                    &scope.database_url,
                    &run_id,
                    shape.events,
                ))?;
                finish(
                    service,
                    scope,
                    baseline,
                    process,
                    counts,
                    ExternalOracle::new(shape.batch as u64, 0, exact),
                )
            }
            "coordinator.recovery.v1" => {
                let run_id = prepared_run_id
                    .context("recovery fixture has no run")?
                    .to_owned();
                let leases = parse_single_dimension(tuple, "leases")?;
                require_success(
                    invoke(
                        binary,
                        coordinator_args_with_limits(
                            &scope.database_url,
                            &scope.messaging_url,
                            1000,
                            64,
                            leases,
                            64,
                        ),
                        &env,
                        Duration::from_secs(180),
                        stdout_limit,
                        stderr_limit,
                    )?,
                    "prepare recovery dispatch",
                )?;
                require_success(
                    invoke(
                        binary,
                        coordinator_args_with_limits(
                            &scope.database_url,
                            &scope.messaging_url,
                            leases,
                            64,
                            leases,
                            64,
                        ),
                        &env,
                        Duration::from_secs(180),
                        stdout_limit,
                        stderr_limit,
                    )?,
                    "publish recovery fixture work",
                )?;
                service.purge_scope_queues(scope)?;
                expire_chunk_leases(&scope.database_url, &run_id, leases)?;
                let baseline = service.begin_measurement(scope)?;
                let started = Instant::now();
                let process = aggregate(
                    vec![
                        invoke(
                            binary,
                            coordinator_args_with_limits(
                                &scope.database_url,
                                &scope.messaging_url,
                                leases,
                                64,
                                leases,
                                64,
                            ),
                            &env,
                            watchdog,
                            stdout_limit,
                            stderr_limit,
                        )?,
                        invoke(
                            binary,
                            coordinator_args_with_limits(
                                &scope.database_url,
                                &scope.messaging_url,
                                leases,
                                64,
                                leases,
                                64,
                            ),
                            &env,
                            watchdog,
                            stdout_limit,
                            stderr_limit,
                        )?,
                    ],
                    started.elapsed(),
                );
                let counts = product(oracle_recovery(&scope.database_url, &run_id, leases))?;
                finish(
                    service,
                    scope,
                    baseline,
                    process,
                    counts,
                    ExternalOracle::new(leases as u64, 0, exact),
                )
            }
            "coordinator.finalization.v1" => {
                let first_run = prepared_run_id
                    .context("finalization fixture has no run")?
                    .to_owned();
                let runs = parse_single_dimension(tuple, "runs")?;
                let mut run_ids = vec![first_run];
                for index in 1..runs {
                    let inputs =
                        self.inputs(&format!("{workload_id}:{tuple}:run-{index}"), 1, evaluators)?;
                    run_ids.push(create_run(
                        binary,
                        &scope.database_url,
                        &inputs,
                        &env,
                        false,
                        Duration::from_secs(180),
                        stdout_limit,
                        stderr_limit,
                    )?);
                }
                require_success(
                    invoke(
                        binary,
                        coordinator_args_with_limits(
                            &scope.database_url,
                            &scope.messaging_url,
                            runs,
                            64,
                            1000,
                            runs,
                        ),
                        &env,
                        Duration::from_secs(180),
                        stdout_limit,
                        stderr_limit,
                    )?,
                    "prepare finalization dispatch",
                )?;
                require_success(
                    invoke(
                        binary,
                        coordinator_args_with_limits(
                            &scope.database_url,
                            &scope.messaging_url,
                            runs,
                            64,
                            1000,
                            runs,
                        ),
                        &env,
                        Duration::from_secs(180),
                        stdout_limit,
                        stderr_limit,
                    )?,
                    "publish finalization fixture work",
                )?;
                service.settled_queue_counts(scope, runs as u64, 0)?;
                for _ in 0..fixture.lifecycle.worker_pass_limit {
                    invoke_workers_batched(
                        runs,
                        8,
                        binary,
                        &scope.database_url,
                        &scope.messaging_url,
                        &env,
                        watchdog,
                        stdout_limit,
                        stderr_limit,
                    )?;
                    if terminal_executions(&scope.database_url)? == runs as i64 {
                        break;
                    }
                    std::thread::sleep(Duration::from_secs(2));
                }
                require_terminal_executions(&scope.database_url, runs)?;
                let baseline = service.begin_measurement(scope)?;
                let process = invoke(
                    binary,
                    coordinator_args_with_limits(
                        &scope.database_url,
                        &scope.messaging_url,
                        runs,
                        64,
                        1000,
                        runs,
                    ),
                    &env,
                    watchdog,
                    stdout_limit,
                    stderr_limit,
                )?;
                let counts = product(oracle_finalization(&scope.database_url, runs))?;
                let _ = run_ids;
                finish(
                    service,
                    scope,
                    baseline,
                    process,
                    counts,
                    ExternalOracle::new(0, 0, exact),
                )
            }
            "worker.execute-wasm.v1"
            | "agent.http-scaling.v1"
            | "agent.http-variants.v1"
            | "evaluator.wasm-scaling.v1"
            | "worker.persistence-scaling.v1" => {
                let run_id = prepared_run_id
                    .context("worker fixture has no run")?
                    .to_owned();
                require_success(
                    invoke(
                        binary,
                        coordinator_args(&scope.database_url, &scope.messaging_url),
                        &env,
                        Duration::from_secs(120),
                        stdout_limit,
                        stderr_limit,
                    )?,
                    "prepare worker queue",
                )?;
                require_success(
                    invoke(
                        binary,
                        coordinator_args(&scope.database_url, &scope.messaging_url),
                        &env,
                        Duration::from_secs(120),
                        stdout_limit,
                        stderr_limit,
                    )?,
                    "publish worker queue",
                )?;
                let baseline = service.begin_measurement(scope)?;
                let process = invoke(
                    binary,
                    worker_args(&scope.database_url, &scope.messaging_url),
                    &env,
                    watchdog,
                    stdout_limit,
                    stderr_limit,
                )?;
                let counts = product(oracle_worker(
                    &scope.database_url,
                    &run_id,
                    cases,
                    evaluators,
                ))?;
                finish(
                    service,
                    scope,
                    baseline,
                    process,
                    counts,
                    ExternalOracle::new(0, cases as u64, exact),
                )
            }
            "system.lifecycle.v1" | "system.capacity.v1" => {
                let inputs = self.inputs(&format!("{workload_id}:{tuple}"), cases, evaluators)?;
                let baseline = service.begin_measurement(scope)?;
                let started = Instant::now();
                let create = invoke(
                    binary,
                    create_args(&scope.database_url, &inputs)?,
                    &env,
                    watchdog,
                    stdout_limit,
                    stderr_limit,
                )?;
                product(require_success_ref(&create, "lifecycle run creation"))?;
                let run_id = product(parse_run_id(&create.stdout.text()))?;
                let mut outcomes = vec![create];
                let workers = lifecycle_workers(tuple)?;
                let cycle_limit = if workload_id == "system.capacity.v1" {
                    fixture.lifecycle.capacity_cycle_limit
                } else {
                    fixture.lifecycle.coordinator_cycle_limit
                };
                for _ in 0..cycle_limit {
                    outcomes.push(invoke(
                        binary,
                        coordinator_args(&scope.database_url, &scope.messaging_url),
                        &env,
                        watchdog,
                        stdout_limit,
                        stderr_limit,
                    )?);
                    outcomes.extend(invoke_workers(
                        workers,
                        binary,
                        &scope.database_url,
                        &scope.messaging_url,
                        &env,
                        watchdog,
                        stdout_limit,
                        stderr_limit,
                    )?);
                    outcomes.push(invoke(
                        binary,
                        coordinator_args(&scope.database_url, &scope.messaging_url),
                        &env,
                        watchdog,
                        stdout_limit,
                        stderr_limit,
                    )?);
                    if run_status(&scope.database_url, &run_id)? == "completed" {
                        let counts = product(oracle_lifecycle(
                            &scope.database_url,
                            &run_id,
                            cases,
                            evaluators,
                        ))?;
                        let process = aggregate(outcomes, started.elapsed());
                        return finish(
                            service,
                            scope,
                            baseline,
                            process,
                            counts,
                            ExternalOracle::new(0, cases as u64, exact),
                        );
                    }
                }
                Err(product_failure(format!(
                    "lifecycle run {run_id} did not complete within its cycle limit"
                )))
            }
            "system.soak.v1" => self.execute_soak(
                tuple,
                binary,
                cases,
                evaluators,
                reliability.context("soak workload omitted its reliability contract")?,
                scope,
                &env,
                watchdog,
                stdout_limit,
                stderr_limit,
            ),
            "system.recovery.v1" => self.execute_recovery(
                tuple,
                binary,
                cases,
                evaluators,
                reliability.context("recovery workload omitted its reliability contract")?,
                scope,
                &env,
                watchdog,
                stdout_limit,
                stderr_limit,
            ),
            _ => bail!("unsupported service workload: {workload_id}"),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_soak(
        &mut self,
        tuple: &str,
        binary: &Path,
        cases: usize,
        evaluators: usize,
        contract: &ReliabilityContract,
        scope: &SampleScope,
        env: &[(String, String)],
        watchdog: Duration,
        stdout_limit: usize,
        stderr_limit: usize,
    ) -> Result<WorkloadOutcome> {
        if tuple != "steady-30m" {
            bail!("unsupported soak tuple: {tuple}");
        }
        let service = self
            .service
            .as_ref()
            .context("service topology was not started")?;
        service.configure_agent(
            &self
                .fixture
                .as_ref()
                .context("fixture was not loaded")?
                .agent_response_text,
            self.fixture
                .as_ref()
                .context("fixture was not loaded")?
                .agent_payload_bytes,
            Duration::ZERO,
        )?;
        let baseline = service.begin_measurement(scope)?;
        let started = Instant::now();
        let mut coordinator = spawn_resident(
            binary,
            coordinator_start_args(&scope.database_url, &scope.messaging_url),
            env,
            stdout_limit,
            stderr_limit,
        )?;
        let mut worker = spawn_resident(
            binary,
            worker_start_args(&scope.database_url, &scope.messaging_url),
            env,
            stdout_limit,
            stderr_limit,
        )?;
        let mut outcomes = Vec::new();
        let mut run_ids = Vec::new();
        let mut observations = Vec::new();
        let duration = Duration::from_secs(contract.duration_secs);
        let interval = Duration::from_secs(contract.observation_interval_secs);
        let mut next_observation = interval;
        while started.elapsed() < duration {
            let inputs = self.inputs(
                &format!("system.soak.v1:{tuple}:{}", run_ids.len()),
                cases,
                evaluators,
            )?;
            let create = invoke(
                binary,
                create_args(&scope.database_url, &inputs)?,
                env,
                watchdog,
                stdout_limit,
                stderr_limit,
            )?;
            product(require_success_ref(&create, "soak run creation"))?;
            let run_id = product(parse_run_id(&create.stdout.text()))?;
            outcomes.push(create);
            product(wait_for_completed_run(
                &scope.database_url,
                &run_id,
                &mut coordinator,
                &mut worker,
                Duration::from_secs(contract.recovery_deadline_secs),
            ))?;
            product(oracle_lifecycle(
                &scope.database_url,
                &run_id,
                cases,
                evaluators,
            ))?;
            run_ids.push(run_id);
            sleep_until(started, next_observation.min(duration));
            observations.push(stability_observation(
                started.elapsed(),
                run_ids.len() as u64 * cases as u64,
                &mut coordinator,
                &mut worker,
            )?);
            next_observation = next_observation.saturating_add(interval);
        }
        if observations
            .last()
            .is_some_and(|observation| observation.elapsed_secs < contract.duration_secs as f64)
        {
            observations.push(stability_observation(
                started.elapsed(),
                run_ids.len() as u64 * cases as u64,
                &mut coordinator,
                &mut worker,
            )?);
        }
        let coordinator_running = coordinator.snapshot()?.running;
        let worker_running = worker.snapshot()?.running;
        let coordinator = stop_resident(coordinator, coordinator_running, "coordinator")?;
        let worker = stop_resident(worker, worker_running, "worker")?;
        outcomes.extend([coordinator, worker]);
        let queue = service.wait_for_queue_counts(
            scope,
            0,
            0,
            Duration::from_secs(contract.recovery_deadline_secs),
        )?;
        let deliveries = service.wait_for_worker_deliveries(
            scope,
            run_ids.len() as u64,
            Duration::from_secs(contract.recovery_deadline_secs),
        )?;
        let (counts, totals) =
            reliability_counts(&scope.database_url, &run_ids, cases, queue, deliveries)?;
        let failures = evaluate_soak(contract, &observations, &totals);
        self.write_reliability_artifact(ReliabilityArtifact {
            schema_id: RELIABILITY_SCHEMA.into(),
            workload_id: "system.soak.v1".into(),
            tuple_id: tuple.into(),
            kind: "soak".into(),
            observations,
            totals,
            recovery_seconds: None,
            fault_injected: false,
            passed: failures.is_empty(),
            failures: failures.clone(),
        })?;
        product(require_pass(&failures))?;
        finish(
            service,
            scope,
            baseline,
            aggregate(outcomes, started.elapsed()),
            counts,
            ExternalOracle::new(0, run_ids.len() as u64 * cases as u64, &BTreeMap::new()),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_recovery(
        &mut self,
        tuple: &str,
        binary: &Path,
        cases: usize,
        evaluators: usize,
        contract: &ReliabilityContract,
        scope: &SampleScope,
        env: &[(String, String)],
        watchdog: Duration,
        stdout_limit: usize,
        stderr_limit: usize,
    ) -> Result<WorkloadOutcome> {
        if tuple != "rabbitmq-restart" {
            bail!("unsupported recovery tuple: {tuple}");
        }
        let service = self
            .service
            .as_ref()
            .context("service topology was not started")?;
        let baseline = service.begin_measurement(scope)?;
        let started = Instant::now();
        let mut coordinator = spawn_resident(
            binary,
            coordinator_start_args(&scope.database_url, &scope.messaging_url),
            env,
            stdout_limit,
            stderr_limit,
        )?;
        let mut worker = spawn_resident(
            binary,
            worker_start_args(&scope.database_url, &scope.messaging_url),
            env,
            stdout_limit,
            stderr_limit,
        )?;
        let mut outcomes = Vec::new();
        let mut run_ids = Vec::new();
        let inputs = self.inputs(
            &format!("system.recovery.v1:{tuple}:before"),
            cases,
            evaluators,
        )?;
        let create = invoke(
            binary,
            create_args(&scope.database_url, &inputs)?,
            env,
            watchdog,
            stdout_limit,
            stderr_limit,
        )?;
        product(require_success_ref(
            &create,
            "pre-fault recovery run creation",
        ))?;
        let run_id = product(parse_run_id(&create.stdout.text()))?;
        outcomes.push(create);
        product(wait_for_completed_run(
            &scope.database_url,
            &run_id,
            &mut coordinator,
            &mut worker,
            Duration::from_secs(contract.recovery_deadline_secs),
        ))?;
        product(oracle_lifecycle(
            &scope.database_url,
            &run_id,
            cases,
            evaluators,
        ))?;
        run_ids.push(run_id);
        let mut observations = vec![stability_observation(
            started.elapsed(),
            cases as u64,
            &mut coordinator,
            &mut worker,
        )?];
        let pre_fault_deliveries = service.wait_for_worker_deliveries(
            scope,
            1,
            Duration::from_secs(contract.recovery_deadline_secs),
        )?;
        let fault_started = Instant::now();
        service.restart_rabbitmq()?;
        let inputs = self.inputs(
            &format!("system.recovery.v1:{tuple}:after"),
            cases,
            evaluators,
        )?;
        let create = invoke(
            binary,
            create_args(&scope.database_url, &inputs)?,
            env,
            watchdog,
            stdout_limit,
            stderr_limit,
        )?;
        product(require_success_ref(
            &create,
            "post-fault recovery run creation",
        ))?;
        let run_id = product(parse_run_id(&create.stdout.text()))?;
        outcomes.push(create);
        product(wait_for_completed_run(
            &scope.database_url,
            &run_id,
            &mut coordinator,
            &mut worker,
            Duration::from_secs(contract.recovery_deadline_secs),
        ))?;
        let recovery_seconds = fault_started.elapsed().as_secs_f64();
        product(oracle_lifecycle(
            &scope.database_url,
            &run_id,
            cases,
            evaluators,
        ))?;
        run_ids.push(run_id);
        observations.push(stability_observation(
            started.elapsed(),
            run_ids.len() as u64 * cases as u64,
            &mut coordinator,
            &mut worker,
        )?);
        let coordinator_running = coordinator.snapshot()?.running;
        let worker_running = worker.snapshot()?.running;
        let coordinator = stop_resident(coordinator, coordinator_running, "coordinator")?;
        let worker = stop_resident(worker, worker_running, "worker")?;
        outcomes.extend([coordinator, worker]);
        let queue = service.wait_for_queue_counts(
            scope,
            0,
            0,
            Duration::from_secs(contract.recovery_deadline_secs),
        )?;
        let post_fault_deliveries = service.wait_for_worker_deliveries(
            scope,
            1,
            Duration::from_secs(contract.recovery_deadline_secs),
        )?;
        let (counts, totals) = reliability_counts(
            &scope.database_url,
            &run_ids,
            cases,
            queue,
            pre_fault_deliveries.saturating_add(post_fault_deliveries),
        )?;
        let failures = evaluate_recovery(
            contract,
            &observations,
            &totals,
            true,
            Some(recovery_seconds),
        );
        self.write_reliability_artifact(ReliabilityArtifact {
            schema_id: RELIABILITY_SCHEMA.into(),
            workload_id: "system.recovery.v1".into(),
            tuple_id: tuple.into(),
            kind: "recovery".into(),
            observations,
            totals,
            recovery_seconds: Some(recovery_seconds),
            fault_injected: true,
            passed: failures.is_empty(),
            failures: failures.clone(),
        })?;
        product(require_pass(&failures))?;
        finish(
            service,
            scope,
            baseline,
            aggregate(outcomes, started.elapsed()),
            counts,
            ExternalOracle::new(0, run_ids.len() as u64 * cases as u64, &BTreeMap::new()),
        )
    }

    fn write_reliability_artifact(&self, artifact: ReliabilityArtifact) -> Result<()> {
        let directory = self.run_dir.join("reliability");
        std::fs::create_dir_all(&directory)?;
        let name = format!("{}-{}", artifact.workload_id, artifact.tuple_id)
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() || character == '-' {
                    character
                } else {
                    '-'
                }
            })
            .collect::<String>();
        super::artifact::atomic_json(&directory.join(format!("{name}.json")), &artifact)?;
        super::reliability::rerender(&self.run_dir)
    }

    /// Renders deterministic profile and dataset inputs beneath the campaign directory.
    fn inputs(&self, identity: &str, cases: usize, evaluators: usize) -> Result<RunInputs> {
        let fixture = self.fixture.as_ref().context("fixture was not loaded")?;
        let service = self
            .service
            .as_ref()
            .context("service topology was not started")?;
        let directory = self
            .run_dir
            .join("fixtures")
            .join(identity.replace([':', '\\', '/'], "_"));
        write_run_inputs(
            &directory,
            fixture,
            identity,
            service.agent_url(),
            cases,
            evaluators,
        )
    }

    fn inputs_with_payload(
        &self,
        identity: &str,
        cases: usize,
        evaluators: usize,
        payload_bytes: usize,
    ) -> Result<RunInputs> {
        let fixture = self.fixture.as_ref().context("fixture was not loaded")?;
        let service = self
            .service
            .as_ref()
            .context("service topology was not started")?;
        let directory = self
            .run_dir
            .join("fixtures")
            .join(identity.replace([':', '\\', '/'], "_"));
        write_run_inputs_with_payload(
            &directory,
            fixture,
            identity,
            service.agent_url(),
            cases,
            evaluators,
            payload_bytes,
        )
    }
}

fn combine_sample_cleanup<T>(
    result: Result<T>,
    scope_cleanup: Result<()>,
    route_cleanup: Result<()>,
) -> Result<T> {
    match (result, scope_cleanup, route_cleanup) {
        (Ok(outcome), Ok(()), Ok(())) => Ok(outcome),
        (Err(error), Ok(()), Ok(())) => Err(error),
        (Ok(_), scope_cleanup, route_cleanup) => {
            bail!("sample cleanup failed: scope={scope_cleanup:?}, routes={route_cleanup:?}")
        }
        (Err(error), scope_cleanup, route_cleanup) => Err(error.context(format!(
            "sample cleanup also returned scope={scope_cleanup:?}, routes={route_cleanup:?}"
        ))),
    }
}

/// Resolves a registered workload tuple into its setup cardinalities.
///
/// Unknown workload IDs and tuples fail closed; future workload versions add
/// new match arms without changing callers or shared service infrastructure.
fn workload_shape(
    fixture: &FixtureCatalog,
    workload_id: &str,
    tuple: &str,
) -> Result<WorkloadShape> {
    let shape = match workload_id {
        "run.create.v1" => WorkloadShape {
            cases: fixture.run_create.cases,
            evaluators: 1,
            create_run: false,
        },
        "run.create-ingestion.v1" => {
            let shape = parse_ingestion_tuple(tuple)?;
            WorkloadShape {
                cases: shape.cases,
                evaluators: 1,
                create_run: shape.reuse,
            }
        }
        "run.create-scaling.v1" | "run.create-boundaries.v1" => WorkloadShape {
            cases: parse_single_dimension(tuple, "cases")?,
            evaluators: 1,
            create_run: false,
        },
        "run.cancel-scaling.v1" => WorkloadShape {
            cases: parse_cancel_tuple(tuple)?.executions,
            evaluators: 1,
            create_run: true,
        },
        "run.read.v1" | "run.export.v1" => WorkloadShape {
            cases: parse_read_tuple(tuple)?.executions,
            evaluators: 1,
            create_run: true,
        },
        "shard.move.v1" => WorkloadShape {
            cases: parse_move_tuple(tuple)?.rows,
            evaluators: 1,
            create_run: true,
        },
        "shard.rebalance.v1" => WorkloadShape {
            cases: parse_rebalance_tuple(tuple)? * fixture.coordinator.cases_per_chunk,
            evaluators: 1,
            create_run: true,
        },
        "coordinator.placement-scaling.v1" => WorkloadShape {
            cases: parse_placement_tuple(tuple)? * fixture.coordinator.cases_per_chunk,
            evaluators: 1,
            create_run: true,
        },
        "coordinator.dispatch.v1" => WorkloadShape {
            cases: fixture.coordinator.chunks * fixture.coordinator.cases_per_chunk,
            evaluators: 1,
            create_run: true,
        },
        "coordinator.dispatch-scaling.v1" => WorkloadShape {
            cases: parse_single_dimension(tuple, "chunks")?
                .checked_mul(fixture.coordinator.cases_per_chunk)
                .context("coordinator workload cardinality overflow")?,
            evaluators: 1,
            create_run: true,
        },
        "coordinator.outbox-scaling.v1" => WorkloadShape {
            cases: parse_outbox_tuple(tuple)?
                .events
                .checked_mul(fixture.coordinator.cases_per_chunk)
                .context("outbox workload cardinality overflow")?,
            evaluators: 1,
            create_run: true,
        },
        "coordinator.recovery.v1" => WorkloadShape {
            cases: parse_single_dimension(tuple, "leases")?
                .checked_mul(fixture.coordinator.cases_per_chunk)
                .context("recovery workload cardinality overflow")?,
            evaluators: 1,
            create_run: true,
        },
        "coordinator.finalization.v1" => WorkloadShape {
            cases: 1,
            evaluators: 1,
            create_run: true,
        },
        "worker.execute-wasm.v1" => match tuple {
            "cases-8-evaluators-1" => WorkloadShape {
                cases: fixture.worker.cases_many,
                evaluators: 1,
                create_run: true,
            },
            "cases-1-evaluators-8" => WorkloadShape {
                cases: 1,
                evaluators: fixture.worker.evaluators_many,
                create_run: true,
            },
            _ => bail!("unsupported worker tuple: {tuple}"),
        },
        "agent.http-scaling.v1" | "agent.http-variants.v1" => WorkloadShape {
            cases: parse_http_tuple(tuple)?.0,
            evaluators: 1,
            create_run: true,
        },
        "evaluator.wasm-scaling.v1" => WorkloadShape {
            cases: 1,
            evaluators: parse_single_dimension(tuple, "evaluators")?,
            create_run: true,
        },
        "worker.persistence-scaling.v1" => WorkloadShape {
            cases: parse_single_dimension(tuple, "cases")?,
            evaluators: 1,
            create_run: true,
        },
        "system.lifecycle.v1" => WorkloadShape {
            cases: fixture.lifecycle.cases,
            evaluators: 1,
            create_run: false,
        },
        "system.soak.v1" | "system.recovery.v1" => WorkloadShape {
            cases: fixture.lifecycle.cases,
            evaluators: 1,
            create_run: false,
        },
        "system.capacity.v1" => {
            let (_, load) = capacity_tuple(tuple)?;
            WorkloadShape {
                cases: fixture.lifecycle.cases.saturating_mul(load),
                evaluators: 1,
                create_run: false,
            }
        }
        _ => bail!("unsupported service workload: {workload_id}"),
    };
    Ok(shape)
}

/// Bounded orthogonal input and reuse dimensions for initial creation measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IngestionShape {
    cases: usize,
    payload_bytes: usize,
    reuse: bool,
    reference: bool,
    gates: usize,
    placements: usize,
    creators: usize,
}

/// Caps fixture allocation and keeps the registered ingestion campaign bounded.
fn parse_ingestion_tuple(tuple: &str) -> Result<IngestionShape> {
    let parts = tuple.split('-').collect::<Vec<_>>();
    if !matches!(parts.len(), 5 | 11)
        || parts[0] != "cases"
        || parts[2] != "payload"
        || !matches!(parts[4], "fresh" | "reuse" | "reference")
    {
        bail!("invalid ingestion tuple: {tuple}");
    }
    let (gates, placements, creators) = if parts.len() == 11 {
        if parts[5] != "gates" || parts[7] != "placements" || parts[9] != "creators" {
            bail!("invalid ingestion dimensions");
        }
        (
            parts[6].parse::<usize>()?,
            parts[8].parse::<usize>()?,
            parts[10].parse::<usize>()?,
        )
    } else {
        (0, 0, 1)
    };
    if gates > 8
        || placements > 4
        || !matches!(creators, 1 | 2 | 4)
        || (creators > 1 && parts[4] != "reference")
    {
        bail!("unsupported ingestion dimensions");
    }
    let cases = parts[1].parse::<usize>()?;
    let payload_bytes = parts[3].parse::<usize>()?;
    if !(1..=64_000).contains(&cases)
        || !(1..=65_536).contains(&payload_bytes)
        || cases
            .checked_mul(payload_bytes)
            .and_then(|n| n.checked_mul(creators))
            .is_none_or(|bytes| bytes > 256 * 1024 * 1024)
    {
        bail!("ingestion fixture exceeds case, payload, or aggregate byte bounds");
    }
    Ok(IngestionShape {
        cases,
        payload_bytes,
        reuse: parts[4] != "fresh",
        reference: parts[4] == "reference",
        gates,
        placements,
        creators,
    })
}

fn parse_single_dimension(tuple: &str, dimension: &str) -> Result<usize> {
    let (actual, value) = tuple
        .split_once('-')
        .with_context(|| format!("invalid {dimension} tuple: {tuple}"))?;
    if actual != dimension {
        bail!("invalid {dimension} tuple: {tuple}");
    }
    let value = value.parse::<usize>()?;
    if value == 0 {
        bail!("{dimension} tuple must be positive");
    }
    Ok(value)
}

fn parse_http_tuple(tuple: &str) -> Result<(usize, usize, u64)> {
    let parts = tuple.split('-').collect::<Vec<_>>();
    if parts.len() != 6 || parts[0] != "cases" || parts[2] != "payload" || parts[4] != "delay" {
        bail!("invalid HTTP workload tuple: {tuple}");
    }
    let cases = parts[1].parse::<usize>()?;
    let payload = parts[3].parse::<usize>()?;
    let delay = parts[5].parse::<u64>()?;
    if cases == 0 || payload == 0 {
        bail!("HTTP workload cases and payload must be positive");
    }
    Ok((cases, payload, delay))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AdminShape {
    routes: usize,
    executions: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadOperation {
    Status,
    Results,
    Export,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExportFormat {
    Json,
    Jsonl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReadShape {
    operation: ReadOperation,
    format: Option<ExportFormat>,
    routes: usize,
    executions: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MoveShape {
    rows: usize,
    payload_bytes: usize,
}

/// Parses cancellation fanout without permitting an unreviewed fleet shape.
fn parse_cancel_tuple(tuple: &str) -> Result<AdminShape> {
    let parts = tuple.split('-').collect::<Vec<_>>();
    if parts.len() != 4 || parts[0] != "aliases" || parts[2] != "executions" {
        bail!("invalid cancellation tuple: {tuple}");
    }
    let shape = AdminShape {
        routes: parts[1].parse()?,
        executions: parts[3].parse()?,
    };
    if !matches!(shape.routes, 1 | 4 | 17) || !matches!(shape.executions, 1024 | 8192) {
        bail!("unsupported cancellation tuple: {tuple}");
    }
    Ok(shape)
}

/// Parses status, results, and export tuples with their paging boundary.
fn parse_read_tuple(tuple: &str) -> Result<ReadShape> {
    let parts = tuple.split('-').collect::<Vec<_>>();
    if parts.len() != 5 || parts[1] != "executions" || parts[3] != "routes" {
        bail!("invalid read/export tuple: {tuple}");
    }
    let (operation, format) = match parts[0] {
        "status" => (ReadOperation::Status, None),
        "results" => (ReadOperation::Results, None),
        "json" => (ReadOperation::Export, Some(ExportFormat::Json)),
        "jsonl" => (ReadOperation::Export, Some(ExportFormat::Jsonl)),
        _ => bail!("unsupported read/export operation in tuple: {tuple}"),
    };
    let shape = ReadShape {
        operation,
        format,
        executions: parts[2].parse()?,
        routes: parts[4].parse()?,
    };
    if !matches!(shape.executions, 250 | 251) || !matches!(shape.routes, 1 | 2) {
        bail!("unsupported read/export tuple: {tuple}");
    }
    Ok(shape)
}

/// Parses the reviewed logical-placement points used by the scaling model.
fn parse_placement_tuple(tuple: &str) -> Result<usize> {
    let aliases = parse_single_dimension(tuple, "aliases")?;
    if !matches!(aliases, 1 | 8 | 16 | 32) {
        bail!("unsupported placement tuple: {tuple}");
    }
    Ok(aliases)
}

/// Parses row and payload boundaries for one routed shard move.
fn parse_move_tuple(tuple: &str) -> Result<MoveShape> {
    let parts = tuple.split('-').collect::<Vec<_>>();
    if parts.len() != 4 || parts[0] != "rows" || parts[2] != "payload" {
        bail!("invalid shard move tuple: {tuple}");
    }
    let shape = MoveShape {
        rows: parts[1].parse()?,
        payload_bytes: parts[3].parse()?,
    };
    if !matches!(shape.rows, 4 | 1000 | 1001)
        || !matches!(shape.payload_bytes, 1024 | 1_048_064 | 1_049_088)
    {
        bail!("unsupported shard move tuple: {tuple}");
    }
    Ok(shape)
}

/// Parses the bounded number of shards claimed by a rebalance pass.
fn parse_rebalance_tuple(tuple: &str) -> Result<usize> {
    let shards = parse_single_dimension(tuple, "shards")?;
    if !matches!(shards, 1 | 8) {
        bail!("unsupported shard rebalance tuple: {tuple}");
    }
    Ok(shards)
}

/// Returns the number of logical database routes a workload fixture must own.
fn required_routes(workload_id: &str, tuple: &str) -> Result<usize> {
    match workload_id {
        "run.cancel-scaling.v1" => Ok(parse_cancel_tuple(tuple)?.routes),
        "run.read.v1" | "run.export.v1" => Ok(parse_read_tuple(tuple)?.routes),
        "shard.move.v1" | "shard.rebalance.v1" => Ok(2),
        "coordinator.placement-scaling.v1" => parse_placement_tuple(tuple),
        "run.create-ingestion.v1" => Ok(1 + parse_ingestion_tuple(tuple)?.placements),
        _ => Ok(1),
    }
}

/// Validates the semantic record count of machine-readable read output.
fn validate_read_output(shape: ReadShape, stdout: &str, run_id: &str) -> Result<()> {
    if shape.operation == ReadOperation::Export && shape.format == Some(ExportFormat::Jsonl) {
        let mut run_records = 0_usize;
        let mut summary_records = 0_usize;
        let mut execution_records = 0_usize;
        let mut execution_ids = BTreeSet::new();
        for line in stdout.lines().filter(|line| !line.trim().is_empty()) {
            let value: Value = serde_json::from_str(line).context("parse JSONL export record")?;
            let record_type = value["type"]
                .as_str()
                .context("JSONL export record omitted type")?;
            match record_type {
                "run" => {
                    require_run_id(&value, &["/run/id"], run_id, "JSONL run record")?;
                    run_records += 1;
                }
                "results_summary" => {
                    require_run_id(&value, &["/run_id"], run_id, "JSONL results summary")?;
                    require_json_count(&value, &["results", "execution_count"], shape.executions)?;
                    summary_records += 1;
                }
                "execution" => {
                    require_run_id(&value, &["/run_id"], run_id, "JSONL execution record")?;
                    require_run_id(
                        &value,
                        &["/execution/run_id"],
                        run_id,
                        "JSONL execution payload",
                    )?;
                    let execution_id = value["execution"]["id"]
                        .as_str()
                        .context("JSONL execution record omitted execution.id")?;
                    if !execution_ids.insert(execution_id.to_owned()) {
                        bail!("JSONL export repeated execution {execution_id}");
                    }
                    execution_records += 1;
                }
                "run_scorecard"
                | "execution_route"
                | "execution_aggregate"
                | "execution_attempt"
                | "evaluator_result"
                | "evaluator_diagnostic" => {
                    require_run_id(&value, &["/run_id"], run_id, "JSONL export record")?;
                }
                other => bail!("JSONL export contained unsupported record type {other:?}"),
            }
        }
        if run_records != 1
            || summary_records != 1
            || execution_records != shape.executions
            || execution_ids.len() != shape.executions
        {
            bail!(
                "JSONL export contained {run_records} run, {summary_records} summary, and {execution_records} unique execution records; expected 1, 1, and {}",
                shape.executions
            );
        }
        return Ok(());
    }

    let value: Value = serde_json::from_str(stdout).context("parse read command JSON output")?;
    match shape.operation {
        ReadOperation::Status => {
            require_run_id(
                &value,
                &["/data/run_id", "/data/run/run_id", "/data/run/id"],
                run_id,
                "status output",
            )?;
            let status = value["data"]["status"]
                .as_str()
                .or_else(|| value["data"]["run"]["status"].as_str());
            if status != Some("completed") {
                bail!("status output did not report a completed run");
            }
        }
        ReadOperation::Results => {
            require_run_id(
                &value,
                &["/data/run/run_id", "/data/run/id"],
                run_id,
                "results output",
            )?;
            require_json_count(
                &value,
                &["data", "results", "execution_count"],
                shape.executions,
            )?;
            require_json_count(
                &value,
                &["data", "results", "evaluator_result_count"],
                shape.executions,
            )?;
        }
        ReadOperation::Export => {
            require_run_id(
                &value,
                &["/data/run/run_id", "/data/run/id"],
                run_id,
                "JSON export",
            )?;
            require_json_count(
                &value,
                &["data", "results", "execution_count"],
                shape.executions,
            )?;
            let rows = value["data"]["executions"]
                .as_array()
                .or_else(|| value["executions"].as_array())
                .context("JSON export omitted executions")?;
            if rows.len() != shape.executions {
                bail!(
                    "JSON export contained {} executions; expected {}",
                    rows.len(),
                    shape.executions
                );
            }
            let mut execution_ids = BTreeSet::new();
            for row in rows {
                require_run_id(row, &["/execution/run_id"], run_id, "JSON export execution")?;
                let execution_id = row["execution"]["id"]
                    .as_str()
                    .context("JSON export execution omitted execution.id")?;
                if !execution_ids.insert(execution_id) {
                    bail!("JSON export repeated execution {execution_id}");
                }
            }
        }
    }
    Ok(())
}

fn require_run_id(value: &Value, pointers: &[&str], expected: &str, context: &str) -> Result<()> {
    let actual = pointers
        .iter()
        .find_map(|pointer| value.pointer(pointer).and_then(Value::as_str))
        .with_context(|| format!("{context} omitted run identity"))?;
    if actual != expected {
        bail!("{context} returned run {actual}, expected {expected}");
    }
    Ok(())
}

/// Enforces output-stream timing and format-specific export memory contracts.
fn validate_read_resources(shape: ReadShape, process: &ProcessOutcome) -> Result<()> {
    let first = process
        .stdout
        .first_byte_time
        .context("read command emitted no first-byte timing")?;
    let last = process
        .stdout
        .last_byte_time
        .context("read command emitted no last-byte timing")?;
    if first > last || last > process.wall_time {
        bail!("read command output timing was not monotonic within process wall time");
    }
    if shape.operation == ReadOperation::Export {
        let peak_rss = process
            .peak_rss_bytes
            .context("export memory contract requires a peak-RSS collector")?;
        let limit = match shape.format {
            Some(ExportFormat::Jsonl) => 256 * 1024 * 1024,
            Some(ExportFormat::Json) => 512 * 1024 * 1024,
            None => unreachable!("export shapes declare a format"),
        };
        if peak_rss > limit {
            bail!(
                "export peak RSS was {peak_rss} bytes; {:?} contract allows {limit}",
                shape.format
            );
        }
    }
    Ok(())
}

fn require_json_count(value: &Value, path: &[&str], expected: usize) -> Result<()> {
    let actual = path.iter().fold(value, |value, key| &value[*key]).as_u64();
    if actual != Some(expected as u64) {
        bail!(
            "JSON count {} differed: actual {actual:?}, expected {expected}",
            path.join(".")
        );
    }
    Ok(())
}

/// Requires cancellation output to prove terminal state and replay behavior.
fn validate_cancel_output(stdout: &str, executions: usize, replay: bool) -> Result<()> {
    let value: Value = serde_json::from_str(stdout).context("parse cancellation JSON output")?;
    if value["data"]["status"] != "cancelled"
        || value["meta"]["terminal"] != true
        || value["meta"]["executions_cancelled"].as_u64() != Some(executions as u64)
        || value["meta"]["already_cancelled"].as_bool() != Some(replay)
    {
        bail!("cancellation output violated the exact terminal/replay contract");
    }
    Ok(())
}

/// Requires every table and the authoritative placement to verify at the target.
fn validate_move_output(stdout: &str, target: &str, moved: bool) -> Result<()> {
    let value: Value = serde_json::from_str(stdout).context("parse shard move JSON output")?;
    let tables = value["data"]["tables"]
        .as_array()
        .context("shard move output omitted table reports")?;
    if value["data"]["target_database_alias"] != target
        || value["data"]["placement"]["database_alias"] != target
        || value["meta"]["moved"].as_bool() != Some(moved)
        || value["meta"]["verified"] != true
        || tables.is_empty()
        || tables.iter().any(|table| table["verified"] != true)
    {
        bail!("shard move output violated route-switch or verification contract");
    }
    Ok(())
}

/// Extracts the persisted operation identity needed by apply and verify commands.
fn parse_rebalance_operation_id(stdout: &str) -> Result<String> {
    let value: Value = serde_json::from_str(stdout).context("parse rebalance plan JSON output")?;
    value["data"]["operation"]["id"]
        .as_str()
        .map(ToOwned::to_owned)
        .context("rebalance plan omitted persisted operation ID")
}

/// Requires a completed rebalance with the exact verified item count.
fn validate_rebalance_output(stdout: &str, shards: usize) -> Result<()> {
    let value: Value =
        serde_json::from_str(stdout).context("parse rebalance verify JSON output")?;
    if value["meta"]["verified"] != true
        || value["meta"]["verified_item_count"].as_u64() != Some(shards as u64)
        || value["data"]["operation"]["status"] != "completed"
    {
        bail!("rebalance output violated the completed verification contract");
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OutboxShape {
    events: usize,
    batch: usize,
    parallel: usize,
}

fn parse_outbox_tuple(tuple: &str) -> Result<OutboxShape> {
    let parts = tuple.split('-').collect::<Vec<_>>();
    if parts.len() != 6 || parts[0] != "events" || parts[2] != "batch" || parts[4] != "parallel" {
        bail!("invalid outbox workload tuple: {tuple}");
    }
    let shape = OutboxShape {
        events: parts[1].parse()?,
        batch: parts[3].parse()?,
        parallel: parts[5].parse()?,
    };
    if shape.events == 0
        || !matches!(shape.batch, 1 | 64 | 65 | 256 | 1000)
        || !matches!(shape.parallel, 1 | 8 | 64)
    {
        bail!("unsupported outbox workload tuple: {tuple}");
    }
    Ok(shape)
}

/// Resolves the lifecycle tuple into the number of measured worker processes.
fn lifecycle_workers(tuple: &str) -> Result<usize> {
    match tuple {
        "workers-1" => Ok(1),
        "workers-2" => Ok(2),
        _ => capacity_tuple(tuple)
            .map(|(workers, _)| workers)
            .map_err(|_| anyhow::anyhow!("unsupported lifecycle tuple: {tuple}")),
    }
}

/// Parses one predeclared worker/load capacity-staircase tuple.
pub(super) fn capacity_tuple(tuple: &str) -> Result<(usize, usize)> {
    let parts = tuple.split('-').collect::<Vec<_>>();
    if parts.len() != 4 || parts[0] != "workers" || parts[2] != "load" {
        bail!("unsupported capacity tuple: {tuple}");
    }
    let workers = parts[1].parse::<usize>()?;
    let load = parts[3].parse::<usize>()?;
    if !matches!(workers, 1 | 2) || !matches!(load, 1 | 2 | 4 | 8 | 16) {
        bail!("unsupported capacity tuple: {tuple}");
    }
    Ok((workers, load))
}

impl Drop for WorkloadRunner {
    fn drop(&mut self) {
        if let Some(service) = self.service.as_mut() {
            for prepared in self.prepared.values() {
                for placement in &prepared.placements {
                    if let Err(error) = service.release_database(&placement.url) {
                        eprintln!("failed to release prepared routed database: {error:#}");
                    }
                }
                if let Err(error) = service.release_database(&prepared.url) {
                    eprintln!("failed to release prepared performance database: {error:#}");
                }
            }
        }
    }
}

/// Resolves a required setup asset relative to its immutable build manifest.
fn setup_asset(manifest_path: &Path, relative: &str) -> Result<PathBuf> {
    let path = manifest_path
        .parent()
        .context("build manifest has no parent")?
        .join(relative);
    if !path.exists() {
        bail!("build setup asset is missing: {}", path.display());
    }
    Ok(path)
}

/// Prefixes a Vigilo command with explicit database and machine-output settings.
fn base_args<const N: usize>(database_url: &str, command: [String; N]) -> Vec<String> {
    let mut args = vec![
        "--database-url".into(),
        database_url.into(),
        "-q".into(),
        "-f".into(),
        "json".into(),
    ];
    args.extend(command);
    args
}

fn setup_database(
    binary: &Path,
    database_url: &str,
    migrations: &Path,
    limits: ExecutionLimits,
) -> Result<()> {
    require_success(
        invoke(
            binary,
            base_args(
                database_url,
                [
                    "setup".into(),
                    "--migrations-dir".into(),
                    path_arg(migrations)?,
                    "--skip-evaluators".into(),
                ],
            ),
            &[],
            Duration::from_secs(120),
            limits.stdout,
            limits.stderr,
        )?,
        "prepare database schema",
    )
}

fn database_register_args(database_url: &str, alias: &str, env_key: &str) -> Vec<String> {
    base_args(
        database_url,
        [
            "database".into(),
            "register".into(),
            alias.into(),
            "--database-url-env".into(),
            env_key.into(),
        ],
    )
}

fn database_activate_args(database_url: &str, alias: &str) -> Vec<String> {
    base_args(
        database_url,
        ["database".into(), "activate".into(), alias.into()],
    )
}

fn shard_assign_args(
    database_url: &str,
    run_id: &str,
    run_shard: usize,
    target: &str,
) -> Vec<String> {
    base_args(
        database_url,
        [
            "run".into(),
            "shard".into(),
            "assign".into(),
            run_id.into(),
            run_shard.to_string(),
            "--to".into(),
            target.into(),
        ],
    )
}

fn run_admin_args(database_url: &str, operation: &str, run_id: &str) -> Vec<String> {
    base_args(
        database_url,
        ["run".into(), operation.into(), run_id.into()],
    )
}

fn read_args(database_url: &str, run_id: &str, shape: ReadShape) -> Vec<String> {
    match (shape.operation, shape.format) {
        (ReadOperation::Status, None) => run_admin_args(database_url, "status", run_id),
        (ReadOperation::Results, None) => run_admin_args(database_url, "results", run_id),
        (ReadOperation::Export, Some(format)) => base_args(
            database_url,
            [
                "run".into(),
                "export".into(),
                run_id.into(),
                "--format".into(),
                match format {
                    ExportFormat::Json => "json".into(),
                    ExportFormat::Jsonl => "jsonl".into(),
                },
                "--batch-size".into(),
                "250".into(),
            ],
        ),
        _ => unreachable!("validated read shape has a matching format"),
    }
}

fn shard_move_args(database_url: &str, run_id: &str, target: &str) -> Vec<String> {
    base_args(
        database_url,
        [
            "shard".into(),
            "move".into(),
            run_id.into(),
            "0".into(),
            "--alias".into(),
            target.into(),
        ],
    )
}

fn rebalance_plan_args(database_url: &str, shards: usize) -> Vec<String> {
    base_args(
        database_url,
        [
            "rebalance".into(),
            "plan".into(),
            "--from".into(),
            "primary".into(),
            "--to".into(),
            "perf_01".into(),
            "--max-items".into(),
            shards.to_string(),
        ],
    )
}

fn rebalance_apply_args(database_url: &str, operation_id: &str, shards: usize) -> Vec<String> {
    base_args(
        database_url,
        [
            "rebalance".into(),
            "apply".into(),
            operation_id.into(),
            "--max-items".into(),
            shards.to_string(),
        ],
    )
}

fn rebalance_verify_args(database_url: &str, operation_id: &str, shards: usize) -> Vec<String> {
    base_args(
        database_url,
        [
            "rebalance".into(),
            "verify".into(),
            operation_id.into(),
            "--max-items".into(),
            shards.to_string(),
        ],
    )
}

/// Reference measurements submit no dataset source; file replay remains a separate tuple.
fn ingestion_args(
    database_url: &str,
    inputs: &RunInputs,
    upload: Option<&str>,
) -> Result<Vec<String>> {
    match upload {
        Some(id) => Ok(base_args(
            database_url,
            [
                "run".into(),
                "create".into(),
                "--profile-file".into(),
                path_arg(&inputs.profile)?,
                "--dataset-upload-id".into(),
                id.into(),
            ],
        )),
        None => create_args(database_url, inputs),
    }
}

fn create_args(database_url: &str, inputs: &RunInputs) -> Result<Vec<String>> {
    create_args_with_placement(database_url, inputs, false)
}

fn create_args_with_placement(
    database_url: &str,
    inputs: &RunInputs,
    spread: bool,
) -> Result<Vec<String>> {
    let mut args = base_args(
        database_url,
        [
            "run".into(),
            "create".into(),
            "--profile-file".into(),
            path_arg(&inputs.profile)?,
            "--dataset-file".into(),
            path_arg(&inputs.dataset)?,
        ],
    );
    if spread {
        args.extend([
            "--default-shard-database-alias".into(),
            "primary".into(),
            "--shard-assignment-policy".into(),
            "spread-active".into(),
        ]);
    }
    Ok(args)
}

fn coordinator_args(database_url: &str, messaging_url: &str) -> Vec<String> {
    coordinator_args_with_outbox(database_url, messaging_url, 1000, 64)
}

fn coordinator_start_args(database_url: &str, messaging_url: &str) -> Vec<String> {
    base_args(
        database_url,
        [
            "coordinator".into(),
            "--messaging-url".into(),
            messaging_url.into(),
            "--tick-seconds".into(),
            "1".into(),
            "start".into(),
        ],
    )
}

fn coordinator_args_with_outbox(
    database_url: &str,
    messaging_url: &str,
    outbox_batch: usize,
    publish_parallelism: usize,
) -> Vec<String> {
    coordinator_args_with_limits(
        database_url,
        messaging_url,
        outbox_batch,
        publish_parallelism,
        1000,
        64,
    )
}

fn coordinator_args_with_limits(
    database_url: &str,
    messaging_url: &str,
    outbox_batch: usize,
    publish_parallelism: usize,
    recovery_batch: usize,
    finalize_limit: usize,
) -> Vec<String> {
    base_args(
        database_url,
        [
            "coordinator".into(),
            "--messaging-url".into(),
            messaging_url.into(),
            "--run-chunk-dispatch-window-size".into(),
            "512".into(),
            "--max-dispatch-per-cycle".into(),
            "128".into(),
            "--max-finalize-per-cycle".into(),
            finalize_limit.to_string(),
            "--chunk-lease-recovery-batch-size".into(),
            recovery_batch.to_string(),
            "--outbox-batch-size".into(),
            outbox_batch.to_string(),
            "--outbox-publish-parallelism".into(),
            publish_parallelism.to_string(),
            "once".into(),
        ],
    )
}

#[allow(clippy::too_many_arguments)]
fn invoke_workers_batched(
    count: usize,
    batch: usize,
    binary: &Path,
    database_url: &str,
    messaging_url: &str,
    env: &[(String, String)],
    timeout: Duration,
    stdout_limit: usize,
    stderr_limit: usize,
) -> Result<()> {
    for workers in (0..count).collect::<Vec<_>>().chunks(batch) {
        for outcome in invoke_workers(
            workers.len(),
            binary,
            database_url,
            messaging_url,
            env,
            timeout,
            stdout_limit,
            stderr_limit,
        )? {
            require_success(outcome, "prepare finalization worker")?;
        }
    }
    Ok(())
}

fn worker_args(database_url: &str, messaging_url: &str) -> Vec<String> {
    base_args(
        database_url,
        [
            "worker".into(),
            "--messaging-url".into(),
            messaging_url.into(),
            "once".into(),
        ],
    )
}

fn worker_start_args(database_url: &str, messaging_url: &str) -> Vec<String> {
    base_args(
        database_url,
        [
            "worker".into(),
            "--messaging-url".into(),
            messaging_url.into(),
            "start".into(),
            "--max-inflight-chunks".into(),
            "4".into(),
        ],
    )
}

fn spawn_resident(
    binary: &Path,
    args: Vec<String>,
    env: &[(String, String)],
    stdout_limit: usize,
    stderr_limit: usize,
) -> Result<RunningProcess> {
    RunningProcess::spawn(&ProcessSpec {
        program: binary,
        args: &args,
        current_dir: None,
        env,
        timeout: Duration::MAX,
        stdout_limit,
        stderr_limit,
    })
}

fn stop_resident(process: RunningProcess, was_running: bool, name: &str) -> Result<ProcessOutcome> {
    let mut outcome = process.stop(Duration::from_secs(10))?;
    if !was_running {
        bail!("resident {name} exited before harness shutdown");
    }
    // A harness-requested signal is expected lifecycle, not a product failure.
    outcome.exit_code = Some(0);
    outcome.timed_out = false;
    Ok(outcome)
}

fn wait_for_completed_run(
    database_url: &str,
    run_id: &str,
    coordinator: &mut RunningProcess,
    worker: &mut RunningProcess,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let status = run_status(database_url, run_id)?;
        if status == "completed" {
            return Ok(());
        }
        if matches!(status.as_str(), "failed" | "cancelled") {
            bail!("run {run_id} reached terminal status {status} during reliability workload");
        }
        if !coordinator.snapshot()?.running || !worker.snapshot()?.running {
            bail!("resident process exited before run {run_id} completed");
        }
        if Instant::now() >= deadline {
            bail!("run {run_id} did not complete within the recovery deadline");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn stability_observation(
    elapsed: Duration,
    completed_cases: u64,
    coordinator: &mut RunningProcess,
    worker: &mut RunningProcess,
) -> Result<StabilityObservation> {
    let coordinator = coordinator.snapshot()?;
    let worker = worker.snapshot()?;
    Ok(StabilityObservation {
        elapsed_secs: elapsed.as_secs_f64(),
        completed_cases,
        process_rss_bytes: sum_optional(coordinator.rss_bytes, worker.rss_bytes),
        file_descriptors: sum_optional(coordinator.file_descriptors, worker.file_descriptors),
        processes_running: coordinator.running && worker.running,
    })
}

fn sum_optional(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    left.zip(right)
        .map(|(left, right)| left.saturating_add(right))
}

fn sleep_until(started: Instant, target: Duration) {
    if let Some(remaining) = target.checked_sub(started.elapsed()) {
        std::thread::sleep(remaining);
    }
}

fn reliability_counts(
    database_url: &str,
    run_ids: &[String],
    cases: usize,
    queue: (u64, u64),
    deliveries: u64,
) -> Result<(DurableCounts, ReliabilityTotals)> {
    let mut aggregate = DurableCounts::new();
    for run_id in run_ids {
        for (name, count) in oracle_lifecycle(database_url, run_id, cases, 1)? {
            *aggregate.entry(name).or_default() += count;
        }
    }
    let nonnegative = |name: &str| -> Result<u64> {
        u64::try_from(aggregate.get(name).copied().unwrap_or_default())
            .with_context(|| format!("reliability durable count {name} was negative"))
    };
    let attempts = nonnegative("attempts")?;
    let chunks = nonnegative("chunks")?;
    let completed_cases = nonnegative("executions")?;
    let totals = ReliabilityTotals {
        expected_cases: run_ids.len() as u64 * cases as u64,
        completed_cases,
        attempts,
        chunks,
        deliveries,
        queue_ready: queue.0,
        queue_unacked: queue.1,
    };
    aggregate.insert("completed_cases".into(), completed_cases as i64);
    aggregate.insert("worker_deliveries".into(), deliveries as i64);
    Ok((aggregate, totals))
}

/// Runs one bounded Vigilo process without a shell.
fn invoke(
    binary: &Path,
    args: Vec<String>,
    env: &[(String, String)],
    timeout: Duration,
    stdout_limit: usize,
    stderr_limit: usize,
) -> Result<ProcessOutcome> {
    execute(&ProcessSpec {
        program: binary,
        args: &args,
        current_dir: None,
        env,
        timeout,
        stdout_limit,
        stderr_limit,
    })
}

#[allow(clippy::too_many_arguments)]
/// Runs the requested worker count concurrently and returns every process outcome.
fn invoke_workers(
    count: usize,
    binary: &Path,
    database_url: &str,
    messaging_url: &str,
    env: &[(String, String)],
    timeout: Duration,
    stdout_limit: usize,
    stderr_limit: usize,
) -> Result<Vec<ProcessOutcome>> {
    std::thread::scope(|scope| {
        let handles = (0..count)
            .map(|_| {
                let args = worker_args(database_url, messaging_url);
                scope.spawn(move || invoke(binary, args, env, timeout, stdout_limit, stderr_limit))
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| anyhow::anyhow!("worker process thread panicked"))?
            })
            .collect()
    })
}

/// Starts a fixed, bounded group of creators together; every child is reaped
/// even when another fails. Counts cover the entire group, never an average.
#[allow(clippy::too_many_arguments)]
fn invoke_creators(
    count: usize,
    binary: &Path,
    args: Vec<String>,
    env: &[(String, String)],
    timeout: Duration,
    stdout: usize,
    stderr: usize,
) -> Result<Vec<ProcessOutcome>> {
    anyhow::ensure!(matches!(count, 1 | 2 | 4), "invalid creator concurrency");
    if count == 1 {
        return Ok(vec![invoke(binary, args, env, timeout, stdout, stderr)?]);
    }
    let barrier = std::sync::Barrier::new(count);
    std::thread::scope(|scope| {
        let handles = (0..count)
            .map(|_| {
                let args = args.clone();
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    invoke(binary, args, env, timeout, stdout, stderr)
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| anyhow::anyhow!("creator thread panicked"))?
            })
            .collect()
    })
}

/// Checks every scheduled ordinal and local gate denominator across placements.
/// Metadata is bounded by the fixture's maximum 640 chunks and 128 shards.
fn verify_ingestion_chunks(
    control: &str,
    placements: &[String],
    run_id: &str,
    shape: IngestionShape,
) -> Result<()> {
    let mut chunks = Vec::new();
    let mut ids = std::collections::BTreeSet::new();
    let mut snapshots = std::collections::BTreeSet::new();
    for url in routed_urls(control, placements) {
        let mut db = Client::connect(&url, NoTls)?;
        for row in db.query("SELECT id::text,run_shard,ordinal_start,ordinal_end,status::text,dispatched_at IS NULL,profile_group_id FROM run_chunks WHERE run_id=$1::text::uuid ORDER BY ordinal_start LIMIT 641",&[&run_id])? {
            let id:String=row.get(0); anyhow::ensure!(ids.insert(id),"duplicate scheduled chunk");
            let shard:i16=row.get(1); let start:i32=row.get(2); let end:i32=row.get(3);
            anyhow::ensure!(row.get::<_,String>(4)=="pending" && row.get::<_,bool>(5) && row.get::<_,String>(6)=="default", "changed scheduled chunk state");
            chunks.push((start,end,shard));
        }
        for row in db.query("SELECT run_shard,expected_execution_count,config_snapshot->'gate_planned_counts' FROM run_snapshots WHERE run_id=$1::text::uuid LIMIT 129",&[&run_id])? {
            let shard:i16=row.get(0); let count:i32=row.get(1);
            anyhow::ensure!(snapshots.insert(shard),"duplicate snapshot shard");
            let expected=(0..shape.cases).filter(|n| (n/100%128) as i16 == shard).count();
            anyhow::ensure!(count as usize == expected,"snapshot case count differs");
            let counts:Value=row.get(2);
            let wanted:serde_json::Map<String,Value>=(0..shape.gates).map(|n|(format!("gate-{n:02}"),serde_json::json!(expected))).collect();
            anyhow::ensure!(counts==Value::Object(wanted),"snapshot gate counts differ");
        }
    }
    anyhow::ensure!(
        snapshots.len() == shape.cases.div_ceil(100).min(128),
        "missing shard snapshot"
    );
    chunks.sort();
    anyhow::ensure!(
        chunks.len() == shape.cases.div_ceil(100),
        "chunk count differs"
    );
    for (index, (start, end, shard)) in chunks.into_iter().enumerate() {
        anyhow::ensure!(
            start as usize == index * 100
                && end as usize == ((index + 1) * 100).min(shape.cases)
                && shard as usize == index % 128,
            "scheduled ordinal coverage differs"
        );
    }
    Ok(())
}

/// Creates a prerequisite run and extracts its stable machine-readable ID.
#[allow(clippy::too_many_arguments)]
fn create_run(
    binary: &Path,
    database_url: &str,
    inputs: &RunInputs,
    env: &[(String, String)],
    spread: bool,
    timeout: Duration,
    stdout_limit: usize,
    stderr_limit: usize,
) -> Result<String> {
    let outcome = invoke(
        binary,
        create_args_with_placement(database_url, inputs, spread)?,
        env,
        timeout,
        stdout_limit,
        stderr_limit,
    )?;
    require_success_ref(&outcome, "prepare run fixture")?;
    parse_run_id(&outcome.stdout.text())
}

/// Seeds open execution rows from the immutable case projections for cancellation.
fn ensure_execution_routes(
    binary: &Path,
    database_url: &str,
    run_id: &str,
    expected_routes: usize,
    env: &[(String, String)],
    limits: ExecutionLimits,
) -> Result<()> {
    let mut client = Client::connect(database_url, NoTls)?;
    let existing = scalar(
        &mut client,
        "SELECT COUNT(*)::bigint FROM shard_placements WHERE run_id = $1::text::uuid",
        run_id,
    )? as usize;
    for run_shard in existing..expected_routes {
        let alias = if run_shard == 0 {
            "primary".to_owned()
        } else {
            format!("perf_{:02}", run_shard % expected_routes)
        };
        require_success(
            invoke(
                binary,
                shard_assign_args(database_url, run_id, run_shard, &alias),
                env,
                Duration::from_secs(120),
                limits.stdout,
                limits.stderr,
            )?,
            "assign empty cancellation route",
        )?;
    }
    Ok(())
}

fn seed_open_executions(
    service: &ServiceHarness,
    database_urls: &[String],
    run_id: &str,
) -> Result<()> {
    let reader = service.storage_reader()?;
    let control = database_urls
        .first()
        .context("missing fixture control database")?;
    let mut owners = std::collections::BTreeMap::new();
    for (index, url) in database_urls.iter().enumerate() {
        let mut client = Client::connect(url, NoTls)?;
        for row in client.query(
            "SELECT DISTINCT run_shard FROM run_chunks WHERE run_id=$1::text::uuid",
            &[&run_id],
        )? {
            let shard: i16 = row.get(0);
            ensure!(
                owners.insert(shard, index).is_none(),
                "fixture shard is stored in multiple databases"
            );
        }
    }
    reader.batches(control,run_id,|records|{
        let mut batches=std::collections::BTreeMap::<usize,Vec<Value>>::new();
        for record in records {
            let ordinal=record["case_ordinal"].as_i64().context("missing fixture case ordinal")?;
            let owner=owners.get(&((ordinal/100%128) as i16)).context("case has no fixture execution placement")?;
            batches.entry(*owner).or_default().push(record);
        }
        for (index,records) in batches {
            let mut client=Client::connect(&database_urls[index],NoTls)?;
            client.execute(r#"
                INSERT INTO executions(run_id,run_shard,chunk_id,case_id,case_hash,profile_group_id,task_type,tags,input_payload,expected_output,case_metadata,evaluation_profile_id,evaluation_profile_version,evaluator_manifest,expected_evaluator_count,status)
                SELECT c.run_id,c.run_shard,c.id,(r->'source'->>'id')::uuid,r->>'case_hash','sentiment',r->'source'->>'task_type',COALESCE(r->'source'->'tags','[]'::jsonb),r->'source'->'input',COALESCE(r->'source'->'expected','null'::jsonb),COALESCE(r->'source'->'metadata','{}'::jsonb),run.evaluation_profile_id,run.evaluation_profile_version,'[]'::jsonb,0,'pending'::execution_status
                FROM jsonb_array_elements($2::jsonb) r
                JOIN run_chunks c ON c.run_id=$1::text::uuid AND (r->>'case_ordinal')::int>=c.ordinal_start AND (r->>'case_ordinal')::int<c.ordinal_end
                JOIN runs run ON run.id=c.run_id ON CONFLICT(run_id,run_shard,case_id) DO NOTHING
            "#,&[&run_id,&serde_json::json!(records)])?;
        }
        Ok(())
    })?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
/// Completes a reusable routed run and adds deterministic diagnostic rows.
fn prepare_terminal_run(
    service: &mut ServiceHarness,
    _binary: &Path,
    control_database_url: &str,
    run_id: &str,
    database_urls: &[String],
    _route_env: &[(String, String)],
    cases: usize,
    _worker_pass_limit: usize,
    _limits: ExecutionLimits,
) -> Result<()> {
    seed_open_executions(service, database_urls, run_id)?;
    for database_url in database_urls {
        let mut client = Client::connect(database_url, NoTls)?;
        client.execute(
            r#"
            UPDATE executions
            SET status = 'completed'::execution_status,
                current_attempt_no = 1,
                expected_evaluator_count = 1,
                started_at = now(), completed_at = now(), updated_at = now()
            WHERE run_id = $1::text::uuid
            "#,
            &[&run_id],
        )?;
        client.execute(
            r#"
            INSERT INTO execution_attempts (
                execution_id, run_id, run_shard, attempt_no, status,
                worker_id, worker_host, agent_latency_ms,
                evaluator_latency_ms, total_latency_ms, token_usage,
                outcome_summary, started_at, completed_at
            )
            SELECT id, run_id, run_shard, 1, 'completed'::attempt_status,
                   gen_random_uuid(), 'performance-fixture', 1, 1, 2,
                   '{"input":1,"output":1}'::jsonb,
                   '{"fixture":true}'::jsonb, now(), now()
            FROM executions
            WHERE run_id = $1::text::uuid
            ON CONFLICT (run_id, run_shard, execution_id, attempt_no) DO NOTHING
            "#,
            &[&run_id],
        )?;
        client.execute(
            r#"
            UPDATE executions execution
            SET current_attempt_id = attempt.id, updated_at = now()
            FROM execution_attempts attempt
            WHERE execution.run_id = $1::text::uuid
              AND attempt.run_id = execution.run_id
              AND attempt.run_shard = execution.run_shard
              AND attempt.execution_id = execution.id
              AND attempt.attempt_no = 1
            "#,
            &[&run_id],
        )?;
        client.execute(
            r#"
            INSERT INTO evaluator_results (
                run_id, run_shard, execution_id, attempt_id, binding_id,
                evaluator_id, evaluator_version, evaluator_profile_id,
                evaluator_profile_version, evaluator_interface_version,
                evaluator_runtime_version, dimension, outcome, judgment,
                blocking, measurement_kind, raw_ordinal, normalized_score,
                normalization_policy_hash, pass_threshold, weight,
                raw_evaluator_output
            )
            SELECT execution.run_id, execution.run_shard, execution.id,
                   attempt.id, 'sentiment-00', gen_random_uuid(), '0.1.0',
                   execution.evaluation_profile_id,
                   execution.evaluation_profile_version, '1.0.0',
                   'fixture', 'quality', 'completed'::evaluator_outcome,
                   'passed'::evaluation_status, true, 'ordinal', 'positive',
                   1.0, 'performance-fixture-policy', 0.5, 1.0,
                   '{"label":"positive","fixture":true}'::jsonb
            FROM executions execution
            JOIN execution_attempts attempt
              ON attempt.run_id = execution.run_id
             AND attempt.run_shard = execution.run_shard
             AND attempt.execution_id = execution.id
             AND attempt.attempt_no = 1
            WHERE execution.run_id = $1::text::uuid
            ON CONFLICT (run_id, run_shard, attempt_id, binding_id) DO NOTHING
            "#,
            &[&run_id],
        )?;
        client.execute(
            r#"
            INSERT INTO execution_aggregates (
                execution_id, run_id, run_shard, attempt_id,
                overall_status, aggregate_score, evaluator_result_count,
                dimension_scores, blocking_failures, summary
            )
            SELECT execution.id, execution.run_id, execution.run_shard,
                   attempt.id, 'passed'::evaluation_status, 1.0, 1,
                   '{"quality":1.0}'::jsonb, '[]'::jsonb,
                   '{"fixture":true}'::jsonb
            FROM executions execution
            JOIN execution_attempts attempt
              ON attempt.run_id = execution.run_id
             AND attempt.run_shard = execution.run_shard
             AND attempt.execution_id = execution.id
             AND attempt.attempt_no = 1
            WHERE execution.run_id = $1::text::uuid
            ON CONFLICT (run_id, run_shard, execution_id) DO NOTHING
            "#,
            &[&run_id],
        )?;
        client.execute(
            r#"
            INSERT INTO evaluator_diagnostics (
                run_id, run_shard, evaluator_result_id, diagnostic_index,
                severity, category, reason, evidence, tags
            )
            SELECT run_id, run_shard, id, 0, 'none'::severity,
                   'performance_fixture', 'deterministic diagnostic',
                   '{"source":"performance_fixture"}'::jsonb,
                   ARRAY['performance']::text[]
            FROM evaluator_results
            WHERE run_id = $1::text::uuid
            ON CONFLICT (run_id, run_shard, evaluator_result_id, diagnostic_index)
            DO NOTHING
            "#,
            &[&run_id],
        )?;
        client.execute(
            r#"
            INSERT INTO run_snapshots (
                run_id, run_shard, run_key, dataset_id, dataset_version_id,
                dataset_version, evaluation_profile_id,
                evaluation_profile_version, profile_version_id, profile_hash,
                aggregation_policy_id, aggregation_policy_version,
                aggregation_policy_hash, agent_provider, agent_name,
                agent_version, prompt_config_id, prompt_config_version,
                config_snapshot, expected_execution_count
            )
            SELECT run.id, execution.run_shard, run.run_key, run.dataset_id,
                   run.dataset_version_id, run.dataset_version,
                   run.evaluation_profile_id, run.evaluation_profile_version,
                   run.profile_version_id, run.profile_hash,
                   run.aggregation_policy_id, run.aggregation_policy_version,
                   run.aggregation_policy_hash, run.agent_provider,
                   run.agent_name, run.agent_version, run.prompt_config_id,
                   run.prompt_config_version, run.config_snapshot,
                   COUNT(*)::integer
            FROM runs run
            JOIN executions execution ON execution.run_id = run.id
            WHERE run.id = $1::text::uuid
            GROUP BY run.id, execution.run_shard
            ON CONFLICT (run_id, run_shard) DO UPDATE
            SET expected_execution_count = EXCLUDED.expected_execution_count,
                updated_at = now()
            "#,
            &[&run_id],
        )?;
        client.execute(
            r#"
            INSERT INTO run_shard_summaries (
                run_id, run_shard, expected_execution_count,
                execution_count, terminal_execution_count, aggregate_count,
                passed_execution_count, evaluator_result_count,
                score_count, score_sum, min_score, max_score, status
            )
            SELECT run_id, run_shard, COUNT(*)::integer, COUNT(*)::integer,
                   COUNT(*)::integer, COUNT(*)::integer, COUNT(*)::integer,
                   COUNT(*)::bigint, COUNT(*)::bigint,
                   COUNT(*)::double precision, 1.0, 1.0, 'completed'
            FROM executions
            WHERE run_id = $1::text::uuid
            GROUP BY run_id, run_shard
            ON CONFLICT (run_id, run_shard) DO UPDATE
            SET expected_execution_count = EXCLUDED.expected_execution_count,
                execution_count = EXCLUDED.execution_count,
                terminal_execution_count = EXCLUDED.terminal_execution_count,
                aggregate_count = EXCLUDED.aggregate_count,
                passed_execution_count = EXCLUDED.passed_execution_count,
                evaluator_result_count = EXCLUDED.evaluator_result_count,
                score_count = EXCLUDED.score_count,
                score_sum = EXCLUDED.score_sum,
                min_score = EXCLUDED.min_score,
                max_score = EXCLUDED.max_score,
                status = EXCLUDED.status,
                updated_at = now()
            "#,
            &[&run_id],
        )?;
        client.execute(
            "UPDATE run_chunks SET status = 'completed', dispatched_at = COALESCE(dispatched_at, now()), updated_at = now() WHERE run_id = $1::text::uuid",
            &[&run_id],
        )?;
        client.execute(
            r#"
            UPDATE runs
            SET status = 'completed'::run_status,
                gate_status = 'pass'::gate_status,
                terminal_execution_count = expected_execution_count,
                passed_execution_count = expected_execution_count,
                failed_execution_count = 0,
                errored_execution_count = 0,
                summary = '{"fixture":true,"average_score":1.0}'::jsonb,
                started_at = COALESCE(started_at, now()),
                dispatched_at = COALESCE(dispatched_at, now()),
                finalized_at = now(), completed_at = now(), updated_at = now()
            WHERE id = $1::text::uuid
            "#,
            &[&run_id],
        )?;
    }
    let completed = total_run_count(database_urls, run_id, "executions", "status = 'completed'")?;
    if completed != cases as i64 {
        bail!("seeded {completed} completed executions; expected {cases}");
    }
    require_status_value(&run_status(control_database_url, run_id)?, "completed")
}

fn total_run_count(
    database_urls: &[String],
    run_id: &str,
    table: &str,
    predicate: &str,
) -> Result<i64> {
    let mut total = 0_i64;
    for database_url in database_urls {
        let mut client = Client::connect(database_url, NoTls)?;
        total += scalar(
            &mut client,
            &format!(
                "SELECT COUNT(*)::bigint FROM {table} WHERE run_id = $1::text::uuid AND {predicate}"
            ),
            run_id,
        )?;
    }
    Ok(total)
}

/// Moves all narrow fixture membership and chunks into logical shard zero.
fn consolidate_shard_rows(database_url: &str, run_id: &str) -> Result<()> {
    let mut client = Client::connect(database_url, NoTls)?;
    client.execute(
        "UPDATE run_chunks SET run_shard = 0 WHERE run_id = $1::text::uuid AND run_shard <> 0",
        &[&run_id],
    )?;
    Ok(())
}

fn require_success(outcome: ProcessOutcome, context: &str) -> Result<()> {
    require_success_ref(&outcome, context)
}

fn require_success_ref(outcome: &ProcessOutcome, context: &str) -> Result<()> {
    if outcome.timed_out || outcome.exit_code != Some(0) {
        bail!(
            "{context} failed (exit {:?}, timeout {}): {}",
            outcome.exit_code,
            outcome.timed_out,
            outcome.stderr.text()
        );
    }
    Ok(())
}

fn parse_run_id(stdout: &str) -> Result<String> {
    let value: Value = serde_json::from_str(stdout).context("parse run create JSON output")?;
    value["data"]["run_id"]
        .as_str()
        .map(ToOwned::to_owned)
        .context("run create output omitted data.run_id")
}

fn path_arg(path: &Path) -> Result<String> {
    path.to_str()
        .map(ToOwned::to_owned)
        .with_context(|| format!("path is not valid UTF-8: {}", path.display()))
}

/// Exact external postconditions that a measured process must produce.
struct ExternalOracle<'a> {
    ready: u64,
    http: u64,
    exact: &'a BTreeMap<String, u64>,
}

impl<'a> ExternalOracle<'a> {
    fn new(ready: u64, http: u64, exact: &'a BTreeMap<String, u64>) -> Self {
        Self { ready, http, exact }
    }
}

/// Finalizes a measured region and enforces scoped external postconditions.
///
/// Process success, queue drain state, and deterministic-agent request counts
/// must all match before the observations become a workload outcome.
fn finish(
    service: &ServiceHarness,
    scope: &SampleScope,
    baseline: super::service::MeasurementBaseline,
    process: ProcessOutcome,
    counts: DurableCounts,
    oracle: ExternalOracle<'_>,
) -> Result<WorkloadOutcome> {
    product(require_success_ref(&process, "measured workload"))?;
    let mut external = service.finish_measurement(scope, baseline, counts)?;
    let queue = product(service.settled_queue_counts(scope, oracle.ready, 0))?;
    external.queue_ready = Some(queue.0);
    external.queue_unacked = Some(queue.1);
    if external.queue_ready != Some(oracle.ready)
        || external.queue_unacked != Some(0)
        || external.http_requests != Some(oracle.http)
    {
        return Err(product_failure(format!(
            "scoped external counts differ: ready={:?}, unacked={:?}, http={:?}; expected {}, 0, {}",
            external.queue_ready,
            external.queue_unacked,
            external.http_requests,
            oracle.ready,
            oracle.http,
        )));
    }
    product(verify_exact_observations(&external, oracle.exact))?;
    Ok(WorkloadOutcome { process, external })
}

/// Applies registry-owned exact amplification and durable-output gates.
fn verify_exact_observations(
    external: &ExternalMeasurements,
    expected: &BTreeMap<String, u64>,
) -> Result<()> {
    for (metric, expected) in expected {
        let actual = match metric.as_str() {
            "http_requests" => external.http_requests,
            "queue_ready" => external.queue_ready,
            "queue_unacked" => external.queue_unacked,
            metric if metric.starts_with("durable.") => external
                .durable_counts
                .get(metric.trim_start_matches("durable."))
                .copied()
                .map(|value| u64::try_from(value).context("durable count was negative"))
                .transpose()?,
            _ => bail!("unsupported exact observation {metric}"),
        };
        if actual != Some(*expected) {
            bail!("exact observation {metric} differed: actual {actual:?}, expected {expected}");
        }
    }
    Ok(())
}

fn scalar(client: &mut Client, query: &str, run_id: &str) -> Result<i64> {
    Ok(client.query_one(query, &[&run_id])?.get(0))
}

/// Reads the common durable row counts used by preparation and workload oracles.
fn structural_counts(database_url: &str, run_id: Option<&str>) -> Result<DurableCounts> {
    let mut counts = DurableCounts::new();
    let Some(run_id) = run_id else {
        return Ok(counts);
    };
    let mut client = Client::connect(database_url, NoTls)?;
    for (name, table, run_column) in [
        ("runs", "runs", "id"),
        ("chunks", "run_chunks", "run_id"),
        ("executions", "executions", "run_id"),
        ("attempts", "execution_attempts", "run_id"),
        ("evaluator_results", "evaluator_results", "run_id"),
    ] {
        counts.insert(
            name.into(),
            scalar(
                &mut client,
                &format!(
                    "SELECT COUNT(*)::bigint FROM {table} WHERE {run_column} = $1::text::uuid"
                ),
                run_id,
            )?,
        );
    }
    Ok(counts)
}

fn routed_urls(control: &str, placements: &[String]) -> Vec<String> {
    std::iter::once(control.to_owned())
        .chain(placements.iter().cloned())
        .collect()
}

fn cancellation_counts(
    control_database_url: &str,
    placements: &[String],
    run_id: &str,
    executions: usize,
) -> Result<DurableCounts> {
    let urls = routed_urls(control_database_url, placements);
    let cancelled = total_run_count(&urls, run_id, "executions", "status = 'cancelled'")?;
    if cancelled != executions as i64 {
        bail!("cancelled {cancelled} executions; expected {executions}");
    }
    let mut client = Client::connect(control_database_url, NoTls)?;
    require_status_value(&run_status_with(&mut client, run_id)?, "cancelled")?;
    let routes = scalar(
        &mut client,
        "SELECT COUNT(*)::bigint FROM shard_placements WHERE run_id = $1::text::uuid",
        run_id,
    )?;
    Ok(DurableCounts::from([
        ("runs".into(), 1),
        ("cancelled_executions".into(), cancelled),
        ("execution_routes".into(), routes),
        ("idempotent_replays".into(), 1),
    ]))
}

fn read_counts(
    control_database_url: &str,
    placements: &[String],
    run_id: &str,
    executions: usize,
) -> Result<DurableCounts> {
    let urls = routed_urls(control_database_url, placements);
    let actual_executions = total_run_count(&urls, run_id, "executions", "TRUE")?;
    let results = total_run_count(&urls, run_id, "evaluator_results", "TRUE")?;
    let diagnostics = total_run_count(&urls, run_id, "evaluator_diagnostics", "TRUE")?;
    if actual_executions != executions as i64
        || results != executions as i64
        || diagnostics != executions as i64
    {
        bail!(
            "read fixture counts differed: executions={actual_executions}, results={results}, diagnostics={diagnostics}; expected {executions} each"
        );
    }
    Ok(DurableCounts::from([
        ("executions".into(), actual_executions),
        ("evaluator_results".into(), results),
        ("evaluator_diagnostics".into(), diagnostics),
    ]))
}

fn shard_move_counts(database_url: &str, run_id: &str) -> Result<DurableCounts> {
    let mut client = Client::connect(database_url, NoTls)?;
    let move_row = client.query_one(
        r#"
        SELECT id::text, copied_row_count, copied_byte_count
        FROM shard_move_operations
        WHERE run_id = $1::text::uuid AND run_shard = 0 AND status = 'completed'
        ORDER BY completed_at DESC
        LIMIT 1
        "#,
        &[&run_id],
    )?;
    let move_id: String = move_row.get(0);
    let copied_rows: i64 = move_row.get(1);
    let copied_bytes: i64 = move_row.get(2);
    let progress = client.query(
        r#"
        SELECT table_name, completed_page_count, copied_row_count, copied_byte_count
        FROM shard_move_table_progress
        WHERE move_id = $1::text::uuid
        ORDER BY table_name
        "#,
        &[&move_id],
    )?;
    if progress.is_empty() {
        bail!("completed shard move has no durable page distribution");
    }
    let mut counts = DurableCounts::from([
        ("move_operations".into(), 1),
        ("move_copied_rows".into(), copied_rows),
        ("move_copied_bytes".into(), copied_bytes),
        ("idempotent_replays".into(), 1),
    ]);
    let mut total_pages = 0_i64;
    for row in progress {
        let table: String = row.get(0);
        let pages: i64 = row.get(1);
        let rows: i64 = row.get(2);
        let bytes: i64 = row.get(3);
        let lower_bound =
            ((rows + 999) / 1000).max((bytes + (4 * 1024 * 1024 - 1)) / (4 * 1024 * 1024));
        if pages < lower_bound || pages <= 0 || rows <= 0 || bytes <= 0 {
            bail!(
                "invalid shard page distribution for {table}: pages={pages}, rows={rows}, bytes={bytes}, lower_bound={lower_bound}"
            );
        }
        total_pages += pages;
        counts.insert(format!("move_pages_{table}"), pages);
        counts.insert(format!("move_rows_{table}"), rows);
        counts.insert(format!("move_bytes_{table}"), bytes);
    }
    counts.insert("move_pages".into(), total_pages);
    Ok(counts)
}

fn rebalance_counts(
    database_url: &str,
    operation_id: &str,
    run_id: &str,
    shards: usize,
) -> Result<DurableCounts> {
    let mut client = Client::connect(database_url, NoTls)?;
    let row = client.query_one(
        r#"
        SELECT status, planned_item_count, completed_item_count,
               failed_item_count, cancelled_item_count
        FROM shard_rebalance_operations
        WHERE id = $1::text::uuid
        "#,
        &[&operation_id],
    )?;
    let status: String = row.get(0);
    let planned: i32 = row.get(1);
    let completed: i32 = row.get(2);
    let failed: i32 = row.get(3);
    let cancelled: i32 = row.get(4);
    let target_routes = scalar(
        &mut client,
        "SELECT COUNT(*)::bigint FROM shard_placements WHERE run_id = $1::text::uuid AND database_alias = 'perf_01'",
        run_id,
    )?;
    if status != "completed"
        || planned != shards as i32
        || completed != shards as i32
        || failed != 0
        || cancelled != 0
        || target_routes != shards as i64
    {
        bail!("rebalance durable state violated its exact completion contract");
    }
    Ok(DurableCounts::from([
        ("rebalance_items".into(), i64::from(planned)),
        ("completed_rebalance_items".into(), i64::from(completed)),
        ("target_routes".into(), target_routes),
        ("apply_passes".into(), shards as i64),
    ]))
}

fn routed_coordinator_counts(
    control_database_url: &str,
    placements: &[String],
    run_id: &str,
    aliases: usize,
) -> Result<DurableCounts> {
    let urls = routed_urls(control_database_url, placements);
    let chunks = total_run_count(&urls, run_id, "run_chunks", "TRUE")?;
    let mut client = Client::connect(control_database_url, NoTls)?;
    let routes = scalar(
        &mut client,
        "SELECT COUNT(*)::bigint FROM shard_placements WHERE run_id = $1::text::uuid",
        run_id,
    )?;
    if chunks != aliases as i64 || routes != aliases as i64 {
        bail!("placement fixture produced {chunks} chunks and {routes} routes; expected {aliases}");
    }
    Ok(DurableCounts::from([
        ("chunks".into(), chunks),
        ("execution_routes".into(), routes),
    ]))
}

/// Verifies reusable coordinator and worker templates contain no measured results.
fn verify_prepared(workload_id: &str, counts: &DurableCounts) -> Result<()> {
    if matches!(
        workload_id,
        "coordinator.dispatch.v1"
            | "coordinator.dispatch-scaling.v1"
            | "coordinator.outbox-scaling.v1"
            | "coordinator.recovery.v1"
            | "coordinator.finalization.v1"
            | "worker.execute-wasm.v1"
            | "agent.http-scaling.v1"
            | "agent.http-variants.v1"
            | "evaluator.wasm-scaling.v1"
            | "worker.persistence-scaling.v1"
    ) && (counts.get("runs") != Some(&1)
        || counts.get("executions") != Some(&0)
        || counts.get("attempts") != Some(&0)
        || counts.get("evaluator_results") != Some(&0))
    {
        bail!("prepared {workload_id} fixture has unexpected structural counts: {counts:?}");
    }
    Ok(())
}

/// Collects and validates the run-creation durable-state oracle.
fn oracle_create(
    database_url: &str,
    run_id: &str,
    cases: usize,
    chunks: i64,
) -> Result<DurableCounts> {
    let mut counts = structural_counts(database_url, Some(run_id))?;
    let mut client = Client::connect(database_url, NoTls)?;
    let status = run_status_with(&mut client, run_id)?;
    validate_create_oracle(&counts, &status, chunks)?;
    counts.insert("cases".into(), cases as i64);
    Ok(counts)
}

/// Proves real stored payload/membership counts and reuse without executing cases.
fn oracle_ingestion(
    service: &ServiceHarness,
    database_url: &str,
    placements: &[String],
    run_id: &str,
    shape: IngestionShape,
    chunks: i64,
) -> Result<DurableCounts> {
    let mut counts = if placements.is_empty() {
        oracle_create(database_url, run_id, shape.cases, chunks)?
    } else {
        let urls = routed_urls(database_url, placements);
        let mut counts =
            DurableCounts::from([("runs".into(), 1), ("cases".into(), shape.cases as i64)]);
        for (name, table) in [
            ("chunks", "run_chunks"),
            ("executions", "executions"),
            ("attempts", "execution_attempts"),
            ("evaluator_results", "evaluator_results"),
        ] {
            counts.insert(name.into(), total_run_count(&urls, run_id, table, "TRUE")?);
        }
        let mut db = Client::connect(database_url, NoTls)?;
        validate_create_oracle(&counts, &run_status_with(&mut db, run_id)?, chunks)?;
        let owners:i64=db.query_one("SELECT count(DISTINCT database_alias) FROM shard_placements WHERE run_id=$1::text::uuid AND database_alias <> 'primary'",&[&run_id])?.get(0);
        if owners != shape.placements as i64 {
            bail!("execution placement count differs");
        }
        counts
    };
    verify_ingestion_chunks(database_url, placements, run_id, shape)?;
    service
        .storage_reader()?
        .gate_counts(database_url, run_id, shape.gates, shape.cases)?;
    let mut client = Client::connect(database_url, NoTls)?;
    let cases = i64::try_from(shape.cases)?;
    for (name, query, expected) in [
        (
            "total_runs",
            "SELECT count(*)::bigint FROM runs",
            shape.creators as i64 + i64::from(shape.reuse),
        ),
        (
            "dataset_versions",
            "SELECT count(*)::bigint FROM dataset_versions",
            1,
        ),
        (
            "dataset_catalogs",
            "SELECT count(*)::bigint FROM dataset_catalog",
            1,
        ),
        (
            "referenced_datasets",
            "SELECT count(DISTINCT dataset_upload_id)::bigint FROM run_plans",
            1,
        ),
    ] {
        let actual: i64 = client.query_one(query, &[])?.get(0);
        counts.insert(name.into(), actual);
        require_count(&counts, name, expected)?;
    }
    let stored = service
        .storage_reader()?
        .batches(database_url, run_id, |records| {
            for record in records {
                let payload = record["source"]["input"]["payload"]
                    .as_str()
                    .context("fixture payload missing")?;
                if payload.len() != shape.payload_bytes || !payload.bytes().all(|c| c == b'x') {
                    bail!("stored fixture payload changed");
                }
                if record["source"]["metadata"]["ordinal"] != record["case_ordinal"] {
                    bail!("stored fixture order changed");
                }
            }
            Ok(())
        })?;
    counts.insert("stored_cases".into(), stored);
    require_count(&counts, "stored_cases", cases)?;
    let sql_cases:i64=client.query_one("SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name=ANY(ARRAY['case_blobs','dataset_version_cases','run_shard_cases'])",&[])?.get(0);
    counts.insert("case_payload_tables".into(), sql_cases);
    require_count(&counts, "case_payload_tables", 0)?;
    require_count(&counts, "attempts", 0)?;
    require_count(&counts, "evaluator_results", 0)?;
    Ok(counts)
}

/// Validates exact metadata admission across the registered creation boundaries.
fn oracle_create_boundary(
    database_url: &str,
    run_id: &str,
    cases: usize,
    chunks: i64,
) -> Result<DurableCounts> {
    let mut counts = structural_counts(database_url, Some(run_id))?;
    let mut client = Client::connect(database_url, NoTls)?;
    let status = run_status_with(&mut client, run_id)?;
    require_status_value(&status, "pending")?;
    require_count(&counts, "runs", 1)?;
    require_count(&counts, "chunks", chunks)?;
    require_count(&counts, "executions", 0)?;
    let admitted: i32 = client
        .query_one(
            "SELECT admission_cursor FROM run_plans WHERE run_id=$1::text::uuid",
            &[&run_id],
        )?
        .get(0);
    if i64::from(admitted) != chunks {
        bail!("creation boundary admitted {admitted} chunks, expected {chunks}");
    }
    counts.insert("cases".into(), cases as i64);
    counts.insert("creation_recovery_pending".into(), 0);
    Ok(counts)
}

/// Collects and validates dispatch state plus exact start and chunk-ready events.
fn oracle_coordinator(database_url: &str, run_id: &str, chunks: usize) -> Result<DurableCounts> {
    let mut counts = structural_counts(database_url, Some(run_id))?;
    let mut client = Client::connect(database_url, NoTls)?;
    let status = run_status_with(&mut client, run_id)?;
    let dispatched = scalar(
        &mut client,
        "SELECT COUNT(*)::bigint FROM run_chunks WHERE run_id = $1::text::uuid AND dispatched_at IS NOT NULL",
        run_id,
    )?;
    let started = scalar(
        &mut client,
        "SELECT COUNT(*)::bigint FROM outbox_events WHERE aggregate_id = $1::text::uuid AND event_type = 'run.started'",
        run_id,
    )?;
    let ready = scalar(
        &mut client,
        "SELECT COUNT(*)::bigint FROM outbox_events WHERE aggregate_id = $1::text::uuid AND event_type = 'run.chunk.ready'",
        run_id,
    )?;
    validate_coordinator_oracle(&status, dispatched, started, ready, chunks)?;
    counts.insert("dispatched_chunks".into(), dispatched);
    counts.insert("run_started_events".into(), started);
    counts.insert("chunk_ready_events".into(), ready);
    Ok(counts)
}

/// Collects and validates worker execution, attempt, result, and chunk counts.
fn oracle_worker(
    database_url: &str,
    run_id: &str,
    cases: usize,
    evaluators: usize,
) -> Result<DurableCounts> {
    let counts = structural_counts(database_url, Some(run_id))?;
    let mut client = Client::connect(database_url, NoTls)?;
    let completed = scalar(
        &mut client,
        "SELECT COUNT(*)::bigint FROM run_chunks WHERE run_id = $1::text::uuid AND status = 'completed'",
        run_id,
    )?;
    if let Err(error) = validate_worker_oracle(&counts, completed, cases, evaluators) {
        let diagnostics = worker_oracle_diagnostics(&mut client, run_id)?;
        return Err(error.context(diagnostics));
    }
    Ok(counts)
}

/// Summarizes persisted execution evidence after an exact worker oracle fails.
///
/// This query runs outside the measured region and keeps failure reports useful
/// even though the isolated service topology is removed during campaign cleanup.
fn worker_oracle_diagnostics(client: &mut Client, run_id: &str) -> Result<String> {
    let row = client.query_one(
        r#"
        SELECT
            COUNT(DISTINCT ea.execution_id)::bigint,
            COALESCE(SUM(ea.evaluator_result_count), 0)::bigint,
            COUNT(DISTINCT er.id)::bigint,
            COUNT(DISTINCT e.id) FILTER (WHERE er.id IS NULL)::bigint,
            COUNT(DISTINCT e.id) FILTER (WHERE e.status = 'completed')::bigint,
            COUNT(DISTINCT e.id) FILTER (WHERE e.status = 'failed')::bigint
        FROM executions e
        LEFT JOIN execution_aggregates ea
          ON ea.run_id = e.run_id
         AND ea.run_shard = e.run_shard
         AND ea.execution_id = e.id
         AND ea.attempt_id = e.current_attempt_id
        LEFT JOIN evaluator_results er
          ON er.run_id = e.run_id
         AND er.run_shard = e.run_shard
         AND er.execution_id = e.id
         AND er.attempt_id = e.current_attempt_id
        WHERE e.run_id = $1::text::uuid
        "#,
        &[&run_id],
    )?;
    let failures = client
        .query(
            r#"
            SELECT last_error_message, COUNT(*)::bigint
            FROM executions
            WHERE run_id = $1::text::uuid
              AND last_error_message IS NOT NULL
            GROUP BY last_error_message
            ORDER BY COUNT(*) DESC, last_error_message
            LIMIT 3
            "#,
            &[&run_id],
        )?
        .into_iter()
        .map(|row| format!("{}x {}", row.get::<_, i64>(1), row.get::<_, String>(0)))
        .collect::<Vec<_>>()
        .join("; ");
    Ok(format!(
        "worker oracle diagnostics: aggregates={}, aggregate_result_total={}, result_rows={}, executions_without_results={}, completed_executions={}, failed_executions={}, failures=[{}]",
        row.get::<_, i64>(0),
        row.get::<_, i64>(1),
        row.get::<_, i64>(2),
        row.get::<_, i64>(3),
        row.get::<_, i64>(4),
        row.get::<_, i64>(5),
        failures,
    ))
}

/// Extends the worker oracle with terminal passing-run invariants.
fn oracle_lifecycle(
    database_url: &str,
    run_id: &str,
    cases: usize,
    evaluators: usize,
) -> Result<DurableCounts> {
    let counts = oracle_worker(database_url, run_id, cases, evaluators)?;
    let mut client = Client::connect(database_url, NoTls)?;
    let status = run_status_with(&mut client, run_id)?;
    let row = client.query_one(
        "SELECT expected_execution_count, terminal_execution_count, passed_execution_count FROM runs WHERE id = $1::text::uuid",
        &[&run_id],
    )?;
    let expected: i32 = row.get(0);
    let terminal: i32 = row.get(1);
    let passed: i32 = row.get(2);
    validate_lifecycle_oracle(&status, [expected, terminal, passed], cases)?;
    Ok(counts)
}

fn expire_chunk_leases(database_url: &str, run_id: &str, expected: usize) -> Result<()> {
    let mut client = Client::connect(database_url, NoTls)?;
    let changed = client.execute(
        r#"
        UPDATE run_chunks
        SET status = 'leased',
            lease_token = '00000000-0000-7000-8000-000000000001'::uuid,
            leased_until = now() - interval '1 minute',
            updated_at = now()
        WHERE run_id = $1::text::uuid
        "#,
        &[&run_id],
    )?;
    if changed != expected as u64 {
        bail!("expired {changed} recovery leases; expected {expected}");
    }
    Ok(())
}

fn oracle_recovery(database_url: &str, run_id: &str, expected: usize) -> Result<DurableCounts> {
    let mut counts = structural_counts(database_url, Some(run_id))?;
    let mut client = Client::connect(database_url, NoTls)?;
    let recovered = scalar(
        &mut client,
        "SELECT COUNT(*)::bigint FROM run_chunks WHERE run_id = $1::text::uuid AND recovery_count = 1 AND status = 'pending'",
        run_id,
    )?;
    let events = scalar(
        &mut client,
        "SELECT COUNT(*)::bigint FROM outbox_events WHERE aggregate_id = $1::text::uuid AND event_type = 'run.chunk.ready' AND dedupe_key LIKE '%:recovery:1'",
        run_id,
    )?;
    if recovered != expected as i64 || events != expected as i64 {
        bail!(
            "recovery oracle differs: recovered={recovered}, events={events}, expected={expected}"
        );
    }
    counts.insert("recovered_chunks".into(), recovered);
    counts.insert("recovery_events".into(), events);
    Ok(counts)
}

fn require_terminal_executions(database_url: &str, expected: usize) -> Result<()> {
    let terminal = terminal_executions(database_url)?;
    if terminal != expected as i64 {
        let mut client = Client::connect(database_url, NoTls)?;
        let rows = client.query(
            "SELECT status::text, COUNT(*)::bigint FROM executions GROUP BY status ORDER BY status::text",
            &[],
        )?;
        let statuses = rows
            .into_iter()
            .map(|row| format!("{}={}", row.get::<_, String>(0), row.get::<_, i64>(1)))
            .collect::<Vec<_>>()
            .join(", ");
        bail!(
            "prepared {terminal} terminal executions; expected {expected}; observed [{statuses}]"
        );
    }
    Ok(())
}

fn terminal_executions(database_url: &str) -> Result<i64> {
    let mut client = Client::connect(database_url, NoTls)?;
    let completed: i64 = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM executions WHERE status IN ('completed', 'failed', 'timed_out', 'cancelled')",
            &[],
        )?
        .get(0);
    Ok(completed)
}

fn oracle_finalization(database_url: &str, expected: usize) -> Result<DurableCounts> {
    let mut client = Client::connect(database_url, NoTls)?;
    let values: [i64; 4] = [
        client
            .query_one("SELECT COUNT(*)::bigint FROM runs", &[])?
            .get(0),
        client
            .query_one(
                "SELECT COUNT(*)::bigint FROM runs WHERE status = 'completed'",
                &[],
            )?
            .get(0),
        client
            .query_one("SELECT COUNT(*)::bigint FROM evaluator_results", &[])?
            .get(0),
        client
            .query_one(
                "SELECT COUNT(*)::bigint FROM outbox_events WHERE event_type = 'run.completed'",
                &[],
            )?
            .get(0),
    ];
    if values != [expected as i64; 4] {
        bail!(
            "finalization oracle differs: runs={}, completed={}, results={}, events={}, expected={expected}",
            values[0],
            values[1],
            values[2],
            values[3]
        );
    }
    Ok(DurableCounts::from([
        ("runs".into(), values[0]),
        ("completed_runs".into(), values[1]),
        ("evaluator_results".into(), values[2]),
        ("completion_events".into(), values[3]),
    ]))
}

/// Applies the pure run-creation postcondition to collected observations.
fn validate_create_oracle(counts: &DurableCounts, status: &str, chunks: i64) -> Result<()> {
    require_count(counts, "runs", 1)?;
    require_count(counts, "chunks", chunks)?;
    require_count(counts, "executions", 0)?;
    require_status_value(status, "pending")
}

/// Applies the pure coordinator postcondition to collected observations.
fn validate_coordinator_oracle(
    status: &str,
    dispatched: i64,
    started: i64,
    ready: i64,
    chunks: usize,
) -> Result<()> {
    require_status_value(status, "running")?;
    if dispatched != chunks as i64 {
        bail!("coordinator dispatched {dispatched} chunks; expected {chunks}");
    }
    if started != 1 || ready != chunks as i64 {
        bail!("coordinator outbox counts differ: run.started={started}, run.chunk.ready={ready}");
    }
    Ok(())
}

/// Applies the pure worker/Wasm postcondition to collected observations.
fn validate_worker_oracle(
    counts: &DurableCounts,
    completed: i64,
    cases: usize,
    evaluators: usize,
) -> Result<()> {
    require_count(counts, "executions", cases as i64)?;
    require_count(counts, "attempts", cases as i64)?;
    require_count(
        counts,
        "evaluator_results",
        cases.saturating_mul(evaluators) as i64,
    )?;
    if completed != 1 {
        bail!("worker completed {completed} chunks; expected 1");
    }
    Ok(())
}

/// Applies the pure end-to-end lifecycle postcondition to collected observations.
fn validate_lifecycle_oracle(status: &str, totals: [i32; 3], cases: usize) -> Result<()> {
    require_status_value(status, "completed")?;
    if totals != [cases as i32; 3] {
        bail!(
            "lifecycle execution totals differ: expected={}, terminal={}, passed={}",
            totals[0],
            totals[1],
            totals[2]
        );
    }
    Ok(())
}

fn run_status(database_url: &str, run_id: &str) -> Result<String> {
    let mut client = Client::connect(database_url, NoTls)?;
    run_status_with(&mut client, run_id)
}

fn run_status_with(client: &mut Client, run_id: &str) -> Result<String> {
    Ok(client
        .query_one(
            "SELECT status::text FROM runs WHERE id = $1::text::uuid",
            &[&run_id],
        )?
        .get(0))
}

fn require_status_value(actual: &str, expected: &str) -> Result<()> {
    if actual != expected {
        bail!("run status is {actual}; expected {expected}");
    }
    Ok(())
}

fn require_count(counts: &DurableCounts, name: &str, expected: i64) -> Result<()> {
    let actual = counts.get(name).copied().unwrap_or_default();
    if actual != expected {
        bail!("durable {name} count is {actual}; expected {expected}");
    }
    Ok(())
}

/// Combines a multi-process lifecycle region into one conservative observation.
///
/// CPU is summed, peak memory is maximized, output is concatenated, and any
/// timeout or nonzero exit is retained.
fn aggregate(outcomes: Vec<ProcessOutcome>, wall_time: Duration) -> ProcessOutcome {
    let mut cpu = Some(0u64);
    let mut rss = Some(0u64);
    let mut stdout = CapturedOutput {
        bytes_seen: 0,
        truncated: false,
        data: Vec::new(),
        first_byte_time: None,
        last_byte_time: None,
    };
    let mut stderr = CapturedOutput {
        bytes_seen: 0,
        truncated: false,
        data: Vec::new(),
        first_byte_time: None,
        last_byte_time: None,
    };
    let mut exit_code = Some(0);
    let mut timed_out = false;
    for outcome in outcomes {
        cpu = cpu
            .zip(outcome.cpu_time_ns)
            .map(|(left, right)| left.saturating_add(right));
        rss = rss
            .zip(outcome.peak_rss_bytes)
            .map(|(left, right)| left.max(right));
        stdout.bytes_seen = stdout.bytes_seen.saturating_add(outcome.stdout.bytes_seen);
        stdout.truncated |= outcome.stdout.truncated;
        stdout.first_byte_time = stdout.first_byte_time.or(outcome.stdout.first_byte_time);
        stdout.last_byte_time = outcome.stdout.last_byte_time.or(stdout.last_byte_time);
        stdout.data.extend(outcome.stdout.data);
        stderr.bytes_seen = stderr.bytes_seen.saturating_add(outcome.stderr.bytes_seen);
        stderr.truncated |= outcome.stderr.truncated;
        stderr.first_byte_time = stderr.first_byte_time.or(outcome.stderr.first_byte_time);
        stderr.last_byte_time = outcome.stderr.last_byte_time.or(stderr.last_byte_time);
        stderr.data.extend(outcome.stderr.data);
        timed_out |= outcome.timed_out;
        if outcome.exit_code != Some(0) {
            exit_code = outcome.exit_code;
        }
    }
    ProcessOutcome {
        wall_time,
        cpu_time_ns: cpu,
        peak_rss_bytes: rss,
        resource_source: "aggregate-process-tree",
        exit_code,
        timed_out,
        stdout,
        stderr,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    fn outcome(
        exit_code: Option<i32>,
        cpu: Option<u64>,
        rss: Option<u64>,
        text: &str,
    ) -> ProcessOutcome {
        ProcessOutcome {
            wall_time: Duration::from_millis(1),
            cpu_time_ns: cpu,
            peak_rss_bytes: rss,
            resource_source: "test",
            exit_code,
            timed_out: false,
            stdout: CapturedOutput {
                bytes_seen: text.len() as u64,
                truncated: false,
                data: text.as_bytes().to_vec(),
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

    #[test]
    fn command_arguments_keep_service_endpoints_explicit() {
        let args = coordinator_args("postgres://127.0.0.1/db", "amqp://127.0.0.1/vhost");
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--database-url", "postgres://127.0.0.1/db"])
        );
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--messaging-url", "amqp://127.0.0.1/vhost"])
        );
        let outbox = coordinator_args_with_outbox(
            "postgres://127.0.0.1/db",
            "amqp://127.0.0.1/vhost",
            65,
            8,
        );
        assert!(
            outbox
                .windows(2)
                .any(|pair| pair == ["--outbox-batch-size", "65"])
        );
        assert!(
            outbox
                .windows(2)
                .any(|pair| pair == ["--outbox-publish-parallelism", "8"])
        );

        let worker = worker_args("postgres://127.0.0.1/db", "amqp://127.0.0.1/vhost");
        assert_eq!(worker.last().unwrap(), "once");
        assert_eq!(worker[2..5], ["-q", "-f", "json"]);
    }

    #[test]
    fn run_output_and_structural_count_contracts_fail_closed() {
        assert_eq!(parse_run_id(r#"{"data":{"run_id":"123"}}"#).unwrap(), "123");
        assert!(parse_run_id("not json").is_err());
        assert!(parse_run_id(r#"{"data":{}}"#).is_err());

        let mut counts = DurableCounts::new();
        counts.insert("runs".into(), 1);
        counts.insert("executions".into(), 0);
        counts.insert("attempts".into(), 0);
        counts.insert("evaluator_results".into(), 0);
        assert!(verify_prepared("coordinator.dispatch.v1", &counts).is_ok());
        counts.insert("executions".into(), 1);
        assert!(verify_prepared("coordinator.dispatch.v1", &counts).is_err());
        assert!(verify_prepared("startup.cli-help.v1", &DurableCounts::new()).is_ok());
        assert!(require_count(&counts, "runs", 1).is_ok());
        assert!(require_count(&counts, "missing", 1).is_err());
        assert!(structural_counts("unused", None).unwrap().is_empty());
    }

    #[test]
    fn process_aggregation_preserves_failures_and_resource_totals() {
        let mut second = outcome(Some(7), Some(3), Some(20), "two");
        second.timed_out = true;
        second.stderr.truncated = true;
        let aggregate = aggregate(
            vec![outcome(Some(0), Some(2), Some(10), "one"), second],
            Duration::from_millis(9),
        );
        assert_eq!(aggregate.wall_time, Duration::from_millis(9));
        assert_eq!(aggregate.cpu_time_ns, Some(5));
        assert_eq!(aggregate.peak_rss_bytes, Some(20));
        assert_eq!(aggregate.exit_code, Some(7));
        assert!(aggregate.timed_out);
        assert_eq!(aggregate.stdout.text(), "onetwo");
        assert!(aggregate.stderr.truncated);
        assert!(require_success(outcome(Some(0), None, None, ""), "test").is_ok());
        assert!(require_success(outcome(Some(1), None, None, ""), "test").is_err());
    }

    #[test]
    fn setup_paths_and_empty_worker_batches_are_service_free() {
        let directory = tempfile::tempdir().unwrap();
        let manifest = directory.path().join("build-manifest.json");
        let asset = directory.path().join("assets");
        fs::write(&manifest, "{}").unwrap();
        fs::create_dir(&asset).unwrap();
        assert_eq!(setup_asset(&manifest, "assets").unwrap(), asset);
        assert!(setup_asset(&manifest, "missing").is_err());
        assert_eq!(path_arg(&manifest).unwrap(), manifest.display().to_string());
        let inputs = RunInputs {
            profile: directory.path().join("profile.yml"),
            dataset: directory.path().join("dataset.yml"),
        };
        let args = create_args("postgres://localhost/db", &inputs).unwrap();
        assert!(args.iter().any(|arg| arg == "--profile-file"));
        assert!(
            invoke_workers(
                0,
                Path::new("unused"),
                "unused",
                "unused",
                &[],
                Duration::from_secs(1),
                1,
                1,
            )
            .unwrap()
            .is_empty()
        );
    }

    #[test]
    fn reference_creation_never_submits_the_dataset_file() {
        let inputs = RunInputs {
            profile: "profile.json".into(),
            dataset: "missing.jsonl".into(),
        };
        let args =
            ingestion_args("postgres://localhost/db", &inputs, Some("frozen-upload")).unwrap();
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--dataset-upload-id", "frozen-upload"])
        );
        assert!(
            !args
                .iter()
                .any(|arg| arg == "--dataset-file" || arg == "missing.jsonl")
        );
        assert!(
            ingestion_args("postgres://localhost/db", &inputs, None)
                .unwrap()
                .iter()
                .any(|arg| arg == "--dataset-file")
        );
    }

    #[test]
    fn ingestion_matrix_bounds_concurrency_and_keeps_dimensions_independent() {
        let shape = parse_ingestion_tuple(
            "cases-50000-payload-1024-reference-gates-8-placements-2-creators-4",
        )
        .unwrap();
        assert_eq!((shape.gates, shape.placements, shape.creators), (8, 2, 4));
        assert_eq!(
            required_routes(
                "run.create-ingestion.v1",
                "cases-101-payload-1024-fresh-gates-0-placements-2-creators-1"
            )
            .unwrap(),
            3
        );
        for tuple in [
            "cases-50000-payload-16384-reference-gates-0-placements-0-creators-4",
            "cases-100-payload-1024-fresh-gates-0-placements-0-creators-4",
            "cases-100-payload-1024-reference-gates-9-placements-0-creators-1",
            "cases-100-payload-1024-reference-gates-0-placements-5-creators-1",
            "cases-100-payload-1024-reference-gates-0-placements-0-creators-3",
            "cases-100-payload-1024-reference-wrong-0-placements-0-creators-1",
        ] {
            assert!(parse_ingestion_tuple(tuple).is_err(), "{tuple}");
        }
    }

    #[test]
    fn ingestion_tuples_bound_allocation_and_distinguish_dataset_reuse() {
        for mode in ["fresh", "reuse", "reference"] {
            let shape =
                parse_ingestion_tuple(&format!("cases-10000-payload-16384-{mode}")).unwrap();
            assert_eq!(shape.cases, 10_000);
            assert_eq!(shape.payload_bytes, 16_384);
            assert_eq!(shape.reuse, mode != "fresh");
            assert_eq!(shape.reference, mode == "reference");
        }
        assert!(parse_ingestion_tuple("cases-64000-payload-1-fresh").is_ok());
        for tuple in [
            "cases-0-payload-1-fresh",
            "cases-64001-payload-1-fresh",
            "cases-1-payload-0-fresh",
            "cases-1-payload-65537-fresh",
            "cases-64000-payload-65536-fresh",
            "cases-1-payload-1-other",
            "cases-1-payload-1-fresh-extra",
            "cases-bad-payload-1-reuse",
        ] {
            assert!(parse_ingestion_tuple(tuple).is_err(), "{tuple}");
        }
    }

    #[test]
    fn completed_workload_shapes_resolve_and_unknown_shapes_fail_closed() {
        let root = crate::perf::artifact::workspace_root().unwrap();
        let fixture = load(&root, "mvp-v1").unwrap();

        assert_eq!(
            workload_shape(&fixture, "run.create.v1", "cases-1001").unwrap(),
            WorkloadShape {
                cases: 1001,
                evaluators: 1,
                create_run: false,
            }
        );
        assert_eq!(
            workload_shape(&fixture, "coordinator.dispatch.v1", "chunks-512").unwrap(),
            WorkloadShape {
                cases: 51_200,
                evaluators: 1,
                create_run: true,
            }
        );
        assert_eq!(
            workload_shape(&fixture, "worker.execute-wasm.v1", "cases-8-evaluators-1").unwrap(),
            WorkloadShape {
                cases: 8,
                evaluators: 1,
                create_run: true,
            }
        );
        assert_eq!(
            workload_shape(&fixture, "worker.execute-wasm.v1", "cases-1-evaluators-8").unwrap(),
            WorkloadShape {
                cases: 1,
                evaluators: 8,
                create_run: true,
            }
        );
        assert_eq!(
            workload_shape(&fixture, "system.lifecycle.v1", "workers-2").unwrap(),
            WorkloadShape {
                cases: 100,
                evaluators: 1,
                create_run: false,
            }
        );
        assert_eq!(lifecycle_workers("workers-1").unwrap(), 1);
        assert_eq!(lifecycle_workers("workers-2").unwrap(), 2);
        assert_eq!(
            workload_shape(&fixture, "system.capacity.v1", "workers-2-load-16").unwrap(),
            WorkloadShape {
                cases: 1600,
                evaluators: 1,
                create_run: false,
            }
        );
        assert_eq!(lifecycle_workers("workers-1-load-4").unwrap(), 1);
        assert_eq!(lifecycle_workers("workers-2-load-8").unwrap(), 2);
        assert_eq!(
            workload_shape(&fixture, "run.create-scaling.v1", "cases-2001").unwrap(),
            WorkloadShape {
                cases: 2001,
                evaluators: 1,
                create_run: false,
            }
        );
        assert_eq!(
            workload_shape(&fixture, "coordinator.dispatch-scaling.v1", "chunks-513").unwrap(),
            WorkloadShape {
                cases: 51_300,
                evaluators: 1,
                create_run: true,
            }
        );
        assert_eq!(
            workload_shape(
                &fixture,
                "agent.http-scaling.v1",
                "cases-16-payload-1024-delay-0"
            )
            .unwrap(),
            WorkloadShape {
                cases: 16,
                evaluators: 1,
                create_run: true,
            }
        );
        assert_eq!(
            workload_shape(&fixture, "evaluator.wasm-scaling.v1", "evaluators-9").unwrap(),
            WorkloadShape {
                cases: 1,
                evaluators: 9,
                create_run: true,
            }
        );
        assert_eq!(
            parse_outbox_tuple("events-65-batch-65-parallel-8").unwrap(),
            OutboxShape {
                events: 65,
                batch: 65,
                parallel: 8,
            }
        );
        assert!(parse_outbox_tuple("events-65-batch-63-parallel-8").is_err());
        assert!(parse_single_dimension("cases-0", "cases").is_err());
        assert!(parse_http_tuple("cases-8-payload-0-delay-0").is_err());
        assert!(lifecycle_workers("workers-3-load-4").is_err());
        assert!(lifecycle_workers("workers-1-load-3").is_err());
        assert!(workload_shape(&fixture, "worker.execute-wasm.v1", "future-tuple").is_err());
        assert!(workload_shape(&fixture, "future.workload.v1", "tuple").is_err());
        assert!(lifecycle_workers("workers-3").is_err());
    }

    #[test]
    fn admin_tuple_shapes_accept_registered_boundaries_and_reject_drift() {
        assert_eq!(
            parse_cancel_tuple("aliases-17-executions-8192").unwrap(),
            AdminShape {
                routes: 17,
                executions: 8192,
            }
        );
        assert_eq!(
            parse_read_tuple("results-executions-251-routes-2").unwrap(),
            ReadShape {
                operation: ReadOperation::Results,
                format: None,
                routes: 2,
                executions: 251,
            }
        );
        assert_eq!(
            parse_read_tuple("jsonl-executions-251-routes-2").unwrap(),
            ReadShape {
                operation: ReadOperation::Export,
                format: Some(ExportFormat::Jsonl),
                routes: 2,
                executions: 251,
            }
        );
        assert_eq!(parse_placement_tuple("aliases-32").unwrap(), 32);
        assert_eq!(
            parse_move_tuple("rows-4-payload-1049088").unwrap(),
            MoveShape {
                rows: 4,
                payload_bytes: 1_049_088,
            }
        );
        assert_eq!(parse_rebalance_tuple("shards-8").unwrap(), 8);
        assert_eq!(
            required_routes("run.read.v1", "status-executions-250-routes-1").unwrap(),
            1
        );
        assert_eq!(
            required_routes("shard.move.v1", "rows-1000-payload-1024").unwrap(),
            2
        );
        assert!(parse_cancel_tuple("aliases-0-executions-1024").is_err());
        assert!(parse_cancel_tuple("aliases-18-executions-1024").is_err());
        assert!(parse_read_tuple("xml-executions-251-routes-2").is_err());
        assert!(parse_read_tuple("jsonl-executions-0-routes-2").is_err());
        assert!(parse_placement_tuple("aliases-33").is_err());
        assert!(parse_move_tuple("rows-999-payload-1024").is_err());
        assert!(parse_move_tuple("rows-4-payload-4194304").is_err());
        assert!(parse_rebalance_tuple("shards-9").is_err());
    }

    #[test]
    fn admin_output_oracles_reject_incomplete_or_wrong_results() {
        let status_shape = ReadShape {
            operation: ReadOperation::Status,
            format: None,
            routes: 1,
            executions: 251,
        };
        let status = serde_json::json!({
            "data": {"run_id": "run", "status": "completed"}
        });
        assert!(validate_read_output(status_shape, &status.to_string(), "run").is_ok());
        assert!(validate_read_output(status_shape, &status.to_string(), "other").is_err());

        let results_shape = ReadShape {
            operation: ReadOperation::Results,
            format: None,
            routes: 1,
            executions: 251,
        };
        let results = serde_json::json!({
            "data": {
                "run": {"run_id": "run"},
                "results": {"execution_count": 251, "evaluator_result_count": 251}
            }
        });
        assert!(validate_read_output(results_shape, &results.to_string(), "run").is_ok());
        let mut wrong_results_shape = results_shape;
        wrong_results_shape.executions = 250;
        assert!(validate_read_output(wrong_results_shape, &results.to_string(), "run").is_err());

        let json_shape = ReadShape {
            operation: ReadOperation::Export,
            format: Some(ExportFormat::Json),
            routes: 1,
            executions: 251,
        };
        let executions = (0..251)
            .map(|index| {
                serde_json::json!({
                    "execution": {"id": format!("execution-{index}"), "run_id": "run"}
                })
            })
            .collect::<Vec<_>>();
        let json = serde_json::json!({
            "data": {
                "run": {"run_id": "run"},
                "results": {"execution_count": 251},
                "executions": executions,
            }
        });
        assert!(validate_read_output(json_shape, &json.to_string(), "run").is_ok());
        let mut duplicate = json.clone();
        duplicate["data"]["executions"][1] = duplicate["data"]["executions"][0].clone();
        assert!(validate_read_output(json_shape, &duplicate.to_string(), "run").is_err());

        let jsonl_shape = ReadShape {
            operation: ReadOperation::Export,
            format: Some(ExportFormat::Jsonl),
            routes: 1,
            executions: 251,
        };
        let jsonl = [
            serde_json::json!({"type": "run", "run": {"id": "run"}}),
            serde_json::json!({
                "type": "results_summary",
                "run_id": "run",
                "results": {"execution_count": 251}
            }),
        ]
        .into_iter()
        .chain((0..251).map(|index| {
            serde_json::json!({
                "type": "execution",
                "run_id": "run",
                "execution": {"id": format!("execution-{index}"), "run_id": "run"}
            })
        }))
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join("\n");
        assert!(validate_read_output(jsonl_shape, &jsonl, "run").is_ok());
        let mut wrong_jsonl_shape = jsonl_shape;
        wrong_jsonl_shape.executions = 250;
        assert!(validate_read_output(wrong_jsonl_shape, &jsonl, "run").is_err());
    }

    #[test]
    fn admin_state_change_oracles_accept_success_and_reject_partial_state() {
        let cancelled = serde_json::json!({
            "data": {"status": "cancelled"},
            "meta": {
                "terminal": true,
                "executions_cancelled": 1024,
                "already_cancelled": false
            }
        });
        assert!(validate_cancel_output(&cancelled.to_string(), 1024, false).is_ok());
        assert!(validate_cancel_output(&cancelled.to_string(), 1023, false).is_err());

        let replay = serde_json::json!({
            "data": {"status": "cancelled"},
            "meta": {
                "terminal": true,
                "executions_cancelled": 0,
                "already_cancelled": true
            }
        });
        assert!(validate_cancel_output(&replay.to_string(), 0, true).is_ok());
        assert!(validate_cancel_output(&replay.to_string(), 0, false).is_err());

        let moved = serde_json::json!({
            "data": {
                "target_database_alias": "perf_01",
                "placement": {"database_alias": "perf_01"},
                "tables": [{"name": "executions", "verified": true}]
            },
            "meta": {"moved": true, "verified": true}
        });
        assert!(validate_move_output(&moved.to_string(), "perf_01", true).is_ok());
        assert!(validate_move_output(&moved.to_string(), "perf_01", false).is_err());
        assert!(validate_move_output(&moved.to_string(), "perf_02", true).is_err());
        let mut replayed_move = moved.clone();
        replayed_move["meta"]["moved"] = false.into();
        assert!(validate_move_output(&replayed_move.to_string(), "perf_01", false).is_ok());
        let mut unverified_move = moved.clone();
        unverified_move["data"]["tables"][0]["verified"] = false.into();
        assert!(validate_move_output(&unverified_move.to_string(), "perf_01", true).is_err());

        let plan = serde_json::json!({"data": {"operation": {"id": "operation-1"}}});
        assert_eq!(
            parse_rebalance_operation_id(&plan.to_string()).unwrap(),
            "operation-1"
        );
        assert!(parse_rebalance_operation_id(r#"{"data":{}}"#).is_err());

        let verified = serde_json::json!({
            "data": {"operation": {"status": "completed"}},
            "meta": {"verified": true, "verified_item_count": 8}
        });
        assert!(validate_rebalance_output(&verified.to_string(), 8).is_ok());
        assert!(validate_rebalance_output(&verified.to_string(), 7).is_err());
    }

    #[test]
    fn read_resource_oracle_enforces_stream_timing_and_export_memory() {
        let shape = ReadShape {
            operation: ReadOperation::Export,
            format: Some(ExportFormat::Jsonl),
            routes: 1,
            executions: 250,
        };
        let mut process = outcome(Some(0), Some(1), Some(128 * 1024 * 1024), "output");
        process.wall_time = Duration::from_millis(10);
        process.stdout.first_byte_time = Some(Duration::from_millis(2));
        process.stdout.last_byte_time = Some(Duration::from_millis(8));
        assert!(validate_read_resources(shape, &process).is_ok());

        process.peak_rss_bytes = Some(257 * 1024 * 1024);
        assert!(validate_read_resources(shape, &process).is_err());
        process.peak_rss_bytes = Some(128 * 1024 * 1024);
        process.stdout.first_byte_time = Some(Duration::from_millis(9));
        process.stdout.last_byte_time = Some(Duration::from_millis(8));
        assert!(validate_read_resources(shape, &process).is_err());
        process.stdout.first_byte_time = None;
        assert!(validate_read_resources(shape, &process).is_err());
    }

    #[test]
    fn exact_oracles_accept_completed_workload_outcomes() {
        let create = DurableCounts::from([
            ("runs".into(), 1),
            ("chunks".into(), 11),
            ("executions".into(), 0),
        ]);
        assert!(validate_create_oracle(&create, "pending", 11).is_ok());

        assert!(validate_coordinator_oracle("running", 512, 1, 512, 512).is_ok());

        let worker = DurableCounts::from([
            ("executions".into(), 8),
            ("attempts".into(), 8),
            ("evaluator_results".into(), 8),
        ]);
        assert!(validate_worker_oracle(&worker, 1, 8, 1).is_ok());

        let evaluator_heavy = DurableCounts::from([
            ("executions".into(), 1),
            ("attempts".into(), 1),
            ("evaluator_results".into(), 8),
        ]);
        assert!(validate_worker_oracle(&evaluator_heavy, 1, 1, 8).is_ok());
        assert!(validate_lifecycle_oracle("completed", [100, 100, 100], 100).is_ok());
    }

    #[test]
    fn exact_oracles_reject_status_count_and_event_mismatches() {
        let create = DurableCounts::from([
            ("runs".into(), 1),
            ("chunks".into(), 11),
            ("executions".into(), 0),
        ]);
        assert!(validate_create_oracle(&create, "running", 11).is_err());
        assert!(validate_create_oracle(&create, "pending", 10).is_err());

        assert!(validate_coordinator_oracle("pending", 512, 1, 512, 512).is_err());
        assert!(validate_coordinator_oracle("running", 511, 1, 512, 512).is_err());
        assert!(validate_coordinator_oracle("running", 512, 0, 512, 512).is_err());
        assert!(validate_coordinator_oracle("running", 512, 1, 511, 512).is_err());

        let worker = DurableCounts::from([
            ("executions".into(), 8),
            ("attempts".into(), 8),
            ("evaluator_results".into(), 8),
        ]);
        assert!(validate_worker_oracle(&worker, 0, 8, 1).is_err());
        assert!(validate_worker_oracle(&worker, 1, 8, 2).is_err());
        assert!(validate_lifecycle_oracle("running", [100, 100, 100], 100).is_err());
        assert!(validate_lifecycle_oracle("completed", [100, 99, 100], 100).is_err());
    }

    #[test]
    fn exact_amplification_gate_accepts_match_and_rejects_drift() {
        let external = ExternalMeasurements {
            http_requests: Some(8),
            queue_ready: Some(0),
            queue_unacked: Some(0),
            durable_counts: DurableCounts::from([
                ("attempts".into(), 8),
                ("evaluator_results".into(), 8),
            ]),
            ..Default::default()
        };
        let expected = BTreeMap::from([
            ("http_requests".into(), 8),
            ("queue_ready".into(), 0),
            ("durable.attempts".into(), 8),
            ("durable.evaluator_results".into(), 8),
        ]);
        assert!(verify_exact_observations(&external, &expected).is_ok());

        let mut drift = expected.clone();
        drift.insert("http_requests".into(), 9);
        assert!(verify_exact_observations(&external, &drift).is_err());
        let unsupported = BTreeMap::from([("internal_counter".into(), 1)]);
        assert!(verify_exact_observations(&external, &unsupported).is_err());
    }

    #[test]
    fn product_failures_remain_typed_for_campaign_classification() {
        let error = product::<()>(Err(anyhow::anyhow!("exact oracle differed"))).unwrap_err();
        let failure = error.downcast_ref::<ProductFailure>().unwrap();
        assert!(failure.message().contains("exact oracle differed"));

        let generic = anyhow::anyhow!("collector unavailable");
        assert!(generic.downcast_ref::<ProductFailure>().is_none());

        let combined = combine_sample_cleanup::<()>(
            Err(product_failure("exact oracle differed")),
            Err(anyhow::anyhow!("scope cleanup failed")),
            Err(anyhow::anyhow!("route cleanup failed")),
        )
        .unwrap_err();
        assert!(combined.downcast_ref::<ProductFailure>().is_some());
        assert!(
            combined
                .to_string()
                .contains("sample cleanup also returned")
        );
    }
}
