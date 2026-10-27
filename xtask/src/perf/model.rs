//! Versioned configuration, provenance, sample, comparison, and report models.
//!
//! These types are the performance harness's machine contracts. Schema IDs
//! version document shapes, while stable workload/profile IDs version benchmark
//! meaning. Flattened extension maps preserve unknown additive fields when a
//! document is read and written by the current schema reader.

use std::collections::BTreeMap;

use serde::{
    Deserialize,
    Serialize,
};
use serde_json::Value;

/// Supported environment-manifest schema identifier.
pub const ENVIRONMENT_SCHEMA: &str = "environment/v1";
/// Supported immutable build-manifest schema identifier.
pub const BUILD_SCHEMA: &str = "build-manifest/v1";
/// Supported workload-registry schema identifier.
pub const REGISTRY_SCHEMA: &str = "workload-registry/v1";
/// Supported execution-profile schema identifier.
pub const PROFILE_SCHEMA: &str = "profile/v1";
/// Supported raw-sample and block-record schema identifier.
pub const SAMPLE_SCHEMA: &str = "sample/v1";
/// Supported statistical-comparison schema identifier.
pub const COMPARISON_SCHEMA: &str = "comparison/v1";
/// Supported campaign-manifest and report schema identifier.
pub const REPORT_SCHEMA: &str = "report/v1";
/// Supported canonical-noise calibration artifact schema identifier.
pub const CALIBRATION_SCHEMA: &str = "calibration/v1";
/// Supported bounded-capacity calibration artifact schema identifier.
pub const CAPACITY_SCHEMA: &str = "capacity-calibration/v1";
/// Supported reviewed performance-budget schema identifier.
pub const BUDGET_SCHEMA: &str = "performance-budget/v1";
/// Supported immutable calibrated-baseline manifest schema identifier.
pub const BASELINE_SCHEMA: &str = "performance-baseline/v1";

/// Typed catalog of workload contracts and audited production constants.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadRegistry {
    /// Document shape identifier, currently [`REGISTRY_SCHEMA`].
    pub schema_id: String,
    /// Positive content revision within the schema generation.
    pub revision: u32,
    /// Production limits and batch sizes consumed by fixtures and projections.
    pub constants: RegistryConstants,
    /// Stable semantic workload contracts available to profiles.
    pub workloads: Vec<Workload>,
    /// Unknown additive fields retained for forward compatibility.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Audited production constants that affect fixture shape or capacity models.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryConstants {
    /// Maximum pooled connections allocated per database target.
    pub database_connections_per_target: u32,
    /// Maximum time to acquire a pooled database connection.
    pub database_acquire_timeout_ms: u64,
    /// Outer deadline for one database operation.
    pub database_operation_deadline_ms: u64,
    /// Cases assigned to one run chunk.
    pub run_chunk_size: u32,
    /// Cases read in one run-creation page.
    pub creation_case_page_size: u32,
    /// Maximum creation pages processed per pass.
    pub creation_page_budget: u32,
    /// Case blobs inserted per statement group.
    pub case_blob_group_size: u32,
    /// Dataset memberships inserted per statement group.
    pub membership_group_size: u32,
    /// Run chunks inserted per statement group.
    pub chunk_insert_group_size: u32,
    /// Coordinator service interval in milliseconds.
    pub coordinator_tick_ms: u64,
    /// Creation-recovery work budget per coordinator pass.
    pub coordinator_create_recovery_budget: u32,
    /// Dispatch work budget per coordinator pass.
    pub coordinator_dispatch_budget: u32,
    /// Finalization work budget per coordinator pass.
    pub coordinator_finalization_budget: u32,
    /// Chunks considered in one dispatch window.
    pub dispatch_window_size: u32,
    /// Expired leases reclaimed per recovery batch.
    pub lease_recovery_batch_size: u32,
    /// Outbox deliveries claimed per publication batch.
    pub outbox_batch_size: u32,
    /// Maximum concurrent outbox publications.
    pub outbox_publish_parallelism: u32,
    /// Default in-flight chunk limit for one worker.
    pub worker_default_inflight_chunks: u32,
    /// Worker lease-heartbeat interval in milliseconds.
    pub worker_heartbeat_ms: u64,
    /// Maximum concurrent case executions.
    pub case_concurrency: u32,
    /// Maximum concurrent evaluator executions.
    pub evaluator_concurrency: u32,
    /// Maximum concurrent Wasm component executions.
    pub wasm_concurrency: u32,
    /// Maximum linear memory available to one Wasm evaluation.
    pub wasm_max_memory_mib: u32,
    /// Evaluator results inserted per statement group.
    pub result_insert_group_size: u32,
}

/// Whether the harness has an executable driver for a workload contract.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ImplementationStatus {
    /// The runner can provision, execute, and validate the workload.
    Implemented,
    /// The contract is reserved but its driver or fixtures are not available.
    Planned,
}

/// Unmeasured warmup policy applied before a workload campaign.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Preconditioning {
    /// Start measurement without an additional harness warmup execution.
    None,
    /// Discard one execution for each measured binary before sampling.
    OnePerBinary,
}

/// Shape used to interpret repeated component measurements across cardinalities.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScalingKind {
    /// One fixed intercept plus a per-unit slope over a continuous measured range.
    FixedPlusSlope,
    /// Separate segments split at declared production batching boundaries.
    Stepped,
}

/// One registered cardinality and its exact externally observable effects.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingPoint {
    /// Registered workload tuple represented by this point.
    pub tuple: String,
    /// Independent cardinality used by the scaling model.
    pub input: u64,
    /// Exact observations required after a successful measured command.
    pub exact: BTreeMap<String, u64>,
}

/// Declarative scaling contract for one component workload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingModel {
    /// Model family used for interpolation and discontinuity checks.
    pub kind: ScalingKind,
    /// Human-readable independent variable, such as `cases` or `chunks`.
    pub input_dimension: String,
    /// Maximum accepted relative residual for a fitted point.
    pub max_residual_fraction: f64,
    /// Inputs that begin a new segment for a stepped model.
    #[serde(default)]
    pub discontinuities: Vec<u64>,
    /// Explicit measured points; profiles choose subsets rather than products.
    pub points: Vec<ScalingPoint>,
}

/// One stable semantic workload and its execution requirements.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workload {
    /// Versioned workload identity recorded in every dependent artifact.
    pub id: String,
    /// Repository component responsible for the protected boundary.
    pub owner: String,
    /// Availability of the workload's executable driver.
    pub status: ImplementationStatus,
    /// Capability required from every compared build manifest.
    pub capability: String,
    /// Versioned fixture catalog consumed by the workload driver.
    pub fixture: String,
    /// Explicit fixture shapes that profiles may select.
    pub tuples: Vec<String>,
    /// Denominator used to interpret throughput and resource measurements.
    pub unit: String,
    /// Exact correctness contract applied to an execution.
    pub oracle: String,
    /// External measurements required for a valid workload result.
    pub required_metrics: Vec<String>,
    /// Absolute total SQL statement ceilings by tuple, frozen with the registry.
    /// Evaluated independently of informative wall-time comparisons.
    #[serde(default)]
    pub statement_budgets: BTreeMap<String, u64>,
    /// Outer deadline for one workload execution.
    pub watchdog_ms: u64,
    /// Conservative per-execution duration used for campaign planning.
    pub planning_duration_ms: u64,
    /// Unmeasured warmup policy for this workload.
    pub preconditioning: Preconditioning,
    /// Optional cardinality model and exact amplification contract.
    #[serde(default)]
    pub scaling_model: Option<ScalingModel>,
    /// Optional long-running stability and recovery acceptance contract.
    #[serde(default)]
    pub reliability: Option<ReliabilityContract>,
    /// Arguments passed directly to the measured Vigilo binary.
    #[serde(default)]
    pub command: Vec<String>,
    /// Required substrings in the startup workload's help output.
    #[serde(default)]
    pub help_signatures: Vec<String>,
    /// Unknown additive fields retained for forward compatibility.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Bounded acceptance policy for a soak or controlled-recovery workload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReliabilityContract {
    /// Minimum measured duration for a steady-state soak.
    pub duration_secs: u64,
    /// Interval between durable progress and process-resource observations.
    pub observation_interval_secs: u64,
    /// Maximum time allowed to return to useful work after an injected fault.
    pub recovery_deadline_secs: u64,
    /// Absolute process-tree memory ceiling used as a safety bound.
    pub max_process_rss_bytes: u64,
    /// Maximum increase in open file descriptors across the measured window.
    pub max_file_descriptor_growth: u64,
    /// Minimum end-window/start-window useful-throughput ratio.
    pub min_throughput_retention: f64,
    /// Maximum durable attempts divided by completed useful cases.
    pub max_attempts_per_case: f64,
    /// Maximum broker deliveries divided by useful chunks.
    pub max_deliveries_per_chunk: f64,
}

/// Frozen campaign composition and resource limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Profile {
    /// Document shape identifier, currently [`PROFILE_SCHEMA`].
    pub schema_id: String,
    /// Versioned profile identity recorded in campaign artifacts.
    pub id: String,
    /// Human-readable purpose of the profile.
    pub description: String,
    /// Whether callers must explicitly select at least one workload.
    pub requires_workload_selection: bool,
    /// Hard wall-clock limit for the complete campaign.
    pub campaign_cap_secs: u64,
    /// Stable seed controlling counterbalanced schedule orientation.
    pub schedule_seed: u64,
    /// Maximum total bytes retained in the run directory.
    pub max_artifact_bytes: u64,
    /// Maximum stdout bytes retained from each process.
    pub max_stdout_bytes: usize,
    /// Maximum stderr bytes retained from each process.
    pub max_stderr_bytes: usize,
    /// Optional invalidation threshold for unexplained orientation bias.
    #[serde(default)]
    pub max_residual_orientation_effect: Option<f64>,
    /// Reviewed budget policy used by gating profiles.
    #[serde(default)]
    pub budget_reference: Option<String>,
    /// Ordered workload tuples and sampling policies in the campaign.
    pub workloads: Vec<ProfileWorkload>,
    /// Unknown additive fields retained for forward compatibility.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// One exact workload tuple selected by a profile.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileWorkload {
    /// Stable workload ID resolved through the registry.
    pub id: String,
    /// Registered fixture shape selected for the workload.
    pub tuple: String,
    /// Positive even count of four-execution measurement blocks.
    pub blocks: u32,
    /// Whether results are informative or environment calibration data.
    pub timing: String,
}

/// Immutable identity and compatibility manifest for a release snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildManifest {
    /// Document shape identifier, currently [`BUILD_SCHEMA`].
    pub schema_id: String,
    /// RFC 3339 timestamp at which the snapshot was created.
    pub created_at: String,
    /// Platform-specific executable file name.
    pub executable_name: String,
    /// BLAKE3 digest of the measured executable.
    pub executable_digest: String,
    /// Executable size in bytes.
    pub executable_bytes: u64,
    /// Source Git commit when the worktree has a resolvable `HEAD`.
    pub source_commit: Option<String>,
    /// Whether tracked source differed from the recorded commit.
    pub source_dirty: bool,
    /// Human-readable identity of the source worktree.
    pub source_label: String,
    /// BLAKE3 digest of the source `Cargo.lock`.
    pub cargo_lock_digest: String,
    /// Digest of the resolved Cargo dependency tree.
    pub dependency_tree_digest: String,
    /// Digest of the migration tree copied into setup assets.
    pub migrations_digest: String,
    /// Digest of the supported evaluator WIT contract tree.
    pub evaluator_abi_digest: String,
    /// Full `rustc --version --verbose` provenance.
    pub rustc: String,
    /// Cargo version used to build the snapshot.
    pub cargo: String,
    /// Compilation target triple.
    pub target: String,
    /// Cargo profile used for the measured executable.
    pub profile: String,
    /// Semantic workload contracts this build can execute.
    pub capabilities: Vec<String>,
    /// Immutable setup inputs copied beside the build manifest.
    pub setup_assets: Vec<SetupAsset>,
    /// Unknown additive fields retained for forward compatibility.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// One content-addressed setup input retained with a build snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetupAsset {
    /// Logical asset name used during compatibility negotiation.
    pub name: String,
    /// Path relative to the snapshot directory.
    pub relative_path: String,
    /// Deterministic BLAKE3 digest of the file or tree.
    pub digest: String,
}

/// Host and collector identity recorded for a campaign.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentManifest {
    /// Document shape identifier, currently [`ENVIRONMENT_SCHEMA`].
    pub schema_id: String,
    /// RFC 3339 time at which the environment was observed.
    pub created_at: String,
    /// Stable environment contract ID or local-development identity.
    pub environment_id: String,
    /// Whether the host satisfies the canonical comparison contract.
    pub canonical: bool,
    /// Operating-system identity.
    pub os: String,
    /// Host CPU architecture.
    pub architecture: String,
    /// Logical processors visible to the harness.
    pub logical_cpus: usize,
    /// Host name when it can be observed safely.
    pub hostname: Option<String>,
    /// Resource-collection backend used by the process runner.
    pub collector: String,
    /// Comparability limitations or successful validity observations.
    pub validity: Vec<String>,
    /// Unknown additive fields retained for forward compatibility.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Role assigned to a binary within a measurement schedule.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum BinaryRole {
    /// Before-change executable in a comparison campaign.
    Baseline,
    /// After-change executable in a comparison campaign.
    Candidate,
    /// Sole executable in an informative run campaign.
    Single,
}

/// Ordered binary assignment for one four-execution block.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Orientation {
    /// Baseline, candidate, candidate, baseline order.
    #[serde(rename = "ABBA")]
    Abba,
    /// Candidate, baseline, baseline, candidate order.
    #[serde(rename = "BAAB")]
    Baab,
    /// Four repetitions of a sole binary.
    #[serde(rename = "single")]
    Single,
}

/// Correctness and external-validity classification for one sample.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SampleState {
    /// Execution and required observations satisfied the workload contract.
    Valid,
    /// The measured product violated correctness, liveness, or resource limits.
    ProductFailure,
    /// The harness or environment could not produce a comparable observation.
    Invalid,
}

/// Machine-readable sample classification with diagnostic context.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Validation {
    /// High-level validity classification.
    pub state: SampleState,
    /// Stable reason code for automation and aggregation.
    pub code: String,
    /// Human-readable diagnostic message.
    pub message: String,
}

/// Process-level measurements captured for one execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessMeasurement {
    /// Elapsed wall-clock time in nanoseconds.
    pub wall_time_ns: u64,
    /// Process-tree CPU time in nanoseconds when supported.
    pub cpu_time_ns: Option<u64>,
    /// Peak process-tree resident memory in bytes when supported.
    pub peak_rss_bytes: Option<u64>,
    /// Nanoseconds from launch to the first stdout byte, when output was emitted.
    #[serde(default)]
    pub stdout_first_byte_ns: Option<u64>,
    /// Nanoseconds from launch to the last stdout byte, when output was emitted.
    #[serde(default)]
    pub stdout_last_byte_ns: Option<u64>,
    /// Stable resource-collector identity.
    pub resource_source: String,
    /// Process exit code when observable.
    pub exit_code: Option<i32>,
    /// Whether the harness watchdog terminated the process tree.
    pub timed_out: bool,
    /// Total stdout bytes observed before any retention truncation.
    pub stdout_bytes: u64,
    /// Total stderr bytes observed before any retention truncation.
    pub stderr_bytes: u64,
    /// Whether retained stdout omitted overflow bytes.
    pub stdout_truncated: bool,
    /// Whether retained stderr omitted overflow bytes.
    pub stderr_truncated: bool,
}

/// Scoped service and durable-state measurements for one workload execution.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExternalMeasurements {
    /// PostgreSQL statements executed in the sample scope.
    pub sql_calls: Option<u64>,
    /// Complete statement categories per distinct database; empty when not collected.
    /// Shared control/execution databases cannot be attributed to a single plane.
    #[serde(default)]
    pub sql_databases: Vec<DatabaseStatements>,
    /// PostgreSQL statement execution time in milliseconds.
    pub sql_time_ms: Option<f64>,
    /// Rows observed by scoped PostgreSQL statements.
    pub sql_rows: Option<u64>,
    /// Physical table-row mutation deltas, aggregated by public table across scoped databases.
    #[serde(default)]
    pub table_writes: BTreeMap<String, TableWrites>,
    /// Ceph gateway object-operation and payload-byte deltas, excluding oracle reads.
    /// Empty in artifacts recorded without this collector.
    #[serde(default)]
    pub storage_counters: BTreeMap<String, u64>,
    /// WAL bytes generated during the sample.
    pub wal_bytes: Option<u64>,
    /// Change in durable PostgreSQL database bytes.
    pub database_bytes_delta: Option<i64>,
    /// HTTP requests received by the deterministic agent.
    pub http_requests: Option<u64>,
    /// HTTP request and response bytes observed by the agent.
    pub http_bytes: Option<u64>,
    /// Maximum simultaneous HTTP requests observed by the agent.
    pub http_peak_concurrency: Option<u64>,
    /// Ready RabbitMQ deliveries after workload settlement.
    pub queue_ready: Option<u64>,
    /// Unacknowledged RabbitMQ deliveries after workload settlement.
    pub queue_unacked: Option<u64>,
    /// Peak memory used by the scoped service containers.
    pub service_memory_bytes: Option<u64>,
    /// Maximum sampled aggregate container CPU percentage during execution.
    pub service_cpu_percent: Option<f64>,
    /// Exact durable business-state counts produced by the workload oracle.
    #[serde(default)]
    pub durable_counts: BTreeMap<String, i64>,
    /// Highest-cost PostgreSQL statement fingerprints captured after timing ends.
    #[serde(default)]
    pub query_diagnostics: Vec<QueryDiagnostic>,
}

/// Bounded aggregate counters, independent of top-query diagnostic retention.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DatabaseStatements {
    /// Campaign-owned database name, never a connection URL or credential.
    pub database: String,
    /// `control`, `execution`, or `shared`, determined before measurement.
    pub role: String,
    /// Statement counts: read, dml, mixed (WITH), transaction, and other.
    /// SELECT functions can have side effects; these classify syntax, not row writes.
    pub calls: BTreeMap<String, u64>,
    /// Server statement execution milliseconds across all categories.
    pub execution_ms: f64,
    /// Server statement planning milliseconds across all categories.
    pub planning_ms: f64,
}

/// PostgreSQL tuple mutation counters, independent of statement batching and read rows.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableWrites {
    /// Tuples inserted during the sample, including subsequently deleted tuples.
    pub inserted: u64,
    /// Tuple updates during the sample.
    pub updated: u64,
    /// Tuples deleted during the sample.
    pub deleted: u64,
}

/// Non-gating PostgreSQL statement, buffer, and WAL diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryDiagnostic {
    /// BLAKE3 fingerprint of PostgreSQL's normalized statement text.
    pub query_digest: String,
    /// Normalized statement retained for local performance diagnosis.
    pub query: String,
    /// Number of executions observed during the scoped sample.
    pub calls: u64,
    /// Number of query plans created during the scoped sample.
    #[serde(default)]
    pub plans: u64,
    /// Rows reported by those executions.
    pub rows: u64,
    /// Total server planning time in milliseconds.
    #[serde(default)]
    pub total_plan_time_ms: f64,
    /// Total server execution time in milliseconds.
    pub total_exec_time_ms: f64,
    /// Shared-buffer hits attributed to the statement.
    pub shared_blocks_hit: u64,
    /// Shared-buffer reads attributed to the statement.
    pub shared_blocks_read: u64,
    /// Temporary blocks read and written by the statement.
    pub temporary_blocks: u64,
    /// WAL records generated by the statement.
    pub wal_records: u64,
    /// Full-page WAL images generated by the statement.
    pub wal_full_page_images: u64,
    /// WAL bytes generated by the statement.
    pub wal_bytes: u64,
}

/// Raw observation for one scheduled workload execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sample {
    /// Document shape identifier, currently [`SAMPLE_SCHEMA`].
    pub schema_id: String,
    /// Campaign identity shared by all related artifacts.
    pub run_id: String,
    /// Frozen profile that selected the workload.
    pub profile_id: String,
    /// Stable workload contract ID.
    pub workload_id: String,
    /// Exact registered fixture shape.
    pub tuple_id: String,
    /// Zero-based measurement block index.
    pub block_id: u32,
    /// Pair of opposite-orientation blocks used for balance checks.
    pub orientation_set_id: u32,
    /// Binary ordering assigned to the block.
    pub orientation: Orientation,
    /// Adjacent-pair index within the block.
    pub pair_id: u8,
    /// One-based execution position within the block.
    pub position: u8,
    /// Binary role executed for this sample.
    pub role: BinaryRole,
    /// Whether statistics may consume this sample.
    pub measured: bool,
    /// RFC 3339 timestamp immediately before process execution.
    pub started_at: String,
    /// Externally observed process measurements.
    pub process: ProcessMeasurement,
    /// Correctness and validity result.
    pub validation: Validation,
    /// Scoped database, broker, HTTP, service, and durable-state measurements.
    #[serde(default)]
    pub external: ExternalMeasurements,
    /// Unknown additive fields retained for forward compatibility.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Completeness and validity summary for one measurement block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockRecord {
    /// Document shape identifier, currently [`SAMPLE_SCHEMA`].
    pub schema_id: String,
    /// Campaign identity shared by all related artifacts.
    pub run_id: String,
    /// Stable workload contract ID.
    pub workload_id: String,
    /// Exact registered fixture shape.
    pub tuple_id: String,
    /// Zero-based block index.
    pub block_id: u32,
    /// Pair of opposite-orientation blocks used for balance checks.
    pub orientation_set_id: u32,
    /// Binary ordering assigned to the block.
    pub orientation: Orientation,
    /// Whether all four scheduled executions were recorded.
    pub complete: bool,
    /// Whether all samples satisfied correctness and validity contracts.
    pub valid: bool,
    /// Number of sample records attached to the block.
    pub sample_count: usize,
}

/// Gating or diagnostic disposition of a comparison.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// Candidate remained within the reviewed harmful-effect budget.
    Pass,
    /// Candidate exceeded the budget with required confirmation.
    Regression,
    /// Candidate produced a statistically supported beneficial effect.
    Improvement,
    /// Estimate is reportable but no numerical gate applies.
    Informative,
    /// Available complete blocks cannot support a required conclusion.
    Inconclusive,
    /// External validity or schedule-balance requirements were violated.
    Invalid,
}

/// Statistical comparison and diagnostics for one measured metric.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricComparison {
    /// Stable metric name.
    pub name: String,
    /// Unit shared by baseline, candidate, and interval values.
    pub unit: String,
    /// Direction interpreted as harmful by the gating policy.
    pub direction: String,
    /// Median baseline observation in the declared unit.
    pub baseline_median: f64,
    /// Median candidate observation in the declared unit.
    pub candidate_median: f64,
    /// Unadjusted relative candidate-minus-baseline change.
    pub raw_candidate_delta: f64,
    /// Counterbalanced relative effect expressed in the harmful direction.
    pub harmful_effect: f64,
    /// Lower bound of the bootstrap confidence interval.
    pub confidence_lower: f64,
    /// Upper bound of the bootstrap confidence interval.
    pub confidence_upper: f64,
    /// Calibrated positive-harm threshold, if the profile has one.
    pub practical_budget: Option<f64>,
    /// Disposition derived from the estimate, interval, and budget.
    pub verdict: Verdict,
    /// Complete valid `ABBA` blocks included in the estimate.
    pub valid_abba_blocks: usize,
    /// Complete valid `BAAB` blocks included in the estimate.
    pub valid_baab_blocks: usize,
    /// Valid blocks excluded because no opposite orientation was available.
    pub unmatched_blocks: usize,
    /// Remaining effect associated with block orientation.
    pub residual_orientation_effect: f64,
    /// Median effect grouped by `ABBA` and `BAAB` orientation.
    pub orientation_medians: BTreeMap<String, f64>,
    /// Median observation grouped by binary role and execution position.
    pub position_medians: BTreeMap<String, f64>,
    /// Stable estimator identity needed to interpret the result.
    pub estimator: String,
    /// Seed used for deterministic bootstrap resampling.
    pub bootstrap_seed: u64,
}

/// Versioned comparison artifact for one workload tuple.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonDocument {
    /// Document shape identifier, currently [`COMPARISON_SCHEMA`].
    pub schema_id: String,
    /// Campaign identity shared by all related artifacts.
    pub run_id: String,
    /// Frozen profile that selected the workload.
    pub profile_id: String,
    /// Stable workload contract ID.
    pub workload_id: String,
    /// Exact registered fixture shape.
    pub tuple_id: String,
    /// Digest of the immutable baseline executable.
    pub baseline_digest: String,
    /// Digest of the immutable candidate executable.
    pub candidate_digest: String,
    /// Metric-level estimates and diagnostics.
    pub metrics: Vec<MetricComparison>,
    /// Aggregate disposition for the workload tuple.
    pub verdict: Verdict,
    /// Unknown additive fields retained for forward compatibility.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Checkpointed plan, provenance links, and completion state for a campaign.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CampaignManifest {
    /// Document shape identifier, currently [`REPORT_SCHEMA`].
    pub schema_id: String,
    /// Unique campaign identity.
    pub run_id: String,
    /// Campaign kind, such as single-binary run or comparison.
    pub kind: String,
    /// Current lifecycle or terminal status.
    pub status: String,
    /// RFC 3339 campaign creation timestamp.
    pub created_at: String,
    /// RFC 3339 completion timestamp once terminal.
    pub completed_at: Option<String>,
    /// Frozen profile used to resolve the campaign.
    pub profile_id: String,
    /// Effective deterministic scheduling seed.
    pub schedule_seed: u64,
    /// Ordered workload and tuple identities in the resolved campaign.
    pub selected_workloads: Vec<String>,
    /// Number of scheduled executions eligible for measurement.
    pub planned_measured_executions: u64,
    /// Number of scheduled but discarded preconditioning executions.
    pub planned_preconditioning_executions: u64,
    /// Maximum bytes allowed beneath the campaign directory.
    pub artifact_limit_bytes: u64,
    /// Relative path to the captured environment manifest.
    pub environment_file: String,
    /// Relative path to the frozen workload registry.
    pub registry_file: String,
    /// Relative path to the frozen resolved profile.
    pub profile_file: String,
    /// Relative path to the frozen reviewed budget policy, when one applied.
    pub budget_policy_file: Option<String>,
    /// Relative baseline manifest path for comparison campaigns.
    pub baseline_manifest: Option<String>,
    /// Relative candidate or single-binary manifest path.
    pub candidate_manifest: String,
    /// Terminal failure explanation when the campaign did not pass.
    pub failure: Option<String>,
    /// Unknown additive fields retained for forward compatibility.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Machine source for terminal and Markdown campaign summaries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportDocument {
    /// Document shape identifier, currently [`REPORT_SCHEMA`].
    pub schema_id: String,
    /// Campaign identity shared by all related artifacts.
    pub run_id: String,
    /// Campaign kind, such as single-binary run or comparison.
    pub kind: String,
    /// Terminal campaign status.
    pub status: String,
    /// Frozen profile used to resolve the campaign.
    pub profile_id: String,
    /// RFC 3339 time at which the report view was generated.
    pub generated_at: String,
    /// Per-workload statistical comparison documents.
    pub comparisons: Vec<ComparisonDocument>,
    /// Correctness, validity, or regression explanations.
    pub failures: Vec<String>,
    /// Relative paths to retained campaign artifacts.
    pub artifact_files: Vec<String>,
    /// Unknown additive fields retained for forward compatibility.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Reviewed target used to judge whether a metric can support a budget.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalibrationTarget {
    /// Stable metric name emitted by the comparison estimator.
    pub metric: String,
    /// Maximum acceptable harmful relative effect.
    pub practical_budget: f64,
    /// Desired probability of detecting a budget-sized harmful effect.
    pub target_power: f64,
    /// Maximum permitted residual schedule-orientation effect.
    pub max_residual_orientation_effect: f64,
}

/// Noise and sample-count evidence for one workload metric.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalibrationMetric {
    /// Stable workload contract ID.
    pub workload_id: String,
    /// Exact registered fixture tuple.
    pub tuple_id: String,
    /// Stable metric name.
    pub metric: String,
    /// Counterbalanced no-change harmful effect.
    pub observed_effect: f64,
    /// Lower confidence bound from the no-change comparison.
    pub confidence_lower: f64,
    /// Upper confidence bound from the no-change comparison.
    pub confidence_upper: f64,
    /// Largest absolute observed effect or confidence bound.
    pub noise_bound: f64,
    /// Reviewed practical budget being calibrated.
    pub practical_budget: f64,
    /// Number of independent blocks available in the evidence.
    pub available_blocks: u32,
    /// Even block count recommended for the reference profile.
    pub recommended_blocks: u32,
    /// Reviewed power target used to recommend the block count.
    pub target_power: f64,
    /// Approximate power available from the observed block count.
    pub estimated_power: f64,
    /// Residual effect attributable to schedule orientation.
    pub residual_orientation_effect: f64,
    /// Maximum reviewed orientation effect.
    pub max_residual_orientation_effect: f64,
    /// Whether the evidence can support the reviewed budget.
    pub repeatable: bool,
}

/// Canonical no-change evidence from which reviewed budgets may be published.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalibrationDocument {
    /// Document shape identifier, currently [`CALIBRATION_SCHEMA`].
    pub schema_id: String,
    /// Stable evidence identity.
    pub id: String,
    /// RFC 3339 generation timestamp.
    pub created_at: String,
    /// Source comparison campaign ID.
    pub source_run_id: String,
    /// Canonical environment contract ID.
    pub environment_id: String,
    /// Digest shared by both sides of the no-change campaign.
    pub build_digest: String,
    /// Per-workload metric evidence.
    pub metrics: Vec<CalibrationMetric>,
    /// Whether every metric supports publication.
    pub publishable: bool,
    /// Reasons publication is not allowed.
    pub failures: Vec<String>,
}

/// One observed point in a bounded capacity staircase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacityPoint {
    /// Number of worker processes used by the fixture.
    pub workers: u32,
    /// Declared offered-load step.
    pub load_step: u32,
    /// Useful cases completed by each sample.
    pub cases: u64,
    /// Count of independent valid observations.
    pub samples: usize,
    /// Median useful-case throughput per second.
    pub throughput_per_second: f64,
    /// Sample-level p95 terminal latency in milliseconds.
    pub p95_latency_ms: f64,
    /// Peak process CPU percentage normalized by the declared worker count.
    pub process_cpu_percent_per_worker: f64,
    /// Peak shared-service CPU percentage, when collected.
    pub service_cpu_percent: Option<f64>,
}

/// Result of applying the declared knee rule to one worker-count staircase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacityKnee {
    /// Number of worker processes represented by the staircase.
    pub workers: u32,
    /// First load step satisfying the knee rule, if observed.
    pub knee_step: Option<u32>,
    /// Useful throughput at the observed knee.
    pub knee_throughput_per_second: Option<f64>,
    /// Highest valid observed rate when no knee was found.
    pub observed_rate_lower_bound: Option<f64>,
}

/// Bounded one/two-worker capacity evidence kept separate from fixed-load results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacityDocument {
    /// Document shape identifier, currently [`CAPACITY_SCHEMA`].
    pub schema_id: String,
    /// Source single-binary campaign ID.
    pub source_run_id: String,
    /// Immutable build digest under calibration.
    pub build_digest: String,
    /// Environment contract ID observed by the source campaign.
    pub environment_id: String,
    /// Whether the source campaign ran on the validated canonical host.
    pub canonical: bool,
    /// Valid staircase points ordered by worker count and load.
    pub points: Vec<CapacityPoint>,
    /// One result for each calibrated worker count.
    pub knees: Vec<CapacityKnee>,
    /// Near-origin two-worker scale efficiency when both knees exist.
    pub scale_efficiency_2: Option<f64>,
    /// Whether evidence supports a linear worker-count estimate.
    pub supports_linear_projection: bool,
    /// External-validity or completeness failures.
    pub failures: Vec<String>,
}

/// One reviewed gating rule resolved by workload tuple and metric.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BudgetEntry {
    /// Stable workload contract ID.
    pub workload_id: String,
    /// Exact registered fixture tuple.
    pub tuple_id: String,
    /// Stable metric name.
    pub metric: String,
    /// Maximum accepted harmful relative effect.
    pub practical_budget: f64,
    /// Minimum independent block count required for a verdict.
    pub minimum_blocks: u32,
    /// Maximum permitted residual orientation effect.
    pub max_residual_orientation_effect: f64,
}

/// Reviewed, environment-specific performance gates supported by calibration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BudgetPolicy {
    /// Document shape identifier, currently [`BUDGET_SCHEMA`].
    pub schema_id: String,
    /// Versioned policy identity referenced by profiles.
    pub id: String,
    /// Canonical environment for which the policy is valid.
    pub environment_id: String,
    /// Calibration evidence identity supporting this policy.
    pub calibration_id: String,
    /// RFC 3339 review timestamp.
    pub approved_at: String,
    /// Human or review process that approved the practical budgets.
    pub approved_by: String,
    /// Per-workload metric gates.
    pub entries: Vec<BudgetEntry>,
}

/// Immutable provenance index for one published calibrated baseline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaselineDocument {
    /// Document shape identifier, currently [`BASELINE_SCHEMA`].
    pub schema_id: String,
    /// Versioned baseline and profile identity.
    pub id: String,
    /// RFC 3339 publication timestamp.
    pub created_at: String,
    /// Canonical environment for which the baseline is valid.
    pub environment_id: String,
    /// Immutable executable digest shared by all evidence.
    pub build_digest: String,
    /// Relative canonical noise evidence path.
    pub calibration_file: String,
    /// BLAKE3 digest of the noise evidence.
    pub calibration_digest: String,
    /// Relative bounded-capacity evidence path.
    pub capacity_file: String,
    /// BLAKE3 digest of the capacity evidence.
    pub capacity_digest: String,
    /// Relative reviewed budget policy path.
    pub budget_file: String,
    /// BLAKE3 digest of the reviewed budget policy.
    pub budget_digest: String,
    /// Relative calibrated reference profile path.
    pub profile_file: String,
    /// BLAKE3 digest of the reference profile.
    pub profile_digest: String,
    /// Relative immutable build-manifest snapshot path.
    pub build_manifest_file: String,
    /// BLAKE3 digest of the build-manifest snapshot.
    pub build_manifest_digest: String,
}
