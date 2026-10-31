//! Worker process command.
//!
//! Workers consume chunk-ready queue messages, claim work, warm evaluator
//! components, execute case-level evaluation workflows, and acknowledge or
//! requeue queue messages based on outcome. Chunk-local worker persistence uses
//! the execution database selected by `run_id + run_shard`; run profile
//! snapshots are read from that execution placement, while evaluator registry
//! metadata remains a control-database read. Retryable database failures are
//! contained per alias so the process can continue serving healthy placements.

use std::{
    sync::Arc,
    time::{
        Duration,
        Instant,
    },
};

use async_trait::async_trait;
use clap::{
    Args,
    Subcommand,
};
use futures_util::StreamExt;
use moka::future::Cache;
use serde::Deserialize;
use tokio::{
    sync::{
        Mutex,
        OnceCell,
    },
    task::{
        JoinHandle,
        JoinSet,
    },
};
use tokio_util::sync::CancellationToken;
use tracing::{
    debug,
    info,
    warn,
};
use uuid::Uuid;

use super::{
    Executable,
    args::{
        CircuitBreakerOptions,
        DatabaseOperationTimeoutOptions,
        MessagingOptions,
        WasmOptions,
    },
};
use crate::{
    context::{
        Context,
        database::ExecutionRouteError,
    },
    contracts::run::RunProfile,
    db::{
        tables::run_snapshots,
        workflows::{
            chunk_processing,
            execution_processing,
        },
    },
    runtime::ServiceRunner,
};

mod once;
mod start;

const CHUNK_LEASE_SECONDS: i32 = 60;
const CHUNK_PROCESSING_LEASE_SECONDS: i32 = 120;
const ATTEMPT_LEASE_SECONDS: i32 = 120;
const WORKER_HEARTBEAT_INTERVAL_SECONDS: u64 = 15;
const RUN_CONTEXT_CACHE_MAX_ENTRIES: u64 = 1024;
const RUN_CONTEXT_CACHE_TTI_SECONDS: u64 = 900;
const WARMUP_PARALLELISM: usize = 8;
const DEFAULT_WORKER_MAX_INFLIGHT_CHUNKS: u16 = 1;
const WORKER_MQ_RECONNECT_INITIAL_DELAY_MS: u64 = 250;
const WORKER_MQ_RECONNECT_MAX_DELAY_MS: u64 = 30_000;
const WORKER_ROUTE_RETRY_DELAY_SECONDS: i64 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerCycleOutcome {
    Processed,
    Empty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetrySettlement {
    Retried,
    Delayed,
    AcknowledgedStale,
    FailedExhausted,
}

#[derive(Debug)]
struct WorkerRunContext {
    reader: Arc<crate::case_reader::RunReader>,
    profile: RunProfile,
    evaluator_catalog: execution_processing::RunEvaluatorCatalog,
}

#[derive(Debug, Clone)]
struct WorkerRuntime {
    worker_id: Uuid,
    worker_host: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct WorkerRunContextKey {
    run_id: Uuid,
    run_shard: i16,
}

#[derive(Debug, Deserialize)]
/// Queue payload that signals a run chunk is ready for processing.
struct ChunkReadyMessage {
    run_id: Uuid,
    run_shard: i16,
    chunk_id: Uuid,
    database_alias: String,
    write_epoch: i64,
}

struct ChunkLeaseGuard {
    db: sqlx::PgPool,
    chunk: Mutex<crate::models::run_chunk::RunChunk>,
    lease_seconds: i32,
}

struct ChunkHeartbeat {
    guard: Arc<ChunkLeaseGuard>,
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

#[derive(Debug, Default, Clone, Copy)]
/// Summary of evaluator cache warmup performed before processing a chunk.
struct WarmupStats {
    requested: usize,
    cache_hits: usize,
    loaded: usize,
}

impl WorkerRuntime {
    fn new() -> Self {
        Self {
            worker_id: Uuid::now_v7(),
            worker_host: worker_host_label(),
        }
    }
}

impl ChunkLeaseGuard {
    fn new(
        db: &sqlx::PgPool,
        chunk: crate::models::run_chunk::RunChunk,
        lease_seconds: i32,
    ) -> Arc<Self> {
        Arc::new(Self {
            db: db.clone(),
            chunk: Mutex::new(chunk),
            lease_seconds,
        })
    }

    async fn current_chunk(&self) -> crate::models::run_chunk::RunChunk {
        self.chunk.lock().await.clone()
    }

    async fn renew(&self) -> anyhow::Result<bool> {
        let current = self.current_chunk().await;
        let Some(renewed) =
            chunk_processing::extend_chunk_lease(&self.db, &current, self.lease_seconds).await?
        else {
            return Ok(false);
        };
        *self.chunk.lock().await = renewed;
        Ok(true)
    }
}

impl ChunkHeartbeat {
    fn start(
        context: Context,
        database_alias: String,
        db: &sqlx::PgPool,
        chunk: crate::models::run_chunk::RunChunk,
        runtime: WorkerRuntime,
    ) -> Self {
        let guard = ChunkLeaseGuard::new(db, chunk, CHUNK_PROCESSING_LEASE_SECONDS);
        let cancel = CancellationToken::new();
        let heartbeat_guard = guard.clone();
        let heartbeat_cancel = cancel.clone();
        let handle = tokio::spawn(async move {
            run_chunk_heartbeat(
                context,
                database_alias,
                heartbeat_guard,
                runtime,
                heartbeat_cancel,
            )
            .await;
        });

        Self {
            guard,
            cancel,
            handle,
        }
    }

    async fn stop(self) -> crate::models::run_chunk::RunChunk {
        self.cancel.cancel();
        if let Err(err) = self.handle.await {
            warn!(error = %err, "worker heartbeat task failed to join");
        }
        self.guard.current_chunk().await
    }
}

fn worker_host_label() -> Option<String> {
    ["VIGILO_WORKER_HOST", "HOSTNAME", "COMPUTERNAME"]
        .iter()
        .find_map(|key| std::env::var(key).ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

async fn run_chunk_heartbeat(
    context: Context,
    database_alias: String,
    guard: Arc<ChunkLeaseGuard>,
    runtime: WorkerRuntime,
    cancel: CancellationToken,
) {
    let mut interval =
        tokio::time::interval(Duration::from_secs(WORKER_HEARTBEAT_INTERVAL_SECONDS));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = interval.tick() => {
                let chunk = guard.current_chunk().await;
                let database_router = match context.dbr().await {
                    Ok(database_router) => database_router,
                    Err(err) => {
                        warn!(error = %err, "failed to resolve database router during worker heartbeat");
                        continue;
                    }
                };
                let heartbeat_result = database_router.run_database_operation(
                    &database_alias,
                    "worker_chunk_heartbeat",
                    async {
                        let renewed = guard.renew().await?;
                        let renewed_attempts = if renewed {
                            Some(execution_processing::heartbeat_running_attempts_for_chunk(
                                &guard.db,
                                chunk.run_id,
                                chunk.run_shard,
                                chunk.id,
                                runtime.worker_id,
                                ATTEMPT_LEASE_SECONDS,
                            ).await?)
                        } else {
                            None
                        };
                        Ok((renewed, renewed_attempts))
                    },
                ).await;
                match heartbeat_result {
                    Ok((true, Some(renewed_attempts))) => {
                        debug!(
                            run_id = %chunk.run_id,
                            chunk_id = %chunk.id,
                            worker_id = %runtime.worker_id,
                            renewed_attempts,
                            "renewed worker chunk and attempt leases"
                        );
                    }
                    Ok((true, None)) => {}
                    Ok((false, _)) => {
                        warn!(
                            run_id = %chunk.run_id,
                            chunk_id = %chunk.id,
                            worker_id = %runtime.worker_id,
                            "worker lost chunk lease during heartbeat"
                        );
                        cancel.cancel();
                        return;
                    }
                    Err(err) => {
                        warn!(
                            run_id = %chunk.run_id,
                            chunk_id = %chunk.id,
                            worker_id = %runtime.worker_id,
                            error = %err,
                            "failed to renew worker leases during heartbeat"
                        );
                    }
                }
            }
        }
    }
}

fn broker_message_id(message: &crate::mq::RawConsumedMessage) -> Option<String> {
    message
        .properties
        .message_id()
        .as_ref()
        .map(ToString::to_string)
}

fn worker_stream_prefetch(max_inflight_chunks: u16) -> u16 {
    max_inflight_chunks.max(1)
}

fn execution_route_is_temporarily_blocked(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<crate::db::workflows::local_shard_admission::LocalShardAdmissionError>()
        .is_some()
        || matches!(
            error.downcast_ref::<ExecutionRouteError>(),
            Some(ExecutionRouteError::NonDispatchableShardPlacement { .. })
        )
}

#[derive(Clone)]
/// Worker-local helper that resolves evaluator components into runtime cache.
///
/// This service is intentionally lightweight and command-scoped. It validates
/// evaluator identifiers and state before compiling WASM bytes into cached
/// Wasmtime components. All shard contexts share one bounded immutable input
/// byte cache; mutable placement and lease authority remain independently checked.
struct EvaluatorLoaderService {
    context: Context,
    read_cache: Arc<crate::case_reader::ReadCache>,
    run_context_cache: Arc<Cache<WorkerRunContextKey, Arc<OnceCell<Arc<WorkerRunContext>>>>>,
}

impl EvaluatorLoaderService {
    /// Creates a loader service bound to the current command context.
    fn new(context: Context) -> Self {
        Self {
            context,
            read_cache: Arc::new(crate::case_reader::ReadCache::default()),
            run_context_cache: Arc::new(
                Cache::builder()
                    .max_capacity(RUN_CONTEXT_CACHE_MAX_ENTRIES)
                    .time_to_idle(Duration::from_secs(RUN_CONTEXT_CACHE_TTI_SECONDS))
                    .build(),
            ),
        }
    }

    /// Returns a prepared evaluator from cache or loads it from the registry.
    ///
    /// Errors if:
    /// - evaluator ref format is invalid
    /// - evaluator does not exist
    /// - evaluator is in a non-runnable state
    /// - WASM compilation fails
    async fn get_or_load(
        &self,
        evaluator_ref: &str,
        evaluator: &execution_processing::RunEvaluatorCatalogEntry,
    ) -> anyhow::Result<crate::evaluator_abi::PreparedEvaluator> {
        execution_processing::get_or_load_component(&self.context, evaluator_ref, evaluator).await
    }

    /// Preloads a pinned evaluator catalog into the registry cache.
    async fn warm_catalog(
        &self,
        evaluator_catalog: &execution_processing::RunEvaluatorCatalog,
    ) -> anyhow::Result<WarmupStats> {
        let started = Instant::now();
        let mut stats = WarmupStats {
            requested: evaluator_catalog.len(),
            ..WarmupStats::default()
        };

        let cache = self.context.reg().await?;
        let mut misses = Vec::new();
        for (evaluator_ref, evaluator) in evaluator_catalog {
            if cache
                .get(&evaluator.abi.cache_key(&evaluator.content_hash))
                .await
                .is_some()
            {
                stats.cache_hits += 1;
                continue;
            }

            misses.push((evaluator_ref.clone(), evaluator.clone()));
        }

        for batch in misses.chunks(WARMUP_PARALLELISM) {
            let mut tasks = JoinSet::new();
            for (evaluator_ref, evaluator) in batch {
                let evaluator_ref = evaluator_ref.clone();
                let evaluator = evaluator.clone();
                let loader = self.clone();
                tasks.spawn(async move {
                    loader
                        .get_or_load(&evaluator_ref, &evaluator)
                        .await
                        .map(|_| evaluator_ref)
                });
            }

            while let Some(task_result) = tasks.join_next().await {
                let evaluator_ref = task_result
                    .map_err(|err| anyhow::anyhow!("warmup worker task join failed: {}", err))??;
                stats.loaded += 1;
                debug!(
                    evaluator_ref,
                    "loaded evaluator into runtime cache during warmup"
                );
            }
        }

        debug!(
            requested = stats.requested,
            cache_hits = stats.cache_hits,
            loaded = stats.loaded,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "completed evaluator warmup pass"
        );

        Ok(stats)
    }

    /// Returns run-scoped profile and evaluator metadata, using cache when available.
    async fn get_or_build_run_context(
        &self,
        run_id: Uuid,
        run_shard: i16,
        execution_db: &sqlx::PgPool,
        database_alias: &str,
        cancel: &CancellationToken,
    ) -> anyhow::Result<Arc<WorkerRunContext>> {
        let key = WorkerRunContextKey { run_id, run_shard };
        let cell = self
            .run_context_cache
            .get_with(key, async { Arc::new(OnceCell::new()) })
            .await;
        let context = cell
            .get_or_try_init(|| async {
                let raw: serde_json::Value = self.context.dbr().await?.run_database_operation(database_alias,"worker_run_snapshot",async {
                    Ok(sqlx::query_scalar("SELECT config_snapshot FROM run_snapshots WHERE run_id=$1 AND run_shard=$2")
                        .bind(run_id).bind(run_shard).fetch_one(execution_db).await?)
                }).await?;
                let reference = serde_json::from_value(
                    raw.get("run_plan")
                        .ok_or_else(|| anyhow::anyhow!("run snapshot has no immutable plan"))?
                        .clone(),
                )?;
                let limits = serde_json::from_value(raw["storage_limits"].clone())?;
                let binding = raw["storage_binding_hash"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("missing storage binding"))?;
                let store = self.context.storage(binding, &limits).await?;
                let reader = Arc::new(
                    crate::case_reader::RunReader::open_cached(
                        store,
                        &reference,
                        limits,
                        run_id,
                        cancel,
                        self.read_cache.clone(),
                        binding.to_owned(),
                    )
                    .await?,
                );
                let snapshot =
                    run_snapshots::decode_worker_snapshot(reader.configuration.snapshot.clone())?;
                let evaluator_catalog = execution_processing::build_pinned_evaluator_catalog(
                    snapshot.execution_plan.evaluators.clone(),
                )?;
                let expected_refs =
                    execution_processing::evaluator_refs_from_profile(&snapshot.profile)?;
                if expected_refs.len() != evaluator_catalog.len()
                    || expected_refs
                        .iter()
                        .any(|evaluator_ref| !evaluator_catalog.contains_key(evaluator_ref))
                {
                    anyhow::bail!(
                        "run '{}' execution plan does not cover its profile evaluators",
                        run_id
                    );
                }
                Ok::<_, anyhow::Error>(Arc::new(WorkerRunContext {
                    reader,
                    profile: snapshot.profile,
                    evaluator_catalog,
                }))
            })
            .await?;
        Ok(context.clone())
    }
}

#[derive(Debug, Subcommand)]
/// Worker execution modes.
pub(crate) enum SubCommand {
    /// Start a worker process
    Start {
        /// Maximum chunk messages processed concurrently by one worker process
        #[arg(long, env = "VIGILO_WORKER_MAX_INFLIGHT_CHUNKS", default_value_t = DEFAULT_WORKER_MAX_INFLIGHT_CHUNKS, value_parser = clap::value_parser!(u16).range(1..=1024))]
        max_inflight_chunks: u16,
    },

    /// Process a single worker cycle and exit
    Once,
}

#[derive(Debug, Args)]
/// Arguments for `vigilo worker`.
pub(crate) struct Command {
    #[command(flatten)]
    pub(crate) messaging: MessagingOptions,

    #[command(flatten)]
    pub(crate) circuit_breaker: CircuitBreakerOptions,

    #[command(flatten)]
    pub(crate) database_operation_timeout: DatabaseOperationTimeoutOptions,

    #[command(flatten)]
    pub(crate) wasm: WasmOptions,

    #[command(flatten)]
    pub(crate) agent_http: crate::cli::args::AgentHttpOptions,

    #[command(subcommand)]
    pub command: Option<SubCommand>,
}

#[async_trait]
impl Executable for Command {
    /// Executes the selected worker mode.
    async fn exec(self, context: Context) -> anyhow::Result<()> {
        match self.command {
            Some(SubCommand::Start {
                max_inflight_chunks,
            }) => {
                info!("starting worker process");
                start::exec(context, WorkerRuntime::new(), max_inflight_chunks).await
            }
            Some(SubCommand::Once) => {
                info!("running single worker cycle");
                once::exec(context, WorkerRuntime::new()).await
            }
            None => anyhow::bail!("missing worker subcommand; use `vigilo worker start`"),
        }
    }
}

/// Executes a single worker cycle.
///
/// Cycle sequence:
/// 1. consume queue message
/// 2. parse payload and claim chunk lease
/// 3. warm evaluator components from run profile
/// 4. load and process the chunk case batch
/// 5. ack on success, retry with backoff on recoverable failures
async fn run_worker_drain_pass(
    context: Context,
    evaluator_loader: &EvaluatorLoaderService,
    runtime: WorkerRuntime,
    max_messages: usize,
) -> anyhow::Result<usize> {
    let mut processed = 0usize;

    for _ in 0..max_messages {
        match run_worker_cycle(context.clone(), evaluator_loader, runtime.clone()).await? {
            WorkerCycleOutcome::Processed => {
                processed += 1;
            }
            WorkerCycleOutcome::Empty => break,
        }
    }

    Ok(processed)
}

async fn run_worker_cycle(
    context: Context,
    evaluator_loader: &EvaluatorLoaderService,
    runtime: WorkerRuntime,
) -> anyhow::Result<WorkerCycleOutcome> {
    // --- Fetch worker message ---
    // `Empty` is a normal one-shot outcome, not an error.
    debug!("starting worker cycle pre-flight");

    debug!("acquiring messaging context");
    let mq = context.mq().await?;
    debug!("messaging context ready");

    debug!("attempting to consume worker message");

    let Some(raw_message) = mq.consume_worker_message().await? else {
        debug!("no worker messages available");
        return Ok(WorkerCycleOutcome::Empty);
    };

    // --- Validate outer JSON body ---
    // Invalid JSON is not retryable worker work, so it is quarantined and the
    // cycle is considered processed.
    let message = match serde_json::from_slice::<serde_json::Value>(&raw_message.body) {
        Ok(payload) => crate::mq::ConsumedMessage {
            raw: raw_message,
            payload,
        },
        Err(err) => {
            mq.quarantine_worker_message(
                &raw_message,
                &format!("worker message body was not valid JSON: {}", err),
                "invalid_json",
            )
            .await?;
            warn!("quarantined invalid worker message body");
            return Ok(WorkerCycleOutcome::Processed);
        }
    };

    run_worker_message(context, evaluator_loader, runtime, message).await
}

struct ChunkSettlementContext<'a> {
    database_router: &'a crate::context::database::DatabaseRouter,
    database_alias: &'a str,
    db: &'a sqlx::PgPool,
    mq: &'a crate::mq::Client,
    message: &'a crate::mq::ConsumedMessage,
    chunk: &'a crate::models::run_chunk::RunChunk,
}

impl<'a> ChunkSettlementContext<'a> {
    fn new(
        database_router: &'a crate::context::database::DatabaseRouter,
        database_alias: &'a str,
        db: &'a sqlx::PgPool,
        mq: &'a crate::mq::Client,
        message: &'a crate::mq::ConsumedMessage,
        chunk: &'a crate::models::run_chunk::RunChunk,
    ) -> Self {
        Self {
            database_router,
            database_alias,
            db,
            mq,
            message,
            chunk,
        }
    }
}

async fn settle_retryable_chunk_failure(
    settlement: ChunkSettlementContext<'_>,
    reason: &str,
    error_class: &str,
) -> anyhow::Result<RetrySettlement> {
    // This settlement path is for real worker failures: warmup, DB load,
    // processing, or terminal-transition errors. These consume the bounded
    // RabbitMQ retry budget and eventually fail the chunk if the worker still
    // owns the lease.
    if settlement
        .mq
        .can_retry_worker_message(&settlement.message.raw)
    {
        let released = settlement
            .database_router
            .run_database_operation(
                settlement.database_alias,
                "worker_release_failed_chunk",
                chunk_processing::release_chunk_as_pending(settlement.db, settlement.chunk),
            )
            .await?;
        if released > 0 {
            settlement
                .mq
                .retry_worker_message(&settlement.message.raw, reason, error_class)
                .await?;
            Ok(RetrySettlement::Retried)
        } else {
            settlement.mq.ack(&settlement.message.raw).await?;
            Ok(RetrySettlement::AcknowledgedStale)
        }
    } else {
        let failed = settlement
            .database_router
            .run_database_operation(
                settlement.database_alias,
                "worker_fail_exhausted_chunk",
                chunk_processing::mark_chunk_failed_and_settle(settlement.db, settlement.chunk),
            )
            .await?;
        if failed > 0 {
            settlement
                .mq
                .quarantine_worker_message(
                    &settlement.message.raw,
                    &format!("worker message retry budget exhausted: {}", reason),
                    error_class,
                )
                .await?;
            Ok(RetrySettlement::FailedExhausted)
        } else {
            settlement.mq.ack(&settlement.message.raw).await?;
            Ok(RetrySettlement::AcknowledgedStale)
        }
    }
}

async fn settle_chunk_waiting_for_execution_retry(
    settlement: ChunkSettlementContext<'_>,
    reason: &str,
    next_retry_after: Option<chrono::DateTime<chrono::Utc>>,
) -> anyhow::Result<RetrySettlement> {
    // This settlement path is for planned execution retry waits. It releases
    // the chunk and delays redelivery without incrementing the worker-failure
    // retry count; the database retry_after field remains the source of truth.
    let released = settlement
        .database_router
        .run_database_operation(
            settlement.database_alias,
            "worker_release_retry_wait",
            chunk_processing::release_chunk_as_pending(settlement.db, settlement.chunk),
        )
        .await?;
    if released == 0 {
        settlement.mq.ack(&settlement.message.raw).await?;
        return Ok(RetrySettlement::AcknowledgedStale);
    }

    let delay_seconds = next_retry_after
        .map(|retry_after| {
            retry_after
                .signed_duration_since(chrono::Utc::now())
                .num_seconds()
                .max(1)
        })
        .unwrap_or(1);
    settlement
        .mq
        .delay_worker_message(
            &settlement.message.raw,
            delay_seconds,
            reason,
            "execution_retry",
        )
        .await?;

    Ok(RetrySettlement::Delayed)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkerDatabaseFailureDisposition {
    delay_seconds: i64,
}

fn retry_delay_seconds(duration: Duration) -> i64 {
    let rounded = duration
        .as_secs()
        .saturating_add(u64::from(duration.subsec_nanos() > 0));
    i64::try_from(rounded).unwrap_or(i64::MAX).max(1)
}

fn contain_worker_database_error(
    database_router: &crate::context::database::DatabaseRouter,
    permit: crate::context::database::CircuitPermit,
    error: &anyhow::Error,
    claimed: bool,
) -> Option<WorkerDatabaseFailureDisposition> {
    // Database retries do not consume the worker's application-failure budget.
    // Once claimed, the delivery must wait until its lease cannot still admit
    // writes from this worker.
    let (unavailable, transition) = database_router.record_database_operation_error(permit, error);
    if !unavailable && !crate::context::database::is_database_contention(error) {
        return None;
    }

    let circuit_delay = match transition {
        Some(crate::context::database::CircuitTransition::Opened { retry_after }) => {
            retry_delay_seconds(retry_after)
        }
        _ => WORKER_ROUTE_RETRY_DELAY_SECONDS,
    };
    let lease_delay = if claimed {
        i64::from(CHUNK_PROCESSING_LEASE_SECONDS).saturating_add(5)
    } else {
        0
    };
    Some(WorkerDatabaseFailureDisposition {
        delay_seconds: circuit_delay.max(lease_delay),
    })
}

fn is_retryable_worker_database_error(error: &anyhow::Error) -> bool {
    crate::context::database::is_database_unavailable(error)
        || crate::context::database::is_database_contention(error)
}

async fn run_worker_message(
    context: Context,
    evaluator_loader: &EvaluatorLoaderService,
    runtime: WorkerRuntime,
    message: crate::mq::ConsumedMessage,
) -> anyhow::Result<WorkerCycleOutcome> {
    let Ok(payload) = serde_json::from_value::<ChunkReadyMessage>(message.payload.clone()) else {
        let mut claimed = false;
        return run_worker_message_admitted(
            context,
            evaluator_loader,
            runtime,
            &message,
            &mut claimed,
        )
        .await;
    };
    let database_router = context.dbr().await?;
    let permit = match database_router.acquire_database_operation(&payload.database_alias) {
        Ok(permit) => permit,
        Err(open) => {
            let delay_seconds =
                retry_delay_seconds(open.retry_after).max(WORKER_ROUTE_RETRY_DELAY_SECONDS);
            context
                .mq()
                .await?
                .delay_worker_message(
                    &message.raw,
                    delay_seconds,
                    "execution database circuit is open",
                    "execution_database_unavailable",
                )
                .await?;
            info!(
                database_alias = %payload.database_alias,
                run_id = %payload.run_id,
                run_shard = payload.run_shard,
                chunk_id = %payload.chunk_id,
                delay_seconds,
                "delayed worker message while execution database circuit is open"
            );
            return Ok(WorkerCycleOutcome::Processed);
        }
    };

    let mut claimed = false;
    match run_worker_message_admitted(
        context.clone(),
        evaluator_loader,
        runtime,
        &message,
        &mut claimed,
    )
    .await
    {
        Ok(outcome) => {
            database_router.record_database_operation_success(permit);
            Ok(outcome)
        }
        Err(error) => {
            let Some(disposition) =
                contain_worker_database_error(database_router, permit, &error, claimed)
            else {
                return Err(error);
            };
            context
                .mq()
                .await?
                .delay_worker_message(
                    &message.raw,
                    disposition.delay_seconds,
                    &error.to_string(),
                    "execution_database_unavailable",
                )
                .await?;
            warn!(
                database_alias = %payload.database_alias,
                run_id = %payload.run_id,
                run_shard = payload.run_shard,
                chunk_id = %payload.chunk_id,
                claimed,
                delay_seconds = disposition.delay_seconds,
                error = %error,
                "contained worker execution database failure; delayed message"
            );
            Ok(WorkerCycleOutcome::Processed)
        }
    }
}

async fn run_worker_message_admitted(
    context: Context,
    evaluator_loader: &EvaluatorLoaderService,
    runtime: WorkerRuntime,
    message: &crate::mq::ConsumedMessage,
    claimed_by_worker: &mut bool,
) -> anyhow::Result<WorkerCycleOutcome> {
    // --- Acquire shared services ---
    // Settlement paths use the message queue handle to keep ack/retry behavior
    // centralized. The execution database is resolved after payload validation
    // because routing requires run_id + run_shard from the chunk-ready message.
    let mq = context.mq().await?;

    debug!(
        delivery_tag = message.delivery_tag(),
        "consumed worker message"
    );

    // --- Validate chunk-ready payload ---
    // Schema errors cannot map to a run chunk safely, so they go to quarantine
    // instead of retry.
    let payload = match serde_json::from_value::<ChunkReadyMessage>(message.payload.clone()) {
        Ok(payload) => payload,
        Err(err) => {
            mq.quarantine_worker_message(
                &message.raw,
                &format!("invalid chunk-ready message payload: {}", err),
                "invalid_schema",
            )
            .await?;
            warn!(error = %err, "quarantined invalid chunk-ready message payload");
            debug!(
                delivery_tag = message.delivery_tag(),
                "invalid message quarantined"
            );
            return Ok(WorkerCycleOutcome::Processed);
        }
    };

    debug!(
        run_id = %payload.run_id,
        run_shard = payload.run_shard,
        chunk_id = %payload.chunk_id,
        "parsed chunk-ready message payload"
    );

    // --- Resolve execution database and claim local write authority ---
    // The queue message carries the route used by dispatch. The execution
    // database validates its local epoch in the same transaction as the claim,
    // avoiding a control-database read on the normal worker path.
    let database_router = context.dbr().await?;
    let hinted_route = match database_router
        .run_database_operation(
            &payload.database_alias,
            "worker_execution_route",
            database_router.execution_write_route(
                crate::db::workflows::local_shard_admission::LocalShardRouteHint {
                    run_id: payload.run_id,
                    run_shard: payload.run_shard,
                    database_alias: payload.database_alias.clone(),
                    write_epoch: payload.write_epoch,
                },
            ),
        )
        .await
    {
        Ok(route) => route,
        Err(err) if execution_route_is_temporarily_blocked(&err) => {
            mq.delay_worker_message(
                &message.raw,
                WORKER_ROUTE_RETRY_DELAY_SECONDS,
                &err.to_string(),
                "execution_database_unavailable",
            )
            .await?;
            info!(
                run_id = %payload.run_id,
                run_shard = payload.run_shard,
                chunk_id = %payload.chunk_id,
                delay_seconds = WORKER_ROUTE_RETRY_DELAY_SECONDS,
                error = %err,
                "execution database is temporarily unavailable; delayed worker message"
            );
            return Ok(WorkerCycleOutcome::Processed);
        }
        Err(err) => return Err(err),
    };
    // --- Claim chunk ownership ---
    // Duplicate, stale, cancelled, completed, or not-yet-running chunks are
    // acknowledged because the database refused ownership.
    let (db, claimed) = match database_router
        .run_database_operation(
            &payload.database_alias,
            "worker_chunk_claim",
            claim_hinted_chunk_with_route_refresh(database_router, &hinted_route, payload.chunk_id),
        )
        .await
    {
        Ok(claimed) => claimed,
        Err(err) if execution_route_is_temporarily_blocked(&err) => {
            mq.delay_worker_message(
                &message.raw,
                WORKER_ROUTE_RETRY_DELAY_SECONDS,
                &err.to_string(),
                "execution_route_changed_before_claim",
            )
            .await?;
            info!(
                run_id = %payload.run_id,
                run_shard = payload.run_shard,
                chunk_id = %payload.chunk_id,
                delay_seconds = WORKER_ROUTE_RETRY_DELAY_SECONDS,
                error = %err,
                "execution route changed before chunk claim; delayed worker message"
            );
            return Ok(WorkerCycleOutcome::Processed);
        }
        Err(err) => return Err(err),
    };
    let db = &db;
    let Some(mut chunk) = claimed else {
        mq.ack(&message.raw).await?;
        info!(
            chunk_id = %payload.chunk_id,
            run_id = %payload.run_id,
            "chunk not claimable; acknowledging message"
        );
        debug!(
            delivery_tag = message.delivery_tag(),
            "unclaimable chunk message acknowledged"
        );
        return Ok(WorkerCycleOutcome::Processed);
    };
    *claimed_by_worker = true;

    debug!(
        run_id = %chunk.run_id,
        run_shard = chunk.run_shard,
        chunk_id = %chunk.id,
        ordinal_start = chunk.ordinal_start,
        ordinal_end = chunk.ordinal_end,
        "claimed chunk for processing"
    );

    // --- Load run context and extend lease ---
    // The opaque claim token remains the authority for later writes while the
    // heartbeat advances its expiration deadline.
    let Some(extended_chunk) = database_router
        .run_database_operation(
            &payload.database_alias,
            "worker_chunk_lease_extension",
            chunk_processing::extend_chunk_lease(db, &chunk, CHUNK_PROCESSING_LEASE_SECONDS),
        )
        .await?
    else {
        mq.ack(&message.raw).await?;
        warn!(
            run_id = %chunk.run_id,
            chunk_id = %chunk.id,
            lease_seconds = CHUNK_PROCESSING_LEASE_SECONDS,
            "chunk lease was lost before processing budget extension; acknowledged stale message"
        );
        return Ok(WorkerCycleOutcome::Processed);
    };
    chunk = extended_chunk;
    debug!(
        run_id = %chunk.run_id,
        chunk_id = %chunk.id,
        lease_seconds = CHUNK_PROCESSING_LEASE_SECONDS,
        leased_until = ?chunk.leased_until,
        "extended chunk lease for processing budget"
    );
    let heartbeat = ChunkHeartbeat::start(
        context.clone(),
        payload.database_alias.clone(),
        db,
        chunk.clone(),
        runtime.clone(),
    );
    let loaded = tokio::select! {
        result=evaluator_loader.get_or_build_run_context(chunk.run_id,chunk.run_shard,db,&payload.database_alias,&heartbeat.cancel)=>result,
        _=heartbeat.cancel.cancelled()=>Err(anyhow::anyhow!("chunk authority lost during context read")),
    };
    let run_context = match loaded {
        Ok(context) => context,
        Err(error) => {
            let chunk = heartbeat.stop().await;
            if is_retryable_worker_database_error(&error) {
                return Err(error);
            }
            settle_retryable_chunk_failure(
                ChunkSettlementContext::new(
                    database_router,
                    &payload.database_alias,
                    db,
                    mq,
                    message,
                    &chunk,
                ),
                &error.to_string(),
                "run_context",
            )
            .await?;
            return Ok(WorkerCycleOutcome::Processed);
        }
    };
    let attempt_lease = execution_processing::AttemptLeaseContext {
        worker_id: runtime.worker_id,
        worker_host: runtime.worker_host.clone(),
        queue_message_id: Uuid::now_v7(),
        broker_message_id: broker_message_id(&message.raw),
        lease_seconds: ATTEMPT_LEASE_SECONDS,
    };

    // --- Warm evaluator components ---
    // Warmup failures are treated as recoverable worker failures because no
    // case state has been advanced yet.
    match evaluator_loader
        .warm_catalog(&run_context.evaluator_catalog)
        .await
    {
        Ok(stats) => {
            debug!(
                run_id = %chunk.run_id,
                requested = stats.requested,
                cache_hits = stats.cache_hits,
                loaded = stats.loaded,
                "completed evaluator warmup for run"
            );
        }
        Err(err) => {
            let chunk = heartbeat.stop().await;
            if is_retryable_worker_database_error(&err) {
                return Err(err);
            }
            let settlement = settle_retryable_chunk_failure(
                ChunkSettlementContext::new(
                    database_router,
                    &payload.database_alias,
                    db,
                    mq,
                    message,
                    &chunk,
                ),
                &err.to_string(),
                "evaluator_warmup",
            )
            .await?;
            warn!(
                run_id = %chunk.run_id,
                chunk_id = %chunk.id,
                error = %err,
                ?settlement,
                "failed evaluator warmup; handled claimed chunk lease"
            );
            return Ok(WorkerCycleOutcome::Processed);
        }
    }

    // --- Load chunk case batch ---
    // Loading failure is retryable as long as this worker still owns the chunk
    // lease.
    let batch_result = run_context.reader.prepare(&chunk, &heartbeat.cancel).await;
    match batch_result {
        Ok(mut batches) => {
            let mut case_ids =
                Vec::with_capacity((chunk.ordinal_end - chunk.ordinal_start) as usize);
            let mut succeeded = 0usize;
            let mut failed = 0usize;

            // --- Process due case executions ---
            // The execution workflow skips cases that are terminal or waiting
            // for retry_after.
            let processed = match async {
                let mut outcomes = Vec::new();
                while let Some(cases) = batches.next(&heartbeat.cancel).await? {
                    case_ids.extend(cases.iter().map(|case| case.case_id));
                    outcomes.extend(
                        execution_processing::process_case_batch_execution(
                            &context,
                            db,
                            &payload.database_alias,
                            &chunk,
                            &attempt_lease,
                            &run_context.profile,
                            &run_context.evaluator_catalog,
                            &cases,
                        )
                        .await?,
                    );
                }
                Ok::<_, anyhow::Error>(outcomes)
            }
            .await
            {
                Ok(processed) => processed,
                Err(err) => {
                    let chunk = heartbeat.stop().await;
                    if is_retryable_worker_database_error(&err) {
                        return Err(err);
                    }
                    let settlement = settle_retryable_chunk_failure(
                        ChunkSettlementContext::new(
                            database_router,
                            &payload.database_alias,
                            db,
                            mq,
                            message,
                            &chunk,
                        ),
                        &err.to_string(),
                        "chunk_processing",
                    )
                    .await?;
                    warn!(
                        run_id = %chunk.run_id,
                        chunk_id = %chunk.id,
                        error = %err,
                        ?settlement,
                        "failed chunk case processing; handled claimed chunk lease"
                    );
                    return Ok(WorkerCycleOutcome::Processed);
                }
            };

            // --- Build terminal transition batch ---
            let mut terminal_transitions = Vec::with_capacity(processed.len());
            for processed in processed {
                let completed = processed.terminal_transition.completed;
                let failure_message = processed.terminal_transition.error_message.clone();
                terminal_transitions.push(processed.terminal_transition);

                if completed {
                    succeeded += 1;
                    debug!(
                        run_id = %chunk.run_id,
                        execution_id = %processed.execution_id,
                        attempt_id = %processed.attempt_id,
                        evaluator_result_count = processed.result_count,
                        "completed execution persistence for case"
                    );
                } else {
                    failed += 1;
                    warn!(
                        run_id = %chunk.run_id,
                        execution_id = %processed.execution_id,
                        attempt_id = %processed.attempt_id,
                        error = %failure_message.unwrap_or_else(|| "unknown failure".to_string()),
                        "case execution completed with terminal failure"
                    );
                }
            }

            // --- Apply guarded terminal transitions ---
            // Failures here are retryable worker failures because evidence may
            // have been written, but execution state was not advanced safely.
            if let Err(err) = database_router
                .run_database_operation(
                    &payload.database_alias,
                    "worker_terminal_transitions",
                    execution_processing::finalize_execution_terminal_transitions(
                        db,
                        chunk.run_id,
                        chunk.run_shard,
                        runtime.worker_id,
                        i32::try_from(run_context.profile.defaults.max_attempts)?,
                        &terminal_transitions,
                    ),
                )
                .await
            {
                let chunk = heartbeat.stop().await;
                if is_retryable_worker_database_error(&err) {
                    return Err(err);
                }
                let settlement = settle_retryable_chunk_failure(
                    ChunkSettlementContext::new(
                        database_router,
                        &payload.database_alias,
                        db,
                        mq,
                        message,
                        &chunk,
                    ),
                    &err.to_string(),
                    "terminal_transition",
                )
                .await?;
                warn!(
                    run_id = %chunk.run_id,
                    chunk_id = %chunk.id,
                    error = %err,
                    ?settlement,
                    "failed chunk-level execution terminal transitions; handled claimed chunk lease"
                );
                return Ok(WorkerCycleOutcome::Processed);
            }

            // --- Check for pending execution retries ---
            // Open retry work releases the chunk and delays the message until
            // the next retry window.
            let chunk_state = database_router
                .run_database_operation(
                    &payload.database_alias,
                    "worker_execution_state_summary",
                    execution_processing::summarize_chunk_execution_state(
                        db,
                        chunk.run_id,
                        chunk.run_shard,
                        &case_ids,
                    ),
                )
                .await?;
            if chunk_state.open_execution_count > 0 {
                let chunk = heartbeat.stop().await;
                let reason = format!(
                    "chunk has {} open executions, including {} retry-scheduled executions; next_retry_after={:?}",
                    chunk_state.open_execution_count,
                    chunk_state.retry_scheduled_count,
                    chunk_state.next_retry_after
                );
                let settlement = settle_chunk_waiting_for_execution_retry(
                    ChunkSettlementContext::new(
                        database_router,
                        &payload.database_alias,
                        db,
                        mq,
                        message,
                        &chunk,
                    ),
                    &reason,
                    chunk_state.next_retry_after,
                )
                .await?;
                info!(
                    run_id = %chunk.run_id,
                    chunk_id = %chunk.id,
                    open_execution_count = chunk_state.open_execution_count,
                    retry_scheduled_count = chunk_state.retry_scheduled_count,
                    next_retry_after = ?chunk_state.next_retry_after,
                    ?settlement,
                    "chunk still has open execution retry work; handled claimed chunk lease"
                );
                return Ok(WorkerCycleOutcome::Processed);
            }

            // --- Complete chunk and acknowledge message ---
            // All executions in the chunk are terminal, so complete the chunk
            // under the same lease token.
            let chunk = heartbeat.stop().await;
            let completed = database_router
                .run_database_operation(
                    &payload.database_alias,
                    "worker_chunk_completion",
                    chunk_processing::mark_chunk_completed_and_settle(db, &chunk),
                )
                .await?;
            if completed == 0 {
                mq.ack(&message.raw).await?;
                warn!(
                    run_id = %chunk.run_id,
                    chunk_id = %chunk.id,
                    "chunk lease was no longer owned at completion; acknowledged stale message"
                );
                return Ok(WorkerCycleOutcome::Processed);
            }
            mq.ack(&message.raw).await?;
            info!(
                run_id = %chunk.run_id,
                chunk_id = %chunk.id,
                case_count = case_ids.len(),
                cases_succeeded = succeeded,
                cases_failed = failed,
                "processed case batch from queue chunk"
            );
            debug!(
                delivery_tag = message.delivery_tag(),
                chunk_id = %chunk.id,
                "chunk processing complete; message acknowledged"
            );
            Ok(WorkerCycleOutcome::Processed)
        }
        Err(err) => {
            let chunk = heartbeat.stop().await;
            if is_retryable_worker_database_error(&err) {
                return Err(err);
            }
            let settlement = settle_retryable_chunk_failure(
                ChunkSettlementContext::new(
                    database_router,
                    &payload.database_alias,
                    db,
                    mq,
                    message,
                    &chunk,
                ),
                &err.to_string(),
                "case_batch_load",
            )
            .await?;
            warn!(
                run_id = %chunk.run_id,
                chunk_id = %chunk.id,
                error = %err,
                ?settlement,
                "failed to load chunk case batch; handled claimed chunk lease"
            );
            debug!(
                delivery_tag = message.delivery_tag(),
                chunk_id = %chunk.id,
                ?settlement,
                "chunk processing failed; handled claimed chunk lease"
            );
            Ok(WorkerCycleOutcome::Processed)
        }
    }
}

pub(crate) async fn claim_hinted_chunk_with_route_refresh(
    database_router: &crate::context::database::DatabaseRouter,
    hinted_route: &crate::context::database::ExecutionWriteRoute,
    chunk_id: Uuid,
) -> anyhow::Result<(sqlx::PgPool, Option<crate::models::run_chunk::RunChunk>)> {
    match chunk_processing::claim_hinted_chunk_for_processing(
        database_router,
        hinted_route,
        chunk_id,
        CHUNK_LEASE_SECONDS,
    )
    .await
    {
        Ok(claim) => Ok((hinted_route.pool.clone(), claim)),
        Err(error)
            if error
                .downcast_ref::<crate::db::workflows::local_shard_admission::LocalShardAdmissionError>()
                .is_some() =>
        {
            database_router
                .invalidate_execution_placement(
                    hinted_route.hint.run_id,
                    hinted_route.hint.run_shard,
                )
                .await;
            let route = database_router
                .execution_route(hinted_route.hint.run_id, hinted_route.hint.run_shard)
                .await?;
            let claim = chunk_processing::claim_routed_chunk_for_processing(
                database_router,
                &route,
                hinted_route.hint.run_id,
                hinted_route.hint.run_shard,
                chunk_id,
                CHUNK_LEASE_SECONDS,
            )
            .await?;
            Ok((route.pool, claim))
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;
    use sqlx::{
        PgPool,
        postgres::PgPoolOptions,
    };
    use uuid::Uuid;

    use super::{
        ChunkReadyMessage,
        DEFAULT_WORKER_MAX_INFLIGHT_CHUNKS,
        WORKER_ROUTE_RETRY_DELAY_SECONDS,
        contain_worker_database_error,
        execution_route_is_temporarily_blocked,
        worker_stream_prefetch,
    };
    use crate::context::{
        Context,
        database::{
            self,
            ExecutionRouteError,
        },
        output::OutputFormat,
    };

    #[test]
    fn default_worker_processes_one_chunk_at_a_time() {
        assert_eq!(DEFAULT_WORKER_MAX_INFLIGHT_CHUNKS, 1);
        assert_eq!(
            worker_stream_prefetch(DEFAULT_WORKER_MAX_INFLIGHT_CHUNKS),
            1
        );
    }

    #[test]
    fn worker_stream_prefetch_matches_inflight_chunk_capacity() {
        assert_eq!(worker_stream_prefetch(1), 1);
        assert_eq!(worker_stream_prefetch(4), 4);
        assert_eq!(worker_stream_prefetch(64), 64);
    }

    #[test]
    fn chunk_ready_message_carries_execution_route_key() {
        let run_id = Uuid::now_v7();
        let chunk_id = Uuid::now_v7();

        let message: ChunkReadyMessage = serde_json::from_value(json!({
            "run_id": run_id,
            "run_shard": 42,
            "chunk_id": chunk_id,
            "database_alias": "shard_001",
            "write_epoch": 7,
        }))
        .unwrap();

        assert_eq!(message.run_id, run_id);
        assert_eq!(message.run_shard, 42);
        assert_eq!(message.chunk_id, chunk_id);
        assert_eq!(message.database_alias, "shard_001");
        assert_eq!(message.write_epoch, 7);
    }

    #[test]
    fn non_dispatchable_execution_route_is_temporary_worker_backpressure() {
        let error = anyhow::Error::new(ExecutionRouteError::NonDispatchableShardPlacement {
            run_id: Uuid::now_v7(),
            run_shard: 7,
            status: "moving".to_string(),
        });

        assert!(execution_route_is_temporarily_blocked(&error));
    }

    #[test]
    fn missing_execution_route_is_not_temporary_worker_backpressure() {
        let error = anyhow::Error::new(ExecutionRouteError::MissingShardPlacement {
            run_id: Uuid::now_v7(),
            run_shard: 7,
        });

        assert!(!execution_route_is_temporarily_blocked(&error));
    }

    #[test]
    fn stale_write_epoch_is_temporary_worker_backpressure() {
        let error = anyhow::Error::new(
            crate::db::workflows::local_shard_admission::LocalShardAdmissionError::StaleWriteEpoch {
            run_id: Uuid::now_v7(),
            run_shard: 7,
            database_alias: "primary".to_string(),
            expected_write_epoch: 1,
            actual_write_epoch: 2,
            actual_state: "closed".to_string(),
            redirect_database_alias: Some("shard_001".to_string()),
        });

        assert!(execution_route_is_temporarily_blocked(&error));
    }

    #[tokio::test]
    async fn unavailable_database_is_contained_to_its_worker_alias() {
        let context = test_context("postgres://not-used");
        let database_router = context.dbr().await.unwrap();
        let permit = database_router
            .acquire_database_operation("shard_001")
            .unwrap();
        let error = anyhow::Error::new(sqlx::Error::PoolTimedOut);

        let disposition =
            contain_worker_database_error(database_router, permit, &error, true).unwrap();

        assert!(disposition.delay_seconds > WORKER_ROUTE_RETRY_DELAY_SECONDS);
        assert!(
            database_router
                .acquire_database_operation("shard_001")
                .is_err()
        );
        assert!(
            database_router
                .acquire_database_operation("shard_002")
                .is_ok()
        );
    }

    #[tokio::test]
    async fn non_database_error_remains_process_visible() {
        let context = test_context("postgres://not-used");
        let database_router = context.dbr().await.unwrap();
        let permit = database_router
            .acquire_database_operation("shard_001")
            .unwrap();

        assert!(
            contain_worker_database_error(
                database_router,
                permit,
                &anyhow::anyhow!("invariant failed"),
                false,
            )
            .is_none()
        );
        assert!(
            database_router
                .acquire_database_operation("shard_001")
                .is_ok()
        );
    }

    #[tokio::test]
    async fn failed_run_context_load_preserves_database_error_and_is_retryable() {
        let context = test_context("postgres://not-used");
        let loader = super::EvaluatorLoaderService::new(context);
        let unavailable = PgPoolOptions::new()
            .connect_lazy("postgres://vigilo@127.0.0.1/vigilo")
            .unwrap();
        unavailable.close().await;
        let run_id = Uuid::now_v7();

        for _ in 0..2 {
            let error = loader
                .get_or_build_run_context(
                    run_id,
                    1,
                    &unavailable,
                    "primary",
                    &tokio_util::sync::CancellationToken::new(),
                )
                .await
                .unwrap_err();

            assert!(database::is_database_unavailable(&error));
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for worker database containment tests"]
    async fn failed_worker_database_does_not_block_healthy_database(pool: PgPool) {
        let database_name = sqlx::query_scalar::<_, String>("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .unwrap();
        let mut database_url = url::Url::parse(&std::env::var("DATABASE_URL").unwrap()).unwrap();
        database_url.set_path(&database_name);
        let context = test_context(database_url.as_str());
        let database_router = context.dbr().await.unwrap();
        let unavailable = PgPoolOptions::new()
            .connect_lazy("postgres://vigilo@127.0.0.1/vigilo")
            .unwrap();
        unavailable.close().await;
        let error = sqlx::query("SELECT 1")
            .execute(&unavailable)
            .await
            .unwrap_err();
        let permit = database_router
            .acquire_database_operation("shard_001")
            .unwrap();

        assert!(
            contain_worker_database_error(
                database_router,
                permit,
                &anyhow::Error::new(error),
                false,
            )
            .is_some()
        );
        assert_eq!(
            sqlx::query_scalar::<_, i32>("SELECT 1")
                .fetch_one(database_router.control().await.unwrap())
                .await
                .unwrap(),
            1
        );
    }

    fn test_context(database_url: &str) -> Context {
        Context::new(
            database::Config::new(
                database_url.to_string(),
                1,
                Duration::from_secs(1),
                database::CircuitBreakerConfig::new(
                    true,
                    1,
                    Duration::from_secs(10),
                    Duration::from_secs(10),
                )
                .unwrap(),
                database::PlacementConfig::default_single_database(),
            )
            .with_operation_timeout(Some(
                database::DatabaseOperationTimeoutConfig::new(Duration::from_secs(1)).unwrap(),
            )),
            Some(crate::mq::Config::new("amqp://not-used".to_string())),
            crate::context::wasm::Config::default(),
            OutputFormat::Json,
        )
    }
}
