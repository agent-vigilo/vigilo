//! Coordinator process command.
//!
//! The coordinator drives run-level orchestration:
//! - resumes durable multi-database run creation
//! - atomically starts fully created runs and dispatches bounded chunk windows
//! - aggregates chunk receipts and finalizes runs with exact sealed shard sets
//! - drains aggregation pages within independent budgets, with bounded shard and
//!   database concurrency while retaining transaction-local ownership
//! - publishes outbox event records from active database placements as messages
//! - contains placement-scoped database failures so healthy aliases continue

use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
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
        RunCreationOptions,
    },
};
use crate::{
    context::{
        Context,
        database,
    },
    db::{
        tables::outbox_events,
        workflows::{
            run_creation,
            run_dispatch,
            run_finalize,
            run_shard_summary,
        },
    },
    outbox::{
        EventPublisher,
        MqEventPublisher,
        OutboxPublishStats,
        OutboxPublisherConfig,
        publish_pending_events,
    },
    runtime::ServiceRunner,
};

mod once;
mod start;

use placement::{
    PlacementFailureStats,
    PlacementOperationFailure,
    PlacementPassResult,
    record_failure as record_placement_failure,
    record_open as record_placement_open,
    record_skip as record_placement_skip,
    run_operations as run_placement_operations,
    run_operations_concurrently,
    run_operations_without_outer_deadline,
};

const COORDINATOR_TICK_SECONDS: u64 = 5;
const COORDINATOR_LEASE_SECONDS: i32 = 60;
const COORDINATOR_MAX_CREATE_RECOVERY_PER_CYCLE: u64 = 16;
const COORDINATOR_MAX_DISPATCH_PER_CYCLE: u64 = 64;
const COORDINATOR_MAX_FINALIZE_PER_CYCLE: u64 = 64;
const COORDINATOR_MAX_AGGREGATION_PAGES_PER_CYCLE: u64 = 256;
const COORDINATOR_AGGREGATION_TIME_BUDGET_MS: u64 = 1_000;
const COORDINATOR_AGGREGATION_PARALLELISM: u64 = 4;
const COORDINATOR_AGGREGATION_DATABASE_PARALLELISM: u64 = 4;
const RUN_CHUNK_DISPATCH_WINDOW_SIZE: i64 = 512;
const CHUNK_LEASE_RECOVERY_BATCH_SIZE: i64 = 1_000;
const CHUNK_LEASE_MAX_RECOVERIES: i32 = 3;
const OUTBOX_BATCH_SIZE: i64 = 1_000;
const OUTBOX_PUBLISH_PARALLELISM: u64 = 64;
const OUTBOX_LEASE_SECONDS: i32 = 60;
const OUTBOX_RETRY_DELAY_SECONDS: i32 = 10;

#[derive(Debug, Clone)]
struct CoordinatorRuntimeConfig {
    run_creation: run_creation::Config,
    tick_seconds: u64,
    lease_seconds: i32,
    max_create_recovery_per_cycle: usize,
    max_dispatch_per_cycle: usize,
    max_finalize_per_cycle: usize,
    aggregation: run_shard_summary::AggregationConfig,
    aggregation_database_parallelism: NonZeroUsize,
    run_chunk_dispatch_window_size: i64,
    chunk_lease_recovery_batch_size: i64,
    chunk_lease_max_recoveries: i32,
    outbox_batch_size: i64,
    outbox_publish_parallelism: usize,
    outbox_lease_seconds: i32,
    outbox_retry_delay_seconds: i32,
}

#[derive(Debug, Subcommand)]
/// Coordinator execution modes.
pub(crate) enum SubCommand {
    /// Start a coordinator process
    Start,

    /// Run one coordinator cycle and exit
    Once,
}

#[derive(Debug, Args)]
/// Arguments for `vigilo coordinator`.
///
/// The command requires a subcommand:
/// - `start` runs the loop continuously
/// - `once` executes one orchestration cycle and exits
pub(crate) struct Command {
    #[command(flatten)]
    pub(crate) messaging: MessagingOptions,

    #[command(flatten)]
    pub(crate) circuit_breaker: CircuitBreakerOptions,

    #[command(flatten)]
    pub(crate) database_operation_timeout: DatabaseOperationTimeoutOptions,

    #[command(flatten)]
    pub(crate) run_creation: RunCreationOptions,

    /// Seconds between coordinator cycles in start mode
    #[arg(long, env = "VIGILO_COORDINATOR_TICK_SECONDS", default_value_t = COORDINATOR_TICK_SECONDS, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pub tick_seconds: u64,

    /// Coordinator lease duration for creation recovery, dispatch, and finalization
    #[arg(long, env = "VIGILO_COORDINATOR_LEASE_SECONDS", default_value_t = COORDINATOR_LEASE_SECONDS, value_parser = clap::value_parser!(i32).range(1..=86400))]
    pub lease_seconds: i32,

    /// Maximum incomplete run creations recovered per coordinator cycle
    #[arg(long, env = "VIGILO_COORDINATOR_MAX_CREATE_RECOVERY_PER_CYCLE", default_value_t = COORDINATOR_MAX_CREATE_RECOVERY_PER_CYCLE, value_parser = clap::value_parser!(u64).range(1..=100_000))]
    pub max_create_recovery_per_cycle: u64,

    /// Maximum run-shard dispatch windows prepared per coordinator cycle
    #[arg(long, env = "VIGILO_COORDINATOR_MAX_DISPATCH_PER_CYCLE", default_value_t = COORDINATOR_MAX_DISPATCH_PER_CYCLE, value_parser = clap::value_parser!(u64).range(1..=100_000))]
    pub max_dispatch_per_cycle: u64,

    /// Maximum finalization candidates inspected per coordinator cycle
    #[arg(long, env = "VIGILO_COORDINATOR_MAX_FINALIZE_PER_CYCLE", default_value_t = COORDINATOR_MAX_FINALIZE_PER_CYCLE, value_parser = clap::value_parser!(u64).range(1..=100_000))]
    pub max_finalize_per_cycle: u64,

    /// Maximum aggregation page attempts per execution database per cycle
    #[arg(long, env = "VIGILO_COORDINATOR_MAX_AGGREGATION_PAGES_PER_CYCLE", default_value_t = COORDINATOR_MAX_AGGREGATION_PAGES_PER_CYCLE, value_parser = clap::value_parser!(u64).range(1..=100_000))]
    pub max_aggregation_pages_per_cycle: u64,

    /// Milliseconds to admit aggregation pages per database after candidate discovery
    #[arg(long, env = "VIGILO_COORDINATOR_AGGREGATION_TIME_BUDGET_MS", default_value_t = COORDINATOR_AGGREGATION_TIME_BUDGET_MS, value_parser = clap::value_parser!(u64).range(1..=60_000))]
    pub aggregation_time_budget_ms: u64,

    /// Maximum concurrent aggregation pages on independent shards per database
    #[arg(long, env = "VIGILO_COORDINATOR_AGGREGATION_PARALLELISM", default_value_t = COORDINATOR_AGGREGATION_PARALLELISM, value_parser = clap::value_parser!(u64).range(1..=128))]
    pub aggregation_parallelism: u64,

    /// Maximum execution databases aggregating concurrently per cycle
    #[arg(long, env = "VIGILO_COORDINATOR_AGGREGATION_DATABASE_PARALLELISM", default_value_t = COORDINATOR_AGGREGATION_DATABASE_PARALLELISM, value_parser = clap::value_parser!(u64).range(1..=128))]
    pub aggregation_database_parallelism: u64,

    /// Number of run chunks made ready per run-shard dispatch window
    #[arg(long, env = "VIGILO_RUN_CHUNK_DISPATCH_WINDOW_SIZE", default_value_t = RUN_CHUNK_DISPATCH_WINDOW_SIZE, value_parser = clap::value_parser!(i64).range(1..=1_000_000))]
    pub run_chunk_dispatch_window_size: i64,

    /// Maximum expired chunk leases recovered per coordinator cycle
    #[arg(long, env = "VIGILO_CHUNK_LEASE_RECOVERY_BATCH_SIZE", default_value_t = CHUNK_LEASE_RECOVERY_BATCH_SIZE, value_parser = clap::value_parser!(i64).range(1..=1_000_000))]
    pub chunk_lease_recovery_batch_size: i64,

    /// Maximum times an expired chunk lease can be recovered before the chunk fails
    #[arg(long, env = "VIGILO_CHUNK_LEASE_MAX_RECOVERIES", default_value_t = CHUNK_LEASE_MAX_RECOVERIES, value_parser = clap::value_parser!(i32).range(0..=10_000))]
    pub chunk_lease_max_recoveries: i32,

    /// Maximum outbox event records claimed per placement per publish pass
    #[arg(long, env = "VIGILO_OUTBOX_BATCH_SIZE", default_value_t = OUTBOX_BATCH_SIZE, value_parser = clap::value_parser!(i64).range(1..=1_000_000))]
    pub outbox_batch_size: i64,

    /// Maximum concurrent outbox broker publishes per publish pass
    #[arg(long, env = "VIGILO_OUTBOX_PUBLISH_PARALLELISM", default_value_t = OUTBOX_PUBLISH_PARALLELISM, value_parser = clap::value_parser!(u64).range(1..=10_000))]
    pub outbox_publish_parallelism: u64,

    /// Outbox publish lease duration
    #[arg(long, env = "VIGILO_OUTBOX_LEASE_SECONDS", default_value_t = OUTBOX_LEASE_SECONDS, value_parser = clap::value_parser!(i32).range(1..=86400))]
    pub outbox_lease_seconds: i32,

    /// Delay before a failed outbox publish can be retried
    #[arg(long, env = "VIGILO_OUTBOX_RETRY_DELAY_SECONDS", default_value_t = OUTBOX_RETRY_DELAY_SECONDS, value_parser = clap::value_parser!(i32).range(1..=86400))]
    pub outbox_retry_delay_seconds: i32,

    #[command(subcommand)]
    pub command: Option<SubCommand>,
}

impl Command {
    fn runtime_config(&self) -> CoordinatorRuntimeConfig {
        CoordinatorRuntimeConfig {
            run_creation: self.run_creation.config(),
            tick_seconds: self.tick_seconds,
            lease_seconds: self.lease_seconds,
            max_create_recovery_per_cycle: self.max_create_recovery_per_cycle as usize,
            max_dispatch_per_cycle: self.max_dispatch_per_cycle as usize,
            max_finalize_per_cycle: self.max_finalize_per_cycle as usize,
            aggregation: run_shard_summary::AggregationConfig {
                max_pages: self.max_aggregation_pages_per_cycle as usize,
                time_budget: Duration::from_millis(self.aggregation_time_budget_ms),
                parallelism: self.aggregation_parallelism as usize,
            },
            aggregation_database_parallelism: NonZeroUsize::new(
                self.aggregation_database_parallelism as usize,
            )
            .expect("CLI validates nonzero aggregation database parallelism"),
            run_chunk_dispatch_window_size: self.run_chunk_dispatch_window_size,
            chunk_lease_recovery_batch_size: self.chunk_lease_recovery_batch_size,
            chunk_lease_max_recoveries: self.chunk_lease_max_recoveries,
            outbox_batch_size: self.outbox_batch_size,
            outbox_publish_parallelism: self.outbox_publish_parallelism as usize,
            outbox_lease_seconds: self.outbox_lease_seconds,
            outbox_retry_delay_seconds: self.outbox_retry_delay_seconds,
        }
    }
}

#[async_trait]
impl Executable for Command {
    /// Executes the selected coordinator mode.
    async fn exec(self, context: Context) -> anyhow::Result<()> {
        let config = self.runtime_config();
        match self.command {
            Some(SubCommand::Start) => {
                info!("starting coordinator process");
                start::exec(context, config).await
            }
            Some(SubCommand::Once) => {
                info!("running single coordinator cycle");
                once::exec(context, config).await
            }
            None => anyhow::bail!("missing coordinator subcommand; use `vigilo coordinator start`"),
        }
    }
}

/// Executes one full coordinator cycle.
///
/// The cycle is intentionally ordered to keep run progression deterministic:
/// 1. resume stale multi-database run creation
/// 2. recover expired worker chunk leases
/// 3. atomically start pending runs and dispatch bounded chunk windows
/// 4. aggregate bounded receipt pages, then finalize exact sealed shard sets
/// 5. publish bounded batches of pending outbox records from serviceable placements
async fn run_coordinator_cycle(
    context: Context,
    coordinator_id: Uuid,
    config: &CoordinatorRuntimeConfig,
) -> anyhow::Result<()> {
    let cycle_started = Instant::now();
    // --- Acquire cycle services ---
    // The database router owns control and execution placement routing. The
    // queue handle is acquired only after database-only recovery and dispatch.
    debug!(coordinator_id = %coordinator_id, "starting coordinator cycle pre-flight");

    debug!(coordinator_id = %coordinator_id, "acquiring database router");
    let database_router = context.dbr().await?;
    debug!(coordinator_id = %coordinator_id, "database router ready");

    // --- Resume incomplete run creation ---
    let creation_recovery_started = Instant::now();
    let creation_recovery = crate::run_admission::recover(
        &context,
        config.max_create_recovery_per_cycle,
        config.run_creation,
    )
    .await?;
    let creation_recovery_ms = creation_recovery_started.elapsed().as_millis() as u64;

    // --- Recover expired chunk leases ---
    // Recovery runs before new dispatch so dead workers do not block
    // finalization or leave ready work stranded.
    let recovery_started = Instant::now();
    let recovery = recover_expired_chunk_leases(database_router, coordinator_id, config).await?;
    let recovery_ms = recovery_started.elapsed().as_millis() as u64;

    // --- Dispatch runnable chunk windows ---
    // Dispatch runs before finalization so newly-created work is surfaced
    // promptly.
    let dispatch_started = Instant::now();
    let dispatch = dispatch_ready_chunk_windows(database_router, coordinator_id, config).await?;
    let dispatch_ms = dispatch_started.elapsed().as_millis() as u64;

    let aggregation = aggregate_pending_summaries(database_router, coordinator_id, config).await?;

    // --- Finalize terminal runs ---
    let finalization_started = Instant::now();
    let finalization = finalize_ready_runs(database_router, coordinator_id, config).await?;
    let finalization_ms = finalization_started.elapsed().as_millis() as u64;

    // --- Publish durable outbox event records ---
    // Failed broker publishes stay in the outbox delivery queue for retry.
    debug!(coordinator_id = %coordinator_id, "acquiring messaging context");
    let mq = context.mq().await?;
    debug!(coordinator_id = %coordinator_id, "messaging context ready");
    debug!(coordinator_id = %coordinator_id, "starting outbox publish pass");
    let publisher = MqEventPublisher::new(mq);
    let outbox_config = OutboxPublisherConfig {
        batch_size: config.outbox_batch_size,
        publish_parallelism: config.outbox_publish_parallelism,
        lease_seconds: config.outbox_lease_seconds,
        retry_delay_seconds: config.outbox_retry_delay_seconds,
    };
    let outbox_started = Instant::now();
    let publication =
        publish_outbox_events(database_router, &publisher, &outbox_config, coordinator_id).await?;
    let outbox_publish_ms = outbox_started.elapsed().as_millis() as u64;

    let mut placement_failures = PlacementFailureStats::default();
    placement_failures.merge(recovery.failures);
    placement_failures.merge(dispatch.failures);
    placement_failures.merge(aggregation.failures);
    placement_failures.merge(finalization.failures);
    placement_failures.merge(publication.failures);
    info!(
        run_creations_claimed = creation_recovery.claimed_runs,
        run_creations_completed = creation_recovery.completed_runs,
        run_creations_deferred = creation_recovery.deferred_runs,
        run_creations_failed = creation_recovery.failed_runs,
        creation_recovery_ms,
        expired_chunk_leases_recovered = recovery.output.recovered_chunks,
        expired_chunk_leases_failed = recovery.output.failed_chunks,
        recovery_ms,
        dispatch_windows_prepared = dispatch.output,
        dispatch_ms,
        aggregation_pages_advanced = aggregation.output,
        runs_finalized = finalization.output,
        finalization_ms,
        outbox_events_claimed = publication.output.claimed_events,
        outbox_events_published = publication.output.published_events,
        outbox_events_failed = publication.output.failed_events,
        outbox_stale_claims = publication.output.stale_event_claims,
        outbox_publish_ms,
        skipped_placements = placement_failures.skipped_placement_count(),
        failed_placement_operations = placement_failures.failed_operation_count(),
        retryable_placement_errors = placement_failures.retryable_error_count(),
        terminal_placement_errors = placement_failures.terminal_error_count(),
        coordinator_cycle_ms = cycle_started.elapsed().as_millis() as u64,
        "completed coordinator cycle"
    );

    debug!(coordinator_id = %coordinator_id, "coordinator cycle complete");

    Ok(())
}

fn log_database_operation_success(
    database_router: &database::DatabaseRouter,
    coordinator_id: Uuid,
    operation: &'static str,
    permit: database::CircuitPermit,
    database_alias: &str,
) {
    if matches!(
        database_router.record_database_operation_success(permit),
        Some(database::CircuitTransition::Closed)
    ) {
        info!(
            coordinator_id = %coordinator_id,
            database_alias,
            operation,
            "closed execution database circuit after successful probe"
        );
    }
}

fn log_database_operation_error(
    database_router: &database::DatabaseRouter,
    coordinator_id: Uuid,
    operation: &'static str,
    permit: database::CircuitPermit,
    database_alias: &str,
    error: &anyhow::Error,
) {
    let (_, transition) = database_router.record_database_operation_error(permit, error);
    if let Some(database::CircuitTransition::Opened { retry_after }) = transition {
        warn!(
            coordinator_id = %coordinator_id,
            database_alias,
            operation,
            retry_after_ms = retry_after.as_millis() as u64,
            "opened execution database circuit after availability failures"
        );
    }
}

async fn recover_expired_chunk_leases(
    database_router: &database::DatabaseRouter,
    coordinator_id: Uuid,
    config: &CoordinatorRuntimeConfig,
) -> anyhow::Result<PlacementPassResult<run_dispatch::ChunkLeaseRecoveryStats>> {
    // --- Expired lease recovery pass ---
    // The workflow returns both recovered chunks and chunks failed after
    // exhausting recovery attempts.
    debug!(coordinator_id = %coordinator_id, "recovering expired chunk leases");

    let alias_list_started = Instant::now();
    let aliases = database_router
        .serviceable_execution_database_aliases()
        .await?;
    debug!(
        coordinator_id = %coordinator_id,
        serviceable_execution_placement_count = aliases.len(),
        active_execution_alias_list_ms = alias_list_started.elapsed().as_millis() as u64,
        "listed active execution placements for recovery"
    );
    let batch = run_placement_operations(
        database_router,
        coordinator_id,
        "chunk_lease_recovery",
        aliases,
        |alias| async move {
            let db = database_router.execution_database(&alias).await?;
            let recovery_started = Instant::now();
            let stats = run_dispatch::recover_expired_chunk_leases(
                &db,
                config.chunk_lease_max_recoveries,
                config.chunk_lease_recovery_batch_size,
            )
            .await?;
            Ok((stats, recovery_started.elapsed().as_millis() as u64))
        },
    )
    .await;

    let mut stats = run_dispatch::ChunkLeaseRecoveryStats::default();
    let mut failures = PlacementFailureStats::default();
    for skip in batch.skipped {
        record_placement_skip(&mut failures, coordinator_id, "chunk_lease_recovery", skip);
    }
    for (alias, (alias_stats, recovery_ms)) in batch.successes {
        stats.recovered_chunks += alias_stats.recovered_chunks;
        stats.failed_chunks += alias_stats.failed_chunks;

        if alias_stats.recovered_chunks > 0 || alias_stats.failed_chunks > 0 {
            info!(
                coordinator_id = %coordinator_id,
                database_alias = %alias,
                expired_chunk_leases_recovered = alias_stats.recovered_chunks,
                expired_chunk_leases_failed = alias_stats.failed_chunks,
                recovery_ms,
                "completed expired chunk lease recovery pass for execution placement"
            );
        } else {
            debug!(
                coordinator_id = %coordinator_id,
                database_alias = %alias,
                recovery_ms,
                "no expired chunk leases recovered for execution placement"
            );
        }
    }

    for failure in batch.failures {
        record_placement_failure(
            &mut failures,
            coordinator_id,
            "chunk_lease_recovery",
            failure,
        );
    }

    if stats.recovered_chunks == 0 && stats.failed_chunks == 0 {
        debug!(
            coordinator_id = %coordinator_id,
            "no expired chunk leases recovered for this cycle"
        );
    } else {
        info!(
            coordinator_id = %coordinator_id,
            expired_chunk_leases_recovered = stats.recovered_chunks,
            expired_chunk_leases_failed = stats.failed_chunks,
            "completed expired chunk lease recovery pass"
        );
    }

    Ok(PlacementPassResult {
        output: stats,
        failures,
    })
}

/// Drains placement-local summaries within independent aggregation budgets.
///
/// Database slots bound concurrent drains; each drain independently bounds shard
/// pages. Every admitted placement outcome is recorded before finalization begins,
/// including failures or expiration of the existing database operation deadline.
async fn aggregate_pending_summaries(
    database_router: &database::DatabaseRouter,
    coordinator_id: Uuid,
    config: &CoordinatorRuntimeConfig,
) -> anyhow::Result<PlacementPassResult<usize>> {
    let aliases = database_router
        .serviceable_execution_database_aliases()
        .await?;
    let batch = run_operations_concurrently(
        database_router,
        coordinator_id,
        "shard_summary_aggregation",
        aliases,
        config.aggregation_database_parallelism,
        |alias| async move {
            let db = database_router.execution_database(&alias).await?;
            run_shard_summary::advance_pending_summaries(&db, &config.aggregation).await
        },
    )
    .await;
    let mut failures = PlacementFailureStats::default();
    for skip in batch.skipped {
        record_placement_skip(
            &mut failures,
            coordinator_id,
            "shard_summary_aggregation",
            skip,
        );
    }
    for failure in batch.failures {
        record_placement_failure(
            &mut failures,
            coordinator_id,
            "shard_summary_aggregation",
            failure,
        );
    }
    Ok(PlacementPassResult {
        output: batch.successes.into_iter().map(|(_, n)| n).sum(),
        failures,
    })
}

async fn dispatch_ready_chunk_windows(
    database_router: &database::DatabaseRouter,
    coordinator_id: Uuid,
    config: &CoordinatorRuntimeConfig,
) -> anyhow::Result<PlacementPassResult<usize>> {
    // --- Dispatch pass ---
    // Repeatedly claim one dispatchable run/window until the cycle limit is
    // reached or no pending dispatch work remains.
    debug!(coordinator_id = %coordinator_id, "dispatching ready run-shard windows");

    let mut dispatched = 0usize;
    let mut dispatched_by_alias = BTreeMap::<String, usize>::new();
    let mut failures = PlacementFailureStats::default();
    let control_db = database_router.control().await?;
    // A failed alias is excluded after one attempt, so this allowance keeps
    // placement failures from reducing the successful-window budget.
    let placement_failure_allowance = database_router
        .serviceable_execution_database_aliases()
        .await?
        .len();
    let dispatch_attempt_limit = config
        .max_dispatch_per_cycle
        .saturating_add(placement_failure_allowance);
    let dispatch_backlog = run_dispatch::count_dispatch_cursor_backlog(control_db).await?;
    info!(
        coordinator_id = %coordinator_id,
        dispatch_cursor_backlog = dispatch_backlog,
        dispatch_cycle_limit = config.max_dispatch_per_cycle,
        dispatch_attempt_limit,
        "measured dispatch cursor backlog"
    );
    for _ in 0..dispatch_attempt_limit {
        if dispatched >= config.max_dispatch_per_cycle {
            break;
        }
        let select_started = Instant::now();
        let excluded_aliases = failures.excluded_aliases();
        let Some(mut dispatch_claim) =
            run_dispatch::claim_next_dispatch_route(control_db, &excluded_aliases).await?
        else {
            break;
        };
        let route = dispatch_claim.route.clone();
        let dispatch_route_select_ms = select_started.elapsed().as_millis() as u64;
        let database_alias = route.database_alias.clone();

        debug!(
            coordinator_id = %coordinator_id,
            run_id = %route.run_id,
            run_shard = route.run_shard,
            database_alias = %database_alias,
            placement_status = %route.placement_status,
            dispatch_route_select_ms,
            routing_decision = "selected_dispatch_route",
            "selected dispatch route"
        );

        let Some(snapshot) = run_dispatch::prepare_dispatch_run_snapshot_with(
            &mut dispatch_claim.control_tx,
            &route,
            coordinator_id,
            config.lease_seconds,
        )
        .await?
        else {
            dispatch_claim.control_tx.rollback().await?;
            continue;
        };

        let database_permit = match database_router.acquire_database_operation(&database_alias) {
            Ok(permit) => permit,
            Err(open) => {
                dispatch_claim.control_tx.rollback().await?;
                record_placement_open(
                    &mut failures,
                    coordinator_id,
                    "dispatch_execution_write",
                    database_alias,
                    open,
                );
                continue;
            }
        };
        if database_permit.is_probe() {
            info!(
                coordinator_id = %coordinator_id,
                database_alias = %database_alias,
                operation = "dispatch_execution_write",
                "probing half-open execution database circuit"
            );
        }

        let execution_pool_started = Instant::now();
        let execution_route_result = database_router
            .deadline_database_operation(
                &database_alias,
                "dispatch_execution_pool",
                database_router.execution_route(route.run_id, route.run_shard),
            )
            .await
            .map_err(anyhow::Error::new)
            .and_then(std::convert::identity);
        let execution_route = match execution_route_result {
            Ok(route) => route,
            Err(error) => {
                dispatch_claim.control_tx.rollback().await?;
                log_database_operation_error(
                    database_router,
                    coordinator_id,
                    "dispatch_execution_pool",
                    database_permit,
                    &database_alias,
                    &error,
                );
                record_placement_failure(
                    &mut failures,
                    coordinator_id,
                    "dispatch_execution_pool",
                    PlacementOperationFailure::new(database_alias, error),
                );
                continue;
            }
        };
        if execution_route.placement.database_alias != route.database_alias
            || execution_route.placement.route_version != route.route_version
        {
            debug!(
                run_id = %route.run_id,
                run_shard = route.run_shard,
                selected_database_alias = %route.database_alias,
                selected_route_version = route.route_version,
                current_database_alias = %execution_route.placement.database_alias,
                current_route_version = execution_route.placement.route_version,
                "dispatch route changed before execution pool resolution"
            );
            dispatch_claim.control_tx.rollback().await?;
            log_database_operation_success(
                database_router,
                coordinator_id,
                "dispatch_execution_pool",
                database_permit,
                &database_alias,
            );
            database_router
                .invalidate_execution_placement(route.run_id, route.run_shard)
                .await;
            continue;
        }
        let execution_pool_resolution_ms = execution_pool_started.elapsed().as_millis() as u64;
        let dispatch_started = Instant::now();
        let dispatch_result = match database_router
            .deadline_database_operation(
                &database_alias,
                "dispatch_execution_write",
                run_dispatch::dispatch_admitted_run_window(
                    database_router,
                    dispatch_claim.control_tx,
                    &execution_route,
                    config.run_chunk_dispatch_window_size,
                    &route,
                    &snapshot,
                ),
            )
            .await
        {
            Ok(result) => result,
            Err(error) => Err(run_dispatch::RoutedDispatchError::ExecutionWrite(
                error.into(),
            )),
        };
        let run = match dispatch_result {
            Ok(Some(run)) => {
                log_database_operation_success(
                    database_router,
                    coordinator_id,
                    "dispatch_execution_write",
                    database_permit,
                    &database_alias,
                );
                run
            }
            Ok(None) => {
                log_database_operation_success(
                    database_router,
                    coordinator_id,
                    "dispatch_execution_write",
                    database_permit,
                    &database_alias,
                );
                continue;
            }
            Err(run_dispatch::RoutedDispatchError::ExecutionWrite(error)) => {
                log_database_operation_error(
                    database_router,
                    coordinator_id,
                    "dispatch_execution_write",
                    database_permit,
                    &database_alias,
                    &error,
                );
                record_placement_failure(
                    &mut failures,
                    coordinator_id,
                    "dispatch_execution_write",
                    PlacementOperationFailure::new(database_alias, error),
                );
                continue;
            }
            Err(error) => {
                log_database_operation_success(
                    database_router,
                    coordinator_id,
                    "dispatch_execution_write",
                    database_permit,
                    &database_alias,
                );
                return Err(error.into());
            }
        };
        let dispatch_window_ms = dispatch_started.elapsed().as_millis() as u64;

        dispatched += 1;
        *dispatched_by_alias
            .entry(database_alias.clone())
            .or_default() += 1;
        debug!(
            run_id = %run.id,
            run_key = %run.run_key,
            run_shard = run.run_shard,
            database_alias = %database_alias,
            routing_decision = "dispatch_window_claimed",
            "claimed dispatchable run shard window"
        );
        info!(
            run_id = %run.id,
            run_key = %run.run_key,
            run_shard = run.run_shard,
            database_alias = %database_alias,
            chunk_ready_event_records_inserted = run.chunk_ready_event_records_inserted,
            chunks_marked_dispatched = run.chunks_marked_dispatched,
            run_started_event_records_inserted = run.run_started_event_records_inserted,
            shard_summary_initialized = true,
            execution_pool_resolution_ms,
            dispatch_window_ms,
            "prepared bounded dispatch shard window"
        );
    }

    if dispatched == 0 {
        info!("no dispatchable run-shard windows available for coordinator cycle");
    } else {
        info!(
            coordinator_id = %coordinator_id,
            dispatch_windows_prepared = dispatched,
            "completed coordinator dispatch pass"
        );
        for (alias, alias_dispatched) in dispatched_by_alias {
            info!(
                coordinator_id = %coordinator_id,
                database_alias = %alias,
                dispatch_windows_prepared = alias_dispatched,
                "completed coordinator dispatch pass for execution placement"
            );
        }
    }

    Ok(PlacementPassResult {
        output: dispatched,
        failures,
    })
}

async fn publish_outbox_events(
    database_router: &database::DatabaseRouter,
    publisher: &dyn EventPublisher,
    config: &OutboxPublisherConfig,
    coordinator_id: Uuid,
) -> anyhow::Result<PlacementPassResult<OutboxPublishStats>> {
    let alias_list_started = Instant::now();
    let aliases = database_router
        .serviceable_outbox_database_aliases()
        .await?;
    debug!(
        coordinator_id = %coordinator_id,
        serviceable_outbox_placement_count = aliases.len(),
        active_outbox_alias_list_ms = alias_list_started.elapsed().as_millis() as u64,
        "listed active outbox placements"
    );
    let batch = run_operations_without_outer_deadline(
        database_router,
        coordinator_id,
        "outbox_publication",
        aliases,
        |alias| async move {
            let db = database_router.placement(&alias).await?;
            let backlog_started = Instant::now();
            let outbox_backlog = outbox_events::count_publishable_outbox_backlog(&db).await?;
            let outbox_backlog_query_ms = backlog_started.elapsed().as_millis() as u64;
            let publish_started = Instant::now();
            let alias_stats = publish_pending_events(&db, publisher, config).await?;
            let outbox_publish_ms = publish_started.elapsed().as_millis() as u64;
            Ok((
                alias_stats,
                outbox_backlog,
                outbox_backlog_query_ms,
                outbox_publish_ms,
            ))
        },
    )
    .await;

    let mut stats = OutboxPublishStats::default();
    for (alias, (alias_stats, outbox_backlog, outbox_backlog_query_ms, outbox_publish_ms)) in
        batch.successes
    {
        stats.claimed_events += alias_stats.claimed_events;
        stats.published_events += alias_stats.published_events;
        stats.failed_events += alias_stats.failed_events;
        stats.stale_event_claims += alias_stats.stale_event_claims;

        if alias_stats.claimed_events > 0 {
            info!(
                coordinator_id = %coordinator_id,
                database_alias = %alias,
                outbox_backlog,
                outbox_backlog_query_ms,
                outbox_events_claimed = alias_stats.claimed_events,
                outbox_events_published = alias_stats.published_events,
                outbox_events_failed = alias_stats.failed_events,
                outbox_stale_claims = alias_stats.stale_event_claims,
                outbox_publish_ms,
                "completed outbox publish pass for database placement"
            );
        } else {
            debug!(
                coordinator_id = %coordinator_id,
                database_alias = %alias,
                outbox_backlog,
                outbox_backlog_query_ms,
                outbox_publish_ms,
                "no publishable outbox events for database placement"
            );
        }
    }

    let mut failures = PlacementFailureStats::default();
    for skip in batch.skipped {
        record_placement_skip(&mut failures, coordinator_id, "outbox_publication", skip);
    }
    for failure in batch.failures {
        record_placement_failure(&mut failures, coordinator_id, "outbox_publication", failure);
    }

    Ok(PlacementPassResult {
        output: stats,
        failures,
    })
}

async fn finalize_ready_runs(
    database_router: &database::DatabaseRouter,
    coordinator_id: Uuid,
    config: &CoordinatorRuntimeConfig,
) -> anyhow::Result<PlacementPassResult<usize>> {
    // --- Finalization pass ---
    // Inspect candidates until the cycle limit is reached or none remain.
    // Blocked candidates rotate behind unchecked work for later cycles.
    debug!(coordinator_id = %coordinator_id, "finalizing ready runs");

    let mut finalized = 0usize;
    let mut failures = PlacementFailureStats::default();
    let control_db = database_router.control().await?;
    let finalization_backlog =
        run_finalize::select_finalization_candidate_backlog(control_db).await?;
    info!(
        coordinator_id = %coordinator_id,
        finalization_candidate_backlog = finalization_backlog.candidate_count,
        finalization_oldest_candidate_lag_seconds =
            finalization_backlog.oldest_candidate_lag_seconds,
        finalization_cycle_limit = config.max_finalize_per_cycle,
        "measured finalization candidate backlog"
    );
    let mut checked_run_ids = Vec::with_capacity(config.max_finalize_per_cycle);
    for _ in 0..config.max_finalize_per_cycle {
        let select_started = Instant::now();
        let Some(run) =
            run_finalize::select_next_finalization_candidate(control_db, &checked_run_ids).await?
        else {
            break;
        };
        let finalization_candidate_select_ms = select_started.elapsed().as_millis() as u64;
        checked_run_ids.push(run.id);

        let collection =
            collect_run_shard_summaries(database_router, coordinator_id, run.id).await?;
        let placement_failure_count = collection.failures.failed_operation_count();
        failures.merge(collection.failures);
        let summaries = collection.output.summaries;
        if !collection.output.complete
            || summaries.is_empty()
            || summaries.iter().any(|summary| !summary.is_terminal())
        {
            debug!(
                run_id = %run.id,
                run_key = %run.run_key,
                shard_summary_count = summaries.len(),
                failed_placement_operations = placement_failure_count,
                finalization_candidate_select_ms,
                "finalization candidate is waiting for sealed shard summaries"
            );
            run_finalize::mark_finalization_candidate_checked(control_db, run.id).await?;
            continue;
        }

        if let Err(error) =
            run_finalize::validate_summary_set(run.id, &summaries, &run.aggregation_policy_hash)
        {
            warn!(run_id = %run.id, error = %error, "invalid finalization summary; deferred this candidate");
            run_finalize::mark_finalization_candidate_checked(control_db, run.id).await?;
            continue;
        }

        let Some(claimed) = run_finalize::claim_finalization_candidate(
            control_db,
            run.id,
            coordinator_id,
            config.lease_seconds,
        )
        .await?
        else {
            continue;
        };

        debug!(run_id = %claimed.id, run_key = %claimed.run_key, "claimed run for finalization");
        let finalize_started = Instant::now();
        if let Some(done) = run_finalize::finalize_claimed_run_from_summaries(
            control_db,
            claimed.id,
            coordinator_id,
            &claimed.aggregation_policy_hash,
            &summaries,
        )
        .await?
        {
            finalized += 1;
            info!(
                run_id = %done.id,
                run_key = %done.run_key,
                gate_status = %done.gate_status,
                terminal_execution_count = done.terminal_execution_count,
                passed_execution_count = done.passed_execution_count,
                failed_execution_count = done.failed_execution_count,
                errored_execution_count = done.errored_execution_count,
                finalization_candidate_select_ms,
                finalize_control_write_ms = finalize_started.elapsed().as_millis() as u64,
                "finalized run and enqueued completion event"
            );
        } else {
            debug!(run_id = %claimed.id, "claimed finalizable run but no finalization update was applied");
        }
    }

    if finalized == 0 {
        debug!(
            coordinator_id = %coordinator_id,
            "no finalizable runs available for this cycle"
        );
    } else {
        info!(
            coordinator_id = %coordinator_id,
            runs_finalized = finalized,
            "completed coordinator finalization pass"
        );
    }

    Ok(PlacementPassResult {
        output: finalized,
        failures,
    })
}

#[derive(Debug)]
struct RunShardSummaryCollection {
    summaries: Vec<run_shard_summary::RunShardSummary>,
    complete: bool,
}

async fn collect_run_shard_summaries(
    database_router: &database::DatabaseRouter,
    coordinator_id: Uuid,
    run_id: Uuid,
) -> anyhow::Result<PlacementPassResult<RunShardSummaryCollection>> {
    let placements = database_router.execution_placements_for_run(run_id).await?;
    let mut placements_by_alias = BTreeMap::new();
    for placement in placements {
        placements_by_alias
            .entry(placement.database_alias.clone())
            .or_insert_with(Vec::new)
            .push(placement);
    }

    let aliases = placements_by_alias.keys().cloned().collect::<Vec<_>>();
    let batch = run_placement_operations(
        database_router,
        coordinator_id,
        "finalization_summary_read",
        aliases,
        |alias| {
            let placements = placements_by_alias.get(&alias).cloned().unwrap_or_default();
            async move {
                let db = database_router.execution_database(&alias).await?;
                for placement in &placements {
                    if !placement.is_dispatchable() {
                        return Err(
                            database::ExecutionRouteError::NonDispatchableShardPlacement {
                                run_id,
                                run_shard: placement.run_shard,
                                status: placement.status.clone(),
                            }
                            .into(),
                        );
                    }
                }

                // Sealed summaries are immutable. Receipt aggregation owns all
                // writes; finalization only reads the currently routed shard set.
                let shards = placements
                    .iter()
                    .map(|placement| placement.run_shard)
                    .collect::<Vec<_>>();
                let summaries =
                    run_shard_summary::select_run_shard_summaries(&db, run_id, &shards).await?;
                let complete = summaries.len() == shards.len();
                for placement in &placements {
                    if summaries
                        .binary_search_by_key(&placement.run_shard, |summary| summary.run_shard)
                        .is_err()
                    {
                        debug!(
                            run_id = %run_id,
                            run_shard = placement.run_shard,
                            database_alias = %alias,
                            "run shard summary is not available yet"
                        );
                    }
                }
                for summary in &summaries {
                    debug!(
                        run_id = %summary.run_id,
                        run_shard = summary.run_shard,
                        database_alias = %alias,
                        status = %summary.status,
                        terminal_execution_count = summary.terminal_execution_count,
                        expected_execution_count = summary.expected_execution_count,
                        "loaded run shard summary for finalization"
                    );
                }

                Ok((summaries, complete))
            }
        },
    )
    .await;

    let mut summaries = Vec::new();
    let mut complete = true;
    let mut failures = PlacementFailureStats::default();
    for skip in batch.skipped {
        complete = false;
        record_placement_skip(
            &mut failures,
            coordinator_id,
            "finalization_summary_read",
            skip,
        );
    }
    for (alias, (mut alias_summaries, alias_complete)) in batch.successes {
        debug!(
            run_id = %run_id,
            database_alias = %alias,
            shard_summaries_loaded = alias_summaries.len(),
            "loaded routed shard summaries for finalization from execution placement"
        );
        complete &= alias_complete;
        summaries.append(&mut alias_summaries);
    }

    for failure in batch.failures {
        complete = false;
        record_placement_failure(
            &mut failures,
            coordinator_id,
            "finalization_summary_read",
            failure,
        );
    }

    Ok(PlacementPassResult {
        output: RunShardSummaryCollection {
            summaries,
            complete,
        },
        failures,
    })
}

/// Placement-scoped work-unit support for the coordinator cycle.
///
/// Operations retain independent outcomes but own neither transaction nor
/// retry policy. Control operations stay outside this module and remain hard
/// cycle boundaries.
///
/// This stays inline so files under `commands/coordinator/` continue to map to
/// actual coordinator subcommands while batching and classification retain a
/// focused private namespace.
mod placement {
    use std::{
        collections::BTreeSet,
        future::Future,
        num::NonZeroUsize,
        time::Duration,
    };

    use futures_util::{
        StreamExt,
        stream::FuturesUnordered,
    };
    use tracing::{
        debug,
        info,
        warn,
    };
    use uuid::Uuid;

    use crate::context::database;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ErrorKind {
        DatabaseUnavailable,
        DatabaseContention,
        DatabaseContract,
        RouteState,
        PlacementConfiguration,
    }

    impl ErrorKind {
        fn as_str(self) -> &'static str {
            match self {
                Self::DatabaseUnavailable => "database_unavailable",
                Self::DatabaseContention => "database_contention",
                Self::DatabaseContract => "database_contract",
                Self::RouteState => "route_state",
                Self::PlacementConfiguration => "placement_configuration",
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct ErrorClassification {
        kind: ErrorKind,
        retryable: bool,
    }

    #[derive(Debug)]
    pub(super) struct PlacementOperationFailure {
        database_alias: String,
        error: anyhow::Error,
        classification: ErrorClassification,
    }

    impl PlacementOperationFailure {
        pub(super) fn new(database_alias: String, error: anyhow::Error) -> Self {
            let classification = classify_error(&error);
            Self {
                database_alias,
                error,
                classification,
            }
        }
    }

    #[derive(Debug)]
    pub(super) struct PlacementOperationBatch<T> {
        pub(super) successes: Vec<(String, T)>,
        pub(super) failures: Vec<PlacementOperationFailure>,
        pub(super) skipped: Vec<PlacementCircuitSkip>,
    }

    #[derive(Debug)]
    pub(super) struct PlacementCircuitSkip {
        database_alias: String,
        retry_after: Duration,
    }

    #[derive(Debug, Default)]
    pub(super) struct PlacementFailureStats {
        skipped_database_aliases: BTreeSet<String>,
        failed_operations: usize,
        retryable_errors: usize,
        terminal_errors: usize,
    }

    impl PlacementFailureStats {
        fn record(&mut self, failure: &PlacementOperationFailure) {
            self.skipped_database_aliases
                .insert(failure.database_alias.clone());
            self.failed_operations += 1;
            if failure.classification.retryable {
                self.retryable_errors += 1;
            } else {
                self.terminal_errors += 1;
            }
        }

        fn record_skip(&mut self, database_alias: String) {
            self.skipped_database_aliases.insert(database_alias);
        }

        pub(super) fn merge(&mut self, other: Self) {
            self.skipped_database_aliases
                .extend(other.skipped_database_aliases);
            self.failed_operations += other.failed_operations;
            self.retryable_errors += other.retryable_errors;
            self.terminal_errors += other.terminal_errors;
        }

        pub(super) fn excluded_aliases(&self) -> Vec<String> {
            self.skipped_database_aliases.iter().cloned().collect()
        }

        pub(super) fn skipped_placement_count(&self) -> usize {
            self.skipped_database_aliases.len()
        }

        pub(super) fn failed_operation_count(&self) -> usize {
            self.failed_operations
        }

        pub(super) fn retryable_error_count(&self) -> usize {
            self.retryable_errors
        }

        pub(super) fn terminal_error_count(&self) -> usize {
            self.terminal_errors
        }
    }

    #[derive(Debug)]
    pub(super) struct PlacementPassResult<T> {
        pub(super) output: T,
        pub(super) failures: PlacementFailureStats,
    }

    /// Runs independent placement work sequentially and retains every outcome.
    pub(super) async fn run_operations<T, Operation, OperationFuture>(
        database_router: &database::DatabaseRouter,
        coordinator_id: Uuid,
        operation_name: &'static str,
        aliases: Vec<String>,
        operation: Operation,
    ) -> PlacementOperationBatch<T>
    where
        Operation: FnMut(String) -> OperationFuture,
        OperationFuture: Future<Output = anyhow::Result<T>>,
    {
        run_operations_with_deadline(
            database_router,
            coordinator_id,
            operation_name,
            aliases,
            operation,
            true,
            NonZeroUsize::MIN,
        )
        .await
    }

    /// Bounds independently admitted placement operations while retaining circuit
    /// permits, wall-clock deadlines, and every completed outcome. Only aggregation
    /// uses this mode; other coordinator phases retain sequential execution.
    pub(super) async fn run_operations_concurrently<T, Operation, OperationFuture>(
        database_router: &database::DatabaseRouter,
        coordinator_id: Uuid,
        operation_name: &'static str,
        aliases: Vec<String>,
        parallelism: NonZeroUsize,
        operation: Operation,
    ) -> PlacementOperationBatch<T>
    where
        Operation: FnMut(String) -> OperationFuture,
        OperationFuture: Future<Output = anyhow::Result<T>>,
    {
        run_operations_with_deadline(
            database_router,
            coordinator_id,
            operation_name,
            aliases,
            operation,
            true,
            parallelism,
        )
        .await
    }

    /// Broker publication is intentionally excluded from the database wall-clock
    /// deadline. Its individual SQL statements still use the pool session limits.
    pub(super) async fn run_operations_without_outer_deadline<T, Operation, OperationFuture>(
        database_router: &database::DatabaseRouter,
        coordinator_id: Uuid,
        operation_name: &'static str,
        aliases: Vec<String>,
        operation: Operation,
    ) -> PlacementOperationBatch<T>
    where
        Operation: FnMut(String) -> OperationFuture,
        OperationFuture: Future<Output = anyhow::Result<T>>,
    {
        run_operations_with_deadline(
            database_router,
            coordinator_id,
            operation_name,
            aliases,
            operation,
            false,
            NonZeroUsize::MIN,
        )
        .await
    }

    async fn run_operations_with_deadline<T, Operation, OperationFuture>(
        database_router: &database::DatabaseRouter,
        coordinator_id: Uuid,
        operation_name: &'static str,
        aliases: Vec<String>,
        mut operation: Operation,
        enforce_outer_deadline: bool,
        parallelism: NonZeroUsize,
    ) -> PlacementOperationBatch<T>
    where
        Operation: FnMut(String) -> OperationFuture,
        OperationFuture: Future<Output = anyhow::Result<T>>,
    {
        let mut successes = Vec::with_capacity(aliases.len());
        let mut failures = Vec::new();
        let mut skipped = Vec::new();
        let mut aliases = aliases.into_iter();
        let mut in_flight = FuturesUnordered::new();

        loop {
            while in_flight.len() < parallelism.get() {
                let Some(alias) = aliases.next() else { break };
                let permit = match database_router.acquire_database_operation(&alias) {
                    Ok(permit) => permit,
                    Err(open) => {
                        skipped.push(PlacementCircuitSkip {
                            database_alias: alias,
                            retry_after: open.retry_after,
                        });
                        continue;
                    }
                };
                if permit.is_probe() {
                    info!(
                        coordinator_id = %coordinator_id,
                        database_alias = %alias,
                        operation = operation_name,
                        "probing half-open execution database circuit"
                    );
                }
                let operation_future = operation(alias.clone());
                in_flight.push(async move {
                    let result = if enforce_outer_deadline {
                        database_router
                            .deadline_database_operation(&alias, operation_name, operation_future)
                            .await
                            .map_err(anyhow::Error::new)
                            .and_then(std::convert::identity)
                    } else {
                        operation_future.await
                    };
                    (alias, permit, result)
                });
            }
            let Some((alias, permit, result)) = in_flight.next().await else {
                break;
            };
            match result {
                Ok(output) => {
                    if matches!(
                        database_router.record_database_operation_success(permit),
                        Some(database::CircuitTransition::Closed)
                    ) {
                        info!(
                            coordinator_id = %coordinator_id,
                            database_alias = %alias,
                            operation = operation_name,
                            "closed execution database circuit after successful probe"
                        );
                    }
                    successes.push((alias, output));
                }
                Err(error) => {
                    let (_, transition) =
                        database_router.record_database_operation_error(permit, &error);
                    if let Some(database::CircuitTransition::Opened { retry_after }) = transition {
                        warn!(
                            coordinator_id = %coordinator_id,
                            database_alias = %alias,
                            operation = operation_name,
                            retry_after_ms = retry_after.as_millis() as u64,
                            "opened execution database circuit after availability failures"
                        );
                    }
                    failures.push(PlacementOperationFailure::new(alias, error));
                }
            }
        }

        PlacementOperationBatch {
            successes,
            failures,
            skipped,
        }
    }

    pub(super) fn record_skip(
        failures: &mut PlacementFailureStats,
        coordinator_id: Uuid,
        operation: &'static str,
        skip: PlacementCircuitSkip,
    ) {
        debug!(
            coordinator_id = %coordinator_id,
            database_alias = %skip.database_alias,
            operation,
            retry_after_ms = skip.retry_after.as_millis() as u64,
            "skipped execution database operation while circuit is open"
        );
        failures.record_skip(skip.database_alias);
    }

    pub(super) fn record_open(
        failures: &mut PlacementFailureStats,
        coordinator_id: Uuid,
        operation: &'static str,
        database_alias: String,
        open: database::CircuitOpen,
    ) {
        record_skip(
            failures,
            coordinator_id,
            operation,
            PlacementCircuitSkip {
                database_alias,
                retry_after: open.retry_after,
            },
        );
    }

    pub(super) fn record_failure(
        failures: &mut PlacementFailureStats,
        coordinator_id: Uuid,
        operation: &'static str,
        failure: PlacementOperationFailure,
    ) {
        warn!(
            coordinator_id = %coordinator_id,
            database_alias = %failure.database_alias,
            operation,
            error_kind = failure.classification.kind.as_str(),
            retryable = failure.classification.retryable,
            error = ?failure.error,
            "placement-scoped coordinator operation failed; containing failure"
        );
        failures.record(&failure);
    }

    fn classify_error(error: &anyhow::Error) -> ErrorClassification {
        if error.chain().any(|cause| {
            cause
                .downcast_ref::<database::DatabaseOperationTimeout>()
                .is_some()
        }) {
            return ErrorClassification {
                kind: ErrorKind::DatabaseUnavailable,
                retryable: true,
            };
        }
        if error.chain().any(|cause| {
            cause
                .downcast_ref::<database::ExecutionRouteError>()
                .is_some()
        }) {
            return ErrorClassification {
                kind: ErrorKind::RouteState,
                retryable: false,
            };
        }

        let Some(error) = error
            .chain()
            .find_map(|cause| cause.downcast_ref::<sqlx::Error>())
        else {
            return ErrorClassification {
                kind: ErrorKind::PlacementConfiguration,
                retryable: false,
            };
        };

        match error {
            sqlx::Error::Io(_)
            | sqlx::Error::Tls(_)
            | sqlx::Error::Protocol(_)
            | sqlx::Error::PoolTimedOut
            | sqlx::Error::PoolClosed
            | sqlx::Error::WorkerCrashed
            | sqlx::Error::BeginFailed => ErrorClassification {
                kind: ErrorKind::DatabaseUnavailable,
                retryable: true,
            },
            sqlx::Error::Database(error) => classify_database_error_code(error.code().as_deref()),
            _ => ErrorClassification {
                kind: ErrorKind::DatabaseContract,
                retryable: false,
            },
        }
    }

    fn classify_database_error_code(code: Option<&str>) -> ErrorClassification {
        let Some(code) = code else {
            return ErrorClassification {
                kind: ErrorKind::DatabaseContract,
                retryable: false,
            };
        };

        if matches!(code, "40001" | "40P01" | "55P03") {
            return ErrorClassification {
                kind: ErrorKind::DatabaseContention,
                retryable: true,
            };
        }

        if code == "57014"
            || code.starts_with("08")
            || code.starts_with("53")
            || matches!(code, "57P01" | "57P02" | "57P03" | "58030")
        {
            return ErrorClassification {
                kind: ErrorKind::DatabaseUnavailable,
                retryable: true,
            };
        }

        ErrorClassification {
            kind: ErrorKind::DatabaseContract,
            retryable: false,
        }
    }

    #[cfg(test)]
    mod tests {
        use std::sync::{
            Arc,
            atomic::{
                AtomicUsize,
                Ordering,
            },
        };

        use super::*;

        #[tokio::test]
        async fn concurrent_placement_operations_bound_work_and_collect_every_outcome() {
            for parallelism in [1, 2, 4] {
                let context = test_context();
                let database_router = context.dbr().await.unwrap();
                let active = Arc::new(AtomicUsize::new(0));
                let peak = Arc::new(AtomicUsize::new(0));
                let completed = Arc::new(AtomicUsize::new(0));
                let batch = run_operations_concurrently(
                    database_router,
                    Uuid::now_v7(),
                    "aggregation_test",
                    (0..8).map(|n| format!("shard_{n}")).collect(),
                    NonZeroUsize::new(parallelism).unwrap(),
                    |alias| {
                        let active = active.clone();
                        let peak = peak.clone();
                        let completed = completed.clone();
                        async move {
                            let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                            peak.fetch_max(count, Ordering::SeqCst);
                            tokio::task::yield_now().await;
                            active.fetch_sub(1, Ordering::SeqCst);
                            completed.fetch_add(1, Ordering::SeqCst);
                            if alias == "shard_2" {
                                anyhow::bail!("invalid aggregation manifest");
                            }
                            Ok(alias)
                        }
                    },
                )
                .await;
                assert_eq!(peak.load(Ordering::SeqCst), parallelism);
                assert_eq!(active.load(Ordering::SeqCst), 0);
                assert_eq!(completed.load(Ordering::SeqCst), 8);
                assert_eq!(batch.successes.len(), 7);
                assert_eq!(batch.failures.len(), 1);
                assert_eq!(batch.failures[0].database_alias, "shard_2");
                assert!(batch.skipped.is_empty());
                if parallelism == 1 {
                    assert_eq!(
                        batch
                            .successes
                            .into_iter()
                            .map(|(alias, _)| alias)
                            .collect::<Vec<_>>(),
                        [
                            "shard_0", "shard_1", "shard_3", "shard_4", "shard_5", "shard_6",
                            "shard_7"
                        ]
                    );
                }
            }
        }

        #[tokio::test]
        async fn concurrent_placement_operations_let_healthy_alias_finish_before_slow_alias() {
            let context = test_context_with_timeout(Duration::from_secs(1));
            let database_router = context.dbr().await.unwrap();
            let healthy_finished = tokio::sync::Notify::new();
            let batch = run_operations_concurrently(
                database_router,
                Uuid::now_v7(),
                "aggregation_test",
                ["slow", "healthy", "later"].map(str::to_string).to_vec(),
                NonZeroUsize::new(2).unwrap(),
                |alias| {
                    let healthy_finished = &healthy_finished;
                    async move {
                        if alias == "slow" {
                            healthy_finished.notified().await;
                            return Err(anyhow::Error::new(sqlx::Error::PoolTimedOut));
                        }
                        if alias == "healthy" {
                            healthy_finished.notify_one();
                        }
                        Ok(alias)
                    }
                },
            )
            .await;
            assert_eq!(batch.successes.len(), 2);
            assert_eq!(batch.failures.len(), 1);
            assert!(
                matches!(
                    batch.failures[0].error.downcast_ref::<sqlx::Error>(),
                    Some(sqlx::Error::PoolTimedOut)
                ),
                "healthy work must release the slow operation before its deadline"
            );
        }

        #[tokio::test]
        async fn concurrent_placement_deadlines_release_operations_and_keep_healthy_results() {
            struct InFlight(Arc<AtomicUsize>);
            impl Drop for InFlight {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::SeqCst);
                }
            }
            let context = test_context_with_timeout(Duration::from_millis(20));
            let database_router = context.dbr().await.unwrap();
            let active = Arc::new(AtomicUsize::new(0));
            let batch = run_operations_concurrently(
                database_router,
                Uuid::now_v7(),
                "aggregation_test",
                ["slow", "healthy", "later"].map(str::to_string).to_vec(),
                NonZeroUsize::new(2).unwrap(),
                |alias| {
                    let active = active.clone();
                    async move {
                        active.fetch_add(1, Ordering::SeqCst);
                        let _guard = InFlight(active);
                        if alias == "slow" {
                            std::future::pending::<()>().await;
                        }
                        Ok(alias)
                    }
                },
            )
            .await;
            assert_eq!(active.load(Ordering::SeqCst), 0);
            assert_eq!(batch.successes.len(), 2);
            assert_eq!(batch.failures.len(), 1);
            assert_eq!(batch.failures[0].database_alias, "slow");
            assert!(
                batch.failures[0]
                    .error
                    .downcast_ref::<database::DatabaseOperationTimeout>()
                    .is_some()
            );
            assert_eq!(
                batch.failures[0].classification.kind,
                ErrorKind::DatabaseUnavailable
            );
        }

        #[tokio::test]
        async fn concurrent_placement_operations_skip_open_circuits_without_starting_work() {
            let context = test_context();
            let database_router = context.dbr().await.unwrap();
            for _ in 0..3 {
                let permit = database_router
                    .acquire_database_operation("offline")
                    .unwrap();
                database_router.record_database_operation_error(
                    permit,
                    &anyhow::Error::new(sqlx::Error::PoolTimedOut),
                );
            }
            let mut started = Vec::new();
            let batch = run_operations_concurrently(
                database_router,
                Uuid::now_v7(),
                "aggregation_test",
                ["offline", "healthy", "later"].map(str::to_string).to_vec(),
                NonZeroUsize::new(2).unwrap(),
                |alias| {
                    started.push(alias.clone());
                    async move { Ok::<_, anyhow::Error>(alias) }
                },
            )
            .await;
            assert_eq!(started, ["healthy", "later"]);
            assert_eq!(batch.successes.len(), 2);
            assert!(batch.failures.is_empty());
            assert_eq!(batch.skipped.len(), 1);
            assert_eq!(batch.skipped[0].database_alias, "offline");
        }

        #[tokio::test]
        async fn placement_operations_preserve_successes_and_classify_failures() {
            let context = test_context();
            let database_router = context.dbr().await.unwrap();
            let run_id = Uuid::now_v7();
            let aliases = ["primary", "shard_001", "shard_002", "shard_003"]
                .map(str::to_string)
                .to_vec();

            let batch = run_operations(
                database_router,
                Uuid::now_v7(),
                "test",
                aliases,
                |alias| async move {
                    match alias.as_str() {
                        "shard_001" => Err(anyhow::Error::new(sqlx::Error::PoolTimedOut)),
                        "shard_002" => Err(
                            database::ExecutionRouteError::NonDispatchableShardPlacement {
                                run_id,
                                run_shard: 2,
                                status: "moving".to_string(),
                            }
                            .into(),
                        ),
                        _ => Ok(format!("completed:{alias}")),
                    }
                },
            )
            .await;

            assert_eq!(
                batch.successes,
                vec![
                    ("primary".to_string(), "completed:primary".to_string()),
                    ("shard_003".to_string(), "completed:shard_003".to_string()),
                ]
            );
            assert_eq!(batch.failures.len(), 2);
            assert!(batch.skipped.is_empty());
            assert_eq!(
                batch.failures[0].classification,
                ErrorClassification {
                    kind: ErrorKind::DatabaseUnavailable,
                    retryable: true,
                }
            );
            assert_eq!(
                batch.failures[1].classification,
                ErrorClassification {
                    kind: ErrorKind::RouteState,
                    retryable: false,
                }
            );

            let mut stats = PlacementFailureStats::default();
            for failure in &batch.failures {
                stats.record(failure);
            }
            assert_eq!(
                stats.skipped_database_aliases,
                BTreeSet::from(["shard_001".to_string(), "shard_002".to_string()])
            );
            assert_eq!(stats.failed_operations, 2);
            assert_eq!(stats.retryable_errors, 1);
            assert_eq!(stats.terminal_errors, 1);
        }

        #[tokio::test]
        async fn placement_operations_skip_only_the_open_database_circuit() {
            let context = test_context();
            let database_router = context.dbr().await.unwrap();
            let coordinator_id = Uuid::now_v7();

            for _ in 0..3 {
                let batch = run_operations(
                    database_router,
                    coordinator_id,
                    "test",
                    vec!["shard_001".to_string()],
                    |_| async {
                        Err::<(), _>(anyhow::Error::new(sqlx::Error::Io(std::io::Error::new(
                            std::io::ErrorKind::ConnectionRefused,
                            "database unavailable",
                        ))))
                    },
                )
                .await;
                assert_eq!(batch.failures.len(), 1);
            }

            let batch = run_operations(
                database_router,
                coordinator_id,
                "test",
                vec!["shard_001".to_string(), "shard_002".to_string()],
                |alias| async move { Ok::<_, anyhow::Error>(alias) },
            )
            .await;

            assert_eq!(batch.skipped.len(), 1);
            assert_eq!(batch.skipped[0].database_alias, "shard_001");
            assert_eq!(
                batch.successes,
                vec![("shard_002".to_string(), "shard_002".to_string())]
            );
        }

        #[tokio::test]
        async fn placement_operation_timeout_does_not_block_healthy_aliases() {
            let context = test_context_with_timeout(Duration::from_millis(20));
            let database_router = context.dbr().await.unwrap();
            let started = std::time::Instant::now();

            let batch = run_operations(
                database_router,
                Uuid::now_v7(),
                "test",
                vec!["slow".to_string(), "healthy".to_string()],
                |alias| async move {
                    if alias == "slow" {
                        std::future::pending::<()>().await;
                    }
                    Ok::<_, anyhow::Error>(alias)
                },
            )
            .await;

            assert!(started.elapsed() < Duration::from_secs(1));
            assert_eq!(
                batch.successes,
                vec![("healthy".to_string(), "healthy".to_string())]
            );
            assert_eq!(batch.failures.len(), 1);
            assert!(
                batch.failures[0]
                    .error
                    .downcast_ref::<database::DatabaseOperationTimeout>()
                    .is_some()
            );
            assert_eq!(
                batch.failures[0].classification.kind,
                ErrorKind::DatabaseUnavailable
            );
        }

        #[tokio::test]
        async fn database_operation_timeouts_open_only_the_affected_circuit() {
            let context = test_context_with_timeout(Duration::from_millis(5));
            let database_router = context.dbr().await.unwrap();
            let coordinator_id = Uuid::now_v7();

            for _ in 0..3 {
                let batch = run_operations(
                    database_router,
                    coordinator_id,
                    "test",
                    vec!["slow".to_string()],
                    |_| std::future::pending::<anyhow::Result<()>>(),
                )
                .await;
                assert_eq!(batch.failures.len(), 1);
            }

            let batch = run_operations(
                database_router,
                coordinator_id,
                "test",
                vec!["slow".to_string(), "healthy".to_string()],
                |alias| async move { Ok::<_, anyhow::Error>(alias) },
            )
            .await;

            assert_eq!(batch.skipped.len(), 1);
            assert_eq!(batch.skipped[0].database_alias, "slow");
            assert_eq!(batch.successes.len(), 1);
            assert_eq!(batch.successes[0].0, "healthy");
        }

        #[tokio::test]
        async fn broker_wait_is_excluded_from_database_operation_deadline() {
            let context = test_context_with_timeout(Duration::from_millis(5));
            let database_router = context.dbr().await.unwrap();

            let batch = run_operations_without_outer_deadline(
                database_router,
                Uuid::now_v7(),
                "outbox_publication",
                vec!["primary".to_string()],
                |alias| async move {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    Ok::<_, anyhow::Error>(alias)
                },
            )
            .await;

            assert!(batch.failures.is_empty());
            assert!(batch.skipped.is_empty());
            assert_eq!(batch.successes[0].0, "primary");
        }

        #[test]
        fn postgres_retryable_codes_are_classified_by_failure_kind() {
            assert_eq!(
                classify_database_error_code(Some("40001")),
                ErrorClassification {
                    kind: ErrorKind::DatabaseContention,
                    retryable: true,
                }
            );
            assert_eq!(
                classify_database_error_code(Some("08006")),
                ErrorClassification {
                    kind: ErrorKind::DatabaseUnavailable,
                    retryable: true,
                }
            );
            assert_eq!(
                classify_database_error_code(Some("23505")),
                ErrorClassification {
                    kind: ErrorKind::DatabaseContract,
                    retryable: false,
                }
            );
        }

        fn test_context() -> crate::context::Context {
            crate::context::Context::new(
                database::Config::new(
                    "postgres://not-used".to_string(),
                    1,
                    Duration::from_secs(10),
                    database::CircuitBreakerConfig::default(),
                    database::PlacementConfig::default_single_database(),
                ),
                Some(crate::mq::Config::new("amqp://not-used".to_string())),
                crate::context::wasm::Config::default(),
                crate::context::output::OutputFormat::Json,
            )
        }

        fn test_context_with_timeout(timeout: Duration) -> crate::context::Context {
            crate::context::Context::new(
                database::Config::new(
                    "postgres://not-used".to_string(),
                    1,
                    Duration::from_secs(10),
                    database::CircuitBreakerConfig::default(),
                    database::PlacementConfig::default_single_database(),
                )
                .with_operation_timeout(Some(
                    database::DatabaseOperationTimeoutConfig::new(timeout).unwrap(),
                )),
                Some(crate::mq::Config::new("amqp://not-used".to_string())),
                crate::context::wasm::Config::default(),
                crate::context::output::OutputFormat::Json,
            )
        }
    }
}

#[cfg(test)]
mod postgres_tests;

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[derive(Debug, Parser)]
    struct TestCommand {
        #[command(flatten)]
        command: Command,
    }

    #[test]
    fn aggregation_controls_accept_independent_budgets() {
        let parsed = TestCommand::try_parse_from([
            "coordinator",
            "--messaging-url",
            "amqp://not-used",
            "--max-finalize-per-cycle",
            "1",
            "--max-aggregation-pages-per-cycle",
            "256",
            "--aggregation-time-budget-ms",
            "1000",
            "--aggregation-parallelism",
            "4",
            "--aggregation-database-parallelism",
            "4",
            "once",
        ]);
        assert!(
            parsed.is_ok(),
            "aggregation controls must parse: {parsed:?}"
        );
        let config = parsed.unwrap().command.runtime_config();
        assert_eq!(config.max_finalize_per_cycle, 1);
        assert_eq!(config.aggregation.max_pages, 256);
        assert_eq!(config.aggregation.time_budget, Duration::from_millis(1000));
        assert_eq!(config.aggregation.parallelism, 4);
        assert_eq!(config.aggregation_database_parallelism.get(), 4);
    }

    #[test]
    fn aggregation_controls_have_bounded_defaults_and_reject_invalid_values() {
        let command = TestCommand::try_parse_from([
            "coordinator",
            "--messaging-url",
            "amqp://not-used",
            "once",
        ])
        .unwrap()
        .command;
        let config = command.runtime_config();
        assert_eq!(config.max_finalize_per_cycle, 64);
        assert_eq!(config.aggregation.max_pages, 256);
        assert_eq!(config.aggregation.time_budget, Duration::from_millis(1000));
        assert_eq!(config.aggregation.parallelism, 4);
        assert_eq!(config.aggregation_database_parallelism.get(), 4);
        for (flag, upper) in [
            ("--max-aggregation-pages-per-cycle", 100_000),
            ("--aggregation-time-budget-ms", 60_000),
            ("--aggregation-parallelism", 128),
            ("--aggregation-database-parallelism", 128),
        ] {
            for value in [0, upper + 1] {
                let parsed = TestCommand::try_parse_from([
                    "coordinator",
                    "--messaging-url",
                    "amqp://not-used",
                    flag,
                    &value.to_string(),
                    "once",
                ]);
                assert_eq!(
                    parsed.unwrap_err().kind(),
                    clap::error::ErrorKind::ValueValidation,
                    "flag={flag} value={value}"
                );
            }
            for value in [1, upper] {
                TestCommand::try_parse_from([
                    "coordinator",
                    "--messaging-url",
                    "amqp://not-used",
                    flag,
                    &value.to_string(),
                    "once",
                ])
                .unwrap();
            }
        }
    }
}
