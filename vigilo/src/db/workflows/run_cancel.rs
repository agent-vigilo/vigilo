//! Run cancellation workflow helpers.
//!
//! Cancellation is a terminal, user-requested state transition.
//!
//! The control database owns the run lifecycle and cancellation event. Execution
//! placements own chunks, attempts, and executions, so routed cancellation first
//! marks the control run cancelled and drains dispatch cursors, then fans out
//! idempotent execution-local cancellation to each routed placement.

use std::collections::BTreeMap;

use futures_util::{
    StreamExt,
    stream,
};
use sqlx::{
    PgPool,
    Postgres,
    Transaction,
};
use uuid::Uuid;

use super::run_shard_summary::{
    RunShardSummary,
    initialize_run_shard_summary_with,
};
use crate::{
    context::database::{
        self,
        ExecutionRoute,
    },
    models::run::Run,
};

mod queries;

#[cfg(test)]
use queries::select_run_shards;
use queries::{
    cancel_open_attempts,
    cancel_open_chunks,
    cancel_open_executions,
    enqueue_cancelled_event,
    mark_control_cancellation_requested,
    select_run_execution_progress,
    update_run_cancelled,
};

const CANCEL_REASON: &str = "run cancelled by user";
const ROUTED_CANCEL_PARALLELISM: usize = 16;

/// Result of a cancel request for a run.
#[derive(Debug, Clone)]
pub(crate) struct CancelRunOutcome {
    pub(crate) run: Run,
    pub(crate) cancelled: bool,
    pub(crate) already_cancelled: bool,
    pub(crate) chunks_cancelled: i64,
    pub(crate) executions_cancelled: i64,
    pub(crate) attempts_cancelled: i64,
    pub(crate) outbox_events_enqueued: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct CancellationCounts {
    chunks_cancelled: i64,
    executions_cancelled: i64,
    attempts_cancelled: i64,
}

impl CancellationCounts {
    fn add(&mut self, other: &Self) {
        self.chunks_cancelled += other.chunks_cancelled;
        self.executions_cancelled += other.executions_cancelled;
        self.attempts_cancelled += other.attempts_cancelled;
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, sqlx::FromRow)]
struct ExecutionProgressCounts {
    expected_execution_count: i64,
    execution_count: i64,
    terminal_execution_count: i64,
    passed_execution_count: i64,
    failed_execution_count: i64,
    errored_execution_count: i64,
    skipped_execution_count: i64,
    missing_aggregate_count: i64,
    chunk_count: i64,
    completed_chunk_count: i64,
    failed_chunk_count: i64,
    cancelled_chunk_count: i64,
    cancelled_execution_count: i64,
}

impl ExecutionProgressCounts {
    fn add(&mut self, other: &Self) {
        self.expected_execution_count += other.expected_execution_count;
        self.execution_count += other.execution_count;
        self.terminal_execution_count += other.terminal_execution_count;
        self.passed_execution_count += other.passed_execution_count;
        self.failed_execution_count += other.failed_execution_count;
        self.errored_execution_count += other.errored_execution_count;
        self.skipped_execution_count += other.skipped_execution_count;
        self.missing_aggregate_count += other.missing_aggregate_count;
        self.chunk_count += other.chunk_count;
        self.completed_chunk_count += other.completed_chunk_count;
        self.failed_chunk_count += other.failed_chunk_count;
        self.cancelled_chunk_count += other.cancelled_chunk_count;
        self.cancelled_execution_count += other.cancelled_execution_count;
    }

    fn from_control_run(run: &Run) -> Self {
        Self {
            expected_execution_count: i64::from(run.expected_execution_count),
            terminal_execution_count: i64::from(run.terminal_execution_count),
            passed_execution_count: i64::from(run.passed_execution_count),
            failed_execution_count: i64::from(run.failed_execution_count),
            errored_execution_count: i64::from(run.errored_execution_count),
            ..Self::default()
        }
    }
}

#[derive(Debug)]
struct ControlCancellationState {
    run: Run,
    cancelled: bool,
    already_cancelled: bool,
}

struct ExecutionCancellationTarget {
    routes: Vec<ExecutionRoute>,
}

#[derive(Debug, Default)]
struct ExecutionCancellationOutcome {
    counts: CancellationCounts,
    progress: ExecutionProgressCounts,
    summaries: Vec<RunShardSummary>,
}

impl ExecutionCancellationOutcome {
    fn add(&mut self, other: Self) {
        self.counts.add(&other.counts);
        self.progress.add(&other.progress);
        self.summaries.extend(other.summaries);
    }
}

fn is_cancelable_status(status: &str) -> bool {
    matches!(status, "pending" | "running" | "finalizing")
}

fn is_already_cancelled_status(status: &str) -> bool {
    status == "cancelled"
}

fn is_uncancelable_terminal_status(status: &str) -> bool {
    matches!(status, "completed" | "failed")
}

async fn cancel_execution_target(
    database_router: &database::DatabaseRouter,
    run_id: Uuid,
    target: ExecutionCancellationTarget,
) -> anyhow::Result<ExecutionCancellationOutcome> {
    let run_shards = target
        .routes
        .iter()
        .map(|route| route.placement.run_shard)
        .collect();
    let tx = database_router
        .begin_execution_cleanup_admission(&target.routes)
        .await?;
    cancel_execution_target_with_transaction(run_id, run_shards, tx).await
}

async fn cancel_execution_target_with_transaction(
    run_id: Uuid,
    run_shards: Vec<i16>,
    mut tx: Transaction<'static, Postgres>,
) -> anyhow::Result<ExecutionCancellationOutcome> {
    // Match worker persistence and recovery lock order: chunk before attempt.
    // The chunk token remains visible to movement until this transaction commits.
    let chunks_cancelled = cancel_open_chunks(&mut tx, run_id).await?;
    let attempts_cancelled = cancel_open_attempts(&mut tx, run_id).await?;
    let executions_cancelled = cancel_open_executions(&mut tx, run_id).await?;
    let counts = CancellationCounts {
        chunks_cancelled,
        executions_cancelled,
        attempts_cancelled,
    };
    let progress = select_run_execution_progress(&mut tx, run_id).await?;
    let mut summaries = Vec::with_capacity(run_shards.len());
    for run_shard in run_shards {
        super::run_shard_summary::settle_terminal_chunks_with(&mut tx, run_id, run_shard).await?;
        if let Some(summary) = initialize_run_shard_summary_with(&mut tx, run_id, run_shard).await?
        {
            summaries.push(summary);
        }
    }
    tx.commit().await?;

    Ok(ExecutionCancellationOutcome {
        counts,
        progress,
        summaries,
    })
}

fn group_execution_routes(routes: Vec<ExecutionRoute>) -> Vec<ExecutionCancellationTarget> {
    let mut grouped = BTreeMap::<String, ExecutionCancellationTarget>::new();

    for route in routes {
        let alias = route.placement.database_alias.clone();
        grouped
            .entry(alias)
            .and_modify(|target| target.routes.push(route.clone()))
            .or_insert_with(|| ExecutionCancellationTarget {
                routes: vec![route],
            });
    }

    grouped.into_values().collect()
}

async fn cancel_execution_targets(
    database_router: &database::DatabaseRouter,
    run_id: Uuid,
    targets: Vec<ExecutionCancellationTarget>,
) -> anyhow::Result<ExecutionCancellationOutcome> {
    let mut stream = stream::iter(targets.into_iter().map(|target| async move {
        cancel_execution_target(database_router, run_id, target).await
    }))
    .buffer_unordered(ROUTED_CANCEL_PARALLELISM);

    let mut combined = ExecutionCancellationOutcome::default();
    while let Some(outcome) = stream.next().await {
        combined.add(outcome?);
    }

    Ok(combined)
}

fn fallback_progress(
    control: ExecutionProgressCounts,
    mut progress: ExecutionProgressCounts,
) -> ExecutionProgressCounts {
    if progress.expected_execution_count == 0 {
        progress.expected_execution_count = control.expected_execution_count;
    }

    if progress.execution_count == 0 && progress.terminal_execution_count == 0 {
        progress.execution_count = control.execution_count;
        progress.terminal_execution_count = control.terminal_execution_count;
        progress.passed_execution_count = control.passed_execution_count;
        progress.failed_execution_count = control.failed_execution_count;
        progress.errored_execution_count = control.errored_execution_count;
    }

    progress
}

async fn finalize_control_cancellation(
    db: &PgPool,
    state: ControlCancellationState,
    execution_outcome: ExecutionCancellationOutcome,
    execution_route_count: usize,
) -> anyhow::Result<CancelRunOutcome> {
    let counts = execution_outcome.counts;
    let progress = fallback_progress(
        ExecutionProgressCounts::from_control_run(&state.run),
        execution_outcome.progress,
    );
    let shard_summary_count = execution_outcome.summaries.len();

    let mut tx = db.begin().await?;
    let run = update_run_cancelled(
        &mut tx,
        state.run.id,
        &progress,
        &counts,
        execution_route_count,
        shard_summary_count,
    )
    .await?;
    let mut outcome = CancelRunOutcome {
        run,
        cancelled: state.cancelled,
        already_cancelled: state.already_cancelled,
        chunks_cancelled: counts.chunks_cancelled,
        executions_cancelled: counts.executions_cancelled,
        attempts_cancelled: counts.attempts_cancelled,
        outbox_events_enqueued: 0,
    };
    outcome.outbox_events_enqueued = enqueue_cancelled_event(&mut tx, &outcome).await?;

    tx.commit().await?;
    Ok(outcome)
}

/// Cancels a pending/running/finalizing run through routed execution databases.
///
/// Returns `Ok(None)` when the run does not exist. Repeated cancellation of an
/// already-cancelled run is idempotent; completed or failed runs are rejected.
///
/// Workflow behavior:
/// - Mark the control run cancelled and drain control dispatch cursors under a
///   short control transaction.
/// - Resolve execution routes and cancel each placement once, grouped by
///   database alias and bounded by `ROUTED_CANCEL_PARALLELISM`.
/// - Hold shared movement admission while cancelling execution-local rows and
///   committing their terminal chunk receipts in one transaction.
/// - Count execution-local progress directly for shards that were never
///   dispatched.
/// - Finalize the control run summary and emit one idempotent control outbox
///   event. If fanout fails, retrying the command resumes from the already
///   cancelled control state and repeats idempotent execution cleanup.
pub(crate) async fn cancel_run_routed(
    database_router: &database::DatabaseRouter,
    run_id: Uuid,
) -> anyhow::Result<Option<CancelRunOutcome>> {
    let control_db = database_router.control().await?;
    let Some(state) = mark_control_cancellation_requested(control_db, run_id).await? else {
        return Ok(None);
    };

    let routes = database_router
        .execution_read_routes_with_fences_for_run(run_id)
        .await?;
    let execution_route_count = routes.len();
    let targets = group_execution_routes(routes);
    let execution_outcome = cancel_execution_targets(database_router, run_id, targets).await?;

    finalize_control_cancellation(control_db, state, execution_outcome, execution_route_count)
        .await
        .map(Some)
}

/// Single-pool compatibility helper for workflow tests.
#[cfg(test)]
pub(crate) async fn cancel_run(
    db: &PgPool,
    run_id: Uuid,
) -> anyhow::Result<Option<CancelRunOutcome>> {
    let Some(state) = mark_control_cancellation_requested(db, run_id).await? else {
        return Ok(None);
    };

    let run_shards = select_run_shards(db, run_id).await?;
    let tx = db.begin().await?;
    let execution_outcome =
        cancel_execution_target_with_transaction(run_id, run_shards, tx).await?;

    finalize_control_cancellation(db, state, execution_outcome, 1)
        .await
        .map(Some)
}

#[cfg(test)]
mod tests {
    use chrono::{
        DateTime,
        Utc,
    };
    use sqlx::{
        PgPool,
        postgres::PgPoolOptions,
    };
    use uuid::Uuid;

    use super::*;
    use crate::{
        context::database::ExecutionRoute,
        models::shard_placement::ShardPlacement,
    };

    #[test]
    fn cancelable_statuses_are_open_run_states() {
        assert!(is_cancelable_status("pending"));
        assert!(is_cancelable_status("running"));
        assert!(is_cancelable_status("finalizing"));
        assert!(!is_cancelable_status("completed"));
        assert!(!is_cancelable_status("failed"));
        assert!(!is_cancelable_status("cancelled"));
    }

    #[test]
    fn cancelled_status_is_idempotent() {
        assert!(is_already_cancelled_status("cancelled"));
        assert!(!is_already_cancelled_status("running"));
    }

    #[test]
    fn completed_and_failed_runs_are_not_cancelable() {
        assert!(is_uncancelable_terminal_status("completed"));
        assert!(is_uncancelable_terminal_status("failed"));
        assert!(!is_uncancelable_terminal_status("cancelled"));
    }

    #[test]
    fn execution_progress_counts_adds_all_routed_totals() {
        let mut first = progress_counts(1);
        let second = progress_counts(2);

        first.add(&second);

        assert_eq!(first, progress_counts(3));
    }

    #[test]
    fn cancellation_counts_adds_all_routed_totals() {
        let mut total = CancellationCounts {
            chunks_cancelled: 1,
            executions_cancelled: 2,
            attempts_cancelled: 3,
        };

        total.add(&CancellationCounts {
            chunks_cancelled: 4,
            executions_cancelled: 5,
            attempts_cancelled: 6,
        });

        assert_eq!(
            total,
            CancellationCounts {
                chunks_cancelled: 5,
                executions_cancelled: 7,
                attempts_cancelled: 9,
            }
        );
    }

    #[test]
    fn fallback_progress_uses_control_counts_only_when_execution_data_is_absent() {
        let control = ExecutionProgressCounts {
            expected_execution_count: 10,
            terminal_execution_count: 8,
            passed_execution_count: 6,
            failed_execution_count: 1,
            errored_execution_count: 1,
            ..ExecutionProgressCounts::default()
        };

        let fallback = fallback_progress(control.clone(), ExecutionProgressCounts::default());
        assert_eq!(fallback.expected_execution_count, 10);
        assert_eq!(fallback.terminal_execution_count, 8);
        assert_eq!(fallback.passed_execution_count, 6);

        let execution = ExecutionProgressCounts {
            expected_execution_count: 4,
            execution_count: 1,
            terminal_execution_count: 0,
            ..ExecutionProgressCounts::default()
        };
        assert_eq!(fallback_progress(control, execution.clone()), execution);
    }

    #[tokio::test]
    async fn execution_routes_are_grouped_once_per_database_alias() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://localhost/vigilo")
            .unwrap();
        let run_id = Uuid::nil();
        let targets = group_execution_routes(vec![
            execution_route(&pool, run_id, 2, "shard_b"),
            execution_route(&pool, run_id, 0, "shard_a"),
            execution_route(&pool, run_id, 1, "shard_b"),
        ]);

        assert_eq!(targets.len(), 2);
        assert_eq!(targets[0].routes[0].placement.database_alias, "shard_a");
        assert_eq!(targets[0].routes.len(), 1);
        assert_eq!(targets[1].routes[0].placement.database_alias, "shard_b");
        assert_eq!(targets[1].routes.len(), 2);
    }

    fn progress_counts(value: i64) -> ExecutionProgressCounts {
        ExecutionProgressCounts {
            expected_execution_count: value,
            execution_count: value,
            terminal_execution_count: value,
            passed_execution_count: value,
            failed_execution_count: value,
            errored_execution_count: value,
            skipped_execution_count: value,
            missing_aggregate_count: value,
            chunk_count: value,
            completed_chunk_count: value,
            failed_chunk_count: value,
            cancelled_chunk_count: value,
            cancelled_execution_count: value,
        }
    }

    fn execution_route(
        pool: &PgPool,
        run_id: Uuid,
        run_shard: i16,
        database_alias: &str,
    ) -> ExecutionRoute {
        let timestamp = DateTime::<Utc>::from_timestamp(0, 0).unwrap();
        ExecutionRoute {
            placement: ShardPlacement {
                run_id,
                run_shard,
                database_alias: database_alias.to_string(),
                status: "active".to_string(),
                move_target_database_alias: None,
                route_version: 1,
                write_epoch: 1,
                created_at: timestamp,
                updated_at: timestamp,
            },
            pool: pool.clone(),
        }
    }
}
