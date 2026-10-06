//! Run dispatch workflow helpers.
//!
//! Coordinators use this module to start pending runs in the control database,
//! prepare execution-local run snapshots, and insert one outbox event record per
//! chunk in a bounded dispatch window. Dispatch cursors stay in the control
//! database and serialize route claims while execution databases own chunk
//! scans and chunk-ready outbox event records.

use sqlx::{
    PgPool,
    Postgres,
    Transaction,
};
use uuid::Uuid;

use super::run_shard_summary;
use crate::context::database::{
    self,
    ExecutionRoute,
};

mod queries;

#[cfg(test)]
use queries::claim_exact_dispatch_cursor;
#[cfg(test)]
pub(crate) use queries::select_next_dispatch_route;
pub(crate) use queries::{
    claim_next_dispatch_route,
    count_dispatch_cursor_backlog,
    prepare_dispatch_run_snapshot_with,
};
use queries::{
    dispatch_run_window,
    recover_expired_chunks,
    update_dispatch_cursor,
};

/// Run projection returned after a coordinator dispatches one shard window.
#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct DispatchedRun {
    pub(crate) id: Uuid,
    pub(crate) run_key: String,
    pub(crate) run_shard: i16,
    pub(crate) chunk_ready_event_records_inserted: i64,
    pub(crate) chunks_marked_dispatched: i64,
    pub(crate) run_started_event_records_inserted: i64,
}

/// Immutable run context copied from the control database before execution dispatch.
#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct DispatchRunSnapshot {
    pub(crate) run_id: Uuid,
    pub(crate) run_shard: i16,
    pub(crate) run_key: String,
    pub(crate) dataset_id: Uuid,
    pub(crate) dataset_version_id: Uuid,
    pub(crate) dataset_version: String,
    pub(crate) evaluation_profile_id: String,
    pub(crate) evaluation_profile_version: String,
    pub(crate) profile_version_id: String,
    pub(crate) profile_hash: String,
    pub(crate) aggregation_policy_id: String,
    pub(crate) aggregation_policy_version: String,
    pub(crate) aggregation_policy_hash: String,
    pub(crate) agent_provider: String,
    pub(crate) agent_name: String,
    pub(crate) agent_version: Option<String>,
    pub(crate) prompt_config_id: String,
    pub(crate) prompt_config_version: String,
    pub(crate) config_snapshot: serde_json::Value,
    pub(crate) run_started_event_records_inserted: i64,
}

/// Control-database route selected for one dispatch attempt.
#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct DispatchRoute {
    pub(crate) run_id: Uuid,
    pub(crate) run_shard: i16,
    pub(crate) database_alias: String,
    pub(crate) placement_status: String,
    pub(crate) route_version: i64,
    pub(crate) write_epoch: i64,
}

pub(crate) struct ClaimedDispatchRoute {
    pub(crate) route: DispatchRoute,
    pub(crate) control_tx: Transaction<'static, Postgres>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct DispatchedRunWindow {
    id: Uuid,
    run_key: String,
    run_shard: i16,
    chunk_ready_event_records_inserted: i64,
    chunks_marked_dispatched: i64,
    run_started_event_records_inserted: i64,
    has_remaining_chunks: bool,
}

impl From<&DispatchedRunWindow> for DispatchedRun {
    fn from(dispatched: &DispatchedRunWindow) -> Self {
        Self {
            id: dispatched.id,
            run_key: dispatched.run_key.clone(),
            run_shard: dispatched.run_shard,
            chunk_ready_event_records_inserted: dispatched.chunk_ready_event_records_inserted,
            chunks_marked_dispatched: dispatched.chunks_marked_dispatched,
            run_started_event_records_inserted: dispatched.run_started_event_records_inserted,
        }
    }
}

/// Counts returned after one expired chunk lease recovery pass.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ChunkLeaseRecoveryStats {
    pub(crate) recovered_chunks: i64,
    pub(crate) failed_chunks: i64,
}

/// Identifies which database boundary failed during a routed dispatch.
///
/// Execution write failures are safe for the coordinator to isolate because
/// the control cursor transaction rolls back and leaves the cursor open.
/// Control failures remain cycle-fatal because cursor ownership can no longer
/// be determined reliably.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RoutedDispatchError {
    #[error("dispatch invariant failed: {0}")]
    Invariant(#[source] anyhow::Error),
    #[error("control database dispatch operation failed: {0}")]
    Control(#[source] anyhow::Error),
    #[error("execution placement dispatch write failed: {0}")]
    ExecutionWrite(#[source] anyhow::Error),
}

fn validate_dispatch_snapshot_route(
    route_run_id: Uuid,
    route_run_shard: i16,
    snapshot_run_id: Uuid,
    snapshot_run_shard: i16,
) -> Result<(), RoutedDispatchError> {
    if snapshot_run_id != route_run_id || snapshot_run_shard != route_run_shard {
        return Err(RoutedDispatchError::Invariant(anyhow::anyhow!(
            "dispatch snapshot route mismatch: route {} shard {}, snapshot {} shard {}",
            route_run_id,
            route_run_shard,
            snapshot_run_id,
            snapshot_run_shard
        )));
    }

    Ok(())
}

fn dispatch_cursor_status(has_remaining_chunks: bool) -> &'static str {
    if has_remaining_chunks {
        "open"
    } else {
        "drained"
    }
}

/// Recovers expired worker chunk leases for prepared run snapshots.
///
/// Recovered chunks are moved back to `pending` and receive a fresh
/// `run.chunk.ready` outbox record with a recovery-scoped dedupe key. Chunks that have
/// already reached the recovery limit are marked failed so finalization can
/// terminate the run instead of leaving it blocked by a dead lease forever.
///
/// Query behavior:
/// - Runs in one transaction so recovery and poison-chunk failure see a
///   consistent lease snapshot.
/// - First query locks a bounded oldest-expired set below `max_recoveries`,
///   resets those chunks to `pending`, increments recovery metadata, and emits
///   idempotent recovery-scoped chunk-ready outbox records.
/// - Second query locks a bounded oldest-expired set already at the recovery
///   limit and marks them `failed`.
/// - `SKIP LOCKED` lets multiple coordinators run recovery without blocking
///   each other on the same chunk rows.
/// - Exhausted chunks commit immutable settlement receipts with retired attempt
///   authority before commit; coordinators aggregate them asynchronously.
/// - Recovery clears the chunk's opaque claim token, fencing a worker from
///   allocating attempts or persisting results after reassignment.
pub(crate) async fn recover_expired_chunk_leases(
    db: &PgPool,
    max_recoveries: i32,
    batch_size: i64,
) -> anyhow::Result<ChunkLeaseRecoveryStats> {
    let mut tx = db.begin().await?;

    // Acquire shared movement admission before chunk locks. The following SQL
    // takes a fresh snapshot and rechecks local state after these nonblocking
    // claims, so a completed move cannot leave recovery writing the old owner.
    let candidates = sqlx::query_as::<_, (Uuid, i16)>(
        r#"
        SELECT DISTINCT run_id,run_shard FROM (
            (SELECT run_id,run_shard FROM run_chunks WHERE status = 'leased'
                AND leased_until < now() AND recovery_count < $1
                ORDER BY leased_until,run_id,id LIMIT $2)
            UNION ALL
            (SELECT run_id,run_shard FROM run_chunks WHERE status = 'leased'
                AND leased_until < now() AND recovery_count >= $1
                ORDER BY leased_until,run_id,id LIMIT $2)
        ) candidates ORDER BY run_id,run_shard
    "#,
    )
    .bind(max_recoveries)
    .bind(batch_size)
    .fetch_all(&mut *tx)
    .await?;
    let mut admitted_runs = Vec::new();
    let mut admitted_shards = Vec::new();
    for (run_id, shard) in candidates {
        let admitted: bool = sqlx::query_scalar(
            "SELECT pg_try_advisory_xact_lock_shared(hashtextextended($1::uuid::text,$2::bigint))",
        )
        .bind(run_id)
        .bind(i64::from(shard))
        .fetch_one(&mut *tx)
        .await?;
        if admitted {
            admitted_runs.push(run_id);
            admitted_shards.push(shard);
        }
    }
    let (recovered, failed_rows) = recover_expired_chunks(
        &mut tx,
        max_recoveries,
        batch_size,
        &admitted_runs,
        &admitted_shards,
    )
    .await?;

    for &(run_id, run_shard, chunk_id) in &failed_rows {
        run_shard_summary::settle_chunk_with(&mut tx, run_id, run_shard, chunk_id).await?;
    }
    tx.commit().await?;

    Ok(ChunkLeaseRecoveryStats {
        recovered_chunks: recovered,
        failed_chunks: i64::try_from(failed_rows.len())?,
    })
}

/// Starts a dispatchable run in the control database and returns its snapshot.
///
/// The returned data is copied into the execution database by
/// [`dispatch_admitted_run_window`] before worker-visible chunk events are
/// inserted. `run.started` is a control-database outbox record, so it is inserted here.
#[cfg(test)]
pub(crate) async fn prepare_dispatch_run_snapshot(
    db: &PgPool,
    route: &DispatchRoute,
    coordinator_id: Uuid,
    lease_seconds: i32,
) -> anyhow::Result<Option<DispatchRunSnapshot>> {
    let mut tx = db.begin().await?;
    let snapshot =
        prepare_dispatch_run_snapshot_with(&mut tx, route, coordinator_id, lease_seconds).await?;
    tx.commit().await?;
    Ok(snapshot)
}

/// Claims one dispatch cursor and processes a bounded chunk window for its shard.
///
/// The caller claims one open control cursor and prepares its control-owned
/// [`DispatchRunSnapshot`] in the same transaction. The execution write copies
/// that snapshot, scans only that shard's pending chunks, and then releases or
/// drains the held cursor.
///
/// Query behavior:
/// - Upserts the execution-local snapshot before worker-visible events.
/// - Keeps the control cursor claim through the execution write.
/// - Selects a bounded chunk window inside one `run_id + run_shard`, emits
///   idempotent `run.chunk.ready` events, and advances each chunk's
///   `dispatched_at` cursor in the same statement.
/// - Releases the control cursor if more undispatched chunks remain, or marks it
///   drained when that shard has no undispatched pending chunks left.
#[cfg(test)]
async fn dispatch_next_run_window(
    db: &PgPool,
    coordinator_id: Uuid,
    lease_seconds: i32,
    chunk_window_size: i64,
) -> anyhow::Result<Option<DispatchedRun>> {
    let Some(mut claim) = claim_next_dispatch_route(db, &[]).await? else {
        return Ok(None);
    };
    let route = claim.route.clone();
    let Some(snapshot) = prepare_dispatch_run_snapshot_with(
        &mut claim.control_tx,
        &route,
        coordinator_id,
        lease_seconds,
    )
    .await?
    else {
        claim.control_tx.rollback().await?;
        return Ok(None);
    };
    let execution_tx = db.begin().await?;

    Ok(dispatch_routed_run_window_with_transactions(
        claim.control_tx,
        execution_tx,
        chunk_window_size,
        &route,
        &snapshot,
    )
    .await?)
}

/// Claims and dispatches one exact run-shard through fenced write admission.
pub(crate) async fn dispatch_admitted_run_window(
    database_router: &database::DatabaseRouter,
    control_tx: Transaction<'static, Postgres>,
    execution_route: &ExecutionRoute,
    chunk_window_size: i64,
    route: &DispatchRoute,
    snapshot: &DispatchRunSnapshot,
) -> Result<Option<DispatchedRun>, RoutedDispatchError> {
    let execution_tx = database_router
        .begin_execution_admission(execution_route)
        .await
        .map_err(RoutedDispatchError::ExecutionWrite)?;

    dispatch_routed_run_window_with_transactions(
        control_tx,
        execution_tx,
        chunk_window_size,
        route,
        snapshot,
    )
    .await
}

#[cfg(test)]
async fn dispatch_routed_run_window(
    control_db: &PgPool,
    execution_db: &PgPool,
    chunk_window_size: i64,
    route: &DispatchRoute,
    snapshot: &DispatchRunSnapshot,
) -> Result<Option<DispatchedRun>, RoutedDispatchError> {
    let Some(control_tx) = claim_exact_dispatch_cursor(control_db, route).await? else {
        return Ok(None);
    };
    let execution_tx = execution_db
        .begin()
        .await
        .map_err(anyhow::Error::from)
        .map_err(RoutedDispatchError::ExecutionWrite)?;

    dispatch_routed_run_window_with_transactions(
        control_tx,
        execution_tx,
        chunk_window_size,
        route,
        snapshot,
    )
    .await
}

/// Applies a dispatch window while the caller holds shard write admission.
///
/// The route is selected from the control database before the caller resolves the
/// execution placement. This function keeps the shard-local dispatch statement
/// constrained to that routed `run_id + run_shard`.
async fn dispatch_routed_run_window_with_transactions(
    mut control_tx: Transaction<'static, Postgres>,
    mut execution_tx: Transaction<'static, Postgres>,
    chunk_window_size: i64,
    route: &DispatchRoute,
    snapshot: &DispatchRunSnapshot,
) -> Result<Option<DispatchedRun>, RoutedDispatchError> {
    validate_dispatch_snapshot_route(
        route.run_id,
        route.run_shard,
        snapshot.run_id,
        snapshot.run_shard,
    )?;

    let dispatched =
        dispatch_run_window(&mut execution_tx, chunk_window_size, route, snapshot).await?;

    if let Some(dispatched) = &dispatched {
        run_shard_summary::insert_run_shard_summary_with(
            &mut execution_tx,
            dispatched.id,
            dispatched.run_shard,
        )
        .await
        .map_err(RoutedDispatchError::ExecutionWrite)?;
    }

    execution_tx
        .commit()
        .await
        .map_err(anyhow::Error::from)
        .map_err(RoutedDispatchError::ExecutionWrite)?;

    if let Some(dispatched) = &dispatched {
        let next_status = dispatch_cursor_status(dispatched.has_remaining_chunks);
        update_dispatch_cursor(
            &mut control_tx,
            dispatched.id,
            dispatched.run_shard,
            next_status,
        )
        .await
        .map_err(RoutedDispatchError::Control)?;
        control_tx
            .commit()
            .await
            .map_err(anyhow::Error::from)
            .map_err(RoutedDispatchError::Control)?;
    } else {
        control_tx
            .rollback()
            .await
            .map_err(anyhow::Error::from)
            .map_err(RoutedDispatchError::Control)?;
    }

    Ok(dispatched.as_ref().map(DispatchedRun::from))
}

#[cfg(test)]
#[path = "run_dispatch/postgres_tests.rs"]
mod postgres_tests;
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_snapshot_must_match_the_claimed_route() {
        let run_id = Uuid::nil();
        let other_run_id = Uuid::from_u128(1);

        assert!(validate_dispatch_snapshot_route(run_id, 4, run_id, 4).is_ok());
        for (snapshot_run_id, snapshot_run_shard) in [(other_run_id, 4), (run_id, 5)] {
            assert!(matches!(
                validate_dispatch_snapshot_route(run_id, 4, snapshot_run_id, snapshot_run_shard),
                Err(RoutedDispatchError::Invariant(_))
            ));
        }
    }

    #[test]
    fn dispatch_cursor_stays_open_only_while_chunks_remain() {
        assert_eq!(dispatch_cursor_status(true), "open");
        assert_eq!(dispatch_cursor_status(false), "drained");
    }
}
