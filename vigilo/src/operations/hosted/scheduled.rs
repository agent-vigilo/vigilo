//! Bounded scheduler operations for one accepted run or one routed shard.
//! Targets are hints supplied by trusted scheduling code. Database claims and
//! ownership checks govern every state change.

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    Coordinator,
    Error,
};
use crate::{
    cli::commands::coordinator,
    db::{
        scope,
        workflows::{
            run_dispatch,
            run_shard_summary::{
                self,
                PageOutcome,
            },
        },
    },
    models::run_chunk::RUN_SHARD_COUNT,
};

/// Scope for a scheduler operation on one accepted run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunTarget {
    pub tenant_id: Uuid,
    pub run_id: Uuid,
}

impl RunTarget {
    fn validate(self) -> Result<(), Error> {
        if self.run_id.is_nil() || self.tenant_id.is_nil() {
            return Err(Error::InvalidInput("invalid run target"));
        }
        Ok(())
    }
}

/// Current dispatch route and write generation for a run shard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardTarget {
    pub run: RunTarget,
    pub run_shard: i16,
    pub database_alias: String,
    pub write_epoch: i64,
}

impl ShardTarget {
    fn validate(&self) -> Result<(), Error> {
        self.run.validate()?;
        if !(0..RUN_SHARD_COUNT).contains(&self.run_shard)
            || self.database_alias.is_empty()
            || self.database_alias.trim() != self.database_alias
            || self.write_epoch <= 0
        {
            return Err(Error::InvalidInput("invalid shard target"));
        }
        Ok(())
    }
}

/// Admission can be retried after interruption. A ready run returns its current
/// placement routes even when admission was committed by an earlier call.
#[derive(Debug, PartialEq, Eq)]
pub enum AdmissionProgress {
    Deferred,
    Ready(Vec<ShardTarget>),
}

/// A committed dispatch window and whether its cursor needs another pass.
#[derive(Debug, PartialEq, Eq)]
pub struct DispatchProgress {
    pub chunks_dispatched: i64,
    pub has_more: bool,
}

/// Observable result of processing one settlement page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregationProgress {
    /// Work is busy or no eligible page exists.
    Deferred,
    /// A page committed and another page may advance the shard.
    Advanced,
    /// All currently available receipts were consumed.
    CaughtUp,
    /// Exact membership is settled. Replaying an already sealed shard returns
    /// this outcome so the caller can recover the finalization handoff.
    Sealed,
}

impl Coordinator {
    /// Advances one accepted plan within the configured creation budget.
    /// Replaying a ready run returns its current routes for dispatch.
    pub async fn admit_run(
        &self,
        run: RunTarget,
        cancel: &CancellationToken,
    ) -> Result<AdmissionProgress, Error> {
        run.validate()?;
        let _active = self.lock_operation()?;
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let progress = crate::run_admission::advance(
            &self.context,
            run.run_id,
            run.tenant_id,
            self.options.run_creation,
            cancel,
        )
        .await
        .map_err(Error::Dependency)?;
        if progress == Some(false) {
            return Ok(AdmissionProgress::Deferred);
        }
        let db = self
            .context
            .dbr()
            .await
            .map_err(Error::Dependency)?
            .control()
            .await
            .map_err(Error::Dependency)?;
        let routes = admitted_shards(db, run).await.map_err(Error::Dependency)?;
        if routes.is_empty() {
            Ok(AdmissionProgress::Deferred)
        } else {
            Ok(AdmissionProgress::Ready(routes))
        }
    }

    /// Dispatches at most one window for the specified shard. A retry advances
    /// only undispatched chunks. Stale, blocked, and drained cursors yield no work.
    pub async fn dispatch_run_shard(
        &self,
        shard: &ShardTarget,
    ) -> Result<Option<DispatchProgress>, Error> {
        shard.validate()?;
        let _active = self.lock_operation()?;
        self.dispatch_target(shard).await.map_err(Error::Dependency)
    }

    async fn dispatch_target(
        &self,
        shard: &ShardTarget,
    ) -> anyhow::Result<Option<DispatchProgress>> {
        let router = self.context.dbr().await?;
        let control = router.control().await?;
        let Some(mut claim) = run_dispatch::claim_target_dispatch_route(
            control,
            shard.run.tenant_id,
            shard.run.run_id,
            shard.run_shard,
            &shard.database_alias,
            shard.write_epoch,
        )
        .await?
        else {
            return Ok(None);
        };
        let Some(snapshot) = run_dispatch::prepare_dispatch_run_snapshot_with(
            &mut claim.control_tx,
            &claim.route,
            self.id,
            self.options.lease_seconds,
        )
        .await?
        else {
            claim.control_tx.rollback().await?;
            return Ok(None);
        };
        router
            .run_database_operation(&shard.database_alias, "dispatch_execution_write", async {
                let route = router
                    .execution_route_from_placements(
                        &snapshot.shard_placement,
                        &snapshot.database_placement,
                    )
                    .await?;
                let result = run_dispatch::dispatch_admitted_run_window(
                    router,
                    claim.control_tx,
                    &route,
                    self.options.run_chunk_dispatch_window_size,
                    &claim.route,
                    &snapshot,
                )
                .await?;
                Ok(result.map(|window| DispatchProgress {
                    chunks_dispatched: window.chunks_marked_dispatched,
                    has_more: window.has_remaining_chunks,
                }))
            })
            .await
    }

    /// Consumes at most one shard-local settlement page. A moved route must be
    /// refreshed from admission before the caller retries the write.
    pub async fn aggregate_run_shard(
        &self,
        shard: &ShardTarget,
    ) -> Result<AggregationProgress, Error> {
        shard.validate()?;
        let _active = self.lock_operation()?;
        let router = self.context.dbr().await.map_err(Error::Dependency)?;
        let result = router
            .run_database_operation(&shard.database_alias, "shard_summary_aggregation", async {
                let db = router
                    .execution_database_from_hint(&shard.database_alias)
                    .await?;
                run_shard_summary::advance_shard_summary_page(
                    &db,
                    shard.run.tenant_id,
                    shard.run.run_id,
                    shard.run_shard,
                    shard.write_epoch,
                    Some(&shard.database_alias),
                )
                .await
            })
            .await
            .map_err(Error::Dependency)?;
        Ok(match result {
            PageOutcome::Deferred => AggregationProgress::Deferred,
            PageOutcome::Advanced => AggregationProgress::Advanced,
            PageOutcome::CaughtUp => AggregationProgress::CaughtUp,
            PageOutcome::Sealed => AggregationProgress::Sealed,
        })
    }

    /// Attempts to finalize one run from its exact sealed shard summaries.
    /// Returns true when this call commits finalization.
    pub async fn finalize_run(&self, run: RunTarget) -> Result<bool, Error> {
        run.validate()?;
        let _active = self.lock_operation()?;
        coordinator::finalize_target(
            self.context.dbr().await.map_err(Error::Dependency)?,
            self.id,
            run.tenant_id,
            run.run_id,
            self.options.lease_seconds,
        )
        .await
        .map_err(Error::Dependency)
    }

    fn lock_operation(&self) -> Result<tokio::sync::MutexGuard<'_, ()>, Error> {
        self.active
            .try_lock()
            .map_err(|_| Error::Conflict("coordinator is already running"))
    }
}

async fn admitted_shards(db: &sqlx::PgPool, run: RunTarget) -> anyhow::Result<Vec<ShardTarget>> {
    let mut tx = db.begin().await?;
    scope::set_system(&mut tx, run.tenant_id, run.run_id).await?;
    let rows: Vec<(i16, String, i64)> = sqlx::query_as(
        "SELECT p.run_shard,p.database_alias,p.write_epoch
         FROM shard_placements p JOIN runs r ON r.id=p.run_id AND r.tenant_id=p.tenant_id
         WHERE r.id=$1 AND r.tenant_id=$2 AND r.status IN ('pending','running','finalizing')
         ORDER BY p.run_shard",
    )
    .bind(run.run_id)
    .bind(run.tenant_id)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(rows
        .into_iter()
        .map(|(run_shard, database_alias, write_epoch)| ShardTarget {
            run,
            run_shard,
            database_alias,
            write_epoch,
        })
        .collect())
}
