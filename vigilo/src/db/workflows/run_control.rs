//! Durable, revision-fenced execution control delivery.
//!
//! The control transaction changes one run and records its physical destinations.
//! Coordinators claim bounded deliveries, release the control connection before
//! remote I/O, and acknowledge only the claimed revision after the local commit.
//! Worker quiescence is observed separately from application. Retries never undo a
//! newer revision and a disconnected caller cannot discard committed intent.
//! Each delivery follow-up binds its claimed identity on an owned transaction;
//! no control transaction stays open across execution-database I/O.
use anyhow::{
    Context,
    ensure,
};
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

use crate::{
    context::database::DatabaseRouter,
    operations::{
        ControlOutcome,
        ExecutionControl,
    },
};

#[derive(sqlx::FromRow)]
struct Delivery {
    tenant_id: Uuid,
    run_id: Uuid,
    database_alias: String,
    revision: i64,
    execution_control: ExecutionControl,
    claim_token: Uuid,
    applied: bool,
}

/// Records a transition and its destinations under the same run-row lock used by
/// admission. Repeated requests preserve the revision and outstanding retries.
pub(crate) async fn request(
    pool: &PgPool,
    run_id: Uuid,
    next: ExecutionControl,
) -> anyhow::Result<ControlOutcome> {
    let mut tx = pool.begin().await?;
    let (tenant, previous, revision, status): (Uuid, ExecutionControl, i64, String) = sqlx::query_as(
        "SELECT tenant_id,execution_control,control_revision,status::text FROM runs WHERE id=$1 AND tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid FOR UPDATE")
        .bind(run_id).fetch_optional(&mut *tx).await?.context("accessible run not found")?;
    ensure!(
        previous.permits_transition(next),
        "revoked execution cannot be restored"
    );
    ensure!(
        matches!(
            status.as_str(),
            "creating" | "pending" | "running" | "finalizing"
        ) || previous == next,
        "terminal run cannot change execution control"
    );
    if previous != next {
        let revision = revision
            .checked_add(1)
            .context("control revision exhausted")?;
        sqlx::query("UPDATE runs SET execution_control=$2,control_revision=$3,control_reason=CASE WHEN $2='revoked' THEN 'run execution permission revoked' ELSE NULL END,updated_at=clock_timestamp() WHERE id=$1")
            .bind(run_id).bind(next).bind(revision).execute(&mut *tx).await?;
        enqueue(&mut tx, tenant, run_id, revision, next).await?;
    }
    let result = progress_with(&mut *tx, run_id).await?;
    tx.commit().await?;
    Ok(result)
}

async fn enqueue(
    tx: &mut Transaction<'_, Postgres>,
    tenant: Uuid,
    run: Uuid,
    revision: i64,
    control: ExecutionControl,
) -> anyhow::Result<()> {
    // Frozen assignments cover admission not yet published as shard placements;
    // active movement adds both ends until the move is completely settled.
    sqlx::query(r#"
        INSERT INTO run_control_deliveries(tenant_id,run_id,database_alias,revision,execution_control)
        SELECT $1,$2,alias,$3,$4 FROM (
            SELECT database_alias AS alias FROM shard_placements WHERE run_id=$2
            UNION SELECT a->>'database_alias' FROM run_plans p CROSS JOIN LATERAL jsonb_array_elements(p.assignments) a WHERE p.run_id=$2
            UNION SELECT source_database_alias FROM shard_move_operations WHERE run_id=$2 AND status='active'
            UNION SELECT target_database_alias FROM shard_move_operations WHERE run_id=$2 AND status='active'
        ) destinations
        ON CONFLICT (tenant_id,run_id,database_alias) DO UPDATE SET
            revision=EXCLUDED.revision,execution_control=EXCLUDED.execution_control,
            applied_at=NULL,quiescent_at=NULL,available_at=clock_timestamp(),
            attempts=0,claim_token=NULL,claimed_until=NULL,last_error=NULL
        WHERE run_control_deliveries.revision < EXCLUDED.revision
    "#).bind(tenant).bind(run).bind(revision).bind(control).execute(&mut **tx).await?;
    Ok(())
}

async fn progress_with<'e, E>(executor: E, run: Uuid) -> anyhow::Result<ControlOutcome>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    let (execution_control,revision,status,settled_revision,placement_count,applied_placement_count,quiescent_placement_count):
        (ExecutionControl,i64,String,i64,i64,i64,i64) = sqlx::query_as(r#"
        SELECT r.execution_control,r.control_revision,r.status::text,r.control_settled_revision,
               count(d.database_alias),count(d.applied_at),count(d.quiescent_at)
        FROM runs r LEFT JOIN run_control_deliveries d ON d.tenant_id=r.tenant_id AND d.run_id=r.id AND d.revision=r.control_revision
        WHERE r.id=$1 GROUP BY r.id
    "#).bind(run).fetch_one(executor).await?;
    Ok(ControlOutcome {
        run_id: run,
        execution_control,
        revision,
        placement_count,
        applied_placement_count,
        quiescent_placement_count,
        settled: matches!(status.as_str(), "completed" | "cancelled" | "failed")
            && (execution_control != ExecutionControl::Revoked || settled_revision == revision),
    })
}

/// One bounded shared scheduler pass; no scan or pool per tenant or case.
pub(crate) async fn deliver(
    router: &DatabaseRouter,
    run_id: Option<Uuid>,
) -> anyhow::Result<Vec<super::run_cancel::CancelRunOutcome>> {
    let pool = router.control().await?;
    let deliveries: Vec<Delivery> =
        sqlx::query_as("SELECT * FROM vigilo_claim_control_deliveries($1)")
            .bind(run_id)
            .fetch_all(pool)
            .await?;
    let outcomes = stream::iter(deliveries).map(|delivery| async move {
        let applied = router.run_database_operation(&delivery.database_alias,"run_control_delivery",async {
            let local = router.execution_database(&delivery.database_alias).await?;
            if delivery.applied {
                observe_quiescence(&local, &delivery).await
            } else {
                apply(&local, &delivery).await
            }
        }).await;
        match applied {
            Ok(quiescent) => {
                acknowledge(pool, &delivery, quiescent).await?;
            }
            Err(error) => {
                tracing::warn!(run_id=%delivery.run_id,database_alias=%delivery.database_alias,error=%error,"execution control delivery deferred");
                defer(pool, &delivery).await?;
            }
        }
        Ok::<_,anyhow::Error>(())
    }).buffer_unordered(16).collect::<Vec<_>>().await;
    for outcome in outcomes {
        outcome?;
    }
    settle_revocations(router, run_id).await
}

async fn apply(pool: &PgPool, delivery: &Delivery) -> anyhow::Result<bool> {
    let mut tx = pool.begin().await?;
    // The fixed lock statement follows scope installation in the same submission.
    // Only canonical UUIDs enter this SQL; arbitrary fields remain bound below.
    let sql = format!(
        "{}SELECT control_revision,execution_control FROM runs WHERE tenant_id='{}'::uuid AND id='{}'::uuid FOR UPDATE",
        crate::db::scope::system_settings(delivery.tenant_id, delivery.run_id),
        delivery.tenant_id,
        delivery.run_id,
    );
    let row = sqlx::Executor::fetch_optional(&mut *tx, sql.as_str()).await?;
    let local = row
        .as_ref()
        .map(<(i64, ExecutionControl) as sqlx::FromRow<_>>::from_row)
        .transpose()?;
    // No local root means no admitted work. Canonical control fences admission;
    // no existing queue work can claim a nonexistent chunk.
    let Some((revision, state)) = local else {
        tx.commit().await?;
        return Ok(true);
    };
    ensure!(
        revision != delivery.revision || state == delivery.execution_control,
        "control revision has conflicting contents"
    );
    if revision <= delivery.revision {
        sqlx::query("UPDATE runs SET execution_control=$3,control_revision=$4,control_reason=CASE WHEN $3='revoked' THEN 'run execution permission revoked' ELSE NULL END WHERE tenant_id=$1 AND id=$2 AND control_revision<$4")
            .bind(delivery.tenant_id).bind(delivery.run_id).bind(delivery.execution_control).bind(delivery.revision).execute(&mut *tx).await?;
    }
    let (revision, state) = if revision > delivery.revision {
        (revision, state)
    } else {
        (delivery.revision, delivery.execution_control)
    };
    // A move can copy rows after an earlier delivery. Reconcile those rows to
    // the newest local root; a stale retry must never repair them backwards.
    sqlx::query("UPDATE run_chunks SET execution_control=$3,control_revision=$4 WHERE tenant_id=$1 AND run_id=$2 AND control_revision<$4")
        .bind(delivery.tenant_id).bind(delivery.run_id).bind(state).bind(revision).execute(&mut *tx).await?;
    let quiet:bool = sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM run_chunks WHERE tenant_id=$1 AND run_id=$2 AND status='leased' AND leased_until>=clock_timestamp())")
        .bind(delivery.tenant_id).bind(delivery.run_id).fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok(quiet)
}

/// Retries terminal cleanup only after every recorded destination is quiescent.
/// Cleanup is idempotent and an interrupted acknowledgment remains selectable.
async fn settle_revocations(
    router: &DatabaseRouter,
    run_id: Option<Uuid>,
) -> anyhow::Result<Vec<super::run_cancel::CancelRunOutcome>> {
    let pool = router.control().await?;
    let runs: Vec<(Uuid, i64, Uuid)> =
        sqlx::query_as("SELECT * FROM vigilo_claim_revocation_settlements($1)")
            .bind(run_id)
            .fetch_all(pool)
            .await?;
    let outcomes = stream::iter(runs).map(|(run,revision,token)| async move {
        match super::run_cancel::cancel_run_routed(router,run).await {
            Ok(Some(outcome)) => {
                acknowledge_settlement(pool, run, revision, token).await?;
                Ok(Some(outcome))
            }
            outcome => {
                tracing::warn!(run_id=%run,error=?outcome,"revocation settlement deferred");
                sqlx::query("UPDATE runs SET updated_at=clock_timestamp(),control_settlement_until=clock_timestamp()+interval '10 seconds' WHERE id=$1 AND control_revision=$2 AND control_settlement_token=$3")
                    .bind(run).bind(revision).bind(token).execute(pool).await?;
                Ok::<_,anyhow::Error>(None)
            }
        }
    }).buffer_unordered(8).collect::<Vec<_>>().await;
    let mut settled = Vec::new();
    for outcome in outcomes {
        if let Some(outcome) = outcome? {
            settled.push(outcome);
        }
    }

    Ok(settled)
}

/// A stale cleanup owner cannot acknowledge a replacement claim or revision.
async fn acknowledge_settlement(
    pool: &PgPool,
    run: Uuid,
    revision: i64,
    token: Uuid,
) -> anyhow::Result<bool> {
    let result = sqlx::query("UPDATE runs SET control_settled_revision=$2,control_settlement_token=NULL,control_settlement_until=NULL WHERE id=$1 AND control_revision=$2 AND execution_control='revoked' AND control_settlement_token=$3")
        .bind(run).bind(revision).bind(token).execute(pool).await?;
    Ok(result.rows_affected() == 1)
}

#[cfg(test)]
#[path = "run_control_tests.rs"]
mod postgres_tests;

/// Reconciles copied state before a destination admits work. Invoked after the
/// last copy/replay, including ambiguous cutover recovery. Concurrent newer
/// delivery wins through the same revision fence.
pub(crate) async fn synchronize_destination(
    control: &PgPool,
    target: &PgPool,
    run_id: Uuid,
) -> anyhow::Result<()> {
    let (tenant_id, revision, execution_control): (Uuid, i64, ExecutionControl) =
        sqlx::query_as("SELECT tenant_id,control_revision,execution_control FROM runs WHERE id=$1")
            .bind(run_id)
            .fetch_one(control)
            .await?;
    if revision > 0 {
        apply(
            target,
            &Delivery {
                tenant_id,
                run_id,
                database_alias: String::new(),
                revision,
                execution_control,
                claim_token: Uuid::nil(),
                applied: false,
            },
        )
        .await?;
    }
    Ok(())
}

/// Adds newly published placements to the same durable revision while holding
/// the canonical run lock. Concurrent requests either include the route or run
/// after this transaction; neither can miss the destination.
pub(crate) async fn include_current_destinations(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
) -> anyhow::Result<()> {
    let (tenant, revision, state): (Uuid, i64, ExecutionControl) = sqlx::query_as(
        "SELECT tenant_id,control_revision,execution_control FROM runs WHERE id=$1 FOR UPDATE",
    )
    .bind(run_id)
    .fetch_one(&mut **tx)
    .await?;
    if revision > 0 {
        enqueue(tx, tenant, run_id, revision, state).await?;
    }
    Ok(())
}

/// Read-only control progress. Canonical lifecycle status alone does not attest
/// that all cross-database cleanup and its acknowledgment have committed.
pub(crate) async fn progress(pool: &PgPool, run_id: Uuid) -> anyhow::Result<ControlOutcome> {
    progress_with(pool, run_id).await
}

/// Already-applied destinations only need a lease observation, not another
/// mass update. Lost application acknowledgments still replay the full apply.
async fn observe_quiescence(pool: &PgPool, delivery: &Delivery) -> anyhow::Result<bool> {
    let mut tx = pool.begin().await?;
    let sql = format!(
        "{}SELECT NOT EXISTS(SELECT 1 FROM run_chunks WHERE tenant_id='{}'::uuid AND run_id='{}'::uuid AND status='leased' AND leased_until>=clock_timestamp())",
        crate::db::scope::system_settings(delivery.tenant_id, delivery.run_id),
        delivery.tenant_id,
        delivery.run_id,
    );
    let row = sqlx::Executor::fetch_one(&mut *tx, sql.as_str()).await?;
    let (quiet,) = <(bool,) as sqlx::FromRow<_>>::from_row(&row)?;
    tx.commit().await?;
    Ok(quiet)
}

/// Acknowledge only the revision and lease actually applied. Lost acknowledgments
/// replay safely; a delayed sender cannot clear a replacement claim or revision.
async fn acknowledge(pool: &PgPool, delivery: &Delivery, quiescent: bool) -> anyhow::Result<bool> {
    let mut tx = delivery.begin(pool).await?;
    let result = sqlx::query("UPDATE run_control_deliveries SET applied_at=coalesce(applied_at,clock_timestamp()),quiescent_at=CASE WHEN $6 THEN clock_timestamp() ELSE NULL END,claim_token=NULL,claimed_until=NULL,available_at=clock_timestamp()+interval '2 seconds',last_error=NULL WHERE tenant_id=$1 AND run_id=$2 AND database_alias=$3 AND revision=$4 AND claim_token=$5")
        .bind(delivery.tenant_id).bind(delivery.run_id).bind(&delivery.database_alias).bind(delivery.revision).bind(delivery.claim_token).bind(quiescent).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(result.rows_affected() == 1)
}

impl Delivery {
    /// Scope one parameterized acknowledgement or retry mutation to its claim.
    /// SQLx owns the transaction before setup, so cancellation rolls it back.
    async fn begin(&self, pool: &PgPool) -> Result<Transaction<'static, Postgres>, sqlx::Error> {
        let mut tx = pool.begin().await?;
        sqlx::Executor::execute(
            &mut *tx,
            crate::db::scope::system_settings(self.tenant_id, self.run_id).as_str(),
        )
        .await?;
        Ok(tx)
    }
}

/// Defer only this revision/token; a newer claim cannot be cleared by an old sender.
/// Persist a bounded non-secret reason; full diagnostics stay in trusted logs.
async fn defer(pool: &PgPool, delivery: &Delivery) -> anyhow::Result<bool> {
    let mut tx = delivery.begin(pool).await?;
    let result=sqlx::query("UPDATE run_control_deliveries SET claim_token=NULL,claimed_until=NULL,available_at=clock_timestamp()+least(attempts,30)*interval '1 second',last_error='placement control delivery unavailable' WHERE tenant_id=$1 AND run_id=$2 AND database_alias=$3 AND revision=$4 AND claim_token=$5")
        .bind(delivery.tenant_id).bind(delivery.run_id).bind(&delivery.database_alias).bind(delivery.revision).bind(delivery.claim_token).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(result.rows_affected() == 1)
}
