//! Lease-fenced admission of immutable plan metadata to execution placements.
//!
//! Local commits precede control acknowledgements and are exactly replayable.
//! Only an exact, fully seeded plan can publish routes and dispatch cursors in
//! the activation transaction. Case payloads and per-case lists never enter SQL.
use std::collections::BTreeMap;

use anyhow::{
    Result,
    ensure,
};
use sqlx::{
    PgPool,
    Postgres,
    Transaction,
};
use uuid::Uuid;

use super::{
    Lease,
    bounded,
    queries,
};
use crate::db::workflows::run_shard_summary::plan::Manifest;

/// Records execution intent independently of immutable preparation. A prepared
/// run remains held until its caller requests admission; coordinators resume
/// only requested plans after process failure.
pub(crate) async fn request(db: &PgPool, id: Uuid) -> Result<()> {
    bounded(async {
        let mut tx=db.begin().await?;
        sqlx::query(queries::LOCK_RUN).bind(id).fetch_one(&mut *tx).await?;
        let n=sqlx::query("UPDATE run_plans SET execution_requested=true WHERE run_id=$1 AND EXISTS(SELECT 1 FROM runs WHERE id=$1 AND status='creating')")
            .bind(id).execute(&mut *tx).await?.rows_affected();
        ensure!(n == 1,"run is no longer creating");
        tx.commit().await?;Ok(())
    }).await
}
pub(crate) async fn claim(db: &PgPool, id: Uuid) -> Result<Lease> {
    bounded(async {
        let mut tx = db.begin().await?;
        sqlx::query(queries::LOCK_RUN)
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
        let owner = Uuid::now_v7();
        let n = sqlx::query(
            &queries::CLAIM_RUN.replace("'preparing'", "'ready' AND execution_requested"),
        )
        .bind(id)
        .bind(owner)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        ensure!(
            n == 1,
            "run admission is leased or terminal, or plan is not ready"
        );
        let generation = sqlx::query_scalar(queries::NEXT_GENERATION)
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(Lease {
            run_id: id,
            owner,
            generation,
        })
    })
    .await
}
async fn guard(tx: &mut Transaction<'_, Postgres>, lease: Lease) -> Result<()> {
    sqlx::query(queries::LOCK_RUN)
        .bind(lease.run_id)
        .fetch_one(&mut **tx)
        .await?;
    let n = sqlx::query(&queries::RENEW_RUN.replace("'preparing'", "'ready'"))
        .bind(lease.run_id)
        .bind(lease.owner)
        .bind(lease.generation)
        .execute(&mut **tx)
        .await?
        .rows_affected();
    ensure!(n == 1, "execution admission lost its live creation lease");
    Ok(())
}
pub(crate) async fn renew(db: &PgPool, lease: Lease) -> Result<()> {
    bounded(async {
        let mut tx = db.begin().await?;
        guard(&mut tx, lease).await?;
        tx.commit().await?;
        Ok(())
    })
    .await
}
/// Acknowledges one contiguous page only after every placement commits it.
pub(crate) async fn acknowledge(
    db: &PgPool,
    lease: Lease,
    start: i32,
    end: i32,
    state: &BTreeMap<i16, Manifest>,
) -> Result<()> {
    ensure!(
        end > start && state.len() <= 128,
        "invalid admission checkpoint"
    );
    bounded(async {
        let mut tx=db.begin().await?;
        guard(&mut tx,lease).await?;
        let n=sqlx::query("UPDATE run_plans SET admission_cursor=$3,admission_state=$4 WHERE run_id=$1 AND admission_cursor=$2 AND chunk_cursor >= $3")
            .bind(lease.run_id).bind(start).bind(end).bind(serde_json::to_value(state)?).execute(&mut *tx).await?.rows_affected();
        ensure!(n == 1, "admission checkpoint is not contiguous");
        tx.commit().await?; Ok(())
    }).await
}
/// Publishes all routes and cursors with pending status under live authority.
/// The caller has independently checked each local shard's exact chunk hash.
pub(crate) async fn activate(
    db: &PgPool,
    lease: Lease,
    expected: &BTreeMap<i16, Manifest>,
) -> Result<()> {
    bounded(async {
        let mut tx=db.begin().await?;
        guard(&mut tx,lease).await?;
        let (cursor,total,state):(i32,i32,serde_json::Value)=sqlx::query_as("SELECT admission_cursor,chunk_cursor,admission_state FROM run_plans WHERE run_id=$1")
            .bind(lease.run_id).fetch_one(&mut *tx).await?;
        let actual: BTreeMap<i16,Manifest>=serde_json::from_value(state)?;
        ensure!(cursor == total && total > 0 && actual == *expected, "execution placements are not fully admitted");
        let rows:Vec<(i16,i32,i32,String)>=sqlx::query_as("SELECT run_shard,case_count,chunk_count,planned_chunk_hash FROM run_plan_shards WHERE run_id=$1 ORDER BY run_shard")
            .bind(lease.run_id).fetch_all(&mut *tx).await?;
        ensure!(rows.len() == actual.len() && rows.iter().all(|(s,c,n,h)| actual.get(s).is_some_and(|p| p.case_count == *c && p.chunk_count == *n && p.hash == *h)), "admission differs from published plan");
        let assignments:Vec<super::run_create::RunShardPlacementAssignment>=sqlx::query_as::<_,(i16,String)>("SELECT run_shard,database_alias FROM run_plan_shards WHERE run_id=$1 ORDER BY run_shard").bind(lease.run_id).fetch_all(&mut *tx).await?.into_iter().map(|(run_shard,database_alias)|super::run_create::RunShardPlacementAssignment{run_shard,database_alias}).collect();
        super::run_create::bulk_insert_shard_placements(&mut tx,lease.run_id,&assignments).await?;
        let representatives=expected.values().map(|m|{
            let c=m.last.as_ref().expect("nonempty verified shard");
            crate::models::run_chunk::RunChunkDraft{chunk_id:c.chunk_id,run_shard:c.run_shard,profile_group_id:"default".into(),ordinal_start:c.ordinal_start,ordinal_end:c.ordinal_end}
        }).collect::<Vec<_>>();
        super::run_create::bulk_insert_run_shard_dispatch_cursors(&mut tx,lease.run_id,&representatives).await?;
        sqlx::query("UPDATE runs SET status='pending',coordinator_id=NULL,coordinator_leased_until=NULL,coordinator_heartbeat_at=NULL,error_message=NULL,updated_at=clock_timestamp() WHERE id=$1")
            .bind(lease.run_id).execute(&mut *tx).await?;
        tx.commit().await?; Ok(())
    }).await
}

/// Stops deterministic seed conflicts under current creation authority. A
/// cancellation or replacement owner always wins over this failure transition.
pub(crate) async fn fail(db: &PgPool, lease: Lease, error: &str) -> Result<()> {
    let error = error.chars().take(4096).collect::<String>();
    bounded(async {
        let mut tx=db.begin().await?;
        guard(&mut tx,lease).await?;
        sqlx::query("UPDATE runs SET status='failed',error_message=$2,completed_at=clock_timestamp(),coordinator_id=NULL,coordinator_leased_until=NULL,coordinator_heartbeat_at=NULL,updated_at=clock_timestamp() WHERE id=$1")
            .bind(lease.run_id).bind(error).execute(&mut *tx).await?;
        tx.commit().await?;Ok(())
    }).await
}
