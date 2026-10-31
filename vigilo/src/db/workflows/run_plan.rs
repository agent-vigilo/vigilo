//! Control-owned preparation of immutable run plans.
//!
//! Uses the run's existing creation lease, augmented by a monotonic generation.
//! Every mutation locks the run and rechecks wall-clock authority after waiting.
//! File ownership precedes PUT and exact acknowledgement follows verification.
//! Ready publication seals compact placement metadata atomically, without making
//! work queue-visible. Execution admission owns the later activation boundary.
use anyhow::{
    Context,
    Result,
    ensure,
};
use sqlx::{
    PgPool,
    Postgres,
    Transaction,
    types::Json,
};
use uuid::Uuid;

pub(crate) mod admission;
pub(crate) mod batches;
mod queries;

use super::{
    dataset_upload::Object,
    run_create,
};
use crate::{
    contracts::{
        case_storage::*,
        evaluator_abi::EvaluatorExecutionPlan,
    },
    models::run::RunDraft,
};

#[derive(Clone, Debug, sqlx::FromRow)]
pub(crate) struct Plan {
    pub run_id: Uuid,
    pub dataset_upload_id: Uuid,
    pub status: String,
    pub storage_binding_hash: String,
    pub limits: Json<FormatLimits>,
    pub execution_plan: Json<EvaluatorExecutionPlan>,
    pub assignments: Json<Vec<run_create::RunShardPlacementAssignment>>,
    pub chunk_cursor: i32,
    pub execution_requested: bool,
    pub admission_cursor: i32,
    pub admission_state: Json<serde_json::Value>,
    pub manifest: Option<Json<ObjectRef>>,
}
#[derive(Clone, Copy)]
pub(crate) struct Lease {
    pub run_id: Uuid,
    pub owner: Uuid,
    pub generation: i64,
}
#[derive(Clone, Debug)]
pub(crate) struct Shard {
    pub run_shard: i16,
    pub database_alias: String,
    pub case_count: i32,
    pub chunk_count: i32,
    pub planned_chunk_hash: String,
    pub case_projection_hash: String,
}

async fn bounded<T>(future: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    tokio::time::timeout(std::time::Duration::from_secs(30), future).await?
}
pub(crate) async fn load(db: &PgPool, id: Uuid) -> Result<Option<Plan>> {
    bounded(async {
        Ok(sqlx::query_as(queries::SELECT_PLAN)
            .bind(id)
            .fetch_optional(db)
            .await?)
    })
    .await
}
/// Begin atomically with the run row; replay checks all frozen identities and
/// never re-resolves evaluator artifacts for an already durable creation.
/// Execution intent commits with identity; a valid creating replay can request
/// execution, but preparation-only retries never revoke an existing request.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn begin(
    db: &PgPool,
    id: Uuid,
    dataset: Uuid,
    binding: &str,
    limits: &FormatLimits,
    draft: &RunDraft,
    execution: &EvaluatorExecutionPlan,
    assignments: &[run_create::RunShardPlacementAssignment],
    execute: bool,
) -> Result<Plan> {
    limits.validate()?;
    ensure!(
        serde_json::to_vec(execution)?.len() <= 512 * 1024,
        "execution plan exceeds 512 KiB"
    );
    bounded(async {
        let mut tx = db.begin().await?;
        run_create::upsert_dataset_version(
            &mut tx,
            draft.dataset_version_id,
            draft.dataset_id,
            &draft.dataset_version,
        )
        .await?;
        // Serialize competing begins on the same public run key. Existing run
        // inserts retain their own conflict checks and cannot be overwritten.
        sqlx::query(queries::LOCK_IDENTITY)
            .bind(id.to_string())
            .execute(&mut *tx)
            .await?;
        let creating = if let Some(existing) = sqlx::query_as::<
            _,
            (String, String, Json<serde_json::Value>, String),
        >(queries::SELECT_RUN)
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?
        {
            ensure!(existing.3 != "cancelled", "run was cancelled");
            ensure!(
                existing.0 == draft.profile_hash
                    && existing.1 == draft.aggregation_policy_hash
                    && {
                        let mut snapshot = existing.2.0;
                        snapshot
                            .as_object_mut()
                            .context("run snapshot must be an object")?
                            .remove("run_plan");
                        snapshot
                            .as_object_mut()
                            .expect("object checked")
                            .remove("plan_status");
                        // Publication derives this bounded finalization read
                        // projection from the same immutable shard metadata.
                        snapshot
                            .as_object_mut()
                            .expect("object checked")
                            .remove("settlement_plan");
                        snapshot == draft.config_snapshot
                    },
                "run replay changed profile, dataset, or storage owner"
            );
            existing.3 == "creating"
        } else {
            run_create::insert_run_create(&mut tx, id, draft, "creating").await?;
            sqlx::query(queries::INSERT_PLAN)
                .bind(id)
                .bind(dataset)
                .bind(binding)
                .bind(Json(limits))
                .bind(Json(execution))
                .bind(Json(assignments))
                .bind(execute)
                .execute(&mut *tx)
                .await?;
            true
        };
        let mut row: Plan = sqlx::query_as(queries::SELECT_PLAN)
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| anyhow::anyhow!("run ID belongs to another creation workflow"))?;
        ensure!(
            row.dataset_upload_id == dataset
                && row.storage_binding_hash == binding
                && row.limits.0 == *limits
                && row.execution_plan.0.hash()? == execution.hash()?
                && row.assignments.0 == assignments,
            "run plan replay changed immutable inputs"
        );
        if execute && creating && !row.execution_requested {
            let changed = sqlx::query(queries::REQUEST_EXECUTION)
                .bind(id)
                .execute(&mut *tx)
                .await?
                .rows_affected();
            ensure!(changed == 1, "run is no longer creating");
            row.execution_requested = true;
        }
        tx.commit().await?;
        Ok(row)
    })
    .await
}
/// Claim an unleased/expired preparation and advance its generation. Ready,
/// cancelled, active and currently leased runs cannot acquire preparation authority.
pub(crate) async fn claim(db: &PgPool, id: Uuid) -> Result<Lease> {
    bounded(async {
        let owner = Uuid::now_v7();
        let mut tx = db.begin().await?;
        let live: Option<Uuid> = sqlx::query_scalar(queries::LOCK_RUN)
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?;
        ensure!(live.is_some(), "run not found");
        let claimed = sqlx::query(queries::CLAIM_RUN)
            .bind(id)
            .bind(owner)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        ensure!(claimed == 1, "run plan is leased, ready, or terminal");
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
    let n = sqlx::query(queries::RENEW_RUN)
        .bind(lease.run_id)
        .bind(lease.owner)
        .bind(lease.generation)
        .execute(&mut **tx)
        .await?
        .rows_affected();
    ensure!(n == 1, "run plan lost its live creation lease");
    Ok(())
}
/// Renew only live authority, rechecking expiry after acquiring the run lock.
pub(crate) async fn renew(db: &PgPool, lease: Lease) -> Result<()> {
    bounded(async {
        let mut tx = db.begin().await?;
        guard(&mut tx, lease).await?;
        tx.commit().await?;
        Ok(())
    })
    .await
}
/// Yield still-owned authority and retain at most 4,096 characters of error
/// context. An obsolete owner cannot clear a replacement owner's lease.
pub(crate) async fn release(db: &PgPool, lease: Lease, error: Option<&str>) -> Result<()> {
    let error = error.map(|message| message.chars().take(4096).collect::<String>());
    bounded(async {
        sqlx::query(queries::RELEASE_RUN)
            .bind(lease.run_id)
            .bind(lease.owner)
            .bind(lease.generation)
            .bind(error)
            .execute(db)
            .await?;
        Ok(())
    })
    .await
}
/// Register exact ownership before I/O. Unique range slots reject divergent
/// retries, even when changed bytes would otherwise generate another key.
pub(crate) async fn plan(db: &PgPool, lease: Lease, object: &Object) -> Result<bool> {
    Ok(batches::plan(db, lease, std::slice::from_ref(object)).await?[0])
}
/// Acknowledge one verified owned object through the bounded checkpoint contract.
pub(crate) async fn acknowledge(db: &PgPool, lease: Lease, object: &Object) -> Result<()> {
    batches::acknowledge(db, lease, std::slice::from_ref(object)).await
}
/// Read at most 256 acknowledged children after an exclusive range cursor.
/// The indexed kind/level/range boundary supports level-wise tree construction.
pub(crate) async fn objects(
    db: &PgPool,
    id: Uuid,
    kind: &str,
    level: i16,
    after: i32,
) -> Result<Vec<Object>> {
    bounded(async {
        Ok(sqlx::query_as(queries::SELECT_OBJECTS)
            .bind(id)
            .bind(kind)
            .bind(level)
            .bind(after)
            .fetch_all(db)
            .await?)
    })
    .await
}
/// One bounded page of abandoned capability probes, retained across process death.
pub(crate) async fn pending_probes(db: &PgPool, id: Uuid) -> Result<Vec<Object>> {
    bounded(async {
        Ok(sqlx::query_as(queries::PENDING_PROBES)
            .bind(id)
            .fetch_all(db)
            .await?)
    })
    .await
}
/// Publication is compact and atomic. It deliberately creates no dispatch cursor
/// or shard route: a verified plan alone is insufficient execution admission.
pub(crate) async fn publish(
    db: &PgPool,
    lease: Lease,
    manifest: &RunPlanManifest,
    root: &ObjectRef,
    shards: &[Shard],
) -> Result<()> {
    bounded(async {
        let mut tx = db.begin().await?;
        guard(&mut tx, lease).await?;
        let row: Plan = sqlx::query_as(queries::SELECT_PLAN)
            .bind(lease.run_id)
            .fetch_one(&mut *tx)
            .await?;
        let bytes = encode(manifest, &row.limits)?;
        root.verify(&bytes, &row.limits)?;
        ensure!(
            manifest.run_id == lease.run_id && shards.len() == manifest.shards.len(),
            "run manifest identity mismatch"
        );
        let (catalog, profile_hash, expected): (Json<ObjectRef>, String, i32) =
            sqlx::query_as(queries::SELECT_FROZEN_IDENTITY)
                .bind(lease.run_id)
                .fetch_one(&mut *tx)
                .await?;
        ensure!(
            root.schema == StorageFormat::RunPlanManifest
                && root.storage_alias == catalog.storage_alias
                && manifest.dataset == catalog.0
                && manifest.profile_hash == profile_hash
                && manifest.aggregation_policy_hash == row.execution_plan.aggregation_policy_hash
                && manifest.execution_plan_hash == row.execution_plan.hash()?,
            "published plan changes frozen identity"
        );
        let references=[&manifest.configuration,&manifest.chunks.object]
            .into_iter().chain(manifest.shards.iter().filter_map(|s|s.gate_counts.first.as_ref()))
            .map(|reference|serde_json::json!({"object_key":reference.key,"reference":reference})).collect::<Vec<_>>();
        let (acknowledged, missing, manifest_acknowledged): (bool, bool, bool) =
            sqlx::query_as(queries::SELECT_PUBLICATION_CHECKS)
                .bind(lease.run_id)
                .bind(Json(references))
                .bind(&root.key)
                .bind(Json(root))
                .fetch_one(&mut *tx)
                .await?;
        ensure!(acknowledged, "plan root references an unacknowledged object");
        ensure!(
            !missing && row.chunk_cursor == manifest.chunk_count,
            "incomplete run-plan object coverage"
        );
        ensure!(manifest_acknowledged, "manifest object not acknowledged");
        let mut total = 0i32;
        for (shard, expected) in shards.iter().zip(&manifest.shards) {
            ensure!(
                shard.run_shard == expected.run_shard
                    && shard.case_count == expected.case_count
                    && shard.chunk_count == expected.chunk_count
                    && shard.planned_chunk_hash == expected.planned_chunk_hash,
                "shard coverage disagrees with manifest"
            );
            ensure!(
                row.assignments
                    .iter()
                    .any(|a| a.run_shard == shard.run_shard
                        && a.database_alias == shard.database_alias),
                "plan changes frozen shard placement"
            );
            total = total
                .checked_add(shard.case_count)
                .ok_or_else(|| anyhow::anyhow!("case total overflow"))?;
        }
        let mut query=sqlx::QueryBuilder::<Postgres>::new("INSERT INTO run_plan_shards(run_id,run_shard,database_alias,case_count,chunk_count,planned_chunk_hash,case_projection_hash) ");
        query.push_values(shards,|mut row,shard|{
            row.push_bind(lease.run_id).push_bind(shard.run_shard).push_bind(&shard.database_alias)
                .push_bind(shard.case_count).push_bind(shard.chunk_count)
                .push_bind(&shard.planned_chunk_hash).push_bind(&shard.case_projection_hash);
        });
        query.build().execute(&mut *tx).await?;
        ensure!(total == expected, "run plan omits canonical cases");
        // Recheck authority after all writes; expiration or cancellation cannot
        // publish through a transaction that began with a once-valid lease.
        guard(&mut tx, lease).await?;
        sqlx::query(queries::PUBLISH_PLAN)
            .bind(lease.run_id)
            .bind(Json(root))
            .execute(&mut *tx)
            .await?;
        sqlx::query(queries::RELEASE_PUBLISHED_RUN)
            .bind(lease.run_id)
            .bind(Json(root))
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    })
    .await
}
#[cfg(test)]
#[path = "run_plan/postgres_tests.rs"]
mod postgres_tests;
