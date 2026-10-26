//! Durable dataset-upload authority and compact file progress in the control DB.
//!
//! Every mutation locks one upload and checks its live generation/owner fence.
//! No transaction spans storage I/O. Objects are recorded before PUT and acknowledged
//! afterward; batch acknowledgements advance only contiguous ordinal coverage.
//! Publication atomically inserts a catalog reference and closes the upload.
//! Abort/expiry revoke authority and quarantine exact-key cleanup beyond the last
//! possible 120-second request deadline. SQL tombstones remain for retention policy.
use anyhow::{
    Result,
    ensure,
};
use serde::Serialize;
use sqlx::{
    PgPool,
    Postgres,
    Transaction,
    types::Json,
};
use uuid::Uuid;
mod queries;
// Abort/expiry SQL quarantines writes for 120 seconds. Changing the shared
// operation default requires updating and re-verifying that cleanup contract.
const _: () = assert!(crate::blob_storage::DEFAULT_OPERATION_SECONDS == 120);

use crate::contracts::case_storage::{
    DatasetManifest,
    FormatLimits,
    ObjectRef,
    identity::DatasetIdentity,
};

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub(crate) struct Upload {
    pub id: Uuid,
    pub status: String,
    pub storage_alias: String,
    pub binding_hash: String,
    pub identity: Json<DatasetIdentity>,
    pub limits: Json<FormatLimits>,
    pub case_cursor: i32,
    pub batch_count: i32,
    pub generation: i64,
    pub lease_owner: Option<Uuid>,
    pub leased_until: Option<chrono::DateTime<chrono::Utc>>,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    pub cleanup_after: Option<chrono::DateTime<chrono::Utc>>,
    pub cleaned_at: Option<chrono::DateTime<chrono::Utc>>,
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct Lease {
    pub id: Uuid,
    pub owner: Uuid,
    pub generation: i64,
}
#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct Object {
    pub object_key: String,
    pub kind: String,
    pub level: i16,
    pub range_start: Option<i32>,
    pub range_end: Option<i32>,
    pub reference: Json<ObjectRef>,
    pub acknowledged: bool,
}
#[derive(Clone, Debug, sqlx::FromRow, Serialize)]
pub(crate) struct Catalog {
    pub id: Uuid,
    pub manifest: Json<ObjectRef>,
}

/// Bound pool waits, lock waits and SQL execution as a single database operation.
async fn bounded<T>(future: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    tokio::time::timeout(std::time::Duration::from_secs(30), future).await?
}

pub(crate) async fn load(db: &PgPool, id: Uuid) -> Result<Upload> {
    bounded(async {
        Ok(sqlx::query_as::<_, Upload>(queries::SELECT_UPLOAD)
            .bind(id)
            .fetch_one(db)
            .await?)
    })
    .await
}
pub(crate) async fn catalog(db: &PgPool, id: Uuid) -> Result<Option<Catalog>> {
    bounded(async {
        Ok(sqlx::query_as(queries::SELECT_CATALOG)
            .bind(id)
            .fetch_optional(db)
            .await?)
    })
    .await
}
pub(crate) async fn reusable(
    db: &PgPool,
    alias: &str,
    binding: &str,
    identity: &DatasetIdentity,
    limits: &FormatLimits,
) -> Result<Option<Catalog>> {
    bounded(async {
        Ok(sqlx::query_as(queries::FIND_REUSABLE)
            .bind(alias)
            .bind(binding)
            .bind(&identity.dataset_hash)
            .bind(Json(limits))
            .fetch_optional(db)
            .await?)
    })
    .await
}

/// Creating/replaying an upload never changes its admitted identity or owner.
pub(crate) async fn begin(
    db: &PgPool,
    id: Uuid,
    alias: &str,
    binding: &str,
    identity: &DatasetIdentity,
    limits: &FormatLimits,
) -> Result<Upload> {
    limits.validate()?;
    crate::contracts::case_storage::validate_digest(&identity.dataset_hash)?;
    ensure!(
        identity.case_count > 0
            && identity
                .dataset_version
                .as_ref()
                .is_none_or(|s| s.len() <= 1024),
        "invalid dataset identity"
    );
    bounded(async {
        sqlx::query(queries::INSERT_UPLOAD)
            .bind(id)
            .bind(alias)
            .bind(binding)
            .bind(Json(identity))
            .bind(Json(limits))
            .execute(db)
            .await?;
        let row = load(db, id).await?;
        ensure!(
            row.storage_alias == alias
                && row.binding_hash == binding
                && row.identity.0 == *identity
                && row.limits.0 == *limits,
            "upload replay changed identity, batching, or storage owner"
        );
        Ok(row)
    })
    .await
}
pub(crate) async fn claim(db: &PgPool, id: Uuid, cleanup: bool) -> Result<Lease> {
    bounded(async {
        let owner = Uuid::now_v7();
        let generation: Option<i64> = sqlx::query_scalar(queries::CLAIM_UPLOAD)
            .bind(id)
            .bind(owner)
            .bind(cleanup)
            .fetch_optional(db)
            .await?;
        Ok(Lease {
            id,
            owner,
            generation: generation.ok_or_else(|| {
                anyhow::anyhow!(
                    "upload is leased, terminal, expired, or waiting for cleanup quarantine"
                )
            })?,
        })
    })
    .await
}
async fn guard(tx: &mut Transaction<'_, Postgres>, lease: Lease, cleanup: bool) -> Result<Upload> {
    let row = sqlx::query_as::<_, Upload>(queries::LOCK_LIVE_UPLOAD)
        .bind(lease.id)
        .bind(lease.owner)
        .bind(lease.generation)
        .bind(cleanup)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("dataset upload authority lost"))?;
    // Re-check wall-clock authority after the row lock is acquired: time can
    // advance while SELECT FOR UPDATE waits even if the row itself is unchanged.
    let renewed = sqlx::query(queries::RENEW_LIVE_UPLOAD)
        .bind(lease.id)
        .bind(lease.owner)
        .bind(lease.generation)
        .bind(cleanup)
        .execute(&mut **tx)
        .await?;
    ensure!(
        renewed.rows_affected() == 1,
        "dataset upload authority expired during lock acquisition"
    );
    Ok(row)
}
pub(crate) async fn release(db: &PgPool, lease: Lease) -> Result<()> {
    bounded(async {
        sqlx::query(queries::YIELD_UPLOAD)
            .bind(lease.id)
            .bind(lease.owner)
            .bind(lease.generation)
            .execute(db)
            .await?;
        Ok(())
    })
    .await
}

/// Record exact ownership before I/O. A replay must match the same logical slot,
/// including its bytes, range, schema and owner. SQL stores no payload or case IDs.
pub(crate) async fn plan(
    db: &PgPool,
    lease: Lease,
    object: &Object,
    cleanup: bool,
) -> Result<bool> {
    bounded(async {
        let mut tx = db.begin().await?;
        let upload = guard(&mut tx, lease, cleanup).await?;
        object.reference.validate(&upload.limits)?;
        let expected_schema = match object.kind.as_str() {
            "batch" => crate::contracts::case_storage::StorageFormat::CaseBatch,
            "index" => crate::contracts::case_storage::StorageFormat::OrdinalIndex,
            "source" => crate::contracts::case_storage::StorageFormat::DatasetSourceFields,
            "manifest" | "probe" => crate::contracts::case_storage::StorageFormat::DatasetManifest,
            _ => anyhow::bail!("unknown upload object kind"),
        };
        ensure!(
            object.reference.schema == expected_schema
                && (object.kind == "index" || object.level == 0),
            "upload object kind/schema mismatch"
        );
        if object.kind != "probe" {
            ensure!(
                object.range_start.is_some_and(|s| s >= 0)
                    && object
                        .range_end
                        .is_some_and(|e| e > object.range_start.unwrap_or(e)
                            && e <= upload.identity.case_count),
                "invalid upload object range"
            );
        }
        ensure!(
            object.reference.storage_alias == upload.storage_alias
                && object.object_key == object.reference.key,
            "object owner mismatch"
        );
        let prefix = if object.kind == "probe" {
            format!("capability-probes/{}/", lease.id)
        } else {
            format!("uploads/{}/", lease.id)
        };
        ensure!(
            object.object_key.starts_with(&prefix),
            "object key is outside upload ownership"
        );
        ensure!(
            !cleanup || object.kind == "probe",
            "cleanup cannot plan data"
        );
        sqlx::query(queries::REGISTER_OBJECT)
            .bind(lease.id)
            .bind(&object.object_key)
            .bind(&object.kind)
            .bind(object.level)
            .bind(object.range_start)
            .bind(object.range_end)
            .bind(&object.reference)
            .execute(&mut *tx)
            .await?;
        let old: Object = sqlx::query_as(queries::SELECT_OBJECT_SLOT)
            .bind(lease.id)
            .bind(&object.object_key)
            .bind(&object.kind)
            .bind(object.level)
            .bind(object.range_start)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            old.reference.0 == object.reference.0
                && old.kind == object.kind
                && old.level == object.level
                && old.range_start == object.range_start
                && old.range_end == object.range_end,
            "object replay changed stored content or range"
        );
        let acknowledged = old.acknowledged;
        tx.commit().await?;
        Ok(acknowledged)
    })
    .await
}
pub(crate) async fn acknowledge(
    db: &PgPool,
    lease: Lease,
    object: &Object,
    cleanup: bool,
) -> Result<()> {
    bounded(async {
        let mut tx = db.begin().await?;
        let upload = guard(&mut tx, lease, cleanup).await?;
        let old: Object = sqlx::query_as(queries::LOCK_OBJECT)
            .bind(lease.id)
            .bind(&object.object_key)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            old.reference.0 == object.reference.0,
            "acknowledgement identity changed"
        );
        if !old.acknowledged && old.kind == "batch" {
            ensure!(
                old.range_start == Some(upload.case_cursor)
                    && old
                        .range_end
                        .is_some_and(|e| e <= upload.identity.case_count),
                "batch acknowledgement gap, overlap or excess"
            );
            sqlx::query(queries::ADVANCE_PROGRESS)
                .bind(lease.id)
                .bind(old.range_end)
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query(queries::ACKNOWLEDGE_OBJECT)
            .bind(lease.id)
            .bind(&object.object_key)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    })
    .await
}
pub(crate) async fn objects(
    db: &PgPool,
    id: Uuid,
    kind: &str,
    level: i16,
    after: i32,
) -> Result<Vec<Object>> {
    bounded(async {
        Ok(sqlx::query_as(queries::SELECT_OBJECT_PAGE)
            .bind(id)
            .bind(kind)
            .bind(level)
            .bind(after)
            .fetch_all(db)
            .await?)
    })
    .await
}
pub(crate) async fn pending_cleanup(
    db: &PgPool,
    id: Uuid,
    probes_only: bool,
    limit: i64,
) -> Result<Vec<Object>> {
    ensure!((1..=256).contains(&limit), "invalid cleanup page limit");
    bounded(async {
        Ok(sqlx::query_as(queries::SELECT_CLEANUP_PAGE)
            .bind(id)
            .bind(probes_only)
            .bind(limit)
            .fetch_all(db)
            .await?)
    })
    .await
}
pub(crate) async fn cleaned(db: &PgPool, lease: Lease, key: &str, cleanup: bool) -> Result<()> {
    bounded(async {
        let mut tx = db.begin().await?;
        guard(&mut tx, lease, cleanup).await?;
        sqlx::query(queries::MARK_OBJECT_CLEANED)
            .bind(lease.id)
            .bind(key)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    })
    .await
}
pub(crate) async fn finalizing(db: &PgPool, lease: Lease) -> Result<()> {
    bounded(async {
        let mut tx = db.begin().await?;
        let row = guard(&mut tx, lease, false).await?;
        ensure!(
            row.case_cursor == row.identity.case_count,
            "dataset upload is incomplete"
        );
        sqlx::query(queries::BEGIN_FINALIZATION)
            .bind(lease.id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    })
    .await
}

/// Publish only a complete manifest under current authority. File and membership
/// verification precedes this transaction; neither a stale lease nor abort can win.
pub(crate) async fn publish(
    db: &PgPool,
    lease: Lease,
    manifest: &DatasetManifest,
    reference: &ObjectRef,
) -> Result<Catalog> {
    bounded(async {
        let mut tx = db.begin().await?;
        let row = guard(&mut tx, lease, false).await?;
        crate::contracts::case_storage::StorageDocument::validate(manifest, &row.limits)?;
        reference.validate(&row.limits)?;
        let encoded = crate::contracts::case_storage::encode(manifest, &row.limits)?;
        reference.verify(&encoded, &row.limits)?;
        ensure!(
            manifest.batch_limits == row.limits.batch
                && manifest.dataset_version == row.identity.dataset_version
                && manifest.source_fields.storage_alias == row.storage_alias
                && reference.schema
                    == crate::contracts::case_storage::StorageFormat::DatasetManifest,
            "manifest policy/owner mismatch"
        );
        ensure!(
            row.status == "finalizing" && row.case_cursor == row.identity.case_count,
            "upload is not finalizable"
        );
        ensure!(
            manifest.dataset_id == row.identity.dataset_id
                && manifest.dataset_version_id == row.identity.dataset_version_id
                && manifest.dataset_hash == row.identity.dataset_hash
                && manifest.case_count == row.identity.case_count,
            "manifest identity disagrees with upload"
        );
        let ready: bool = sqlx::query_scalar(queries::CHECK_PUBLICATION_READY)
            .bind(lease.id)
            .bind(Json(reference))
            .bind(Json(&manifest.source_fields))
            .bind(Json(&manifest.batches.object))
            .fetch_one(&mut *tx)
            .await?;
        ensure!(ready, "unverified upload objects remain");
        sqlx::query(queries::INSERT_CATALOG)
            .bind(lease.id)
            .bind(&row.storage_alias)
            .bind(&row.binding_hash)
            .bind(&manifest.dataset_hash)
            .bind(manifest.dataset_version_id)
            .bind(&row.limits)
            .bind(Json(reference))
            .execute(&mut *tx)
            .await?;
        let finalized = sqlx::query(queries::FINALIZE_UPLOAD)
            .bind(lease.id)
            .bind(lease.owner)
            .bind(lease.generation)
            .execute(&mut *tx)
            .await?;
        ensure!(
            finalized.rows_affected() == 1,
            "dataset publication authority expired before commit"
        );
        tx.commit().await?;
        Ok(Catalog {
            id: lease.id,
            manifest: Json(reference.clone()),
        })
    })
    .await
}
pub(crate) async fn abort(db: &PgPool, id: Uuid) -> Result<()> {
    bounded(async {
        let result = sqlx::query(queries::ABORT_UPLOAD)
            .bind(id)
            .execute(db)
            .await?;
        ensure!(
            result.rows_affected() == 1 || load(db, id).await?.status == "aborted",
            "finalized or expired upload cannot be aborted"
        );
        Ok(())
    })
    .await
}
/// One bounded expiry page per invocation; an operator/job scheduler repeats
/// cleanup. Expiry and cleanup never enlist a coordinator or scan blob buckets.
pub(crate) async fn expire(db: &PgPool) -> Result<Vec<Uuid>> {
    bounded(async {
        Ok(sqlx::query_scalar(queries::EXPIRE_UPLOADS)
            .fetch_all(db)
            .await?)
    })
    .await
}
pub(crate) async fn finish_cleanup(db: &PgPool, lease: Lease) -> Result<bool> {
    bounded(async {
        let mut tx = db.begin().await?;
        guard(&mut tx, lease, true).await?;
        let remaining: bool = sqlx::query_scalar(queries::CHECK_CLEANUP_PENDING)
            .bind(lease.id)
            .fetch_one(&mut *tx)
            .await?;
        if !remaining {
            sqlx::query(queries::FINISH_CLEANUP)
                .bind(lease.id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(!remaining)
    })
    .await
}

#[cfg(test)]
mod postgres_tests;

/// Mint remote budgets before this renewal. A paused former owner cannot start
/// I/O after the live lease window because its monotonic request deadline expires.
pub(crate) async fn renew(db: &PgPool, lease: Lease, cleanup: bool) -> Result<()> {
    bounded(async {
        let changed = sqlx::query(queries::RENEW_LIVE_UPLOAD)
            .bind(lease.id)
            .bind(lease.owner)
            .bind(lease.generation)
            .bind(cleanup)
            .execute(db)
            .await?;
        ensure!(
            changed.rows_affected() == 1,
            "dataset upload authority lost"
        );
        Ok(())
    })
    .await
}
