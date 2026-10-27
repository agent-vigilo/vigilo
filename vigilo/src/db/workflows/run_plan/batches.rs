//! Atomic ownership and acknowledgement of bounded plan-file groups.
//!
//! The run lock serializes registration, checkpoints, cancellation and takeover.
//! Every exact reference remains durable before remote I/O; replay compares all
//! fields and acknowledgements advance only contiguous chunk coverage. Callers
//! verify complete bytes before ACK. No transaction includes blob operations.
use anyhow::{
    Context,
    Result,
    ensure,
};
use sqlx::{
    PgPool,
    types::Json,
};

use super::{
    Lease,
    Object,
    bounded,
    guard,
    queries,
};

pub(crate) const MAX_FILES: usize = 64;
pub(crate) const MAX_METADATA_BYTES: usize = 256 * 1024;
pub(crate) const GROUP_BYTES: u64 = 8 * 1024 * 1024;

fn argument(objects: &[Object]) -> Result<serde_json::Value> {
    ensure!(
        !objects.is_empty() && objects.len() <= MAX_FILES,
        "invalid plan checkpoint file count"
    );
    let mut keys = std::collections::BTreeSet::new();
    let mut slots = std::collections::BTreeSet::new();
    let mut bytes = 0_u64;
    for object in objects {
        ensure!(
            keys.insert(&object.object_key),
            "duplicate plan checkpoint key"
        );
        if let Some(start) = object.range_start {
            ensure!(
                slots.insert((&object.kind, object.level, start)),
                "duplicate plan checkpoint slot"
            );
        }
        bytes = bytes
            .checked_add(object.reference.byte_count)
            .context("plan checkpoint byte overflow")?;
    }
    // Large, individually format-admitted documents are handled alone.
    ensure!(
        objects.len() == 1 || bytes <= GROUP_BYTES,
        "plan checkpoint payload exceeds group bound"
    );
    let bytes = serde_json::to_vec(objects)?;
    ensure!(
        bytes.len() <= MAX_METADATA_BYTES,
        "plan checkpoint metadata exceeds bound"
    );
    Ok(serde_json::from_slice(&bytes)?)
}

fn same(a: &Object, b: &Object) -> bool {
    a.object_key == b.object_key
        && a.kind == b.kind
        && a.level == b.level
        && a.range_start == b.range_start
        && a.range_end == b.range_end
        && a.reference.0 == b.reference.0
}

/// Register all keys atomically and return replay states in input order. A
/// conflicting key or logical slot rolls back every new row in the group.
pub(crate) async fn plan(db: &PgPool, lease: Lease, objects: &[Object]) -> Result<Vec<bool>> {
    let argument = argument(objects)?;
    bounded(async {
        let mut tx = db.begin().await?;
        guard(&mut tx, lease).await?;
        let (limits, alias): (Json<crate::contracts::case_storage::FormatLimits>, String) =
            sqlx::query_as(queries::SELECT_OWNER)
                .bind(lease.run_id)
                .fetch_one(&mut *tx)
                .await?;
        for object in objects {
            object.reference.validate(&limits)?;
            ensure!(
                object.reference.storage_alias == alias,
                "run-plan object changes storage owner"
            );
            ensure!(
                object.object_key == object.reference.key
                    && (object
                        .object_key
                        .starts_with(&format!("runs/{}/", lease.run_id))
                        || (object.kind == "probe"
                            && object
                                .object_key
                                .starts_with(&format!("capability-probes/{}/", lease.run_id)))),
                "unowned run-plan object key"
            );
        }
        sqlx::query(REGISTER)
            .bind(lease.run_id)
            .bind(Json(&argument))
            .execute(&mut *tx)
            .await?;
        let old: Vec<Object> = sqlx::query_as(REPLAY)
            .bind(lease.run_id)
            .bind(Json(&argument))
            .fetch_all(&mut *tx)
            .await?;
        ensure!(
            old.len() == objects.len() && old.iter().zip(objects).all(|(a, b)| same(a, b)),
            "run-plan replay changed reference or range"
        );
        tx.commit().await?;
        Ok(old.iter().map(|o| o.acknowledged).collect())
    })
    .await
}

/// Acknowledge only a completely verified group. Mixed replay preserves the
/// acknowledged prefix; gaps, changed references and stale owners change nothing.
pub(crate) async fn acknowledge(db: &PgPool, lease: Lease, objects: &[Object]) -> Result<()> {
    let argument = argument(objects)?;
    bounded(async {
        let mut tx = db.begin().await?;
        guard(&mut tx, lease).await?;
        let old: Vec<Object> = sqlx::query_as(LOCK).bind(lease.run_id).bind(Json(&argument)).fetch_all(&mut *tx).await?;
        ensure!(old.len() == objects.len() && old.iter().zip(objects).all(|(a,b)| same(a,b)), "plan acknowledgement changed or missing reference");
        if old.iter().any(|o| o.kind == "chunks") {
            let start: i32 = sqlx::query_scalar("SELECT chunk_cursor FROM run_plans WHERE run_id=$1").bind(lease.run_id).fetch_one(&mut *tx).await?;
            let mut cursor = start;
            for object in old.iter().filter(|o| o.kind == "chunks") {
                let end = object.range_end.context("chunk range end missing")?;
                if object.acknowledged {
                    ensure!(end <= start, "acknowledged chunk beyond progress");
                } else {
                    ensure!(object.range_start == Some(cursor), "run chunk acknowledgement gap or overlap");
                    cursor = end;
                }
            }
            if cursor != start {
                let n = sqlx::query(queries::ADVANCE_CHUNKS).bind(lease.run_id).bind(start).bind(cursor).execute(&mut *tx).await?.rows_affected();
                ensure!(n == 1, "run chunk progress changed");
            }
        }
        let keys: Vec<_> = objects.iter().map(|o| &o.object_key).collect();
        let changed = sqlx::query("UPDATE run_plan_objects SET acknowledged=true WHERE run_id=$1 AND object_key=ANY($2) AND NOT acknowledged")
            .bind(lease.run_id).bind(keys).execute(&mut *tx).await?.rows_affected();
        ensure!(changed as usize == old.iter().filter(|o| !o.acknowledged).count(), "plan acknowledgement count changed");
        tx.commit().await?;
        Ok(())
    }).await
}

const REGISTER: &str = "INSERT INTO run_plan_objects(run_id,object_key,kind,level,range_start,range_end,reference) SELECT $1,r.object_key,r.kind,r.level,r.range_start,r.range_end,r.reference FROM jsonb_to_recordset($2) AS r(object_key text,kind text,level smallint,range_start integer,range_end integer,reference jsonb) ON CONFLICT DO NOTHING";
const REPLAY: &str = "SELECT a.object_key,a.kind,a.level,a.range_start,a.range_end,a.reference,a.acknowledged FROM jsonb_array_elements($2) WITH ORDINALITY AS e(value,position) JOIN run_plan_objects a ON a.run_id=$1 AND a.object_key=e.value->>'object_key' ORDER BY e.position";
const LOCK: &str = "SELECT a.object_key,a.kind,a.level,a.range_start,a.range_end,a.reference,a.acknowledged FROM jsonb_array_elements($2) WITH ORDINALITY AS e(value,position) JOIN run_plan_objects a ON a.run_id=$1 AND a.object_key=e.value->>'object_key' ORDER BY e.position FOR UPDATE OF a";
