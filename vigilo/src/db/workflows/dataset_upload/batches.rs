//! Bounded, atomic checkpoints for contiguous dataset case files.
//!
//! Registration owns every exact key before remote I/O. Acknowledgement checks
//! every persisted identity and advances one contiguous verified prefix in one
//! transaction. The upload lock serializes cleanup, takeover, and checkpoints;
//! its live fence is checked after lock acquisition. No payload enters SQL and
//! no transaction spans remote work. Single metadata objects use the parent API.
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
    validate_object,
};

pub(crate) const MAX_FILES: usize = 64;
pub(crate) const MAX_PAYLOAD_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const MAX_METADATA_BYTES: usize = 256 * 1024;

/// Size the SQL argument without retaining case bytes. The array punctuation
/// is charged by callers as well as checked again at the database boundary.
pub(crate) fn metadata_bytes(object: &Object) -> Result<usize> {
    Ok(serde_json::to_vec(object)?.len() + 1)
}

fn argument(objects: &[Object]) -> Result<serde_json::Value> {
    ensure!(
        !objects.is_empty() && objects.len() <= MAX_FILES,
        "invalid batch checkpoint file count"
    );
    let mut bytes = 0_u64;
    let mut previous = None;
    let mut keys = std::collections::BTreeSet::new();
    for object in objects {
        ensure!(
            object.kind == "batch" && object.level == 0,
            "checkpoint contains non-batch object"
        );
        let start = object.range_start.context("batch start missing")?;
        let end = object.range_end.context("batch end missing")?;
        ensure!(
            start >= 0 && end > start && previous.is_none_or(|p| p == start),
            "checkpoint ranges are not contiguous"
        );
        ensure!(keys.insert(&object.object_key), "duplicate checkpoint key");
        previous = Some(end);
        bytes = bytes
            .checked_add(object.reference.byte_count)
            .context("checkpoint byte overflow")?;
    }
    ensure!(
        bytes <= MAX_PAYLOAD_BYTES,
        "checkpoint payload exceeds byte limit"
    );
    let encoded = serde_json::to_vec(objects)?;
    ensure!(
        encoded.len() <= MAX_METADATA_BYTES,
        "checkpoint metadata exceeds byte limit"
    );
    Ok(serde_json::from_slice(&encoded)?)
}

fn same_object(old: &Object, expected: &Object) -> bool {
    old.object_key == expected.object_key
        && old.reference.0 == expected.reference.0
        && old.kind == expected.kind
        && old.level == expected.level
        && old.range_start == expected.range_start
        && old.range_end == expected.range_end
}

/// Register a bounded group in one fenced transaction. Returns acknowledged
/// states in input order; any conflicting slot rolls back the entire group.
pub(crate) async fn plan(db: &PgPool, lease: Lease, objects: &[Object]) -> Result<Vec<bool>> {
    let argument = argument(objects)?;
    bounded(async {
        let mut tx = db.begin().await?;
        let upload = guard(&mut tx, lease, false).await?;
        for object in objects {
            validate_object(&upload, lease, object, false)?;
        }
        sqlx::query(REGISTER)
            .bind(lease.id)
            .bind(Json(&argument))
            .execute(&mut *tx)
            .await?;
        let keys: Vec<_> = objects.iter().map(|o| o.object_key.clone()).collect();
        let starts: Vec<_> = objects.iter().map(|o| o.range_start.unwrap()).collect();
        let old: Vec<Object> = sqlx::query_as(REPLAY)
            .bind(lease.id)
            .bind(&keys)
            .bind(&starts)
            .fetch_all(&mut *tx)
            .await?;
        ensure!(
            old.len() == objects.len() && old.iter().zip(objects).all(|(a, b)| same_object(a, b)),
            "batch checkpoint replay changed content or range"
        );
        let acknowledged = old.iter().map(|o| o.acknowledged).collect();
        tx.commit().await?;
        Ok(acknowledged)
    })
    .await
}

/// Commit only files whose complete bytes have been verified by the caller.
/// A replay may contain an acknowledged prefix followed by contiguous new files.
/// Any gap, missing key, changed identity or lost authority leaves all progress intact.
pub(crate) async fn acknowledge(db: &PgPool, lease: Lease, objects: &[Object]) -> Result<()> {
    argument(objects)?;
    bounded(async {
        let mut tx = db.begin().await?;
        let upload = guard(&mut tx, lease, false).await?;
        let keys: Vec<_> = objects.iter().map(|o| o.object_key.clone()).collect();
        let old: Vec<Object> = sqlx::query_as(LOCK)
            .bind(lease.id)
            .bind(&keys)
            .fetch_all(&mut *tx)
            .await?;
        ensure!(
            old.len() == objects.len() && old.iter().zip(objects).all(|(a, b)| same_object(a, b)),
            "batch acknowledgement identity changed or missing"
        );
        let mut cursor = upload.case_cursor;
        let mut count = 0_i32;
        for object in &old {
            let end = object.range_end.context("missing batch end")?;
            if object.acknowledged {
                ensure!(
                    count == 0 && end <= upload.case_cursor,
                    "acknowledged batch lies beyond progress"
                );
            } else {
                ensure!(
                    object.range_start == Some(cursor) && end <= upload.identity.case_count,
                    "batch acknowledgement gap, overlap or excess"
                );
                cursor = end;
                count += 1;
            }
        }
        if count > 0 {
            sqlx::query(ADVANCE)
                .bind(lease.id)
                .bind(cursor)
                .bind(count)
                .execute(&mut *tx)
                .await?;
            let changed = sqlx::query(ACKNOWLEDGE)
                .bind(lease.id)
                .bind(&keys)
                .execute(&mut *tx)
                .await?;
            ensure!(
                changed.rows_affected() == count as u64,
                "batch acknowledgement count changed"
            );
        }
        tx.commit().await?;
        Ok(())
    })
    .await
}

const REGISTER: &str = r#"
INSERT INTO dataset_upload_objects(upload_id,object_key,kind,level,range_start,range_end,reference)
SELECT $1,r.object_key,r.kind,r.level,r.range_start,r.range_end,r.reference
FROM jsonb_to_recordset($2) AS r(object_key text,kind text,level smallint,range_start integer,range_end integer,reference jsonb)
ON CONFLICT DO NOTHING
"#;
// Matching both keys and logical slots detects conflicting retries even when
// ON CONFLICT suppressed an insertion. Uniqueness bounds this to at most 128 rows.
const REPLAY: &str = r#"
SELECT * FROM dataset_upload_objects WHERE upload_id=$1
AND (object_key=ANY($2) OR (kind='batch' AND level=0 AND range_start=ANY($3)))
ORDER BY range_start LIMIT 129
"#;
const LOCK: &str = r#"
SELECT * FROM dataset_upload_objects WHERE upload_id=$1 AND object_key=ANY($2)
ORDER BY range_start LIMIT 65 FOR UPDATE
"#;
const ADVANCE: &str =
    "UPDATE dataset_uploads SET case_cursor=$2,batch_count=batch_count+$3 WHERE id=$1";
const ACKNOWLEDGE: &str = "UPDATE dataset_upload_objects SET acknowledged=true WHERE upload_id=$1 AND object_key=ANY($2) AND NOT acknowledged";
