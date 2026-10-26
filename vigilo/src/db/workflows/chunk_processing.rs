//! Chunk leasing and case loading workflow helpers.
//!
//! Workers use this module to claim a run chunk, load the corresponding dataset
//! case rows, and then either complete or release the chunk. Lease fields
//! and local write epochs are checked on completion/release so stale workers
//! cannot overwrite a newer lease holder or shard owner.

use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    context::database::{
        self,
        ExecutionRoute,
        ExecutionWriteRoute,
    },
    models::run_chunk::RunChunk,
};

mod queries;

use queries::{
    claim_chunk_for_processing_in_transaction,
    mark_chunk_completed,
    mark_chunk_failed,
};
pub(crate) use queries::{
    extend_chunk_lease,
    release_chunk_as_pending,
};

/// Claims a chunk through a previously resolved execution route.
pub(crate) async fn claim_routed_chunk_for_processing(
    database_router: &database::DatabaseRouter,
    route: &ExecutionRoute,
    run_id: Uuid,
    run_shard: i16,
    chunk_id: Uuid,
    lease_seconds: i32,
) -> anyhow::Result<Option<RunChunk>> {
    let mut tx = database_router.begin_execution_admission(route).await?;
    let mut chunk = claim_chunk_for_processing_in_transaction(
        &mut tx,
        run_id,
        run_shard,
        chunk_id,
        lease_seconds,
    )
    .await?;
    if let Some(chunk) = &mut chunk {
        chunk.write_epoch = route.placement.write_epoch;
    }
    tx.commit().await?;
    Ok(chunk)
}

/// Claims a chunk using a queue-carried route and local write epoch.
pub(crate) async fn claim_hinted_chunk_for_processing(
    database_router: &database::DatabaseRouter,
    route: &ExecutionWriteRoute,
    chunk_id: Uuid,
    lease_seconds: i32,
) -> anyhow::Result<Option<RunChunk>> {
    let mut tx = database_router
        .begin_execution_write_admission(route)
        .await?;
    let mut chunk = claim_chunk_for_processing_in_transaction(
        &mut tx,
        route.hint.run_id,
        route.hint.run_shard,
        chunk_id,
        lease_seconds,
    )
    .await?;
    if let Some(chunk) = &mut chunk {
        chunk.write_epoch = route.hint.write_epoch;
    }
    tx.commit().await?;
    Ok(chunk)
}

/// Dataset case row materialized for worker-side execution.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, sqlx::FromRow)]
pub(crate) struct WorkerCaseBatchItem {
    pub(crate) case_id: Uuid,
    pub(crate) case_hash: String,
    pub(crate) case_ordinal: i32,
    pub(crate) task_type: String,
    pub(crate) case_group: Option<String>,
    pub(crate) input_payload: serde_json::Value,
    pub(crate) expected_output: serde_json::Value,
    pub(crate) context_payload: serde_json::Value,
    pub(crate) tags: serde_json::Value,
    pub(crate) metadata: serde_json::Value,
}

/// Marks a leased chunk complete if the caller still owns the same lease.
///
/// Query behavior: clears an unexpired token-owned lease and writes its immutable
/// settlement receipt in the same transaction. A zero row count means the worker is
/// stale.
pub(crate) async fn mark_chunk_completed_and_settle(
    db: &PgPool,
    chunk: &RunChunk,
) -> anyhow::Result<u64> {
    let lease_token = require_chunk_lease_token(chunk)?;
    let mut tx = db.begin().await?;
    crate::db::shard_write_fence::lock_shared(&mut tx, chunk.run_id, chunk.run_shard).await?;
    let terminal_identity = mark_chunk_completed(&mut tx, chunk, lease_token).await?;
    let rows_affected = u64::from(terminal_identity.is_some());
    if let Some(identity) = terminal_identity {
        super::run_shard_summary::settle_locked_chunk_with(
            &mut tx,
            chunk.run_id,
            chunk.run_shard,
            chunk.id,
            identity,
        )
        .await?;
    }
    tx.commit().await?;

    Ok(rows_affected)
}

/// Marks a leased chunk failed if the caller still owns the lease.
///
/// Query behavior: clears the lease, makes the chunk terminal `failed`, and
/// writes a settlement receipt in one transaction. Workers use this when
/// bounded message retries are exhausted.
pub(crate) async fn mark_chunk_failed_and_settle(
    db: &PgPool,
    chunk: &RunChunk,
) -> anyhow::Result<u64> {
    let lease_token = require_chunk_lease_token(chunk)?;
    let mut tx = db.begin().await?;
    crate::db::shard_write_fence::lock_shared(&mut tx, chunk.run_id, chunk.run_shard).await?;
    let terminal_identity = mark_chunk_failed(&mut tx, chunk, lease_token).await?;
    let rows_affected = u64::from(terminal_identity.is_some());
    if let Some(identity) = terminal_identity {
        super::run_shard_summary::settle_locked_chunk_with(
            &mut tx,
            chunk.run_id,
            chunk.run_shard,
            chunk.id,
            identity,
        )
        .await?;
    }
    tx.commit().await?;

    Ok(rows_affected)
}

fn require_chunk_lease_token(chunk: &RunChunk) -> anyhow::Result<Uuid> {
    chunk.lease_token.ok_or_else(|| {
        anyhow::anyhow!(
            "leased chunk {} for run {} shard {} has no lease token",
            chunk.id,
            chunk.run_id,
            chunk.run_shard
        )
    })
}

#[cfg(test)]
mod tests {
    use chrono::{
        DateTime,
        Utc,
    };
    use uuid::Uuid;

    use super::*;

    #[test]
    fn chunk_operations_require_an_ownership_token() {
        let lease_token = Uuid::from_u128(1);
        assert_eq!(
            require_chunk_lease_token(&chunk(Some(lease_token))).unwrap(),
            lease_token
        );

        let chunk = chunk(None);
        let error = require_chunk_lease_token(&chunk).unwrap_err();
        assert!(error.to_string().contains(&chunk.id.to_string()));
        assert!(error.to_string().contains(&chunk.run_id.to_string()));
        assert!(error.to_string().contains("shard 4"));
    }

    fn chunk(lease_token: Option<Uuid>) -> RunChunk {
        let timestamp = DateTime::<Utc>::from_timestamp(0, 0).unwrap();
        RunChunk {
            write_epoch: 3,
            id: Uuid::from_u128(2),
            run_id: Uuid::from_u128(3),
            run_shard: 4,
            dataset_version_id: Uuid::from_u128(5),
            profile_group_id: "default".to_string(),
            ordinal_start: 0,
            ordinal_end: 1,
            status: "leased".to_string(),
            lease_token,
            leased_until: Some(timestamp),
            created_at: timestamp,
            updated_at: timestamp,
        }
    }
}
