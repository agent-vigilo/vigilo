//! Transaction-scoped admission locks for execution-owned shard writes.
//!
//! Runtime claims, settlement, and admitted mover writes take the shared lock.
//! Shard movement takes the exclusive lock for source lifecycle transitions
//! and target authority installation. Target writes validate the installed
//! move generation and token while holding the shared lock. PostgreSQL releases
//! either lock with the transaction, including rollback and connection loss.

use sqlx::{
    Postgres,
    Transaction,
};
use uuid::Uuid;

pub(crate) async fn lock_shared(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    run_shard: i16,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        SELECT pg_advisory_xact_lock_shared(
            hashtextextended($1::uuid::text, $2::bigint)
        )
        "#,
    )
    .bind(run_id)
    .bind(i64::from(run_shard))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(crate) async fn lock_exclusive(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    run_shard: i16,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        SELECT pg_advisory_xact_lock(
            hashtextextended($1::uuid::text, $2::bigint)
        )
        "#,
    )
    .bind(run_id)
    .bind(i64::from(run_shard))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Acquire a bounded set of shared fences in ascending shard order, matching
/// cancellation's exclusive lock order. Each shard keeps its existing lock key.
pub(crate) async fn lock_shared_many(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    shards: &[i16],
) -> anyhow::Result<()> {
    anyhow::ensure!(
        !shards.is_empty()
            && shards.len() <= 128
            && shards.iter().all(|s| (0..128).contains(s))
            && shards.windows(2).all(|w| w[0] < w[1]),
        "invalid ordered shard fences"
    );
    sqlx::query("WITH ordered AS MATERIALIZED (SELECT shard FROM unnest($2::smallint[]) AS shard ORDER BY shard) SELECT pg_advisory_xact_lock_shared(hashtextextended($1::uuid::text,shard::bigint)) FROM ordered ORDER BY shard")
        .bind(run_id).bind(shards).execute(&mut **tx).await?;
    Ok(())
}
