//! administration queries for shard administration.

use super::super::*;

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub(in crate::db::workflows::shard_admin) struct DatabaseIdentity {
    database_name: String,
    server_address: Option<String>,
    server_port: Option<i32>,
    postmaster_started_at: DateTime<Utc>,
}

const DATABASE_IDENTITY_SQL: &str = r#"
    SELECT
        current_database() AS database_name,
        inet_server_addr()::text AS server_address,
        inet_server_port() AS server_port,
        pg_postmaster_start_time() AS postmaster_started_at
"#;

pub(in crate::db::workflows::shard_admin) async fn select_database_identity(
    db: &PgPool,
) -> anyhow::Result<DatabaseIdentity> {
    sqlx::query_as(DATABASE_IDENTITY_SQL)
        .fetch_one(db)
        .await
        .map_err(Into::into)
}

#[derive(Debug, Clone, Copy, sqlx::FromRow)]
pub(in crate::db::workflows::shard_admin) struct DatabasePlacementReferences {
    pub(in crate::db::workflows::shard_admin) route_count: i64,
    pub(in crate::db::workflows::shard_admin) creation_count: i64,
    pub(in crate::db::workflows::shard_admin) rebalance_count: i64,
}

pub(in crate::db::workflows::shard_admin) async fn select_database_placement_references(
    tx: &mut Transaction<'_, Postgres>,
    alias: &str,
) -> anyhow::Result<DatabasePlacementReferences> {
    sqlx::query_as(
        r#"
        SELECT
            (SELECT COUNT(*)::bigint
             FROM shard_placements
             WHERE database_alias = $1) AS route_count,
            (SELECT COUNT(*)::bigint
             FROM run_plan_shards creation
             JOIN runs run ON run.id = creation.run_id
             WHERE creation.database_alias = $1
               AND run.status = 'creating'::run_status) AS creation_count,
            (SELECT COUNT(*)::bigint
             FROM shard_rebalance_items item
             JOIN shard_rebalance_operations operation
               ON operation.id = item.operation_id
             WHERE operation.status IN ('planned', 'running')
               AND item.status IN ('pending', 'running')
               AND ($1 = item.source_database_alias OR $1 = item.target_database_alias)) AS rebalance_count
        "#,
    )
    .bind(alias)
    .fetch_one(&mut **tx)
    .await
    .map_err(Into::into)
}

pub(in crate::db::workflows::shard_admin) async fn select_shard_move_inspection(
    db: &PgPool,
    run_id: Uuid,
    run_shard: i16,
) -> anyhow::Result<Option<ShardMoveInspection>> {
    sqlx::query_as(
        r#"
        SELECT
            operation.id,
            operation.phase,
            COALESCE(SUM(page.completed_page_count), 0)::bigint AS completed_page_count,
            operation.copied_row_count,
            operation.copied_byte_count
        FROM shard_move_operations operation
        LEFT JOIN shard_move_table_progress page ON page.move_id = operation.id
        WHERE operation.run_id = $1::uuid
          AND operation.run_shard = $2
          AND operation.status = 'active'
        GROUP BY operation.id
        "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .fetch_optional(db)
    .await
    .map_err(Into::into)
}

pub(in crate::db::workflows::shard_admin) async fn count_shard_owned_rows_with<'e, E>(
    executor: E,
    run_id: Uuid,
    run_shard: i16,
) -> anyhow::Result<i64>
where
    E: Executor<'e, Database = Postgres>,
{
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COALESCE(SUM(row_count), 0)::bigint
        FROM (
            SELECT COUNT(*)::bigint AS row_count FROM run_chunks WHERE run_id = $1::uuid AND run_shard = $2
            UNION ALL
            SELECT COUNT(*)::bigint FROM run_snapshots WHERE run_id = $1::uuid AND run_shard = $2
            UNION ALL
            SELECT COUNT(*)::bigint FROM run_shard_summaries WHERE run_id = $1::uuid AND run_shard = $2
            UNION ALL
            SELECT COUNT(*)::bigint FROM run_chunk_settlements WHERE run_id = $1::uuid AND run_shard = $2
            UNION ALL
            SELECT COUNT(*)::bigint FROM run_chunk_settlement_consumptions WHERE run_id = $1::uuid AND run_shard = $2
            UNION ALL
            SELECT COUNT(*)::bigint FROM executions WHERE run_id = $1::uuid AND run_shard = $2
            UNION ALL
            SELECT COUNT(*)::bigint FROM execution_attempts WHERE run_id = $1::uuid AND run_shard = $2
            UNION ALL
            SELECT COUNT(*)::bigint FROM execution_aggregates WHERE run_id = $1::uuid AND run_shard = $2
            UNION ALL
            SELECT COUNT(*)::bigint FROM evaluator_results WHERE run_id = $1::uuid AND run_shard = $2
            UNION ALL
            SELECT COUNT(*)::bigint FROM evaluator_diagnostics WHERE run_id = $1::uuid AND run_shard = $2
        ) counts
        "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .fetch_one(executor)
    .await?;

    Ok(count)
}

pub(in crate::db::workflows::shard_admin) async fn count_active_shard_work_with<'e, E>(
    executor: E,
    run_id: Uuid,
    run_shard: i16,
) -> anyhow::Result<i64>
where
    E: Executor<'e, Database = Postgres>,
{
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT (
            SELECT COUNT(*)::bigint
            FROM run_chunks
            WHERE run_id = $1::uuid
              AND run_shard = $2
              AND status = 'leased'
        ) + (
            SELECT COUNT(*)::bigint
            FROM execution_attempts
            WHERE run_id = $1::uuid
              AND run_shard = $2
              AND status = 'running'::attempt_status
        )
        "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .fetch_one(executor)
    .await?;

    Ok(count)
}

pub(in crate::db::workflows::shard_admin) async fn databases_share_identity(
    source: &mut Transaction<'_, Postgres>,
    target: &PgPool,
) -> anyhow::Result<bool> {
    let source_identity = sqlx::query_as::<_, DatabaseIdentity>(DATABASE_IDENTITY_SQL)
        .fetch_one(&mut **source)
        .await?;
    let target_identity = select_database_identity(target).await?;

    Ok(source_identity == target_identity)
}

pub(in crate::db::workflows::shard_admin) async fn ensure_run_creation_is_inactive(
    db: &PgPool,
    run_id: Uuid,
) -> anyhow::Result<()> {
    let status =
        sqlx::query_scalar::<_, String>("SELECT status::text FROM runs WHERE id = $1::uuid")
            .bind(run_id)
            .fetch_optional(db)
            .await?;
    if status.as_deref() == Some("creating") {
        anyhow::bail!(
            "run {} is still creating; shard routes cannot change until creation finishes",
            run_id
        );
    }
    Ok(())
}
