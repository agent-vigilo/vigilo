//! rebalance queries for shard administration.

use super::super::*;

pub(in crate::db::workflows::shard_admin) async fn select_rebalance_operation_for_update(
    tx: &mut Transaction<'_, Postgres>,
    operation_id: Uuid,
) -> anyhow::Result<Option<ShardRebalanceOperation>> {
    sqlx::query_as(
        r#"
        SELECT *
        FROM shard_rebalance_operations
        WHERE id = $1::uuid
        FOR UPDATE
        "#,
    )
    .bind(operation_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(Into::into)
}

pub(in crate::db::workflows::shard_admin) async fn cancel_rebalance_operation(
    tx: &mut Transaction<'_, Postgres>,
    operation_id: Uuid,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE shard_rebalance_operations
        SET status = 'cancelled',
            planning_state = NULL,
            cancelled_at = COALESCE(cancelled_at, now()),
            updated_at = now()
        WHERE id = $1::uuid
        "#,
    )
    .bind(operation_id)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        r#"
        WITH cancellable AS (
            SELECT operation_id, run_id, run_shard, status, claim_token
            FROM shard_rebalance_items
            WHERE operation_id = $1::uuid
              AND (status = 'pending' OR (status = 'running' AND claimed_until <= now()))
            FOR UPDATE
        )
        UPDATE shard_rebalance_items item
        SET status = 'cancelled',
            error_message = NULL,
            claim_token = NULL,
            claimed_by = NULL,
            claimed_until = NULL,
            completed_at = now(),
            updated_at = now()
        FROM cancellable
        WHERE item.operation_id = cancellable.operation_id
          AND item.run_id = cancellable.run_id
          AND item.run_shard = cancellable.run_shard
          AND item.status = cancellable.status
          AND item.claim_token IS NOT DISTINCT FROM cancellable.claim_token
        "#,
    )
    .bind(operation_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(in crate::db::workflows::shard_admin) async fn list_active_rebalance_candidates(
    db: &PgPool,
) -> anyhow::Result<Vec<ShardRebalanceCandidate>> {
    let candidates = sqlx::query_as::<_, (Uuid, i16, String, i64)>(
        r#"
        SELECT sp.run_id, sp.run_shard, sp.database_alias, sp.route_version
        FROM shard_placements sp
        JOIN database_placements dp
          ON dp.alias = sp.database_alias
        LEFT JOIN runs run ON run.id = sp.run_id
        WHERE sp.status = 'active'
          AND dp.status IN ('active', 'draining')
          AND dp.role IN ('shard', 'control_and_shard')
          AND (run.id IS NULL OR run.status <> 'creating'::run_status)
        ORDER BY sp.database_alias, sp.run_id, sp.run_shard
        "#,
    )
    .fetch_all(db)
    .await?
    .into_iter()
    .map(
        |(run_id, run_shard, source_database_alias, route_version)| ShardRebalanceCandidate {
            run_id,
            run_shard,
            source_database_alias,
            route_version,
        },
    )
    .collect();

    Ok(candidates)
}

pub(in crate::db::workflows::shard_admin) async fn insert_rebalance_operation(
    db: &PgPool,
    strategy: &str,
    source_database_alias: Option<&str>,
    target_database_alias: &str,
    items: &[PlannedShardRebalanceItem],
) -> anyhow::Result<ShardRebalanceOperation> {
    let mut tx = db.begin().await?;
    let operation = sqlx::query_as::<_, ShardRebalanceOperation>(
        r#"
        INSERT INTO shard_rebalance_operations (
            strategy,
            source_database_alias,
            target_database_alias,
            planned_item_count
        )
        VALUES ($1, $2, $3, $4)
        RETURNING *
        "#,
    )
    .bind(strategy)
    .bind(source_database_alias)
    .bind(target_database_alias)
    .bind(items.len() as i32)
    .fetch_one(tx.as_mut())
    .await?;

    let mut sequence = 0;
    for page in items.chunks(256) {
        append_rebalance_items(&mut tx, &operation, sequence, page).await?;
        sequence += page.len() as i32;
    }

    tx.commit().await?;
    Ok(operation)
}

/// Stores a scored route page atomically with its planning cursor. Resolve the
/// owning tenant from the run rather than a connection's default tenant.
pub(in crate::db::workflows::shard_admin) async fn append_rebalance_items(
    tx: &mut Transaction<'_, Postgres>,
    operation: &ShardRebalanceOperation,
    sequence: i32,
    items: &[PlannedShardRebalanceItem],
) -> anyhow::Result<()> {
    if items.is_empty() {
        return Ok(());
    }
    let runs: Vec<_> = items.iter().map(|item| item.run_id).collect();
    let shards: Vec<_> = items.iter().map(|item| item.run_shard).collect();
    let sources: Vec<_> = items
        .iter()
        .map(|item| item.source_database_alias.as_str())
        .collect();
    let versions: Vec<_> = items
        .iter()
        .map(|item| item.planned_route_version)
        .collect();
    let inserted = sqlx::query(
        r#"
        INSERT INTO shard_rebalance_items (tenant_id, operation_id, sequence_no, run_id, run_shard,
            source_database_alias, target_database_alias, planned_route_version, move_operation_id)
        SELECT run.tenant_id, $1, ($2::int + item.position - 1)::int, item.run_id, item.run_shard,
               item.source_alias, $7, item.route_version, CASE WHEN $8 THEN gen_random_uuid() END
        FROM unnest($3::uuid[], $4::smallint[], $5::text[], $6::bigint[]) WITH ORDINALITY
            AS item(run_id, run_shard, source_alias, route_version, position)
        JOIN runs run ON run.id = item.run_id
        "#,
    )
    .bind(operation.id)
    .bind(sequence)
    .bind(runs)
    .bind(shards)
    .bind(sources)
    .bind(versions)
    .bind(&operation.target_database_alias)
    .bind(operation.library_max_items.is_some())
    .execute(&mut **tx)
    .await?
    .rows_affected();
    anyhow::ensure!(
        inserted == items.len() as u64,
        "a planned shard lost its run before persistence"
    );
    Ok(())
}

pub(in crate::db::workflows::shard_admin) async fn select_rebalance_operation(
    db: &PgPool,
    operation_id: Uuid,
) -> anyhow::Result<Option<ShardRebalanceOperation>> {
    let operation = sqlx::query_as::<_, ShardRebalanceOperation>(
        r#"
        SELECT *
        FROM shard_rebalance_operations
        WHERE id = $1::uuid
        "#,
    )
    .bind(operation_id)
    .fetch_optional(db)
    .await?;

    Ok(operation)
}

pub(in crate::db::workflows::shard_admin) async fn list_rebalance_items_by_status(
    db: &PgPool,
    operation_id: Uuid,
    status: &str,
    limit: usize,
) -> anyhow::Result<Vec<ShardRebalanceItem>> {
    let items = sqlx::query_as::<_, ShardRebalanceItem>(
        r#"
        SELECT *
        FROM shard_rebalance_items
        WHERE operation_id = $1::uuid
          AND status = $2
        ORDER BY sequence_no
        LIMIT $3
        "#,
    )
    .bind(operation_id)
    .bind(status)
    .bind(limit as i64)
    .fetch_all(db)
    .await?;

    Ok(items)
}

pub(in crate::db::workflows::shard_admin) async fn claim_next_rebalance_apply_item(
    db: &PgPool,
    operation_id: Uuid,
    claimed_by: Uuid,
    lease_seconds: i32,
) -> anyhow::Result<Option<ClaimedShardRebalanceItem>> {
    claim_next_rebalance_item_after(db, operation_id, claimed_by, lease_seconds, -1).await
}

pub(in crate::db::workflows::shard_admin) async fn claim_next_rebalance_item_after(
    db: &PgPool,
    operation_id: Uuid,
    claimed_by: Uuid,
    lease_seconds: i32,
    after_sequence: i32,
) -> anyhow::Result<Option<ClaimedShardRebalanceItem>> {
    let item = sqlx::query_as::<_, ClaimedShardRebalanceItem>(
        r#"
        WITH active_operation AS (
            UPDATE shard_rebalance_operations
            SET status = 'running',
                started_at = COALESCE(started_at, now()),
                updated_at = now()
            WHERE id = $1::uuid
              AND status IN ('planned', 'running')
            RETURNING id
        ),
        candidate AS (
            SELECT
                i.operation_id,
                i.run_id,
                i.run_shard,
                i.status = 'running' AS reclaimed
            FROM shard_rebalance_items i
            JOIN active_operation operation
              ON operation.id = i.operation_id
            WHERE i.sequence_no > $4 AND (i.status = 'pending'
               OR (
                    i.status = 'running'
                    AND i.claimed_until <= now()
               ))
            ORDER BY i.sequence_no
            FOR UPDATE OF i SKIP LOCKED
            LIMIT 1
        ),
        claimed AS (
            UPDATE shard_rebalance_items i
            SET status = 'running',
                error_message = NULL,
                claim_token = gen_random_uuid(),
                claimed_by = $2::uuid,
                claimed_until = now() + ($3::int * interval '1 second'),
                started_at = COALESCE(i.started_at, now()),
                completed_at = NULL,
                updated_at = now()
            FROM candidate
            WHERE i.operation_id = candidate.operation_id
              AND i.run_id = candidate.run_id
              AND i.run_shard = candidate.run_shard
            RETURNING i.*, candidate.reclaimed
        )
        SELECT *
        FROM claimed
        "#,
    )
    .bind(operation_id)
    .bind(claimed_by)
    .bind(lease_seconds)
    .bind(after_sequence)
    .fetch_optional(db)
    .await?;

    Ok(item)
}

pub(in crate::db::workflows::shard_admin) async fn defer_rebalance_item(
    db: &PgPool,
    item: &ShardRebalanceItem,
    claim_token: Uuid,
    reason: &str,
) -> anyhow::Result<Option<ShardRebalanceItem>> {
    let item = sqlx::query_as::<_, ShardRebalanceItem>(
        r#"
        WITH operation AS MATERIALIZED (
            SELECT status FROM shard_rebalance_operations WHERE id = $1::uuid FOR UPDATE
        )
        UPDATE shard_rebalance_items item
        SET status = CASE WHEN operation.status = 'cancelled' THEN 'cancelled' ELSE 'pending' END,
            error_message = CASE WHEN operation.status = 'cancelled' THEN NULL ELSE $5 END,
            claim_token = NULL,
            claimed_by = NULL,
            claimed_until = NULL,
            completed_at = CASE WHEN operation.status = 'cancelled' THEN now() ELSE NULL END,
            updated_at = now()
        FROM operation
        WHERE item.operation_id = $1::uuid
          AND item.run_id = $2::uuid
          AND item.run_shard = $3
          AND item.status = 'running'
          AND item.claim_token = $4::uuid
        RETURNING item.*
        "#,
    )
    .bind(item.operation_id)
    .bind(item.run_id)
    .bind(item.run_shard)
    .bind(claim_token)
    .bind(reason)
    .fetch_optional(db)
    .await?;

    Ok(item)
}

pub(in crate::db::workflows::shard_admin) async fn mark_rebalance_item_status(
    db: &PgPool,
    item: &ShardRebalanceItem,
    claim_token: Uuid,
    status: &str,
    error_message: Option<&str>,
) -> anyhow::Result<Option<ShardRebalanceItem>> {
    let item = sqlx::query_as::<_, ShardRebalanceItem>(
        r#"
        UPDATE shard_rebalance_items
        SET status = $5,
            error_message = $6,
            claim_token = NULL,
            claimed_by = NULL,
            claimed_until = NULL,
            completed_at = now(),
            updated_at = now()
        WHERE operation_id = $1::uuid
          AND run_id = $2::uuid
          AND run_shard = $3
          AND status = 'running'
          AND claim_token = $4::uuid
        RETURNING *
        "#,
    )
    .bind(item.operation_id)
    .bind(item.run_id)
    .bind(item.run_shard)
    .bind(claim_token)
    .bind(status)
    .bind(error_message)
    .fetch_optional(db)
    .await?;

    Ok(item)
}

pub(in crate::db::workflows::shard_admin) async fn rebalance_operation_is_cancelled(
    db: &PgPool,
    operation_id: Uuid,
) -> anyhow::Result<bool> {
    let cancelled = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT status = 'cancelled'
        FROM shard_rebalance_operations
        WHERE id = $1::uuid
        "#,
    )
    .bind(operation_id)
    .fetch_one(db)
    .await?;

    Ok(cancelled)
}

pub(in crate::db::workflows::shard_admin) async fn refresh_rebalance_operation_status(
    db: &PgPool,
    operation_id: Uuid,
) -> anyhow::Result<ShardRebalanceOperation> {
    let mut tx = db.begin().await?;
    let current_status = sqlx::query_scalar::<_, String>(
        r#"
        SELECT status
        FROM shard_rebalance_operations
        WHERE id = $1::uuid
        FOR UPDATE
        "#,
    )
    .bind(operation_id)
    .fetch_one(tx.as_mut())
    .await?;
    let (pending, running, completed, failed, cancelled) =
        sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE status = 'pending')::bigint,
                COUNT(*) FILTER (WHERE status = 'running')::bigint,
                COUNT(*) FILTER (WHERE status = 'completed')::bigint,
                COUNT(*) FILTER (WHERE status = 'failed')::bigint,
                COUNT(*) FILTER (WHERE status = 'cancelled')::bigint
            FROM shard_rebalance_items
            WHERE operation_id = $1::uuid
            "#,
        )
        .bind(operation_id)
        .fetch_one(tx.as_mut())
        .await?;
    let status = match current_status.as_str() {
        REBALANCE_OPERATION_STATUS_CANCELLED => REBALANCE_OPERATION_STATUS_CANCELLED,
        REBALANCE_OPERATION_STATUS_COMPLETED => REBALANCE_OPERATION_STATUS_COMPLETED,
        REBALANCE_OPERATION_STATUS_FAILED => REBALANCE_OPERATION_STATUS_FAILED,
        _ if pending == 0 && running == 0 && failed == 0 => REBALANCE_OPERATION_STATUS_COMPLETED,
        _ if pending == 0 && running == 0 && failed > 0 => REBALANCE_OPERATION_STATUS_FAILED,
        _ => REBALANCE_OPERATION_STATUS_RUNNING,
    };
    let error_message = if status == REBALANCE_OPERATION_STATUS_FAILED {
        Some("one or more rebalance items failed")
    } else {
        None
    };

    let operation = sqlx::query_as::<_, ShardRebalanceOperation>(
        r#"
        UPDATE shard_rebalance_operations
        SET status = $2,
            completed_item_count = $3,
            failed_item_count = $4,
            cancelled_item_count = $5,
            error_message = $6,
            completed_at = CASE
                WHEN $2 IN ('completed', 'failed') THEN COALESCE(completed_at, now())
                ELSE completed_at
            END,
            updated_at = now()
        WHERE id = $1::uuid
        RETURNING *
        "#,
    )
    .bind(operation_id)
    .bind(status)
    .bind(completed as i32)
    .bind(failed as i32)
    .bind(cancelled as i32)
    .bind(error_message)
    .fetch_one(tx.as_mut())
    .await?;
    tx.commit().await?;

    Ok(operation)
}
