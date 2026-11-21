//! Shard placement table access.
//!
//! Shard placements are the durable routing decisions from a logical
//! `run_id + run_shard` pair to a database placement alias.

use sqlx::{
    Executor,
    PgPool,
    Postgres,
};
use uuid::Uuid;

use crate::models::shard_placement::ShardPlacement;

pub(crate) async fn select_shard_placement(
    db: &PgPool,
    run_id: Uuid,
    run_shard: i16,
) -> anyhow::Result<Option<ShardPlacement>> {
    select_shard_placement_with(db, run_id, run_shard).await
}

pub(crate) async fn select_shard_placement_with<'e, E>(
    executor: E,
    run_id: Uuid,
    run_shard: i16,
) -> anyhow::Result<Option<ShardPlacement>>
where
    E: Executor<'e, Database = Postgres>,
{
    let placement = sqlx::query_as::<_, ShardPlacement>(
        r#"
        SELECT run_id, run_shard, database_alias, status, move_target_database_alias,
               route_version, write_epoch, created_at, updated_at
        FROM shard_placements
        WHERE run_id = $1
          AND run_shard = $2
        "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .fetch_optional(executor)
    .await?;

    Ok(placement)
}

pub(crate) async fn mark_shard_placement_copying<'e, E>(
    executor: E,
    run_id: Uuid,
    run_shard: i16,
    expected_database_alias: &str,
    expected_route_version: i64,
    target_database_alias: &str,
) -> anyhow::Result<Option<ShardPlacement>>
where
    E: Executor<'e, Database = Postgres>,
{
    let placement = sqlx::query_as::<_, ShardPlacement>(
        r#"
        UPDATE shard_placements
        SET status = 'copying',
            move_target_database_alias = $5,
            route_version = route_version + 1,
            updated_at = now()
        WHERE run_id = $1::uuid
          AND run_shard = $2
          AND database_alias = $3
          AND status = 'active'
          AND route_version = $4
        RETURNING run_id, run_shard, database_alias, status, move_target_database_alias,
                  route_version, write_epoch, created_at, updated_at
        "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .bind(expected_database_alias)
    .bind(expected_route_version)
    .bind(target_database_alias)
    .fetch_optional(executor)
    .await?;

    Ok(placement)
}

pub(crate) async fn mark_shard_placement_draining<'e, E>(
    executor: E,
    run_id: Uuid,
    run_shard: i16,
    expected_database_alias: &str,
    expected_route_version: i64,
    target_database_alias: &str,
) -> anyhow::Result<Option<ShardPlacement>>
where
    E: Executor<'e, Database = Postgres>,
{
    let placement = sqlx::query_as::<_, ShardPlacement>(
        r#"
        UPDATE shard_placements
        SET status = 'draining',
            route_version = route_version + 1,
            updated_at = now()
        WHERE run_id = $1::uuid
          AND run_shard = $2
          AND database_alias = $3
          AND status = 'copying'
          AND route_version = $4
          AND move_target_database_alias = $5
        RETURNING run_id, run_shard, database_alias, status, move_target_database_alias,
                  route_version, write_epoch, created_at, updated_at
        "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .bind(expected_database_alias)
    .bind(expected_route_version)
    .bind(target_database_alias)
    .fetch_optional(executor)
    .await?;

    Ok(placement)
}

pub(crate) async fn mark_shard_placement_moving<'e, E>(
    executor: E,
    run_id: Uuid,
    run_shard: i16,
    expected_database_alias: &str,
    expected_route_version: i64,
    target_database_alias: &str,
) -> anyhow::Result<Option<ShardPlacement>>
where
    E: Executor<'e, Database = Postgres>,
{
    let placement = sqlx::query_as::<_, ShardPlacement>(
        r#"
        UPDATE shard_placements
        SET status = 'moving',
            route_version = route_version + 1,
            updated_at = now()
        WHERE run_id = $1::uuid
          AND run_shard = $2
          AND database_alias = $3
          AND status = 'draining'
          AND route_version = $4
          AND move_target_database_alias = $5
        RETURNING run_id, run_shard, database_alias, status, move_target_database_alias,
                  route_version, write_epoch, created_at, updated_at
        "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .bind(expected_database_alias)
    .bind(expected_route_version)
    .bind(target_database_alias)
    .fetch_optional(executor)
    .await?;

    Ok(placement)
}

pub(crate) async fn abort_shard_placement_move<'e, E>(
    executor: E,
    run_id: Uuid,
    run_shard: i16,
    expected_database_alias: &str,
    expected_route_version: i64,
    target_database_alias: &str,
) -> anyhow::Result<Option<ShardPlacement>>
where
    E: Executor<'e, Database = Postgres>,
{
    let placement = sqlx::query_as::<_, ShardPlacement>(
        r#"
        UPDATE shard_placements
        SET status = 'active',
            move_target_database_alias = NULL,
            route_version = route_version + 1,
            write_epoch = write_epoch + CASE
                WHEN status IN ('draining', 'moving') THEN 1
                ELSE 0
            END,
            updated_at = now()
        WHERE run_id = $1::uuid
          AND run_shard = $2
          AND database_alias = $3
          AND status IN ('copying', 'draining', 'moving')
          AND route_version = $4
          AND move_target_database_alias = $5
        RETURNING run_id, run_shard, database_alias, status, move_target_database_alias,
                  route_version, write_epoch, created_at, updated_at
        "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .bind(expected_database_alias)
    .bind(expected_route_version)
    .bind(target_database_alias)
    .fetch_optional(executor)
    .await?;

    Ok(placement)
}

/// Advances an unchanged active route to fence a prepared move that never
/// reached the `copying` lifecycle state.
pub(crate) async fn fence_active_shard_placement<'e, E>(
    executor: E,
    run_id: Uuid,
    run_shard: i16,
    expected_database_alias: &str,
    expected_route_version: i64,
) -> anyhow::Result<Option<ShardPlacement>>
where
    E: Executor<'e, Database = Postgres>,
{
    let placement = sqlx::query_as::<_, ShardPlacement>(
        r#"
        UPDATE shard_placements
        SET route_version = route_version + 1,
            updated_at = now()
        WHERE run_id = $1::uuid
          AND run_shard = $2
          AND database_alias = $3
          AND status = 'active'
          AND move_target_database_alias IS NULL
          AND route_version = $4
        RETURNING run_id, run_shard, database_alias, status, move_target_database_alias,
                  route_version, write_epoch, created_at, updated_at
        "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .bind(expected_database_alias)
    .bind(expected_route_version)
    .fetch_optional(executor)
    .await?;
    Ok(placement)
}

pub(crate) async fn activate_moved_shard_placement<'e, E>(
    executor: E,
    run_id: Uuid,
    run_shard: i16,
    expected_database_alias: &str,
    expected_route_version: i64,
    target_database_alias: &str,
) -> anyhow::Result<Option<ShardPlacement>>
where
    E: Executor<'e, Database = Postgres>,
{
    let placement = sqlx::query_as::<_, ShardPlacement>(
        r#"
        UPDATE shard_placements
        SET database_alias = $5,
            status = 'active',
            move_target_database_alias = NULL,
            route_version = route_version + 1,
            write_epoch = write_epoch + 1,
            updated_at = now()
        WHERE run_id = $1::uuid
          AND run_shard = $2
          AND database_alias = $3
          AND status = 'moving'
          AND route_version = $4
          AND move_target_database_alias = $5
        RETURNING run_id, run_shard, database_alias, status, move_target_database_alias,
                  route_version, write_epoch, created_at, updated_at
        "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .bind(expected_database_alias)
    .bind(expected_route_version)
    .bind(target_database_alias)
    .fetch_optional(executor)
    .await?;

    Ok(placement)
}

pub(crate) async fn count_inflight_moves_to_database<'e, E>(
    executor: E,
    database_alias: &str,
) -> anyhow::Result<i64>
where
    E: Executor<'e, Database = Postgres>,
{
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM shard_placements
        WHERE move_target_database_alias = $1
          AND status IN ('copying', 'draining', 'moving')
        "#,
    )
    .bind(database_alias)
    .fetch_one(executor)
    .await?;

    Ok(count)
}

pub(crate) async fn change_empty_active_shard_placement<'e, E>(
    executor: E,
    run_id: Uuid,
    run_shard: i16,
    expected_database_alias: &str,
    expected_route_version: i64,
    target_database_alias: &str,
) -> anyhow::Result<Option<ShardPlacement>>
where
    E: Executor<'e, Database = Postgres>,
{
    let placement = sqlx::query_as::<_, ShardPlacement>(
        r#"
        UPDATE shard_placements
        SET database_alias = $5,
            move_target_database_alias = NULL,
            route_version = route_version + 1,
            write_epoch = write_epoch + 1,
            updated_at = now()
        WHERE run_id = $1::uuid
          AND run_shard = $2
          AND database_alias = $3
          AND status = 'active'
          AND route_version = $4
        RETURNING run_id, run_shard, database_alias, status, move_target_database_alias,
                  route_version, write_epoch, created_at, updated_at
        "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .bind(expected_database_alias)
    .bind(expected_route_version)
    .bind(target_database_alias)
    .fetch_optional(executor)
    .await?;

    Ok(placement)
}

pub(crate) async fn list_shard_placements_for_run(
    db: impl Executor<'_, Database = Postgres>,
    run_id: Uuid,
) -> anyhow::Result<Vec<ShardPlacement>> {
    let placements = sqlx::query_as::<_, ShardPlacement>(
        r#"
        SELECT run_id, run_shard, database_alias, status, move_target_database_alias,
               route_version, write_epoch, created_at, updated_at
        FROM shard_placements
        WHERE run_id = $1
        ORDER BY run_shard
        "#,
    )
    .bind(run_id)
    .fetch_all(db)
    .await?;

    Ok(placements)
}

pub(crate) async fn insert_active_shard_placement<'e, E>(
    executor: E,
    run_id: Uuid,
    run_shard: i16,
    database_alias: &str,
) -> anyhow::Result<ShardPlacement>
where
    E: Executor<'e, Database = Postgres>,
{
    let placement = sqlx::query_as::<_, ShardPlacement>(
        r#"
        INSERT INTO shard_placements (run_id, run_shard, database_alias, status)
        VALUES ($1::uuid, $2, $3, 'active')
        RETURNING run_id, run_shard, database_alias, status, move_target_database_alias,
                  route_version, write_epoch, created_at, updated_at
        "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .bind(database_alias)
    .fetch_one(executor)
    .await?;

    Ok(placement)
}
