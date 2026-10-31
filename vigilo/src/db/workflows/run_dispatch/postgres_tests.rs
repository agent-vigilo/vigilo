// PostgreSQL-backed workflow scenarios and fixtures.

use std::{
    collections::BTreeSet,
    time::Duration,
};

use sqlx::{
    PgPool,
    postgres::PgPoolOptions,
};

use super::*;
use crate::db::workflows::{
    run_cancel,
    run_finalize,
};

fn dispatched_shards(windows: &[DispatchedRun]) -> BTreeSet<i16> {
    windows.iter().map(|window| window.run_shard).collect()
}

async fn seed_pending_run(pool: &PgPool, shard_chunk_counts: &[(i16, i32)]) -> Uuid {
    let dataset_id = Uuid::now_v7();
    let dataset_version_id = Uuid::now_v7();
    let run_id = Uuid::now_v7();
    let expected_execution_count = shard_chunk_counts
        .iter()
        .map(|(_, count)| *count)
        .sum::<i32>();

    sqlx::query(
        r#"
            INSERT INTO dataset_versions (dataset_version_id, dataset_id, dataset_version)
            VALUES ($1::uuid, $2::uuid, 'test')
            "#,
    )
    .bind(dataset_version_id)
    .bind(dataset_id)
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        r#"
            INSERT INTO runs (
                id,
                run_key,
                dataset_id,
                dataset_version_id,
                dataset_version,
                evaluation_profile_id,
                evaluation_profile_version,
                profile_version_id,
                profile_hash,
                aggregation_policy_id,
                aggregation_policy_version,
                aggregation_policy_hash,
                agent_provider,
                agent_name,
                prompt_config_id,
                prompt_config_version,
                expected_execution_count
            )
            VALUES (
                $1::uuid,
                $2,
                $3::uuid,
                $4::uuid,
                'test',
                'profile',
                '1.0.0',
                'profile-version',
                'profile-hash',
                'aggregation',
                '1.0.0',
                'aggregation-hash',
                'example',
                'agent',
                'prompt',
                '1.0.0',
                $5
            )
            "#,
    )
    .bind(run_id)
    .bind(format!("run-{run_id}"))
    .bind(dataset_id)
    .bind(dataset_version_id)
    .bind(expected_execution_count)
    .execute(pool)
    .await
    .unwrap();

    let mut ordinal = 0;
    for (run_shard, chunk_count) in shard_chunk_counts {
        sqlx::query(
            r#"
                INSERT INTO shard_placements (run_id, run_shard, database_alias, status)
                VALUES ($1::uuid, $2, 'primary', 'active')
                ON CONFLICT (run_id, run_shard) DO UPDATE
                SET database_alias = EXCLUDED.database_alias,
                    status = EXCLUDED.status,
                    updated_at = now()
                "#,
        )
        .bind(run_id)
        .bind(run_shard)
        .execute(pool)
        .await
        .unwrap();

        sqlx::query(
            r#"
                INSERT INTO run_shard_dispatch_cursors (run_id, run_shard, status)
                VALUES ($1::uuid, $2, 'open')
                "#,
        )
        .bind(run_id)
        .bind(run_shard)
        .execute(pool)
        .await
        .unwrap();

        for _ in 0..*chunk_count {
            sqlx::query(
                r#"
                    INSERT INTO run_chunks (
                        id,
                        run_id,
                        run_shard,
                        dataset_version_id,
                        profile_group_id,
                        ordinal_start,
                        ordinal_end,
                        status
                    )
                    VALUES (
                        $1::uuid,
                        $2::uuid,
                        $3,
                        $4::uuid,
                        'default',
                        $5,
                        $6,
                        'pending'
                    )
                    "#,
            )
            .bind(Uuid::now_v7())
            .bind(run_id)
            .bind(run_shard)
            .bind(dataset_version_id)
            .bind(ordinal)
            .bind(ordinal + 1)
            .execute(pool)
            .await
            .unwrap();

            ordinal += 1;
        }
    }

    run_id
}

async fn mark_run_running(pool: &PgPool, run_id: Uuid) {
    sqlx::query(
        r#"
            UPDATE runs
            SET status = 'running'::run_status,
                started_at = now(),
                dispatched_at = now(),
                updated_at = now()
            WHERE id = $1::uuid
            "#,
    )
    .bind(run_id)
    .execute(pool)
    .await
    .unwrap();
}

async fn mark_all_chunks_completed(pool: &PgPool, run_id: Uuid) {
    sqlx::query(
        r#"
            UPDATE run_chunks
            SET status = 'completed',
                dispatched_at = COALESCE(dispatched_at, now()),
                updated_at = now()
            WHERE run_id = $1::uuid
            "#,
    )
    .bind(run_id)
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        r#"
            UPDATE run_shard_dispatch_cursors
            SET status = 'drained',
                updated_at = now()
            WHERE run_id = $1::uuid
            "#,
    )
    .bind(run_id)
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_shard_placements(pool: &PgPool, run_id: Uuid, placements: &[(i16, &str)]) {
    if placements.iter().any(|(_, status)| *status != "active") {
        sqlx::query(
            r#"
                INSERT INTO database_placements (alias, database_url_env, role, status)
                VALUES ('test_move_target', 'DATABASE_URL', 'shard', 'active')
                ON CONFLICT (alias) DO NOTHING
                "#,
        )
        .execute(pool)
        .await
        .unwrap();
    }

    for (run_shard, status) in placements {
        sqlx::query(
            r#"
                INSERT INTO shard_placements (
                    run_id,
                    run_shard,
                    database_alias,
                    status,
                    move_target_database_alias
                )
                VALUES (
                    $1::uuid,
                    $2,
                    'primary',
                    $3,
                    CASE WHEN $3 = 'active' THEN NULL ELSE 'test_move_target' END
                )
                ON CONFLICT (run_id, run_shard) DO UPDATE
                SET database_alias = EXCLUDED.database_alias,
                    status = EXCLUDED.status,
                    move_target_database_alias = EXCLUDED.move_target_database_alias,
                    updated_at = now()
                "#,
        )
        .bind(run_id)
        .bind(run_shard)
        .bind(status)
        .execute(pool)
        .await
        .unwrap();
    }
}

async fn lock_run_for_share(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, run_id: Uuid) {
    sqlx::query(
        r#"
            SELECT id
            FROM runs
            WHERE id = $1::uuid
            FOR SHARE
            "#,
    )
    .bind(run_id)
    .execute(&mut **tx)
    .await
    .unwrap();
}

async fn lock_run_for_update(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, run_id: Uuid) {
    sqlx::query(
        r#"
            SELECT id
            FROM runs
            WHERE id = $1::uuid
            FOR UPDATE
            "#,
    )
    .bind(run_id)
    .execute(&mut **tx)
    .await
    .unwrap();
}

async fn dispatch_window(pool: &PgPool) -> Option<DispatchedRun> {
    dispatch_next_run_window(pool, Uuid::now_v7(), 60, 10)
        .await
        .unwrap()
}

async fn dispatched_chunk_count(pool: &PgPool, run_id: Uuid, run_shard: i16) -> i64 {
    sqlx::query_scalar::<_, i64>(
        r#"
            SELECT COUNT(*)::bigint
            FROM run_chunks
            WHERE run_id = $1::uuid
              AND run_shard = $2
              AND dispatched_at IS NOT NULL
            "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn cursor_status(pool: &PgPool, run_id: Uuid, run_shard: i16) -> String {
    sqlx::query_scalar::<_, String>(
        r#"
            SELECT status
            FROM run_shard_dispatch_cursors
            WHERE run_id = $1::uuid
              AND run_shard = $2
            "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn cursor_status_count(pool: &PgPool, run_id: Uuid, status: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(
        r#"
            SELECT COUNT(*)::bigint
            FROM run_shard_dispatch_cursors
            WHERE run_id = $1::uuid
              AND status = $2
            "#,
    )
    .bind(run_id)
    .bind(status)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn run_status(pool: &PgPool, run_id: Uuid) -> String {
    sqlx::query_scalar::<_, String>(
        r#"
            SELECT status::text
            FROM runs
            WHERE id = $1::uuid
            "#,
    )
    .bind(run_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn chunk_ready_event_count(pool: &PgPool, run_id: Uuid) -> i64 {
    sqlx::query_scalar::<_, i64>(
        r#"
            SELECT COUNT(*)::bigint
            FROM outbox_events
            WHERE aggregate_id = $1::uuid
              AND event_type = 'run.chunk.ready'
            "#,
    )
    .bind(run_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn run_started_event_count(pool: &PgPool, run_id: Uuid) -> i64 {
    sqlx::query_scalar::<_, i64>(
        r#"
            SELECT COUNT(*)::bigint
            FROM outbox_events
            WHERE aggregate_id = $1::uuid
              AND event_type = 'run.started'
            "#,
    )
    .bind(run_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn run_snapshot_count(pool: &PgPool, run_id: Uuid, run_shard: i16) -> i64 {
    sqlx::query_scalar::<_, i64>(
        r#"
            SELECT COUNT(*)::bigint
            FROM run_snapshots
            WHERE run_id = $1::uuid
              AND run_shard = $2
            "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn dispatch_scans_one_run_shard_at_a_time(pool: PgPool) {
    let run_id = seed_pending_run(&pool, &[(0, 2), (1, 2)]).await;

    let first = dispatch_window(&pool).await.unwrap();
    assert_eq!(first.id, run_id);
    assert_eq!(first.run_shard, 0);
    assert_eq!(first.chunks_marked_dispatched, 2);

    assert_eq!(dispatched_chunk_count(&pool, run_id, 1).await, 0);
    assert_eq!(cursor_status(&pool, run_id, 0).await, "drained");
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn select_next_dispatch_route_skips_moving_shard_placement(pool: PgPool) {
    let run_id = seed_pending_run(&pool, &[(0, 1), (1, 1)]).await;
    insert_shard_placements(&pool, run_id, &[(0, "moving"), (1, "active")]).await;

    let route = select_next_dispatch_route(&pool, &[])
        .await
        .unwrap()
        .unwrap();

    assert_eq!(route.run_id, run_id);
    assert_eq!(route.run_shard, 1);
    assert_eq!(route.database_alias, "primary");
    assert_eq!(route.placement_status, "active");
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn select_next_dispatch_route_excludes_failed_aliases(pool: PgPool) {
    seed_pending_run(&pool, &[(0, 1)]).await;

    let route = select_next_dispatch_route(&pool, &["primary".to_string()])
        .await
        .unwrap();

    assert!(route.is_none());
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn select_next_dispatch_route_includes_draining_database_owner(pool: PgPool) {
    let run_id = seed_pending_run(&pool, &[(0, 1)]).await;
    sqlx::query(
        r#"
            INSERT INTO database_placements (alias, database_url_env, role, status)
            VALUES ('shard_001', 'DATABASE_URL', 'shard', 'draining')
            "#,
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
            UPDATE shard_placements
            SET database_alias = 'shard_001',
                updated_at = now()
            WHERE run_id = $1::uuid
              AND run_shard = 0
            "#,
    )
    .bind(run_id)
    .execute(&pool)
    .await
    .unwrap();

    let route = select_next_dispatch_route(&pool, &[])
        .await
        .unwrap()
        .unwrap();

    assert_eq!(route.database_alias, "shard_001");
    assert_eq!(count_dispatch_cursor_backlog(&pool).await.unwrap(), 1);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn count_dispatch_cursor_backlog_matches_dispatchable_routes(pool: PgPool) {
    let run_id = seed_pending_run(&pool, &[(0, 1), (1, 1), (2, 1)]).await;
    insert_shard_placements(
        &pool,
        run_id,
        &[(0, "active"), (1, "moving"), (2, "draining")],
    )
    .await;

    let backlog = count_dispatch_cursor_backlog(&pool).await.unwrap();

    assert_eq!(backlog, 1);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn dispatch_routed_run_window_dispatches_exact_route(pool: PgPool) {
    let run_id = seed_pending_run(&pool, &[(0, 1), (1, 1)]).await;
    let route = DispatchRoute {
        run_id,
        run_shard: 1,
        database_alias: "primary".to_string(),
        placement_status: "active".to_string(),
        route_version: 1,
        write_epoch: 1,
    };
    let snapshot = prepare_dispatch_run_snapshot(&pool, &route, Uuid::now_v7(), 60)
        .await
        .unwrap()
        .unwrap();

    let dispatched = dispatch_routed_run_window(&pool, &pool, 10, &route, &snapshot)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(dispatched.id, run_id);
    assert_eq!(dispatched.run_shard, 1);
    assert_eq!(dispatched_chunk_count(&pool, run_id, 0).await, 0);
    assert_eq!(dispatched_chunk_count(&pool, run_id, 1).await, 1);
    assert_eq!(run_snapshot_count(&pool, run_id, 1).await, 1);
    let payload = sqlx::query_scalar::<_, serde_json::Value>(
            "SELECT payload FROM outbox_events WHERE event_type = 'run.chunk.ready' AND aggregate_id = $1 LIMIT 1",
        )
        .bind(run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(payload["database_alias"], "primary");
    assert_eq!(payload["write_epoch"], 1);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn failed_execution_write_leaves_control_cursor_open(pool: PgPool) {
    let run_id = seed_pending_run(&pool, &[(0, 1)]).await;
    let route = select_next_dispatch_route(&pool, &[])
        .await
        .unwrap()
        .unwrap();
    let snapshot = prepare_dispatch_run_snapshot(&pool, &route, Uuid::now_v7(), 60)
        .await
        .unwrap()
        .unwrap();
    let unavailable = PgPoolOptions::new()
        .connect_lazy("postgres://vigilo@127.0.0.1/vigilo")
        .unwrap();
    unavailable.close().await;

    let error = dispatch_routed_run_window(&pool, &unavailable, 10, &route, &snapshot)
        .await
        .unwrap_err();

    assert!(matches!(error, RoutedDispatchError::ExecutionWrite(_)));
    assert_eq!(cursor_status(&pool, run_id, 0).await, "open");
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn dispatch_releases_open_cursor_when_shard_has_more_chunks(pool: PgPool) {
    let run_id = seed_pending_run(&pool, &[(0, 3)]).await;

    let first = dispatch_next_run_window(&pool, Uuid::now_v7(), 60, 2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.run_shard, 0);
    assert_eq!(first.chunks_marked_dispatched, 2);
    assert_eq!(cursor_status(&pool, run_id, 0).await, "open");

    let second = dispatch_next_run_window(&pool, Uuid::now_v7(), 60, 2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.run_shard, 0);
    assert_eq!(second.chunks_marked_dispatched, 1);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn concurrent_dispatchers_claim_distinct_shards_for_same_running_run(pool: PgPool) {
    let run_id = seed_pending_run(&pool, &[(0, 1), (1, 1)]).await;
    mark_run_running(&pool, run_id).await;

    let (left, right) = tokio::join!(dispatch_window(&pool), dispatch_window(&pool));
    let windows = [left, right]
        .into_iter()
        .flatten()
        .collect::<Vec<DispatchedRun>>();

    assert_eq!(windows.len(), 2);
    assert_eq!(dispatched_shards(&windows), BTreeSet::from([0, 1]));
    assert_eq!(chunk_ready_event_count(&pool, run_id).await, 2);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn concurrent_dispatchers_do_not_duplicate_one_shard_cursor(pool: PgPool) {
    let run_id = seed_pending_run(&pool, &[(0, 1)]).await;
    mark_run_running(&pool, run_id).await;

    let (left, right) = tokio::join!(dispatch_window(&pool), dispatch_window(&pool));
    let windows = [left, right]
        .into_iter()
        .flatten()
        .collect::<Vec<DispatchedRun>>();

    assert_eq!(windows.len(), 1);
    assert_eq!(windows[0].run_shard, 0);
    assert_eq!(dispatched_chunk_count(&pool, run_id, 0).await, 1);
    assert_eq!(chunk_ready_event_count(&pool, run_id).await, 1);
    assert_eq!(cursor_status(&pool, run_id, 0).await, "drained");
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn concurrent_pending_run_start_emits_one_started_event(pool: PgPool) {
    let run_id = seed_pending_run(&pool, &[(0, 1), (1, 1)]).await;

    let (left, right) = tokio::join!(dispatch_window(&pool), dispatch_window(&pool));
    let windows = [left, right]
        .into_iter()
        .flatten()
        .collect::<Vec<DispatchedRun>>();

    assert_eq!(windows.len(), 2);
    assert_eq!(dispatched_shards(&windows), BTreeSet::from([0, 1]));
    assert_eq!(run_status(&pool, run_id).await, "running");
    assert_eq!(run_started_event_count(&pool, run_id).await, 1);
    assert_eq!(chunk_ready_event_count(&pool, run_id).await, 2);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn running_dispatch_does_not_wait_on_parent_run_share_lock(pool: PgPool) {
    let run_id = seed_pending_run(&pool, &[(0, 1)]).await;
    mark_run_running(&pool, run_id).await;

    let mut tx = pool.begin().await.unwrap();
    lock_run_for_share(&mut tx, run_id).await;

    let dispatched = tokio::time::timeout(Duration::from_secs(1), dispatch_window(&pool))
        .await
        .expect("dispatch should not wait on a compatible parent run share lock")
        .unwrap();

    assert_eq!(dispatched.id, run_id);
    assert_eq!(dispatched.run_shard, 0);
    assert_eq!(dispatched.chunks_marked_dispatched, 1);
    assert_eq!(dispatched.run_started_event_records_inserted, 0);

    tx.rollback().await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn running_dispatch_waits_on_parent_run_update_lock(pool: PgPool) {
    let run_id = seed_pending_run(&pool, &[(0, 1)]).await;
    mark_run_running(&pool, run_id).await;

    let mut tx = pool.begin().await.unwrap();
    lock_run_for_update(&mut tx, run_id).await;

    let dispatch_result =
        tokio::time::timeout(Duration::from_millis(100), dispatch_window(&pool)).await;
    assert!(
        dispatch_result.is_err(),
        "dispatch should wait behind an exclusive lifecycle update lock"
    );

    tx.rollback().await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn cancel_waits_behind_active_dispatch_lifecycle_share_lock(pool: PgPool) {
    let run_id = seed_pending_run(&pool, &[(0, 1)]).await;
    mark_run_running(&pool, run_id).await;

    let mut tx = pool.begin().await.unwrap();
    lock_run_for_share(&mut tx, run_id).await;

    let cancel_result = tokio::time::timeout(
        Duration::from_millis(100),
        run_cancel::cancel_run(&pool, run_id),
    )
    .await;
    assert!(
        cancel_result.is_err(),
        "cancellation should wait behind active dispatch lifecycle locks"
    );

    tx.rollback().await.unwrap();

    let outcome = run_cancel::cancel_run(&pool, run_id)
        .await
        .unwrap()
        .unwrap();
    assert!(outcome.cancelled);
    assert_eq!(run_status(&pool, run_id).await, "cancelled");
    assert_eq!(cursor_status_count(&pool, run_id, "drained").await, 1);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn finalization_skips_run_with_active_dispatch_lifecycle_share_lock(pool: PgPool) {
    let run_id = seed_pending_run(&pool, &[(0, 1)]).await;
    mark_run_running(&pool, run_id).await;
    mark_all_chunks_completed(&pool, run_id).await;

    let mut tx = pool.begin().await.unwrap();
    lock_run_for_share(&mut tx, run_id).await;

    let skipped = run_finalize::claim_next_finalizable_run(&pool, Uuid::now_v7(), 60)
        .await
        .unwrap();
    assert!(skipped.is_none());

    tx.rollback().await.unwrap();

    let claimed = run_finalize::claim_next_finalizable_run(&pool, Uuid::now_v7(), 60)
        .await
        .unwrap();
    assert_eq!(claimed.unwrap().id, run_id);
}
