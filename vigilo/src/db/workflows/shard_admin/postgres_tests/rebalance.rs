use super::*;
#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx shard admin tests"]
async fn rebalance_defers_a_draining_shard_until_active_work_drains(pool: PgPool) {
    let database_url = isolated_database_url(&pool).await;
    let database_router = database_router_with_control_pool(pool.clone(), database_url);
    let (operation_id, items) = seed_rebalance_operation(&pool, 1).await;
    let (run_id, run_shard) = items[0];
    let dataset_id = Uuid::now_v7();
    let dataset_version_id = Uuid::now_v7();
    seed_run(&pool, run_id, dataset_id, dataset_version_id).await;
    seed_run_snapshot(&pool, run_id, run_shard, dataset_id, dataset_version_id).await;
    sqlx::query(         r#"             INSERT INTO shard_placements (run_id, run_shard, database_alias, status)             VALUES ($1::uuid, $2, 'primary', 'active')             "#,     )     .bind(run_id)     .bind(run_shard)     .execute(&pool)     .await     .unwrap();
    sqlx::query(         r#"             INSERT INTO run_chunks (                 id, run_id, run_shard, dataset_version_id, profile_group_id,                 ordinal_start, ordinal_end, status, lease_token, leased_until             )             VALUES (                 $1::uuid, $2::uuid, $3, $4::uuid, 'default',                 0, 1, 'leased', gen_random_uuid(), now() + interval '1 minute'             )             "#,     )     .bind(Uuid::now_v7())     .bind(run_id)     .bind(run_shard)     .bind(dataset_version_id)     .execute(&pool)     .await     .unwrap();
    let outcome = apply_shard_rebalance(
        &database_router,
        operation_id,
        ShardRebalanceApplyOptions {
            max_items: 1,
            lease_seconds: 60,
            force: false,
        },
    )
    .await
    .unwrap();
    let placement = shard_placements::select_shard_placement(&pool, run_id, run_shard)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outcome.operation.status, REBALANCE_OPERATION_STATUS_RUNNING);
    assert_eq!(outcome.processed_items[0].status, "pending");
    assert_eq!(placement.status, SHARD_PLACEMENT_STATUS_DRAINING);
    assert_eq!(
        placement.move_target_database_alias.as_deref(),
        Some("shard_001")
    );
}
#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx shard admin tests"]
async fn rebalance_claims_pending_and_expired_items_but_skips_fresh_claims(pool: PgPool) {
    let (operation_id, items) = seed_rebalance_operation(&pool, 3).await;
    let fresh_token = Uuid::now_v7();
    let fresh_owner = Uuid::now_v7();
    let expired_token = Uuid::now_v7();
    let expired_owner = Uuid::now_v7();
    sqlx::query(         r#"             UPDATE shard_rebalance_items             SET status = 'running',                 claim_token = $2::uuid,                 claimed_by = $3::uuid,                 claimed_until = now() + interval '1 hour'             WHERE operation_id = $1::uuid               AND sequence_no = 1             "#,     )     .bind(operation_id)     .bind(fresh_token)     .bind(fresh_owner)     .execute(&pool)     .await     .unwrap();
    sqlx::query(         r#"             UPDATE shard_rebalance_items             SET status = 'running',                 claim_token = $2::uuid,                 claimed_by = $3::uuid,                 claimed_until = now() - interval '1 hour'             WHERE operation_id = $1::uuid               AND sequence_no = 2             "#,     )     .bind(operation_id)     .bind(expired_token)     .bind(expired_owner)     .execute(&pool)     .await     .unwrap();
    let claimant = Uuid::now_v7();
    let pending = claim_next_rebalance_apply_item(&pool, operation_id, claimant, 60)
        .await
        .unwrap()
        .unwrap();
    let expired = claim_next_rebalance_apply_item(&pool, operation_id, claimant, 60)
        .await
        .unwrap()
        .unwrap();
    let none = claim_next_rebalance_apply_item(&pool, operation_id, claimant, 60)
        .await
        .unwrap();
    assert_eq!(pending.item.run_id, items[0].0);
    assert!(!pending.reclaimed);
    assert_eq!(expired.item.run_id, items[2].0);
    assert!(expired.reclaimed);
    assert!(none.is_none());
    let fresh = sqlx::query_as::<_, ShardRebalanceItem>(         r#"             SELECT *             FROM shard_rebalance_items             WHERE operation_id = $1::uuid               AND sequence_no = 1             "#,     )     .bind(operation_id)     .fetch_one(&pool)     .await     .unwrap();
    assert_eq!(fresh.claim_token, Some(fresh_token));
    assert_eq!(fresh.claimed_by, Some(fresh_owner));
}
#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx shard admin tests"]
async fn expired_rebalance_claim_can_be_reclaimed_and_fences_stale_settlement(pool: PgPool) {
    let (operation_id, _) = seed_rebalance_operation(&pool, 1).await;
    let first = claim_next_rebalance_apply_item(&pool, operation_id, Uuid::now_v7(), 60)
        .await
        .unwrap()
        .unwrap();
    let first_token = first.item.claim_token.unwrap();
    sqlx::query(         r#"             UPDATE shard_rebalance_items             SET claimed_until = now() - interval '1 second'             WHERE operation_id = $1::uuid             "#,     )     .bind(operation_id)     .execute(&pool)     .await     .unwrap();
    let second = claim_next_rebalance_apply_item(&pool, operation_id, Uuid::now_v7(), 60)
        .await
        .unwrap()
        .unwrap();
    let second_token = second.item.claim_token.unwrap();
    assert!(second.reclaimed);
    assert_ne!(first_token, second_token);
    let stale = mark_rebalance_item_completed(&pool, &first.item, first_token)
        .await
        .unwrap();
    assert!(stale.is_none());
    let completed = mark_rebalance_item_completed(&pool, &second.item, second_token)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.status, REBALANCE_ITEM_STATUS_COMPLETED);
    assert!(completed.claim_token.is_none());
    assert!(completed.claimed_by.is_none());
    assert!(completed.claimed_until.is_none());
}
#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx shard admin tests"]
async fn rebalance_cancellation_preserves_fresh_claim_and_fences_claimed_cancel(pool: PgPool) {
    let database_url = isolated_database_url(&pool).await;
    let database_router = database_router_with_control_pool(pool.clone(), database_url);
    let (operation_id, _) = seed_rebalance_operation(&pool, 3).await;
    let fresh = claim_next_rebalance_apply_item(&pool, operation_id, Uuid::now_v7(), 60)
        .await
        .unwrap()
        .unwrap();
    let expired = claim_next_rebalance_apply_item(&pool, operation_id, Uuid::now_v7(), 60)
        .await
        .unwrap()
        .unwrap();
    sqlx::query(         r#"             UPDATE shard_rebalance_items             SET claimed_until = now() - interval '1 second'             WHERE operation_id = $1::uuid               AND run_id = $2::uuid               AND run_shard = $3             "#,     )     .bind(operation_id)     .bind(expired.item.run_id)     .bind(expired.item.run_shard)     .execute(&pool)     .await     .unwrap();
    let cancelled = cancel_shard_rebalance(&database_router, operation_id)
        .await
        .unwrap();
    assert_eq!(cancelled.status, REBALANCE_OPERATION_STATUS_CANCELLED);
    assert_eq!(cancelled.cancelled_item_count, 2);
    let fresh_state = sqlx::query_as::<_, ShardRebalanceItem>(         r#"             SELECT *             FROM shard_rebalance_items             WHERE operation_id = $1::uuid               AND run_id = $2::uuid               AND run_shard = $3             "#,     )     .bind(operation_id)     .bind(fresh.item.run_id)     .bind(fresh.item.run_shard)     .fetch_one(&pool)     .await     .unwrap();
    assert_eq!(fresh_state.status, "running");
    assert_eq!(fresh_state.claim_token, fresh.item.claim_token);
    let claim_token = fresh.item.claim_token.unwrap();
    let settled = mark_rebalance_item_cancelled(&pool, &fresh.item, claim_token)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(settled.status, REBALANCE_ITEM_STATUS_CANCELLED);
    let operation = refresh_rebalance_operation_status(&pool, operation_id)
        .await
        .unwrap();
    assert_eq!(operation.status, REBALANCE_OPERATION_STATUS_CANCELLED);
    assert_eq!(operation.cancelled_item_count, 3);
}
#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx shard admin tests"]
async fn concurrent_rebalance_apply_workers_move_each_item_once(pool: PgPool) {
    let database_url = isolated_database_url(&pool).await;
    let database_router = database_router_with_control_pool(pool.clone(), database_url);
    let (operation_id, items) = seed_rebalance_operation(&pool, 6).await;
    for (run_id, run_shard) in &items {
        sqlx::query(             r#"                 INSERT INTO shard_placements (                     run_id,                     run_shard,                     database_alias,                     status                 )                 VALUES ($1::uuid, $2, 'primary', 'active')                 "#,         )         .bind(run_id)         .bind(run_shard)         .execute(&pool)         .await         .unwrap();
    }
    let options = ShardRebalanceApplyOptions {
        max_items: items.len(),
        lease_seconds: 60,
        force: false,
    };
    let (first, second) = tokio::join!(
        apply_shard_rebalance(&database_router, operation_id, options),
        apply_shard_rebalance(&database_router, operation_id, options),
    );
    let first = first.unwrap();
    let second = second.unwrap();
    let processed = first
        .processed_items
        .iter()
        .chain(&second.processed_items)
        .map(|item| (item.run_id, item.run_shard))
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        first.processed_items.len() + second.processed_items.len(),
        items.len()
    );
    assert_eq!(processed.len(), items.len());
    let operation = select_rebalance_operation(&pool, operation_id)
        .await
        .unwrap()
        .unwrap();
    let item_states = sqlx::query_as::<_, (i32, String, Option<String>)>(         r#"             SELECT sequence_no, status, error_message             FROM shard_rebalance_items             WHERE operation_id = $1::uuid             ORDER BY sequence_no             "#,     )     .bind(operation_id)     .fetch_all(&pool)     .await     .unwrap();
    assert_eq!(
        operation.status, REBALANCE_OPERATION_STATUS_COMPLETED,
        "unexpected item states: {item_states:?}"
    );
    assert_eq!(operation.completed_item_count, items.len() as i32);
    let route_versions = sqlx::query_scalar::<_, i64>(         r#"             SELECT route_version             FROM shard_placements             WHERE run_id = ANY($1::uuid[])             ORDER BY run_id             "#,     )     .bind(items.iter().map(|(run_id, _)| *run_id).collect::<Vec<_>>())     .fetch_all(&pool)     .await     .unwrap();
    assert_eq!(route_versions.len(), items.len());
    assert!(route_versions.into_iter().all(|version| version == 5));
}
