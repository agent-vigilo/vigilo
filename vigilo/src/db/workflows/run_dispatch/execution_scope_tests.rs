//! Restricted execution dispatch and transaction cleanup regression coverage.
use futures_util::FutureExt;

use super::*;

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn execution_dispatch_uses_claimed_scope(pool: PgPool) {
    let run = seed_pending_run(&pool, &[(0, 2)]).await;
    let other = seed_pending_run(&pool, &[(0, 1)]).await;
    let tenant = Uuid::now_v7();
    let tenant_pool = PgPoolOptions::new()
        .max_connections(1)
        .after_connect(move |c, _| {
            Box::pin(async move {
                sqlx::query("SELECT set_config('vigilo.tenant_id',$1,false)")
                    .bind(tenant.to_string())
                    .execute(c)
                    .await?;
                Ok(())
            })
        })
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    let third = seed_pending_run(&tenant_pool, &[(0, 1)]).await;
    tenant_pool.close().await;
    let role = format!("dispatch_exec_{}", Uuid::now_v7().simple());
    sqlx::raw_sql(&format!(
        "CREATE ROLE {role} LOGIN PASSWORD 'fixture'; GRANT USAGE ON SCHEMA public TO {role};"
    ))
    .execute(&pool)
    .await
    .unwrap();
    for (table, key) in [
        ("local_shard_admissions", "run_id"),
        ("run_snapshots", "run_id"),
        ("run_chunks", "run_id"),
        ("run_shard_summaries", "run_id"),
        ("shard_move_captures", "run_id"),
        ("outbox_events", "aggregate_id"),
    ] {
        sqlx::raw_sql(&format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY; CREATE POLICY work_scope ON {table} TO {role} USING(tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND {key}=NULLIF(current_setting('vigilo.run_id',true),'')::uuid AND current_setting('vigilo.actor_id',true)=''); GRANT SELECT,INSERT,UPDATE ON {table} TO {role};")).execute(&pool).await.unwrap();
    }
    sqlx::raw_sql(&format!("GRANT SELECT,INSERT,UPDATE ON outbox_delivery_queue,shard_move_dirty_keys TO {role}; ALTER TABLE outbox_delivery_queue ENABLE ROW LEVEL SECURITY; CREATE POLICY work_scope ON outbox_delivery_queue TO {role} USING(tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND EXISTS(SELECT 1 FROM outbox_events e WHERE e.id=event_id AND e.tenant_id=outbox_delivery_queue.tenant_id)); ALTER TABLE shard_move_dirty_keys ENABLE ROW LEVEL SECURITY; CREATE POLICY work_scope ON shard_move_dirty_keys TO {role} USING(tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND EXISTS(SELECT 1 FROM shard_move_captures c WHERE c.move_id=shard_move_dirty_keys.move_id));")).execute(&pool).await.unwrap();
    let restricted = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(
            pool.connect_options()
                .as_ref()
                .clone()
                .username(&role)
                .password("fixture"),
        )
        .await
        .unwrap();
    let result = std::panic::AssertUnwindSafe(async {
        let router = database::DatabaseRouter::new(database::Config {
            uri: "postgres://127.0.0.1:1/unavailable-control".into(),
            max_connections_per_pool: 1,
            acquire_timeout: Duration::from_secs(1),
            circuit_breaker_config: Default::default(),
            placement_config: database::PlacementConfig::default_single_database(),
            operation_timeout_config: None,
        });
        let mut repaired_count = 0;
        for _ in 0..4 {
            let mut claim = claim_next_dispatch_route(&pool, &[])
                .await
                .unwrap()
                .unwrap();
            let expected_run = claim.route.run_id;
            let mut snapshot = prepare_dispatch_run_snapshot_with(
                &mut claim.control_tx,
                &claim.route,
                Uuid::now_v7(),
                60,
            )
            .await
            .unwrap()
            .unwrap();
            let placement =
                crate::db::tables::shard_placements::select_shard_placement(&pool, expected_run, 0)
                    .await
                    .unwrap()
                    .unwrap();
            let route = ExecutionRoute {
                placement,
                pool: restricted.clone(),
            };
            let expected_tenant = if expected_run == third {
                tenant
            } else {
                Uuid::nil()
            };
            assert_eq!(claim.route.tenant_id, expected_tenant);
            if expected_run == other {
                let mut wrong = snapshot.clone();
                wrong.tenant_id = tenant;
                let failed = dispatch_admitted_run_window(
                    &router,
                    claim.control_tx,
                    &route,
                    1,
                    &claim.route,
                    &wrong,
                )
                .await;
                assert!(matches!(failed, Err(RoutedDispatchError::Invariant(_))));
                claim = claim_next_dispatch_route(&pool, &[])
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(claim.route.run_id, expected_run);
                snapshot = prepare_dispatch_run_snapshot_with(
                    &mut claim.control_tx,
                    &claim.route,
                    Uuid::now_v7(),
                    60,
                )
                .await
                .unwrap()
                .unwrap();
            }
            let (admitted, queries) =
                crate::db::workflows::run_shard_summary::test_support::record_queries(
                    router.begin_dispatch_admission(&route, expected_tenant, &mut claim.control_tx),
                )
                .await;
            let mut admitted = admitted.unwrap();
            let repaired = queries.len() == 4;
            repaired_count += usize::from(repaired);
            assert!(
                matches!(queries.len(), 2 | 4),
                "fence+read; missing admission also reads control and inserts: {queries:?}"
            );
            let visible: Vec<Uuid> = sqlx::query_scalar("SELECT DISTINCT run_id FROM run_chunks")
                .fetch_all(&mut *admitted)
                .await
                .unwrap();
            assert_eq!(visible, vec![expected_run]);
            let changed = sqlx::query("UPDATE run_chunks SET dispatched_at=now() WHERE run_id<>$1")
                .bind(expected_run)
                .execute(&mut *admitted)
                .await
                .unwrap()
                .rows_affected();
            assert_eq!(changed, 0);
            // Cancelling an owned transaction rolls back writes and any repair.
            let before = dispatched_chunk_count(&pool, expected_run, 0).await;
            sqlx::query("UPDATE run_chunks SET dispatched_at=now() WHERE run_id=$1")
                .bind(expected_run)
                .execute(&mut *admitted)
                .await
                .unwrap();
            let timeout = tokio::time::timeout(Duration::from_millis(10), async move {
                let _held = admitted;
                std::future::pending::<()>().await;
            })
            .await;
            assert!(timeout.is_err());
            let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM run_chunks")
                .fetch_one(&restricted)
                .await
                .unwrap();
            assert_eq!(visible, 0);
            assert_eq!(dispatched_chunk_count(&pool, expected_run, 0).await, before);
            let (dispatched, queries) =
                crate::db::workflows::run_shard_summary::test_support::record_queries(
                    dispatch_admitted_run_window(
                        &router,
                        claim.control_tx,
                        &route,
                        1,
                        &claim.route,
                        &snapshot,
                    ),
                )
                .await;
            let dispatched = dispatched
                .expect("dispatch must scope execution admission and all writes")
                .unwrap();
            assert_eq!(
                queries.len(),
                if repaired { 7 } else { 5 },
                "dispatch submission budget: {queries:?}"
            );
            assert_eq!(dispatched.chunks_marked_dispatched, 1);
            let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM run_chunks")
                .fetch_one(&restricted)
                .await
                .unwrap();
            assert_eq!(visible, 0, "scope must not survive pooled commit");
        }
        assert_eq!(
            repaired_count, 3,
            "only the first window of each run repairs admission"
        );
        let mut route = ExecutionRoute {
            placement: crate::db::tables::shard_placements::select_shard_placement(&pool, run, 0)
                .await
                .unwrap()
                .unwrap(),
            pool: restricted.clone(),
        };
        let mut control = pool.begin().await.unwrap();
        route.placement.write_epoch += 1;
        assert!(
            router
                .begin_dispatch_admission(&route, Uuid::nil(), &mut control)
                .await
                .is_err(),
            "stale local epochs must not trigger repair"
        );
        route.placement.write_epoch -= 1;
        sqlx::query("UPDATE local_shard_admissions SET state='draining' WHERE run_id=$1")
            .bind(run)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            router
                .begin_dispatch_admission(&route, Uuid::nil(), &mut control)
                .await
                .is_err(),
            "draining placement must reject new dispatch"
        );
        control.rollback().await.unwrap();
        let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM run_chunks")
            .fetch_one(&restricted)
            .await
            .unwrap();
        assert_eq!(visible, 0, "errors clear context before pool reuse");
        assert_eq!(chunk_ready_event_count(&pool, run).await, 2);
        assert_eq!(chunk_ready_event_count(&pool, other).await, 1);
        assert_eq!(chunk_ready_event_count(&pool, third).await, 1);
    })
    .catch_unwind()
    .await;
    restricted.close().await;
    sqlx::raw_sql(&format!("DROP OWNED BY {role}; DROP ROLE {role};"))
        .execute(&pool)
        .await
        .unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
