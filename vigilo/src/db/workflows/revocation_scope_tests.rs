//! Real-role routed and partial-admission settlement with preserved evidence.
use futures_util::FutureExt;

use super::*;
use crate::db::workflows::run_shard_summary::test_support as fixtures;

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn claimed_revocation_scopes_cleanup_and_preserves_results(pool: PgPool) {
    let (other, _) = fixtures::fixture(&pool, 1).await;
    let tenant = Uuid::now_v7();
    let owned = sqlx::postgres::PgPoolOptions::new()
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
    let mut runs = Vec::new();
    for partial in [false, true] {
        let (run, dataset) = fixtures::fixture(&owned, 2).await;
        let done = fixtures::ranged_chunk(&owned, run, dataset, 0, 1, "completed").await;
        let pending = fixtures::ranged_chunk(&owned, run, dataset, 1, 2, "pending").await;
        let execution = Uuid::now_v7();
        let attempt = Uuid::now_v7();
        fixtures::seed_execution(&owned, run, 4, done, execution, attempt, "completed").await;
        fixtures::seed_aggregate(&owned, run, 4, execution, attempt, "passed").await;
        if partial {
            sqlx::query("INSERT INTO dataset_uploads(id,storage_alias,binding_hash,identity,limits) VALUES($1,'test',repeat('0',64),'{}','{}')").bind(run).execute(&owned).await.unwrap();
            sqlx::query("INSERT INTO dataset_catalog(id,storage_alias,binding_hash,dataset_hash,dataset_version_id,limits,manifest) VALUES($1,'test',repeat('0',64),repeat('0',64),$2,'{}','{}')").bind(run).bind(dataset).execute(&owned).await.unwrap();
            sqlx::query("INSERT INTO run_plans(run_id,dataset_upload_id,storage_binding_hash,limits,execution_plan,assignments) VALUES($1,$1,repeat('0',64),'{}','{}','[{\"run_shard\":4,\"database_alias\":\"primary\"}]')").bind(run).execute(&owned).await.unwrap();
            sqlx::query("INSERT INTO run_plan_shards(run_id,run_shard,database_alias,case_count,chunk_count,planned_chunk_hash,case_projection_hash) VALUES($1,4,'primary',2,2,'fixture','fixture')").bind(run).execute(&owned).await.unwrap();
            sqlx::query("UPDATE runs SET status='creating' WHERE id=$1")
                .bind(run)
                .execute(&owned)
                .await
                .unwrap();
        } else {
            sqlx::query("INSERT INTO shard_placements(run_id,run_shard,database_alias,status) VALUES($1,4,'primary','active')").bind(run).execute(&owned).await.unwrap();
        }
        request(&owned, run, ExecutionControl::Revoked)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE run_control_deliveries SET applied_at=now(),quiescent_at=now() WHERE run_id=$1",
        )
        .bind(run)
        .execute(&owned)
        .await
        .unwrap();
        runs.push((run, pending, execution, partial));
    }
    owned.close().await;
    let role = format!("revocation_scope_{}", Uuid::now_v7().simple());
    sqlx::raw_sql(&format!("CREATE ROLE {role} LOGIN PASSWORD 'fixture'; GRANT USAGE ON SCHEMA public TO {role}; GRANT SELECT ON database_placements TO {role}; GRANT EXECUTE ON FUNCTION vigilo_claim_revocation_settlements(uuid) TO {role};")).execute(&pool).await.unwrap();
    for (table, key) in [
        ("runs", "id"),
        ("run_plans", "run_id"),
        ("run_plan_shards", "run_id"),
        ("shard_placements", "run_id"),
        ("run_shard_dispatch_cursors", "run_id"),
        ("run_chunks", "run_id"),
        ("run_snapshots", "run_id"),
        ("local_shard_admissions", "run_id"),
        ("executions", "run_id"),
        ("execution_attempts", "run_id"),
        ("execution_aggregates", "run_id"),
        ("evaluator_results", "run_id"),
        ("run_shard_summaries", "run_id"),
        ("run_chunk_settlements", "run_id"),
        ("run_chunk_settlement_consumptions", "run_id"),
        ("shard_move_captures", "run_id"),
        ("outbox_events", "aggregate_id"),
    ] {
        sqlx::raw_sql(&format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY; CREATE POLICY work_scope ON {table} TO {role} USING(tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND {key}=NULLIF(current_setting('vigilo.run_id',true),'')::uuid AND current_setting('vigilo.actor_id',true)=''); GRANT SELECT,INSERT,UPDATE ON {table} TO {role};")).execute(&pool).await.unwrap();
    }
    sqlx::raw_sql(&format!("GRANT SELECT,INSERT,UPDATE ON outbox_delivery_queue,shard_move_dirty_keys TO {role}; ALTER TABLE outbox_delivery_queue ENABLE ROW LEVEL SECURITY; CREATE POLICY work_scope ON outbox_delivery_queue TO {role} USING(tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND EXISTS(SELECT 1 FROM outbox_events e WHERE e.id=event_id AND e.tenant_id=outbox_delivery_queue.tenant_id)); ALTER TABLE shard_move_dirty_keys ENABLE ROW LEVEL SECURITY; CREATE POLICY work_scope ON shard_move_dirty_keys TO {role} USING(tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND EXISTS(SELECT 1 FROM shard_move_captures c WHERE c.move_id=shard_move_dirty_keys.move_id));")).execute(&pool).await.unwrap();
    let restricted = sqlx::postgres::PgPoolOptions::new()
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
    let router = DatabaseRouter::new(crate::context::database::Config {
        uri: "postgres://127.0.0.1:1/unused".into(),
        max_connections_per_pool: 1,
        acquire_timeout: std::time::Duration::from_secs(2),
        circuit_breaker_config: Default::default(),
        placement_config: crate::context::database::PlacementConfig::default_single_database(),
        operation_timeout_config: None,
    });
    router.control_pool.set(restricted.clone()).unwrap();
    let work = async {
        for (run, pending, execution, partial) in runs {
            let claim: Settlement =
                sqlx::query_as("SELECT * FROM vigilo_claim_revocation_settlements($1)")
                    .bind(run)
                    .fetch_one(&restricted)
                    .await
                    .unwrap();
            assert_eq!(claim.tenant_id, tenant);
            assert!(
                crate::db::workflows::run_cancel::cancel_claimed_run_routed(
                    &router,
                    Uuid::nil(),
                    run
                )
                .await
                .unwrap()
                .is_none(),
                "wrong ownership cannot see the run"
            );
            let (deferred, queries) =
                fixtures::record_queries(defer_settlement(&restricted, &claim)).await;
            assert!(deferred.unwrap());
            assert_eq!(queries.len(), 2);
            // Failed local mutation must leave pending work available for replay.
            sqlx::raw_sql(&format!("REVOKE UPDATE ON execution_attempts FROM {role}"))
                .execute(&pool)
                .await
                .unwrap();
            assert!(
                crate::db::workflows::run_cancel::cancel_claimed_run_routed(&router, tenant, run)
                    .await
                    .is_err()
            );
            let status: String =
                sqlx::query_scalar("SELECT status FROM run_chunks WHERE run_id=$1 AND id=$2")
                    .bind(run)
                    .bind(pending)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(status, "pending");
            sqlx::raw_sql(&format!("GRANT UPDATE ON execution_attempts TO {role}"))
                .execute(&pool)
                .await
                .unwrap();
            let (outcome, queries) = fixtures::record_queries(
                crate::db::workflows::run_cancel::cancel_claimed_run_routed(&router, tenant, run),
            )
            .await;
            let outcome = outcome.unwrap().unwrap();
            assert_eq!(outcome.run.status, "cancelled");
            assert_eq!(outcome.chunks_cancelled, 1);
            assert_eq!(
                queries.len(),
                if partial { 38 } else { 34 },
                "bounded settlement query budget: {queries:?}"
            );
            eprintln!(
                "scoped settlement partial={partial}: {} submissions",
                queries.len()
            );
            let status: String =
                sqlx::query_scalar("SELECT status::text FROM executions WHERE run_id=$1 AND id=$2")
                    .bind(run)
                    .bind(execution)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(status, "completed");
            let passed:i64=sqlx::query_scalar("SELECT count(*) FROM execution_aggregates WHERE run_id=$1 AND overall_status='passed'").bind(run).fetch_one(&pool).await.unwrap();
            assert_eq!(passed, 1);
            let receipts: i64 =
                sqlx::query_scalar("SELECT count(*) FROM run_chunk_settlements WHERE run_id=$1")
                    .bind(run)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(receipts, 2);
            let replay =
                crate::db::workflows::run_cancel::cancel_claimed_run_routed(&router, tenant, run)
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(replay.chunks_cancelled, 0);
            assert!(
                !acknowledge_settlement(&restricted, tenant, run, claim.revision, Uuid::now_v7())
                    .await
                    .unwrap()
            );
            let (acked, queries) = fixtures::record_queries(acknowledge_settlement(
                &restricted,
                tenant,
                run,
                claim.revision,
                claim.claim_token,
            ))
            .await;
            assert!(acked.unwrap());
            assert_eq!(queries.len(), 2);
            assert!(!defer_settlement(&restricted, &claim).await.unwrap());
            let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM runs")
                .fetch_one(&restricted)
                .await
                .unwrap();
            assert_eq!(visible, 0);
        }
        let status: String = sqlx::query_scalar("SELECT status::text FROM runs WHERE id=$1")
            .bind(other)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_ne!(status, "cancelled");
    };
    let result = std::panic::AssertUnwindSafe(work).catch_unwind().await;
    restricted.close().await;
    sqlx::raw_sql(&format!("DROP OWNED BY {role}; DROP ROLE {role}"))
        .execute(&pool)
        .await
        .unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
