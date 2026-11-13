//! Execution-local bootstrap and lease persistence under restricted work policies.

use futures_util::FutureExt;

use super::*;
use crate::db::workflows::run_shard_summary::test_support::{
    fixture,
    ranged_chunk,
    record_queries,
};

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn worker_bootstrap_verifies_local_identity_before_protected_admission(pool: PgPool) {
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
    let (run, dataset) = fixture(&owned, 1).await;
    let id = ranged_chunk(&owned, run, dataset, 0, 1, "pending").await;
    sqlx::query("UPDATE run_snapshots SET config_snapshot=config_snapshot || '{\"run_plan\":{\"digest\":\"accepted-root\"}}'::jsonb WHERE run_id=$1")
        .bind(run).execute(&owned).await.unwrap();
    owned.close().await;
    let role = format!("worker_scope_{}", Uuid::now_v7().simple());
    let owner = format!("worker_lookup_{}", Uuid::now_v7().simple());
    sqlx::raw_sql(&format!("CREATE ROLE {role} LOGIN PASSWORD 'fixture'; GRANT USAGE ON SCHEMA public TO {role}; GRANT SELECT ON database_placements TO {role};
    GRANT EXECUTE ON FUNCTION vigilo_verify_worker_identity(uuid,smallint,uuid,uuid,text) TO {role};"))
        .execute(&pool).await.unwrap();
    for (table, key) in [
        ("evaluator_results", "run_id"),
        ("evaluator_diagnostics", "run_id"),
        ("execution_attempts", "run_id"),
        ("execution_aggregates", "run_id"),
        ("run_chunk_settlement_consumptions", "run_id"),
        ("executions", "run_id"),
        ("run_chunks", "run_id"),
        ("run_snapshots", "run_id"),
        ("shard_placements", "run_id"),
        ("local_shard_admissions", "run_id"),
        ("run_chunk_settlements", "run_id"),
        ("run_shard_summaries", "run_id"),
        ("shard_move_captures", "run_id"),
    ] {
        sqlx::raw_sql(&format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY; CREATE POLICY work ON {table} TO {role} USING(tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND {key}=NULLIF(current_setting('vigilo.run_id',true),'')::uuid AND current_setting('vigilo.actor_id',true)='' AND COALESCE(current_setting('vigilo.run_batch',true),'')=''); GRANT SELECT,INSERT,UPDATE ON {table} TO {role};")).execute(&pool).await.unwrap();
    }
    sqlx::raw_sql(&format!("CREATE ROLE {owner} NOLOGIN; GRANT USAGE ON SCHEMA public TO {owner}; GRANT SELECT ON run_chunks,run_snapshots TO {owner}; CREATE POLICY lookup_owner ON run_chunks TO {owner} USING(true); CREATE POLICY lookup_owner ON run_snapshots TO {owner} USING(true); ALTER FUNCTION vigilo_verify_worker_identity(uuid,smallint,uuid,uuid,text) OWNER TO {owner};")).execute(&pool).await.unwrap();
    let restricted=sqlx::postgres::PgPoolOptions::new().max_connections(1)
        .after_connect(|c,_|Box::pin(async move {
            sqlx::query("SELECT set_config('vigilo.tenant_id','',false),set_config('vigilo.run_id','',false),set_config('vigilo.actor_id','',false),set_config('vigilo.run_batch','',false)").execute(c).await?;Ok(())
        })).connect_with(pool.connect_options().as_ref().clone().username(&role).password("fixture")).await.unwrap();
    let result = std::panic::AssertUnwindSafe(async {
        let router = database::DatabaseRouter::new(database::Config {
            uri: "postgres://127.0.0.1:1/unavailable-control".into(),
            max_connections_per_pool: 1,
            acquire_timeout: std::time::Duration::from_secs(1),
            circuit_breaker_config: Default::default(),
            placement_config: database::PlacementConfig::default_single_database(),
            operation_timeout_config: Default::default(),
        });
        let route = ExecutionWriteRoute {
            pool: restricted.clone(),
            hint: super::super::local_shard_admission::LocalShardRouteHint {
                run_id: run,
                run_shard: 4,
                database_alias: "primary".into(),
                write_epoch: 1,
            },
        };
        sqlx::query("SELECT 1").execute(&restricted).await.unwrap();
        for (claimed_owner, digest, chunk_id) in [
            (Uuid::now_v7(), "accepted-root", id),
            (tenant, "forged-root", id),
            (tenant, "accepted-root", Uuid::now_v7()),
        ] {
            assert!(
                claim_hinted_chunk_for_processing(
                    &router,
                    &route,
                    chunk_id,
                    60,
                    Some(&AcceptedWork {
                        tenant_id: claimed_owner,
                        plan_digest: digest
                    })
                )
                .await
                .unwrap()
                .is_none()
            );
        }
        let mut wrong = route.clone();
        wrong.hint.write_epoch += 1;
        let stale = claim_hinted_chunk_for_processing(
            &router, &wrong, id, 60,
            Some(&AcceptedWork { tenant_id: tenant, plan_digest: "accepted-root" }),
        ).await.unwrap_err();
        assert!(stale.downcast_ref::<crate::db::workflows::local_shard_admission::LocalShardAdmissionError>().is_some());
        let verified = *stale.downcast_ref::<crate::db::scope::VerifiedWorkerIdentity>().unwrap();
        assert_eq!(verified.tenant_id(), tenant);
        assert_eq!(verified.run_id(), run);
        // The verifier retains the same movement lock through admission/claim.
        let held = router
            .begin_execution_write_admission(
                &route,
                id,
                Some(&AcceptedWork {
                    tenant_id: tenant,
                    plan_digest: "accepted-root",
                }),
            )
            .await
            .unwrap()
            .unwrap();
        let exclusive: bool = sqlx::query_scalar(
            "SELECT pg_try_advisory_xact_lock(hashtextextended($1::uuid::text,4))",
        )
        .bind(run)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(!exclusive);
        held.rollback().await.unwrap();
        let (chunk, calls) = record_queries(claim_hinted_chunk_for_processing(
            &router,
            &route,
            id,
            60,
            Some(&AcceptedWork {
                tenant_id: tenant,
                plan_digest: "accepted-root",
            }),
        ))
        .await;
        let chunk = chunk
            .unwrap()
            .expect("verified local work must be claimable");
        assert_eq!(
            calls.len(),
            5,
            "three work submissions plus two cold SQLx enum metadata reads: {calls:#?}"
        );
        assert_eq!(
            calls
                .iter()
                .filter(
                    |s| s.contains("pg_catalog.pg_type") || s.contains("FROM pg_catalog.pg_enum")
                )
                .count(),
            2
        );
        assert_eq!(chunk.tenant_id, tenant);
        assert_eq!(chunk.id, id);
        assert!(
            extend_chunk_lease(&restricted, &chunk, 120)
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            release_chunk_as_pending(&restricted, &chunk).await.unwrap(),
            1
        );
        sqlx::query("UPDATE run_chunks SET execution_control='paused' WHERE id=$1")
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            claim_hinted_chunk_for_processing(
                &router,
                &route,
                id,
                60,
                Some(&AcceptedWork {
                    tenant_id: tenant,
                    plan_digest: "accepted-root"
                })
            )
            .await
            .unwrap()
            .is_none()
        );
        assert!(
            is_verified_chunk_paused(
                &restricted,
                run,
                4,
                id,
                &AcceptedWork {
                    tenant_id: tenant,
                    plan_digest: "accepted-root"
                }
            )
            .await
            .unwrap()
        );
        assert!(
            !is_verified_chunk_paused(
                &restricted,
                run,
                4,
                id,
                &AcceptedWork {
                    tenant_id: Uuid::now_v7(),
                    plan_digest: "accepted-root"
                }
            )
            .await
            .unwrap()
        );
        sqlx::query("UPDATE run_chunks SET execution_control='enabled' WHERE id=$1")
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();

        let (chunk, calls) = record_queries(claim_hinted_chunk_for_processing(
            &router,
            &route,
            id,
            60,
            Some(&AcceptedWork {
                tenant_id: tenant,
                plan_digest: "accepted-root",
            }),
        ))
        .await;
        let chunk = chunk.unwrap().unwrap();
        assert_eq!(
            calls.len(),
            3,
            "warm claim must retain the existing submission budget"
        );
        assert_eq!(
            mark_chunk_failed_and_settle(&restricted, &chunk)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            mark_chunk_failed_and_settle(&restricted, &chunk)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM run_chunk_settlements WHERE chunk_id=$1"
            )
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap(),
            1
        );
        let mut movement = pool.begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::uuid::text,4))")
            .bind(run)
            .execute(&mut *movement)
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(150),
                claim_hinted_chunk_for_processing(
                    &router,
                    &route,
                    id,
                    60,
                    Some(&AcceptedWork {
                        tenant_id: tenant,
                        plan_digest: "accepted-root"
                    })
                )
            )
            .await
            .is_err()
        );
        movement.rollback().await.unwrap();
        // Exceptional control discovery uses the verified error context, never
        // message fields, and releases this same one-connection pool before reuse.
        sqlx::query("INSERT INTO shard_placements(tenant_id,run_id,run_shard,database_alias,status,write_epoch) VALUES($1,$2,4,'primary','active',2)").bind(tenant).bind(run).execute(&pool).await.unwrap();
        router.control_pool.set(restricted.clone()).unwrap();
        let (refreshed, calls) = record_queries(router.execution_route_for_verified_worker(verified)).await;
        assert_eq!(refreshed.unwrap().placement.write_epoch, 2);
        assert_eq!(calls.len(), 4, "scoped route read plus two existing cold placement-catalog reads: {calls:#?}");
        assert_eq!(calls.iter().filter(|s| s.contains("FROM database_placements")).count(), 2);
        let (refreshed, calls) = record_queries(router.execution_route_for_verified_worker(verified)).await;
        assert_eq!(refreshed.unwrap().placement.write_epoch, 2);
        assert_eq!(calls.len(), 3, "scoped route read plus existing placement status read: {calls:#?}");
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM run_chunks")
                .fetch_one(&restricted)
                .await
                .unwrap(),
            0
        );
    })
    .catch_unwind()
    .await;
    restricted.close().await;
    sqlx::raw_sql(&format!(
        "DROP OWNED BY {role}; DROP ROLE {role}; DROP OWNED BY {owner}; DROP ROLE {owner};"
    ))
    .execute(&pool)
    .await
    .unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
