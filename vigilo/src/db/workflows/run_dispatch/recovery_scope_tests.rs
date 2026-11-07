//! Restricted-role recovery retains batched authority and durable receipts.
use futures_util::FutureExt;

use super::*;
use crate::db::workflows::run_shard_summary::test_support as fixtures;

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn recovery_scopes_multiple_owners_in_one_batch(pool: PgPool) {
    let mut runs = Vec::new();
    for tenant in [Uuid::nil(), Uuid::now_v7(), Uuid::now_v7()] {
        let owned = PgPoolOptions::new()
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
        for _ in 0..2 {
            let (run, dataset) = fixtures::fixture(&owned, 2).await;
            for ordinal in 0..2 {
                let chunk =
                    fixtures::ranged_chunk(&owned, run, dataset, ordinal, ordinal + 1, "pending")
                        .await;
                sqlx::query("UPDATE run_chunks SET status='leased',lease_token=gen_random_uuid(),leased_until=now()-interval '1 minute',recovery_count=$3 WHERE run_id=$1 AND id=$2").bind(run).bind(chunk).bind(ordinal*3).execute(&owned).await.unwrap();
            }
            runs.push((tenant, run));
        }
        owned.close().await;
    }
    let role = format!("recovery_scope_{}", Uuid::now_v7().simple());
    sqlx::raw_sql(&format!("CREATE ROLE {role} LOGIN PASSWORD 'fixture'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION vigilo_claim_chunk_recovery(integer,bigint) TO {role};")).execute(&pool).await.unwrap();
    for (table, key) in [
        ("runs", "id"),
        ("run_chunks", "run_id"),
        ("run_snapshots", "run_id"),
        ("local_shard_admissions", "run_id"),
        ("executions", "run_id"),
        ("execution_attempts", "run_id"),
        ("execution_aggregates", "run_id"),
        ("evaluator_results", "run_id"),
        ("run_chunk_settlements", "run_id"),
        ("run_chunk_settlement_consumptions", "run_id"),
        ("outbox_events", "aggregate_id"),
        ("shard_move_captures", "run_id"),
    ] {
        sqlx::raw_sql(&format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY; CREATE POLICY batch_scope ON {table} TO {role} USING (({key},tenant_id) IN (SELECT key::uuid,value::uuid FROM jsonb_each_text(NULLIF(current_setting('vigilo.run_batch',true),'')::jsonb)) AND current_setting('vigilo.actor_id',true)='' AND current_setting('vigilo.tenant_id',true)='' AND current_setting('vigilo.run_id',true)=''); GRANT SELECT,INSERT,UPDATE ON {table} TO {role};")).execute(&pool).await.unwrap();
    }
    sqlx::raw_sql(&format!("GRANT SELECT,INSERT ON outbox_delivery_queue TO {role}; ALTER TABLE outbox_delivery_queue ENABLE ROW LEVEL SECURITY; CREATE POLICY batch_scope ON outbox_delivery_queue TO {role} USING(EXISTS(SELECT 1 FROM outbox_events e WHERE e.id=event_id AND e.tenant_id=outbox_delivery_queue.tenant_id));")).execute(&pool).await.unwrap();
    sqlx::raw_sql(&format!("GRANT SELECT,INSERT,UPDATE ON shard_move_dirty_keys TO {role}; ALTER TABLE shard_move_dirty_keys ENABLE ROW LEVEL SECURITY; CREATE POLICY batch_scope ON shard_move_dirty_keys TO {role} USING(EXISTS(SELECT 1 FROM shard_move_captures c WHERE c.move_id=shard_move_dirty_keys.move_id AND c.tenant_id=shard_move_dirty_keys.tenant_id));")).execute(&pool).await.unwrap();
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
    let work = async {
        // Exact pairs, not independently admitted owner/run sets; a second run
        // belonging to the same owner must remain hidden too.
        let mut tx = restricted.begin().await.unwrap();
        let (tenant, run) = runs[2];
        let exact = std::collections::BTreeMap::from([(run, tenant)]);
        sqlx::query("SELECT set_config('vigilo.actor_id',$1,true),set_config('vigilo.tenant_id',$1,true),set_config('vigilo.run_id',$1,true)").bind(Uuid::now_v7().to_string()).execute(&mut *tx).await.unwrap();
        crate::db::scope::set_system_batch(&mut tx, &exact)
            .await
            .unwrap();
        let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM run_chunks")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(visible, 2);
        let wrong = std::collections::BTreeMap::from([(run, runs[4].0), (runs[4].1, tenant)]);
        crate::db::scope::set_system_batch(&mut tx, &wrong)
            .await
            .unwrap();
        let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM run_chunks")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(visible, 0);
        crate::db::scope::set_system(&mut tx, tenant, run)
            .await
            .unwrap();
        let batch: String = sqlx::query_scalar("SELECT current_setting('vigilo.run_batch')")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert!(batch.is_empty());
        crate::db::scope::set_system_batch(&mut tx, &exact)
            .await
            .unwrap();
        drop(tx); // SQLx must roll back batch authority before pooled reuse.
        let mut tx = crate::db::scope::begin(
            &restricted,
            crate::db::scope::UserContext::new(tenant, Uuid::now_v7()).unwrap(),
            run,
        )
        .await
        .unwrap();
        let batch: String = sqlx::query_scalar("SELECT current_setting('vigilo.run_batch')")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert!(batch.is_empty());
        tx.rollback().await.unwrap();

        sqlx::raw_sql(&format!(
            "REVOKE INSERT ON run_chunk_settlement_consumptions FROM {role}"
        ))
        .execute(&pool)
        .await
        .unwrap();
        assert!(
            recover_expired_chunk_leases(&restricted, 3, 100)
                .await
                .is_err()
        );
        let leased: i64 =
            sqlx::query_scalar("SELECT count(*) FROM run_chunks WHERE status='leased'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            leased, 12,
            "receipt failure rolls back requeues and exhausted transitions"
        );
        sqlx::raw_sql(&format!(
            "GRANT INSERT ON run_chunk_settlement_consumptions TO {role}"
        ))
        .execute(&pool)
        .await
        .unwrap();
        let (result, queries) =
            fixtures::record_queries(recover_expired_chunk_leases(&restricted, 3, 100)).await;
        let recovered = result.unwrap();
        assert_eq!(recovered.recovered_chunks, 6);
        assert_eq!(recovered.failed_chunks, 6);
        eprintln!("mixed recovery submissions: {}", queries.len());
        assert_eq!(queries.len(), 40, "{queries:?}");
        for (tenant, run) in &runs {
            let receipts:i64=sqlx::query_scalar("SELECT count(*) FROM run_chunk_settlements s JOIN run_chunk_settlement_consumptions c USING(tenant_id,run_id,run_shard,chunk_id) WHERE s.tenant_id=$1 AND s.run_id=$2").bind(tenant).bind(run).fetch_one(&pool).await.unwrap();
            assert_eq!(receipts, 1);
        }
        // Requeue-only cost is constant as the number of runs increases.
        for count in [1usize, 6] {
            let ids: Vec<_> = runs[..count].iter().map(|(_, run)| *run).collect();
            sqlx::query("UPDATE run_chunks SET status='leased',leased_until=now()-interval '1 minute',lease_token=gen_random_uuid() WHERE run_id=ANY($1) AND status='pending'").bind(&ids).execute(&pool).await.unwrap();
            let (result, queries) =
                fixtures::record_queries(recover_expired_chunk_leases(&restricted, 10, 100)).await;
            assert_eq!(result.unwrap().recovered_chunks, count as i64);
            assert_eq!(queries.len(), 4, "{queries:?}");
        }
        let (result, queries) =
            fixtures::record_queries(recover_expired_chunk_leases(&restricted, 3, 100)).await;
        assert_eq!(result.unwrap().recovered_chunks, 0);
        assert_eq!(queries.len(), 1);
        let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM run_chunks")
            .fetch_one(&restricted)
            .await
            .unwrap();
        assert_eq!(visible, 0);
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
