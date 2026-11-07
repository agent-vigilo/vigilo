//! Restricted coordinator finalization across routed reads and terminal commits.
use futures_util::FutureExt;

use super::*;

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn finalization_carries_ownership_through_reads_and_commit(pool: PgPool) {
    let tenant = Uuid::now_v7();
    let mut runs = Vec::new();
    for (index, owner) in [tenant, tenant, Uuid::now_v7()].into_iter().enumerate() {
        let owned = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .after_connect(move |c, _| {
                Box::pin(async move {
                    sqlx::query("SELECT set_config('vigilo.tenant_id',$1,false)")
                        .bind(owner.to_string())
                        .execute(c)
                        .await?;
                    Ok(())
                })
            })
            .connect_with(pool.connect_options().as_ref().clone())
            .await
            .unwrap();
        let (run, dataset) = fixture(&owned, 1).await;
        route(&owned, run).await;
        let chunk = ranged_chunk(
            &owned,
            run,
            dataset,
            0,
            1,
            if index == 0 { "failed" } else { "pending" },
        )
        .await;
        let mut tx = owned.begin().await.unwrap();
        if index == 0 {
            run_shard_summary::settle_chunk_with(&mut tx, run, 4, chunk)
                .await
                .unwrap();
        }
        run_shard_summary::initialize_run_shard_summary_with(&mut tx, run, 4)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        if index == 0 {
            assert_eq!(
                run_shard_summary::advance_pending_summaries(&owned, &config().aggregation)
                    .await
                    .unwrap(),
                2
            );
        }
        runs.push(run);
        owned.close().await;
    }
    let role = format!("finalization_scope_{}", Uuid::now_v7().simple());
    sqlx::raw_sql(&format!("CREATE ROLE {role} LOGIN PASSWORD 'fixture'; GRANT USAGE ON SCHEMA public TO {role}; GRANT SELECT ON database_placements TO {role}; GRANT EXECUTE ON FUNCTION vigilo_finalization_backlog(),vigilo_next_finalization_candidate(uuid[]),vigilo_claim_finalization(uuid,uuid,integer) TO {role};")).execute(&pool).await.unwrap();
    for (table, key) in [
        ("runs", "id"),
        ("shard_placements", "run_id"),
        ("run_shard_dispatch_cursors", "run_id"),
        ("run_shard_summaries", "run_id"),
        ("run_scorecards", "run_id"),
        ("outbox_events", "aggregate_id"),
        ("shard_move_captures", "run_id"),
    ] {
        sqlx::raw_sql(&format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY; CREATE POLICY work_scope ON {table} TO {role} USING(tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND {key}=NULLIF(current_setting('vigilo.run_id',true),'')::uuid AND current_setting('vigilo.actor_id',true)='' AND COALESCE(current_setting('vigilo.run_batch',true),'')=''); GRANT SELECT,INSERT,UPDATE ON {table} TO {role};")).execute(&pool).await.unwrap();
    }
    sqlx::raw_sql(&format!("GRANT SELECT,INSERT,UPDATE ON shard_move_dirty_keys,outbox_delivery_queue TO {role}; ALTER TABLE shard_move_dirty_keys ENABLE ROW LEVEL SECURITY; CREATE POLICY work_scope ON shard_move_dirty_keys TO {role} USING(EXISTS(SELECT 1 FROM shard_move_captures c WHERE c.move_id=shard_move_dirty_keys.move_id AND c.tenant_id=shard_move_dirty_keys.tenant_id)); ALTER TABLE outbox_delivery_queue ENABLE ROW LEVEL SECURITY; CREATE POLICY work_scope ON outbox_delivery_queue TO {role} USING(EXISTS(SELECT 1 FROM outbox_events e WHERE e.id=event_id AND e.tenant_id=outbox_delivery_queue.tenant_id));")).execute(&pool).await.unwrap();
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
    let context = context(&restricted);
    let router = context.dbr().await.unwrap();
    router.control_pool.set(restricted.clone()).unwrap();
    let work = async {
        let wrong = collect_run_shard_summaries(router, Uuid::now_v7(), Uuid::now_v7(), runs[0])
            .await
            .unwrap();
        assert!(wrong.output.summaries.is_empty());
        let (result, queries) = record_queries(collect_run_shard_summaries(
            router,
            Uuid::now_v7(),
            tenant,
            runs[0],
        ))
        .await;
        let collection = result.unwrap();
        assert!(collection.output.complete);
        assert_eq!(collection.output.summaries.len(), 1);
        assert_eq!(
            queries
                .iter()
                .filter(|s| s.contains("SET LOCAL vigilo.tenant_id"))
                .count(),
            2,
            "{queries:?}"
        );
        assert_eq!(
            queries
                .iter()
                .filter(|s| s.contains("FROM run_shard_summaries"))
                .count(),
            1,
            "{queries:?}"
        );
        assert_eq!(
            queries
                .iter()
                .filter(|s| s.contains("FROM shard_placements"))
                .count(),
            1,
            "{queries:?}"
        );
        assert_eq!(queries.len(), 6, "{queries:?}");
        eprintln!("scoped summary collection: {} submissions", queries.len());
        let (checked, queries) = record_queries(run_finalize::mark_finalization_candidate_checked(
            &restricted,
            tenant,
            runs[1],
        ))
        .await;
        assert!(checked.unwrap());
        assert_eq!(queries.len(), 1);
        assert!(
            !run_finalize::mark_finalization_candidate_checked(
                &restricted,
                Uuid::now_v7(),
                runs[1]
            )
            .await
            .unwrap()
        );
        let coordinator = Uuid::now_v7();
        let claim =
            run_finalize::claim_finalization_candidate(&restricted, runs[0], coordinator, 60)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(claim.tenant_id, tenant);
        let (stale, queries) = record_queries(run_finalize::finalize_claimed_run_from_summaries(
            &restricted,
            tenant,
            runs[0],
            Uuid::now_v7(),
            &claim.aggregation_policy_hash,
            &collection.output.summaries,
        ))
        .await;
        assert!(stale.unwrap().is_none());
        assert_eq!(queries.len(), 2);
        assert!(
            run_finalize::finalize_claimed_run_from_summaries(
                &restricted,
                Uuid::now_v7(),
                runs[0],
                coordinator,
                &claim.aggregation_policy_hash,
                &collection.output.summaries
            )
            .await
            .unwrap()
            .is_none()
        );
        // Fail after terminal/event writes reach the delivery trigger, proving
        // scorecard, root and event changes cannot partially commit.
        sqlx::raw_sql("CREATE FUNCTION reject_final_delivery() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected final delivery failure'; END $$; CREATE TRIGGER reject_final_delivery BEFORE INSERT ON outbox_delivery_queue FOR EACH ROW EXECUTE FUNCTION reject_final_delivery();").execute(&pool).await.unwrap();
        let error = run_finalize::finalize_claimed_run_from_summaries(
            &restricted,
            tenant,
            runs[0],
            coordinator,
            &claim.aggregation_policy_hash,
            &collection.output.summaries,
        )
        .await
        .unwrap_err();
        assert!(format!("{error:#}").contains("injected final delivery failure"));
        let state:(String,i64,i64)=sqlx::query_as("SELECT status::text,(SELECT count(*) FROM run_scorecards WHERE run_id=$1),(SELECT count(*) FROM outbox_events WHERE aggregate_id=$1 AND event_type='run.completed') FROM runs WHERE id=$1").bind(runs[0]).fetch_one(&pool).await.unwrap();
        assert_eq!(state, ("finalizing".into(), 0, 0));
        sqlx::raw_sql("DROP TRIGGER reject_final_delivery ON outbox_delivery_queue; DROP FUNCTION reject_final_delivery();").execute(&pool).await.unwrap();
        sqlx::query(
            "UPDATE runs SET coordinator_leased_until=now()-interval '1 second' WHERE id=$1",
        )
        .bind(runs[0])
        .execute(&pool)
        .await
        .unwrap();
        let result = finalize_ready_runs(router, Uuid::now_v7(), &config())
            .await
            .unwrap();
        assert_eq!(result.output, 1);
        assert_eq!(result.failures.failed_operation_count(), 0);
        let event_owner:Uuid=sqlx::query_scalar("SELECT tenant_id FROM outbox_events WHERE aggregate_id=$1 AND event_type='run.completed'").bind(runs[0]).fetch_one(&pool).await.unwrap();
        assert_eq!(event_owner, tenant);
        let scorecard_owner: Uuid =
            sqlx::query_scalar("SELECT tenant_id FROM run_scorecards WHERE run_id=$1")
                .bind(runs[0])
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(scorecard_owner, tenant);
        assert_eq!(
            finalize_ready_runs(router, Uuid::now_v7(), &config())
                .await
                .unwrap()
                .output,
            0
        );
        let events:i64=sqlx::query_scalar("SELECT count(*) FROM outbox_events WHERE aggregate_id=$1 AND event_type='run.completed'").bind(runs[0]).fetch_one(&pool).await.unwrap();
        assert_eq!(events, 1);
        let untouched:i64=sqlx::query_scalar("SELECT count(*) FROM runs WHERE id=ANY($1) AND status='running' AND coordinator_heartbeat_at IS NOT NULL").bind(&runs[1..]).fetch_one(&pool).await.unwrap();
        assert_eq!(untouched, 2);

        let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM runs")
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
