//! Restricted worker batch allocation, evidence, transitions and pooled reuse.
use futures_util::FutureExt;

use super::{
    super::{
        heartbeat_running_attempts_for_chunk,
        summarize_chunk_execution_state,
    },
    *,
};
use crate::db::workflows::run_shard_summary::test_support::record_queries;

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn worker_batches_preserve_verified_owner_and_atomic_results(pool: PgPool) {
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
    let mut seed = seed_running_attempt(&owned, 120, 120).await;
    sqlx::query("UPDATE executions SET status='retry_scheduled',retry_after=now()-interval '1 second' WHERE id=$1").bind(seed.execution_id).execute(&owned).await.unwrap();
    sqlx::query("UPDATE execution_attempts SET leased_until=now()-interval '1 second' WHERE id=$1")
        .bind(seed.attempt_id)
        .execute(&owned)
        .await
        .unwrap();
    owned.close().await;
    let role = format!("worker_batch_{}", Uuid::now_v7().simple());
    sqlx::raw_sql(&format!(
        "CREATE ROLE {role} LOGIN PASSWORD 'fixture'; GRANT USAGE ON SCHEMA public TO {role};"
    ))
    .execute(&pool)
    .await
    .unwrap();
    for table in [
        "run_snapshots",
        "local_shard_admissions",
        "run_chunks",
        "executions",
        "execution_attempts",
        "evaluator_results",
        "evaluator_diagnostics",
        "execution_aggregates",
        "shard_move_captures",
    ] {
        sqlx::raw_sql(&format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY; CREATE POLICY work ON {table} TO {role} USING(tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND run_id=NULLIF(current_setting('vigilo.run_id',true),'')::uuid AND current_setting('vigilo.actor_id',true)='' AND COALESCE(current_setting('vigilo.run_batch',true),'')=''); GRANT SELECT,INSERT,UPDATE ON {table} TO {role};")).execute(&pool).await.unwrap();
    }
    let restricted=sqlx::postgres::PgPoolOptions::new().max_connections(1)
        .after_connect(|c,_|Box::pin(async move {sqlx::query("SELECT set_config('vigilo.tenant_id','',false),set_config('vigilo.run_id','',false),set_config('vigilo.actor_id','',false),set_config('vigilo.run_batch','',false)").execute(c).await?;Ok(())}))
        .connect_with(pool.connect_options().as_ref().clone().username(&role).password("fixture")).await.unwrap();
    let result = std::panic::AssertUnwindSafe(async {
        let mut profile = run_profile(vec![case_group(
            "default",
            "classification",
            vec!["sentiment"],
            "test/evaluator:1.0.0",
            AggregationMethod::WeightedMean,
        )]);
        profile.defaults.max_attempts = 2;
        let mut case = worker_case(None);
        case.case_id = seed.case_id;
        let plan = evaluation_plan_for_case(&profile, &case).unwrap();
        let lease = AttemptLeaseContext {
            worker_id: seed.worker_id,
            worker_host: None,
            queue_message_id: Uuid::now_v7(),
            broker_message_id: None,
            lease_seconds: 120,
        };
        let (allocated, calls) = record_queries(allocate_execution_attempts_for_cases(
            &restricted,
            &seed.chunk,
            &lease,
            &profile,
            &[case],
            &[plan],
        ))
        .await;
        let allocated = allocated.unwrap();
        assert_eq!(allocated.len(), 1);
        assert!(allocated[0].should_process);
        assert_eq!(
            calls
                .iter()
                .filter(|s| s.contains("SET LOCAL vigilo.tenant_id"))
                .count(),
            1
        );
        seed.execution_id = allocated[0].execution_id;
        seed.attempt_id = allocated[0].attempt_id.unwrap();
        assert_eq!(
            heartbeat_running_attempts_for_chunk(
                &restricted,
                tenant,
                seed.run_id,
                seed.run_shard,
                seed.chunk_id,
                seed.worker_id,
                120
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            heartbeat_running_attempts_for_chunk(
                &restricted,
                Uuid::now_v7(),
                seed.run_id,
                seed.run_shard,
                seed.chunk_id,
                seed.worker_id,
                120
            )
            .await
            .unwrap(),
            0
        );
        let numeric_row = evaluator_results::EvaluatorResultInsertRow {
            run_id: seed.run_id,
            run_shard: seed.run_shard,
            execution_id: seed.execution_id,
            attempt_id: seed.attempt_id,
            binding_id: "quality_score".to_string(),
            evaluator_id: Uuid::now_v7(),
            evaluator_version: "1.0.0".to_string(),
            evaluator_profile_id: "profile".to_string(),
            evaluator_profile_version: "1.0.0".to_string(),
            evaluator_interface_version: Some("1.0.0".to_string()),
            evaluator_runtime_version: Some("test".to_string()),
            dimension: "quality".to_string(),
            outcome: "completed".to_string(),
            judgment: Some("failed".to_string()),
            blocking: true,
            measurement_kind: Some("numeric".to_string()),
            raw_boolean: None,
            raw_numeric: Some(0.2),
            raw_ordinal: None,
            raw_unit: Some("ratio".to_string()),
            normalized_score: Some(0.2),
            normalization_policy_hash: "policy-hash".to_string(),
            pass_threshold: 0.8,
            weight: 1.0,
            error_code: None,
            error_message: None,
            abstention_category: None,
            abstention_reason: None,
            raw_evaluator_output: json!({"outcome": "completed"}),
            diagnostics: vec![evaluator_results::EvaluatorDiagnosticInsertRow {
                diagnostic_index: 0,
                severity: "medium".to_string(),
                category: "style".to_string(),
                reason: Some("too terse".to_string()),
                evidence: json!({"span": "answer"}),
                tags: vec!["quality".to_string()],
            }],
        };
        let completed = CompletedExecutionPersistence {
            execution_id: seed.execution_id,
            attempt_id: seed.attempt_id,
            attempt_no: allocated[0].attempt_no,
            result_rows: vec![numeric_row],
            overall_status: "passed".into(),
            aggregate_score: Some(0.2),
            evaluator_result_count: 1,
            dimension_scores: json!({}),
            blocking_failures: json!([]),
            summary: json!({}),
        };
        let mut wrong = seed.chunk.clone();
        wrong.tenant_id = Uuid::now_v7();
        assert!(
            persist_completed_execution_results_batch(
                &restricted,
                &wrong,
                seed.worker_id,
                std::slice::from_ref(&completed)
            )
            .await
            .is_err()
        );
        sqlx::raw_sql("CREATE FUNCTION worker_batch_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected batch failure'; END $$; CREATE TRIGGER worker_batch_failure BEFORE INSERT ON execution_aggregates FOR EACH ROW EXECUTE FUNCTION worker_batch_failure();").execute(&pool).await.unwrap();
        let error=persist_completed_execution_results_batch(&restricted,&seed.chunk,seed.worker_id,std::slice::from_ref(&completed)).await.unwrap_err();
        assert!(error.to_string().contains("injected batch failure"));
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM evaluator_results WHERE attempt_id=$1").bind(seed.attempt_id).fetch_one(&pool).await.unwrap(),0);
        sqlx::raw_sql("DROP TRIGGER worker_batch_failure ON execution_aggregates; DROP FUNCTION worker_batch_failure();").execute(&pool).await.unwrap();
        persist_completed_execution_results_batch(
            &restricted,
            &seed.chunk,
            seed.worker_id,
            &[completed],
        )
        .await
        .unwrap();
        let mut transition = terminal(&seed);
        transition.attempt_no = allocated[0].attempt_no;
        transition.completed = true;
        transition.error_message = None;
        assert!(
            finalize_execution_terminal_transitions(
                &restricted,
                Uuid::now_v7(),
                seed.run_id,
                seed.run_shard,
                seed.worker_id,
                2,
                std::slice::from_ref(&transition)
            )
            .await
            .is_err()
        );
        finalize_execution_terminal_transitions(
            &restricted,
            tenant,
            seed.run_id,
            seed.run_shard,
            seed.worker_id,
            2,
            &[transition],
        )
        .await
        .unwrap();
        assert_eq!(
            summarize_chunk_execution_state(
                &restricted,
                tenant,
                seed.run_id,
                seed.run_shard,
                &[seed.case_id]
            )
            .await
            .unwrap()
            .open_execution_count,
            0
        );
        for table in [
            "executions",
            "execution_attempts",
            "evaluator_results",
            "evaluator_diagnostics",
            "execution_aggregates",
        ] {
            assert!(
                sqlx::query_scalar::<_, i64>(&format!(
                    "SELECT count(*) FROM {table} WHERE tenant_id=$1 AND run_id=$2"
                ))
                .bind(tenant)
                .bind(seed.run_id)
                .fetch_one(&pool)
                .await
                .unwrap()
                    > 0,
                "{table}"
            );
            assert_eq!(
                sqlx::query_scalar::<_, i64>(&format!("SELECT count(*) FROM {table}"))
                    .fetch_one(&restricted)
                    .await
                    .unwrap(),
                0
            );
        }
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
