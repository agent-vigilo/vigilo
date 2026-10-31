//! Storage-backed coordinator settlement, partial coverage, and candidate isolation.

use serde_json::{
    Value,
    json,
};
use sqlx::{
    ConnectOptions,
    PgPool,
};

use super::*;
use crate::db::workflows::run_shard_summary::test_support::*;

fn context(pool: &PgPool) -> Context {
    Context::new(
        database::Config::new(
            pool.connect_options().to_url_lossy().to_string(),
            8,
            Duration::from_secs(10),
            database::CircuitBreakerConfig::default(),
            database::PlacementConfig::default_single_database(),
        ),
        None,
        crate::context::wasm::Config::default(),
        crate::context::output::OutputFormat::Json,
    )
}

fn config() -> CoordinatorRuntimeConfig {
    CoordinatorRuntimeConfig {
        run_creation: Default::default(),
        tick_seconds: 1,
        lease_seconds: 60,
        max_create_recovery_per_cycle: 8,
        max_dispatch_per_cycle: 8,
        max_finalize_per_cycle: 8,
        aggregation: run_shard_summary::AggregationConfig {
            max_pages: 256,
            time_budget: Duration::from_secs(10),
            parallelism: 4,
        },
        aggregation_database_parallelism: NonZeroUsize::new(4).unwrap(),
        run_chunk_dispatch_window_size: 64,
        chunk_lease_recovery_batch_size: 64,
        chunk_lease_max_recoveries: 3,
        outbox_batch_size: 64,
        outbox_publish_parallelism: 4,
        outbox_lease_seconds: 60,
        outbox_retry_delay_seconds: 1,
    }
}

async fn route(pool: &PgPool, run: Uuid) {
    sqlx::query("INSERT INTO shard_placements (run_id,run_shard,database_alias,status) VALUES ($1,4,'primary','active')")
        .bind(run).execute(pool).await.unwrap();
    sqlx::query(
        "INSERT INTO run_shard_dispatch_cursors (run_id,run_shard,status) VALUES ($1,4,'drained')",
    )
    .bind(run)
    .execute(pool)
    .await
    .unwrap();
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for coordinator settlement tests"]
async fn aggregation_seals_multiple_runs_in_one_pass_independently_of_finalization_limit(
    pool: PgPool,
) {
    let context = context(&pool);
    let router = context.dbr().await.unwrap();
    let mut config = config();
    config.max_finalize_per_cycle = 1;
    let mut runs = Vec::new();
    for _ in 0..2 {
        let (run, dataset) = fixture(&pool, 1).await;
        route(&pool, run).await;
        let chunk = ranged_chunk(&pool, run, dataset, 0, 1, "failed").await;
        let mut tx = pool.begin().await.unwrap();
        run_shard_summary::settle_chunk_with(&mut tx, run, 4, chunk)
            .await
            .unwrap();
        run_shard_summary::initialize_run_shard_summary_with(&mut tx, run, 4)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        runs.push(run);
    }

    let aggregation = aggregate_pending_summaries(router, Uuid::now_v7(), &config)
        .await
        .unwrap();
    assert_eq!(
        aggregation.output, 4,
        "both progress and canonical pages must finish for each run in one aggregation pass"
    );
    assert_eq!(aggregation.failures.failed_operation_count(), 0);
    for run in &runs {
        let summary = run_shard_summary::select_run_shard_summary(&pool, *run, 4)
            .await
            .unwrap()
            .unwrap();
        assert!(summary.sealed);
        assert_eq!(summary.settled_chunk_count, 1);
        assert_eq!(summary.missing_execution_count, 1);
    }
    for expected in [1, 1, 0] {
        assert_eq!(
            finalize_ready_runs(router, Uuid::now_v7(), &config)
                .await
                .unwrap()
                .output,
            expected
        );
    }
    let events: Vec<(Uuid, i64)> = sqlx::query_as("SELECT aggregate_id,count(*) FROM outbox_events WHERE aggregate_id=ANY($1) AND event_type='run.completed' GROUP BY aggregate_id ORDER BY aggregate_id")
        .bind(&runs).fetch_all(&pool).await.unwrap();
    assert_eq!(
        events,
        runs.into_iter().map(|run| (run, 1)).collect::<Vec<_>>()
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for coordinator settlement tests"]
async fn failed_evaluation_waits_for_live_work_then_finalizes_missing_results(pool: PgPool) {
    let context = context(&pool);
    let router = context.dbr().await.unwrap();
    // Exercise the real coordinator with and without configured gates.
    for gates in [
        json!([]),
        json!([{"id":"quality", "dimension":"quality",
        "min_mean_score":0.5,"min_coverage":1.0}]),
    ] {
        let (run, dataset) = fixture(&pool, 2).await;
        route(&pool, run).await;
        sqlx::query("UPDATE run_snapshots SET config_snapshot=$2 WHERE run_id=$1")
            .bind(run)
            .bind(profile_snapshot(gates.clone()))
            .execute(&pool)
            .await
            .unwrap();
        let done = ranged_chunk(&pool, run, dataset, 0, 1, "completed").await;
        let open = ranged_chunk(&pool, run, dataset, 1, 2, "pending").await;
        sqlx::query("UPDATE run_chunks SET status='leased', lease_token=gen_random_uuid(), leased_until=now()+interval '1 hour', recovery_count=3 WHERE id=$1")
            .bind(open).execute(&pool).await.unwrap();
        let execution = Uuid::now_v7();
        let attempt = Uuid::now_v7();
        seed_execution(&pool, run, 4, done, execution, attempt, "completed").await;
        seed_aggregate(&pool, run, 4, execution, attempt, "failed").await;
        let mut tx = pool.begin().await.unwrap();
        run_shard_summary::settle_chunk_with(&mut tx, run, 4, done)
            .await
            .unwrap();
        run_shard_summary::initialize_run_shard_summary_with(&mut tx, run, 4)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        for _ in 0..4 {
            aggregate_pending_summaries(router, Uuid::now_v7(), &config())
                .await
                .unwrap();
            assert_eq!(
                finalize_ready_runs(router, Uuid::now_v7(), &config())
                    .await
                    .unwrap()
                    .output,
                0
            );
        }
        let status: String = sqlx::query_scalar("SELECT status::text FROM runs WHERE id=$1")
            .bind(run)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "running");
        let summary = run_shard_summary::select_run_shard_summary(&pool, run, 4)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            summary.failed_execution_count, 1,
            "gates={gates}, summary={summary:?}"
        );
        assert!(!summary.sealed);
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM outbox_events WHERE aggregate_id=$1 AND event_type='run.completed'").bind(run).fetch_one(&pool).await.unwrap(),0);

        sqlx::query("UPDATE run_chunks SET leased_until=now()-interval '1 second' WHERE id=$1")
            .bind(open)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            run_dispatch::recover_expired_chunk_leases(&pool, 3, 64)
                .await
                .unwrap()
                .failed_chunks,
            1
        );
        let mut finalized = 0;
        for _ in 0..4 {
            aggregate_pending_summaries(router, Uuid::now_v7(), &config())
                .await
                .unwrap();
            finalized += finalize_ready_runs(router, Uuid::now_v7(), &config())
                .await
                .unwrap()
                .output;
        }
        assert_eq!(finalized, 1);
        let (status, gate, report): (String, String, Value) =
            sqlx::query_as("SELECT status::text,gate_status::text,summary FROM runs WHERE id=$1")
                .bind(run)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!((status.as_str(), gate.as_str()), ("completed", "fail"));
        assert_eq!(report["missing_execution_count"], 1);
        assert_eq!(report["terminal_execution_count"], 1);
        assert_eq!(report["coverage_complete"], false);
        assert_eq!(report["settled"], true);
        let summary = run_shard_summary::select_run_shard_summary(&pool, run, 4)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(summary.aggregate_count, 1);
        assert_eq!(
            summary.scorecard["entries"].as_array().unwrap().len(),
            gates.as_array().unwrap().len()
        );
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM outbox_events WHERE aggregate_id=$1 AND event_type='run.completed'").bind(run).fetch_one(&pool).await.unwrap(),1);
    }
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for coordinator settlement tests"]
async fn invalid_candidate_does_not_block_a_healthy_run(pool: PgPool) {
    let context = context(&pool);
    let router = context.dbr().await.unwrap();
    let (invalid, dataset) = fixture(&pool, 1).await;
    route(&pool, invalid).await;
    ranged_chunk(&pool, invalid, dataset, 0, 1, "failed").await;
    // A locally sealed report must still match the control policy.
    sqlx::query("UPDATE run_snapshots SET aggregation_policy_hash='wrong-policy' WHERE run_id=$1")
        .bind(invalid)
        .execute(&pool)
        .await
        .unwrap();
    run_shard_summary::refresh_run_shard_summary(&pool, invalid, 4)
        .await
        .unwrap();
    let (healthy, dataset) = fixture(&pool, 1).await;
    route(&pool, healthy).await;
    let done = ranged_chunk(&pool, healthy, dataset, 0, 1, "completed").await;
    let execution = Uuid::now_v7();
    let attempt = Uuid::now_v7();
    seed_execution(&pool, healthy, 4, done, execution, attempt, "completed").await;
    seed_aggregate(&pool, healthy, 4, execution, attempt, "passed").await;
    run_shard_summary::refresh_run_shard_summary(&pool, healthy, 4)
        .await
        .unwrap();
    let result = finalize_ready_runs(router, Uuid::now_v7(), &config())
        .await
        .unwrap();
    assert_eq!(result.output, 1);
    let rows: Vec<(Uuid, String, String)> =
        sqlx::query_as("SELECT id,status::text,gate_status::text FROM runs ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(rows.contains(&(invalid, "running".into(), "unknown".into())));
    assert!(rows.contains(&(healthy, "completed".into(), "pass".into())));
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for coordinator settlement tests"]
async fn finalization_summary_collection_does_not_advance_pending_receipts(pool: PgPool) {
    let context = context(&pool);
    let router = context.dbr().await.unwrap();
    let (run, dataset) = fixture(&pool, 1).await;
    route(&pool, run).await;
    let chunk = ranged_chunk(&pool, run, dataset, 0, 1, "failed").await;
    let mut tx = pool.begin().await.unwrap();
    run_shard_summary::settle_chunk_with(&mut tx, run, 4, chunk)
        .await
        .unwrap();
    run_shard_summary::initialize_run_shard_summary_with(&mut tx, run, 4)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let before: Value =
        sqlx::query_scalar("SELECT to_jsonb(s) FROM run_shard_summaries s WHERE run_id=$1")
            .bind(run)
            .fetch_one(&pool)
            .await
            .unwrap();

    let collection = collect_run_shard_summaries(router, Uuid::now_v7(), run)
        .await
        .unwrap();
    assert!(collection.output.complete);
    assert_eq!(collection.output.summaries.len(), 1);
    assert!(!collection.output.summaries[0].sealed);
    let after: Value =
        sqlx::query_scalar("SELECT to_jsonb(s) FROM run_shard_summaries s WHERE run_id=$1")
            .bind(run)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        after, before,
        "finalization reads must leave receipt aggregation to its own pass"
    );
    let pending: bool = sqlx::query_scalar(
        "SELECT NOT consumed FROM run_chunk_settlement_consumptions WHERE chunk_id=$1",
    )
    .bind(chunk)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(pending);

    for expected in [2, 0] {
        assert_eq!(
            aggregate_pending_summaries(router, Uuid::now_v7(), &config())
                .await
                .unwrap()
                .output,
            expected
        );
    }
    assert_eq!(
        finalize_ready_runs(router, Uuid::now_v7(), &config())
            .await
            .unwrap()
            .output,
        1
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for coordinator settlement tests"]
async fn finalization_batches_only_routed_summaries_and_defers_missing_or_moving_shards(
    pool: PgPool,
) {
    let context = context(&pool);
    let router = context.dbr().await.unwrap();
    router.control_pool.set(pool.clone()).unwrap();
    let (run, dataset) = fixture(&pool, 1).await;
    let dataset_id: Uuid = sqlx::query_scalar("SELECT dataset_id FROM runs WHERE id=$1")
        .bind(run)
        .fetch_one(&pool)
        .await
        .unwrap();
    for shard in 5..=7 {
        seed_run_snapshot(&pool, run, shard, dataset_id, dataset, 1).await;
    }
    sqlx::query("INSERT INTO shard_placements(run_id,run_shard,database_alias,status) SELECT $1,shard,'primary','active' FROM generate_series(4,6) shard")
        .bind(run).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO run_shard_summaries(run_id,run_shard,expected_execution_count,status) SELECT $1,shard,1,'running' FROM generate_series(4,7) shard WHERE shard<>6")
        .bind(run).execute(&pool).await.unwrap();

    let (result, queries) =
        record_queries(collect_run_shard_summaries(router, Uuid::now_v7(), run)).await;
    let collection = result.unwrap();
    assert!(!collection.output.complete);
    assert_eq!(collection.failures.failed_operation_count(), 0);
    assert_eq!(
        collection
            .output
            .summaries
            .iter()
            .map(|s| s.run_shard)
            .collect::<Vec<_>>(),
        vec![4, 5]
    );
    let summary_reads = queries
        .iter()
        .filter(|sql| sql.contains("FROM run_shard_summaries"))
        .count();
    assert_eq!(
        summary_reads, 1,
        "same-database finalization summary reads: {summary_reads}; queries: {queries:?}"
    );

    sqlx::query("INSERT INTO database_placements(alias,database_url_env,role,status) VALUES ('move-target','VIGILO_TEST_MOVE_TARGET_DATABASE_URL','shard','active')")
        .execute(&pool).await.unwrap();
    sqlx::query("UPDATE shard_placements SET status='moving',move_target_database_alias='move-target' WHERE run_id=$1 AND run_shard=5")
        .bind(run)
        .execute(&pool)
        .await
        .unwrap();
    let moving = collect_run_shard_summaries(router, Uuid::now_v7(), run)
        .await
        .unwrap();
    assert!(!moving.output.complete);
    assert!(moving.output.summaries.is_empty());
    assert_eq!(moving.failures.failed_operation_count(), 1);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for coordinator settlement tests"]
async fn sealed_summary_finalization_reads_without_local_write_admission(pool: PgPool) {
    let context = context(&pool);
    let router = context.dbr().await.unwrap();
    let (run, dataset) = fixture(&pool, 1).await;
    route(&pool, run).await;
    ranged_chunk(&pool, run, dataset, 0, 1, "failed").await;
    let expected = run_shard_summary::refresh_run_shard_summary(&pool, run, 4)
        .await
        .unwrap()
        .unwrap();
    assert!(expected.is_terminal());
    sqlx::query("UPDATE local_shard_admissions SET state='closed',write_epoch=write_epoch+1 WHERE run_id=$1 AND run_shard=4")
        .bind(run).execute(&pool).await.unwrap();
    let before: Value = sqlx::query_scalar(
        "SELECT jsonb_build_object('summary',to_jsonb(s),'consumptions',(SELECT jsonb_agg(to_jsonb(p)) FROM run_chunk_settlement_consumptions p WHERE p.run_id=s.run_id AND p.run_shard=s.run_shard)) FROM run_shard_summaries s WHERE run_id=$1 AND run_shard=4",
    ).bind(run).fetch_one(&pool).await.unwrap();

    let collection = collect_run_shard_summaries(router, Uuid::now_v7(), run)
        .await
        .unwrap();
    assert!(collection.output.complete);
    assert_eq!(collection.failures.failed_operation_count(), 0);
    assert_eq!(
        serde_json::to_value(collection.output.summaries).unwrap(),
        json!([expected])
    );
    assert_eq!(
        finalize_ready_runs(router, Uuid::now_v7(), &config())
            .await
            .unwrap()
            .output,
        1
    );
    let (status, gate): (String, String) =
        sqlx::query_as("SELECT status::text,gate_status::text FROM runs WHERE id=$1")
            .bind(run)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!((status.as_str(), gate.as_str()), ("completed", "fail"));
    let after: Value = sqlx::query_scalar(
        "SELECT jsonb_build_object('summary',to_jsonb(s),'consumptions',(SELECT jsonb_agg(to_jsonb(p)) FROM run_chunk_settlement_consumptions p WHERE p.run_id=s.run_id AND p.run_shard=s.run_shard)) FROM run_shard_summaries s WHERE run_id=$1 AND run_shard=4",
    ).bind(run).fetch_one(&pool).await.unwrap();
    assert_eq!(after, before);
}
