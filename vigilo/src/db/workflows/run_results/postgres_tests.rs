//! PostgreSQL contracts for batched routed results and stable score arithmetic.

use serde_json::{
    Value,
    json,
};
use sqlx::{
    ConnectOptions,
    PgPool,
};
use tokio::sync::OnceCell;

use super::*;
use crate::{
    context::database::{
        CircuitBreakerConfig,
        DatabaseCircuitBreakers,
        DatabaseRouter,
        PlacementConfig,
        new_shard_placement_cache,
    },
    db::workflows::run_shard_summary::test_support::*,
};

fn router(pool: &PgPool) -> DatabaseRouter {
    let router = DatabaseRouter {
        uri: pool.connect_options().to_url_lossy().to_string(),
        max_connections_per_pool: 5,
        acquire_timeout: std::time::Duration::from_secs(10),
        placement_config: PlacementConfig::default_single_database(),
        circuit_breakers: DatabaseCircuitBreakers::new(CircuitBreakerConfig::default()),
        operation_timeout_config: None,
        control_pool: OnceCell::new(),
        placement_pools: OnceCell::new(),
        dynamic_placement_pools: moka::future::Cache::builder().max_capacity(1_000).build(),
        shard_placement_cache: new_shard_placement_cache(),
    };
    assert!(router.control_pool.set(pool.clone()).is_ok());
    router
}

fn result_contract(result: &RunResultsSummary) -> Value {
    json!({
        "executions": result.execution_count, "aggregates": result.aggregate_count,
        "passed": result.passed_execution_count, "failed": result.failed_execution_count,
        "errored": result.error_execution_count, "skipped": result.skipped_execution_count,
        "missing_aggregates": result.missing_aggregate_count, "evaluator_results": result.evaluator_result_count,
        "blocking_failures": result.blocking_failure_count,
        "average": result.average_score, "min": result.min_score, "max": result.max_score,
        "planned_chunks": result.planned_chunk_count, "settled_chunks": result.settled_chunk_count,
        "sealed_shards": result.sealed_shard_count,
        "missing_executions": result.missing_execution_count, "unresolved_executions": result.unresolved_execution_count,
    })
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn results_batch_preserves_partial_unscored_and_unavailable_route_behavior(pool: PgPool) {
    let run = Uuid::now_v7();
    let dataset_id = Uuid::now_v7();
    let dataset = Uuid::now_v7();
    seed_control_run(&pool, run, dataset_id, dataset).await;
    for shard in 0..128 {
        seed_run_snapshot(&pool, run, shard, dataset_id, dataset, 1).await;
    }
    sqlx::query("INSERT INTO shard_placements(run_id,run_shard,database_alias,status) SELECT $1,shard,'primary','active' FROM generate_series(0,126) shard")
        .bind(run).execute(&pool).await.unwrap();
    // Shard17 is still missing; shard127 has a summary but has no route.
    sqlx::query("INSERT INTO run_shard_summaries(run_id,run_shard,expected_execution_count,execution_count,terminal_execution_count,aggregate_count,passed_execution_count,evaluator_result_count,planned_chunk_count,settled_chunk_count,status) SELECT $1,shard,1,1,1,1,1,2,2,1,'running' FROM generate_series(0,127) shard WHERE shard<>17")
        .bind(run).execute(&pool).await.unwrap();
    let router = router(&pool);
    let (result, queries) = record_queries(select_run_results_summary(&router, run)).await;
    assert_eq!(
        result_contract(&result.unwrap()),
        json!({
            "executions":126,"aggregates":126,"passed":126,"failed":0,"errored":0,"skipped":0,
            "missing_aggregates":0,"evaluator_results":252,"blocking_failures":0,
            "average":null,"min":null,"max":null,"planned_chunks":252,"settled_chunks":126,
            "sealed_shards":0,"missing_executions":0,"unresolved_executions":0,
        })
    );
    sqlx::query("DELETE FROM run_shard_summaries WHERE run_id=$1")
        .bind(run)
        .execute(&pool)
        .await
        .unwrap();
    for id in [run, Uuid::now_v7()] {
        let empty = select_run_results_summary(&router, id).await.unwrap();
        assert_eq!(
            result_contract(&empty),
            result_contract(&RunResultsSummary::default())
        );
    }
    let missing_env = format!("VIGILO_TEST_RESULTS_MISSING_{}", Uuid::now_v7().simple());
    sqlx::query("INSERT INTO database_placements(alias,database_url_env,role,status) VALUES ('results_unavailable',$1,'shard','active')")
        .bind(&missing_env).execute(&pool).await.unwrap();
    sqlx::query("UPDATE shard_placements SET database_alias='results_unavailable' WHERE run_id=$1 AND run_shard=126")
        .bind(run).execute(&pool).await.unwrap();
    let error = select_run_results_summary(&router, run).await.unwrap_err();
    assert!(error.to_string().contains(&missing_env));

    let reads = queries
        .iter()
        .filter(|sql| sql.contains("FROM run_shard_summaries"))
        .count();
    assert_eq!(reads, 1, "same-database results summary reads: {reads}");
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn results_batch_preserves_ascending_shard_score_addition_across_aliases(pool: PgPool) {
    let run = Uuid::now_v7();
    let dataset_id = Uuid::now_v7();
    let dataset = Uuid::now_v7();
    seed_control_run(&pool, run, dataset_id, dataset).await;
    sqlx::query("INSERT INTO database_placements(alias,database_url_env,role,status) VALUES ('a_results','DATABASE_URL','shard','active')")
        .execute(&pool).await.unwrap();
    for (shard, score, alias) in [
        (0, 1.0, "primary"),
        (1, f64::EPSILON / 2.0, "a_results"),
        (2, f64::EPSILON / 2.0, "a_results"),
    ] {
        seed_run_snapshot(&pool, run, shard, dataset_id, dataset, 1).await;
        sqlx::query("INSERT INTO shard_placements(run_id,run_shard,database_alias,status) VALUES ($1,$2,$3,'active')")
            .bind(run).bind(shard).bind(alias).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO run_shard_summaries(run_id,run_shard,expected_execution_count,execution_count,terminal_execution_count,aggregate_count,passed_execution_count,score_count,score_sum,min_score,max_score,planned_chunk_count,settled_chunk_count,status) VALUES ($1,$2,1,1,1,1,1,1,$3,$3,$3,1,1,'running')")
            .bind(run).bind(shard).bind(score).execute(&pool).await.unwrap();
    }
    let router = router(&pool);
    let result = select_run_results_summary(&router, run).await.unwrap();
    assert_eq!(
        result.average_score.unwrap().to_bits(),
        (1.0f64 / 3.0).to_bits()
    );
    assert_eq!(result.min_score, Some(f64::EPSILON / 2.0));
    assert_eq!(result.max_score, Some(1.0));
    assert_eq!(result.execution_count, 3);
    assert_eq!(result.aggregate_count, 3);
    assert_eq!(result.passed_execution_count, 3);
}
