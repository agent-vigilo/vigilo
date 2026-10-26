// PostgreSQL-backed workflow scenarios and fixtures.

use tokio::sync::OnceCell;

use super::*;
use crate::context::database::{
    CircuitBreakerConfig,
    DatabaseCircuitBreakers,
    DatabaseRouter,
    PlacementConfig,
    new_shard_placement_cache,
};

fn database_router_with_control_pool(pool: sqlx::PgPool) -> DatabaseRouter {
    let database_router = DatabaseRouter {
        uri: "postgres://injected-control-pool".to_string(),
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
    assert!(database_router.control_pool.set(pool).is_ok());
    database_router
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx status tests"]
async fn creation_failure_status_does_not_resolve_unseeded_routes(pool: sqlx::PgPool) {
    let run_id = Uuid::now_v7();
    let dataset_id = Uuid::now_v7();
    let dataset_version_id = Uuid::now_v7();
    sqlx::query(
            "INSERT INTO dataset_versions (dataset_version_id, dataset_id, dataset_version) VALUES ($1, $2, 'test')",
        )
        .bind(dataset_version_id)
        .bind(dataset_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        r#"
            INSERT INTO runs (
                id, run_key, dataset_id, dataset_version_id, dataset_version,
                evaluation_profile_id, evaluation_profile_version,
                profile_version_id, profile_hash, aggregation_policy_id,
                aggregation_policy_version, aggregation_policy_hash,
                agent_provider, agent_name, prompt_config_id,
                prompt_config_version, status, error_message, completed_at
            )
            VALUES (
                $1, $2, $3, $4, 'test', 'profile', '1.0.0', 'profile-version',
                'profile-hash', 'aggregation', '1.0.0', 'aggregation-hash',
                'example', 'agent', 'prompt', '1.0.0', 'failed'::run_status,
                'immutable seed mismatch', now()
            )
            "#,
    )
    .bind(run_id)
    .bind(format!("run-{run_id}"))
    .bind(dataset_id)
    .bind(dataset_version_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
            INSERT INTO database_placements (alias, database_url_env, role, status)
            VALUES ('unseeded', 'VIGILO_TEST_UNSEEDED_DATABASE_URL', 'shard', 'active')
            "#,
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
            INSERT INTO shard_placements (run_id, run_shard, database_alias, status)
            VALUES ($1, 0, 'unseeded', 'active')
            "#,
    )
    .bind(run_id)
    .execute(&pool)
    .await
    .unwrap();
    let database_router = database_router_with_control_pool(pool);

    let status = select_run_status(&database_router, run_id)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(status.execution_route_count, 0);
    assert_eq!(
        status
            .creation_progress
            .as_ref()
            .map(|progress| progress.failed_placement_count),
        Some(0)
    );
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx status tests"]
async fn status_batches_same_database_shards_and_preserves_partial_progress(pool: sqlx::PgPool) {
    use crate::db::workflows::run_shard_summary::test_support::*;

    let run = Uuid::now_v7();
    let dataset_id = Uuid::now_v7();
    let dataset = Uuid::now_v7();
    seed_control_run(&pool, run, dataset_id, dataset).await;
    for shard in 0..128 {
        seed_run_snapshot(&pool, run, shard, dataset_id, dataset, 1).await;
    }
    sqlx::query("INSERT INTO shard_placements(run_id,run_shard,database_alias,status) SELECT $1,shard,'primary','active' FROM generate_series(0,126) shard")
        .bind(run).execute(&pool).await.unwrap();
    // One routed summary is still missing; shard127 has data but no route.
    sqlx::query("INSERT INTO run_shard_summaries(run_id,run_shard,expected_execution_count,execution_count,terminal_execution_count,passed_execution_count,planned_chunk_count,settled_chunk_count,status) SELECT $1,shard,1,1,1,1,2,1,'running' FROM generate_series(0,127) shard WHERE shard<>17")
        .bind(run).execute(&pool).await.unwrap();
    let router = database_router_with_control_pool(pool.clone());

    let (result, queries) = record_queries(select_run_status(&router, run)).await;
    let projection = result.unwrap().unwrap();
    assert_eq!(projection.execution_route_count, 127);
    assert_eq!(projection.shard_summary_count, 126);
    assert!(!projection.live_progress_complete());
    assert_eq!(projection.progress_source(), "execution_shards");
    assert_eq!(projection.live_progress.expected_execution_count, 126);
    assert_eq!(projection.live_progress.passed_execution_count, 126);
    assert_eq!(projection.live_progress.planned_chunk_count, 252);
    assert_eq!(projection.live_progress.settled_chunk_count, 126);
    let summary_reads = queries
        .iter()
        .filter(|sql| sql.contains("FROM run_shard_summaries"))
        .count();
    assert_eq!(
        summary_reads, 1,
        "same-database status summary reads: {summary_reads}"
    );

    sqlx::query("DELETE FROM run_shard_summaries WHERE run_id=$1")
        .bind(run)
        .execute(&pool)
        .await
        .unwrap();
    let empty = select_run_status(&router, run).await.unwrap().unwrap();
    assert_eq!(empty.progress_source(), "control_run");
    assert_eq!(empty.execution_route_count, 127);
    assert_eq!(empty.shard_summary_count, 0);
    assert_eq!(empty.live_progress.expected_execution_count, 2);
}
