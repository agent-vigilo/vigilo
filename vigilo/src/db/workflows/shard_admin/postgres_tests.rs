// PostgreSQL-backed workflow scenarios and fixtures.

use sqlx::{
    PgPool,
    Postgres,
    migrate::MigrateDatabase,
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
    models::database_placement::{
        DEFAULT_DATABASE_ALIAS,
        DEFAULT_DATABASE_URL_ENV,
    },
};

fn database_router_with_control_pool(pool: PgPool, uri: String) -> DatabaseRouter {
    let database_router = DatabaseRouter {
        uri,
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

#[path = "postgres_tests/placement.rs"]
mod placement;
#[path = "postgres_tests/rebalance.rs"]
mod rebalance;
#[path = "postgres_tests/shard_move.rs"]
mod shard_move;

async fn wait_for_waiting_advisory_lock(pool: &PgPool, operation: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let waiting = sqlx::query_scalar::<_, bool>(
                r#"
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_locks
                        WHERE locktype = 'advisory'
                          AND database = (
                              SELECT oid
                              FROM pg_database
                              WHERE datname = current_database()
                          )
                          AND NOT granted
                    )
                    "#,
            )
            .fetch_one(pool)
            .await
            .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{operation} did not wait behind source admission"));
}

async fn database_router_with_isolated_control_pool(pool: PgPool) -> DatabaseRouter {
    let database_url = isolated_database_url(&pool).await;
    database_router_with_control_pool(pool, database_url)
}

async fn isolated_database_url(pool: &PgPool) -> String {
    let database_name = sqlx::query_scalar::<_, String>("SELECT current_database()")
        .fetch_one(pool)
        .await
        .unwrap();
    let mut database_url =
        url::Url::parse(&std::env::var(DEFAULT_DATABASE_URL_ENV).unwrap()).unwrap();
    database_url.set_path(&database_name);
    database_url.to_string()
}

async fn create_migrated_target_database() -> (String, String) {
    let database_name = format!("vigilo_activation_{}", Uuid::now_v7().simple());
    let mut database_url =
        url::Url::parse(&std::env::var(DEFAULT_DATABASE_URL_ENV).unwrap()).unwrap();
    database_url.set_path(&database_name);
    let database_url = database_url.to_string();

    Postgres::create_database(&database_url).await.unwrap();
    let pool = PgPool::connect(&database_url).await.unwrap();
    sqlx::migrate!("../migrations").run(&pool).await.unwrap();
    pool.close().await;

    (database_name, database_url)
}

async fn drop_target_database(database_url: &str) {
    Postgres::drop_database(database_url).await.unwrap();
}

async fn seed_rebalance_operation(pool: &PgPool, item_count: usize) -> (Uuid, Vec<(Uuid, i16)>) {
    sqlx::query(
        r#"
            INSERT INTO database_placements (
                alias,
                database_url_env,
                role,
                status
            )
            VALUES ('shard_001', 'DATABASE_URL', 'shard', 'active')
            "#,
    )
    .execute(pool)
    .await
    .unwrap();

    let operation_id = Uuid::now_v7();
    sqlx::query(
        r#"
            INSERT INTO shard_rebalance_operations (
                id,
                strategy,
                source_database_alias,
                target_database_alias,
                planned_item_count
            )
            VALUES ($1::uuid, 'drain-source', 'primary', 'shard_001', $2)
            "#,
    )
    .bind(operation_id)
    .bind(item_count as i32)
    .execute(pool)
    .await
    .unwrap();

    let mut items = Vec::with_capacity(item_count);
    for sequence_no in 0..item_count {
        let run_id = Uuid::now_v7();
        let run_shard = sequence_no as i16;
        sqlx::query(
            r#"
                INSERT INTO shard_rebalance_items (
                    operation_id,
                    sequence_no,
                    run_id,
                    run_shard,
                    source_database_alias,
                    target_database_alias,
                    planned_route_version
                )
                VALUES ($1::uuid, $2, $3::uuid, $4, 'primary', 'shard_001', 1)
                "#,
        )
        .bind(operation_id)
        .bind(sequence_no as i32)
        .bind(run_id)
        .bind(run_shard)
        .execute(pool)
        .await
        .unwrap();
        items.push((run_id, run_shard));
    }

    (operation_id, items)
}

async fn prerequisite_fingerprints(pool: &PgPool, run_id: Uuid) -> Vec<(i64, String)> {
    let mut fingerprints = Vec::new();

    for table in PREREQUISITE_TABLES {
        let fingerprint = prerequisite_table_fingerprint(pool, table, run_id, 0)
            .await
            .unwrap();
        fingerprints.push((fingerprint.row_count, fingerprint.checksum));
    }

    fingerprints
}

async fn seed_run(pool: &PgPool, run_id: Uuid, dataset_id: Uuid, dataset_version_id: Uuid) {
    sqlx::query(
        r#"
            INSERT INTO dataset_versions (dataset_version_id, dataset_id, dataset_version)
            VALUES ($1::uuid, $2::uuid, 'dataset')
            "#,
    )
    .bind(dataset_version_id)
    .bind(dataset_id)
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        r#"
            INSERT INTO runs (
                id,
                run_key,
                dataset_id,
                dataset_version_id,
                dataset_version,
                evaluation_profile_id,
                evaluation_profile_version,
                profile_version_id,
                profile_hash,
                aggregation_policy_id,
                aggregation_policy_version,
                aggregation_policy_hash,
                agent_provider,
                agent_name,
                prompt_config_id,
                prompt_config_version,
                status,
                expected_execution_count
            )
            VALUES (
                $1::uuid,
                $2,
                $3::uuid,
                $4::uuid,
                'dataset',
                'profile',
                '1.0.0',
                'profile-version',
                'profile-hash',
                'aggregation',
                '1.0.0',
                'aggregation-hash',
                'example',
                'agent',
                'prompt',
                '1.0.0',
                'running'::run_status,
                1
            )
            "#,
    )
    .bind(run_id)
    .bind(format!("run-{run_id}"))
    .bind(dataset_id)
    .bind(dataset_version_id)
    .execute(pool)
    .await
    .unwrap();
}

async fn seed_run_snapshot(
    pool: &PgPool,
    run_id: Uuid,
    run_shard: i16,
    dataset_id: Uuid,
    dataset_version_id: Uuid,
) {
    sqlx::query(
        r#"
            INSERT INTO run_snapshots (
                run_id,
                run_shard,
                run_key,
                dataset_id,
                dataset_version_id,
                dataset_version,
                evaluation_profile_id,
                evaluation_profile_version,
                profile_version_id,
                profile_hash,
                aggregation_policy_id,
                aggregation_policy_version,
                aggregation_policy_hash,
                agent_provider,
                agent_name,
                prompt_config_id,
                prompt_config_version,
                expected_execution_count
            )
            VALUES (
                $1::uuid,
                $2,
                'run-key',
                $3::uuid,
                $4::uuid,
                'dataset',
                'profile',
                '1.0.0',
                'profile-version',
                'profile-hash',
                'aggregation',
                '1.0.0',
                'aggregation-hash',
                'example',
                'agent',
                'prompt',
                '1.0.0',
                1
            )
            "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .bind(dataset_id)
    .bind(dataset_version_id)
    .execute(pool)
    .await
    .unwrap();
}
