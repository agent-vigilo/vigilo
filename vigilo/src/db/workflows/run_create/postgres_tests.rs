// PostgreSQL-backed workflow scenarios and fixtures.

use sqlx::PgPool;

use super::{
    tests::chunk,
    *,
};

async fn insert_minimal_run(pool: &PgPool, run_id: Uuid) {
    let dataset_id = Uuid::now_v7();
    let dataset_version_id = Uuid::now_v7();

    sqlx::query(
        r#"
            INSERT INTO dataset_versions (dataset_version_id, dataset_id, dataset_version)
            VALUES ($1::uuid, $2::uuid, 'test')
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
                expected_execution_count
            )
            VALUES (
                $1::uuid,
                $2,
                $3::uuid,
                $4::uuid,
                'test',
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
                3
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

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn bulk_insert_run_shard_dispatch_cursors_inserts_distinct_open_shards(pool: PgPool) {
    let run_id = Uuid::now_v7();
    insert_minimal_run(&pool, run_id).await;

    let chunks = vec![chunk(0, 0), chunk(1, 1), chunk(1, 2)];
    let mut tx = pool.begin().await.unwrap();
    bulk_insert_run_shard_dispatch_cursors(&mut tx, run_id, &chunks)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let rows = sqlx::query_as::<_, (i16, String)>(
        r#"
            SELECT run_shard, status
            FROM run_shard_dispatch_cursors
            WHERE run_id = $1::uuid
            ORDER BY run_shard
            "#,
    )
    .bind(run_id)
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(rows, vec![(0, "open".to_string()), (1, "open".to_string())]);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn bulk_insert_run_chunks_is_idempotent(pool: PgPool) {
    let run_id = Uuid::now_v7();
    insert_minimal_run(&pool, run_id).await;
    let dataset_version_id =
        sqlx::query_scalar::<_, Uuid>("SELECT dataset_version_id FROM runs WHERE id = $1::uuid")
            .bind(run_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let chunks = vec![chunk(0, 0)];

    for _ in 0..2 {
        let mut tx = pool.begin().await.unwrap();
        bulk_insert_run_chunks(&mut tx, run_id, dataset_version_id, &chunks)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::bigint FROM run_chunks WHERE run_id = $1::uuid",
    )
    .bind(run_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 1);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn bulk_insert_shard_placements_inserts_distinct_active_shards(pool: PgPool) {
    let run_id = Uuid::now_v7();
    insert_minimal_run(&pool, run_id).await;

    let assignments = vec![
        RunShardPlacementAssignment {
            run_shard: 0,
            database_alias: "primary".to_string(),
        },
        RunShardPlacementAssignment {
            run_shard: 1,
            database_alias: "primary".to_string(),
        },
    ];
    let mut tx = pool.begin().await.unwrap();
    bulk_insert_shard_placements(&mut tx, run_id, &assignments)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let rows = sqlx::query_as::<_, (i16, String, String)>(
        r#"
            SELECT run_shard, database_alias, status
            FROM shard_placements
            WHERE run_id = $1::uuid
            ORDER BY run_shard
            "#,
    )
    .bind(run_id)
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(
        rows,
        vec![
            (0, "primary".to_string(), "active".to_string()),
            (1, "primary".to_string(), "active".to_string())
        ]
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn bulk_insert_shard_placements_rejects_control_only_alias(pool: PgPool) {
    let run_id = Uuid::now_v7();
    insert_minimal_run(&pool, run_id).await;

    sqlx::query(
        r#"
            UPDATE database_placements
            SET role = 'control'
            WHERE alias = 'primary'
            "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    let assignments = vec![RunShardPlacementAssignment {
        run_shard: 0,
        database_alias: "primary".to_string(),
    }];
    let mut tx = pool.begin().await.unwrap();
    let error = bulk_insert_shard_placements(&mut tx, run_id, &assignments)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("not shard-capable"));
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn execution_seed_reuses_dataset_identity_without_a_write_lock(pool: PgPool) {
    let version = Uuid::now_v7();
    let dataset = Uuid::now_v7();
    sqlx::query("INSERT INTO dataset_versions(dataset_version_id,dataset_id,dataset_version) VALUES($1,$2,'fixture')").bind(version).bind(dataset).execute(&pool).await.unwrap();
    let mut lock = pool.begin().await.unwrap();
    sqlx::query("SELECT dataset_version_id FROM dataset_versions WHERE dataset_version_id=$1 FOR NO KEY UPDATE").bind(version).fetch_one(&mut *lock).await.unwrap();
    let reused = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        let mut tx = pool.begin().await?;
        upsert_dataset_version(&mut tx, version, dataset, "fixture").await?;
        tx.commit().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await;
    lock.rollback().await.unwrap();
    assert!(
        reused.is_ok(),
        "execution seeding serialized on an immutable dataset write lock"
    );
    reused.unwrap().unwrap();
}
