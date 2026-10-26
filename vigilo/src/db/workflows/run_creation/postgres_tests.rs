// PostgreSQL-backed workflow scenarios and fixtures.

use sqlx::PgPool;

use super::*;

async fn insert_creation_fixture(pool: &PgPool, status: &str) -> (Uuid, Uuid) {
    let run_id = Uuid::now_v7();
    let owner_id = Uuid::now_v7();
    let dataset_id = Uuid::now_v7();
    let dataset_version_id = Uuid::now_v7();
    let chunk_id = Uuid::now_v7();

    sqlx::query(
            "INSERT INTO dataset_versions (dataset_version_id, dataset_id, dataset_version) VALUES ($1, $2, 'test')",
        )
        .bind(dataset_version_id)
        .bind(dataset_id)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        r#"
            INSERT INTO runs (
                id, run_key, dataset_id, dataset_version_id, dataset_version,
                evaluation_profile_id, evaluation_profile_version,
                profile_version_id, profile_hash,
                aggregation_policy_id, aggregation_policy_version,
                aggregation_policy_hash, agent_provider, agent_name,
                prompt_config_id, prompt_config_version,
                expected_execution_count, status,
                coordinator_id, coordinator_leased_until
            )
            VALUES (
                $1, $2, $3, $4, 'test',
                'profile', '1.0.0', 'profile-version', 'profile-hash',
                'aggregation', '1.0.0', 'aggregation-hash',
                'test', 'agent', 'prompt', '1.0.0', 1,
                'creating'::run_status, $5, now() + interval '5 minutes'
            )
            "#,
    )
    .bind(run_id)
    .bind(format!("run-{run_id}"))
    .bind(dataset_id)
    .bind(dataset_version_id)
    .bind(owner_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
            INSERT INTO run_creation_placements (
                run_id, database_alias, status, expected_case_count,
                seeded_case_count, last_seeded_case_ordinal,
                case_projection_hash, seeded_at
            )
            VALUES (
                $1, 'primary', $2, 1,
                CASE WHEN $2 = 'seeded' THEN 1 ELSE 0 END,
                CASE WHEN $2 = 'seeded' THEN 0 ELSE NULL END,
                'fixture-projection-hash',
                CASE WHEN $2 = 'seeded' THEN now() ELSE NULL END
            )
            "#,
    )
    .bind(run_id)
    .bind(status)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
            INSERT INTO run_creation_chunks (
                run_id, database_alias, chunk_id, run_shard,
                profile_group_id, ordinal_start, ordinal_end
            )
            VALUES ($1, 'primary', $2, 0, 'default', 0, 1)
            "#,
    )
    .bind(run_id)
    .bind(chunk_id)
    .execute(pool)
    .await
    .unwrap();

    (run_id, owner_id)
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx creation tests"]
async fn projection_seed_page_reads_only_persisted_placement_ranges(pool: PgPool) {
    let (run_id, _) = insert_creation_fixture(&pool, "pending").await;
    let dataset_version_id =
        sqlx::query_scalar::<_, Uuid>("SELECT dataset_version_id FROM runs WHERE id = $1")
            .bind(run_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    for ordinal in 0..3 {
        let case_id = Uuid::now_v7();
        let case_hash = format!("seed-page-{ordinal}");
        sqlx::query(
                "INSERT INTO case_blobs (case_hash, task_type, input_payload, expected_output) VALUES ($1, 'test', '{}'::jsonb, 'null'::jsonb)",
            )
            .bind(&case_hash)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
                "INSERT INTO dataset_version_cases (dataset_version_id, case_id, case_ordinal, case_hash) VALUES ($1, $2, $3, $4)",
            )
            .bind(dataset_version_id)
            .bind(case_id)
            .bind(ordinal)
            .bind(case_hash)
            .execute(&pool)
            .await
            .unwrap();
    }

    let page = select_projection_seed_page(&pool, run_id, "primary", None, 10)
        .await
        .unwrap();
    let blobs = select_projection_page_blobs(&pool, &page).await.unwrap();
    let next_page = select_projection_seed_page(&pool, run_id, "primary", Some(0), 10)
        .await
        .unwrap();

    assert_eq!(page.len(), 1);
    assert_eq!(page[0].case_ordinal, 0);
    assert_eq!(blobs.len(), 1);
    assert_eq!(blobs[0].case_hash, page[0].case_hash);
    assert!(next_page.is_empty());
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn activation_requires_every_placement_to_be_seeded(pool: PgPool) {
    let (run_id, owner_id) = insert_creation_fixture(&pool, "pending").await;

    activate_claimed_run(&pool, run_id, owner_id).await.unwrap();

    let status =
        sqlx::query_scalar::<_, String>("SELECT status::text FROM runs WHERE id = $1::uuid")
            .bind(run_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let cursor_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::bigint FROM run_shard_dispatch_cursors WHERE run_id = $1::uuid",
    )
    .bind(run_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(status, RUN_STATUS_CREATING);
    assert_eq!(cursor_count, 0);
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn activation_creates_cursors_once_and_clears_chunk_plan(pool: PgPool) {
    let (run_id, owner_id) = insert_creation_fixture(&pool, "seeded").await;

    activate_claimed_run(&pool, run_id, owner_id).await.unwrap();

    let status =
        sqlx::query_scalar::<_, String>("SELECT status::text FROM runs WHERE id = $1::uuid")
            .bind(run_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let cursor_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::bigint FROM run_shard_dispatch_cursors WHERE run_id = $1::uuid",
    )
    .bind(run_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let plan_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::bigint FROM run_creation_chunks WHERE run_id = $1::uuid",
    )
    .bind(run_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(status, RUN_STATUS_PENDING);
    assert_eq!(cursor_count, 1);
    assert_eq!(plan_count, 0);
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn activation_retry_rolls_back_partial_control_writes(pool: PgPool) {
    let (run_id, owner_id) = insert_creation_fixture(&pool, "seeded").await;
    sqlx::query(
        r#"
            CREATE FUNCTION fail_run_creation_activation()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $$
            BEGIN
                IF OLD.status = 'creating' AND NEW.status = 'pending' THEN
                    RAISE EXCEPTION 'injected activation failure';
                END IF;
                RETURN NEW;
            END;
            $$
            "#,
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
            CREATE TRIGGER fail_run_creation_activation
            BEFORE UPDATE ON runs
            FOR EACH ROW
            EXECUTE FUNCTION fail_run_creation_activation()
            "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    finish_claimed_run(&pool, run_id, owner_id).await.unwrap();
    let (cursor_count, plan_count, error_message) =
            sqlx::query_as::<_, (i64, i64, Option<String>)>(
                r#"
                SELECT
                    (SELECT COUNT(*)::bigint FROM run_shard_dispatch_cursors WHERE run_id = $1::uuid),
                    (SELECT COUNT(*)::bigint FROM run_creation_chunks WHERE run_id = $1::uuid),
                    error_message
                FROM runs
                WHERE id = $1::uuid
                "#,
            )
            .bind(run_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(cursor_count, 0);
    assert_eq!(plan_count, 1);
    assert!(
        error_message
            .as_deref()
            .is_some_and(|message| message.contains("injected activation failure"))
    );

    sqlx::query("DROP TRIGGER fail_run_creation_activation ON runs")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DROP FUNCTION fail_run_creation_activation()")
        .execute(&pool)
        .await
        .unwrap();
    finish_claimed_run(&pool, run_id, owner_id).await.unwrap();

    let (cursor_count, error_message) = sqlx::query_as::<_, (i64, Option<String>)>(
        r#"
            SELECT
                (SELECT COUNT(*)::bigint FROM run_shard_dispatch_cursors WHERE run_id = $1::uuid),
                error_message
            FROM runs
            WHERE id = $1::uuid
            "#,
    )
    .bind(run_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(cursor_count, 1);
    assert_eq!(error_message, None);
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn terminal_creation_failure_never_creates_dispatch_cursors(pool: PgPool) {
    let (run_id, owner_id) = insert_creation_fixture(&pool, "pending").await;

    fail_claimed_run(&pool, run_id, owner_id, "immutable seed mismatch")
        .await
        .unwrap();

    let (run_status, placement_status, cursor_count) = sqlx::query_as::<_, (String, String, i64)>(
        r#"
            SELECT
                run.status::text,
                creation.status,
                (
                    SELECT COUNT(*)::bigint
                    FROM run_shard_dispatch_cursors cursor
                    WHERE cursor.run_id = run.id
                )
            FROM runs run
            JOIN run_creation_placements creation ON creation.run_id = run.id
            WHERE run.id = $1::uuid
            "#,
    )
    .bind(run_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(run_status, RUN_STATUS_FAILED);
    assert_eq!(placement_status, "failed");
    assert_eq!(cursor_count, 0);
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn expired_creation_lease_can_be_reclaimed(pool: PgPool) {
    let (run_id, first_owner) = insert_creation_fixture(&pool, "pending").await;
    let second_owner = Uuid::now_v7();

    assert_eq!(
        claim_next_creating_run(&pool, second_owner, 60)
            .await
            .unwrap(),
        None
    );
    sqlx::query(
            "UPDATE runs SET coordinator_leased_until = now() - interval '1 second' WHERE id = $1::uuid",
        )
        .bind(run_id)
        .execute(&pool)
        .await
        .unwrap();

    assert_eq!(
        claim_next_creating_run(&pool, second_owner, 60)
            .await
            .unwrap(),
        Some(run_id)
    );
    assert_ne!(first_owner, second_owner);
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn blob_preparation_is_not_claimed_or_activated_by_sql_seed_recovery(pool: PgPool) {
    use sqlx::types::Json;

    use crate::{
        contracts::{
            case_storage::{
                FormatLimits,
                identity::DatasetIdentity,
            },
            evaluator_abi::EvaluatorExecutionPlan,
        },
        db::workflows::dataset_upload,
    };
    let (run_id, owner) = insert_creation_fixture(&pool, "seeded").await;
    let (dataset_id, version): (Uuid, Uuid) =
        sqlx::query_as("SELECT dataset_id,dataset_version_id FROM runs WHERE id=$1")
            .bind(run_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let limits = FormatLimits::default();
    let upload = dataset_upload::begin(
        &pool,
        Uuid::now_v7(),
        "cases",
        &"2".repeat(64),
        &DatasetIdentity {
            dataset_id,
            dataset_version: Some("test".into()),
            dataset_version_id: version,
            dataset_hash: "1".repeat(64),
            case_count: 1,
        },
        &limits,
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO dataset_catalog(id,storage_alias,binding_hash,dataset_hash,dataset_version_id,limits,manifest) VALUES($1,'cases',$2,$3,$4,$5,'{}')").bind(upload.id).bind(&upload.binding_hash).bind(&upload.identity.dataset_hash).bind(version).bind(Json(&limits)).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO run_plans(run_id,dataset_upload_id,storage_binding_hash,limits,execution_plan,assignments) VALUES($1,$2,$3,$4,$5,$6)").bind(run_id).bind(upload.id).bind(&upload.binding_hash).bind(Json(&limits)).bind(Json(EvaluatorExecutionPlan::new("3".repeat(64),Vec::new()))).bind(serde_json::json!([{"run_shard":0,"database_alias":"primary"}])).execute(&pool).await.unwrap();
    sqlx::query("UPDATE runs SET creation_kind='blob-plan',coordinator_leased_until=clock_timestamp()-interval '1 second' WHERE id=$1").bind(run_id).execute(&pool).await.unwrap();
    assert_eq!(
        claim_next_creating_run(&pool, Uuid::now_v7(), 300)
            .await
            .unwrap(),
        None
    );
    assert!(activate_claimed_run(&pool, run_id, owner).await.is_err());
    let (status,cursors):(String,i64)=sqlx::query_as("SELECT status::text,(SELECT count(*) FROM run_shard_dispatch_cursors WHERE run_id=$1) FROM runs WHERE id=$1").bind(run_id).fetch_one(&pool).await.unwrap();
    assert_eq!(status, "creating");
    assert_eq!(cursors, 0);
}
