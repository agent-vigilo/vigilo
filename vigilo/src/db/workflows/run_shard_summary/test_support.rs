//! Shared PostgreSQL settlement fixtures for workflow and coordinator tests.

use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

/// Records application SQL submissions within one polled future, excluding
/// transaction commands. Database triggers and query-internal operations are
/// intentionally outside this round-trip observation. The subscriber is local
/// to the future, so parallel PostgreSQL tests do not share observations.
pub(crate) async fn record_queries<F: std::future::Future>(future: F) -> (F::Output, Vec<String>) {
    use std::sync::{
        Arc,
        Mutex,
    };

    use tracing::{
        field::Visit,
        instrument::WithSubscriber,
    };
    use tracing_subscriber::{
        layer::Context,
        prelude::*,
    };

    struct Queries(Arc<Mutex<Vec<String>>>);

    #[derive(Default)]
    struct Statement {
        sql: String,
        summary: String,
    }

    impl Visit for Statement {
        fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            match field.name() {
                "db.statement" => self.sql = value.trim().to_owned(),
                "summary" => self.summary = value.to_owned(),
                _ => {}
            }
        }
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Queries {
        fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
            if event.metadata().target() != "sqlx::query" {
                return;
            }
            let mut statement = Statement::default();
            event.record(&mut statement);
            let sql = if statement.sql.is_empty() {
                statement.summary
            } else {
                statement.sql
            };
            if !sql.is_empty()
                && !matches!(sql.trim_end_matches(';'), "BEGIN" | "COMMIT" | "ROLLBACK")
            {
                self.0.lock().unwrap().push(sql);
            }
        }
    }

    let queries = Arc::new(Mutex::new(Vec::new()));
    // tracing-core's single-dispatch fast path consults the registering thread's
    // default subscriber. A concurrent unrecorded test can otherwise register a
    // SQLx callsite as disabled for this recorder too. Two live dispatchers make
    // registration consult the dispatcher registry, while events stay scoped to
    // the polled future. The inert dispatcher is never installed as a default.
    let registration_guard = tracing::Dispatch::new(tracing_subscriber::registry());
    let subscriber = tracing_subscriber::registry().with(Queries(queries.clone()));
    let dispatcher = tracing::Dispatch::new(subscriber);
    let output = future.with_subscriber(dispatcher).await;
    drop(registration_guard);
    let statements = std::mem::take(&mut *queries.lock().unwrap());
    (output, statements)
}

#[tokio::test]
async fn query_recorder_observes_callsites_first_used_on_an_unrecorded_thread() {
    fn emit_query_event() {
        tracing::event!(
            target: "sqlx::query",
            tracing::Level::DEBUG,
            summary = "SELECT recorder_probe",
            db.statement = "SELECT recorder_probe"
        );
    }

    let ((), statements) = record_queries(async {
        std::thread::spawn(emit_query_event).join().unwrap();
        emit_query_event();
    })
    .await;
    assert_eq!(statements, ["SELECT recorder_probe"]);
}

pub(crate) async fn fixture(pool: &PgPool, count: i32) -> (Uuid, Uuid) {
    let run = Uuid::now_v7();
    let dataset_id = Uuid::now_v7();
    let dataset = Uuid::now_v7();
    seed_control_run(pool, run, dataset_id, dataset).await;
    sqlx::query("UPDATE runs SET expected_execution_count=$2 WHERE id=$1")
        .bind(run)
        .bind(count)
        .execute(pool)
        .await
        .unwrap();
    seed_run_snapshot(pool, run, 4, dataset_id, dataset, count).await;
    (run, dataset)
}

pub(crate) async fn ranged_chunk(
    pool: &PgPool,
    run: Uuid,
    dataset: Uuid,
    start: i32,
    end: i32,
    status: &str,
) -> Uuid {
    let id = seed_chunk(pool, run, 4, dataset, status).await;
    sqlx::query("UPDATE run_chunks SET ordinal_start=$3,ordinal_end=$4 WHERE run_id=$1 AND id=$2")
        .bind(run)
        .bind(id)
        .bind(start)
        .bind(end)
        .execute(pool)
        .await
        .unwrap();
    freeze_plan(pool, run, 4).await;
    id
}

/// Gives directly seeded chunks the frozen manifest normally produced by creation.
pub(crate) async fn freeze_plan(pool: &PgPool, run: Uuid, shard: i16) {
    let chunks = sqlx::query_as::<_, super::plan::ChunkIdentity>(
        "SELECT id AS chunk_id,run_shard,ordinal_start,ordinal_end FROM run_chunks WHERE run_id=$1 AND run_shard=$2 ORDER BY ordinal_start,id",
    ).bind(run).bind(shard).fetch_all(pool).await.unwrap();
    let mut manifest = super::plan::Manifest::default();
    for chunk in chunks {
        manifest.push(&chunk).unwrap();
    }
    let plan = json!({"version": 1, "shards": {shard.to_string(): manifest}});
    sqlx::query("UPDATE run_snapshots SET config_snapshot=jsonb_set(config_snapshot,'{settlement_plan}',$3) WHERE run_id=$1 AND run_shard=$2")
        .bind(run).bind(shard).bind(&plan).execute(pool).await.unwrap();
    sqlx::query("UPDATE runs SET config_snapshot=jsonb_set(config_snapshot,'{settlement_plan}',$2) WHERE id=$1")
        .bind(run).bind(plan).execute(pool).await.unwrap();
}

pub(crate) async fn seed_control_run(
    pool: &PgPool,
    run_id: Uuid,
    dataset_id: Uuid,
    dataset_version_id: Uuid,
) {
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
                ('run-' || $1::uuid::text),
                $2::uuid,
                $3::uuid,
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
                2
            )
            "#,
    )
    .bind(run_id)
    .bind(dataset_id)
    .bind(dataset_version_id)
    .execute(pool)
    .await
    .unwrap();
}

pub(crate) async fn seed_run_snapshot(
    pool: &PgPool,
    run_id: Uuid,
    run_shard: i16,
    dataset_id: Uuid,
    dataset_version_id: Uuid,
    expected_execution_count: i32,
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
                ('run-' || $1::uuid::text),
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
                $5
            )
            "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .bind(dataset_id)
    .bind(dataset_version_id)
    .bind(expected_execution_count)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO local_shard_admissions (run_id, run_shard, database_alias, write_epoch, state) VALUES ($1,$2,'primary',1,'open')")
        .bind(run_id).bind(run_shard).execute(pool).await.unwrap();
    sqlx::query(
        "UPDATE run_snapshots SET config_snapshot = $3 WHERE run_id = $1 AND run_shard = $2",
    )
    .bind(run_id)
    .bind(run_shard)
    .bind(profile_snapshot(json!([])))
    .execute(pool)
    .await
    .unwrap();
}

pub(crate) async fn seed_chunk(
    pool: &PgPool,
    run_id: Uuid,
    run_shard: i16,
    dataset_version_id: Uuid,
    status: &str,
) -> Uuid {
    let chunk_id = Uuid::now_v7();
    sqlx::query(
        r#"
            INSERT INTO run_chunks (
                id,
                run_id,
                run_shard,
                dataset_version_id,
                profile_group_id,
                ordinal_start,
                ordinal_end,
                status,
                dispatched_at
            )
            VALUES ($1::uuid, $2::uuid, $3, $4::uuid, 'default', 0, (SELECT expected_execution_count FROM run_snapshots WHERE run_id = $2 AND run_shard = $3), $5, now())
            "#,
    )
    .bind(chunk_id)
    .bind(run_id)
    .bind(run_shard)
    .bind(dataset_version_id)
    .bind(status)
    .execute(pool)
    .await
    .unwrap();

    chunk_id
}

pub(crate) async fn seed_execution(
    pool: &PgPool,
    run_id: Uuid,
    run_shard: i16,
    chunk_id: Uuid,
    execution_id: Uuid,
    attempt_id: Uuid,
    status: &str,
) {
    let case_id = Uuid::now_v7();
    sqlx::query(
        r#"
            INSERT INTO executions (
                id,
                run_id,
                run_shard,
                chunk_id,
                case_id,
                case_hash,
                profile_group_id,
                task_type,
                evaluation_profile_id,
                evaluation_profile_version,
                expected_evaluator_count,
                status,
                current_attempt_id,
                current_attempt_no
            )
            VALUES (
                $1::uuid,
                $2::uuid,
                $3,
                $4::uuid,
                $5::uuid,
                'case-hash',
                'default',
                'classification',
                'profile',
                '1.0.0',
                1,
                $6::execution_status,
                $7::uuid,
                1
            )
            "#,
    )
    .bind(execution_id)
    .bind(run_id)
    .bind(run_shard)
    .bind(chunk_id)
    .bind(case_id)
    .bind(status)
    .bind(attempt_id)
    .execute(pool)
    .await
    .unwrap();

    let (dataset, ordinal): (Uuid, i32) = sqlx::query_as("SELECT c.dataset_version_id, c.ordinal_start + (SELECT COUNT(*)::int - 1 FROM executions e WHERE e.run_id = c.run_id AND e.run_shard = c.run_shard AND e.chunk_id = c.id) FROM run_chunks c WHERE c.run_id = $1 AND c.run_shard = $2 AND c.id = $3")
        .bind(run_id).bind(run_shard).bind(chunk_id).fetch_one(pool).await.unwrap();
    seed_planned_case(pool, run_id, run_shard, dataset, case_id, ordinal).await;

    sqlx::query(
        r#"
            INSERT INTO execution_attempts (
                id,
                run_id,
                run_shard,
                execution_id,
                attempt_no,
                status,
                completed_at
            )
            VALUES ($1::uuid, $2::uuid, $3, $4::uuid, 1, 'completed'::attempt_status, now())
            "#,
    )
    .bind(attempt_id)
    .bind(run_id)
    .bind(run_shard)
    .bind(execution_id)
    .execute(pool)
    .await
    .unwrap();
}

pub(crate) async fn seed_aggregate(
    pool: &PgPool,
    run_id: Uuid,
    run_shard: i16,
    execution_id: Uuid,
    attempt_id: Uuid,
    overall_status: &str,
) {
    sqlx::query(
        r#"
            INSERT INTO execution_aggregates (
                execution_id,
                run_id,
                run_shard,
                attempt_id,
                overall_status,
                evaluator_result_count
            )
            VALUES ($1::uuid, $2::uuid, $3, $4::uuid, $5::evaluation_status, 1)
            "#,
    )
    .bind(execution_id)
    .bind(run_id)
    .bind(run_shard)
    .bind(attempt_id)
    .bind(overall_status)
    .execute(pool)
    .await
    .unwrap();
}

pub(crate) fn profile_snapshot(gates: serde_json::Value) -> serde_json::Value {
    let mut profile: serde_json::Value =
        serde_yaml::from_str(include_str!("../../../../../example/profile.yaml")).unwrap();
    profile["case_groups"][0]["id"] = json!("sentiment");
    profile["case_groups"][0]["evaluators"][0]["id"] = json!("quality_eval");
    profile["scorecard"]["gates"] = gates;
    json!({"profile": profile})
}

pub(crate) async fn seed_planned_case(
    pool: &PgPool,
    run_id: Uuid,
    shard: i16,
    dataset: Uuid,
    case: Uuid,
    ordinal: i32,
) {
    let hash = case.to_string();
    sqlx::query("INSERT INTO case_blobs (case_hash, task_type, case_group, input_payload, expected_output, tags) VALUES ($1,'classification','sentiment','null','null','[\"release\"]')")
        .bind(&hash).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO run_shard_cases (run_id,run_shard,dataset_version_id,case_id,case_ordinal,case_hash) VALUES ($1,$2,$3,$4,$5,$6)")
        .bind(run_id).bind(shard).bind(dataset).bind(case).bind(ordinal).bind(hash).execute(pool).await.unwrap();
}
