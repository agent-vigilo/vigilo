// PostgreSQL-backed workflow scenarios and fixtures.

use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    AttemptLeaseContext,
    CompletedExecutionPersistence,
    ExecutionTerminalTransition,
    allocate_execution_attempts_for_cases,
    evaluation_plan_for_case,
    finalize_execution_terminal_transitions,
    persist_completed_execution_results_batch,
    tests::{
        case_group,
        run_profile,
        worker_case,
    },
};
use crate::{
    contracts::{
        aggregation::{
            AggregationBinding,
            AggregationResult,
            aggregate_results,
        },
        evaluator::EvaluationStatus,
        run::{
            AggregationMethod,
            AggregationSettings,
            DimensionAggregation,
            RunDefaults,
        },
    },
    db::{
        tables::evaluator_results,
        workflows::run_dispatch,
    },
};

struct SeededAttempt {
    run_id: Uuid,
    run_shard: i16,
    chunk_id: Uuid,
    chunk: crate::models::run_chunk::RunChunk,
    case_id: Uuid,
    execution_id: Uuid,
    attempt_id: Uuid,
    worker_id: Uuid,
}

#[derive(Debug, PartialEq, sqlx::FromRow)]
struct PersistedRawMeasurement {
    binding_id: String,
    raw_boolean: Option<bool>,
    raw_numeric: Option<f64>,
    raw_ordinal: Option<String>,
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn evaluator_invocation_and_diagnostics_persist_as_separate_rows(pool: PgPool) {
    let seed = seed_running_attempt(&pool, 60, 120).await;
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
    let mut binary_row = numeric_row.clone();
    binary_row.binding_id = "schema_valid".to_string();
    binary_row.judgment = Some("passed".to_string());
    binary_row.measurement_kind = Some("binary".to_string());
    binary_row.raw_boolean = Some(true);
    binary_row.raw_numeric = None;
    binary_row.raw_unit = None;
    binary_row.normalized_score = Some(1.0);
    binary_row.normalization_policy_hash = "binary-policy-hash".to_string();
    binary_row.diagnostics.clear();
    let mut ordinal_row = numeric_row.clone();
    ordinal_row.binding_id = "preference".to_string();
    ordinal_row.measurement_kind = Some("ordinal".to_string());
    ordinal_row.raw_numeric = None;
    ordinal_row.raw_ordinal = Some("tie".to_string());
    ordinal_row.raw_unit = None;
    ordinal_row.normalized_score = Some(0.3);
    ordinal_row.normalization_policy_hash = "ordinal-policy-hash".to_string();
    ordinal_row.diagnostics.clear();
    let rows = [numeric_row, binary_row, ordinal_row];

    let mut tx = pool.begin().await.unwrap();
    assert_eq!(
        evaluator_results::insert_evaluator_results_batch(&mut tx, &rows)
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        evaluator_results::insert_evaluator_results_batch(&mut tx, &rows)
            .await
            .unwrap(),
        0
    );
    tx.commit().await.unwrap();

    let persisted: (
        String,
        Option<String>,
        Option<f64>,
        Option<f64>,
        Option<String>,
        i64,
    ) = sqlx::query_as(
        r#"
        SELECT er.outcome::text, er.judgment::text, er.raw_numeric, er.normalized_score,
               er.raw_unit,
               COUNT(ed.id)::bigint
        FROM evaluator_results er
        LEFT JOIN evaluator_diagnostics ed
          ON ed.run_id = er.run_id AND ed.run_shard = er.run_shard
         AND ed.evaluator_result_id = er.id
        WHERE er.run_id = $1::uuid AND er.run_shard = $2 AND er.binding_id = $3
        GROUP BY er.outcome, er.judgment, er.raw_numeric, er.normalized_score, er.raw_unit
        "#,
    )
    .bind(seed.run_id)
    .bind(seed.run_shard)
    .bind("quality_score")
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(
        persisted,
        (
            "completed".to_string(),
            Some("failed".to_string()),
            Some(0.2),
            Some(0.2),
            Some("ratio".to_string()),
            1
        )
    );

    let typed_raw = sqlx::query_as::<_, PersistedRawMeasurement>(
        r#"
        SELECT binding_id, raw_boolean, raw_numeric, raw_ordinal
        FROM evaluator_results
        WHERE run_id = $1::uuid AND run_shard = $2
        ORDER BY binding_id
        "#,
    )
    .bind(seed.run_id)
    .bind(seed.run_shard)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        typed_raw,
        vec![
            PersistedRawMeasurement {
                binding_id: "preference".to_string(),
                raw_boolean: None,
                raw_numeric: None,
                raw_ordinal: Some("tie".to_string()),
            },
            PersistedRawMeasurement {
                binding_id: "quality_score".to_string(),
                raw_boolean: None,
                raw_numeric: Some(0.2),
                raw_ordinal: None,
            },
            PersistedRawMeasurement {
                binding_id: "schema_valid".to_string(),
                raw_boolean: Some(true),
                raw_numeric: None,
                raw_ordinal: None,
            },
        ]
    );
}

async fn seed_running_attempt(
    pool: &PgPool,
    attempt_lease_seconds: i32,
    chunk_lease_seconds: i32,
) -> SeededAttempt {
    let dataset_id = Uuid::now_v7();
    let dataset_version_id = Uuid::now_v7();
    let run_id = Uuid::now_v7();
    let chunk_id = Uuid::now_v7();
    let execution_id = Uuid::now_v7();
    let attempt_id = Uuid::now_v7();
    let case_id = Uuid::now_v7();
    let worker_id = Uuid::now_v7();
    let queue_message_id = Uuid::now_v7();
    let run_shard = 0i16;

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
                status,
                expected_execution_count,
                started_at,
                dispatched_at
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
                'running'::run_status,
                1,
                now(),
                now()
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
                $3,
                $4::uuid,
                $5::uuid,
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
                1
            )
            "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .bind(format!("run-{run_id}"))
    .bind(dataset_id)
    .bind(dataset_version_id)
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        r#"
            INSERT INTO local_shard_admissions (
                run_id, run_shard, database_alias, write_epoch, state
            )
            VALUES ($1::uuid, $2, 'primary', 1, 'open')
            "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .execute(pool)
    .await
    .unwrap();

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
                lease_token,
                leased_until,
                dispatched_at
            )
            VALUES (
                $1::uuid,
                $2::uuid,
                $3,
                $4::uuid,
                'default',
                0,
                1,
                'leased',
                gen_random_uuid(),
                now() + ($5::int * interval '1 second'),
                now()
            )
            "#,
    )
    .bind(chunk_id)
    .bind(run_id)
    .bind(run_shard)
    .bind(dataset_version_id)
    .bind(chunk_lease_seconds)
    .execute(pool)
    .await
    .unwrap();

    let chunk = sqlx::query_as::<_, crate::models::run_chunk::RunChunk>(
        r#"
            SELECT
                1::bigint AS write_epoch,
                id, run_id, run_shard, dataset_version_id, profile_group_id,
                ordinal_start, ordinal_end, status, lease_token, leased_until,
                created_at, updated_at
            FROM run_chunks
            WHERE run_id = $1::uuid
              AND run_shard = $2
              AND id = $3::uuid
            "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .bind(chunk_id)
    .fetch_one(pool)
    .await
    .unwrap();

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
                current_attempt_no,
                started_at
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
                0,
                'running'::execution_status,
                1,
                now()
            )
            "#,
    )
    .bind(execution_id)
    .bind(run_id)
    .bind(run_shard)
    .bind(chunk_id)
    .bind(case_id)
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        r#"
            INSERT INTO execution_attempts (
                id,
                execution_id,
                run_id,
                run_shard,
                attempt_no,
                status,
                worker_id,
                worker_host,
                queue_message_id,
                broker_message_id,
                leased_until,
                heartbeat_at,
                started_at
            )
            VALUES (
                $1::uuid,
                $2::uuid,
                $3::uuid,
                $4,
                1,
                'running'::attempt_status,
                $5::uuid,
                'worker-a',
                $6::uuid,
                'broker-message',
                now() + ($7::int * interval '1 second'),
                now(),
                now()
            )
            "#,
    )
    .bind(attempt_id)
    .bind(execution_id)
    .bind(run_id)
    .bind(run_shard)
    .bind(worker_id)
    .bind(queue_message_id)
    .bind(attempt_lease_seconds)
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        r#"
            UPDATE executions
            SET current_attempt_id = $4::uuid
            WHERE run_id = $1::uuid
              AND run_shard = $2
              AND id = $3::uuid
            "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .bind(execution_id)
    .bind(attempt_id)
    .execute(pool)
    .await
    .unwrap();

    SeededAttempt {
        run_id,
        run_shard,
        chunk_id,
        chunk,
        case_id,
        execution_id,
        attempt_id,
        worker_id,
    }
}

async fn insert_passing_aggregate(pool: &PgPool, seed: &SeededAttempt) {
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
            VALUES (
                $1::uuid,
                $2::uuid,
                $3,
                $4::uuid,
                'passed'::evaluation_status,
                0
            )
            "#,
    )
    .bind(seed.execution_id)
    .bind(seed.run_id)
    .bind(seed.run_shard)
    .bind(seed.attempt_id)
    .execute(pool)
    .await
    .unwrap();
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn terminal_transition_rejects_expired_attempt_lease(pool: PgPool) {
    let seed = seed_running_attempt(&pool, -5, 120).await;
    insert_passing_aggregate(&pool, &seed).await;

    let result = finalize_execution_terminal_transitions(
        &pool,
        seed.run_id,
        seed.run_shard,
        seed.worker_id,
        1,
        &[ExecutionTerminalTransition {
            execution_id: seed.execution_id,
            attempt_id: seed.attempt_id,
            attempt_no: 1,
            completed: true,
            error_message: None,
            requires_worker_lease: true,
            agent_invocation: None,
            agent_failure: None,
        }],
    )
    .await;

    assert!(result.is_err());
    let status = sqlx::query_scalar::<_, String>(
        r#"
            SELECT status::text
            FROM execution_attempts
            WHERE run_id = $1::uuid
              AND run_shard = $2
              AND id = $3::uuid
            "#,
    )
    .bind(seed.run_id)
    .bind(seed.run_shard)
    .bind(seed.attempt_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(status, "stale");
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn expired_chunk_recovery_stales_attempt_and_requeues_chunk(pool: PgPool) {
    let seed = seed_running_attempt(&pool, -5, -5).await;

    let outcome = run_dispatch::recover_expired_chunk_leases(&pool, 3, 10)
        .await
        .unwrap();

    assert_eq!(outcome.recovered_chunks, 1);
    assert_eq!(outcome.failed_chunks, 0);

    let (chunk_status, recovery_count) = sqlx::query_as::<_, (String, i32)>(
        r#"
            SELECT status, recovery_count
            FROM run_chunks
            WHERE run_id = $1::uuid
              AND run_shard = $2
              AND id = $3::uuid
            "#,
    )
    .bind(seed.run_id)
    .bind(seed.run_shard)
    .bind(seed.chunk_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(chunk_status, "pending");
    assert_eq!(recovery_count, 1);

    let attempt_status = sqlx::query_scalar::<_, String>(
        r#"
            SELECT status::text
            FROM execution_attempts
            WHERE run_id = $1::uuid
              AND run_shard = $2
              AND id = $3::uuid
            "#,
    )
    .bind(seed.run_id)
    .bind(seed.run_shard)
    .bind(seed.attempt_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(attempt_status, "stale");

    let requeued = sqlx::query_scalar::<_, i64>(
        r#"
            SELECT COUNT(*)::bigint
            FROM outbox_events
            WHERE event_type = 'run.chunk.ready'
              AND aggregate_id = $1::uuid
              AND dedupe_key = format(
                    'run:%s:chunk:%s:ready:recovery:%s',
                    $1::uuid,
                    $2::uuid,
                    1
                  )
            "#,
    )
    .bind(seed.run_id)
    .bind(seed.chunk_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(requeued, 1);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn recovered_attempt_cannot_persist_late_results(pool: PgPool) {
    let seed = seed_running_attempt(&pool, -5, -5).await;
    run_dispatch::recover_expired_chunk_leases(&pool, 3, 10)
        .await
        .unwrap();
    let completed = CompletedExecutionPersistence {
        execution_id: seed.execution_id,
        attempt_id: seed.attempt_id,
        attempt_no: 1,
        result_rows: Vec::new(),
        overall_status: "passed".to_string(),
        aggregate_score: Some(1.0),
        evaluator_result_count: 0,
        dimension_scores: json!({}),
        blocking_failures: json!([]),
        summary: json!({}),
    };

    let result =
        persist_completed_execution_results_batch(&pool, &seed.chunk, seed.worker_id, &[completed])
            .await;

    assert!(result.is_err(), "a recovered attempt no longer owns writes");
    let aggregate_count = sqlx::query_scalar::<_, i64>(
        r#"
            SELECT COUNT(*)::bigint
            FROM execution_aggregates
            WHERE run_id = $1::uuid
              AND run_shard = $2
              AND execution_id = $3::uuid
            "#,
    )
    .bind(seed.run_id)
    .bind(seed.run_shard)
    .bind(seed.execution_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(aggregate_count, 0);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn incomplete_required_evaluator_persists_error_without_score(pool: PgPool) {
    let seed = seed_running_attempt(&pool, 60, 120).await;
    let scored_evaluator_id = Uuid::now_v7();
    let errored_evaluator_id = Uuid::now_v7();
    let bindings = [
        AggregationBinding {
            binding_id: "scored".to_string(),
            required: true,
        },
        AggregationBinding {
            binding_id: "errored".to_string(),
            required: true,
        },
    ];
    let results = [
        AggregationResult {
            binding_id: "scored".to_string(),
            evaluator_id: scored_evaluator_id,
            binding_dimension: "quality".to_string(),
            status: EvaluationStatus::Passed,
            normalized_score: Some(1.0),
            blocking: false,
            binding_weight: 1.0,
            failure_category: None,
            reason: None,
        },
        AggregationResult {
            binding_id: "errored".to_string(),
            evaluator_id: errored_evaluator_id,
            binding_dimension: "quality".to_string(),
            status: EvaluationStatus::Error,
            normalized_score: None,
            blocking: false,
            binding_weight: 1.0,
            failure_category: Some("evaluator_runtime_error".to_string()),
            reason: Some("test error".to_string()),
        },
    ];
    let outcome = aggregate_results(
        &RunDefaults {
            max_attempts: 1,
            request_timeout_secs: 30,
            fail_on_any_blocking_failure: true,
            min_execution_score: 0.8,
        },
        &AggregationSettings {
            dimensions: [(
                "quality".to_string(),
                DimensionAggregation {
                    method: AggregationMethod::WeightedMean,
                    blocking: false,
                    weight: 1.0,
                },
            )]
            .into_iter()
            .collect(),
        },
        seed.attempt_id,
        &bindings,
        &results,
    );
    let completed = CompletedExecutionPersistence {
        execution_id: seed.execution_id,
        attempt_id: seed.attempt_id,
        attempt_no: 1,
        result_rows: Vec::new(),
        overall_status: outcome.overall_status,
        aggregate_score: outcome.aggregate_score,
        evaluator_result_count: i32::try_from(results.len()).unwrap(),
        dimension_scores: outcome.dimension_scores,
        blocking_failures: outcome.blocking_failures,
        summary: outcome.summary,
    };

    persist_completed_execution_results_batch(&pool, &seed.chunk, seed.worker_id, &[completed])
        .await
        .unwrap();

    let (overall_status, aggregate_score, dimension_scores, summary) =
        sqlx::query_as::<_, (String, Option<f64>, serde_json::Value, serde_json::Value)>(
            r#"
            SELECT overall_status::text, aggregate_score, dimension_scores, summary
            FROM execution_aggregates
            WHERE run_id = $1::uuid
              AND run_shard = $2
              AND execution_id = $3::uuid
            "#,
        )
        .bind(seed.run_id)
        .bind(seed.run_shard)
        .bind(seed.execution_id)
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(overall_status, "error");
    assert_eq!(aggregate_score, None);
    assert_eq!(dimension_scores, json!({}));
    assert_eq!(summary["evaluator_completeness"]["required_error_count"], 1);
    assert_eq!(summary["evaluator_completeness"]["complete"], false);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn recovered_chunk_token_cannot_allocate_a_new_attempt(pool: PgPool) {
    let seed = seed_running_attempt(&pool, -5, -5).await;
    run_dispatch::recover_expired_chunk_leases(&pool, 3, 10)
        .await
        .unwrap();
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
        worker_id: Uuid::now_v7(),
        worker_host: Some("worker-b".to_string()),
        queue_message_id: Uuid::now_v7(),
        broker_message_id: None,
        lease_seconds: 60,
    };

    let result = allocate_execution_attempts_for_cases(
        &pool,
        &seed.chunk,
        &lease,
        &profile,
        &[case],
        &[plan],
    )
    .await;

    assert!(result.is_err(), "a recovered chunk token must be stale");
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("no longer owns a live lease")
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
async fn chunk_heartbeat_preserves_claim_token(pool: PgPool) {
    let seed = seed_running_attempt(&pool, 60, 60).await;
    let renewed =
        crate::db::workflows::chunk_processing::extend_chunk_lease(&pool, &seed.chunk, 120)
            .await
            .unwrap()
            .unwrap();

    assert_eq!(renewed.lease_token, seed.chunk.lease_token);
    assert!(renewed.leased_until > seed.chunk.leased_until);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn authentication_failure_is_terminal_before_attempt_budget_exhaustion(pool: PgPool) {
    let seed = seed_running_attempt(&pool, 120, 120).await;
    finalize_execution_terminal_transitions(
        &pool,
        seed.run_id,
        seed.run_shard,
        seed.worker_id,
        3,
        &[ExecutionTerminalTransition {
            execution_id: seed.execution_id,
            attempt_id: seed.attempt_id,
            attempt_no: 1,
            completed: false,
            error_message: Some(
                "agent invocation failed: agent HTTP request failed with status 401".into(),
            ),
            requires_worker_lease: true,
            agent_invocation: None,
            agent_failure: Some(crate::agent_client::failure::Failure::http(
                401,
                None,
                chrono::Utc::now(),
                600,
            )),
        }],
    )
    .await
    .unwrap();
    let (status, retry_after, score): (String, Option<chrono::DateTime<chrono::Utc>>, Option<f64>) = sqlx::query_as(
        "SELECT e.status::text, e.retry_after, a.aggregate_score FROM executions e LEFT JOIN execution_aggregates a ON a.run_id=e.run_id AND a.run_shard=e.run_shard AND a.execution_id=e.id WHERE e.id=$1"
    ).bind(seed.execution_id).fetch_one(&pool).await.unwrap();
    assert_eq!(status, "failed");
    assert!(retry_after.is_none());
    assert!(score.is_none(), "invocation failures are not zero scores");
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn agent_retry_policy_persists_not_before_and_preserves_budget_and_legacy_failures(
    pool: PgPool,
) {
    use crate::agent_client::failure::{
        Failure,
        Kind,
    };
    for (failure, max_attempts, expected) in [
        (
            Some(Failure::credential_unavailable()),
            3,
            "retry_scheduled",
        ),
        (Some(Failure::credential_unavailable()), 1, "failed"),
        (
            Some(Failure::terminal(
                Kind::Configuration,
                "credential binding unavailable",
            )),
            3,
            "failed",
        ),
        (
            Some(Failure::http(429, Some("120"), chrono::Utc::now(), 600)),
            3,
            "retry_scheduled",
        ),
        (
            Some(Failure::http(503, None, chrono::Utc::now(), 600)),
            1,
            "failed",
        ),
        (
            Some(Failure::terminal(Kind::BodyLimit, "oversized")),
            3,
            "failed",
        ),
        (
            Some(Failure::http(503, Some("601"), chrono::Utc::now(), 600)),
            3,
            "failed",
        ),
        (None, 3, "retry_scheduled"),
    ] {
        let seed = seed_running_attempt(&pool, 120, 120).await;
        let floor = failure.as_ref().and_then(|f| f.retry_not_before);
        let transition = ExecutionTerminalTransition {
            execution_id: seed.execution_id,
            attempt_id: seed.attempt_id,
            attempt_no: 1,
            completed: false,
            error_message: Some("opaque diagnostic; do not classify this text".into()),
            requires_worker_lease: true,
            agent_invocation: None,
            agent_failure: failure.clone(),
        };
        finalize_execution_terminal_transitions(
            &pool,
            seed.run_id,
            seed.run_shard,
            seed.worker_id,
            max_attempts,
            &[transition],
        )
        .await
        .unwrap();
        // Read persisted scheduling and diagnostics through another connection: no
        // worker-local timer or error object is needed for subsequent processing.
        let mut connection = pool.acquire().await.unwrap();
        let (status, retry_after, summary, attempt_status): (String, Option<chrono::DateTime<chrono::Utc>>, serde_json::Value, String) = sqlx::query_as(
            "SELECT e.status::text, e.retry_after, a.outcome_summary, a.status::text FROM executions e JOIN execution_attempts a ON a.run_id=e.run_id AND a.run_shard=e.run_shard AND a.id=e.current_attempt_id WHERE e.id=$1"
        ).bind(seed.execution_id).fetch_one(&mut *connection).await.unwrap();
        assert_eq!(status, expected);
        if let Some(floor) = floor {
            assert!(retry_after.unwrap() >= floor);
        }
        if let Some(failure) = failure {
            assert_eq!(
                summary["agent_failure"]["kind"],
                serde_json::to_value(failure.kind).unwrap()
            );
            assert_eq!(attempt_status, "failed_agent_call");
        } else {
            assert_eq!(attempt_status, "failed_evaluation");
            assert!(retry_after.is_some());
        }
        if expected == "failed" {
            assert!(retry_after.is_none());
            let (outcome, score): (String,Option<f64>) = sqlx::query_as("SELECT overall_status::text,aggregate_score FROM execution_aggregates WHERE execution_id=$1").bind(seed.execution_id).fetch_one(&pool).await.unwrap();
            assert_eq!(outcome, "error");
            assert!(score.is_none());
        }
    }
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn invocation_diagnostics_use_existing_terminal_summary_for_every_outcome(pool: PgPool) {
    use crate::agent_client::{
        diagnostics::Observation,
        failure::{
            Failure,
            Kind,
        },
    };
    for (completed, agent_failed) in [(true, false), (false, true), (false, false)] {
        let seed = seed_running_attempt(&pool, 120, 120).await;
        if completed {
            insert_passing_aggregate(&pool, &seed).await;
        }
        let mut observation = Observation::new(
            seed.run_id,
            seed.execution_id,
            seed.attempt_id,
            "https://example.com/?secret=canary",
        );
        observation.data.http_status = Some(if agent_failed { 503 } else { 200 });
        observation.data.latency_ms = Some(12);
        let diagnostics = observation.finish(agent_failed.then_some(Kind::HttpStatus));
        sqlx::query("UPDATE execution_attempts SET outcome_summary = '{\"existing\":true}'::jsonb WHERE id=$1").bind(seed.attempt_id).execute(&pool).await.unwrap();
        finalize_execution_terminal_transitions(
            &pool,
            seed.run_id,
            seed.run_shard,
            seed.worker_id,
            2,
            &[ExecutionTerminalTransition {
                execution_id: seed.execution_id,
                attempt_id: seed.attempt_id,
                attempt_no: 1,
                completed,
                error_message: (!completed).then(|| "safe error".into()),
                requires_worker_lease: true,
                agent_invocation: Some(diagnostics.clone()),
                agent_failure: agent_failed
                    .then(|| Failure::http(503, None, chrono::Utc::now(), 600)),
            }],
        )
        .await
        .unwrap();
        let summary: serde_json::Value =
            sqlx::query_scalar("SELECT outcome_summary FROM execution_attempts WHERE id=$1")
                .bind(seed.attempt_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(summary["existing"], true);
        assert_eq!(
            summary["agent_invocation"],
            serde_json::to_value(diagnostics).unwrap()
        );
        assert_eq!(summary.get("agent_failure").is_some(), agent_failed);
        assert!(!summary.to_string().contains("canary"));
    }
}
