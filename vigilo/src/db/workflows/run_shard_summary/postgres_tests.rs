// PostgreSQL-backed workflow scenarios and fixtures.

use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    test_support::*,
    *,
};

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn aggregation_rejects_missing_frozen_plan(pool: PgPool) {
    let (run, dataset) = fixture(&pool, 1).await;
    ranged_chunk(&pool, run, dataset, 0, 1, "failed").await;
    sqlx::query("UPDATE run_snapshots SET config_snapshot=config_snapshot-'settlement_plan' WHERE run_id=$1")
        .bind(run).execute(&pool).await.unwrap();
    let error = advance_shard_summary(&pool, run, 4, 1).await.unwrap_err();
    assert!(error.to_string().contains("missing settlement plan"));
    assert!(
        select_run_shard_summary(&pool, run, 4)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM run_chunk_settlements WHERE run_id=$1")
            .bind(run)
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn progress_includes_later_receipts_behind_an_open_chunk(pool: PgPool) {
    let (run, dataset) = fixture(&pool, 2).await;
    let first = ranged_chunk(&pool, run, dataset, 0, 1, "pending").await;
    let last = ranged_chunk(&pool, run, dataset, 1, 2, "failed").await;
    let execution = Uuid::now_v7();
    let attempt = Uuid::now_v7();
    seed_execution(&pool, run, 4, last, execution, attempt, "failed").await;
    seed_aggregate(&pool, run, 4, execution, attempt, "failed").await;
    freeze_plan(&pool, run, 4).await;
    let summary = refresh_run_shard_summary(&pool, run, 4)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(summary.failed_execution_count, 1);
    assert_eq!(summary.settled_chunk_count, 1);
    assert_eq!(summary.planned_chunk_count, 2);
    assert_eq!(
        summary.missing_execution_count, 0,
        "open work is not settled missing work"
    );
    assert!(!summary.is_terminal());
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM run_chunks WHERE run_id=$1 AND id=$2")
            .bind(run)
            .bind(first)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "pending"
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn failed_chunk_retains_scores_and_fences_unfinished_evidence(pool: PgPool) {
    let (run, dataset) = fixture(&pool, 3).await;
    let chunk = ranged_chunk(&pool, run, dataset, 0, 3, "failed").await;
    let good = Uuid::now_v7();
    let good_attempt = Uuid::now_v7();
    let partial = Uuid::now_v7();
    let partial_attempt = Uuid::now_v7();
    seed_execution(&pool, run, 4, chunk, good, good_attempt, "completed").await;
    seed_aggregate(&pool, run, 4, good, good_attempt, "passed").await;
    seed_execution(&pool, run, 4, chunk, partial, partial_attempt, "running").await;
    seed_aggregate(&pool, run, 4, partial, partial_attempt, "passed").await;
    sqlx::query("UPDATE execution_aggregates SET aggregate_score = 0.8, dimension_scores = '{\"quality\":0.8}' WHERE run_id=$1")
        .bind(run).execute(&pool).await.unwrap();
    sqlx::query("UPDATE execution_attempts SET status='running', worker_id=$3, leased_until=now()+interval '1 hour' WHERE run_id=$1 AND id=$2")
        .bind(run).bind(partial_attempt).bind(Uuid::now_v7()).execute(&pool).await.unwrap();
    let gates = json!([
        {"id":"dimension", "dimension":"quality", "min_coverage":1.0},
        {"id":"binding", "dimension":"quality", "binding_id":"quality_eval", "min_coverage":1.0},
        {"id":"group", "dimension":"quality", "case_group":"sentiment", "min_coverage":1.0},
        {"id":"tags", "dimension":"quality", "tags_all":["release"], "min_coverage":1.0}
    ]);
    sqlx::query("UPDATE run_snapshots SET config_snapshot=config_snapshot || $2 WHERE run_id=$1")
        .bind(run)
        .bind(profile_snapshot(gates))
        .execute(&pool)
        .await
        .unwrap();
    freeze_plan(&pool, run, 4).await;
    let summary = refresh_run_shard_summary(&pool, run, 4)
        .await
        .unwrap()
        .unwrap();
    assert!(summary.is_terminal());
    assert_eq!(summary.missing_execution_count, 1);
    assert_eq!(summary.unresolved_execution_count, 1);
    assert_eq!(summary.aggregate_count, 1);
    assert_eq!(summary.score_count, 1);
    assert_eq!(summary.score_sum, 0.8);
    assert_eq!(summary.passed_execution_count, 1);
    for gate in summary.scorecard["entries"].as_array().unwrap() {
        assert_eq!(
            gate["expected_count"], 3,
            "every eligible planned case supplies its denominator"
        );
        assert_eq!(
            gate["error_count"], 0,
            "missing work is not an invented evaluator error"
        );
        assert_eq!(gate["abstained_count"], 0);
        if gate["id"] != "binding" {
            assert_eq!(gate["scored_count"], 1);
            assert_eq!(gate["score_sum"], 0.8);
        }
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM execution_aggregates WHERE run_id=$1")
            .bind(run)
            .fetch_one(&pool)
            .await
            .unwrap(),
        2,
        "partial evidence is retained"
    );
    let late = crate::db::workflows::execution_processing::finalize_execution_terminal_transitions(
        &pool,
        run,
        4,
        Uuid::now_v7(),
        1,
        &[
            crate::db::workflows::execution_processing::ExecutionTerminalTransition {
                execution_id: partial,
                attempt_id: partial_attempt,
                attempt_no: 1,
                completed: true,
                error_message: None,
                requires_worker_lease: false,
                agent_failure: None,
            },
        ],
    )
    .await
    .unwrap_err();
    assert!(late.to_string().contains("settlement authority"));
    assert!(!advance_shard_summary(&pool, run, 4, 1).await.unwrap());
    assert_eq!(
        serde_json::to_value(
            select_run_shard_summary(&pool, run, 4)
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(summary).unwrap()
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn concurrent_workers_settle_without_waiting_for_the_aggregator(pool: PgPool) {
    let (run, dataset) = fixture(&pool, 2).await;
    let mut chunks = Vec::new();
    for ordinal in 0..2 {
        let id = ranged_chunk(&pool, run, dataset, ordinal, ordinal + 1, "pending").await;
        let execution = Uuid::now_v7();
        let attempt = Uuid::now_v7();
        seed_execution(&pool, run, 4, id, execution, attempt, "completed").await;
        seed_aggregate(&pool, run, 4, execution, attempt, "passed").await;
        sqlx::query("UPDATE run_chunks SET status='leased', lease_token=gen_random_uuid(), leased_until=now()+interval '1 hour' WHERE run_id=$1 AND id=$2")
            .bind(run).bind(id).execute(&pool).await.unwrap();
        let mut chunk: crate::models::run_chunk::RunChunk =
            sqlx::query_as("SELECT * FROM run_chunks WHERE run_id=$1 AND id=$2")
                .bind(run)
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap();
        chunk.write_epoch = 1;
        chunks.push(chunk);
    }
    let mut claim = pool.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::uuid::text || ':summary', 4))")
        .bind(run)
        .execute(&mut *claim)
        .await
        .unwrap();
    let (first, second) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(
            crate::db::workflows::chunk_processing::mark_chunk_completed_and_settle(
                &pool, &chunks[0]
            ),
            crate::db::workflows::chunk_processing::mark_chunk_completed_and_settle(
                &pool, &chunks[1]
            ),
        )
    })
    .await
    .expect("workers must not wait on the aggregator's claim");
    assert_eq!(first.unwrap(), 1);
    assert_eq!(second.unwrap(), 1);
    assert!(!advance_shard_summary(&pool, run, 4, 1).await.unwrap());
    claim.rollback().await.unwrap();
    for _ in 0..8 {
        let (a, b) = tokio::join!(
            advance_shard_summary(&pool, run, 4, 1),
            advance_shard_summary(&pool, run, 4, 1)
        );
        a.unwrap();
        b.unwrap();
    }
    let summary = select_run_shard_summary(&pool, run, 4)
        .await
        .unwrap()
        .unwrap();
    assert!(summary.is_terminal());
    assert_eq!(summary.passed_execution_count, 2);
    assert_eq!(summary.settled_chunk_count, 2);
    assert_eq!(
        crate::db::workflows::chunk_processing::mark_chunk_completed_and_settle(&pool, &chunks[0])
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM run_chunk_settlement_consumptions WHERE run_id=$1 AND consumed"
        )
        .bind(run)
        .fetch_one(&pool)
        .await
        .unwrap(),
        2
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn aggregation_pages_resume_and_failed_consumption_rolls_back(pool: PgPool) {
    let (run, dataset) = fixture(&pool, 257).await;
    for ordinal in 0..257 {
        ranged_chunk(&pool, run, dataset, ordinal, ordinal + 1, "failed").await;
    }
    let mut tx = pool.begin().await.unwrap();
    settle_terminal_chunks_with(&mut tx, run, 4).await.unwrap();
    initialize_run_shard_summary_with(&mut tx, run, 4)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    sqlx::query("CREATE FUNCTION fail_consumption() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected consumption failure'; END $$")
        .execute(&pool).await.unwrap();
    sqlx::query("CREATE TRIGGER fail_consumption BEFORE UPDATE ON run_chunk_settlement_consumptions FOR EACH ROW EXECUTE FUNCTION fail_consumption()")
        .execute(&pool).await.unwrap();
    let error = advance_shard_summary(&pool, run, 4, 1).await.unwrap_err();
    assert!(error.to_string().contains("injected consumption failure"));
    assert_eq!(
        select_run_shard_summary(&pool, run, 4)
            .await
            .unwrap()
            .unwrap()
            .settled_chunk_count,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM run_chunk_settlement_consumptions WHERE run_id=$1 AND consumed"
        )
        .bind(run)
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    sqlx::query("DROP TRIGGER fail_consumption ON run_chunk_settlement_consumptions")
        .execute(&pool)
        .await
        .unwrap();
    assert!(advance_shard_summary(&pool, run, 4, 1).await.unwrap());
    assert_eq!(
        select_run_shard_summary(&pool, run, 4)
            .await
            .unwrap()
            .unwrap()
            .settled_chunk_count,
        256
    );
    for _ in 0..4 {
        advance_shard_summary(&pool, run, 4, 1).await.unwrap();
    }
    let summary = select_run_shard_summary(&pool, run, 4)
        .await
        .unwrap()
        .unwrap();
    assert!(summary.is_terminal());
    assert_eq!(summary.missing_execution_count, 257);
    assert_eq!(summary.failed_chunk_count, 257);
    assert_eq!(summary.aggregate_count, 0);
    let error =
        sqlx::query("UPDATE run_chunk_settlements SET disposition='completed' WHERE run_id=$1")
            .bind(run)
            .execute(&pool)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("immutable"));
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn changed_membership_and_stale_ownership_cannot_seal(pool: PgPool) {
    let (run, dataset) = fixture(&pool, 1).await;
    let id = ranged_chunk(&pool, run, dataset, 0, 1, "failed").await;
    let wrong = crate::models::run_chunk::RunChunkDraft {
        chunk_id: Uuid::now_v7(),
        run_shard: 4,
        profile_group_id: "default".to_owned(),
        ordinal_start: 0,
        ordinal_end: 1,
    };
    sqlx::query("UPDATE run_snapshots SET config_snapshot=jsonb_set(config_snapshot,'{settlement_plan}',$2) WHERE run_id=$1")
        .bind(run).bind(chunk_plan_manifests(&[wrong]).unwrap()).execute(&pool).await.unwrap();
    let mut tx = pool.begin().await.unwrap();
    super::settle_chunk_with(&mut tx, run, 4, id).await.unwrap();
    tx.commit().await.unwrap();
    assert!(
        advance_shard_summary(&pool, run, 4, 2)
            .await
            .unwrap_err()
            .to_string()
            .contains("admission")
    );
    // SQLx queues rollback when the rejected transaction is dropped. Wait for
    // its claim to be released before requiring progress from the nonblocking
    // aggregator; another pool connection can otherwise correctly defer.
    // This standalone statement releases its own transaction lock on completion.
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        sqlx::query(
            "SELECT pg_advisory_xact_lock(hashtextextended($1::uuid::text || ':summary', 4))",
        )
        .bind(run)
        .execute(&pool),
    )
    .await
    .expect("rejected aggregation must release its claim after rollback")
    .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM run_chunk_settlement_consumptions WHERE run_id=$1 AND consumed"
        )
        .bind(run)
        .fetch_one(&pool)
        .await
        .unwrap(),
        0,
        "stale ownership must not consume receipts"
    );
    assert!(advance_shard_summary(&pool, run, 4, 1).await.unwrap());
    let error = advance_shard_summary(&pool, run, 4, 1).await.unwrap_err();
    assert!(error.to_string().contains("membership"));
    assert!(
        !select_run_shard_summary(&pool, run, 4)
            .await
            .unwrap()
            .unwrap()
            .is_terminal()
    );
    assert_eq!(
        sqlx::query_scalar::<_, Uuid>("SELECT chunk_id FROM run_chunk_settlements WHERE run_id=$1")
            .bind(run)
            .fetch_one(&pool)
            .await
            .unwrap(),
        id
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx summary tests"]
async fn failed_evaluation_does_not_settle_a_shard_with_leased_work(pool: PgPool) {
    let run_id = Uuid::now_v7();
    let dataset_id = Uuid::now_v7();
    let dataset_version_id = Uuid::now_v7();
    let shard = 4;
    seed_control_run(&pool, run_id, dataset_id, dataset_version_id).await;
    seed_run_snapshot(&pool, run_id, shard, dataset_id, dataset_version_id, 3).await;
    let completed = seed_chunk(&pool, run_id, shard, dataset_version_id, "completed").await;
    sqlx::query("UPDATE run_chunks SET ordinal_end = 2 WHERE run_id = $1 AND id = $2")
        .bind(run_id)
        .bind(completed)
        .execute(&pool)
        .await
        .unwrap();
    let leased = seed_chunk(&pool, run_id, shard, dataset_version_id, "pending").await;
    sqlx::query("UPDATE run_chunks SET ordinal_start = 2, ordinal_end = 3, status = 'leased', lease_token = gen_random_uuid(), leased_until = now() + interval '1 hour' WHERE run_id = $1 AND id = $2")
        .bind(run_id).bind(leased).execute(&pool).await.unwrap();
    let execution = Uuid::now_v7();
    let attempt = Uuid::now_v7();
    seed_execution(
        &pool,
        run_id,
        shard,
        completed,
        execution,
        attempt,
        "completed",
    )
    .await;
    seed_aggregate(&pool, run_id, shard, execution, attempt, "failed").await;

    freeze_plan(&pool, run_id, shard).await;
    let summary = refresh_run_shard_summary(&pool, run_id, shard)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(summary.failed_execution_count, 1);
    assert_eq!(
        summary.status, "running",
        "evaluation failure cannot settle leased work"
    );
    assert!(!summary.is_terminal());
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx summary tests"]
async fn settled_missing_executions_keep_configured_scorecard_gates(pool: PgPool) {
    let run_id = Uuid::now_v7();
    let dataset_id = Uuid::now_v7();
    let dataset_version_id = Uuid::now_v7();
    let shard = 4;
    seed_control_run(&pool, run_id, dataset_id, dataset_version_id).await;
    seed_run_snapshot(&pool, run_id, shard, dataset_id, dataset_version_id, 2).await;
    seed_chunk(&pool, run_id, shard, dataset_version_id, "failed").await;
    sqlx::query(
        "UPDATE run_snapshots SET config_snapshot = $3 WHERE run_id = $1 AND run_shard = $2",
    )
    .bind(run_id)
    .bind(shard)
    .bind(profile_snapshot(
        json!([{"id": "quality", "dimension": "quality", "min_coverage": 1.0}]),
    ))
    .execute(&pool)
    .await
    .unwrap();

    freeze_plan(&pool, run_id, shard).await;
    let summary = refresh_run_shard_summary(&pool, run_id, shard)
        .await
        .unwrap()
        .unwrap();
    assert!(
        summary.is_terminal(),
        "exhausted missing work must eventually settle"
    );
    assert_eq!(summary.terminal_execution_count, 0);
    assert_eq!(summary.scorecard["entries"][0]["id"], json!("quality"));
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx summary tests"]
async fn refresh_run_shard_summary_counts_terminal_outcomes(pool: PgPool) {
    let run_id = Uuid::now_v7();
    let run_shard = 4i16;
    let dataset_id = Uuid::now_v7();
    let dataset_version_id = Uuid::now_v7();
    let completed_execution_id = Uuid::now_v7();
    let completed_attempt_id = Uuid::now_v7();
    let failed_execution_id = Uuid::now_v7();
    let failed_attempt_id = Uuid::now_v7();

    seed_control_run(&pool, run_id, dataset_id, dataset_version_id).await;
    seed_run_snapshot(&pool, run_id, run_shard, dataset_id, dataset_version_id, 2).await;
    let chunk_id = seed_chunk(&pool, run_id, run_shard, dataset_version_id, "completed").await;
    seed_execution(
        &pool,
        run_id,
        run_shard,
        chunk_id,
        completed_execution_id,
        completed_attempt_id,
        "completed",
    )
    .await;
    seed_execution(
        &pool,
        run_id,
        run_shard,
        chunk_id,
        failed_execution_id,
        failed_attempt_id,
        "failed",
    )
    .await;
    seed_aggregate(
        &pool,
        run_id,
        run_shard,
        completed_execution_id,
        completed_attempt_id,
        "passed",
    )
    .await;
    seed_aggregate(
        &pool,
        run_id,
        run_shard,
        failed_execution_id,
        failed_attempt_id,
        "failed",
    )
    .await;

    freeze_plan(&pool, run_id, run_shard).await;
    let summary = refresh_run_shard_summary(&pool, run_id, run_shard)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(summary.expected_execution_count, 2);
    assert_eq!(summary.execution_count, 2);
    assert_eq!(summary.terminal_execution_count, 2);
    assert_eq!(summary.aggregate_count, 2);
    assert_eq!(summary.passed_execution_count, 1);
    assert_eq!(summary.failed_execution_count, 1);
    assert_eq!(summary.errored_execution_count, 0);
    assert_eq!(summary.skipped_execution_count, 0);
    assert_eq!(summary.missing_aggregate_count, 0);
    assert_eq!(summary.evaluator_result_count, 2);
    assert_eq!(summary.blocking_failure_count, 0);
    assert_eq!(summary.score_count, 0);
    assert_eq!(summary.score_sum, 0.0);
    assert_eq!(summary.status, "failed");
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for sqlx summary tests"]
async fn refresh_run_shard_summary_builds_bounded_scorecard_gate(pool: PgPool) {
    let run_id = Uuid::now_v7();
    let run_shard = 4i16;
    let dataset_id = Uuid::now_v7();
    let dataset_version_id = Uuid::now_v7();
    let execution_id = Uuid::now_v7();
    let attempt_id = Uuid::now_v7();

    seed_control_run(&pool, run_id, dataset_id, dataset_version_id).await;
    seed_run_snapshot(&pool, run_id, run_shard, dataset_id, dataset_version_id, 1).await;
    sqlx::query(
        r#"
        UPDATE run_snapshots
        SET config_snapshot = jsonb_set(config_snapshot, '{profile,scorecard,gates}',
            '[{"id":"quality_release","dimension":"quality","case_group":"sentiment","tags_all":["release"],"min_mean_score":0.9,"min_coverage":1.0}]'::jsonb)
        WHERE run_id = $1::uuid AND run_shard = $2
        "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .execute(&pool)
    .await
    .unwrap();
    let chunk_id = seed_chunk(&pool, run_id, run_shard, dataset_version_id, "completed").await;
    seed_execution(
        &pool,
        run_id,
        run_shard,
        chunk_id,
        execution_id,
        attempt_id,
        "completed",
    )
    .await;
    sqlx::query(
        r#"
        UPDATE executions
        SET profile_group_id = 'sentiment',
            tags = '["release"]'::jsonb,
            evaluator_manifest = '[{"id":"quality_eval","required":true,"dimension":"quality"}]'::jsonb
        WHERE run_id = $1::uuid AND run_shard = $2 AND id = $3::uuid
        "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .bind(execution_id)
    .execute(&pool)
    .await
    .unwrap();
    seed_aggregate(&pool, run_id, run_shard, execution_id, attempt_id, "passed").await;
    sqlx::query(
        "UPDATE execution_aggregates SET dimension_scores = '{\"quality\":0.8}'::jsonb WHERE run_id = $1::uuid AND run_shard = $2 AND execution_id = $3::uuid",
    )
    .bind(run_id)
    .bind(run_shard)
    .bind(execution_id)
    .execute(&pool)
    .await
    .unwrap();

    freeze_plan(&pool, run_id, run_shard).await;
    let summary = refresh_run_shard_summary(&pool, run_id, run_shard)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(summary.scorecard["version"], json!(1));
    assert_eq!(summary.scorecard["run_shard"], json!(run_shard));
    assert_eq!(
        summary.scorecard["entries"][0]["id"],
        json!("quality_release")
    );
    assert_eq!(summary.scorecard["entries"][0]["expected_count"], json!(1));
    assert_eq!(summary.scorecard["entries"][0]["scored_count"], json!(1));
    assert_eq!(summary.scorecard["entries"][0]["score_sum"], json!(0.8));
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for settlement concurrency tests"]
async fn terminal_failure_serializes_with_a_waiting_result_writer(pool: PgPool) {
    use crate::db::workflows::execution_processing::{
        ExecutionTerminalTransition,
        finalize_execution_terminal_transitions,
    };
    let (run, dataset) = fixture(&pool, 1).await;
    let chunk = ranged_chunk(&pool, run, dataset, 0, 1, "pending").await;
    let execution = Uuid::now_v7();
    let attempt = Uuid::now_v7();
    let worker = Uuid::now_v7();
    seed_execution(&pool, run, 4, chunk, execution, attempt, "running").await;
    seed_aggregate(&pool, run, 4, execution, attempt, "passed").await;
    sqlx::query("UPDATE run_chunks SET status='leased',lease_token=gen_random_uuid(),leased_until=now()+interval '1 hour' WHERE id=$1").bind(chunk).execute(&pool).await.unwrap();
    sqlx::query("UPDATE execution_attempts SET status='running',worker_id=$2,leased_until=now()+interval '1 hour' WHERE id=$1").bind(attempt).bind(worker).execute(&pool).await.unwrap();
    let mut tx = pool.begin().await.unwrap();
    sqlx::query(
        "UPDATE run_chunks SET status='failed',lease_token=NULL,leased_until=NULL WHERE id=$1",
    )
    .bind(chunk)
    .execute(&mut *tx)
    .await
    .unwrap();
    let writer_pool = pool.clone();
    let writer = tokio::spawn(async move {
        finalize_execution_terminal_transitions(
            &writer_pool,
            run,
            4,
            worker,
            1,
            &[ExecutionTerminalTransition {
                execution_id: execution,
                attempt_id: attempt,
                attempt_no: 1,
                completed: true,
                error_message: None,
                requires_worker_lease: true,
                agent_failure: None,
            }],
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!writer.is_finished());
    super::settle_chunk_with(&mut tx, run, 4, chunk)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), writer)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("settlement authority"));
    freeze_plan(&pool, run, 4).await;
    let summary = refresh_run_shard_summary(&pool, run, 4)
        .await
        .unwrap()
        .unwrap();
    assert!(summary.sealed);
    assert_eq!(summary.aggregate_count, 0);
    assert_eq!(summary.unresolved_execution_count, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM execution_aggregates WHERE run_id=$1")
            .bind(run)
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert!(
        sqlx::query("UPDATE run_shard_summaries SET score_sum=1 WHERE run_id=$1")
            .bind(run)
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE run_chunks SET status='pending' WHERE id=$1")
            .bind(chunk)
            .execute(&pool)
            .await
            .is_err()
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for settlement byte-bound tests"]
async fn receipt_byte_budget_limits_pages_and_resumes_exactly(pool: PgPool) {
    let (run, dataset) = fixture(&pool, 6).await;
    for n in 0..6 {
        ranged_chunk(&pool, run, dataset, n, n + 1, "failed").await;
    }
    sqlx::raw_sql("CREATE FUNCTION pad_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.summary=NEW.summary || jsonb_build_object('test_padding',repeat('x',900000)); RETURN NEW; END $$; CREATE TRIGGER pad_receipt BEFORE INSERT ON run_chunk_settlements FOR EACH ROW EXECUTE FUNCTION pad_receipt();")
        .execute(&pool).await.unwrap();
    let mut tx = pool.begin().await.unwrap();
    super::settle_terminal_chunks_with(&mut tx, run, 4)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(
        super::advance_shard_summary(&pool, run, 4, 1)
            .await
            .unwrap()
    );
    assert_eq!(
        select_run_shard_summary(&pool, run, 4)
            .await
            .unwrap()
            .unwrap()
            .settled_chunk_count,
        4
    );
    for _ in 0..4 {
        super::advance_shard_summary(&pool, run, 4, 1)
            .await
            .unwrap();
    }
    let summary = select_run_shard_summary(&pool, run, 4)
        .await
        .unwrap()
        .unwrap();
    assert!(summary.sealed);
    assert_eq!(summary.missing_execution_count, 6);
    assert_eq!(summary.settled_chunk_count, 6);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for cancellation settlement tests"]
async fn undispatched_cancellation_settles_without_promoting_unfinished_evidence(pool: PgPool) {
    let (run, dataset) = fixture(&pool, 2).await;
    let chunk = ranged_chunk(&pool, run, dataset, 0, 2, "pending").await;
    let execution = Uuid::now_v7();
    let attempt = Uuid::now_v7();
    seed_execution(&pool, run, 4, chunk, execution, attempt, "running").await;
    seed_aggregate(&pool, run, 4, execution, attempt, "passed").await;
    sqlx::query("UPDATE execution_attempts SET status='running' WHERE id=$1")
        .bind(attempt)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM run_snapshots WHERE run_id=$1")
        .bind(run)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE run_chunks SET dispatched_at=NULL WHERE run_id=$1")
        .bind(run)
        .execute(&pool)
        .await
        .unwrap();
    let cancelled = crate::db::workflows::run_cancel::cancel_run(&pool, run)
        .await
        .unwrap()
        .unwrap();
    assert!(cancelled.cancelled);
    assert_eq!(cancelled.chunks_cancelled, 1);
    assert_eq!(cancelled.run.passed_execution_count, 0);
    let aggregation = super::AggregationConfig {
        max_pages: 8,
        time_budget: std::time::Duration::from_secs(30),
        parallelism: 2,
    };
    assert_eq!(
        super::advance_pending_summaries(&pool, &aggregation)
            .await
            .unwrap(),
        2
    );
    let summary = select_run_shard_summary(&pool, run, 4)
        .await
        .unwrap()
        .unwrap();
    assert!(summary.sealed);
    assert_eq!(summary.missing_execution_count, 1);
    assert_eq!(summary.unresolved_execution_count, 1);
    assert_eq!(summary.aggregate_count, 0);
    let again = crate::db::workflows::run_cancel::cancel_run(&pool, run)
        .await
        .unwrap()
        .unwrap();
    assert!(again.already_cancelled);
    assert_eq!(again.chunks_cancelled, 0);
    assert_eq!(
        super::advance_pending_summaries(&pool, &aggregation)
            .await
            .unwrap(),
        0
    );
    let unchanged = select_run_shard_summary(&pool, run, 4)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(unchanged).unwrap(),
        serde_json::to_value(summary).unwrap()
    );
}
