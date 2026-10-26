//! PostgreSQL contracts for worker completion and immutable settlement evidence.

use std::time::Duration;

use serde_json::{
    Value,
    json,
};
use sqlx::PgPool;
use uuid::Uuid;

use super::super::test_support::*;
use crate::{
    db::workflows::chunk_processing::{
        mark_chunk_completed_and_settle,
        mark_chunk_failed_and_settle,
    },
    models::run_chunk::RunChunk,
};

async fn lease(pool: &PgPool, chunk_id: Uuid) -> RunChunk {
    let mut chunk = sqlx::query_as::<_, RunChunk>(
        "UPDATE run_chunks SET status='leased',lease_token=gen_random_uuid(),leased_until=now()+interval '1 hour' WHERE id=$1 RETURNING *",
    ).bind(chunk_id).fetch_one(pool).await.unwrap();
    chunk.write_epoch = 1;
    chunk
}

async fn receipt(pool: &PgPool, run: Uuid, chunk: Uuid) -> Value {
    sqlx::query_scalar(
        "SELECT summary FROM run_chunk_settlements WHERE run_id=$1 AND run_shard=4 AND chunk_id=$2",
    )
    .bind(run)
    .bind(chunk)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn settle(pool: &PgPool, chunk: &RunChunk, failed: bool) -> anyhow::Result<u64> {
    if failed {
        mark_chunk_failed_and_settle(pool, chunk).await
    } else {
        mark_chunk_completed_and_settle(pool, chunk).await
    }
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn worker_settlement_preserves_mixed_outcomes_and_stored_chunk_identity(pool: PgPool) {
    for (failed, gates) in [(false, false), (true, false), (false, true), (true, true)] {
        let (run, dataset) = fixture(&pool, 7).await;
        let chunk_id = ranged_chunk(&pool, run, dataset, 0, 7, "pending").await;
        for (status, attempt_status) in [
            ("completed", "completed"),
            ("failed", "failed_evaluation"),
            ("pending", "pending"),
            ("running", "running"),
            ("awaiting_evaluators", "running"),
            ("retry_scheduled", "pending"),
        ] {
            let execution = Uuid::now_v7();
            let attempt = Uuid::now_v7();
            seed_execution(&pool, run, 4, chunk_id, execution, attempt, status).await;
            seed_aggregate(
                &pool,
                run,
                4,
                execution,
                attempt,
                if status == "failed" {
                    "failed"
                } else {
                    "passed"
                },
            )
            .await;
            sqlx::query("UPDATE execution_attempts SET status=$2::attempt_status WHERE id=$1")
                .bind(attempt)
                .bind(attempt_status)
                .execute(&pool)
                .await
                .unwrap();
        }
        sqlx::query("UPDATE execution_aggregates SET aggregate_score=0.5,dimension_scores='{\"quality\":0.5}' WHERE run_id=$1")
            .bind(run).execute(&pool).await.unwrap();
        let entries = if gates {
            sqlx::query(
                "UPDATE run_snapshots SET config_snapshot=config_snapshot || $2 WHERE run_id=$1",
            )
            .bind(run)
            .bind(profile_snapshot(json!([
                {"id":"quality", "dimension":"quality", "min_coverage":1.0, "score_threshold":0.5}
            ])))
            .execute(&pool)
            .await
            .unwrap();
            freeze_plan(&pool, run, 4).await;
            json!([{
                "id":"quality", "dimension":"quality", "binding_id":null,
                "case_group":null, "tags_all":[], "min_mean_score":null,
                "score_threshold":0.5, "min_pass_rate":null, "min_coverage":1,
                "max_error_rate":null, "max_abstention_rate":null,
                "expected_count":6, "scored_count":2, "passed_count":2,
                "error_count":0, "abstained_count":0, "score_sum":1,
                "min_score":0.5, "max_score":0.5
            }])
        } else {
            json!([])
        };
        let mut chunk = lease(&pool, chunk_id).await;
        // Persisted identity is authoritative even if the caller retains stale
        // descriptive fields; only its identity and lease authorize completion.
        chunk.ordinal_start = 100;
        chunk.ordinal_end = 101;
        chunk.status = "pending".into();
        let (completed, statements) = record_queries(settle(&pool, &chunk, failed)).await;
        assert_eq!(completed.unwrap(), 1);
        let expected = json!({
            "run_id": run, "run_shard": 4, "expected_execution_count": 7,
            "execution_count": 6, "terminal_execution_count": 6,
            "aggregate_count": 2, "passed_execution_count": 1,
            "failed_execution_count": 1, "errored_execution_count": 0,
            "skipped_execution_count": 0, "missing_aggregate_count": 4,
            "evaluator_result_count": 2, "blocking_failure_count": 0,
            "score_count": 2, "score_sum": 1, "min_score": 0.5, "max_score": 0.5,
            "scorecard": {"version": 1, "run_shard": 4, "policy_hash": "aggregation-hash", "entries": entries},
            "failed_chunk_count": i32::from(failed), "cancelled_chunk_count": 0,
            "status": "failed",
            "missing_execution_count": 1, "unresolved_execution_count": 4
        });
        assert_eq!(receipt(&pool, run, chunk_id).await, expected);
        assert_eq!(settle(&pool, &chunk, failed).await.unwrap(), 0);
        assert_eq!(receipt(&pool, run, chunk_id).await, expected);
        let attempts: Vec<String> = sqlx::query_scalar(
            "SELECT status::text FROM execution_attempts WHERE run_id=$1 ORDER BY execution_id",
        )
        .bind(run)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            attempts.iter().filter(|status| *status == "stale").count(),
            4
        );
        assert_eq!(
            attempts
                .iter()
                .filter(|status| *status == "completed")
                .count(),
            1
        );
        assert_eq!(
            attempts
                .iter()
                .filter(|status| *status == "failed_evaluation")
                .count(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM execution_aggregates WHERE run_id=$1"
            )
            .bind(run)
            .fetch_one(&pool)
            .await
            .unwrap(),
            6
        );
        assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM run_chunk_settlement_consumptions WHERE run_id=$1 AND NOT consumed")
            .bind(run).fetch_one(&pool).await.unwrap(), 1);
        let limit = if gates { 7 } else { 6 };
        println!(
            "worker settlement failed={failed}, gates={gates}: {} SQL submissions",
            statements.len()
        );
        assert!(
            !statements.is_empty() && statements.len() <= limit,
            "worker settlement SQL submissions: {}",
            statements.len()
        );
    }
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn worker_settlement_observes_a_writer_committed_while_waiting_for_chunk(pool: PgPool) {
    let (run, dataset) = fixture(&pool, 1).await;
    let chunk_id = ranged_chunk(&pool, run, dataset, 0, 1, "pending").await;
    let execution = Uuid::now_v7();
    let attempt = Uuid::now_v7();
    seed_execution(&pool, run, 4, chunk_id, execution, attempt, "running").await;
    seed_aggregate(&pool, run, 4, execution, attempt, "passed").await;
    let chunk = lease(&pool, chunk_id).await;
    let mut writer = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM run_chunks WHERE run_id=$1 AND run_shard=4 AND id=$2 FOR UPDATE")
        .bind(run)
        .bind(chunk_id)
        .execute(&mut *writer)
        .await
        .unwrap();
    let completion_pool = pool.clone();
    let mut completion =
        tokio::spawn(
            async move { mark_chunk_completed_and_settle(&completion_pool, &chunk).await },
        );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut completion)
            .await
            .is_err()
    );
    sqlx::query("UPDATE executions SET status='completed' WHERE run_id=$1 AND id=$2")
        .bind(run)
        .bind(execution)
        .execute(&mut *writer)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE execution_aggregates SET aggregate_score=0.75 WHERE run_id=$1 AND execution_id=$2",
    )
    .bind(run)
    .bind(execution)
    .execute(&mut *writer)
    .await
    .unwrap();
    writer.commit().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), completion)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        1
    );
    let result = receipt(&pool, run, chunk_id).await;
    assert_eq!(result["aggregate_count"], 1);
    assert_eq!(result["passed_execution_count"], 1);
    assert_eq!(result["score_sum"], 0.75);
    assert_eq!(result["unresolved_execution_count"], 0);
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn settlement_replay_checks_policy_and_preserves_evidence(pool: PgPool) {
    let (run, dataset) = fixture(&pool, 1).await;
    let chunk_id = ranged_chunk(&pool, run, dataset, 0, 1, "pending").await;
    let chunk = lease(&pool, chunk_id).await;
    mark_chunk_completed_and_settle(&pool, &chunk)
        .await
        .unwrap();
    let expected = receipt(&pool, run, chunk_id).await;
    let mut replay = pool.begin().await.unwrap();
    super::settle_chunk_with(&mut replay, run, 4, chunk_id)
        .await
        .unwrap();
    replay.commit().await.unwrap();
    assert_eq!(receipt(&pool, run, chunk_id).await, expected);
    sqlx::query("UPDATE run_snapshots SET aggregation_policy_hash='conflicting-policy' WHERE run_id=$1 AND run_shard=4")
        .bind(run).execute(&pool).await.unwrap();
    let mut conflict = pool.begin().await.unwrap();
    let error = super::settle_chunk_with(&mut conflict, run, 4, chunk_id)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("conflicting chunk settlement"));
    conflict.rollback().await.unwrap();
    assert_eq!(receipt(&pool, run, chunk_id).await, expected);
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn worker_settlement_rejects_lost_lease_and_placement_authority(pool: PgPool) {
    for failed in [false, true] {
        for invalid in ["token", "expiry", "epoch", "admission"] {
            let (run, dataset) = fixture(&pool, 1).await;
            let chunk_id = ranged_chunk(&pool, run, dataset, 0, 1, "pending").await;
            let mut chunk = lease(&pool, chunk_id).await;
            match invalid {
                "token" => chunk.lease_token = Some(Uuid::now_v7()),
                "epoch" => chunk.write_epoch += 1,
                "expiry" => {
                    sqlx::query(
                        "UPDATE run_chunks SET leased_until=now()-interval '1 second' WHERE id=$1",
                    )
                    .bind(chunk_id)
                    .execute(&pool)
                    .await
                    .unwrap();
                }
                "admission" => {
                    sqlx::query("UPDATE local_shard_admissions SET state='closed' WHERE run_id=$1 AND run_shard=4")
                        .bind(run).execute(&pool).await.unwrap();
                }
                _ => unreachable!(),
            }
            assert_eq!(settle(&pool, &chunk, failed).await.unwrap(), 0, "{invalid}");
            assert_eq!(
                sqlx::query_scalar::<_, String>(
                    "SELECT status FROM run_chunks WHERE run_id=$1 AND id=$2"
                )
                .bind(run)
                .bind(chunk_id)
                .fetch_one(&pool)
                .await
                .unwrap(),
                "leased"
            );
            assert_eq!(
                sqlx::query_scalar::<_, i64>(
                    "SELECT count(*) FROM run_chunk_settlements WHERE run_id=$1"
                )
                .bind(run)
                .fetch_one(&pool)
                .await
                .unwrap(),
                0
            );
        }
    }
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn worker_settlement_rolls_back_when_local_snapshot_is_missing(pool: PgPool) {
    for failed in [false, true] {
        let (run, dataset) = fixture(&pool, 1).await;
        let chunk_id = ranged_chunk(&pool, run, dataset, 0, 1, "pending").await;
        let chunk = lease(&pool, chunk_id).await;
        sqlx::query("DELETE FROM run_snapshots WHERE run_id=$1 AND run_shard=4")
            .bind(run)
            .execute(&pool)
            .await
            .unwrap();
        assert!(settle(&pool, &chunk, failed).await.is_err());
        let (status, token): (String, Option<Uuid>) =
            sqlx::query_as("SELECT status,lease_token FROM run_chunks WHERE run_id=$1 AND id=$2")
                .bind(run)
                .bind(chunk_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "leased");
        assert_eq!(token, chunk.lease_token);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM run_chunk_settlements WHERE run_id=$1"
            )
            .bind(run)
            .fetch_one(&pool)
            .await
            .unwrap(),
            0
        );
    }
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn missing_gate_counts_roll_back_worker_and_attempt_retirement(pool: PgPool) {
    let (run, dataset) = fixture(&pool, 2).await;
    let chunk_id = ranged_chunk(&pool, run, dataset, 0, 2, "pending").await;
    let execution = Uuid::now_v7();
    let attempt = Uuid::now_v7();
    seed_execution(&pool, run, 4, chunk_id, execution, attempt, "running").await;
    seed_aggregate(&pool, run, 4, execution, attempt, "passed").await;
    sqlx::query("UPDATE execution_attempts SET status='running' WHERE id=$1")
        .bind(attempt)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE run_snapshots SET config_snapshot=config_snapshot || $2 WHERE run_id=$1")
        .bind(run)
        .bind(profile_snapshot(json!([
            {"id":"quality", "dimension":"quality", "min_coverage":1.0}
        ])))
        .execute(&pool)
        .await
        .unwrap();
    let chunk = lease(&pool, chunk_id).await;
    let error = mark_chunk_failed_and_settle(&pool, &chunk)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("incomplete planned gate counts"));
    let states: (String, String, String, Option<Uuid>) = sqlx::query_as(
        "SELECT e.status::text,a.status::text,c.status,c.lease_token FROM executions e JOIN execution_attempts a ON a.run_id=e.run_id AND a.run_shard=e.run_shard AND a.id=e.current_attempt_id JOIN run_chunks c ON c.run_id=e.run_id AND c.run_shard=e.run_shard AND c.id=e.chunk_id WHERE e.run_id=$1 AND e.id=$2",
    ).bind(run).bind(execution).fetch_one(&pool).await.unwrap();
    assert_eq!(
        states,
        (
            "running".into(),
            "running".into(),
            "leased".into(),
            chunk.lease_token
        )
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM execution_aggregates WHERE run_id=$1")
            .bind(run)
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
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
