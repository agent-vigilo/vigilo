//! PostgreSQL access-work regression for fixed-size chunk receipt reads.
//!
//! Uses the production receipt query with larger surrounding run histories.
//! Exact receipt outcomes are checked before EXPLAIN measures execution-row
//! visits. No elapsed-time budget, index name, or specific scan type is required.

use serde_json::{
    Value,
    json,
};
use sqlx::PgPool;
use uuid::Uuid;

use super::test_support::fixture;

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL for chunk receipt access-work regression"]
async fn receipt_execution_reads_stay_within_the_chunk_as_run_history_grows(pool: PgPool) {
    let mut observations = Vec::new();
    for cases in [10_000_i32, 50_000] {
        let (run, dataset) = fixture(&pool, cases).await;
        seed_completed_history(&pool, run, dataset, cases).await;
        // Use the naturally chosen plan with current statistics, including the
        // leaf partitions whose statistics drive partition-pruned queries.
        for table in [
            "run_chunks",
            "executions",
            "execution_attempts",
            "execution_aggregates",
        ] {
            sqlx::query(&format!("ANALYZE {table}"))
                .execute(&pool)
                .await
                .unwrap();
        }
        for ordinal in [0, cases / 2, cases - 100] {
            let chunk: Uuid = sqlx::query_scalar(
                "SELECT id FROM run_chunks WHERE run_id=$1 AND run_shard=4 AND ordinal_start=$2",
            )
            .bind(run)
            .bind(ordinal)
            .fetch_one(&pool)
            .await
            .unwrap();
            let receipt: Value = sqlx::query_scalar(include_str!("receipt.sql"))
                .bind(run)
                .bind(4_i16)
                .bind(chunk)
                .bind(json!([]))
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(
                receipt,
                json!({
                    "run_id": run, "run_shard": 4, "expected_execution_count": 100,
                    "execution_count": 100, "terminal_execution_count": 100,
                    "aggregate_count": 100, "passed_execution_count": 100,
                    "failed_execution_count": 0, "errored_execution_count": 0,
                    "skipped_execution_count": 0, "missing_aggregate_count": 0,
                    "missing_execution_count": 0, "unresolved_execution_count": 0,
                    "evaluator_result_count": 100, "blocking_failure_count": 0,
                    "score_count": 100, "score_sum": 50, "min_score": 0.5, "max_score": 0.5,
                    "scorecard": {"version": 1, "run_shard": 4, "policy_hash": "aggregation-hash", "entries": []},
                    "failed_chunk_count": 0, "cancelled_chunk_count": 0, "status": "completed"
                })
            );
            let explained: Value = sqlx::query_scalar(&format!(
                "EXPLAIN (ANALYZE, BUFFERS, TIMING FALSE, FORMAT JSON) {}",
                include_str!("receipt.sql")
            ))
            .bind(run)
            .bind(4_i16)
            .bind(chunk)
            .bind(json!([]))
            .fetch_one(&pool)
            .await
            .unwrap();
            let visits = execution_rows_examined(&explained[0]["Plan"]);
            println!(
                "chunk access: shard_cases={cases}, chunk_start={ordinal}, chunk_cases=100, execution_rows_examined={visits}"
            );
            observations.push((cases, ordinal, visits));
        }
    }
    // Row visits are an access-work contract, not a wall-clock performance claim.
    // Allow rechecks while rejecting scans of unrelated execution history.
    for (cases, ordinal, visits) in observations {
        assert!(
            (100.0..=200.0).contains(&visits),
            "100-case receipt examined {visits} execution rows at ordinal {ordinal} in a {cases}-case shard"
        );
    }
}

fn execution_rows_examined(plan: &Value) -> f64 {
    let local = if plan["Relation Name"]
        .as_str()
        .is_some_and(|name| name == "executions" || name.starts_with("executions_p"))
    {
        [
            "Actual Rows",
            "Rows Removed by Filter",
            "Rows Removed by Index Recheck",
        ]
        .into_iter()
        .map(|key| plan[key].as_f64().unwrap_or(0.0))
        .sum::<f64>()
            * plan["Actual Loops"].as_f64().unwrap_or(0.0)
    } else {
        0.0
    };
    local
        + plan["Plans"].as_array().map_or(0.0, |children| {
            children.iter().map(execution_rows_examined).sum()
        })
}

/// Seeds fixed-size chunks with one completed authoritative result per case.
/// SQLx owns the isolated database and removes it after a successful test.
async fn seed_completed_history(pool: &PgPool, run: Uuid, dataset: Uuid, cases: i32) {
    sqlx::query("INSERT INTO run_chunks (id,run_id,run_shard,dataset_version_id,profile_group_id,ordinal_start,ordinal_end,status) SELECT md5('chunk-' || n)::uuid,$1,4,$2,'default',n,n+100,'completed' FROM generate_series(0,$3-100,100) n")
        .bind(run).bind(dataset).bind(cases).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO executions (id,run_id,run_shard,chunk_id,case_id,case_hash,profile_group_id,task_type,evaluation_profile_id,evaluation_profile_version,expected_evaluator_count,status,current_attempt_id,current_attempt_no) SELECT md5('execution-' || n)::uuid,$1,4,md5('chunk-' || (n/100*100))::uuid,md5('case-' || n)::uuid,'case-' || n,'default','classification','profile','1.0.0',1,'completed',md5('attempt-' || n)::uuid,1 FROM generate_series(0,$2-1) n")
        .bind(run).bind(cases).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO execution_attempts (id,run_id,run_shard,execution_id,attempt_no,status,completed_at) SELECT current_attempt_id,run_id,run_shard,id,1,'completed',now() FROM executions WHERE run_id=$1 AND run_shard=4")
        .bind(run).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO execution_aggregates (execution_id,run_id,run_shard,attempt_id,overall_status,evaluator_result_count,aggregate_score) SELECT id,run_id,run_shard,current_attempt_id,'passed',1,0.5 FROM executions WHERE run_id=$1 AND run_shard=4")
        .bind(run).execute(pool).await.unwrap();
}
