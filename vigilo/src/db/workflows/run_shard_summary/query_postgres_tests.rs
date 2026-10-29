//! PostgreSQL contracts for isolated batched reads and atomic checkpoint writes.

use serde_json::{
    Value,
    json,
};
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    test_support::*,
    *,
};

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn batched_summary_reads_preserve_run_and_requested_shard_isolation(pool: PgPool) {
    let (run, dataset) = fixture(&pool, 11).await;
    let dataset_id: Uuid = sqlx::query_scalar("SELECT dataset_id FROM runs WHERE id=$1")
        .bind(run)
        .fetch_one(&pool)
        .await
        .unwrap();
    for (shard, expected) in [(0, 3), (4, 11), (7, 5), (127, 2)] {
        if shard != 4 {
            seed_run_snapshot(&pool, run, shard, dataset_id, dataset, expected).await;
        }
        let mut tx = pool.begin().await.unwrap();
        initialize_run_shard_summary_with(&mut tx, run, shard)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    let (other_run, _) = fixture(&pool, 99).await;
    let mut tx = pool.begin().await.unwrap();
    initialize_run_shard_summary_with(&mut tx, other_run, 4)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Includes both shard boundaries, a duplicate, an absent shard, and a shard
    // present on another run. Unrequested shard 7 must not enter the result.
    let (result, statements) =
        record_queries(select_run_shard_summaries(&pool, run, &[127, 0, 4, 0, 9])).await;
    let expected = [(0, 3), (4, 11), (127, 2)].map(|(run_shard, expected_execution_count)| {
        RunShardSummary {
            run_id: run,
            run_shard,
            expected_execution_count,
            status: "running".into(),
            scorecard: json!({"version":1,"run_shard":run_shard,"policy_hash":"aggregation-hash","entries":[]}),
            ..Default::default()
        }
    });
    assert_eq!(
        serde_json::to_value(result.unwrap()).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
    assert_eq!(statements.len(), 1);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn empty_and_invalid_summary_reads_do_not_submit_sql(pool: PgPool) {
    let run = Uuid::now_v7();
    let (empty, statements) = record_queries(select_run_shard_summaries(&pool, run, &[])).await;
    assert!(empty.unwrap().is_empty());
    assert!(statements.is_empty());
    for shards in [vec![-1], vec![128], vec![0; 129]] {
        let (invalid, statements) =
            record_queries(select_run_shard_summaries(&pool, run, &shards)).await;
        assert!(
            invalid
                .unwrap_err()
                .to_string()
                .contains("supported logical shard range")
        );
        assert!(statements.is_empty());
    }
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn missing_checkpoint_update_rolls_back_receipt_consumption(pool: PgPool) {
    let (run, dataset) = fixture(&pool, 1).await;
    let chunk = ranged_chunk(&pool, run, dataset, 0, 1, "failed").await;
    let mut tx = pool.begin().await.unwrap();
    settle_chunk_with(&mut tx, run, 4, chunk).await.unwrap();
    initialize_run_shard_summary_with(&mut tx, run, 4)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let before: Value = sqlx::query_scalar(
        "SELECT to_jsonb(s) FROM run_shard_summaries s WHERE run_id=$1 AND run_shard=4",
    )
    .bind(run)
    .fetch_one(&pool)
    .await
    .unwrap();
    sqlx::raw_sql(
        "CREATE FUNCTION skip_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;
         CREATE TRIGGER skip_checkpoint BEFORE UPDATE ON run_shard_summaries FOR EACH ROW EXECUTE FUNCTION skip_checkpoint();",
    ).execute(&pool).await.unwrap();

    let error = advance_shard_summary(&pool, run, 4, 1).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("checkpoint lost its unsealed row")
    );
    let after: Value = sqlx::query_scalar(
        "SELECT to_jsonb(s) FROM run_shard_summaries s WHERE run_id=$1 AND run_shard=4",
    )
    .bind(run)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(after, before);
    assert!(!sqlx::query_scalar::<_, bool>("SELECT consumed FROM run_chunk_settlement_consumptions WHERE run_id=$1 AND run_shard=4 AND chunk_id=$2")
        .bind(run).bind(chunk).fetch_one(&pool).await.unwrap());

    sqlx::query("DROP TRIGGER skip_checkpoint ON run_shard_summaries")
        .execute(&pool)
        .await
        .unwrap();
    assert!(advance_shard_summary(&pool, run, 4, 1).await.unwrap());
    assert!(advance_shard_summary(&pool, run, 4, 1).await.unwrap());
    let summary = select_run_shard_summary(&pool, run, 4)
        .await
        .unwrap()
        .unwrap();
    assert!(summary.is_terminal());
    assert_eq!(summary.settled_chunk_count, 1);
    assert_eq!(summary.missing_execution_count, 1);
    assert_eq!(summary.failed_chunk_count, 1);
}
