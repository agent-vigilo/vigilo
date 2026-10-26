//! PostgreSQL contracts for bounded aggregation and ready-work scheduling.
//!
//! Fixtures exercise the public workflow boundary, including idle shards, claim
//! contention, and final folding after all progress receipts were consumed.

use std::time::Duration;

use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    test_support::*,
    *,
};

fn scheduling(max_pages: usize) -> AggregationConfig {
    AggregationConfig {
        max_pages,
        time_budget: Duration::from_secs(30),
        parallelism: 2,
    }
}

/// Joins the test-owned drain, cancelling it if a regression leaves it stuck.
async fn finish_drain(mut task: tokio::task::JoinHandle<anyhow::Result<usize>>) -> usize {
    match tokio::time::timeout(Duration::from_secs(10), &mut task).await {
        Ok(result) => result.unwrap().unwrap(),
        Err(error) => {
            task.abort();
            let _ = task.await;
            panic!("aggregation drain did not finish after releasing its test fence: {error}");
        }
    }
}

async fn initialized_chunk(pool: &PgPool, status: &str) -> (Uuid, Uuid) {
    let (run, dataset) = fixture(pool, 1).await;
    let chunk = ranged_chunk(pool, run, dataset, 0, 1, status).await;
    let mut tx = pool.begin().await.unwrap();
    if matches!(status, "failed" | "completed" | "cancelled") {
        settle_chunk_with(&mut tx, run, 4, chunk).await.unwrap();
    }
    initialize_run_shard_summary_with(&mut tx, run, 4)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    (run, chunk)
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn initialized_aggregation_has_bounded_query_cost(pool: PgPool) {
    let (run, _) = initialized_chunk(&pool, "failed").await;
    let mut query_counts = Vec::new();
    for progressed in [true, true, false] {
        let (result, queries) = record_queries(advance_shard_summary(&pool, run, 4, 1)).await;
        assert_eq!(result.unwrap(), progressed);
        assert!(
            !queries.is_empty(),
            "SQL recorder must observe the workflow"
        );
        query_counts.push(queries.len());
    }
    let summary = select_run_shard_summary(&pool, run, 4)
        .await
        .unwrap()
        .unwrap();
    assert!(summary.is_terminal());
    assert_eq!(summary.missing_execution_count, 1);
    assert_eq!(summary.failed_chunk_count, 1);
    for (actual, maximum) in query_counts.iter().zip([5, 7, 3]) {
        assert!(
            *actual <= maximum,
            "aggregation query counts: {query_counts:?}"
        );
    }
    eprintln!("progress/final/sealed aggregation SQL submissions: {query_counts:?}");
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn ready_work_skips_idle_shards_and_finishes_consumed_receipts(pool: PgPool) {
    let (idle, _) = initialized_chunk(&pool, "pending").await;
    let (ready, _) = initialized_chunk(&pool, "failed").await;
    sqlx::query("UPDATE run_shard_summaries SET updated_at='2000-01-01' WHERE run_id=$1")
        .bind(idle)
        .execute(&pool)
        .await
        .unwrap();

    assert_eq!(
        advance_pending_summaries(&pool, &scheduling(1))
            .await
            .unwrap(),
        1
    );
    let summary = select_run_shard_summary(&pool, ready, 4)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(summary.settled_chunk_count, 1);
    assert!(!summary.sealed);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM run_chunk_settlement_consumptions WHERE run_id=$1 AND NOT consumed")
            .bind(ready)
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        advance_pending_summaries(&pool, &scheduling(1))
            .await
            .unwrap(),
        1
    );
    assert!(
        select_run_shard_summary(&pool, ready, 4)
            .await
            .unwrap()
            .unwrap()
            .is_terminal()
    );
    let (idle_result, idle_queries) =
        record_queries(advance_pending_summaries(&pool, &scheduling(1))).await;
    assert_eq!(idle_result.unwrap(), 0);
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT updated_at='2000-01-01'::timestamptz AND aggregation_state IS NULL FROM run_shard_summaries WHERE run_id=$1")
            .bind(idle)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "idle shards must not perform scheduling writes or checkpoint work"
    );
    let idle_summary = select_run_shard_summary(&pool, idle, 4)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(idle_summary.planned_chunk_count, 1);
    assert!(!idle_summary.planned_chunk_hash.is_empty());
    assert_eq!(
        idle_queries.len(),
        1,
        "idle scheduling performs only discovery"
    );
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn unavailable_ready_shards_rotate_without_starving_progress(pool: PgPool) {
    let (busy, _) = initialized_chunk(&pool, "failed").await;
    let (invalid, _) = initialized_chunk(&pool, "failed").await;
    let (ready, _) = initialized_chunk(&pool, "failed").await;
    sqlx::query("UPDATE run_shard_summaries SET updated_at='2000-01-01' WHERE run_id=$1")
        .bind(busy)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE run_shard_summaries SET updated_at='2000-01-02' WHERE run_id=$1")
        .bind(invalid)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE run_snapshots SET config_snapshot=config_snapshot-'settlement_plan' WHERE run_id=$1")
        .bind(invalid)
        .execute(&pool)
        .await
        .unwrap();
    let mut claim = pool.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::uuid::text || ':summary', 4))")
        .bind(busy)
        .execute(&mut *claim)
        .await
        .unwrap();

    for _ in 0..3 {
        advance_pending_summaries(&pool, &scheduling(1))
            .await
            .unwrap();
    }
    assert_eq!(
        select_run_shard_summary(&pool, ready, 4)
            .await
            .unwrap()
            .unwrap()
            .settled_chunk_count,
        1
    );
    for run in [busy, invalid] {
        assert_eq!(
            select_run_shard_summary(&pool, run, 4)
                .await
                .unwrap()
                .unwrap()
                .settled_chunk_count,
            0
        );
    }
    claim.rollback().await.unwrap();
    for _ in 0..8 {
        advance_pending_summaries(&pool, &scheduling(1))
            .await
            .unwrap();
    }
    for run in [busy, ready] {
        assert!(
            select_run_shard_summary(&pool, run, 4)
                .await
                .unwrap()
                .unwrap()
                .is_terminal()
        );
    }
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn compact_receipts_use_bounded_256_row_pages_for_both_folds(pool: PgPool) {
    let (run, dataset) = fixture(&pool, 257).await;
    let ids = (0..257).map(|_| Uuid::now_v7()).collect::<Vec<_>>();
    sqlx::query(
        "INSERT INTO run_chunks (id,run_id,run_shard,dataset_version_id,profile_group_id,ordinal_start,ordinal_end,status,dispatched_at)
         SELECT id,$1,4,$3,'default',ordinal::int-1,ordinal::int,'failed',now()
         FROM unnest($2::uuid[]) WITH ORDINALITY AS chunks(id,ordinal)",
    )
    .bind(run)
    .bind(ids)
    .bind(dataset)
    .execute(&pool)
    .await
    .unwrap();
    freeze_plan(&pool, run, 4).await;
    let mut tx = pool.begin().await.unwrap();
    settle_terminal_chunks_with(&mut tx, run, 4).await.unwrap();
    initialize_run_shard_summary_with(&mut tx, run, 4)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    for expected in [256, 257] {
        assert!(advance_shard_summary(&pool, run, 4, 1).await.unwrap());
        let summary = select_run_shard_summary(&pool, run, 4)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(summary.settled_chunk_count, expected);
        assert_eq!(summary.failed_chunk_count, expected);
        assert_eq!(summary.missing_execution_count, expected);
        assert_eq!(summary.aggregate_count, 0);
        assert!(!summary.sealed);
    }
    assert!(advance_shard_summary(&pool, run, 4, 1).await.unwrap());
    assert!(
        !select_run_shard_summary(&pool, run, 4)
            .await
            .unwrap()
            .unwrap()
            .sealed,
        "canonical verification must also yield at its row limit"
    );
    assert!(advance_shard_summary(&pool, run, 4, 1).await.unwrap());
    let summary = select_run_shard_summary(&pool, run, 4)
        .await
        .unwrap()
        .unwrap();
    assert!(summary.is_terminal());
    assert_eq!(summary.settled_chunk_count, 257);
    assert_eq!(summary.failed_chunk_count, 257);
    assert_eq!(summary.missing_execution_count, 257);
    assert_eq!(summary.aggregate_count, 0);
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn gated_receipts_keep_64_row_pages_and_exact_missing_case_populations(pool: PgPool) {
    let (run, dataset) = fixture(&pool, 65).await;
    sqlx::query(
        "UPDATE run_snapshots SET config_snapshot=jsonb_set(config_snapshot,'{profile,scorecard,gates}',
         '[{\"id\":\"quality_release\",\"dimension\":\"quality\",\"case_group\":\"sentiment\",\"tags_all\":[\"release\"],\"min_mean_score\":0.9,\"min_coverage\":1.0}]'::jsonb)
         WHERE run_id=$1",
    )
    .bind(run)
    .execute(&pool)
    .await
    .unwrap();
    for ordinal in 0..65 {
        ranged_chunk(&pool, run, dataset, ordinal, ordinal + 1, "failed").await;
    }
    let mut tx = pool.begin().await.unwrap();
    settle_terminal_chunks_with(&mut tx, run, 4).await.unwrap();
    initialize_run_shard_summary_with(&mut tx, run, 4)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    for expected in [64, 65] {
        assert!(advance_shard_summary(&pool, run, 4, 1).await.unwrap());
        let summary = select_run_shard_summary(&pool, run, 4)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(summary.settled_chunk_count, expected);
        assert_eq!(summary.missing_execution_count, expected);
        assert_eq!(summary.scorecard["entries"][0]["expected_count"], json!(65));
        assert_eq!(summary.scorecard["entries"][0]["scored_count"], json!(0));
        assert!(!summary.sealed);
    }
    assert!(advance_shard_summary(&pool, run, 4, 1).await.unwrap());
    assert!(
        !select_run_shard_summary(&pool, run, 4)
            .await
            .unwrap()
            .unwrap()
            .sealed
    );
    assert!(advance_shard_summary(&pool, run, 4, 1).await.unwrap());
    let summary = select_run_shard_summary(&pool, run, 4)
        .await
        .unwrap()
        .unwrap();
    assert!(summary.is_terminal());
    assert_eq!(summary.missing_execution_count, 65);
    assert_eq!(summary.scorecard["entries"][0]["expected_count"], json!(65));
    assert_eq!(summary.scorecard["entries"][0]["scored_count"], json!(0));
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn aggregation_scheduling_accepts_draining_and_excludes_closed_shards(pool: PgPool) {
    let (draining, _) = initialized_chunk(&pool, "failed").await;
    let (closed, _) = initialized_chunk(&pool, "failed").await;
    sqlx::query("UPDATE local_shard_admissions SET state='draining' WHERE run_id=$1")
        .bind(draining)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE local_shard_admissions SET state='closed' WHERE run_id=$1")
        .bind(closed)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        advance_pending_summaries(&pool, &scheduling(8))
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        advance_pending_summaries(&pool, &scheduling(8))
            .await
            .unwrap(),
        0
    );
    assert!(
        select_run_shard_summary(&pool, draining, 4)
            .await
            .unwrap()
            .unwrap()
            .is_terminal()
    );
    assert_eq!(
        select_run_shard_summary(&pool, closed, 4)
            .await
            .unwrap()
            .unwrap()
            .settled_chunk_count,
        0
    );
    assert!(
        advance_shard_summary(&pool, closed, 4, 1)
            .await
            .unwrap_err()
            .to_string()
            .contains("admission")
    );
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn aggregation_page_budget_visits_ready_shards_before_revisiting_and_resumes(pool: PgPool) {
    let mut runs = Vec::new();
    for _ in 0..3 {
        runs.push(initialized_chunk(&pool, "failed").await.0);
    }

    assert_eq!(
        advance_pending_summaries(&pool, &scheduling(3))
            .await
            .unwrap(),
        3
    );
    for run in &runs {
        let summary = select_run_shard_summary(&pool, *run, 4)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(summary.settled_chunk_count, 1);
        assert_eq!(summary.missing_execution_count, 1);
        assert!(
            !summary.sealed,
            "every ready shard receives a turn before revisits"
        );
    }

    // A separate invocation resumes the committed checkpoints without needing
    // scheduler-local state, including the progress-to-canonical transition.
    assert_eq!(
        advance_pending_summaries(&pool, &scheduling(3))
            .await
            .unwrap(),
        3
    );
    for run in runs {
        let summary = select_run_shard_summary(&pool, run, 4)
            .await
            .unwrap()
            .unwrap();
        assert!(summary.is_terminal());
        assert_eq!(summary.settled_chunk_count, 1);
        assert_eq!(summary.failed_chunk_count, 1);
        assert_eq!(summary.missing_execution_count, 1);
    }
    assert_eq!(
        advance_pending_summaries(&pool, &scheduling(3))
            .await
            .unwrap(),
        0
    );
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn aggregation_progresses_other_shards_while_one_waits_for_movement(pool: PgPool) {
    let (blocked, _) = initialized_chunk(&pool, "failed").await;
    let (healthy, _) = initialized_chunk(&pool, "failed").await;
    sqlx::query("UPDATE run_shard_summaries SET updated_at='2000-01-01' WHERE run_id=$1")
        .bind(blocked)
        .execute(&pool)
        .await
        .unwrap();
    let mut movement = pool.begin().await.unwrap();
    crate::db::shard_write_fence::lock_exclusive(&mut movement, blocked, 4)
        .await
        .unwrap();
    let worker_pool = pool.clone();
    let drain =
        tokio::spawn(async move { advance_pending_summaries(&worker_pool, &scheduling(8)).await });

    let observed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if select_run_shard_summary(&pool, healthy, 4)
                .await
                .unwrap()
                .unwrap()
                .is_terminal()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    // Always release the held fence and join the task before asserting so a
    // regression cannot leave a detached operation behind the test fixture.
    movement.rollback().await.unwrap();
    let pages = finish_drain(drain).await;
    assert!(
        observed.is_ok(),
        "healthy shard stalled behind an unrelated movement fence"
    );
    assert_eq!(pages, 4);
    for run in [blocked, healthy] {
        let summary = select_run_shard_summary(&pool, run, 4)
            .await
            .unwrap()
            .unwrap();
        assert!(summary.is_terminal());
        assert_eq!(summary.missing_execution_count, 1);
    }
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn concurrent_aggregation_drains_publish_each_receipt_once(pool: PgPool) {
    let mut runs = Vec::new();
    for _ in 0..3 {
        runs.push(initialized_chunk(&pool, "failed").await.0);
    }
    let config = scheduling(12);
    let (left, right) = tokio::join!(
        advance_pending_summaries(&pool, &config),
        advance_pending_summaries(&pool, &config),
    );
    assert_eq!(left.unwrap() + right.unwrap(), 6);
    assert_eq!(advance_pending_summaries(&pool, &config).await.unwrap(), 0);
    for run in runs {
        let summary = select_run_shard_summary(&pool, run, 4)
            .await
            .unwrap()
            .unwrap();
        assert!(summary.is_terminal());
        assert_eq!(summary.settled_chunk_count, 1);
        assert_eq!(summary.failed_chunk_count, 1);
        assert_eq!(summary.missing_execution_count, 1);
    }
    let (receipts, consumed): (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM run_chunk_settlements),
                (SELECT count(*) FROM run_chunk_settlement_consumptions WHERE consumed)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((receipts, consumed), (3, 3));
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn slow_discovery_does_not_exhaust_the_page_scheduling_window(pool: PgPool) {
    let (run, _) = initialized_chunk(&pool, "failed").await;
    let mut discovery_lock = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE run_shard_summaries IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *discovery_lock)
        .await
        .unwrap();
    let worker_pool = pool.clone();
    let drain = tokio::spawn(async move {
        let mut config = scheduling(1);
        config.time_budget = Duration::from_millis(100);
        advance_pending_summaries(&worker_pool, &config).await
    });
    let waiting = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                 WHERE datname=current_database() AND pid<>pg_backend_pid()
                 AND wait_event_type='Lock'
                 AND query LIKE '%SELECT s.run_id,s.run_shard,a.write_epoch%')",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            if blocked {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    // Establish that discovery has started before exceeding the page window.
    // This tests liveness under waiting, not database timing performance.
    if waiting.is_ok() {
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    discovery_lock.rollback().await.unwrap();
    let pages = finish_drain(drain).await;
    assert!(
        waiting.is_ok(),
        "discovery did not reach the held table lock"
    );
    assert_eq!(
        pages, 1,
        "a successful slow discovery must still admit ready work"
    );
    assert_eq!(
        select_run_shard_summary(&pool, run, 4)
            .await
            .unwrap()
            .unwrap()
            .settled_chunk_count,
        1
    );
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn aggregation_does_not_probe_again_when_available_progress_is_consumed(pool: PgPool) {
    for ready_count in [1, 256] {
        let (run, dataset) = fixture(&pool, ready_count + 1).await;
        let ids = (0..=ready_count)
            .map(|_| Uuid::now_v7())
            .collect::<Vec<_>>();
        sqlx::query(
            "INSERT INTO run_chunks (id,run_id,run_shard,dataset_version_id,profile_group_id,ordinal_start,ordinal_end,status,dispatched_at)
             SELECT id,$1,4,$3,'default',ordinal::int-1,ordinal::int,
                    CASE WHEN ordinal <= $4 THEN 'failed' ELSE 'pending' END,now()
             FROM unnest($2::uuid[]) WITH ORDINALITY AS chunks(id,ordinal)",
        ).bind(run).bind(ids).bind(dataset).bind(i64::from(ready_count)).execute(&pool).await.unwrap();
        freeze_plan(&pool, run, 4).await;
        let mut tx = pool.begin().await.unwrap();
        settle_terminal_chunks_with(&mut tx, run, 4).await.unwrap();
        initialize_run_shard_summary_with(&mut tx, run, 4)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let (result, queries) =
            record_queries(advance_pending_summaries(&pool, &scheduling(8))).await;
        assert_eq!(result.unwrap(), 1);
        let summary = select_run_shard_summary(&pool, run, 4)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(summary.settled_chunk_count, ready_count);
        assert_eq!(summary.missing_execution_count, ready_count);
        assert!(!summary.sealed);
        assert_eq!(
            queries.len(),
            6,
            "discovery plus one productive page must not add an empty-page probe (ready_count={ready_count}): {queries:?}"
        );
    }
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn aggregation_drains_receipts_across_byte_limited_pages(pool: PgPool) {
    let (run, dataset) = fixture(&pool, 6).await;
    for ordinal in 0..6 {
        ranged_chunk(&pool, run, dataset, ordinal, ordinal + 1, "failed").await;
    }
    // Padding creates a deterministic byte-boundary fixture without changing
    // the scorecard or introducing unfinished execution evidence.
    sqlx::raw_sql("CREATE FUNCTION pad_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.summary=NEW.summary || jsonb_build_object('test_padding',repeat('x',900000)); RETURN NEW; END $$; CREATE TRIGGER pad_receipt BEFORE INSERT ON run_chunk_settlements FOR EACH ROW EXECUTE FUNCTION pad_receipt();")
        .execute(&pool).await.unwrap();
    let mut tx = pool.begin().await.unwrap();
    settle_terminal_chunks_with(&mut tx, run, 4).await.unwrap();
    initialize_run_shard_summary_with(&mut tx, run, 4)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    assert_eq!(
        advance_pending_summaries(&pool, &scheduling(8))
            .await
            .unwrap(),
        4
    );
    let summary = select_run_shard_summary(&pool, run, 4)
        .await
        .unwrap()
        .unwrap();
    assert!(summary.is_terminal());
    assert_eq!(summary.settled_chunk_count, 6);
    assert_eq!(summary.missing_execution_count, 6);
    assert_eq!(summary.failed_chunk_count, 6);
    assert_eq!(
        advance_pending_summaries(&pool, &scheduling(8))
            .await
            .unwrap(),
        0
    );
}
