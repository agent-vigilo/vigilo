//! Bounded reads, initialization, and checkpoints for shard-local run summaries.
//!
//! Read callers request a run's routed shards in one database batch without
//! advancing aggregation. Dispatch prepares summaries from frozen snapshots;
//! cleanup can initialize missing local context before first dispatch. Receipt
//! consumption and its summary checkpoint commit together under the caller's
//! aggregation claim, and published seals cannot be overwritten.

#[cfg(test)]
use sqlx::PgPool;
use sqlx::{
    Executor,
    Postgres,
};
use uuid::Uuid;

use super::RunShardSummary;

/// Reads only the requested logical shards from one execution database.
/// A run has at most 128 logical shards, bounding each routed summary read.
pub(crate) async fn select_run_shard_summaries<'e, E>(
    db: E,
    run_id: Uuid,
    run_shards: &[i16],
) -> anyhow::Result<Vec<RunShardSummary>>
where
    E: Executor<'e, Database = Postgres>,
{
    anyhow::ensure!(
        run_shards.len() <= 128 && run_shards.iter().all(|shard| (0..128).contains(shard)),
        "summary read exceeds the supported logical shard range"
    );
    if run_shards.is_empty() {
        return Ok(Vec::new());
    }
    let summaries = sqlx::query_as::<_, RunShardSummary>(
        r#"
        SELECT
            run_id,
            run_shard,
            expected_execution_count,
            execution_count,
            terminal_execution_count,
            aggregate_count,
            passed_execution_count,
            failed_execution_count,
            errored_execution_count,
            skipped_execution_count,
            missing_aggregate_count,
            evaluator_result_count,
            blocking_failure_count,
            score_count,
            score_sum,
            min_score,
            max_score,
            scorecard,
            failed_chunk_count,
            cancelled_chunk_count,
            status, sealed, planned_chunk_hash, planned_chunk_count, settled_chunk_count,
            missing_execution_count, unresolved_execution_count
        FROM run_shard_summaries
        WHERE run_id = $1::uuid
          AND run_shard = ANY($2::smallint[])
        ORDER BY run_shard
        "#,
    )
    .bind(run_id)
    .bind(run_shards)
    .fetch_all(db)
    .await?;

    Ok(summaries)
}

/// Reads the latest published progress or sealed summary without doing work.
#[cfg(test)]
pub(crate) async fn select_run_shard_summary(
    db: &PgPool,
    run_id: Uuid,
    run_shard: i16,
) -> anyhow::Result<Option<RunShardSummary>> {
    Ok(select_run_shard_summaries(db, run_id, &[run_shard])
        .await?
        .pop())
}

/// Freezes local run context for cleanup before first dispatch.
/// Existing snapshots remain the authority and are never replaced.
pub(super) async fn ensure_snapshot_with(
    db: &mut sqlx::PgConnection,
    run_id: Uuid,
    shard: i16,
) -> anyhow::Result<()> {
    sqlx::query(r#"
        INSERT INTO run_snapshots (run_id, run_shard, run_key, dataset_id, dataset_version_id, dataset_version, evaluation_profile_id, evaluation_profile_version, profile_version_id, profile_hash, aggregation_policy_id, aggregation_policy_version, aggregation_policy_hash, agent_provider, agent_name, agent_version, prompt_config_id, prompt_config_version, config_snapshot, expected_execution_count)
        SELECT r.id, $2, r.run_key, r.dataset_id, r.dataset_version_id, r.dataset_version, r.evaluation_profile_id, r.evaluation_profile_version, r.profile_version_id, r.profile_hash, r.aggregation_policy_id, r.aggregation_policy_version, r.aggregation_policy_hash, r.agent_provider, r.agent_name, r.agent_version, r.prompt_config_id, r.prompt_config_version, r.config_snapshot,
            (SELECT COALESCE(SUM(c.ordinal_end - c.ordinal_start), 0)::int
             FROM run_chunks c WHERE c.run_id = r.id AND c.run_shard = $2)
        FROM runs r WHERE r.id = $1
          AND NOT EXISTS (SELECT 1 FROM run_snapshots s WHERE s.run_id = $1 AND s.run_shard = $2)
        ON CONFLICT (run_id, run_shard) DO NOTHING
    "#).bind(run_id).bind(shard).execute(db).await?;
    Ok(())
}

/// Prepares an empty summary once; dispatch never overwrites aggregation progress.
pub(crate) async fn initialize_run_shard_summary_with(
    db: &mut sqlx::PgConnection,
    run_id: Uuid,
    run_shard: i16,
) -> anyhow::Result<Option<RunShardSummary>> {
    ensure_snapshot_with(db, run_id, run_shard).await?;
    insert_run_shard_summary_with(db, run_id, run_shard).await?;
    Ok(select_run_shard_summaries(db, run_id, &[run_shard])
        .await?
        .pop())
}

/// Ensures a summary exists after its caller has frozen the local snapshot.
/// Dispatch uses this single write; repeated windows cannot overwrite progress.
pub(crate) async fn insert_run_shard_summary_with(
    db: &mut sqlx::PgConnection,
    run_id: Uuid,
    run_shard: i16,
) -> anyhow::Result<()> {
    sqlx::query(r#"
        INSERT INTO run_shard_summaries (
            run_id, run_shard, expected_execution_count, scorecard, status,
            planned_chunk_count, planned_chunk_hash
        )
        SELECT run_id, run_shard, expected_execution_count,
            jsonb_build_object('version', 1, 'run_shard', run_shard, 'policy_hash', aggregation_policy_hash, 'entries', '[]'::jsonb), 'running',
            COALESCE((config_snapshot #>> ARRAY['settlement_plan','shards',run_shard::text,'chunk_count'])::int, 0),
            COALESCE(config_snapshot #>> ARRAY['settlement_plan','shards',run_shard::text,'hash'], '')
        FROM run_snapshots
        WHERE run_id = $1
        AND run_shard = $2
        ON CONFLICT (run_id, run_shard) DO NOTHING
        "#)
        .bind(run_id).bind(run_shard).execute(&mut *db).await?;
    Ok(())
}

/// Commits a bounded receipt fold and its resumable checkpoint together.
pub(super) async fn store_with(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    summary: &RunShardSummary,
    state: serde_json::Value,
    consumed: &[Uuid],
) -> anyhow::Result<()> {
    let result = sqlx::query(
        r#"WITH consumed AS (
            UPDATE run_chunk_settlement_consumptions SET consumed = true
            WHERE run_id = $1 AND run_shard = $2
              AND chunk_id = ANY($5::uuid[]) AND NOT consumed
        )
        UPDATE run_shard_summaries AS s SET
        expected_execution_count = incoming.expected_execution_count,
        execution_count = incoming.execution_count,
        terminal_execution_count = incoming.terminal_execution_count,
        aggregate_count = incoming.aggregate_count,
        passed_execution_count = incoming.passed_execution_count,
        failed_execution_count = incoming.failed_execution_count,
        errored_execution_count = incoming.errored_execution_count,
        skipped_execution_count = incoming.skipped_execution_count,
        missing_aggregate_count = incoming.missing_aggregate_count,
        evaluator_result_count = incoming.evaluator_result_count,
        blocking_failure_count = incoming.blocking_failure_count,
        score_count = incoming.score_count,
        score_sum = incoming.score_sum,
        min_score = incoming.min_score,
        max_score = incoming.max_score,
        scorecard = incoming.scorecard,
        failed_chunk_count = incoming.failed_chunk_count,
        cancelled_chunk_count = incoming.cancelled_chunk_count,
        status = incoming.status,
        sealed = incoming.sealed,
        planned_chunk_hash = incoming.planned_chunk_hash,
        planned_chunk_count = incoming.planned_chunk_count,
        settled_chunk_count = incoming.settled_chunk_count,
        missing_execution_count = incoming.missing_execution_count,
        unresolved_execution_count = incoming.unresolved_execution_count,
        aggregation_state = $4, updated_at = now()
        FROM jsonb_populate_record(NULL::run_shard_summaries, $3::jsonb) AS incoming
        WHERE s.run_id = $1 AND s.run_shard = $2 AND NOT s.sealed"#,
    )
    .bind(summary.run_id)
    .bind(summary.run_shard)
    .bind(serde_json::to_value(summary)?)
    .bind(state)
    .bind(consumed)
    .execute(&mut **tx)
    .await?;
    anyhow::ensure!(
        result.rows_affected() == 1,
        "shard summary checkpoint lost its unsealed row"
    );
    Ok(())
}

#[cfg(test)]
pub(crate) async fn refresh_run_shard_summary(
    db: &PgPool,
    run_id: Uuid,
    shard: i16,
) -> anyhow::Result<Option<RunShardSummary>> {
    let mut tx = db.begin().await?;
    super::settle_terminal_chunks_with(&mut tx, run_id, shard).await?;
    tx.commit().await?;
    for _ in 0..1000 {
        if !super::advance_shard_summary(db, run_id, shard, 1).await? {
            break;
        }
    }
    select_run_shard_summary(db, run_id, shard).await
}
