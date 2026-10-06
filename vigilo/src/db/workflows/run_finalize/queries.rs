//! PostgreSQL operations for run finalization.

use super::*;

/// Selects the next run whose dispatch cursors are drained.
///
/// Query behavior:
/// - Does not inspect execution-owned rows.
/// - Requires all control dispatch cursors for the run to be drained.
/// - Returns `finalizing` runs only when their coordinator lease has expired.
/// - Excludes candidates already inspected by the current coordinator cycle.
/// - Orders candidates by their last coordinator heartbeat for bounded fairness.
pub(crate) async fn select_next_finalization_candidate(
    db: &PgPool,
    excluded_run_ids: &[Uuid],
) -> anyhow::Result<Option<ClaimedRunForFinalization>> {
    let candidate = sqlx::query_as::<_, ClaimedRunForFinalization>(
        r#"
        SELECT r.id, r.run_key, r.aggregation_policy_hash
        FROM runs r
        WHERE r.status IN ('running'::run_status, 'finalizing'::run_status)
          AND (
              r.status <> 'finalizing'::run_status
              OR r.coordinator_leased_until IS NULL
              OR r.coordinator_leased_until < now()
          )
          AND EXISTS (
              SELECT 1
              FROM run_shard_dispatch_cursors c
              WHERE c.run_id = r.id
          )
          AND NOT EXISTS (
              SELECT 1
              FROM run_shard_dispatch_cursors c
              WHERE c.run_id = r.id
                AND c.status = 'open'
          )
          AND NOT (r.id = ANY($1::uuid[]))
        ORDER BY COALESCE(r.coordinator_heartbeat_at, r.updated_at) ASC, r.id ASC
        LIMIT 1
        "#,
    )
    .bind(excluded_run_ids)
    .fetch_optional(db)
    .await?;

    Ok(candidate)
}

/// Records that a coordinator inspected a candidate that is not ready yet.
///
/// Candidate selection orders by this heartbeat so bounded coordinator cycles
/// rotate past nonterminal runs instead of repeatedly selecting the oldest one.
/// The run lifecycle and user-visible `updated_at` timestamp are unchanged.
pub(crate) async fn mark_finalization_candidate_checked(
    db: &PgPool,
    run_id: Uuid,
) -> anyhow::Result<bool> {
    let result = sqlx::query(
        r#"
        UPDATE runs
        SET coordinator_heartbeat_at = now()
        WHERE id = $1::uuid
          AND status IN ('running'::run_status, 'finalizing'::run_status)
        "#,
    )
    .bind(run_id)
    .execute(db)
    .await?;

    Ok(result.rows_affected() == 1)
}

/// Measures control-database runs that are eligible for finalization consideration.
///
/// This intentionally checks only control-owned dispatch cursor state. The
/// coordinator still reads routed shard summaries before claiming and
/// finalizing a candidate.
pub(crate) async fn select_finalization_candidate_backlog(
    db: &PgPool,
) -> anyhow::Result<FinalizationCandidateBacklog> {
    let backlog = sqlx::query_as::<_, FinalizationCandidateBacklog>(
        r#"
        WITH candidates AS (
            SELECT r.updated_at
            FROM runs r
            WHERE r.status IN ('running'::run_status, 'finalizing'::run_status)
              AND (
                  r.status <> 'finalizing'::run_status
                  OR r.coordinator_leased_until IS NULL
                  OR r.coordinator_leased_until < now()
              )
              AND EXISTS (
                  SELECT 1
                  FROM run_shard_dispatch_cursors c
                  WHERE c.run_id = r.id
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM run_shard_dispatch_cursors c
                  WHERE c.run_id = r.id
                    AND c.status = 'open'
              )
        )
        SELECT
            COUNT(*)::bigint AS candidate_count,
            FLOOR(EXTRACT(EPOCH FROM now() - MIN(updated_at)))::bigint
                AS oldest_candidate_lag_seconds
        FROM candidates
        "#,
    )
    .fetch_one(db)
    .await?;

    Ok(backlog)
}

/// Claims a finalization candidate in the control database.
///
/// The coordinator reads shard summaries before calling this. Claiming only
/// serializes the final control write and protects retries if the process dies
/// after the claim but before completion. A dispatch lifecycle share lock makes
/// the candidate temporarily ineligible instead of blocking this coordinator.
pub(crate) async fn claim_finalization_candidate(
    db: &PgPool,
    run_id: Uuid,
    coordinator_id: Uuid,
    lease_seconds: i32,
) -> anyhow::Result<Option<ClaimedRunForFinalization>> {
    let claimed = sqlx::query_as::<_, ClaimedRunForFinalization>(
        r#"
        WITH candidate AS (
            SELECT r.id
            FROM runs r
            WHERE r.id = $1::uuid
              AND r.status IN ('running'::run_status, 'finalizing'::run_status)
              AND (
                  r.status <> 'finalizing'::run_status
                  OR r.coordinator_leased_until IS NULL
                  OR r.coordinator_leased_until < now()
              )
            FOR UPDATE SKIP LOCKED
        )
        UPDATE runs r
        SET status = 'finalizing'::run_status,
            coordinator_id = $2,
            coordinator_leased_until = now() + ($3::int * interval '1 second'),
            coordinator_heartbeat_at = now(),
            updated_at = now()
        FROM candidate
        WHERE r.id = candidate.id
        RETURNING r.id, r.run_key, r.aggregation_policy_hash
        "#,
    )
    .bind(run_id)
    .bind(coordinator_id)
    .bind(lease_seconds)
    .fetch_optional(db)
    .await?;

    Ok(claimed)
}

pub(super) async fn finalize_claimed_run(
    db: &PgPool,
    run_id: Uuid,
    coordinator_id: Uuid,
    summary: &FinalizationSummary,
    shards: &[i16],
    shard_plans: serde_json::Value,
) -> anyhow::Result<Option<FinalizedRun>> {
    let scorecard = serde_json::to_value(&summary.scorecard)?;
    let finalized = sqlx::query_as::<_, FinalizedRun>(
        r#"
        WITH run_row AS (
            SELECT
                id,
                run_key,
                expected_execution_count,
                aggregation_policy_hash
            FROM runs
            WHERE id = $1::uuid
              AND status = 'finalizing'::run_status
              AND coordinator_id = $14::uuid
              AND coordinator_leased_until >= now()
              AND aggregation_policy_hash = $16::text
              AND expected_execution_count = $3
              AND config_snapshot #>> '{settlement_plan,version}' = '1'
              AND $20::smallint[] = ARRAY(SELECT run_shard FROM shard_placements WHERE run_id = $1 ORDER BY run_shard)
              AND NOT EXISTS (SELECT 1 FROM run_shard_dispatch_cursors WHERE run_id = $1 AND status <> 'drained')
              AND NOT EXISTS (
                  SELECT 1 FROM jsonb_array_elements($23::jsonb) s
                  WHERE (
                      config_snapshot #>> ARRAY['settlement_plan','shards',s->>'shard','hash'] IS DISTINCT FROM s->>'hash'
                      OR (config_snapshot #>> ARRAY['settlement_plan','shards',s->>'shard','case_count'])::int IS DISTINCT FROM (s->>'cases')::int
                      OR (config_snapshot #>> ARRAY['settlement_plan','shards',s->>'shard','chunk_count'])::int IS DISTINCT FROM (s->>'chunks')::int
                  )
              )
            FOR UPDATE
        ),
        inserted_scorecard AS (
            INSERT INTO run_scorecards (
                run_id,
                schema_version,
                aggregation_policy_hash,
                shard_count,
                passed,
                scorecard
            )
            SELECT
                rr.id,
                $15::smallint,
                $16::text,
                $17::int,
                $18::bool,
                $19::jsonb
            FROM run_row rr
            ON CONFLICT (run_id) DO NOTHING
            RETURNING run_id
        ),
        finalized AS (
            UPDATE runs r
            SET status = 'completed'::run_status,
                gate_status = $2::gate_status,
                terminal_execution_count = $4,
                passed_execution_count = $5,
                failed_execution_count = $6,
                errored_execution_count = $7,
                summary = jsonb_build_object(
                    'expected_execution_count', $3::int,
                    'terminal_execution_count', $4::int,
                    'passed_execution_count', $5::int,
                    'failed_execution_count', $6::int,
                    'errored_execution_count', $7::int,
                    'missing_aggregate_count', $8::int,
                    'failed_chunk_count', $9::int,
                    'cancelled_chunk_count', $10::int,
                    'coverage_complete', $11::bool,
                    'settled', true,
                    'planned_chunk_count', (SELECT SUM((p->>'chunks')::int) FROM jsonb_array_elements($23::jsonb) p),
                    'settled_chunk_count', (SELECT SUM((p->>'chunks')::int) FROM jsonb_array_elements($23::jsonb) p),
                    'sealed_shard_count', $13::int,
                    'missing_execution_count', $21::int,
                    'unresolved_execution_count', $22::int,
                    'has_terminal_chunk_failure', $12::bool,
                    'shard_summary_count', $13::int
                ),
                finalized_at = COALESCE(r.finalized_at, now()),
                completed_at = COALESCE(r.completed_at, now()),
                coordinator_leased_until = NULL,
                coordinator_heartbeat_at = now(),
                updated_at = now()
            FROM run_row rr
            JOIN inserted_scorecard scorecard ON scorecard.run_id = rr.id
            WHERE r.id = rr.id
            RETURNING
                r.id,
                r.run_key,
                r.gate_status::text as gate_status,
                r.terminal_execution_count,
                r.passed_execution_count,
                r.failed_execution_count,
                r.errored_execution_count
        ),
        drained_cursors AS (
            UPDATE run_shard_dispatch_cursors c
            SET status = 'drained',
                updated_at = now()
            FROM finalized f
            WHERE c.run_id = f.id
            RETURNING 1
        ),
        inserted_event AS (
            INSERT INTO outbox_events (event_type, aggregate_type, aggregate_id, dedupe_key, payload)
            SELECT
                'run.completed',
                'run',
                f.id,
                format('run:%s:completed', f.id),
                jsonb_build_object(
                    'run_id', f.id,
                    'run_key', f.run_key,
                    'gate_status', f.gate_status,
                    'terminal_execution_count', f.terminal_execution_count,
                    'passed_execution_count', f.passed_execution_count,
                    'failed_execution_count', f.failed_execution_count,
                    'errored_execution_count', f.errored_execution_count
                )
            FROM finalized f
            ON CONFLICT (dedupe_key) DO NOTHING
            RETURNING id
        )
        SELECT
            f.id,
            f.run_key,
            f.gate_status,
            f.terminal_execution_count,
            f.passed_execution_count,
            f.failed_execution_count,
            f.errored_execution_count
        FROM finalized f
        "#,
    )
    .bind(run_id)
    .bind(summary.gate_status)
    .bind(summary.expected_execution_count)
    .bind(summary.terminal_execution_count)
    .bind(summary.passed_execution_count)
    .bind(summary.failed_execution_count)
    .bind(summary.errored_execution_count)
    .bind(summary.missing_aggregate_count)
    .bind(summary.failed_chunk_count)
    .bind(summary.cancelled_chunk_count)
    .bind(summary.coverage_complete)
    .bind(summary.has_terminal_chunk_failure)
    .bind(summary.shard_summary_count)
    .bind(coordinator_id)
    .bind(i16::try_from(summary.scorecard.version)?)
    .bind(&summary.scorecard.policy_hash)
    .bind(i32::try_from(summary.scorecard.shard_count)?)
    .bind(summary.scorecard.passed)
    .bind(scorecard)
    .bind(shards)
    .bind(summary.missing_execution_count)
    .bind(summary.unresolved_execution_count)
    .bind(shard_plans)
    .fetch_optional(db)
    .await?;

    Ok(finalized)
}
