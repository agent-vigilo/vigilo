//! Lease renewal and authoritative batched attempt settlement.
//!
//! These queries fence shard/chunk ownership and current-attempt authority before
//! committing results, retries, and terminal state. Bounded invocation evidence is
//! merged into the existing attempt update; it has no independent write path and
//! cannot bypass the same ownership checks. Other chunks retain shared run access.

use super::super::*;

/// Renews live attempt leases owned by one worker for the current chunk.
///
/// The chunk lease remains the scheduling boundary. Attempt leases mirror that
/// ownership at case granularity so stale workers cannot complete attempts after
/// coordinator recovery or reassignment.
pub(in crate::db::workflows::execution_processing) async fn heartbeat_running_attempts_for_chunk_query(
    db: &PgPool,
    run_id: Uuid,
    run_shard: i16,
    chunk_id: Uuid,
    worker_id: Uuid,
    lease_seconds: i32,
) -> anyhow::Result<u64> {
    let result = sqlx::query(
        r#"
        UPDATE execution_attempts
        SET leased_until = now() + ($5::int * interval '1 second'),
            heartbeat_at = now(),
            updated_at = now()
        FROM executions
        WHERE execution_attempts.run_id = $1::uuid
          AND execution_attempts.run_shard = $2
          AND execution_attempts.worker_id = $4::uuid
          AND execution_attempts.status = 'running'::attempt_status
          AND executions.run_id = execution_attempts.run_id
          AND executions.run_shard = execution_attempts.run_shard
          AND executions.id = execution_attempts.execution_id
          AND executions.chunk_id = $3::uuid
          AND executions.current_attempt_id = execution_attempts.id
        "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .bind(chunk_id)
    .bind(worker_id)
    .bind(lease_seconds)
    .execute(db)
    .await?;

    Ok(result.rows_affected())
}

/// Applies terminal execution transitions as one authoritative batch.
///
/// The batch updates attempts and executions together. If any transition no
/// longer owns the current attempt, the entire batch is rejected so stale
/// workers cannot mark executions terminal. The run-state guard is shared so
/// other chunks for the same running run are not serialized on the run row.
///
/// Query behavior:
/// - Unnests the worker's transition batch into a relational input set.
/// - Re-checks the run is still `running` and every transition still owns the
///   execution's current attempt id and attempt number.
/// - Requires completed transitions to have an aggregate for the same attempt.
/// - Retries eligible failures within the attempt budget, respecting persisted
///   server not-before advice and bounded local backoff/jitter.
/// - Writes terminal error aggregates when a failure is non-retryable or exhausted.
pub(crate) async fn finalize_execution_terminal_transitions(
    db: &PgPool,
    run_id: Uuid,
    run_shard: i16,
    worker_id: Uuid,
    max_attempts: i32,
    transitions: &[ExecutionTerminalTransition],
) -> anyhow::Result<()> {
    if transitions.is_empty() {
        return Ok(());
    }

    let execution_ids = transitions
        .iter()
        .map(|transition| transition.execution_id)
        .collect::<Vec<_>>();
    let attempt_ids = transitions
        .iter()
        .map(|transition| transition.attempt_id)
        .collect::<Vec<_>>();
    let attempt_nos = transitions
        .iter()
        .map(|transition| transition.attempt_no)
        .collect::<Vec<_>>();
    let completed_flags = transitions
        .iter()
        .map(|transition| transition.completed)
        .collect::<Vec<_>>();
    let error_messages = transitions
        .iter()
        .map(|transition| transition.error_message.clone())
        .collect::<Vec<_>>();
    let requires_worker_lease = transitions
        .iter()
        .map(|transition| transition.requires_worker_lease)
        .collect::<Vec<_>>();

    let agent_invocations = transitions
        .iter()
        .map(|t| {
            t.agent_invocation
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let agent_failures = transitions
        .iter()
        .map(|t| {
            t.agent_failure
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let retryable = transitions
        .iter()
        .map(|t| t.agent_failure.as_ref().is_none_or(|f| f.retryable))
        .collect::<Vec<_>>();
    let retry_not_before = transitions
        .iter()
        .map(|t| t.agent_failure.as_ref().and_then(|f| f.retry_not_before))
        .collect::<Vec<_>>();
    // Attempt IDs supply stable jitter across persistence retries without a global RNG.
    let jitter_ms = transitions
        .iter()
        .map(|t| {
            if t.agent_failure.is_some() {
                i32::from(u16::from_be_bytes([
                    t.attempt_id.as_bytes()[14],
                    t.attempt_id.as_bytes()[15],
                ])) % 1001
            } else {
                0
            }
        })
        .collect::<Vec<_>>();

    let retry_delay_secs = transitions
        .iter()
        .map(|t| {
            t.agent_failure
                .as_ref()
                .and_then(|f| f.retry_delay_secs)
                .map(i64::from)
        })
        .collect::<Vec<_>>();

    // Query outline:
    //
    // transition_input       - worker-produced terminal transitions.
    // run_guard             - shared check that the run is still running.
    // authoritative_input   - transitions that still own current_attempt_*.
    // authority_check       - all-or-nothing stale worker guard.
    // terminal_input        - requires aggregates for completed attempts.
    // attempt_update        - marks attempts completed/failed and computes retry.
    // failed_aggregate_upsert
    //                       - creates final error aggregate only after retries end.
    // execution_update      - moves executions to completed/retry_scheduled/failed.
    let mut tx = db.begin().await?;
    crate::db::shard_write_fence::lock_shared(&mut tx, run_id, run_shard).await?;
    // Take the chunk lock before any attempt mutation, just like persistence
    // and settlement. A separate statement refreshes the snapshot after waiting.
    let chunks = sqlx::query_as::<_, (Uuid, bool)>(
        "SELECT c.id, c.status = 'leased' AND c.leased_until >= now() AS live FROM run_chunks c WHERE c.run_id = $1 AND c.run_shard = $2 AND EXISTS (SELECT 1 FROM executions e WHERE e.run_id = c.run_id AND e.run_shard = c.run_shard AND e.chunk_id = c.id AND e.id = ANY($3::uuid[])) ORDER BY c.id FOR UPDATE OF c",
    ).bind(run_id).bind(run_shard).bind(&execution_ids).fetch_all(&mut *tx).await?;
    anyhow::ensure!(
        !chunks.is_empty() && chunks.iter().all(|(_, live)| *live),
        "terminal transitions lost chunk settlement authority"
    );
    let applied = sqlx::query_scalar::<_, i64>(
        r#"
        WITH transition_input AS (
            SELECT *
            FROM UNNEST(
                $1::uuid[],
                $2::uuid[],
                $3::int4[],
                $4::bool[],
                $5::text[],
                $6::bool[],
                $13::jsonb[],
                $14::bool[],
                $15::timestamptz[],
                $16::int4[],
                $17::int8[],
                $18::jsonb[]
            ) AS t(execution_id, attempt_id, attempt_no, completed, error_message, requires_worker_lease, agent_failure, retryable, retry_not_before, jitter_ms, retry_delay_secs, agent_invocation)
        ),
        transition_count AS (
            SELECT COUNT(*) AS expected_count
            FROM transition_input
        ),
        run_guard AS (
            SELECT run_id AS id
            FROM run_snapshots
            WHERE run_id = $7::uuid
              AND run_shard = $8
            FOR SHARE
        ),
        authoritative_input AS (
            SELECT
                transition_input.*,
                executions.run_id,
                executions.run_shard
            FROM transition_input
            JOIN executions
              ON executions.run_id = $7::uuid
             AND executions.run_shard = $8
             AND executions.id = transition_input.execution_id
             AND executions.current_attempt_id = transition_input.attempt_id
             AND executions.current_attempt_no = transition_input.attempt_no
            JOIN execution_attempts
              ON execution_attempts.run_id = executions.run_id
             AND execution_attempts.run_shard = executions.run_shard
             AND execution_attempts.id = transition_input.attempt_id
             AND execution_attempts.execution_id = transition_input.execution_id
             AND (
                    (
                        transition_input.requires_worker_lease
                        AND execution_attempts.status = 'running'::attempt_status
                        AND execution_attempts.worker_id = $9::uuid
                        AND execution_attempts.leased_until >= now()
                    )
                    OR (
                        NOT transition_input.requires_worker_lease
                        AND execution_attempts.status IN (
                            'running'::attempt_status,
                            'stale'::attempt_status
                        )
                    )
                 )
            JOIN run_guard
              ON run_guard.id = executions.run_id
        ),
        authority_check AS (
            SELECT transition_count.expected_count
            FROM transition_count
            WHERE transition_count.expected_count = (
                SELECT COUNT(*)
                FROM authoritative_input
            )
        ),
        terminal_input AS (
            SELECT
                authoritative_input.*,
                CASE
                    WHEN authoritative_input.completed THEN execution_aggregates.overall_status
                    ELSE 'error'::evaluation_status
                END AS overall_status
            FROM authoritative_input
            LEFT JOIN execution_aggregates
              ON execution_aggregates.run_id = $7::uuid
             AND execution_aggregates.run_shard = $8
             AND execution_aggregates.execution_id = authoritative_input.execution_id
             AND execution_aggregates.attempt_id = authoritative_input.attempt_id
            WHERE NOT authoritative_input.completed
               OR execution_aggregates.execution_id IS NOT NULL
        ),
        terminal_input_check AS (
            SELECT transition_count.expected_count
            FROM transition_count, authority_check
            WHERE transition_count.expected_count = (
                SELECT COUNT(*)
                FROM terminal_input
            )
        ),
        stale_attempt_update AS (
            UPDATE execution_attempts
            SET status = 'stale'::attempt_status,
                error_message = COALESCE(
                    transition_input.error_message,
                    'attempt lost authority before terminal transition'
                ),
                completed_at = now(),
                leased_until = NULL,
                updated_at = now()
            FROM transition_input
            JOIN executions
              ON executions.run_id = $7::uuid
             AND executions.run_shard = $8
             AND executions.id = transition_input.execution_id
            WHERE execution_attempts.run_id = $7::uuid
              AND execution_attempts.run_shard = $8
              AND execution_attempts.id = transition_input.attempt_id
              AND execution_attempts.execution_id = transition_input.execution_id
              AND execution_attempts.status = 'running'::attempt_status
              AND transition_input.requires_worker_lease
              AND (
                  executions.current_attempt_id IS DISTINCT FROM transition_input.attempt_id
                  OR executions.current_attempt_no IS DISTINCT FROM transition_input.attempt_no
                  OR execution_attempts.worker_id IS DISTINCT FROM $9::uuid
                  OR execution_attempts.leased_until IS NULL
                  OR execution_attempts.leased_until < now()
              )
            RETURNING execution_attempts.id
        ),
        attempt_update AS (
            UPDATE execution_attempts
            SET status = CASE
                    WHEN terminal_input.completed THEN 'completed'::attempt_status
                    WHEN terminal_input.agent_failure IS NOT NULL
                        THEN 'failed_agent_call'::attempt_status
                    ELSE 'failed_evaluation'::attempt_status
                END,
                error_message = CASE
                    WHEN terminal_input.completed THEN NULL
                    ELSE terminal_input.error_message
                END,
                outcome_summary = execution_attempts.outcome_summary
                    || CASE WHEN terminal_input.agent_failure IS NOT NULL
                        THEN jsonb_build_object('agent_failure', terminal_input.agent_failure)
                        ELSE '{}'::jsonb END
                    || CASE WHEN terminal_input.agent_invocation IS NOT NULL
                        THEN jsonb_build_object('agent_invocation', terminal_input.agent_invocation)
                        ELSE '{}'::jsonb END,
                completed_at = now(),
                updated_at = now()
            FROM terminal_input, terminal_input_check
            WHERE execution_attempts.run_id = $7::uuid
              AND execution_attempts.run_shard = $8
              AND execution_attempts.id = terminal_input.attempt_id
              AND execution_attempts.execution_id = terminal_input.execution_id
            RETURNING
                terminal_input.execution_id,
                terminal_input.run_id,
                terminal_input.run_shard,
                terminal_input.attempt_id,
                terminal_input.attempt_no,
                terminal_input.completed,
                terminal_input.error_message,
                terminal_input.overall_status,
                terminal_input.retry_not_before,
                terminal_input.jitter_ms,
                terminal_input.retry_delay_secs,
                (
                    NOT terminal_input.completed
                    AND terminal_input.retryable
                    AND terminal_input.attempt_no < $10::int
                ) AS retry_scheduled
        ),
        failed_aggregate_upsert AS (
            INSERT INTO execution_aggregates (
                execution_id,
                run_id,
                run_shard,
                attempt_id,
                overall_status,
                aggregate_score,
                evaluator_result_count,
                dimension_scores,
                blocking_failures,
                summary,
                updated_at
            )
            SELECT
                attempt_update.execution_id,
                attempt_update.run_id,
                attempt_update.run_shard,
                attempt_update.attempt_id,
                'error'::evaluation_status,
                NULL,
                0,
                '{}'::jsonb,
                jsonb_build_array(jsonb_build_object(
                    'status', 'error',
                    'reason', attempt_update.error_message
                )),
                jsonb_build_object(
                    'attempt_id', attempt_update.attempt_id,
                    'result_count', 0,
                    'overall_status', 'error',
                    'error_message', attempt_update.error_message
                ),
                now()
            FROM attempt_update
            WHERE NOT attempt_update.completed
              AND NOT attempt_update.retry_scheduled
            ON CONFLICT (run_id, run_shard, execution_id) DO UPDATE
            SET attempt_id = EXCLUDED.attempt_id,
                overall_status = EXCLUDED.overall_status,
                aggregate_score = EXCLUDED.aggregate_score,
                evaluator_result_count = EXCLUDED.evaluator_result_count,
                dimension_scores = EXCLUDED.dimension_scores,
                blocking_failures = EXCLUDED.blocking_failures,
                summary = EXCLUDED.summary,
                updated_at = now()
            RETURNING execution_id
        ),
        execution_update AS (
            UPDATE executions
            SET status = CASE
                    WHEN attempt_update.completed THEN 'completed'::execution_status
                    WHEN attempt_update.retry_scheduled THEN 'retry_scheduled'::execution_status
                    ELSE 'failed'::execution_status
                END,
                current_attempt_no = attempt_update.attempt_no,
                current_attempt_id = attempt_update.attempt_id,
                last_error_message = CASE
                    WHEN attempt_update.completed THEN NULL
                    ELSE attempt_update.error_message
                END,
                retry_after = CASE
                    WHEN attempt_update.retry_scheduled THEN
                        GREATEST(
                            now() + (LEAST($12::int * 1000,
                                LEAST($11::int * POWER(3::numeric, LEAST(GREATEST(attempt_update.attempt_no - 1, 0), 5)), $12::int::numeric)::int * 1000
                                + attempt_update.jitter_ms) * interval '1 millisecond'),
                            attempt_update.retry_not_before,
                            now() + attempt_update.retry_delay_secs * interval '1 second'
                        )
                    ELSE NULL
                END,
                retry_count = CASE
                    WHEN attempt_update.retry_scheduled THEN executions.retry_count + 1
                    ELSE executions.retry_count
                END,
                last_attempt_completed_at = now(),
                completed_at = CASE
                    WHEN attempt_update.retry_scheduled THEN NULL
                    ELSE now()
                END,
                updated_at = now()
            FROM attempt_update
            LEFT JOIN failed_aggregate_upsert
              ON failed_aggregate_upsert.execution_id = attempt_update.execution_id
            WHERE executions.run_id = $7::uuid
              AND executions.run_shard = $8
              AND executions.id = attempt_update.execution_id
              AND executions.current_attempt_id = attempt_update.attempt_id
              AND executions.current_attempt_no = attempt_update.attempt_no
            RETURNING
                executions.id AS execution_id,
                executions.run_id,
                attempt_update.attempt_id,
                attempt_update.overall_status
        )
        SELECT
            (SELECT COUNT(*)::bigint FROM execution_update)
        "#,
    )
    .bind(execution_ids)
    .bind(attempt_ids)
    .bind(attempt_nos)
    .bind(completed_flags)
    .bind(error_messages)
    .bind(requires_worker_lease)
    .bind(run_id)
    .bind(run_shard)
    .bind(worker_id)
    .bind(max_attempts)
    .bind(EXECUTION_RETRY_BASE_SECONDS)
    .bind(EXECUTION_RETRY_MAX_SECONDS)
    .bind(agent_failures)
    .bind(retryable)
    .bind(retry_not_before)
    .bind(jitter_ms)
    .bind(retry_delay_secs)
    .bind(agent_invocations)
    .fetch_one(&mut *tx)
    .await?;

    // Preserve the existing stale-attempt retirement even when the batch loses
    // authority. The SQL authority guard prevents execution completion in that case.
    tx.commit().await?;
    let expected = u64::try_from(transitions.len())?;
    if u64::try_from(applied)? != expected {
        anyhow::bail!(
            "terminal transition batch applied {} current executions out of {}; at least one attempt lost authority or completed without an aggregate",
            applied,
            expected
        );
    }

    Ok(())
}

/// Summarizes whether a chunk's executions are terminal or still waiting for retry.
///
/// Query behavior: counts open execution statuses for the chunk's case ids and
/// returns the earliest retry window. The worker uses this after terminal
/// transitions to decide whether to complete the chunk or release it and delay
/// the message until retry work is due.
pub(crate) async fn summarize_chunk_execution_state(
    db: &PgPool,
    run_id: Uuid,
    run_shard: i16,
    case_ids: &[Uuid],
) -> anyhow::Result<ChunkExecutionState> {
    if case_ids.is_empty() {
        return Ok(ChunkExecutionState {
            open_execution_count: 0,
            retry_scheduled_count: 0,
            next_retry_after: None,
        });
    }

    let state = sqlx::query_as::<_, ChunkExecutionState>(
        r#"
        SELECT
            COUNT(*) FILTER (
                WHERE status IN (
                    'pending'::execution_status,
                    'running'::execution_status,
                    'awaiting_evaluators'::execution_status,
                    'retry_scheduled'::execution_status
                )
            )::bigint AS open_execution_count,
            COUNT(*) FILTER (
                WHERE status = 'retry_scheduled'::execution_status
            )::bigint AS retry_scheduled_count,
            MIN(retry_after) FILTER (
                WHERE status = 'retry_scheduled'::execution_status
            ) AS next_retry_after
        FROM executions
        WHERE run_id = $1::uuid
          AND run_shard = $2
          AND case_id = ANY($3::uuid[])
        "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .bind(case_ids)
    .fetch_one(db)
    .await?;

    Ok(state)
}
