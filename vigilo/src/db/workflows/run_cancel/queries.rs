//! PostgreSQL operations for run cancellation.

use super::*;

pub(super) async fn select_run_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: Uuid,
) -> anyhow::Result<Option<Run>> {
    // Query behavior: read the run projection inside the caller's transaction.
    // Used for idempotent already-cancelled responses after the status row has
    // been locked by `cancel_run`.
    let run = sqlx::query_as::<_, Run>(
        r#"
        SELECT
            id, run_key, name, description,
            dataset_id, dataset_version,
            evaluation_profile_id, evaluation_profile_version,
            aggregation_policy_id, aggregation_policy_version,
            agent_provider, agent_name, agent_version,
            prompt_config_id, prompt_config_version,
            config_snapshot,
            status::text as status,
            gate_status::text as gate_status,
            coordinator_id,
            coordinator_leased_until,
            coordinator_heartbeat_at,
            expected_execution_count,
            terminal_execution_count,
            passed_execution_count,
            failed_execution_count,
            errored_execution_count,
            summary,
            error_message,
            created_at,
            started_at,
            dispatched_at,
            finalized_at,
            completed_at,
            updated_at
        FROM runs
        WHERE id = $1::uuid
        "#,
    )
    .bind(run_id)
    .fetch_optional(&mut **tx)
    .await?;

    Ok(run)
}

pub(super) async fn cancel_open_attempts(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: Uuid,
) -> anyhow::Result<i64> {
    // Query behavior: close only pending/running attempts for the run, clear
    // lease ownership, preserve any existing error text, and count affected
    // rows for the cancellation summary.
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        WITH updated AS (
            UPDATE execution_attempts
            SET status = 'cancelled'::attempt_status,
                leased_until = NULL,
                error_message = COALESCE(error_message, $2),
                completed_at = COALESCE(completed_at, now()),
                updated_at = now()
            WHERE run_id = $1::uuid
              AND status IN ('pending'::attempt_status, 'running'::attempt_status)
            RETURNING 1
        )
        SELECT COUNT(*)::bigint
        FROM updated
        "#,
    )
    .bind(run_id)
    .bind(CANCEL_REASON)
    .fetch_one(&mut **tx)
    .await?;

    Ok(count)
}

pub(super) async fn cancel_open_executions(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: Uuid,
) -> anyhow::Result<i64> {
    // Query behavior: move every non-terminal execution state to cancelled,
    // including retry_scheduled rows, so no worker can later allocate or
    // finalize additional attempts for this run.
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        WITH updated AS (
            UPDATE executions
            SET status = 'cancelled'::execution_status,
                last_error_message = COALESCE(last_error_message, $2),
                completed_at = COALESCE(completed_at, now()),
                updated_at = now()
            WHERE run_id = $1::uuid
              AND status IN (
                  'pending'::execution_status,
                  'running'::execution_status,
                  'awaiting_evaluators'::execution_status,
                  'retry_scheduled'::execution_status
              )
            RETURNING 1
        )
        SELECT COUNT(*)::bigint
        FROM updated
        "#,
    )
    .bind(run_id)
    .bind(CANCEL_REASON)
    .fetch_one(&mut **tx)
    .await?;

    Ok(count)
}

pub(super) async fn cancel_open_chunks(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: Uuid,
) -> anyhow::Result<i64> {
    // Query behavior: move only open chunks to cancelled, clear their worker
    // lease, and drain shard dispatch cursors so no later coordinator cycle
    // tries to dispatch stale open cursor rows. Completed/failed chunks remain
    // as historical terminal outcomes.
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        WITH updated AS (
            UPDATE run_chunks
            SET status = 'cancelled',
                lease_token = NULL,
                leased_until = NULL,
                updated_at = now()
            WHERE run_id = $1::uuid
              AND status IN ('pending', 'leased')
            RETURNING 1
        ),
        drained_cursors AS (
            UPDATE run_shard_dispatch_cursors
            SET status = 'drained',
                updated_at = now()
            WHERE run_id = $1::uuid
            RETURNING 1
        )
        SELECT COUNT(*)::bigint
        FROM updated
        "#,
    )
    .bind(run_id)
    .fetch_one(&mut **tx)
    .await?;

    Ok(count)
}

pub(super) async fn drain_control_dispatch_cursors(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: Uuid,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE run_shard_dispatch_cursors
        SET status = 'drained',
            updated_at = now()
        WHERE run_id = $1::uuid
        "#,
    )
    .bind(run_id)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

pub(super) async fn mark_control_cancellation_requested(
    db: &PgPool,
    run_id: Uuid,
) -> anyhow::Result<Option<ControlCancellationState>> {
    let mut tx = db.begin().await?;

    let Some(status) = sqlx::query_scalar::<_, String>(
        r#"
        SELECT status::text
        FROM runs
        WHERE id = $1::uuid
        FOR UPDATE
        "#,
    )
    .bind(run_id)
    .fetch_optional(&mut *tx)
    .await?
    else {
        tx.commit().await?;
        return Ok(None);
    };

    if is_uncancelable_terminal_status(&status) {
        anyhow::bail!(
            "run '{}' is already terminal with status '{}' and cannot be cancelled",
            run_id,
            status
        );
    }

    if is_already_cancelled_status(&status) {
        drain_control_dispatch_cursors(&mut tx, run_id).await?;
        let run = select_run_in_tx(&mut tx, run_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("run '{}' disappeared during cancellation", run_id))?;
        tx.commit().await?;
        return Ok(Some(ControlCancellationState {
            run,
            cancelled: false,
            already_cancelled: true,
        }));
    }

    if !is_cancelable_status(&status) {
        anyhow::bail!(
            "run '{}' has unsupported status '{}' and cannot be cancelled",
            run_id,
            status
        );
    }

    drain_control_dispatch_cursors(&mut tx, run_id).await?;
    let run = mark_run_cancelled_pending(&mut tx, run_id).await?;
    tx.commit().await?;

    Ok(Some(ControlCancellationState {
        run,
        cancelled: true,
        already_cancelled: false,
    }))
}

pub(super) async fn mark_run_cancelled_pending(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: Uuid,
) -> anyhow::Result<Run> {
    let run = sqlx::query_as::<_, Run>(
        r#"
        UPDATE runs r
        SET status = 'cancelled'::run_status,
            gate_status = 'fail'::gate_status,
            summary = jsonb_build_object(
                'cancelled', true,
                'reason', $2,
                'cancellation_pending', true,
                'expected_execution_count', r.expected_execution_count
            ),
            error_message = COALESCE(r.error_message, $2),
            completed_at = COALESCE(r.completed_at, now()),
            coordinator_id = NULL,
            coordinator_leased_until = NULL,
            coordinator_heartbeat_at = now(),
            updated_at = now()
        WHERE r.id = $1::uuid
        RETURNING
            r.id, r.run_key, r.name, r.description,
            r.dataset_id, r.dataset_version,
            r.evaluation_profile_id, r.evaluation_profile_version,
            r.aggregation_policy_id, r.aggregation_policy_version,
            r.agent_provider, r.agent_name, r.agent_version,
            r.prompt_config_id, r.prompt_config_version,
            r.config_snapshot,
            r.status::text as status,
            r.gate_status::text as gate_status,
            r.coordinator_id,
            r.coordinator_leased_until,
            r.coordinator_heartbeat_at,
            r.expected_execution_count,
            r.terminal_execution_count,
            r.passed_execution_count,
            r.failed_execution_count,
            r.errored_execution_count,
            r.summary,
            r.error_message,
            r.created_at,
            r.started_at,
            r.dispatched_at,
            r.finalized_at,
            r.completed_at,
            r.updated_at
        "#,
    )
    .bind(run_id)
    .bind(CANCEL_REASON)
    .fetch_one(&mut **tx)
    .await?;

    Ok(run)
}

pub(super) async fn update_run_cancelled(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: Uuid,
    progress: &ExecutionProgressCounts,
    counts: &CancellationCounts,
    execution_route_count: usize,
    shard_summary_count: usize,
) -> anyhow::Result<Run> {
    let run = sqlx::query_as::<_, Run>(
        r#"
        UPDATE runs r
        SET status = 'cancelled'::run_status,
            gate_status = 'fail'::gate_status,
            terminal_execution_count = $3::int,
            passed_execution_count = $4::int,
            failed_execution_count = $5::int,
            errored_execution_count = $6::int,
            summary = jsonb_build_object(
                'cancelled', true,
                'reason', $2,
                'cancellation_pending', false,
                'expected_execution_count', r.expected_execution_count,
                'execution_expected_count', $7::bigint,
                'execution_count', $8::bigint,
                'terminal_execution_count', $9::bigint,
                'passed_execution_count', $10::bigint,
                'failed_execution_count', $11::bigint,
                'errored_execution_count', $12::bigint,
                'skipped_execution_count', $13::bigint,
                'missing_aggregate_count', $14::bigint,
                'cancelled_execution_count', $15::bigint,
                'chunk_count', $16::bigint,
                'completed_chunk_count', $17::bigint,
                'failed_chunk_count', $18::bigint,
                'cancelled_chunk_count', $19::bigint,
                'chunks_cancelled_by_request', $20::bigint,
                'executions_cancelled_by_request', $21::bigint,
                'attempts_cancelled_by_request', $22::bigint,
                'execution_route_count', $23::int,
                'shard_summary_count', $24::int
            ),
            error_message = COALESCE(r.error_message, $2),
            completed_at = COALESCE(r.completed_at, now()),
            coordinator_id = NULL,
            coordinator_leased_until = NULL,
            coordinator_heartbeat_at = now(),
            updated_at = now()
        WHERE r.id = $1::uuid
        RETURNING
            r.id, r.run_key, r.name, r.description,
            r.dataset_id, r.dataset_version,
            r.evaluation_profile_id, r.evaluation_profile_version,
            r.aggregation_policy_id, r.aggregation_policy_version,
            r.agent_provider, r.agent_name, r.agent_version,
            r.prompt_config_id, r.prompt_config_version,
            r.config_snapshot,
            r.status::text as status,
            r.gate_status::text as gate_status,
            r.coordinator_id,
            r.coordinator_leased_until,
            r.coordinator_heartbeat_at,
            r.expected_execution_count,
            r.terminal_execution_count,
            r.passed_execution_count,
            r.failed_execution_count,
            r.errored_execution_count,
            r.summary,
            r.error_message,
            r.created_at,
            r.started_at,
            r.dispatched_at,
            r.finalized_at,
            r.completed_at,
            r.updated_at
        "#,
    )
    .bind(run_id)
    .bind(CANCEL_REASON)
    .bind(i32::try_from(progress.terminal_execution_count)?)
    .bind(i32::try_from(progress.passed_execution_count)?)
    .bind(i32::try_from(progress.failed_execution_count)?)
    .bind(i32::try_from(progress.errored_execution_count)?)
    .bind(progress.expected_execution_count)
    .bind(progress.execution_count)
    .bind(progress.terminal_execution_count)
    .bind(progress.passed_execution_count)
    .bind(progress.failed_execution_count)
    .bind(progress.errored_execution_count)
    .bind(progress.skipped_execution_count)
    .bind(progress.missing_aggregate_count)
    .bind(progress.cancelled_execution_count)
    .bind(progress.chunk_count)
    .bind(progress.completed_chunk_count)
    .bind(progress.failed_chunk_count)
    .bind(progress.cancelled_chunk_count)
    .bind(counts.chunks_cancelled)
    .bind(counts.executions_cancelled)
    .bind(counts.attempts_cancelled)
    .bind(i32::try_from(execution_route_count)?)
    .bind(i32::try_from(shard_summary_count)?)
    .fetch_one(&mut **tx)
    .await?;

    Ok(run)
}

pub(super) async fn select_run_execution_progress(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
) -> anyhow::Result<ExecutionProgressCounts> {
    let counts = sqlx::query_as::<_, ExecutionProgressCounts>(
        r#"
        WITH execution_counts AS (
            SELECT
                COUNT(e.id)::bigint AS execution_count,
                COUNT(e.id) FILTER (
                    WHERE e.status IN (
                        'completed'::execution_status,
                        'failed'::execution_status,
                        'timed_out'::execution_status,
                        'cancelled'::execution_status
                    )
                )::bigint AS terminal_execution_count,
                COUNT(e.id) FILTER (
                    WHERE e.status IN (
                        'completed'::execution_status,
                        'failed'::execution_status,
                        'timed_out'::execution_status,
                        'cancelled'::execution_status
                    )
                      AND ea.overall_status = 'passed'::evaluation_status
                )::bigint AS passed_execution_count,
                COUNT(e.id) FILTER (
                    WHERE e.status IN (
                        'completed'::execution_status,
                        'failed'::execution_status,
                        'timed_out'::execution_status,
                        'cancelled'::execution_status
                    )
                      AND ea.overall_status = 'failed'::evaluation_status
                )::bigint AS failed_execution_count,
                COUNT(e.id) FILTER (
                    WHERE e.status IN (
                        'completed'::execution_status,
                        'failed'::execution_status,
                        'timed_out'::execution_status,
                        'cancelled'::execution_status
                    )
                      AND ea.overall_status = 'error'::evaluation_status
                )::bigint AS errored_execution_count,
                COUNT(e.id) FILTER (
                    WHERE e.status IN (
                        'completed'::execution_status,
                        'failed'::execution_status,
                        'timed_out'::execution_status,
                        'cancelled'::execution_status
                    )
                      AND ea.overall_status = 'skipped'::evaluation_status
                )::bigint AS skipped_execution_count,
                COUNT(e.id) FILTER (
                    WHERE e.status IN (
                        'completed'::execution_status,
                        'failed'::execution_status,
                        'timed_out'::execution_status,
                        'cancelled'::execution_status
                    )
                      AND ea.execution_id IS NULL
                )::bigint AS missing_aggregate_count,
                COUNT(e.id) FILTER (WHERE e.status = 'cancelled'::execution_status)::bigint AS cancelled_execution_count
            FROM executions e
            LEFT JOIN execution_aggregates ea
              ON ea.run_id = e.run_id
             AND ea.run_shard = e.run_shard
             AND ea.execution_id = e.id
             AND ea.attempt_id = e.current_attempt_id
             AND EXISTS (SELECT 1 FROM execution_attempts a
                 WHERE a.run_id=e.run_id AND a.run_shard=e.run_shard
                   AND a.execution_id=e.id AND a.id=e.current_attempt_id
                   AND a.status IN ('completed','failed_agent_call','failed_evaluation','timed_out'))
            WHERE e.run_id = $1::uuid
        ),
        chunk_counts AS (
            SELECT
                COALESCE(SUM(ordinal_end - ordinal_start), 0)::bigint AS expected_execution_count,
                COUNT(*)::bigint AS chunk_count,
                COUNT(*) FILTER (WHERE status = 'completed')::bigint AS completed_chunk_count,
                COUNT(*) FILTER (WHERE status = 'failed')::bigint AS failed_chunk_count,
                COUNT(*) FILTER (WHERE status = 'cancelled')::bigint AS cancelled_chunk_count
            FROM run_chunks
            WHERE run_id = $1::uuid
        )
        SELECT
            chunk_counts.expected_execution_count,
            execution_counts.execution_count,
            execution_counts.terminal_execution_count,
            execution_counts.passed_execution_count,
            execution_counts.failed_execution_count,
            execution_counts.errored_execution_count,
            execution_counts.skipped_execution_count,
            execution_counts.missing_aggregate_count,
            chunk_counts.chunk_count,
            chunk_counts.completed_chunk_count,
            chunk_counts.failed_chunk_count,
            chunk_counts.cancelled_chunk_count,
            execution_counts.cancelled_execution_count
        FROM execution_counts, chunk_counts
        "#,
    )
    .bind(run_id)
    .fetch_one(&mut **tx)
    .await?;

    Ok(counts)
}

#[allow(dead_code)]
pub(super) async fn select_run_shards(db: &PgPool, run_id: Uuid) -> anyhow::Result<Vec<i16>> {
    let shards = sqlx::query_scalar::<_, i16>(
        r#"
        SELECT DISTINCT run_shard
        FROM run_chunks
        WHERE run_id = $1::uuid
        ORDER BY run_shard
        "#,
    )
    .bind(run_id)
    .fetch_all(db)
    .await?;

    Ok(shards)
}

pub(super) async fn enqueue_cancelled_event(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    outcome: &CancelRunOutcome,
) -> anyhow::Result<i64> {
    // Query behavior: insert the durable run.cancelled event with a deterministic
    // dedupe key. Repeated cancellation calls return zero inserted events.
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        WITH inserted AS (
            INSERT INTO outbox_events (
                event_type,
                aggregate_type,
                aggregate_id,
                dedupe_key,
                payload
            )
            VALUES (
                'run.cancelled',
                'run',
                $1::uuid,
                format('run:%s:cancelled', $1::uuid),
                jsonb_build_object(
                    'run_id', $1::uuid,
                    'run_key', $2::text,
                    'status', $3::text,
                    'gate_status', $4::text,
                    'expected_execution_count', $5::int,
                    'terminal_execution_count', $6::int,
                    'chunks_cancelled', $7::bigint,
                    'executions_cancelled', $8::bigint,
                    'attempts_cancelled', $9::bigint
                )
            )
            ON CONFLICT (dedupe_key) DO NOTHING
            RETURNING 1
        )
        SELECT COUNT(*)::bigint
        FROM inserted
        "#,
    )
    .bind(outcome.run.id)
    .bind(&outcome.run.run_key)
    .bind(&outcome.run.status)
    .bind(&outcome.run.gate_status)
    .bind(outcome.run.expected_execution_count)
    .bind(outcome.run.terminal_execution_count)
    .bind(outcome.chunks_cancelled)
    .bind(outcome.executions_cancelled)
    .bind(outcome.attempts_cancelled)
    .fetch_one(&mut **tx)
    .await?;

    Ok(count)
}
