//! Batched attempt allocation under verified chunk ownership.

use super::super::*;

/// Allocates durable execution attempts for a chunk case batch.
///
/// Transaction behavior:
/// - Materializes the input cases as an inline table to preserve chunk order.
/// - Takes a shared run-state guard so workers for other chunks can continue,
///   while cancellation/finalization still waits for this short write.
/// - Upserts execution rows without resetting durable retry or terminal state.
/// - Allocates attempts in a second statement so PostgreSQL can observe rows
///   inserted or updated by the execution upsert.
/// - Splits rows into terminal, retry-waiting, exhausted-open, and retry-eligible
///   buckets.
/// - For eligible rows, marks older running attempts stale, increments the
///   execution attempt number, inserts a new running attempt, and stores it as
///   the current authoritative attempt.
///
/// The returned flags tell the worker whether a case should run now, wait for
/// `retry_after`, be skipped because it is already terminal, or be failed
/// because its retry budget is exhausted.
#[allow(clippy::too_many_arguments)]
pub(in crate::db::workflows::execution_processing) async fn allocate_execution_attempts_for_cases(
    db: &PgPool,
    chunk: &RunChunk,
    lease: &AttemptLeaseContext,
    run_profile: &RunProfile,
    cases: &[chunk_processing::WorkerCaseBatchItem],
    evaluation_plans_by_case: &[CaseEvaluationPlan],
) -> anyhow::Result<Vec<AttemptAllocation>> {
    let Some(max_attempts) = validate_attempt_allocation_batch(
        cases.len(),
        evaluation_plans_by_case.len(),
        run_profile.defaults.max_attempts,
        lease.lease_seconds,
    )?
    else {
        return Ok(Vec::new());
    };
    let run_id = chunk.run_id;
    let run_shard = chunk.run_shard;
    let chunk_id = chunk.id;

    struct AllocationInput<'a> {
        case: &'a chunk_processing::WorkerCaseBatchItem,
        profile_group_id: &'a str,
        evaluator_manifest: serde_json::Value,
        expected_evaluator_count: i32,
        tags: serde_json::Value,
        input_payload: serde_json::Value,
        expected_output: serde_json::Value,
        case_metadata: serde_json::Value,
        input_ordinal: i32,
    }

    let inputs = cases
        .iter()
        .zip(evaluation_plans_by_case)
        .enumerate()
        .map(|(index, (case, plan))| {
            Ok(AllocationInput {
                case,
                profile_group_id: &plan.profile_group_id,
                evaluator_manifest: persisted_evaluator_manifest(
                    run_profile,
                    &plan.evaluator_bindings,
                )?,
                expected_evaluator_count: i32::try_from(plan.evaluator_bindings.len())?,
                tags: persisted_case_tags(run_profile, &case.tags),
                input_payload: persisted_case_input_payload(run_profile, case),
                expected_output: persisted_case_expected_output(run_profile, case),
                case_metadata: persisted_case_metadata(run_profile, case),
                input_ordinal: i32::try_from(index)?,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    // Transaction outline:
    //
    // upsert input      - chunk cases and resolved evaluator manifests.
    // run_guard         - shared lifecycle lock held through attempt allocation.
    // execution upsert  - create/update rows without clearing retry state.
    // allocation input  - case ids and ordinals used to preserve chunk order.
    // attempt_policy    - max_attempts copied into SQL for retry decisions.
    // attempt_state     - durable state after the upsert.
    // terminal_or_closed - already terminal rows, skipped by worker.
    // retry_waiting     - retry_scheduled rows whose retry_after is not due.
    // exhausted_open    - open rows at max_attempts, failed by caller.
    // retry_eligible    - rows that should receive a new attempt now.
    // superseded_attempts/bumped/inserted_attempt
    //                   - create the new running attempt.
    // authority update  - point each execution at its new attempt.
    let mut tx = db.begin().await?;
    crate::db::scope::set_system(&mut tx, chunk.tenant_id, chunk.run_id).await?;
    lock_live_chunk_lease(&mut tx, chunk).await?;
    let mut query_builder = QueryBuilder::<Postgres>::new(
        r#"
        WITH input (
            case_id,
            case_hash,
            task_type,
            tags,
            input_payload,
            expected_output,
            case_metadata,
            profile_group_id,
            evaluator_manifest,
            expected_evaluator_count,
            input_ordinal
        ) AS (
        "#,
    );

    query_builder.push_values(inputs.iter(), |mut b, row| {
        b.push_bind(row.case.case_id)
            .push_bind(&row.case.case_hash)
            .push_bind(&row.case.task_type)
            .push_bind(jsonb_text(&row.tags))
            .push_unseparated("::jsonb")
            .push_bind(jsonb_text(&row.input_payload))
            .push_unseparated("::jsonb")
            .push_bind(jsonb_text(&row.expected_output))
            .push_unseparated("::jsonb")
            .push_bind(jsonb_text(&row.case_metadata))
            .push_unseparated("::jsonb")
            .push_bind(row.profile_group_id)
            .push_bind(jsonb_text(&row.evaluator_manifest))
            .push_unseparated("::jsonb")
            .push_bind(row.expected_evaluator_count)
            .push_bind(row.input_ordinal);
    });

    query_builder.push(
        r#"
        ),
        run_guard AS (
            SELECT run_id AS id
            FROM run_snapshots
            WHERE run_id =
        "#,
    );
    query_builder.push_bind(run_id);
    query_builder.push(
        r#"::uuid
              AND run_shard =
        "#,
    );
    query_builder.push_bind(run_shard);
    query_builder.push(
        r#"
            FOR SHARE
        )
        INSERT INTO executions (
                run_id,
                run_shard,
                chunk_id,
                case_id,
                case_hash,
                profile_group_id,
                task_type,
                tags,
                input_payload,
                expected_output,
                case_metadata,
                evaluation_profile_id,
                evaluation_profile_version,
                evaluator_manifest,
                expected_evaluator_count,
                status,
                started_at,
                updated_at
            )
            SELECT
                "#,
    );
    query_builder.push_bind(run_id);
    query_builder.push(
        r#"::uuid,
                "#,
    );
    query_builder.push_bind(run_shard);
    query_builder.push(
        r#",
                "#,
    );
    query_builder.push_bind(chunk_id);
    query_builder.push(
        r#"::uuid,
                input.case_id,
                input.case_hash,
                input.profile_group_id,
                input.task_type,
                input.tags::jsonb,
                input.input_payload::jsonb,
                input.expected_output::jsonb,
                input.case_metadata::jsonb,
                "#,
    );
    query_builder.push_bind(&run_profile.profile_id);
    query_builder.push(",");
    query_builder.push_bind(&run_profile.profile_version);
    query_builder.push(
        r#",
                input.evaluator_manifest::jsonb,
                input.expected_evaluator_count,
                'pending'::execution_status,
                NULL,
                now()
            FROM input
            JOIN run_guard
              ON true
            ON CONFLICT (run_id, run_shard, case_id) DO UPDATE
            SET case_hash = EXCLUDED.case_hash,
                profile_group_id = EXCLUDED.profile_group_id,
                task_type = EXCLUDED.task_type,
                tags = EXCLUDED.tags,
                input_payload = EXCLUDED.input_payload,
                expected_output = EXCLUDED.expected_output,
                case_metadata = EXCLUDED.case_metadata,
                evaluation_profile_id = EXCLUDED.evaluation_profile_id,
                evaluation_profile_version = EXCLUDED.evaluation_profile_version,
                evaluator_manifest = EXCLUDED.evaluator_manifest,
                expected_evaluator_count = EXCLUDED.expected_evaluator_count,
                updated_at = now()
        "#,
    );

    let upserted = query_builder.build().execute(&mut *tx).await?;
    if upserted.rows_affected() != cases.len() as u64 {
        anyhow::bail!(
            "upserted {} executions for {} cases",
            upserted.rows_affected(),
            cases.len()
        );
    }

    let mut query_builder = QueryBuilder::<Postgres>::new(
        r#"
        WITH input (case_id, input_ordinal) AS (
        "#,
    );
    query_builder.push_values(inputs.iter(), |mut b, row| {
        b.push_bind(row.case.case_id).push_bind(row.input_ordinal);
    });
    query_builder.push(
        r#"
        ),
        attempt_policy AS (
            SELECT
        "#,
    );
    query_builder.push_bind(max_attempts);
    query_builder.push(
        r#"::int AS max_attempts
        ),
        attempt_lease AS (
            SELECT
                "#,
    );
    query_builder.push_bind(lease.worker_id);
    query_builder.push(
        r#"::uuid AS worker_id,
                "#,
    );
    query_builder.push_bind(&lease.worker_host);
    query_builder.push(
        r#"::text AS worker_host,
                "#,
    );
    query_builder.push_bind(lease.queue_message_id);
    query_builder.push(
        r#"::uuid AS queue_message_id,
                "#,
    );
    query_builder.push_bind(&lease.broker_message_id);
    query_builder.push(
        r#"::text AS broker_message_id,
                "#,
    );
    query_builder.push_bind(lease.lease_seconds);
    query_builder.push(
        r#"::int AS lease_seconds
        ),
        attempt_state AS (
            SELECT
                input.case_id,
                input.input_ordinal,
                executions.id AS execution_id,
                executions.run_id,
                executions.run_shard,
                executions.status,
                executions.current_attempt_id,
                executions.current_attempt_no,
                executions.retry_after
            FROM input
            JOIN executions
              ON executions.run_id =
        "#,
    );
    query_builder.push_bind(run_id);
    query_builder.push(
        r#"::uuid
             AND executions.run_shard =
        "#,
    );
    query_builder.push_bind(run_shard);
    query_builder.push(
        r#"
             AND executions.case_id = input.case_id
        ),
        terminal_or_closed AS (
            SELECT
                attempt_state.case_id,
                attempt_state.execution_id,
                attempt_state.current_attempt_id AS attempt_id,
                attempt_state.current_attempt_no AS attempt_no,
                true AS already_terminal,
                false AS retry_not_due,
                false AS max_attempts_exhausted,
                attempt_state.input_ordinal
            FROM attempt_state, attempt_policy
            WHERE attempt_state.status IN (
                    'completed'::execution_status,
                    'failed'::execution_status,
                    'timed_out'::execution_status,
                    'cancelled'::execution_status
                )
        ),
        retry_waiting AS (
            SELECT
                attempt_state.case_id,
                attempt_state.execution_id,
                attempt_state.current_attempt_id AS attempt_id,
                attempt_state.current_attempt_no AS attempt_no,
                false AS already_terminal,
                true AS retry_not_due,
                false AS max_attempts_exhausted,
                attempt_state.input_ordinal
            FROM attempt_state, attempt_policy
            WHERE attempt_state.status = 'retry_scheduled'::execution_status
              AND attempt_state.current_attempt_no < attempt_policy.max_attempts
              AND attempt_state.retry_after > now()
        ),
        exhausted_open AS (
            SELECT
                attempt_state.case_id,
                attempt_state.execution_id,
                attempt_state.current_attempt_id AS attempt_id,
                attempt_state.current_attempt_no AS attempt_no,
                false AS already_terminal,
                false AS retry_not_due,
                true AS max_attempts_exhausted,
                attempt_state.input_ordinal
            FROM attempt_state, attempt_policy
            WHERE attempt_state.status IN (
                    'pending'::execution_status,
                    'running'::execution_status,
                    'awaiting_evaluators'::execution_status,
                    'retry_scheduled'::execution_status
                )
              AND attempt_state.current_attempt_no >= attempt_policy.max_attempts
              AND attempt_state.current_attempt_id IS NOT NULL
        ),
        retry_eligible AS (
            SELECT
                attempt_state.case_id,
                attempt_state.input_ordinal,
                attempt_state.execution_id AS id,
                attempt_state.run_id,
                attempt_state.run_shard
            FROM attempt_state, attempt_policy
            WHERE attempt_state.status IN (
                    'pending'::execution_status,
                    'running'::execution_status,
                    'awaiting_evaluators'::execution_status,
                    'retry_scheduled'::execution_status
                )
              AND attempt_state.current_attempt_no < attempt_policy.max_attempts
              AND (
                    attempt_state.status <> 'retry_scheduled'::execution_status
                    OR attempt_state.retry_after IS NULL
                    OR attempt_state.retry_after <= now()
              )
        ),
        superseded_attempts AS (
            UPDATE execution_attempts
            SET status = 'stale'::attempt_status,
                error_message = COALESCE(
                    execution_attempts.error_message,
                    'attempt superseded by a newer worker attempt'
                ),
                completed_at = COALESCE(execution_attempts.completed_at, now()),
                updated_at = now()
            FROM retry_eligible
            WHERE execution_attempts.run_id = retry_eligible.run_id
              AND execution_attempts.run_shard = retry_eligible.run_shard
              AND execution_attempts.execution_id = retry_eligible.id
              AND execution_attempts.status = 'running'::attempt_status
            RETURNING execution_attempts.id
        ),
        bumped AS (
            UPDATE executions
            SET status = 'running'::execution_status,
                current_attempt_no = executions.current_attempt_no + 1,
                last_error_message = NULL,
                retry_after = NULL,
                started_at = COALESCE(executions.started_at, now()),
                completed_at = NULL,
                updated_at = now()
            FROM retry_eligible
            WHERE executions.run_id = retry_eligible.run_id
              AND executions.run_shard = retry_eligible.run_shard
              AND executions.id = retry_eligible.id
            RETURNING
                executions.id AS execution_id,
                executions.run_id,
                executions.run_shard,
                executions.current_attempt_no AS attempt_no
        ),
        inserted_attempt AS (
            INSERT INTO execution_attempts (
                execution_id,
                run_id,
                run_shard,
                attempt_no,
                worker_id,
                worker_host,
                queue_message_id,
                broker_message_id,
                status,
                leased_until,
                heartbeat_at,
                started_at,
                created_at,
                updated_at
            )
            SELECT
                bumped.execution_id,
                bumped.run_id,
                bumped.run_shard,
                bumped.attempt_no,
                attempt_lease.worker_id,
                attempt_lease.worker_host,
                attempt_lease.queue_message_id,
                attempt_lease.broker_message_id,
                'running'::attempt_status,
                now() + (attempt_lease.lease_seconds * interval '1 second'),
                now(),
                now(),
                now(),
                now()
            FROM bumped
            CROSS JOIN attempt_lease
            RETURNING id AS attempt_id, execution_id, run_id, run_shard, attempt_no
        ),
        allocated AS (
            SELECT
                retry_eligible.case_id,
                inserted_attempt.execution_id,
                inserted_attempt.attempt_id,
                inserted_attempt.attempt_no,
                true AS should_process,
                false AS already_terminal,
                false AS retry_not_due,
                false AS max_attempts_exhausted,
                retry_eligible.input_ordinal
            FROM inserted_attempt
            JOIN retry_eligible
              ON retry_eligible.id = inserted_attempt.execution_id
            UNION ALL
            SELECT
                terminal_or_closed.case_id,
                terminal_or_closed.execution_id,
                terminal_or_closed.attempt_id,
                terminal_or_closed.attempt_no,
                false AS should_process,
                terminal_or_closed.already_terminal,
                terminal_or_closed.retry_not_due,
                terminal_or_closed.max_attempts_exhausted,
                terminal_or_closed.input_ordinal
            FROM terminal_or_closed
            UNION ALL
            SELECT
                retry_waiting.case_id,
                retry_waiting.execution_id,
                retry_waiting.attempt_id,
                retry_waiting.attempt_no,
                false AS should_process,
                retry_waiting.already_terminal,
                retry_waiting.retry_not_due,
                retry_waiting.max_attempts_exhausted,
                retry_waiting.input_ordinal
            FROM retry_waiting
            UNION ALL
            SELECT
                exhausted_open.case_id,
                exhausted_open.execution_id,
                exhausted_open.attempt_id,
                exhausted_open.attempt_no,
                false AS should_process,
                exhausted_open.already_terminal,
                exhausted_open.retry_not_due,
                exhausted_open.max_attempts_exhausted,
                exhausted_open.input_ordinal
            FROM exhausted_open
        )
        SELECT
            allocated.case_id,
            allocated.execution_id,
            allocated.attempt_id,
            allocated.attempt_no,
            allocated.should_process,
            allocated.already_terminal,
            allocated.retry_not_due,
            allocated.max_attempts_exhausted
        FROM allocated
        ORDER BY allocated.input_ordinal
        "#,
    );

    let allocations = query_builder
        .build_query_as::<AttemptAllocation>()
        .fetch_all(&mut *tx)
        .await?;

    if allocations.len() != cases.len() {
        anyhow::bail!(
            "allocated {} execution attempts for {} cases",
            allocations.len(),
            cases.len()
        );
    }

    let new_attempts = allocations
        .iter()
        .filter(|allocation| allocation.should_process)
        .map(|allocation| {
            allocation
                .attempt_id
                .map(|attempt_id| (allocation.execution_id, attempt_id))
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "execution '{}' was allocated without an attempt id",
                        allocation.execution_id
                    )
                })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    if !new_attempts.is_empty() {
        let mut authority_update = QueryBuilder::<Postgres>::new(
            r#"
            WITH input (execution_id, attempt_id) AS (
            "#,
        );
        authority_update.push_values(&new_attempts, |mut b, (execution_id, attempt_id)| {
            b.push_bind(execution_id).push_bind(attempt_id);
        });
        authority_update.push(
            r#"
            )
            UPDATE executions
            SET current_attempt_id = input.attempt_id,
                updated_at = now()
            FROM input
            WHERE executions.run_id =
            "#,
        );
        authority_update.push_bind(run_id);
        authority_update.push(
            r#"::uuid
              AND executions.run_shard =
            "#,
        );
        authority_update.push_bind(run_shard);
        authority_update.push(
            r#"
              AND executions.id = input.execution_id
            "#,
        );

        let updated = authority_update.build().execute(&mut *tx).await?;
        if updated.rows_affected() != new_attempts.len() as u64 {
            anyhow::bail!(
                "updated {} current execution attempts for {} allocations",
                updated.rows_affected(),
                new_attempts.len()
            );
        }
    }

    tx.commit().await?;

    Ok(allocations)
}

pub(in crate::db::workflows::execution_processing) async fn lock_live_chunk_lease(
    tx: &mut Transaction<'_, Postgres>,
    chunk: &RunChunk,
) -> anyhow::Result<()> {
    let lease_token = chunk.lease_token.ok_or_else(|| {
        anyhow::anyhow!(
            "chunk {} for run {} shard {} has no lease token",
            chunk.id,
            chunk.run_id,
            chunk.run_shard
        )
    })?;
    let locked = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT id
        FROM run_chunks
        WHERE run_id = $1::uuid
          AND run_shard = $2
          AND id = $3::uuid
          AND status = 'leased'
          AND lease_token = $4::uuid
          AND leased_until >= now()
          AND EXISTS (
              SELECT 1
              FROM local_shard_admissions admission
              WHERE admission.run_id = run_chunks.run_id
                AND admission.run_shard = run_chunks.run_shard
                AND admission.write_epoch = $5
                AND admission.state IN ('open', 'draining')
          )
        FOR UPDATE
        "#,
    )
    .bind(chunk.run_id)
    .bind(chunk.run_shard)
    .bind(chunk.id)
    .bind(lease_token)
    .bind(chunk.write_epoch)
    .fetch_optional(&mut **tx)
    .await?;

    if locked.is_none() {
        anyhow::bail!(
            "chunk {} for run {} shard {} no longer owns a live lease",
            chunk.id,
            chunk.run_id,
            chunk.run_shard
        );
    }
    Ok(())
}
