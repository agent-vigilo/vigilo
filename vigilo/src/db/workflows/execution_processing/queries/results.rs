//! Atomic evaluator evidence and aggregate persistence under verified ownership.

use super::{
    super::*,
    allocation::lock_live_chunk_lease,
};

/// Persists successful evaluator evidence and execution aggregates.
///
/// Query behavior:
/// - Locks and validates the live chunk claim.
/// - Checks every completed record still owns a live worker attempt, then locks
///   that attempt with its current execution.
/// - Inserts evaluator results as append-oriented evidence; uniqueness handles
///   redelivery by turning duplicate result rows into conflicts.
/// - Upserts aggregates for completed attempts after evidence insertion.
/// - Leaves execution status changes to `finalize_execution_terminal_transitions`
///   so state mutation remains one authority-checked batch.
pub(in crate::db::workflows::execution_processing) async fn persist_completed_execution_results_batch(
    db: &PgPool,
    chunk: &RunChunk,
    worker_id: Uuid,
    completed: &[CompletedExecutionPersistence],
) -> anyhow::Result<BatchPersistenceStats> {
    if completed.is_empty() {
        return Ok(BatchPersistenceStats::default());
    }

    // Query outline:
    //
    // authority_query - short shared run-state/current-attempt guard.
    // result insert   - append evaluator evidence for authoritative attempts.
    // aggregate upsert- summarize completed attempts for terminal transition.
    //
    // The shared run-state guard preserves cancellation/finalization ordering
    // without turning the run row into an exclusive worker-side mutex.
    let mut tx = db.begin().await?;
    crate::db::scope::set_system(&mut tx, chunk.tenant_id, chunk.run_id).await?;
    lock_live_chunk_lease(&mut tx, chunk).await?;
    let run_id = chunk.run_id;
    let run_shard = chunk.run_shard;

    let mut authority_query = QueryBuilder::<Postgres>::new(
        r#"
        WITH input (
            execution_id,
            attempt_id,
            attempt_no
        ) AS (
        "#,
    );
    authority_query.push_values(completed, |mut b, row| {
        b.push_bind(row.execution_id)
            .push_bind(row.attempt_id)
            .push_bind(row.attempt_no);
    });
    authority_query.push(
        r#"
        ),
        run_guard AS (
            SELECT run_id AS id
            FROM run_snapshots
            WHERE run_id =
        "#,
    );
    authority_query.push_bind(run_id);
    authority_query.push(
        r#"::uuid
              AND run_shard =
        "#,
    );
    authority_query.push_bind(run_shard);
    authority_query.push(
        r#"
            FOR SHARE
        ),
        locked AS (
            SELECT executions.id
        FROM run_guard
        JOIN executions
          ON executions.run_id = run_guard.id
         AND executions.run_shard =
        "#,
    );
    authority_query.push_bind(run_shard);
    authority_query.push(
        r#"
        JOIN input
          ON input.execution_id = executions.id
        JOIN execution_attempts
          ON execution_attempts.run_id = executions.run_id
         AND execution_attempts.run_shard = executions.run_shard
         AND execution_attempts.execution_id = executions.id
         AND execution_attempts.id = input.attempt_id
        WHERE executions.current_attempt_id = input.attempt_id
          AND executions.current_attempt_no = input.attempt_no
          AND execution_attempts.attempt_no = input.attempt_no
          AND execution_attempts.status = 'running'::attempt_status
          AND execution_attempts.worker_id =
        "#,
    );
    authority_query.push_bind(worker_id);
    authority_query.push(
        r#"::uuid
          AND execution_attempts.leased_until >= now()
        FOR UPDATE OF executions, execution_attempts
        )
        SELECT COUNT(*)::bigint
        FROM locked
        "#,
    );

    let current_attempt_count = authority_query
        .build_query_scalar::<i64>()
        .fetch_one(&mut *tx)
        .await?;
    if usize::try_from(current_attempt_count)? != completed.len() {
        anyhow::bail!(
            "aggregate persistence batch locked {} current executions out of {}; at least one attempt lost authority",
            current_attempt_count,
            completed.len()
        );
    }

    let result_rows = completed
        .iter()
        .flat_map(|row| row.result_rows.iter().cloned())
        .collect::<Vec<_>>();
    let evaluator_results_inserted =
        evaluator_results::insert_evaluator_results_batch(&mut tx, &result_rows).await?;

    let mut aggregate_query = QueryBuilder::<Postgres>::new(
        r#"
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
            summary
        )
        "#,
    );
    aggregate_query.push_values(completed, |mut b, row| {
        b.push_bind(row.execution_id)
            .push_bind(run_id)
            .push_bind(run_shard)
            .push_bind(row.attempt_id)
            .push_bind(&row.overall_status)
            .push_unseparated("::evaluation_status")
            .push_bind(row.aggregate_score)
            .push_bind(row.evaluator_result_count)
            .push_bind(&row.dimension_scores)
            .push_bind(&row.blocking_failures)
            .push_bind(&row.summary)
            .push_unseparated("::jsonb");
    });
    aggregate_query.push(
        r#"
        ON CONFLICT (run_id, run_shard, execution_id) DO UPDATE
        SET attempt_id = EXCLUDED.attempt_id,
            overall_status = EXCLUDED.overall_status,
            aggregate_score = EXCLUDED.aggregate_score,
            evaluator_result_count = EXCLUDED.evaluator_result_count,
            dimension_scores = EXCLUDED.dimension_scores,
            blocking_failures = EXCLUDED.blocking_failures,
            summary = EXCLUDED.summary,
            updated_at = now()
        "#,
    );
    aggregate_query.build().execute(&mut *tx).await?;

    tx.commit().await?;

    let evaluator_results_attempted = result_rows.len();
    Ok(BatchPersistenceStats {
        evaluator_results_attempted,
        evaluator_results_inserted,
        evaluator_result_conflicts: evaluator_results_attempted
            .saturating_sub(evaluator_results_inserted as usize),
    })
}
