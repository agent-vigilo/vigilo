//! Chunk-local terminal receipts and retirement of abandoned attempt authority.
//!
//! Every terminal producer calls this inside its chunk-state transaction. The
//! chunk lock serializes result persistence and terminal transitions; fencing
//! preserves evidence while excluding unfinished/stale attempts from scoring.

use serde_json::{
    Value,
    json,
};
use sqlx::{
    Postgres,
    Transaction,
};
use uuid::Uuid;

use super::RunShardSummary;
use crate::{
    contracts::run::RunProfile,
    db::workflows::{
        chunk_processing::WorkerCaseBatchItem,
        execution_processing,
    },
};

/// Creates a receipt only after locking terminal chunk state and retiring writers.
/// Replays verify immutable identity and leave the original evidence unchanged.
pub(crate) async fn settle_chunk_with(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    shard: i16,
    chunk_id: Uuid,
) -> anyhow::Result<()> {
    let identity: (i32, i32, String) = sqlx::query_as(
        "SELECT ordinal_start, ordinal_end, status FROM run_chunks WHERE run_id = $1 AND run_shard = $2 AND id = $3 FOR UPDATE",
    ).bind(run_id).bind(shard).bind(chunk_id).fetch_one(&mut **tx).await?;
    anyhow::ensure!(
        matches!(identity.2.as_str(), "completed" | "failed" | "cancelled"),
        "cannot settle open chunk {chunk_id}"
    );
    super::queries::ensure_snapshot_with(tx, run_id, shard).await?;
    settle_locked_chunk_with(tx, run_id, shard, chunk_id, identity).await
}

/// Settles the identity returned by a terminal chunk update in this transaction.
///
/// The caller must retain the chunk lock and have an existing local snapshot.
/// Worker claims require that snapshot, so completion never initializes it again.
/// A separate read after the update observes evidence committed by writers that
/// held the chunk lock first; caller-side cached chunk metadata is not trusted.
pub(crate) async fn settle_locked_chunk_with(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    shard: i16,
    chunk_id: Uuid,
    (start, end, disposition): (i32, i32, String),
) -> anyhow::Result<()> {
    anyhow::ensure!(
        matches!(disposition.as_str(), "completed" | "failed" | "cancelled"),
        "cannot settle open chunk {chunk_id}"
    );
    let (snapshot, policy, receipt_matches, _plan_mode): (Value, String, Option<bool>, String) =
        sqlx::query_as(
            r#"
        SELECT snapshot.config_snapshot, snapshot.aggregation_policy_hash,
            CASE WHEN receipt.chunk_id IS NOT NULL THEN
                receipt.ordinal_start = $4 AND receipt.ordinal_end = $5
                AND receipt.disposition = $6
                AND receipt.aggregation_policy_hash = snapshot.aggregation_policy_hash
            END AS receipt_matches,
            set_config('plan_cache_mode', 'force_custom_plan', true)
        FROM run_snapshots snapshot
        LEFT JOIN run_chunk_settlements receipt
            ON receipt.run_id = snapshot.run_id
            AND receipt.run_shard = snapshot.run_shard
            AND receipt.chunk_id = $3
        WHERE snapshot.run_id = $1 AND snapshot.run_shard = $2
        "#,
        )
        // Generic plans retain every partition lock, including PostgreSQL's internal
        // foreign-key queries. Keep this read custom and force custom plans for the
        // remaining transaction; set_config(..., true) resets at commit or rollback.
        .persistent(false)
        .bind(run_id)
        .bind(shard)
        .bind(chunk_id)
        .bind(start)
        .bind(end)
        .bind(&disposition)
        .fetch_one(&mut **tx)
        .await?;
    if let Some(matches) = receipt_matches {
        anyhow::ensure!(matches, "conflicting chunk settlement {chunk_id}");
        return Ok(());
    }
    // Mark abandoned authority stale without fabricating evaluator errors or
    // promoting a persisted aggregate whose terminal transition never committed.
    // These updates touch different rows under the already-held chunk lock and
    // do not depend on each other's new values. Receipt calculation stays in a
    // subsequent statement so its snapshot sees both sets of retired writers.
    sqlx::query(r#"
        WITH retired_attempts AS (
        UPDATE execution_attempts a
        SET status = 'stale', leased_until = NULL, completed_at = COALESCE(a.completed_at, now()), error_message = COALESCE(a.error_message, 'chunk settled before attempt completion'), updated_at = now()
        FROM executions e
        WHERE e.run_id = $1
        AND e.run_shard = $2
        AND e.chunk_id = $3
        AND a.run_id = e.run_id
        AND a.run_shard = e.run_shard
        AND a.execution_id = e.id
        AND a.status IN ('pending', 'running')
        )
        UPDATE executions
        SET status = CASE WHEN $4 = 'cancelled' THEN 'cancelled'::execution_status ELSE 'failed'::execution_status END, retry_after = NULL, completed_at = COALESCE(completed_at, now()), last_error_message = COALESCE(last_error_message, 'chunk settled with unresolved work'), updated_at = now()
        WHERE run_id = $1
        AND run_shard = $2
        AND chunk_id = $3
        AND status IN ('pending', 'running', 'awaiting_evaluators', 'retry_scheduled')
        "#)
        .bind(run_id).bind(shard).bind(chunk_id).bind(&disposition).execute(&mut **tx).await?;

    let has_gates = snapshot
        .pointer("/profile/scorecard/gates")
        .and_then(Value::as_array)
        .is_some_and(|g| !g.is_empty());
    let planned = if has_gates {
        let profile: RunProfile = serde_json::from_value(snapshot["profile"].clone())?;
        let cases = sqlx::query_as::<_, WorkerCaseBatchItem>(
            r#"
        SELECT p.case_id, p.case_hash, p.case_ordinal, b.task_type, b.case_group, 'null'::jsonb AS input_payload, 'null'::jsonb AS expected_output, 'null'::jsonb AS context_payload, b.tags, '{}'::jsonb AS metadata
        FROM run_shard_cases p
        JOIN case_blobs b USING (case_hash)
        WHERE p.run_id = $1
        AND p.run_shard = $2
        AND p.case_ordinal >= $3
        AND p.case_ordinal < $4
        ORDER BY p.case_ordinal
        "#,
        ).bind(run_id).bind(shard).bind(start).bind(end).fetch_all(&mut **tx).await?;
        anyhow::ensure!(
            cases.len() == usize::try_from(end - start)?
                && cases
                    .iter()
                    .enumerate()
                    .all(|(i, c)| c.case_ordinal == start + i as i32),
            "chunk {chunk_id} has incomplete planned scorecard membership"
        );
        cases
            .iter()
            .map(|case| {
                let plan = execution_processing::evaluation_plan_for_case(&profile, case)?;
                Ok(
                    json!({"case_id": case.case_id, "profile_group_id": plan.profile_group_id,
                "tags": case.tags, "evaluator_manifest": plan.evaluator_bindings}),
                )
            })
            .collect::<anyhow::Result<Vec<_>>>()?
    } else {
        Vec::new()
    };
    let value: Value = sqlx::query_scalar(include_str!("receipt.sql"))
        .bind(run_id)
        .bind(shard)
        .bind(chunk_id)
        .bind(json!(planned))
        .fetch_one(&mut **tx)
        .await?;
    let receipt: RunShardSummary = serde_json::from_value(value.clone())?;
    anyhow::ensure!(
        receipt.missing_execution_count >= 0 && receipt.unresolved_execution_count >= 0,
        "chunk {chunk_id} contains more executions or aggregates than planned"
    );
    anyhow::ensure!(
        serde_json::to_vec(&value)?.len() <= 1024 * 1024,
        "chunk {chunk_id} settlement exceeds the receipt byte limit"
    );
    sqlx::query(r#"
        WITH receipt AS (INSERT INTO run_chunk_settlements (run_id, run_shard, chunk_id, ordinal_start, ordinal_end, aggregation_policy_hash, disposition, summary) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        RETURNING run_id,run_shard,chunk_id,ordinal_start) INSERT INTO run_chunk_settlement_consumptions (run_id,run_shard,chunk_id,ordinal_start) SELECT run_id,run_shard,chunk_id,ordinal_start
        FROM receipt
        "#)
        .bind(run_id).bind(shard).bind(chunk_id).bind(start).bind(end)
        .bind(policy).bind(disposition).bind(value).execute(&mut **tx).await?;
    Ok(())
}

/// Adds missing receipts to already-terminal chunks in a caller-owned cleanup
/// transaction, paging identities without retaining the whole shard in memory.
pub(crate) async fn settle_terminal_chunks_with(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    shard: i16,
) -> anyhow::Result<()> {
    loop {
        let ids = sqlx::query_scalar::<_, Uuid>(
            r#"
        SELECT c.id
        FROM run_chunks c
        WHERE c.run_id = $1
        AND c.run_shard = $2
        AND c.status IN ('completed','failed','cancelled')
        AND NOT EXISTS (SELECT 1
        FROM run_chunk_settlements s
        WHERE s.run_id = c.run_id
        AND s.run_shard = c.run_shard
        AND s.chunk_id = c.id)
        ORDER BY c.ordinal_start, c.id
        LIMIT 64
        "#,
        )
        .bind(run_id)
        .bind(shard)
        .fetch_all(&mut **tx)
        .await?;
        if ids.is_empty() {
            return Ok(());
        }
        for id in ids {
            settle_chunk_with(tx, run_id, shard, id).await?;
        }
    }
}

#[cfg(test)]
#[path = "completion_postgres_tests.rs"]
mod completion_postgres_tests;
