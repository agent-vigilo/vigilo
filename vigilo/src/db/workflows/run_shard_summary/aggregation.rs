//! Bounded coordinator receipt aggregation against frozen planned membership.
//!
//! A transaction-scoped, nonblocking claim owns one shard page. Its checkpoint
//! commits with counters, so rollback/connection loss releases ownership without
//! a detached job or a lease-renewal task. Movement's shared admission fence
//! protects the whole page. Workers never take this aggregation claim. A bounded
//! fair queue drains multiple pages and runs independent shard pages concurrently;
//! completed page transactions remain the recovery and visibility boundary.

use std::time::{
    Duration,
    Instant,
};

use anyhow::Context as _;
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::{
    Value,
    json,
};
use sqlx::{
    PgPool,
    Postgres,
    Transaction,
};
use uuid::Uuid;

use super::{
    RunShardSummary,
    aggregation_scheduling::{
        PageOutcome,
        drain,
    },
    plan::{
        ChunkIdentity,
        Manifest,
        snapshot_manifest,
    },
    queries,
};
use crate::contracts::scorecard::{
    ShardScorecard,
    accumulate_chunk_scorecard,
    merge_shard_scorecards,
};

// Compact receipts share more setup work; the byte bound still limits payloads
// for gate-heavy receipts. Candidate pages retain identities and byte lengths,
// fetching JSON only after applying the byte budget. Gate-bearing receipts keep
// a smaller candidate bound because their payload varies with gate populations.
const COMPACT_PAGE_ROWS: i64 = 256;
const GATED_PAGE_ROWS: i64 = 64;
const PAGE_BYTES: i64 = 4 * 1024 * 1024;

/// Per-database aggregation work admitted during one coordinator pass.
/// The time budget stops new page scheduling; already-started transactions finish.
#[derive(Clone, Debug)]
pub(crate) struct AggregationConfig {
    /// Maximum attempted pages, including busy or invalid candidates.
    pub(crate) max_pages: usize,
    /// Page scheduling allowance after discovery; does not cancel active pages.
    pub(crate) time_budget: Duration,
    /// Maximum active shard pages within this database pass.
    pub(crate) parallelism: usize,
}

impl AggregationConfig {
    fn candidate_limit(&self) -> anyhow::Result<i64> {
        anyhow::ensure!(
            self.max_pages > 0,
            "aggregation page budget must be positive"
        );
        anyhow::ensure!(
            self.parallelism > 0,
            "aggregation parallelism must be positive"
        );
        anyhow::ensure!(
            !self.time_budget.is_zero(),
            "aggregation time budget must be positive"
        );
        i64::try_from(self.max_pages).context("aggregation page budget exceeds database limit")
    }
}

#[derive(sqlx::FromRow)]
struct Context {
    admitted: bool,
    snapshot: Option<Value>,
    policy: Option<String>,
    expected: Option<i32>,
    summary: Option<sqlx::types::Json<RunShardSummary>>,
    stored: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct State {
    version: u16,
    policy_hash: String,
    plan: Manifest,
    folded: Manifest,
    final_summary: Option<RunShardSummary>,
}

/// Advances at most one receipt page, returning whether it progressed.
/// Unknown ownership, changed plans, and invalid receipts fail closed. An owned
/// or not-yet-ready shard returns promptly for the next coordinator candidate.
#[cfg(test)]
pub(crate) async fn advance_shard_summary(
    db: &PgPool,
    run_id: Uuid,
    shard: i16,
    write_epoch: i64,
) -> anyhow::Result<bool> {
    Ok(matches!(
        advance_shard_summary_page(db, run_id, shard, write_epoch).await?,
        PageOutcome::Advanced | PageOutcome::Sealed | PageOutcome::CaughtUp
    ))
}

/// Owns one bounded page transaction and reports whether another page may help.
/// The nonblocking shard claim and shared movement fence precede all page reads.
/// A sealing commit ends scheduling without another sealed-summary lookup.
async fn advance_shard_summary_page(
    db: &PgPool,
    run_id: Uuid,
    shard: i16,
    write_epoch: i64,
) -> anyhow::Result<PageOutcome> {
    let mut tx = db.begin().await?;
    let claimed: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtextextended($1::uuid::text || ':summary', $2::bigint))")
        .bind(run_id).bind(i64::from(shard)).fetch_one(&mut *tx).await?;
    if !claimed {
        return Ok(PageOutcome::Deferred);
    }
    crate::db::shard_write_fence::lock_shared(&mut tx, run_id, shard).await?;
    let mut context = load_context(&mut tx, run_id, shard, write_epoch).await?;
    if context
        .summary
        .as_ref()
        .is_some_and(|summary| summary.sealed)
    {
        return Ok(PageOutcome::Deferred);
    }
    // Dispatch and cancellation normally prepare local state. Retain recovery
    // for a missing snapshot/summary without submitting no-op inserts per page.
    if context.snapshot.is_none() {
        queries::ensure_snapshot_with(&mut tx, run_id, shard).await?;
        context = load_context(&mut tx, run_id, shard, write_epoch).await?;
    }
    let (Some(snapshot), Some(policy), Some(expected)) =
        (context.snapshot, context.policy, context.expected)
    else {
        return Ok(PageOutcome::Deferred);
    };
    let planned = snapshot_manifest(&snapshot, shard)?;
    let page_rows = if snapshot
        .pointer("/profile/scorecard/gates")
        .and_then(Value::as_array)
        .is_some_and(|gates| !gates.is_empty())
    {
        GATED_PAGE_ROWS
    } else {
        COMPACT_PAGE_ROWS
    };
    let mut summary = if let Some(summary) = context.summary {
        summary.0
    } else {
        queries::initialize_run_shard_summary_with(&mut tx, run_id, shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("shard {shard} aggregation lost its snapshot"))?
    };
    let mut progressed = context.stored.is_none();
    let mut state: State = if let Some(stored) = context.stored {
        serde_json::from_value(stored)?
    } else {
        summary = RunShardSummary {
            run_id,
            run_shard: shard,
            expected_execution_count: expected,
            status: "running".to_owned(),
            scorecard: json!({"version": 1, "run_shard": shard, "policy_hash": policy, "entries": []}),
            ..Default::default()
        };
        State {
            version: 1,
            policy_hash: policy.clone(),
            plan: planned.clone(),
            folded: Manifest::default(),
            final_summary: None,
        }
    };
    anyhow::ensure!(
        state.version == 1
            && state.policy_hash == policy
            && summary.expected_execution_count == expected,
        "shard {shard} aggregation identity changed"
    );
    anyhow::ensure!(
        state.plan == planned,
        "shard {shard} planned membership changed"
    );
    let plan = &state.plan;
    anyhow::ensure!(
        plan.case_count == expected && plan.chunk_count > 0 && !plan.hash.is_empty(),
        "shard {shard} has invalid planned coverage"
    );
    let mut consumed = Vec::new();
    let ready_again;

    if summary.settled_chunk_count < plan.chunk_count {
        // Progress consumes every available receipt, even behind a slow earlier
        // chunk. The consumption relation prevents late commits being skipped.
        // One extra identity determines whether to revisit without another SQL
        // call; only the configured page's receipt payloads are fetched.
        let receipts = sqlx::query_as::<_, (Uuid, Value, i64)>(
            r#"
        WITH pending AS MATERIALIZED (SELECT chunk_id,ordinal_start
        FROM run_chunk_settlement_consumptions
        WHERE run_id=$1
        AND run_shard=$2
        AND NOT consumed
        ORDER BY ordinal_start,chunk_id
        LIMIT ($3::bigint + 1)),
        page_ids AS MATERIALIZED (SELECT chunk_id,ordinal_start
        FROM pending ORDER BY ordinal_start,chunk_id LIMIT $3),
        candidates AS MATERIALIZED (SELECT p.chunk_id,p.ordinal_start,octet_length(s.summary::text) AS receipt_bytes
        FROM page_ids p
        JOIN run_chunk_settlements s ON s.run_id=$1
        AND s.run_shard=$2
        AND s.chunk_id=p.chunk_id), bounded AS (SELECT *, SUM(receipt_bytes) OVER (ORDER BY ordinal_start,chunk_id) AS bytes
        FROM candidates) SELECT b.chunk_id,s.summary,(SELECT count(*) FROM pending) AS pending_count
        FROM bounded b
        JOIN run_chunk_settlements s ON s.run_id=$1 AND s.run_shard=$2 AND s.chunk_id=b.chunk_id
        WHERE b.bytes <= $4
        ORDER BY b.ordinal_start,b.chunk_id
        "#,
        ).bind(run_id).bind(shard).bind(page_rows).bind(PAGE_BYTES).fetch_all(&mut *tx).await?;
        let pending_count = receipts.first().map_or(0, |(_, _, count)| *count);
        consumed.reserve(receipts.len());
        for (id, value, _) in receipts {
            let receipt: RunShardSummary = serde_json::from_value(value)?;
            anyhow::ensure!(
                receipt.run_id == run_id && receipt.run_shard == shard,
                "receipt run or shard identity mismatch"
            );
            fold(&mut summary, &receipt, &policy)?;
            consumed.push(id);
        }
        if !consumed.is_empty() {
            progressed = true;
        }
        anyhow::ensure!(
            summary.settled_chunk_count <= plan.chunk_count,
            "more chunk receipts than planned"
        );
        ready_again = summary.settled_chunk_count == plan.chunk_count
            || pending_count > i64::try_from(consumed.len())?;
    } else {
        ready_again = true;
        let final_summary = state.final_summary.get_or_insert_with(|| RunShardSummary {
            run_id,
            run_shard: shard,
            expected_execution_count: expected,
            status: "running".to_owned(),
            ..Default::default()
        });
        let chunks = chunk_page(&mut tx, run_id, shard, &state.folded, page_rows).await?;
        anyhow::ensure!(
            !chunks.is_empty() || state.folded.chunk_count == plan.chunk_count,
            "shard {shard} lost planned chunks before sealing"
        );
        let ids = chunks.iter().map(|c| c.chunk_id).collect::<Vec<_>>();
        let receipts = sqlx::query_as::<_, (Uuid, Value)>(r#"
        WITH selected AS MATERIALIZED (SELECT s.chunk_id,s.ordinal_start,octet_length(s.summary::text) AS receipt_bytes
        FROM run_chunk_settlements s
        WHERE s.run_id = $1
        AND s.run_shard = $2
        AND s.chunk_id = ANY($3::uuid[])),
        bounded AS (SELECT *,SUM(receipt_bytes) OVER (ORDER BY ordinal_start,chunk_id) AS bytes
        FROM selected) SELECT b.chunk_id,s.summary
        FROM bounded b
        JOIN run_chunk_settlements s ON s.run_id=$1 AND s.run_shard=$2 AND s.chunk_id=b.chunk_id
        WHERE b.bytes <= $4
        "#)
            .bind(run_id).bind(shard).bind(&ids).bind(PAGE_BYTES).fetch_all(&mut *tx).await?
            .into_iter().collect::<std::collections::BTreeMap<_, _>>();
        anyhow::ensure!(
            chunks
                .first()
                .is_none_or(|c| receipts.contains_key(&c.chunk_id)),
            "shard {shard} lost an accounted settlement receipt"
        );
        let mut page_bytes = 0usize;
        for chunk in chunks {
            let Some(value) = receipts.get(&chunk.chunk_id) else {
                break;
            };
            let value = value.clone();
            let bytes = serde_json::to_vec(&value)?.len();
            anyhow::ensure!(
                bytes <= 1024 * 1024,
                "chunk receipt exceeds supported byte bound"
            );
            if page_bytes + bytes > PAGE_BYTES as usize {
                break;
            }
            page_bytes += bytes;
            let receipt: RunShardSummary = serde_json::from_value(value)?;
            anyhow::ensure!(
                receipt.run_id == run_id
                    && receipt.run_shard == shard
                    && receipt.expected_execution_count == chunk.ordinal_end - chunk.ordinal_start,
                "chunk {} receipt identity mismatch",
                chunk.chunk_id
            );
            fold(final_summary, &receipt, &policy)?;
            state.folded.push(&chunk)?;
            progressed = true;
        }
        anyhow::ensure!(
            state.folded.chunk_count <= plan.chunk_count && state.folded.case_count <= expected,
            "shard {shard} contains unplanned chunk receipts"
        );
        if state.folded.chunk_count == plan.chunk_count {
            anyhow::ensure!(
                state.folded == *plan
                    && chunk_page(&mut tx, run_id, shard, &state.folded, page_rows)
                        .await?
                        .is_empty(),
                "shard {shard} settled membership does not match its immutable plan"
            );
            summary = final_summary.clone();
            summary.sealed = true;
            summary.status = if summary.failed_chunk_count > 0
                || summary.cancelled_chunk_count > 0
                || summary.failed_execution_count > 0
                || summary.errored_execution_count > 0
                || summary.missing_execution_count > 0
                || summary.unresolved_execution_count > 0
            {
                "failed"
            } else {
                "completed"
            }
            .to_owned();
            progressed = true;
        }
    }
    summary.planned_chunk_count = state.plan.chunk_count;
    summary.planned_chunk_hash = state.plan.hash.clone();
    if progressed {
        queries::store_with(&mut tx, &summary, serde_json::to_value(state)?, &consumed).await?;
    }
    tx.commit().await?;
    Ok(if !progressed {
        PageOutcome::Deferred
    } else if summary.sealed {
        PageOutcome::Sealed
    } else if ready_again {
        PageOutcome::Advanced
    } else {
        PageOutcome::CaughtUp
    })
}

/// Reads one coherent page context after the movement fence. Joining the saved
/// summary and its checkpoint preserves their atomic publication boundary.
async fn load_context(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    shard: i16,
    write_epoch: i64,
) -> anyhow::Result<Context> {
    let context = sqlx::query_as::<_, Context>(
        r#"
        SELECT EXISTS (SELECT 1 FROM local_shard_admissions
                       WHERE run_id=$1 AND run_shard=$2 AND write_epoch=$3
                         AND state IN ('open','draining')) AS admitted,
               r.config_snapshot AS snapshot, r.aggregation_policy_hash AS policy,
               r.expected_execution_count AS expected,
               to_jsonb(s)-'aggregation_state' AS summary,
               s.aggregation_state AS stored
        FROM (SELECT $1::uuid AS run_id,$2::smallint AS run_shard) AS identity
        LEFT JOIN run_snapshots r USING (run_id,run_shard)
        LEFT JOIN run_shard_summaries s USING (run_id,run_shard)
        "#,
    )
    .bind(run_id)
    .bind(shard)
    .bind(write_epoch)
    .fetch_one(&mut **tx)
    .await?;
    anyhow::ensure!(
        context.admitted,
        "shard {shard} aggregation lost local write admission"
    );
    Ok(context)
}

async fn chunk_page(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    shard: i16,
    cursor: &Manifest,
    page_rows: i64,
) -> anyhow::Result<Vec<ChunkIdentity>> {
    Ok(sqlx::query_as(
        r#"
        SELECT id AS chunk_id, run_shard, ordinal_start, ordinal_end
        FROM run_chunks
        WHERE run_id = $1
        AND run_shard = $2
        AND (ordinal_start, id) > ($3::int, $4::uuid)
        ORDER BY ordinal_start, id
        LIMIT $5
        "#,
    )
    .bind(run_id)
    .bind(shard)
    .bind(cursor.last.as_ref().map_or(-1, |c| c.ordinal_start))
    .bind(cursor.last.as_ref().map_or(Uuid::nil(), |c| c.chunk_id))
    .bind(page_rows)
    .fetch_all(&mut **tx)
    .await?)
}

fn fold(
    target: &mut RunShardSummary,
    receipt: &RunShardSummary,
    policy: &str,
) -> anyhow::Result<()> {
    let source: ShardScorecard = serde_json::from_value(receipt.scorecard.clone())?;
    merge_shard_scorecards(policy, std::slice::from_ref(&source)).map_err(anyhow::Error::msg)?;
    anyhow::ensure!(
        source.run_shard == target.run_shard,
        "chunk scorecard shard mismatch"
    );
    if target.settled_chunk_count == 0 {
        target.scorecard = receipt.scorecard.clone();
    } else {
        let mut scorecard: ShardScorecard = serde_json::from_value(target.scorecard.clone())?;
        accumulate_chunk_scorecard(&mut scorecard, source).map_err(anyhow::Error::msg)?;
        target.scorecard = serde_json::to_value(scorecard)?;
    }
    macro_rules! add { ($($field:ident),+ $(,)?) => { $(
        anyhow::ensure!(receipt.$field >= 0, "negative receipt counter {}", stringify!($field));
        target.$field = target.$field.checked_add(receipt.$field)
            .ok_or_else(|| anyhow::anyhow!("receipt counter {} overflow", stringify!($field)))?;
    )+ }; }
    add!(
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
        failed_chunk_count,
        cancelled_chunk_count,
        missing_execution_count,
        unresolved_execution_count
    );
    target.score_sum += receipt.score_sum;
    anyhow::ensure!(target.score_sum.is_finite(), "receipt score sum overflow");
    if let Some(value) = receipt.min_score {
        target.min_score = Some(target.min_score.map_or(value, |v| v.min(value)));
    }
    if let Some(value) = receipt.max_score {
        target.max_score = Some(target.max_score.map_or(value, |v| v.max(value)));
    }
    target.settled_chunk_count = target
        .settled_chunk_count
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("settled chunk count overflow"))?;
    Ok(())
}

/// Drains a bounded, fair set of pending receipt or final verification pages.
/// Independent shard pages run concurrently. Shards with more work rejoin the queue
/// after first visits, without waiting for another coordinator tick. Idle shards
/// do no work; busy and invalid candidates rotate once and leave this pass.
/// Returns committed productive pages, including sealing pages. Discovery and
/// rotation errors stop new scheduling and propagate after active pages finish.
pub(crate) async fn advance_pending_summaries(
    db: &PgPool,
    config: &AggregationConfig,
) -> anyhow::Result<usize> {
    let limit = config.candidate_limit()?;
    let candidates = sqlx::query_as::<_, (Uuid, i16, i64)>(
        r#"
        SELECT s.run_id,s.run_shard,a.write_epoch
        FROM run_shard_summaries s
        JOIN local_shard_admissions a USING (run_id,run_shard)
        WHERE NOT s.sealed
        AND a.state IN ('open','draining')
        AND ((s.planned_chunk_count > 0 AND s.settled_chunk_count >= s.planned_chunk_count)
             OR EXISTS (SELECT 1 FROM run_chunk_settlement_consumptions p
                        WHERE p.run_id=s.run_id AND p.run_shard=s.run_shard AND NOT p.consumed))
        ORDER BY s.updated_at,s.run_id,s.run_shard
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(db)
    .await?;
    // Slow discovery must not exhaust every pass before it can start a page.
    // The outer database operation deadline still bounds discovery and pages.
    let started = Instant::now();
    drain(
        candidates,
        config,
        || started.elapsed() < config.time_budget,
        |(run_id, shard, epoch)| async move {
            match advance_shard_summary_page(db, run_id, shard, epoch).await {
                Ok(outcome @ (PageOutcome::Advanced | PageOutcome::Sealed | PageOutcome::CaughtUp)) => {
                    // Publishing the checkpoint already rotates this candidate.
                    return Ok(outcome);
                }
                Ok(PageOutcome::Deferred) => {}
                Err(error) => tracing::warn!(%run_id, run_shard = shard, %error,
                    "deferred invalid or unavailable shard aggregation; continuing other shards"),
            }
            // Rotate even unchanged/invalid candidates; no receipt or published
            // counter changes are made by this scheduling-only update.
            sqlx::query("UPDATE run_shard_summaries SET updated_at = now() WHERE run_id = $1 AND run_shard = $2 AND NOT sealed")
                .bind(run_id).bind(shard).execute(db).await
                .with_context(|| format!("rotating aggregation candidate for run {run_id}, shard {shard}"))?;
            Ok(PageOutcome::Deferred)
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregation_budgets_are_positive_and_fit_database_discovery() {
        let config = AggregationConfig {
            max_pages: 256,
            time_budget: Duration::from_secs(1),
            parallelism: 4,
        };
        assert_eq!(config.candidate_limit().unwrap(), 256);
        for invalid in [
            AggregationConfig {
                max_pages: 0,
                ..config.clone()
            },
            AggregationConfig {
                parallelism: 0,
                ..config.clone()
            },
            AggregationConfig {
                time_budget: Duration::ZERO,
                ..config.clone()
            },
        ] {
            assert!(invalid.candidate_limit().is_err());
        }
        if let Ok(too_many) = usize::try_from(u64::MAX) {
            assert!(
                AggregationConfig {
                    max_pages: too_many,
                    ..config
                }
                .candidate_limit()
                .is_err()
            );
        }
    }
}
