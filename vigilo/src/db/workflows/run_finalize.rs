//! Run finalization workflow helpers.
//!
//! Finalization is coordinator-owned and guarded by leases. Workers write
//! execution and aggregate state into shard-local storage. This workflow uses
//! `run_shard_summaries` read by the coordinator from execution placements to
//! complete the authoritative control `runs` row.

use sqlx::PgPool;
use uuid::Uuid;

use super::run_shard_summary::RunShardSummary;
use crate::contracts::scorecard::{
    RunScorecard,
    ShardScorecard,
    merge_shard_scorecards,
};

mod queries;

pub(crate) use queries::{
    claim_finalization_candidate,
    mark_finalization_candidate_checked,
    select_finalization_candidate_backlog,
    select_next_finalization_candidate,
};

/// Minimal run projection returned when a coordinator claims finalization.
#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct ClaimedRunForFinalization {
    pub(crate) id: Uuid,
    pub(crate) run_key: String,
    pub(crate) aggregation_policy_hash: String,
}

/// Run projection returned after final gate status is persisted.
#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct FinalizedRun {
    pub(crate) id: Uuid,
    pub(crate) run_key: String,
    pub(crate) gate_status: String,
    pub(crate) terminal_execution_count: i32,
    pub(crate) passed_execution_count: i32,
    pub(crate) failed_execution_count: i32,
    pub(crate) errored_execution_count: i32,
}

/// Control-database finalization backlog gauge.
#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct FinalizationCandidateBacklog {
    pub(crate) candidate_count: i64,
    pub(crate) oldest_candidate_lag_seconds: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
struct FinalizationSummary {
    expected_execution_count: i32,
    terminal_execution_count: i32,
    passed_execution_count: i32,
    failed_execution_count: i32,
    errored_execution_count: i32,
    missing_aggregate_count: i32,
    missing_execution_count: i32,
    unresolved_execution_count: i32,
    failed_chunk_count: i32,
    cancelled_chunk_count: i32,
    shard_summary_count: i32,
    coverage_complete: bool,
    has_terminal_chunk_failure: bool,
    scorecard: RunScorecard,
    gate_status: &'static str,
}

fn summarize_for_finalization(
    summaries: &[RunShardSummary],
    aggregation_policy_hash: &str,
) -> anyhow::Result<Option<FinalizationSummary>> {
    if summaries.is_empty() || summaries.iter().any(|summary| !summary.is_terminal()) {
        return Ok(None);
    }

    let expected_execution_count =
        checked_summary_total(summaries, "expected_execution_count", |summary| {
            summary.expected_execution_count
        })?;
    let terminal_execution_count =
        checked_summary_total(summaries, "terminal_execution_count", |summary| {
            summary.terminal_execution_count
        })?;
    let passed_execution_count =
        checked_summary_total(summaries, "passed_execution_count", |summary| {
            summary.passed_execution_count
        })?;
    let failed_execution_count =
        checked_summary_total(summaries, "failed_execution_count", |summary| {
            summary.failed_execution_count
        })?;
    let errored_execution_count =
        checked_summary_total(summaries, "errored_execution_count", |summary| {
            summary.errored_execution_count
        })?;
    let missing_aggregate_count =
        checked_summary_total(summaries, "missing_aggregate_count", |summary| {
            summary.missing_aggregate_count
        })?;
    let failed_chunk_count = checked_summary_total(summaries, "failed_chunk_count", |summary| {
        summary.failed_chunk_count
    })?;
    let cancelled_chunk_count =
        checked_summary_total(summaries, "cancelled_chunk_count", |summary| {
            summary.cancelled_chunk_count
        })?;
    let missing_execution_count =
        checked_summary_total(summaries, "missing_execution_count", |s| {
            s.missing_execution_count
        })?;
    let unresolved_execution_count =
        checked_summary_total(summaries, "unresolved_execution_count", |s| {
            s.unresolved_execution_count
        })?;
    let coverage_complete = terminal_execution_count == expected_execution_count
        && missing_execution_count == 0
        && unresolved_execution_count == 0;
    let has_terminal_chunk_failure = failed_chunk_count > 0 || cancelled_chunk_count > 0;
    let shard_scorecards = summaries
        .iter()
        .map(|summary| serde_json::from_value::<ShardScorecard>(summary.scorecard.clone()))
        .collect::<Result<Vec<_>, _>>()?;
    let scorecard = merge_shard_scorecards(aggregation_policy_hash, &shard_scorecards)
        .map_err(anyhow::Error::msg)?;
    let gate_status = if has_terminal_chunk_failure
        || failed_execution_count > 0
        || errored_execution_count > 0
        || missing_aggregate_count > 0
        || !coverage_complete
        || !scorecard.passed
    {
        "fail"
    } else {
        "pass"
    };

    Ok(Some(FinalizationSummary {
        expected_execution_count,
        terminal_execution_count,
        passed_execution_count,
        failed_execution_count,
        errored_execution_count,
        missing_aggregate_count,
        missing_execution_count,
        unresolved_execution_count,
        failed_chunk_count,
        cancelled_chunk_count,
        shard_summary_count: i32::try_from(summaries.len())?,
        coverage_complete,
        has_terminal_chunk_failure,
        scorecard,
        gate_status,
    }))
}

fn checked_summary_total(
    summaries: &[RunShardSummary],
    field: &str,
    value: impl Fn(&RunShardSummary) -> i32,
) -> anyhow::Result<i32> {
    summaries.iter().try_fold(0i32, |total, summary| {
        total.checked_add(value(summary)).ok_or_else(|| {
            anyhow::anyhow!("run shard summary {field} total exceeds the supported i32 range")
        })
    })
}

/// Claims the next control candidate without reading execution databases.
///
/// Retained for existing workflow tests; production finalization uses
/// [`select_next_finalization_candidate`] plus routed shard summary reads.
#[cfg(test)]
pub(crate) async fn claim_next_finalizable_run(
    db: &PgPool,
    coordinator_id: Uuid,
    lease_seconds: i32,
) -> anyhow::Result<Option<ClaimedRunForFinalization>> {
    let Some(candidate) = select_next_finalization_candidate(db, &[]).await? else {
        return Ok(None);
    };

    claim_finalization_candidate(db, candidate.id, coordinator_id, lease_seconds).await
}

/// Finalizes a claimed run from routed shard summaries.
///
/// Query behavior:
/// - Locks the claimed run row and verifies the coordinator still owns an
///   unexpired finalization lease.
/// - Uses precomputed shard summary counters supplied by the coordinator.
/// - Marks the run `completed`, sets the final gate status, persists the
///   global summary, drains leftover cursors, and emits `run.completed` in the
///   control outbox.
pub(crate) async fn finalize_claimed_run_from_summaries(
    db: &PgPool,
    run_id: Uuid,
    coordinator_id: Uuid,
    aggregation_policy_hash: &str,
    summaries: &[RunShardSummary],
) -> anyhow::Result<Option<FinalizedRun>> {
    validate_summary_set(run_id, summaries, aggregation_policy_hash)?;
    let Some(summary) = summarize_for_finalization(summaries, aggregation_policy_hash)? else {
        return Ok(None);
    };

    let mut shards = summaries.iter().map(|s| s.run_shard).collect::<Vec<_>>();
    shards.sort_unstable();
    let plans = summaries
        .iter()
        .map(|s| {
            serde_json::json!({
                "shard": s.run_shard, "hash": s.planned_chunk_hash,
                "cases": s.expected_execution_count, "chunks": s.planned_chunk_count,
            })
        })
        .collect::<Vec<_>>();
    queries::finalize_claimed_run(
        db,
        run_id,
        coordinator_id,
        &summary,
        &shards,
        serde_json::json!(plans),
    )
    .await
}

/// Validates candidate-local contracts before acquiring a control lease so a
/// corrupt shard report cannot terminate the coordinator's entire pass.
pub(crate) fn validate_summary_set(
    run_id: Uuid,
    summaries: &[RunShardSummary],
    policy: &str,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        summaries
            .iter()
            .all(|s| s.run_id == run_id && (0..128).contains(&s.run_shard)),
        "finalization summary run or shard identity mismatch"
    );
    let unique = summaries
        .iter()
        .map(|s| s.run_shard)
        .collect::<std::collections::BTreeSet<_>>();
    anyhow::ensure!(
        unique.len() == summaries.len(),
        "duplicate finalization shard summary"
    );
    for s in summaries {
        anyhow::ensure!(
            s.expected_execution_count >= 0
                && s.terminal_execution_count >= 0
                && s.terminal_execution_count <= s.expected_execution_count
                && s.missing_execution_count >= 0
                && s.unresolved_execution_count >= 0,
            "invalid finalization summary counters"
        );
        if s.is_terminal() {
            let outcomes = i64::from(s.passed_execution_count)
                + i64::from(s.failed_execution_count)
                + i64::from(s.errored_execution_count)
                + i64::from(s.skipped_execution_count);
            anyhow::ensure!(
                s.execution_count >= 0
                    && s.aggregate_count >= 0
                    && s.terminal_execution_count == s.execution_count
                    && i64::from(s.execution_count) + i64::from(s.missing_execution_count)
                        == i64::from(s.expected_execution_count)
                    && i64::from(s.aggregate_count) + i64::from(s.unresolved_execution_count)
                        == i64::from(s.execution_count)
                    && s.missing_aggregate_count == s.unresolved_execution_count
                    && [
                        s.passed_execution_count,
                        s.failed_execution_count,
                        s.errored_execution_count,
                        s.skipped_execution_count
                    ]
                    .iter()
                    .all(|n| *n >= 0)
                    && outcomes <= i64::from(s.aggregate_count)
                    && s.score_count >= 0
                    && s.score_count <= i64::from(s.aggregate_count)
                    && s.score_sum.is_finite(),
                "sealed summary has inconsistent result coverage"
            );
        }
        let scorecard: ShardScorecard = serde_json::from_value(s.scorecard.clone())?;
        anyhow::ensure!(
            scorecard.run_shard == s.run_shard,
            "scorecard and summary shard disagree"
        );
    }
    summarize_for_finalization(summaries, policy)?;
    Ok(())
}

#[cfg(test)]
#[path = "run_finalize/postgres_tests.rs"]
mod postgres_tests;
#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    #[test]
    fn finalization_waits_for_a_complete_terminal_summary_set() {
        let run_id = Uuid::nil();

        assert!(
            summarize_for_finalization(&[], "aggregation-hash")
                .unwrap()
                .is_none()
        );
        assert!(
            summarize_for_finalization(
                &[terminal_summary(run_id, 0, "running")],
                "aggregation-hash",
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn finalization_combines_terminal_shard_counters() {
        let run_id = Uuid::nil();
        let mut first = terminal_summary(run_id, 1, "completed");
        first.expected_execution_count = 2;
        first.terminal_execution_count = 2;
        first.passed_execution_count = 2;
        let mut second = terminal_summary(run_id, 2, "completed");
        second.failed_execution_count = 1;

        let summary = summarize_for_finalization(&[first, second], "aggregation-hash")
            .unwrap()
            .unwrap();

        assert_eq!(summary.expected_execution_count, 3);
        assert_eq!(summary.terminal_execution_count, 3);
        assert_eq!(summary.passed_execution_count, 2);
        assert_eq!(summary.failed_execution_count, 1);
        assert_eq!(summary.shard_summary_count, 2);
        assert_eq!(summary.gate_status, "fail");
    }

    #[test]
    fn finalization_fails_for_each_incomplete_or_failed_signal() {
        let run_id = Uuid::nil();
        let cases = [
            summary_with(run_id, |summary| summary.failed_execution_count = 1),
            summary_with(run_id, |summary| summary.errored_execution_count = 1),
            summary_with(run_id, |summary| summary.missing_aggregate_count = 1),
            summary_with(run_id, |summary| summary.failed_chunk_count = 1),
            summary_with(run_id, |summary| summary.cancelled_chunk_count = 1),
            summary_with(run_id, |summary| summary.terminal_execution_count = 0),
        ];

        for summary in cases {
            let decision = summarize_for_finalization(&[summary], "aggregation-hash")
                .unwrap()
                .unwrap();
            assert_eq!(decision.gate_status, "fail");
        }
    }

    #[test]
    fn finalization_passes_with_complete_successful_coverage() {
        let summary = terminal_summary(Uuid::nil(), 0, "completed");

        let decision = summarize_for_finalization(&[summary], "aggregation-hash")
            .unwrap()
            .unwrap();

        assert!(decision.coverage_complete);
        assert!(!decision.has_terminal_chunk_failure);
        assert_eq!(decision.gate_status, "pass");
    }

    #[test]
    fn finalization_fails_when_run_scorecard_gate_fails() {
        let mut summary = terminal_summary(Uuid::nil(), 0, "completed");
        summary.passed_execution_count = 1;
        summary.scorecard["entries"] = serde_json::json!([{
            "id": "safety",
            "dimension": "safety",
            "binding_id": null,
            "case_group": null,
            "tags_all": ["jailbreak"],
            "min_mean_score": 1.0,
            "score_threshold": null,
            "min_pass_rate": null,
            "min_coverage": 1.0,
            "max_error_rate": 0.0,
            "max_abstention_rate": 0.0,
            "expected_count": 1,
            "scored_count": 1,
            "passed_count": 0,
            "error_count": 0,
            "abstained_count": 0,
            "score_sum": 0.9,
            "min_score": 0.9,
            "max_score": 0.9
        }]);

        let decision = summarize_for_finalization(&[summary], "aggregation-hash")
            .unwrap()
            .unwrap();

        assert_eq!(decision.passed_execution_count, 1);
        assert!(!decision.scorecard.passed);
        assert_eq!(decision.gate_status, "fail");
    }

    #[test]
    fn finalization_rejects_counter_overflow() {
        let run_id = Uuid::nil();
        let mut first = terminal_summary(run_id, 0, "completed");
        first.expected_execution_count = i32::MAX;
        first.terminal_execution_count = i32::MAX;
        let second = terminal_summary(run_id, 1, "completed");

        let error = summarize_for_finalization(&[first, second], "aggregation-hash").unwrap_err();

        assert!(error.to_string().contains("expected_execution_count total"));
    }

    #[test]
    fn finalization_rejects_invalid_identities_and_unsealed_failure() {
        let run = Uuid::nil();
        let summary = terminal_summary(run, 0, "failed");
        let mut invalid = summary.clone();
        invalid.run_id = Uuid::from_u128(1);
        assert!(validate_summary_set(run, &[invalid], "aggregation-hash").is_err());
        assert!(
            validate_summary_set(run, &[summary.clone(), summary.clone()], "aggregation-hash")
                .is_err()
        );
        for shard in [-1, 128] {
            let mut invalid = summary.clone();
            invalid.run_shard = shard;
            assert!(validate_summary_set(run, &[invalid], "aggregation-hash").is_err());
        }
        let mut invalid = summary.clone();
        invalid.scorecard["run_shard"] = serde_json::json!(1);
        assert!(validate_summary_set(run, &[invalid], "aggregation-hash").is_err());
        assert!(validate_summary_set(run, std::slice::from_ref(&summary), "other-policy").is_err());
        let mut invalid = summary.clone();
        invalid.terminal_execution_count = 2;
        assert!(validate_summary_set(run, &[invalid], "aggregation-hash").is_err());
        for (sealed, settled) in [(false, 1), (true, 0)] {
            let mut open = summary.clone();
            open.sealed = sealed;
            open.settled_chunk_count = settled;
            assert!(
                summarize_for_finalization(&[open], "aggregation-hash")
                    .unwrap()
                    .is_none()
            );
        }
    }

    pub(super) fn terminal_summary(run_id: Uuid, run_shard: i16, status: &str) -> RunShardSummary {
        RunShardSummary {
            run_id,
            run_shard,
            expected_execution_count: 1,
            execution_count: 1,
            terminal_execution_count: 1,
            aggregate_count: 1,
            passed_execution_count: 0,
            failed_execution_count: 0,
            errored_execution_count: 0,
            skipped_execution_count: 0,
            missing_aggregate_count: 0,
            evaluator_result_count: 0,
            blocking_failure_count: 0,
            score_count: 0,
            score_sum: 0.0,
            min_score: None,
            max_score: None,
            scorecard: serde_json::json!({
                "version": 1,
                "run_shard": run_shard,
                "policy_hash": "aggregation-hash",
                "entries": [],
            }),
            failed_chunk_count: 0,
            cancelled_chunk_count: 0,
            status: status.to_owned(),
            sealed: true,
            planned_chunk_hash: format!("planned-{run_shard}"),
            planned_chunk_count: 1,
            settled_chunk_count: 1,
            ..Default::default()
        }
    }

    fn summary_with(run_id: Uuid, update: impl FnOnce(&mut RunShardSummary)) -> RunShardSummary {
        let mut summary = terminal_summary(run_id, 0, "completed");
        update(&mut summary);
        summary
    }
}
