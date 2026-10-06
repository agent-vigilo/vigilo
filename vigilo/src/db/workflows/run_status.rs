//! Routed run status projection helpers.
//!
//! Status reads keep the control run row authoritative. Creating runs report
//! control-owned placement progress without contacting unseeded databases;
//! active runs add live progress from shard-local summaries. The workflow
//! avoids scanning execution hot tables on every status or watch poll.

use std::collections::BTreeMap;

use uuid::Uuid;

use super::{
    run_creation::{
        RUN_STATUS_CREATING,
        RunCreationProgress,
        select_creation_progress,
    },
    run_shard_summary::{
        RunShardSummary,
        select_run_shard_summaries,
    },
};
use crate::{
    context::database,
    db::tables::runs,
    models::run::Run,
};

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct RunProgressSummary {
    pub(crate) expected_execution_count: i64,
    pub(crate) execution_count: i64,
    pub(crate) terminal_execution_count: i64,
    pub(crate) passed_execution_count: i64,
    pub(crate) failed_execution_count: i64,
    pub(crate) errored_execution_count: i64,
    pub(crate) skipped_execution_count: i64,
    pub(crate) missing_aggregate_count: i64,
    pub(crate) failed_chunk_count: i64,
    pub(crate) cancelled_chunk_count: i64,
    pub(crate) planned_chunk_count: i64,
    pub(crate) settled_chunk_count: i64,
    pub(crate) sealed_shard_count: usize,
    pub(crate) missing_execution_count: i64,
    pub(crate) unresolved_execution_count: i64,
}

impl RunProgressSummary {
    pub(crate) fn from_control_run(run: &Run) -> Self {
        Self {
            expected_execution_count: i64::from(run.expected_execution_count),
            terminal_execution_count: i64::from(run.terminal_execution_count),
            passed_execution_count: i64::from(run.passed_execution_count),
            failed_execution_count: i64::from(run.failed_execution_count),
            errored_execution_count: i64::from(run.errored_execution_count),
            planned_chunk_count: run.summary["planned_chunk_count"].as_i64().unwrap_or(0),
            settled_chunk_count: run.summary["settled_chunk_count"].as_i64().unwrap_or(0),
            sealed_shard_count: run.summary["sealed_shard_count"]
                .as_u64()
                .and_then(|n| usize::try_from(n).ok())
                .unwrap_or(0),
            missing_execution_count: run.summary["missing_execution_count"].as_i64().unwrap_or(0),
            unresolved_execution_count: run.summary["unresolved_execution_count"]
                .as_i64()
                .unwrap_or(0),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RunStatusProjection {
    pub(crate) run: Run,
    pub(crate) live_progress: RunProgressSummary,
    pub(crate) execution_route_count: usize,
    pub(crate) shard_summary_count: usize,
    pub(crate) creation_progress: Option<RunCreationProgress>,
}

impl RunStatusProjection {
    pub(crate) fn from_control_run(run: Run) -> Self {
        let live_progress = RunProgressSummary::from_control_run(&run);
        Self {
            run,
            live_progress,
            execution_route_count: 0,
            shard_summary_count: 0,
            creation_progress: None,
        }
    }

    pub(crate) fn progress_source(&self) -> &'static str {
        if self.creation_progress.is_some() {
            "run_creation"
        } else if self.shard_summary_count > 0 {
            "execution_shards"
        } else {
            "control_run"
        }
    }

    pub(crate) fn live_progress_complete(&self) -> bool {
        self.execution_route_count > 0 && self.execution_route_count == self.shard_summary_count
    }
}

pub(crate) fn combine_run_shard_progress(summaries: &[RunShardSummary]) -> RunProgressSummary {
    let mut progress = RunProgressSummary::default();

    for summary in summaries {
        progress.planned_chunk_count += i64::from(summary.planned_chunk_count);
        progress.settled_chunk_count += i64::from(summary.settled_chunk_count);
        progress.missing_execution_count += i64::from(summary.missing_execution_count);
        progress.unresolved_execution_count += i64::from(summary.unresolved_execution_count);
        progress.sealed_shard_count += usize::from(summary.is_terminal());
        progress.expected_execution_count += i64::from(summary.expected_execution_count);
        progress.execution_count += i64::from(summary.execution_count);
        progress.terminal_execution_count += i64::from(summary.terminal_execution_count);
        progress.passed_execution_count += i64::from(summary.passed_execution_count);
        progress.failed_execution_count += i64::from(summary.failed_execution_count);
        progress.errored_execution_count += i64::from(summary.errored_execution_count);
        progress.skipped_execution_count += i64::from(summary.skipped_execution_count);
        progress.missing_aggregate_count += i64::from(summary.missing_aggregate_count);
        progress.failed_chunk_count += i64::from(summary.failed_chunk_count);
        progress.cancelled_chunk_count += i64::from(summary.cancelled_chunk_count);
    }

    progress
}

/// Reads a control-only creation projection or routed shard-summary progress.
pub(crate) async fn select_run_status(
    database_router: &database::DatabaseRouter,
    run_id: Uuid,
) -> anyhow::Result<Option<RunStatusProjection>> {
    let control_db = database_router.control().await?;
    let Some(run) = runs::select_run_by_id(control_db, run_id).await? else {
        return Ok(None);
    };

    let creation_owned_status = run.status == RUN_STATUS_CREATING
        || (run.status == "failed" && run.started_at.is_none() && run.dispatched_at.is_none());
    if creation_owned_status {
        let live_progress = RunProgressSummary::from_control_run(&run);
        let creation_progress = select_creation_progress(control_db, run_id).await?;
        if run.status == RUN_STATUS_CREATING || creation_progress.placement_count > 0 {
            return Ok(Some(RunStatusProjection {
                run,
                live_progress,
                execution_route_count: 0,
                shard_summary_count: 0,
                creation_progress: Some(creation_progress),
            }));
        }
    }

    let routes = database_router
        .execution_read_routes_for_run(run_id)
        .await?;
    let mut summaries = Vec::with_capacity(routes.len());

    let mut by_database = BTreeMap::new();
    for (run_shard, alias, db) in &routes {
        by_database
            .entry(alias)
            .or_insert_with(|| (db, Vec::new()))
            .1
            .push(*run_shard);
    }
    for (db, shards) in by_database.into_values() {
        summaries.extend(select_run_shard_summaries(db, run_id, &shards).await?);
    }

    let shard_summary_count = summaries.len();
    let live_progress = if summaries.is_empty() {
        RunProgressSummary::from_control_run(&run)
    } else {
        combine_run_shard_progress(&summaries)
    };

    Ok(Some(RunStatusProjection {
        run,
        live_progress,
        execution_route_count: routes.len(),
        shard_summary_count,
        creation_progress: None,
    }))
}

#[cfg(test)]
#[path = "run_status/postgres_tests.rs"]
mod postgres_tests;
#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    #[test]
    fn combine_run_shard_progress_rolls_up_live_counts() {
        let run_id = Uuid::now_v7();
        let summaries = vec![summary(run_id, 0, 10, 7), summary(run_id, 1, 5, 5)];

        let progress = combine_run_shard_progress(&summaries);

        assert_eq!(progress.expected_execution_count, 15);
        assert_eq!(progress.execution_count, 12);
        assert_eq!(progress.terminal_execution_count, 12);
        assert_eq!(progress.passed_execution_count, 8);
        assert_eq!(progress.failed_execution_count, 2);
        assert_eq!(progress.errored_execution_count, 2);
        assert_eq!(progress.cancelled_chunk_count, 2);
    }

    #[test]
    fn combine_run_shard_progress_handles_empty_summaries() {
        assert_eq!(
            combine_run_shard_progress(&[]),
            RunProgressSummary::default()
        );
    }

    fn summary(
        run_id: Uuid,
        run_shard: i16,
        expected_execution_count: i32,
        terminal_execution_count: i32,
    ) -> RunShardSummary {
        RunShardSummary {
            run_id,
            run_shard,
            expected_execution_count,
            execution_count: terminal_execution_count,
            terminal_execution_count,
            aggregate_count: terminal_execution_count,
            passed_execution_count: terminal_execution_count - 2,
            failed_execution_count: 1,
            errored_execution_count: 1,
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
                "policy_hash": "hash",
                "entries": [],
            }),
            failed_chunk_count: 0,
            cancelled_chunk_count: 1,
            status: "running".to_string(),
            ..Default::default()
        }
    }
}
