//! Routed run result summary helpers.
//!
//! Result summaries are assembled from shard-local `run_shard_summaries` so
//! the control database does not scan execution-owned tables.

use std::collections::BTreeMap;

use uuid::Uuid;

use super::run_shard_summary::{
    RunShardSummary,
    select_run_shard_summaries,
};
use crate::context::database;

pub(crate) async fn select_run_scorecard(
    db: &sqlx::PgPool,
    run_id: Uuid,
) -> anyhow::Result<Option<serde_json::Value>> {
    Ok(
        sqlx::query_scalar("SELECT scorecard FROM run_scorecards WHERE run_id = $1::uuid")
            .bind(run_id)
            .fetch_optional(db)
            .await?,
    )
}

#[derive(Debug, Clone, Default, sqlx::FromRow)]
pub(crate) struct RunResultsSummary {
    pub(crate) execution_count: i64,
    pub(crate) aggregate_count: i64,
    pub(crate) passed_execution_count: i64,
    pub(crate) failed_execution_count: i64,
    pub(crate) error_execution_count: i64,
    pub(crate) skipped_execution_count: i64,
    pub(crate) missing_aggregate_count: i64,
    pub(crate) evaluator_result_count: i64,
    pub(crate) blocking_failure_count: i64,
    pub(crate) average_score: Option<f64>,
    pub(crate) min_score: Option<f64>,
    pub(crate) max_score: Option<f64>,
    pub(crate) planned_chunk_count: i64,
    pub(crate) settled_chunk_count: i64,
    pub(crate) sealed_shard_count: usize,
    pub(crate) missing_execution_count: i64,
    pub(crate) unresolved_execution_count: i64,
}

/// Reads and combines shard-local result summaries for a run.
pub(crate) async fn select_run_results_summary(
    database_router: &database::DatabaseRouter,
    run_id: Uuid,
) -> anyhow::Result<RunResultsSummary> {
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
    // Floating point addition follows ascending shard order independently of
    // alias grouping so routing does not change the reported average.
    summaries.sort_unstable_by_key(|summary| summary.run_shard);

    Ok(combine_run_shard_summaries(&summaries))
}

pub(crate) fn combine_run_shard_summaries(summaries: &[RunShardSummary]) -> RunResultsSummary {
    let mut combined = RunResultsSummary::default();
    let mut score_count = 0i64;
    let mut score_sum = 0.0f64;

    for summary in summaries {
        combined.planned_chunk_count += i64::from(summary.planned_chunk_count);
        combined.settled_chunk_count += i64::from(summary.settled_chunk_count);
        combined.missing_execution_count += i64::from(summary.missing_execution_count);
        combined.unresolved_execution_count += i64::from(summary.unresolved_execution_count);
        combined.sealed_shard_count += usize::from(summary.is_terminal());
        combined.execution_count += i64::from(summary.execution_count);
        combined.aggregate_count += i64::from(summary.aggregate_count);
        combined.passed_execution_count += i64::from(summary.passed_execution_count);
        combined.failed_execution_count += i64::from(summary.failed_execution_count);
        combined.error_execution_count += i64::from(summary.errored_execution_count);
        combined.skipped_execution_count += i64::from(summary.skipped_execution_count);
        combined.missing_aggregate_count += i64::from(summary.missing_aggregate_count);
        combined.evaluator_result_count += summary.evaluator_result_count;
        combined.blocking_failure_count += summary.blocking_failure_count;
        score_count += summary.score_count;
        score_sum += summary.score_sum;

        if let Some(min_score) = summary.min_score {
            combined.min_score = Some(
                combined
                    .min_score
                    .map(|current| current.min(min_score))
                    .unwrap_or(min_score),
            );
        }

        if let Some(max_score) = summary.max_score {
            combined.max_score = Some(
                combined
                    .max_score
                    .map(|current| current.max(max_score))
                    .unwrap_or(max_score),
            );
        }
    }

    combined.average_score = (score_count > 0).then_some(score_sum / score_count as f64);
    combined
}

#[cfg(test)]
#[path = "run_results/postgres_tests.rs"]
mod postgres_tests;
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn combine_run_shard_summaries_rolls_up_counts_and_scores() {
        let run_id = Uuid::now_v7();
        let mut first = summary(run_id, 1, 3);
        first.passed_execution_count = 2;
        first.failed_execution_count = 1;
        first.evaluator_result_count = 9;
        first.blocking_failure_count = 1;
        first.score_count = 2;
        first.score_sum = 1.4;
        first.min_score = Some(0.6);
        first.max_score = Some(0.8);

        let mut second = summary(run_id, 2, 2);
        second.failed_execution_count = 1;
        second.errored_execution_count = 1;
        second.skipped_execution_count = 1;
        second.missing_aggregate_count = 1;
        second.evaluator_result_count = 6;
        second.blocking_failure_count = 2;
        second.score_count = 1;
        second.score_sum = 0.2;
        second.min_score = Some(0.2);
        second.max_score = Some(0.2);

        let summaries = vec![first, second];

        let combined = combine_run_shard_summaries(&summaries);

        assert_eq!(combined.execution_count, 5);
        assert_eq!(combined.aggregate_count, 5);
        assert_eq!(combined.passed_execution_count, 2);
        assert_eq!(combined.failed_execution_count, 2);
        assert_eq!(combined.error_execution_count, 1);
        assert_eq!(combined.skipped_execution_count, 1);
        assert_eq!(combined.missing_aggregate_count, 1);
        assert_eq!(combined.evaluator_result_count, 15);
        assert_eq!(combined.blocking_failure_count, 3);
        assert_eq!(combined.average_score, Some(1.6 / 3.0));
        assert_eq!(combined.min_score, Some(0.2));
        assert_eq!(combined.max_score, Some(0.8));
    }

    #[test]
    fn combine_run_shard_summaries_handles_empty_and_unscored_results() {
        let empty = combine_run_shard_summaries(&[]);
        assert_eq!(empty.execution_count, 0);
        assert_eq!(empty.average_score, None);
        assert_eq!(empty.min_score, None);
        assert_eq!(empty.max_score, None);

        let unscored = combine_run_shard_summaries(&[summary(Uuid::nil(), 0, 1)]);
        assert_eq!(unscored.execution_count, 1);
        assert_eq!(unscored.average_score, None);
        assert_eq!(unscored.min_score, None);
        assert_eq!(unscored.max_score, None);
    }

    fn summary(run_id: Uuid, run_shard: i16, execution_count: i32) -> RunShardSummary {
        RunShardSummary {
            run_id,
            run_shard,
            expected_execution_count: execution_count,
            execution_count,
            terminal_execution_count: execution_count,
            aggregate_count: execution_count,
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
                "policy_hash": "hash",
                "entries": [],
            }),
            failed_chunk_count: 0,
            cancelled_chunk_count: 0,
            status: "completed".to_owned(),
            sealed: true,
            planned_chunk_count: 1,
            settled_chunk_count: 1,
            ..Default::default()
        }
    }
}
