//! Shard-local run summary workflow helpers.
//!
//! Workers commit immutable, chunk-local settlement receipts after fencing all
//! result writers. Coordinators consume bounded pages in planned ordinal order,
//! checkpoint counters and membership atomically, and seal a summary only after
//! the exact planned set settles. Execution coverage and lifecycle settlement
//! are separate: exhausted chunks may retain useful partial results.

use uuid::Uuid;

/// Current shard-local summary for one run shard.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, sqlx::FromRow)]
#[serde(default)]
pub(crate) struct RunShardSummary {
    pub(crate) run_id: Uuid,
    pub(crate) run_shard: i16,
    pub(crate) expected_execution_count: i32,
    pub(crate) execution_count: i32,
    pub(crate) terminal_execution_count: i32,
    pub(crate) aggregate_count: i32,
    pub(crate) passed_execution_count: i32,
    pub(crate) failed_execution_count: i32,
    pub(crate) errored_execution_count: i32,
    pub(crate) skipped_execution_count: i32,
    pub(crate) missing_aggregate_count: i32,
    pub(crate) evaluator_result_count: i64,
    pub(crate) blocking_failure_count: i64,
    pub(crate) score_count: i64,
    pub(crate) score_sum: f64,
    pub(crate) min_score: Option<f64>,
    pub(crate) max_score: Option<f64>,
    pub(crate) scorecard: serde_json::Value,
    pub(crate) failed_chunk_count: i32,
    pub(crate) cancelled_chunk_count: i32,
    pub(crate) status: String,
    pub(crate) sealed: bool,
    pub(crate) planned_chunk_hash: String,
    pub(crate) planned_chunk_count: i32,
    pub(crate) settled_chunk_count: i32,
    pub(crate) missing_execution_count: i32,
    pub(crate) unresolved_execution_count: i32,
}

impl RunShardSummary {
    pub(crate) fn is_terminal(&self) -> bool {
        self.sealed
            && self.planned_chunk_count > 0
            && self.settled_chunk_count == self.planned_chunk_count
            && is_terminal_summary_status(&self.status)
    }
}

fn is_terminal_summary_status(status: &str) -> bool {
    matches!(status, "completed" | "failed")
}

mod aggregation;
mod aggregation_scheduling;
mod plan;
mod queries;
mod settlement;

#[cfg(test)]
pub(crate) use aggregation::advance_shard_summary;
pub(crate) use aggregation::{
    AggregationConfig,
    advance_pending_summaries,
};
pub(crate) use plan::manifests as chunk_plan_manifests;
pub(crate) use queries::{
    initialize_run_shard_summary_with,
    insert_run_shard_summary_with,
    select_run_shard_summaries,
};
#[cfg(test)]
pub(crate) use queries::{
    refresh_run_shard_summary,
    select_run_shard_summary,
};
pub(crate) use settlement::{
    settle_chunk_with,
    settle_locked_chunk_with,
    settle_terminal_chunks_with,
};

#[cfg(test)]
mod access_postgres_tests;
#[cfg(test)]
mod aggregation_postgres_tests;
#[cfg(test)]
#[path = "run_shard_summary/postgres_tests.rs"]
mod postgres_tests;
#[cfg(test)]
mod query_postgres_tests;
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_summary_statuses_are_strict_and_exhaustive() {
        for terminal in ["completed", "failed"] {
            assert!(is_terminal_summary_status(terminal));
        }
        for nonterminal in ["", "running", "pending", "cancelled", "Completed"] {
            assert!(!is_terminal_summary_status(nonterminal));
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support;
