//! Typed summary-only result inspection for interface adapters.
//!
//! Scalar counts and scorecards reuse the routed summary workflow. Persistence,
//! configuration, attempts and diagnostics stay private. Coverage is explicit;
//! a complete summary set is not a cross-database atomic or terminal snapshot.
//! The CLI owns its established output envelope; hosting owns caller authority.
use chrono::{
    DateTime,
    Utc,
};
use serde::Serialize;
use uuid::Uuid;

use super::Error;
use crate::{
    context::database::DatabaseRouter,
    db::{
        tables::runs,
        workflows::run_results,
    },
    models::run::Run,
};

/// Run metadata already exposed by summary inspection, without configuration.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ResultsDetails {
    pub run_id: Uuid,
    pub run_key: String,
    pub status: String,
    pub gate_status: String,
    pub expected_execution_count: i32,
    pub terminal_execution_count: i32,
    pub passed_execution_count: i32,
    pub failed_execution_count: i32,
    pub errored_execution_count: i32,
    pub summary: serde_json::Value,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

/// Observed routed counts and scores. Missing summaries do not contribute zero
/// scores; the weighted average uses only actual scored executions.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ResultsSummary {
    pub execution_count: i64,
    pub aggregate_count: i64,
    pub passed_execution_count: i64,
    pub failed_execution_count: i64,
    pub error_execution_count: i64,
    pub skipped_execution_count: i64,
    pub missing_aggregate_count: i64,
    pub evaluator_result_count: i64,
    pub blocking_failure_count: i64,
    pub average_score: Option<f64>,
    pub min_score: Option<f64>,
    pub max_score: Option<f64>,
    pub planned_chunk_count: i64,
    pub settled_chunk_count: i64,
    pub sealed_shard_count: usize,
    pub missing_execution_count: i64,
    pub unresolved_execution_count: i64,
}

/// Summary-only observation. It contains no execution payloads or diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RunResults {
    pub details: ResultsDetails,
    pub summary: ResultsSummary,
    /// Absent until published, or when no scorecard is accessible to the caller.
    pub scorecard: Option<serde_json::Value>,
    /// Logical routes, not physical database count.
    pub execution_route_count: usize,
    pub shard_summary_count: usize,
    /// Every discovered route has a summary and the route set is nonempty.
    /// This does not mean the run is terminal or every chunk is settled.
    pub summary_complete: bool,
}
impl RunResults {
    pub(crate) fn from_projection(
        run: &Run,
        projection: run_results::RunResultsProjection,
        scorecard: Option<serde_json::Value>,
    ) -> Self {
        let s = projection.summary;
        Self {
            details: ResultsDetails {
                run_id: run.id,
                run_key: run.run_key.clone(),
                status: run.status.clone(),
                gate_status: run.gate_status.clone(),
                expected_execution_count: run.expected_execution_count,
                terminal_execution_count: run.terminal_execution_count,
                passed_execution_count: run.passed_execution_count,
                failed_execution_count: run.failed_execution_count,
                errored_execution_count: run.errored_execution_count,
                summary: run.summary.clone(),
                error_message: run.error_message.clone(),
                created_at: run.created_at,
                completed_at: run.completed_at,
                updated_at: run.updated_at,
            },
            summary: ResultsSummary {
                execution_count: s.execution_count,
                aggregate_count: s.aggregate_count,
                passed_execution_count: s.passed_execution_count,
                failed_execution_count: s.failed_execution_count,
                error_execution_count: s.error_execution_count,
                skipped_execution_count: s.skipped_execution_count,
                missing_aggregate_count: s.missing_aggregate_count,
                evaluator_result_count: s.evaluator_result_count,
                blocking_failure_count: s.blocking_failure_count,
                average_score: s.average_score,
                min_score: s.min_score,
                max_score: s.max_score,
                planned_chunk_count: s.planned_chunk_count,
                settled_chunk_count: s.settled_chunk_count,
                sealed_shard_count: s.sealed_shard_count,
                missing_execution_count: s.missing_execution_count,
                unresolved_execution_count: s.unresolved_execution_count,
            },
            scorecard,
            execution_route_count: projection.execution_route_count,
            shard_summary_count: projection.shard_summary_count,
            summary_complete: projection.execution_route_count > 0
                && projection.execution_route_count == projection.shard_summary_count,
        }
    }
}

/// Standalone and CLI inspection share this exact workflow and query sequence.
pub(crate) async fn read(router: &DatabaseRouter, id: Uuid) -> Result<RunResults, Error> {
    let db = router.control().await.map_err(Error::Dependency)?;
    let run = runs::select_run_by_id(db, id)
        .await
        .map_err(Error::Dependency)?
        .ok_or(Error::NotFound(id))?;
    let projection = run_results::select_run_results_projection(router, id)
        .await
        .map_err(Error::Dependency)?;
    let scorecard = run_results::select_run_scorecard(db, id)
        .await
        .map_err(Error::Dependency)?;
    Ok(RunResults::from_projection(&run, projection, scorecard))
}
