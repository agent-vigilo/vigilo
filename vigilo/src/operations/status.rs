//! Status projections keep persistence rows private and preserve explicit progress sources.
//! Reads delegate to the existing routed workflow without additional database calls.
use chrono::{
    DateTime,
    Utc,
};
use serde::Serialize;
use uuid::Uuid;

use super::Error;
use crate::{
    context::database::DatabaseRouter,
    db::workflows::run_status,
};

/// Observed execution and chunk counts; completeness is reported separately.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LiveProgress {
    pub expected_execution_count: i64,
    pub execution_count: i64,
    pub terminal_execution_count: i64,
    pub passed_execution_count: i64,
    pub failed_execution_count: i64,
    pub errored_execution_count: i64,
    pub skipped_execution_count: i64,
    pub missing_aggregate_count: i64,
    pub failed_chunk_count: i64,
    pub cancelled_chunk_count: i64,
    pub planned_chunk_count: i64,
    pub settled_chunk_count: i64,
    pub sealed_shard_count: usize,
    pub missing_execution_count: i64,
    pub unresolved_execution_count: i64,
}
/// Control-owned progress while a run is being prepared.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CreationProgress {
    pub placement_count: i64,
    pub pending_placement_count: i64,
    pub seeded_placement_count: i64,
    pub failed_placement_count: i64,
    pub attempt_count: i64,
    pub last_error: Option<String>,
    pub plan_status: Option<String>,
    pub plan_chunk_cursor: Option<i32>,
}
/// Run identity, lifecycle and progress projected without configuration or persistence internals.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RunDetails {
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
    pub started_at: Option<DateTime<Utc>>,
    pub dispatched_at: Option<DateTime<Utc>>,
    pub finalized_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub live_progress: LiveProgress,
    pub creation_progress: Option<CreationProgress>,
}
/// A bounded progress snapshot, not a cross-database atomic view.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RunStatus {
    pub details: RunDetails,
    pub terminal: bool,
    pub gate_passed: bool,
    /// `run_creation`, `execution_shards`, or `control_run`; never inferred from counts.
    pub progress_source: &'static str,
    /// True only when a summary exists for every nonempty execution route set.
    pub live_progress_complete: bool,
    /// Number of logical shard routes, not physical databases.
    pub execution_route_count: usize,
    /// Number of summaries found for the requested routes.
    pub shard_summary_count: usize,
}

pub(crate) async fn read(database: &DatabaseRouter, run_id: Uuid) -> Result<RunStatus, Error> {
    let projection = run_status::select_run_status(database, run_id)
        .await
        .map_err(Error::Dependency)?
        .ok_or(Error::NotFound(run_id))?;
    Ok(RunStatus::from(&projection))
}

impl From<&run_status::RunStatusProjection> for RunStatus {
    fn from(value: &run_status::RunStatusProjection) -> Self {
        let run = &value.run;
        Self {
            terminal: matches!(run.status.as_str(), "completed" | "failed" | "cancelled"),
            gate_passed: run.status == "completed" && run.gate_status == "pass",
            progress_source: value.progress_source(),
            live_progress_complete: value.live_progress_complete(),
            execution_route_count: value.execution_route_count,
            shard_summary_count: value.shard_summary_count,
            details: RunDetails {
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
                started_at: run.started_at,
                dispatched_at: run.dispatched_at,
                finalized_at: run.finalized_at,
                completed_at: run.completed_at,
                live_progress: LiveProgress {
                    expected_execution_count: value.live_progress.expected_execution_count,
                    execution_count: value.live_progress.execution_count,
                    terminal_execution_count: value.live_progress.terminal_execution_count,
                    passed_execution_count: value.live_progress.passed_execution_count,
                    failed_execution_count: value.live_progress.failed_execution_count,
                    errored_execution_count: value.live_progress.errored_execution_count,
                    skipped_execution_count: value.live_progress.skipped_execution_count,
                    missing_aggregate_count: value.live_progress.missing_aggregate_count,
                    failed_chunk_count: value.live_progress.failed_chunk_count,
                    cancelled_chunk_count: value.live_progress.cancelled_chunk_count,
                    planned_chunk_count: value.live_progress.planned_chunk_count,
                    settled_chunk_count: value.live_progress.settled_chunk_count,
                    sealed_shard_count: value.live_progress.sealed_shard_count,
                    missing_execution_count: value.live_progress.missing_execution_count,
                    unresolved_execution_count: value.live_progress.unresolved_execution_count,
                },
                creation_progress: value.creation_progress.as_ref().map(|progress| {
                    CreationProgress {
                        placement_count: progress.placement_count,
                        pending_placement_count: progress.pending_placement_count,
                        seeded_placement_count: progress.seeded_placement_count,
                        failed_placement_count: progress.failed_placement_count,
                        attempt_count: progress.attempt_count,
                        last_error: progress.last_error.clone(),
                        plan_status: progress.plan_status.clone(),
                        plan_chunk_cursor: progress.plan_chunk_cursor,
                    }
                }),
            },
        }
    }
}
