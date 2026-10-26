//! Compact creation progress shared by run status and coordinator reporting.
//!
//! Immutable plan publication and execution admission own creation writes.
//! Progress reads inspect at most 128 shard metadata entries; case payloads and
//! per-case membership are never needed to report resumable creation state.
use sqlx::PgPool;
use uuid::Uuid;

pub(crate) const RUN_STATUS_CREATING: &str = "creating";
pub(crate) const DEFAULT_CASE_BATCH_SIZE: usize = 1_000;
pub(crate) const DEFAULT_CASE_PAGE_BUDGET: usize = 64;
/// Bounded creation work policy retained by the command configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Config {
    pub(crate) case_batch_size: usize,
    pub(crate) case_page_budget: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            case_batch_size: DEFAULT_CASE_BATCH_SIZE,
            case_page_budget: DEFAULT_CASE_PAGE_BUDGET,
        }
    }
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RunCreationProgress {
    pub(crate) placement_count: i64,
    pub(crate) pending_placement_count: i64,
    pub(crate) seeded_placement_count: i64,
    pub(crate) failed_placement_count: i64,
    pub(crate) attempt_count: i64,
    pub(crate) last_error: Option<String>,
    pub(crate) plan_status: Option<String>,
    pub(crate) plan_chunk_cursor: Option<i32>,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct RunCreationRecoveryStats {
    pub(crate) claimed_runs: usize,
    pub(crate) completed_runs: usize,
    pub(crate) deferred_runs: usize,
    pub(crate) failed_runs: usize,
}
pub(crate) async fn select_creation_progress(
    db: &PgPool,
    run_id: Uuid,
) -> anyhow::Result<RunCreationProgress> {
    let row:Option<(String,i32,i32,i64,Option<String>,i64)>=sqlx::query_as("SELECT p.status,p.chunk_cursor,p.admission_cursor,p.generation,r.error_message,(SELECT count(DISTINCT database_alias) FROM run_plan_shards WHERE run_id=$1) FROM run_plans p JOIN runs r ON r.id=p.run_id WHERE p.run_id=$1")
        .bind(run_id).fetch_optional(db).await?;
    let Some((status, total, admitted, attempts, error, placements)) = row else {
        return Ok(RunCreationProgress::default());
    };
    let complete = status == "ready" && total == admitted;
    Ok(RunCreationProgress {
        placement_count: placements,
        pending_placement_count: if complete { 0 } else { placements },
        seeded_placement_count: if complete { placements } else { 0 },
        attempt_count: attempts,
        last_error: error,
        plan_status: Some(status),
        plan_chunk_cursor: Some(total),
        ..Default::default()
    })
}
