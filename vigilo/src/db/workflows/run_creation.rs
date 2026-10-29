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
/// Run status and creation progress observed in one PostgreSQL statement snapshot.
/// This is reporting data, never authority for a later mutation.
#[derive(Debug)]
pub(crate) struct RunCreationState {
    pub(crate) status: String,
    pub(crate) error: Option<String>,
    pub(crate) progress: RunCreationProgress,
}

#[derive(sqlx::FromRow)]
struct CreationRow {
    status: String,
    error: Option<String>,
    plan_status: Option<String>,
    total: Option<i32>,
    admitted: Option<i32>,
    attempts: Option<i64>,
    placements: i64,
}

/// Returns current status and bounded placement progress from the same snapshot.
/// Missing runs return None; runs without a blob plan retain empty progress.
pub(crate) async fn select_creation_state(
    db: &PgPool,
    run_id: Uuid,
) -> anyhow::Result<Option<RunCreationState>> {
    let row: Option<CreationRow> = sqlx::query_as(
        "SELECT r.status::text AS status, r.error_message AS error,
                p.status AS plan_status, p.chunk_cursor AS total,
                p.admission_cursor AS admitted, p.generation AS attempts,
                (SELECT count(DISTINCT database_alias) FROM run_plan_shards WHERE run_id=$1) AS placements
         FROM runs r LEFT JOIN run_plans p ON p.run_id=r.id WHERE r.id=$1",
    ).bind(run_id).fetch_optional(db).await?;
    Ok(row.map(|row| {
        let progress = if let Some(status) = row.plan_status {
            let complete = status == "ready" && row.total == row.admitted;
            RunCreationProgress {
                placement_count: row.placements,
                pending_placement_count: if complete { 0 } else { row.placements },
                seeded_placement_count: if complete { row.placements } else { 0 },
                attempt_count: row.attempts.expect("plan generation is non-null"),
                last_error: row.error.clone(),
                plan_status: Some(status),
                plan_chunk_cursor: row.total,
                ..Default::default()
            }
        } else {
            RunCreationProgress::default()
        };
        RunCreationState {
            status: row.status,
            error: row.error,
            progress,
        }
    }))
}

pub(crate) async fn select_creation_progress(
    db: &PgPool,
    run_id: Uuid,
) -> anyhow::Result<RunCreationProgress> {
    Ok(select_creation_state(db, run_id)
        .await?
        .map(|state| state.progress)
        .unwrap_or_default())
}
