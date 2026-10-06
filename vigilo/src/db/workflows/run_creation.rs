//! Durable multi-database run creation workflow.
//!
//! The control database records a non-dispatchable creation plan before any remote
//! execution database commits. Seed writes are idempotent, so a coordinator can
//! resume the same `run_id` after a process or database failure. Dispatch
//! cursors are created only when every selected placement is seeded.

use std::collections::BTreeMap;

use sqlx::{
    PgPool,
    Postgres,
    QueryBuilder,
};
use tracing::{
    debug,
    warn,
};
use uuid::Uuid;

use super::{
    local_shard_admission::{
        LocalShardAdmissionDraft,
        LocalShardAdmissionState,
        upsert_local_shard_admission,
    },
    run_create::{
        self,
        RunShardPlacementAssignment,
    },
};
use crate::{
    context::database,
    db::workflows::case_projection,
    models::{
        case_blob::CaseBlobDraft,
        dataset_version_case::DatasetVersionCaseDraft,
        run::RunDraft,
        run_chunk::RunChunkDraft,
        run_shard_case::RunShardCaseDraft,
    },
};

mod queries;

#[cfg(test)]
use queries::activate_claimed_run;
pub(crate) use queries::select_creation_progress;
use queries::{
    acknowledge_projection_page,
    claim_newly_persisted_run,
    claim_next_creating_run,
    defer_placement,
    fail_pending_creation_placements,
    fail_placement_and_run,
    finish_claimed_run,
    insert_creation_chunks,
    insert_creation_placements,
    load_seed_material,
    mark_placement_seeded,
    mark_run_failed,
    select_creation_chunks,
    select_creation_outcome,
    select_pending_placements,
    select_placement_seed_progress,
    select_projection_page_blobs,
    select_projection_seed_page,
    start_placement_attempt,
    yield_claimed_run,
};

pub(crate) const RUN_STATUS_CREATING: &str = "creating";
const RUN_STATUS_PENDING: &str = "pending";
const RUN_STATUS_FAILED: &str = "failed";
const INITIAL_CREATION_LEASE_SECONDS: i32 = 300;

fn effective_creation_lease_seconds(requested_seconds: i32) -> i32 {
    requested_seconds.max(INITIAL_CREATION_LEASE_SECONDS)
}
const CREATION_RETRY_DELAY_SECONDS: i32 = 10;
pub(crate) const DEFAULT_CASE_BATCH_SIZE: usize = 1_000;
pub(crate) const DEFAULT_CASE_PAGE_BUDGET: usize = 64;

/// Bounded paging policy shared by immediate and coordinator run creation.
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RunCreationOutcome {
    pub(crate) status: String,
    pub(crate) progress: RunCreationProgress,
    pub(crate) error_message: Option<String>,
}

/// Immutable material persisted and seeded for one new run.
pub(crate) struct RunCreationRequest<'a> {
    pub(crate) run_id: Uuid,
    pub(crate) draft: &'a RunDraft,
    pub(crate) case_blobs: &'a [CaseBlobDraft],
    pub(crate) dataset_cases: &'a [DatasetVersionCaseDraft],
    pub(crate) chunks: &'a [RunChunkDraft],
    pub(crate) assignments: &'a [RunShardPlacementAssignment],
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct RunCreationRecoveryStats {
    pub(crate) claimed_runs: usize,
    pub(crate) completed_runs: usize,
    pub(crate) deferred_runs: usize,
    pub(crate) failed_runs: usize,
}

struct RunSeedMaterial<'a> {
    draft: &'a RunDraft,
}

struct OwnedRunSeedMaterial {
    draft: RunDraft,
}

#[derive(Debug, sqlx::FromRow)]
struct PlacementSeedProgress {
    expected_case_count: i64,
    seeded_case_count: i64,
    last_seeded_case_ordinal: Option<i32>,
    case_projection_hash: String,
}

/// Persists and immediately attempts a recoverable run creation operation.
pub(crate) async fn create_run(
    database_router: &database::DatabaseRouter,
    config: Config,
    request: RunCreationRequest<'_>,
) -> anyhow::Result<RunCreationOutcome> {
    let RunCreationRequest {
        run_id,
        draft,
        case_blobs,
        dataset_cases,
        chunks,
        assignments,
    } = request;
    let mut planned_draft = draft.clone();
    planned_draft.config_snapshot["settlement_plan"] =
        super::run_shard_summary::chunk_plan_manifests(chunks)?;
    let draft = &planned_draft;
    let owner_id = Uuid::now_v7();
    persist_creation_plan(
        database_router,
        owner_id,
        run_id,
        draft,
        case_blobs,
        dataset_cases,
        chunks,
        assignments,
    )
    .await?;

    let seed = RunSeedMaterial { draft };
    match resume_claimed_run(
        database_router,
        config,
        run_id,
        owner_id,
        seed,
        INITIAL_CREATION_LEASE_SECONDS,
    )
    .await
    {
        Ok(outcome) => Ok(outcome),
        Err(error) => {
            warn!(run_id = %run_id, error = %error, "run creation is durable but immediate seeding did not finish");
            Ok(RunCreationOutcome {
                status: RUN_STATUS_CREATING.to_string(),
                progress: select_creation_progress(database_router.control().await?, run_id)
                    .await?,
                error_message: Some(error.to_string()),
            })
        }
    }
}

/// Recovers a bounded set of expired `creating` runs.
pub(crate) async fn recover_creating_runs(
    database_router: &database::DatabaseRouter,
    config: Config,
    coordinator_id: Uuid,
    lease_seconds: i32,
    limit: usize,
) -> anyhow::Result<RunCreationRecoveryStats> {
    let mut stats = RunCreationRecoveryStats::default();

    for _ in 0..limit {
        let Some(run_id) = claim_next_creating_run(
            database_router.control().await?,
            coordinator_id,
            lease_seconds,
        )
        .await?
        else {
            break;
        };
        stats.claimed_runs += 1;

        let owned_seed = match load_seed_material(database_router.control().await?, run_id).await {
            Ok(seed) => seed,
            Err(error) if run_create::is_seed_invariant_error(&error) => {
                fail_claimed_run(
                    database_router.control().await?,
                    run_id,
                    coordinator_id,
                    &error.to_string(),
                )
                .await?;
                stats.failed_runs += 1;
                continue;
            }
            Err(error) => return Err(error),
        };
        let seed = RunSeedMaterial {
            draft: &owned_seed.draft,
        };

        match resume_claimed_run(
            database_router,
            config,
            run_id,
            coordinator_id,
            seed,
            lease_seconds,
        )
        .await
        {
            Ok(RunCreationOutcome { status, .. }) if status == RUN_STATUS_PENDING => {
                stats.completed_runs += 1;
            }
            Ok(RunCreationOutcome { status, .. }) if status == RUN_STATUS_FAILED => {
                stats.failed_runs += 1;
            }
            Ok(_) => stats.deferred_runs += 1,
            Err(error) if run_create::is_seed_invariant_error(&error) => {
                fail_claimed_run(
                    database_router.control().await?,
                    run_id,
                    coordinator_id,
                    &error.to_string(),
                )
                .await?;
                stats.failed_runs += 1;
            }
            Err(error) => return Err(error),
        }
    }

    Ok(stats)
}

#[allow(clippy::too_many_arguments)]
async fn persist_creation_plan(
    database_router: &database::DatabaseRouter,
    owner_id: Uuid,
    run_id: Uuid,
    draft: &RunDraft,
    case_blobs: &[CaseBlobDraft],
    dataset_cases: &[DatasetVersionCaseDraft],
    chunks: &[RunChunkDraft],
    assignments: &[RunShardPlacementAssignment],
) -> anyhow::Result<()> {
    let chunks_by_alias = run_create::group_chunks_by_assigned_alias(chunks, assignments)?;
    let projections_by_alias = build_placement_projections(
        run_id,
        draft.dataset_version_id,
        dataset_cases,
        &chunks_by_alias,
    )?;
    let control_db = database_router.control().await?;
    let mut tx = control_db.begin().await?;

    run_create::bulk_insert_case_blobs(&mut tx, case_blobs).await?;
    run_create::upsert_dataset_version(
        &mut tx,
        draft.dataset_version_id,
        draft.dataset_id,
        &draft.dataset_version,
    )
    .await?;
    run_create::bulk_insert_dataset_membership(&mut tx, draft.dataset_version_id, dataset_cases)
        .await?;
    run_create::insert_run_create(&mut tx, run_id, draft, RUN_STATUS_CREATING).await?;
    run_create::bulk_insert_shard_placements(&mut tx, run_id, assignments).await?;
    insert_creation_placements(&mut tx, run_id, &projections_by_alias).await?;
    insert_creation_chunks(&mut tx, run_id, &chunks_by_alias).await?;

    let claimed =
        claim_newly_persisted_run(&mut tx, run_id, owner_id, INITIAL_CREATION_LEASE_SECONDS)
            .await?;
    if claimed != 1 {
        anyhow::bail!("failed to claim newly persisted run creation '{}'", run_id);
    }

    tx.commit().await?;
    Ok(())
}

fn build_placement_projections(
    run_id: Uuid,
    dataset_version_id: Uuid,
    dataset_cases: &[DatasetVersionCaseDraft],
    chunks_by_alias: &BTreeMap<String, Vec<RunChunkDraft>>,
) -> anyhow::Result<BTreeMap<String, Vec<RunShardCaseDraft>>> {
    let mut projections = BTreeMap::new();
    let mut assigned_ordinals = std::collections::BTreeSet::new();
    for (alias, chunks) in chunks_by_alias {
        let projection = case_projection::project_cases_for_chunks(
            run_id,
            dataset_version_id,
            dataset_cases,
            chunks,
        )?;
        for row in &projection {
            if !assigned_ordinals.insert(row.case_ordinal) {
                anyhow::bail!(
                    "case ordinal {} is assigned to multiple execution placements",
                    row.case_ordinal
                );
            }
        }
        projections.insert(alias.clone(), projection);
    }
    if assigned_ordinals.len() != dataset_cases.len() {
        anyhow::bail!(
            "run creation assigns {} of {} canonical cases",
            assigned_ordinals.len(),
            dataset_cases.len()
        );
    }
    Ok(projections)
}

async fn resume_claimed_run(
    database_router: &database::DatabaseRouter,
    config: Config,
    run_id: Uuid,
    owner_id: Uuid,
    seed: RunSeedMaterial<'_>,
    lease_seconds: i32,
) -> anyhow::Result<RunCreationOutcome> {
    let control_db = database_router.control().await?;
    let pending_aliases = select_pending_placements(control_db, run_id, owner_id).await?;

    for database_alias in pending_aliases {
        let database_permit = match database_router.acquire_database_operation(&database_alias) {
            Ok(permit) => permit,
            Err(open) => {
                debug!(
                    run_id = %run_id,
                    database_alias,
                    retry_after_ms = open.retry_after.as_millis() as u64,
                    "deferred run creation placement while database circuit is open"
                );
                yield_claimed_run(control_db, run_id, owner_id).await?;
                return select_creation_outcome(control_db, run_id).await;
            }
        };
        start_placement_attempt(control_db, run_id, owner_id, &database_alias, lease_seconds)
            .await?;
        let chunks = match select_creation_chunks(control_db, run_id, &database_alias).await {
            Ok(chunks) => chunks,
            Err(error) if run_create::is_seed_invariant_error(&error) => {
                fail_placement_and_run(
                    control_db,
                    run_id,
                    owner_id,
                    &database_alias,
                    &error.to_string(),
                )
                .await?;
                return select_creation_outcome(control_db, run_id).await;
            }
            Err(error) => return Err(error),
        };
        let seed_result = database_router
            .deadline_database_operation(
                &database_alias,
                "run_creation_seed",
                seed_execution_placement(
                    database_router,
                    config,
                    run_id,
                    owner_id,
                    &database_alias,
                    seed.draft,
                    &chunks,
                    lease_seconds,
                ),
            )
            .await
            .map_err(anyhow::Error::new)
            .and_then(std::convert::identity);

        match &seed_result {
            Ok(_) => {
                if matches!(
                    database_router.record_database_operation_success(database_permit),
                    Some(database::CircuitTransition::Closed)
                ) {
                    debug!(
                        run_id = %run_id,
                        database_alias,
                        "closed execution database circuit after successful creation probe"
                    );
                }
            }
            Err(error) => {
                let (_, transition) =
                    database_router.record_database_operation_error(database_permit, error);
                if let Some(database::CircuitTransition::Opened { retry_after }) = transition {
                    warn!(
                        run_id = %run_id,
                        database_alias,
                        retry_after_ms = retry_after.as_millis() as u64,
                        "opened execution database circuit after creation availability failures"
                    );
                }
            }
        }

        let placement_complete = match seed_result {
            Ok(complete) => complete,
            Err(error) => {
                if run_create::is_seed_invariant_error(&error) {
                    fail_placement_and_run(
                        control_db,
                        run_id,
                        owner_id,
                        &database_alias,
                        &error.to_string(),
                    )
                    .await?;
                } else {
                    defer_placement(
                        control_db,
                        run_id,
                        owner_id,
                        &database_alias,
                        &error.to_string(),
                    )
                    .await?;
                }
                return select_creation_outcome(control_db, run_id).await;
            }
        };
        if !placement_complete {
            yield_claimed_run(control_db, run_id, owner_id).await?;
            return select_creation_outcome(control_db, run_id).await;
        }

        if let Err(error) =
            mark_placement_seeded(control_db, run_id, owner_id, &database_alias, lease_seconds)
                .await
        {
            defer_placement(
                control_db,
                run_id,
                owner_id,
                &database_alias,
                &error.to_string(),
            )
            .await?;
            return select_creation_outcome(control_db, run_id).await;
        }
    }

    finish_claimed_run(control_db, run_id, owner_id).await?;
    select_creation_outcome(control_db, run_id).await
}

#[allow(clippy::too_many_arguments)]
async fn seed_execution_placement(
    database_router: &database::DatabaseRouter,
    config: Config,
    run_id: Uuid,
    owner_id: Uuid,
    database_alias: &str,
    draft: &RunDraft,
    chunks: &[RunChunkDraft],
    lease_seconds: i32,
) -> anyhow::Result<bool> {
    let db = database_router.execution_database(database_alias).await?;
    let control_db = database_router.control().await?;
    let progress = select_placement_seed_progress(control_db, run_id, database_alias).await?;
    let batch_size = config.case_batch_size;
    let page_budget = config.case_page_budget;
    let mut acknowledged_count = progress.seeded_case_count;
    let mut acknowledged_ordinal = progress.last_seeded_case_ordinal;
    let mut reached_projection_end = false;
    for _ in 0..page_budget {
        let page_rows = select_projection_seed_page(
            control_db,
            run_id,
            database_alias,
            acknowledged_ordinal,
            batch_size,
        )
        .await?;
        if page_rows.is_empty() {
            reached_projection_end = true;
            break;
        }
        let page_blobs = select_projection_page_blobs(control_db, &page_rows).await?;

        let mut tx = db.begin().await?;
        run_create::bulk_insert_case_blobs(&mut tx, &page_blobs).await?;
        run_create::upsert_dataset_version(
            &mut tx,
            draft.dataset_version_id,
            draft.dataset_id,
            &draft.dataset_version,
        )
        .await?;
        let local_run_status = if database_alias == database_router.control_database_alias() {
            RUN_STATUS_CREATING
        } else {
            RUN_STATUS_PENDING
        };
        run_create::insert_run_create(&mut tx, run_id, draft, local_run_status).await?;
        case_projection::insert_projection_page(&mut tx, &page_rows).await?;
        tx.commit().await?;

        let last_ordinal = page_rows
            .last()
            .expect("projection pages are non-empty")
            .case_ordinal;
        acknowledge_projection_page(
            control_db,
            run_id,
            owner_id,
            database_alias,
            acknowledged_count,
            acknowledged_ordinal,
            page_rows.len() as i64,
            last_ordinal,
            lease_seconds,
        )
        .await?;
        acknowledged_count += page_rows.len() as i64;
        acknowledged_ordinal = Some(last_ordinal);
    }
    if acknowledged_count > progress.expected_case_count
        || (reached_projection_end && acknowledged_count != progress.expected_case_count)
    {
        return Err(run_create::seed_invariant_error(format!(
            "run creation '{}' placement '{}' acknowledged {} of {} planned cases",
            run_id, database_alias, acknowledged_count, progress.expected_case_count
        )));
    }
    if acknowledged_count < progress.expected_case_count {
        return Ok(false);
    }

    let mut placement_shards = chunks
        .iter()
        .map(|chunk| chunk.run_shard)
        .collect::<Vec<_>>();
    placement_shards.sort_unstable();
    placement_shards.dedup();
    let (stored_count, stored_hash) =
        case_projection::projection_fingerprint(&db, run_id, &placement_shards).await?;
    if stored_count != progress.expected_case_count || stored_hash != progress.case_projection_hash
    {
        return Err(run_create::seed_invariant_error(format!(
            "run creation '{}' placement '{}' projection verification failed",
            run_id, database_alias
        )));
    }

    let mut tx = db.begin().await?;
    for run_shard in placement_shards {
        upsert_local_shard_admission(
            &mut *tx,
            LocalShardAdmissionDraft {
                run_id,
                run_shard,
                database_alias: database_alias.to_string(),
                write_epoch: 1,
                state: LocalShardAdmissionState::Open,
                redirect_database_alias: None,
                move_fence: None,
            },
        )
        .await?;
    }
    run_create::bulk_insert_run_chunks(&mut tx, run_id, draft.dataset_version_id, chunks).await?;
    tx.commit().await?;
    Ok(true)
}

async fn fail_claimed_run(
    db: &PgPool,
    run_id: Uuid,
    owner_id: Uuid,
    error: &str,
) -> anyhow::Result<()> {
    let mut tx = db.begin().await?;
    fail_pending_creation_placements(&mut tx, run_id, owner_id, error).await?;
    mark_run_failed(&mut tx, run_id, owner_id, error).await?;
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
#[path = "run_creation/postgres_tests.rs"]
mod postgres_tests;
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creation_lease_enforces_the_recovery_floor() {
        assert_eq!(effective_creation_lease_seconds(-1), 300);
        assert_eq!(effective_creation_lease_seconds(300), 300);
        assert_eq!(effective_creation_lease_seconds(600), 600);
    }

    fn projection_case(ordinal: i32) -> DatasetVersionCaseDraft {
        DatasetVersionCaseDraft {
            case_id: Uuid::from_u128(ordinal as u128 + 1),
            case_ordinal: ordinal,
            case_hash: format!("hash-{ordinal}"),
        }
    }

    fn projection_chunk(run_shard: i16, start: i32, end: i32) -> RunChunkDraft {
        RunChunkDraft {
            chunk_id: Uuid::now_v7(),
            run_shard,
            profile_group_id: "default".to_string(),
            ordinal_start: start,
            ordinal_end: end,
        }
    }

    #[test]
    fn placement_projection_plan_covers_every_case_once() {
        let run_id = Uuid::now_v7();
        let dataset_version_id = Uuid::now_v7();
        let cases = (0..4).map(projection_case).collect::<Vec<_>>();
        let chunks = BTreeMap::from([
            ("primary".to_string(), vec![projection_chunk(0, 0, 2)]),
            ("shard_001".to_string(), vec![projection_chunk(1, 2, 4)]),
        ]);

        let projections =
            build_placement_projections(run_id, dataset_version_id, &cases, &chunks).unwrap();

        assert_eq!(projections["primary"].len(), 2);
        assert_eq!(projections["shard_001"].len(), 2);
    }

    #[test]
    fn placement_projection_plan_rejects_unassigned_cases() {
        let error = build_placement_projections(
            Uuid::now_v7(),
            Uuid::now_v7(),
            &(0..3).map(projection_case).collect::<Vec<_>>(),
            &BTreeMap::from([("primary".to_string(), vec![projection_chunk(0, 0, 2)])]),
        )
        .unwrap_err();

        assert!(error.to_string().contains("assigns 2 of 3"));
    }

    #[test]
    fn placement_projection_plan_rejects_cross_placement_overlap() {
        let cases = (0..3).map(projection_case).collect::<Vec<_>>();
        let chunks = BTreeMap::from([
            ("primary".to_string(), vec![projection_chunk(0, 0, 2)]),
            ("shard_001".to_string(), vec![projection_chunk(1, 1, 3)]),
        ]);

        let error = build_placement_projections(Uuid::now_v7(), Uuid::now_v7(), &cases, &chunks)
            .unwrap_err();

        assert!(error.to_string().contains("ordinal 1"));
        assert!(error.to_string().contains("multiple execution placements"));
    }

    #[test]
    fn placement_projection_plan_rejects_invalid_local_chunks() {
        let cases = (0..2).map(projection_case).collect::<Vec<_>>();
        let chunks = BTreeMap::from([("primary".to_string(), vec![projection_chunk(0, 1, 1)])]);

        let error = build_placement_projections(Uuid::now_v7(), Uuid::now_v7(), &cases, &chunks)
            .unwrap_err();

        assert!(error.to_string().contains("invalid ordinal range"));
    }

    #[test]
    fn empty_projection_plan_is_valid_for_an_empty_dataset() {
        let projections =
            build_placement_projections(Uuid::now_v7(), Uuid::now_v7(), &[], &BTreeMap::new())
                .unwrap();

        assert!(projections.is_empty());
    }

    #[test]
    fn run_creation_config_has_bounded_defaults() {
        assert_eq!(
            Config::default(),
            Config {
                case_batch_size: 1_000,
                case_page_budget: 64,
            }
        );
    }
}
