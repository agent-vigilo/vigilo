//! Shard placement administration workflows.
//!
//! These helpers keep shard topology guardrails behind the database workflow
//! boundary. CLI commands call them to list database placements, add shard
//! databases, verify and activate their targets, drain or disable shard
//! databases, assign empty run shards, and move or restore shard-owned routes.

use std::collections::BTreeMap;

use chrono::{
    DateTime,
    Utc,
};
use futures_util::TryStreamExt;
use serde::Serialize;
use serde_json::Value;
use sqlx::{
    Executor,
    PgPool,
    Postgres,
    Transaction,
    types::Json,
};
use tracing::{
    info,
    warn,
};
use uuid::Uuid;

use crate::{
    context::database,
    db::{
        tables::{
            database_placements,
            outbox_events,
            shard_placements,
        },
        workflows::local_shard_admission::{
            LocalShardAdmissionDraft,
            LocalShardAdmissionState,
            LocalShardMoveFence,
            install_local_shard_move_fence,
            select_local_shard_admission,
            transition_local_shard_admission,
            validate_local_shard_move_fence,
        },
    },
    models::{
        database_placement::{
            DATABASE_PLACEMENT_ROLE_SHARD,
            DATABASE_PLACEMENT_STATUS_ACTIVE,
            DATABASE_PLACEMENT_STATUS_DISABLED,
            DATABASE_PLACEMENT_STATUS_DRAINING,
            DATABASE_PLACEMENT_STATUS_PROVISIONING,
            DatabasePlacement,
        },
        run_chunk::RUN_SHARD_COUNT,
        shard_placement::{
            SHARD_PLACEMENT_STATUS_ACTIVE,
            SHARD_PLACEMENT_STATUS_COPYING,
            SHARD_PLACEMENT_STATUS_DRAINING,
            SHARD_PLACEMENT_STATUS_MOVING,
            ShardPlacement,
        },
    },
};

mod queries;

use queries::*;

#[derive(Debug, Clone)]
pub(crate) struct ShardPlacementSetOutcome {
    pub(crate) placement: ShardPlacement,
    pub(crate) previous_database_alias: Option<String>,
    pub(crate) changed: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct DatabasePlacementActivationOutcome {
    pub(crate) placement: DatabasePlacement,
    pub(crate) changed: bool,
    pub(crate) readiness_verified: bool,
    pub(crate) migration_count: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
struct MigrationFingerprint {
    version: i64,
    description: String,
    checksum: Vec<u8>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ShardMoveTableReport {
    pub(crate) table: &'static str,
    pub(crate) source_row_count: Option<i64>,
    pub(crate) target_row_count: Option<i64>,
    pub(crate) copied_row_count: u64,
    pub(crate) source_checksum: Option<String>,
    pub(crate) target_checksum: Option<String>,
    pub(crate) verification_mode: &'static str,
    pub(crate) verified: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ShardMoveOutcome {
    pub(crate) run_id: Uuid,
    pub(crate) run_shard: i16,
    pub(crate) source_database_alias: String,
    pub(crate) target_database_alias: String,
    pub(crate) dry_run: bool,
    pub(crate) verify_only: bool,
    pub(crate) forced: bool,
    pub(crate) active_work_count: i64,
    pub(crate) moved: bool,
    pub(crate) placement: ShardPlacement,
    pub(crate) tables: Vec<ShardMoveTableReport>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ShardMoveAbortOutcome {
    pub(crate) run_id: Uuid,
    pub(crate) run_shard: i16,
    pub(crate) source_database_alias: String,
    pub(crate) target_database_alias: String,
    pub(crate) aborted: bool,
    pub(crate) placement: ShardPlacement,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ShardRouteInspection {
    pub(crate) run_id: Uuid,
    pub(crate) run_shard: i16,
    pub(crate) database_alias: String,
    pub(crate) shard_placement_status: String,
    pub(crate) move_target_database_alias: Option<String>,
    pub(crate) route_version: i64,
    pub(crate) database_role: String,
    pub(crate) database_status: String,
    pub(crate) database_url_env: String,
    pub(crate) database_url_env_resolved: bool,
    pub(crate) dispatchable: bool,
    pub(crate) readable: bool,
    pub(crate) routing_decision: &'static str,
    pub(crate) move_operation_id: Option<Uuid>,
    pub(crate) move_phase: Option<String>,
    pub(crate) move_completed_page_count: Option<i64>,
    pub(crate) move_copied_row_count: Option<i64>,
    pub(crate) move_copied_byte_count: Option<i64>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub(crate) struct ShardRebalanceOperation {
    pub(crate) id: Uuid,
    pub(crate) strategy: String,
    pub(crate) source_database_alias: Option<String>,
    pub(crate) target_database_alias: String,
    pub(crate) status: String,
    pub(crate) planned_item_count: i32,
    pub(crate) completed_item_count: i32,
    pub(crate) failed_item_count: i32,
    pub(crate) cancelled_item_count: i32,
    pub(crate) error_message: Option<String>,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) started_at: Option<DateTime<Utc>>,
    pub(crate) completed_at: Option<DateTime<Utc>>,
    pub(crate) cancelled_at: Option<DateTime<Utc>>,
    pub(crate) updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub(crate) struct ShardRebalanceItem {
    pub(crate) operation_id: Uuid,
    pub(crate) sequence_no: i32,
    pub(crate) run_id: Uuid,
    pub(crate) run_shard: i16,
    pub(crate) source_database_alias: String,
    pub(crate) target_database_alias: String,
    pub(crate) planned_route_version: i64,
    pub(crate) status: String,
    pub(crate) error_message: Option<String>,
    pub(crate) claim_token: Option<Uuid>,
    pub(crate) claimed_by: Option<Uuid>,
    pub(crate) claimed_until: Option<DateTime<Utc>>,
    pub(crate) started_at: Option<DateTime<Utc>>,
    pub(crate) completed_at: Option<DateTime<Utc>>,
    pub(crate) updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct PlannedShardRebalanceItem {
    pub(crate) run_id: Uuid,
    pub(crate) run_shard: i16,
    pub(crate) source_database_alias: String,
    pub(crate) target_database_alias: String,
    pub(crate) planned_route_version: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct ShardRebalancePlanOptions {
    pub(crate) source_database_alias: Option<String>,
    pub(crate) target_database_alias: String,
    pub(crate) max_items: usize,
    pub(crate) dry_run: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ShardRebalancePlanOutcome {
    pub(crate) operation: Option<ShardRebalanceOperation>,
    pub(crate) items: Vec<PlannedShardRebalanceItem>,
    pub(crate) dry_run: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ShardRebalanceApplyOptions {
    pub(crate) max_items: usize,
    pub(crate) lease_seconds: i32,
    pub(crate) force: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ShardRebalanceApplyOutcome {
    pub(crate) operation: ShardRebalanceOperation,
    pub(crate) processed_items: Vec<ShardRebalanceItem>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ShardRebalanceVerifyItem {
    pub(crate) item: ShardRebalanceItem,
    pub(crate) verified: bool,
    pub(crate) tables: Vec<ShardMoveTableReport>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ShardRebalanceVerifyOutcome {
    pub(crate) operation: ShardRebalanceOperation,
    pub(crate) items: Vec<ShardRebalanceVerifyItem>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ShardMoveOptions {
    pub(crate) dry_run: bool,
    pub(crate) verify_only: bool,
    pub(crate) force: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ShardRebalanceCandidate {
    run_id: Uuid,
    run_shard: i16,
    source_database_alias: String,
    route_version: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct ClaimedShardRebalanceItem {
    #[sqlx(flatten)]
    item: ShardRebalanceItem,
    reclaimed: bool,
}

#[derive(Debug, thiserror::Error)]
#[error(
    "planned route is stale for run {run_id} shard {run_shard}; expected {expected_alias} version {expected_version}, found {actual_alias} status {actual_status} version {actual_version}"
)]
struct StaleRebalancePlanError {
    run_id: Uuid,
    run_shard: i16,
    expected_alias: String,
    expected_version: i64,
    actual_alias: String,
    actual_status: String,
    actual_version: i64,
}

#[derive(Debug, thiserror::Error)]
#[error(
    "run {run_id} shard {run_shard} has {active_work_count} leased/running row(s); the route remains draining until work finishes, and --force cannot bypass the shard write fence"
)]
struct ShardMoveDrainPending {
    run_id: Uuid,
    run_shard: i16,
    active_work_count: i64,
}

#[derive(Debug, thiserror::Error)]
#[error(
    "run {run_id} shard {run_shard} retains {remaining_dirty_key_count} dirty key(s) after bounded online catch-up; the route remains copying and the move can be resumed"
)]
struct ShardMoveCatchUpPending {
    run_id: Uuid,
    run_shard: i16,
    remaining_dirty_key_count: i64,
}

#[derive(Debug, Clone, Copy)]
struct ShardTable {
    name: &'static str,
    key_columns: &'static [&'static str],
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct TableFingerprint {
    row_count: i64,
    checksum: String,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct ShardMoveOperation {
    id: Uuid,
    run_id: Uuid,
    run_shard: i16,
    source_database_alias: String,
    target_database_alias: String,
    status: String,
    phase: String,
    target_reset_at: Option<DateTime<Utc>>,
    claim_generation: i64,
    claim_token: Option<Uuid>,
}

#[derive(Debug, Clone)]
struct TargetMoveWriteFence {
    run_id: Uuid,
    run_shard: i16,
    database_alias: String,
    write_epoch: i64,
    move_fence: LocalShardMoveFence,
}

#[derive(Debug)]
struct PreparedShardMove {
    route: ShardPlacement,
    target_fence: Option<TargetMoveWriteFence>,
}

struct ShardMoveCopyContext<'a> {
    control_db: &'a PgPool,
    source_db: &'a PgPool,
    target_db: &'a PgPool,
    target_fence: &'a TargetMoveWriteFence,
}

impl TargetMoveWriteFence {
    fn from_operation(
        operation: &ShardMoveOperation,
        write_epoch: i64,
        claim_token: Uuid,
    ) -> anyhow::Result<Self> {
        if operation.claim_generation <= 0 || operation.claim_token != Some(claim_token) {
            anyhow::bail!(
                "shard move {} returned invalid claim authority",
                operation.id
            );
        }
        Ok(Self {
            run_id: operation.run_id,
            run_shard: operation.run_shard,
            database_alias: operation.target_database_alias.clone(),
            write_epoch,
            move_fence: LocalShardMoveFence {
                move_id: operation.id,
                claim_generation: operation.claim_generation,
                claim_token,
            },
        })
    }
}

#[derive(Debug, sqlx::FromRow)]
struct MoveSourceRow {
    row: Value,
    row_key: String,
    row_bytes: i32,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct DirtyShardKey {
    table_name: String,
    row_key: Value,
    change_version: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct ShardMoveInspection {
    id: Uuid,
    phase: String,
    completed_page_count: i64,
    copied_row_count: i64,
    copied_byte_count: i64,
}

const PREREQUISITE_TABLES: &[&str] = &["case_blobs", "dataset_versions", "runs"];
const SHARD_TABLES: &[ShardTable] = &[
    ShardTable {
        name: "run_shard_cases",
        key_columns: &["run_id", "run_shard", "case_id"],
    },
    ShardTable {
        name: "run_chunks",
        key_columns: &["run_id", "run_shard", "id"],
    },
    ShardTable {
        name: "run_snapshots",
        key_columns: &["run_id", "run_shard"],
    },
    ShardTable {
        name: "executions",
        key_columns: &["run_id", "run_shard", "id"],
    },
    ShardTable {
        name: "execution_attempts",
        key_columns: &["run_id", "run_shard", "id"],
    },
    ShardTable {
        name: "execution_aggregates",
        key_columns: &["run_id", "run_shard", "execution_id"],
    },
    ShardTable {
        name: "evaluator_results",
        key_columns: &["run_id", "run_shard", "id"],
    },
    ShardTable {
        name: "evaluator_diagnostics",
        key_columns: &["run_id", "run_shard", "id"],
    },
    ShardTable {
        name: "run_chunk_settlements",
        key_columns: &["run_id", "run_shard", "chunk_id"],
    },
    ShardTable {
        name: "run_chunk_settlement_consumptions",
        key_columns: &["run_id", "run_shard", "chunk_id"],
    },
    ShardTable {
        name: "run_shard_summaries",
        key_columns: &["run_id", "run_shard"],
    },
];
const SHARD_MOVE_COPY_BATCH_SIZE: usize = 1_000;
const SHARD_MOVE_COPY_BATCH_BYTES: usize = 4 * 1024 * 1024;
const SHARD_MOVE_ONLINE_REPLAY_BATCHES: usize = 100;
const SHARD_MOVE_FINAL_DIRTY_KEY_LIMIT: i64 = SHARD_MOVE_COPY_BATCH_SIZE as i64;

fn bounded_page_len(row_bytes: &[usize], max_rows: usize, max_bytes: usize) -> usize {
    if row_bytes.is_empty() || max_rows == 0 {
        return 0;
    }
    let mut bytes = 0usize;
    let mut rows = 0;
    for row_bytes in row_bytes.iter().copied().take(max_rows) {
        if rows > 0 && bytes.saturating_add(row_bytes) > max_bytes {
            break;
        }
        bytes = bytes.saturating_add(row_bytes);
        rows += 1;
    }
    rows
}

#[derive(Debug, PartialEq, Eq)]
struct MovePageCheckpoint {
    end_key: String,
    row_count: i64,
    byte_count: i64,
    checksum: String,
}

fn move_page_checkpoint(rows: &[MoveSourceRow]) -> anyhow::Result<MovePageCheckpoint> {
    let Some(last_row) = rows.last() else {
        anyhow::bail!("cannot checkpoint an empty shard move page");
    };
    let mut hasher = blake3::Hasher::new();
    for row in rows {
        let encoded = serde_json::to_vec(&row.row)?;
        hasher.update(&(encoded.len() as u64).to_be_bytes());
        hasher.update(&encoded);
    }
    Ok(MovePageCheckpoint {
        end_key: last_row.row_key.clone(),
        row_count: i64::try_from(rows.len())?,
        byte_count: rows.iter().map(|row| i64::from(row.row_bytes.max(0))).sum(),
        checksum: hasher.finalize().to_hex().to_string(),
    })
}

const REBALANCE_STRATEGY_DRAIN_SOURCE: &str = "drain-source";
const REBALANCE_STRATEGY_FILL_TARGET: &str = "fill-target";
const REBALANCE_OPERATION_STATUS_RUNNING: &str = "running";
const REBALANCE_OPERATION_STATUS_COMPLETED: &str = "completed";
const REBALANCE_OPERATION_STATUS_CANCELLED: &str = "cancelled";
const REBALANCE_OPERATION_STATUS_FAILED: &str = "failed";
const REBALANCE_ITEM_STATUS_PENDING: &str = "pending";
const REBALANCE_ITEM_STATUS_CANCELLED: &str = "cancelled";
const REBALANCE_ITEM_STATUS_COMPLETED: &str = "completed";
const REBALANCE_ITEM_STATUS_FAILED: &str = "failed";

pub(crate) async fn list_database_placements(
    db: &PgPool,
) -> anyhow::Result<Vec<DatabasePlacement>> {
    database_placements::list_database_placements(db).await
}

pub(crate) async fn add_shard_database_placement(
    db: &PgPool,
    alias: &str,
    database_url_env: &str,
    defer_env_validation: bool,
) -> anyhow::Result<DatabasePlacement> {
    validate_non_empty(alias, "database alias")?;
    validate_non_empty(database_url_env, "database_url_env")?;

    if database_placements::select_database_placement(db, alias)
        .await?
        .is_some()
    {
        anyhow::bail!("database placement alias {} already exists", alias);
    }

    if !defer_env_validation {
        std::env::var(database_url_env).map_err(|_| {
            anyhow::anyhow!(
                "database_url_env {} is not set in the current process; set it or pass --defer-env-validation",
                database_url_env
            )
        })?;
    }

    database_placements::insert_database_placement(
        db,
        alias,
        database_url_env,
        DATABASE_PLACEMENT_ROLE_SHARD,
        DATABASE_PLACEMENT_STATUS_PROVISIONING,
    )
    .await
}

/// Verifies a registered target and atomically makes it eligible for routing.
///
/// Target I/O runs before the placement row is locked. The final control
/// transaction rechecks the lifecycle and catalog references so slow readiness
/// probes never block unrelated topology work or expose an unchecked target.
pub(crate) async fn activate_database_placement(
    database_router: &database::DatabaseRouter,
    alias: &str,
) -> anyhow::Result<DatabasePlacementActivationOutcome> {
    validate_non_empty(alias, "database alias")?;

    let control_db = database_router.control().await?;
    let snapshot = database_placements::select_database_placement(control_db, alias)
        .await?
        .ok_or_else(|| anyhow::anyhow!("database placement alias {} was not found", alias))?;

    if snapshot.status == DATABASE_PLACEMENT_STATUS_ACTIVE {
        return Ok(DatabasePlacementActivationOutcome {
            placement: snapshot,
            changed: false,
            readiness_verified: false,
            migration_count: None,
        });
    }
    require_provisioning_shard_placement(&snapshot)?;

    let migration_count = database_router
        .run_database_operation(alias, "database_activation", async {
            verify_database_placement_readiness(database_router, &snapshot).await
        })
        .await?;

    let mut tx = control_db.begin().await?;
    let current = database_placements::select_database_placement_for_update(&mut tx, alias)
        .await?
        .ok_or_else(|| anyhow::anyhow!("database placement alias {} was not found", alias))?;

    if current.status == DATABASE_PLACEMENT_STATUS_ACTIVE {
        tx.commit().await?;
        return Ok(DatabasePlacementActivationOutcome {
            placement: current,
            changed: false,
            readiness_verified: true,
            migration_count: Some(migration_count),
        });
    }
    require_provisioning_shard_placement(&current)?;
    if current.database_url_env != snapshot.database_url_env || current.role != snapshot.role {
        anyhow::bail!(
            "database placement alias {} changed while readiness was being verified; retry activation",
            alias
        );
    }

    let references = select_database_placement_references(&mut tx, alias).await?;
    if references.route_count + references.creation_count + references.rebalance_count > 0 {
        anyhow::bail!(
            "database placement alias {} gained control-plane references while provisioning; remove them before activation",
            alias
        );
    }

    let placement = database_placements::update_database_placement_status(
        &mut tx,
        alias,
        DATABASE_PLACEMENT_STATUS_ACTIVE,
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("database placement alias {} was not found", alias))?;
    tx.commit().await?;

    Ok(DatabasePlacementActivationOutcome {
        placement,
        changed: true,
        readiness_verified: true,
        migration_count: Some(migration_count),
    })
}

fn require_provisioning_shard_placement(placement: &DatabasePlacement) -> anyhow::Result<()> {
    if !placement.is_shard_capable() {
        anyhow::bail!(
            "database placement alias {} has role {}, which cannot store shard ownership",
            placement.alias,
            placement.role
        );
    }
    if placement.status != DATABASE_PLACEMENT_STATUS_PROVISIONING {
        anyhow::bail!(
            "database placement alias {} has status {} and cannot be activated",
            placement.alias,
            placement.status
        );
    }
    Ok(())
}

async fn verify_database_placement_readiness(
    database_router: &database::DatabaseRouter,
    placement: &DatabasePlacement,
) -> anyhow::Result<usize> {
    let control_db = database_router.control().await?;
    let target_db = database_router
        .provisioning_database(&placement.alias)
        .await?;
    let target_identity = select_database_identity(&target_db).await?;

    for existing in database_placements::list_serviceable_database_placements(control_db).await? {
        let existing_db = database_router.placement(&existing.alias).await?;
        if select_database_identity(&existing_db).await? == target_identity {
            anyhow::bail!(
                "database placement alias {} resolves to the same PostgreSQL database as serviceable placement {}",
                placement.alias,
                existing.alias
            );
        }
    }

    let control_migrations = select_migration_fingerprints(control_db).await?;
    let target_migrations = select_migration_fingerprints(&target_db).await?;
    if target_migrations != control_migrations {
        anyhow::bail!(
            "database placement alias {} schema migrations do not match the control database",
            placement.alias
        );
    }

    let has_admissions = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM local_shard_admissions LIMIT 1)",
    )
    .fetch_one(&target_db)
    .await?;
    if has_admissions {
        anyhow::bail!(
            "database placement alias {} already contains local shard ownership",
            placement.alias
        );
    }

    verify_target_writable(&target_db, &placement.alias).await?;
    Ok(target_migrations.len())
}

async fn select_migration_fingerprints(db: &PgPool) -> anyhow::Result<Vec<MigrationFingerprint>> {
    sqlx::query_as(
        r#"
        SELECT version, description, checksum
        FROM _sqlx_migrations
        WHERE success
        ORDER BY version
        "#,
    )
    .fetch_all(db)
    .await
    .map_err(Into::into)
}

async fn verify_target_writable(db: &PgPool, alias: &str) -> anyhow::Result<()> {
    let mut tx = db.begin().await?;
    sqlx::query(
        r#"
        INSERT INTO local_shard_admissions (
            run_id,
            run_shard,
            database_alias,
            write_epoch,
            state
        )
        VALUES ($1, 0, $2, 1, 'closed')
        "#,
    )
    .bind(Uuid::now_v7())
    .bind(alias)
    .execute(&mut *tx)
    .await?;
    tx.rollback().await?;
    Ok(())
}

pub(crate) async fn disable_database_placement(
    database_router: &database::DatabaseRouter,
    alias: &str,
) -> anyhow::Result<DatabasePlacement> {
    validate_non_empty(alias, "database alias")?;

    let db = database_router.control().await?;
    let mut tx = db.begin().await?;
    let Some(existing) =
        database_placements::select_database_placement_for_update(&mut tx, alias).await?
    else {
        anyhow::bail!("database placement alias {} was not found", alias);
    };

    if existing.is_control_capable() && existing.status != DATABASE_PLACEMENT_STATUS_DISABLED {
        anyhow::bail!(
            "database placement alias {} is control-capable and {}; disabling it would remove the control plane",
            alias,
            existing.status
        );
    }

    if existing.status == DATABASE_PLACEMENT_STATUS_DISABLED {
        tx.commit().await?;
        return Ok(existing);
    }

    if existing.status != DATABASE_PLACEMENT_STATUS_DRAINING {
        anyhow::bail!(
            "database placement alias {} has status {}; it must be draining before it can be disabled",
            alias,
            existing.status
        );
    }

    reject_inflight_move_target(&mut *tx, alias).await?;

    let references = select_database_placement_references(&mut tx, alias).await?;
    if references.route_count > 0 {
        anyhow::bail!(
            "database placement alias {} still owns {} shard route(s); move every route before disabling it",
            alias,
            references.route_count
        );
    }

    if references.creation_count > 0 {
        anyhow::bail!(
            "database placement alias {} is referenced by {} creating run placement(s)",
            alias,
            references.creation_count
        );
    }

    if references.rebalance_count > 0 {
        anyhow::bail!(
            "database placement alias {} is referenced by {} unfinished rebalance item(s)",
            alias,
            references.rebalance_count
        );
    }

    let placement_db = database_router.execution_database(alias).await?;
    let pending_outbox_count =
        outbox_events::count_pending_outbox_deliveries(&placement_db).await?;
    if pending_outbox_count > 0 {
        anyhow::bail!(
            "database placement alias {} still has {} pending outbox delivery row(s)",
            alias,
            pending_outbox_count
        );
    }

    let placement = database_placements::update_database_placement_status(
        &mut tx,
        alias,
        DATABASE_PLACEMENT_STATUS_DISABLED,
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("database placement alias {} was not found", alias))?;
    tx.commit().await?;
    Ok(placement)
}

pub(crate) async fn drain_database_placement(
    database_router: &database::DatabaseRouter,
    alias: &str,
) -> anyhow::Result<DatabasePlacement> {
    validate_non_empty(alias, "database alias")?;

    let db = database_router.control().await?;
    let mut tx = db.begin().await?;
    let Some(existing) =
        database_placements::select_database_placement_for_update(&mut tx, alias).await?
    else {
        anyhow::bail!("database placement alias {} was not found", alias);
    };

    if existing.is_control_capable() {
        anyhow::bail!(
            "database placement alias {} is control-capable; draining it would remove active control-plane admission",
            alias
        );
    }

    if existing.status == DATABASE_PLACEMENT_STATUS_DISABLED {
        anyhow::bail!(
            "database placement alias {} is disabled and cannot transition back to draining",
            alias
        );
    }

    reject_inflight_move_target(&mut *tx, alias).await?;

    if existing.status == DATABASE_PLACEMENT_STATUS_DRAINING {
        tx.commit().await?;
        return Ok(existing);
    }

    if existing.status != DATABASE_PLACEMENT_STATUS_ACTIVE {
        anyhow::bail!(
            "database placement alias {} has status {}; it must be active before it can be drained",
            alias,
            existing.status
        );
    }

    let placement = database_placements::update_database_placement_status(
        &mut tx,
        alias,
        DATABASE_PLACEMENT_STATUS_DRAINING,
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("database placement alias {} was not found", alias))?;
    tx.commit().await?;
    Ok(placement)
}

async fn reject_inflight_move_target<'e, E>(executor: E, database_alias: &str) -> anyhow::Result<()>
where
    E: Executor<'e, Database = Postgres>,
{
    let move_count =
        shard_placements::count_inflight_moves_to_database(executor, database_alias).await?;
    if move_count > 0 {
        anyhow::bail!(
            "database placement alias {} is the target of {} in-flight shard move(s); complete or abort those moves first",
            database_alias,
            move_count
        );
    }

    Ok(())
}

pub(crate) async fn list_shard_placements(
    db: &PgPool,
    run_id: Uuid,
) -> anyhow::Result<Vec<ShardPlacement>> {
    shard_placements::list_shard_placements_for_run(db, run_id).await
}

pub(crate) async fn select_shard_placement(
    db: &PgPool,
    run_id: Uuid,
    run_shard: i16,
) -> anyhow::Result<Option<ShardPlacement>> {
    validate_run_shard(run_shard)?;
    shard_placements::select_shard_placement(db, run_id, run_shard).await
}

/// Inspects the persisted route for one run shard.
///
/// This reads only control-database metadata. It reports whether the current
/// process can resolve the placement URL env var, but never returns the URL
/// value.
pub(crate) async fn inspect_shard_route(
    database_router: &database::DatabaseRouter,
    run_id: Uuid,
    run_shard: i16,
) -> anyhow::Result<ShardRouteInspection> {
    validate_run_shard(run_shard)?;

    let control_db = database_router.control().await?;
    let placement = shard_placements::select_shard_placement(control_db, run_id, run_shard)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "shard placement for run {} shard {} was not found",
                run_id,
                run_shard
            )
        })?;
    let database_placement =
        database_placements::select_database_placement(control_db, &placement.database_alias)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "database placement alias {} was not found",
                    placement.database_alias
                )
            })?;

    let database_serviceable = database_placement.can_serve_owned_shards();
    let shard_capable = database_placement.is_shard_capable();
    let dispatchable = placement.is_dispatchable() && database_serviceable && shard_capable;
    let readable = database_serviceable && shard_capable;
    let routing_decision = if dispatchable {
        "dispatchable"
    } else if readable {
        "read_only"
    } else {
        "blocked"
    };
    let move_progress = select_shard_move_inspection(control_db, run_id, run_shard).await?;

    Ok(ShardRouteInspection {
        run_id,
        run_shard,
        database_alias: placement.database_alias,
        shard_placement_status: placement.status,
        move_target_database_alias: placement.move_target_database_alias,
        route_version: placement.route_version,
        database_role: database_placement.role,
        database_status: database_placement.status,
        database_url_env_resolved: database_router
            .database_url_env_is_resolved(&database_placement.database_url_env),
        database_url_env: database_placement.database_url_env,
        dispatchable,
        readable,
        routing_decision,
        move_operation_id: move_progress.as_ref().map(|progress| progress.id),
        move_phase: move_progress
            .as_ref()
            .map(|progress| progress.phase.clone()),
        move_completed_page_count: move_progress
            .as_ref()
            .map(|progress| progress.completed_page_count),
        move_copied_row_count: move_progress
            .as_ref()
            .map(|progress| progress.copied_row_count),
        move_copied_byte_count: move_progress.map(|progress| progress.copied_byte_count),
    })
}

pub(crate) async fn set_shard_placement(
    database_router: &database::DatabaseRouter,
    run_id: Uuid,
    run_shard: i16,
    database_alias: &str,
) -> anyhow::Result<ShardPlacementSetOutcome> {
    validate_run_shard(run_shard)?;
    validate_non_empty(database_alias, "database alias")?;

    let control_db = database_router.control().await?;
    ensure_run_creation_is_inactive(control_db, run_id).await?;
    let existing = shard_placements::select_shard_placement(control_db, run_id, run_shard).await?;
    if let Some(existing) = &existing
        && existing.status != SHARD_PLACEMENT_STATUS_ACTIVE
    {
        anyhow::bail!(
            "run {} shard {} has placement status {}; only the shard move workflow may recover or change a non-active route",
            run_id,
            run_shard,
            existing.status
        );
    }

    let previous_database_alias = existing
        .as_ref()
        .map(|placement| placement.database_alias.clone());
    let changed = previous_database_alias
        .as_deref()
        .is_none_or(|previous| previous != database_alias);
    let placement = match &existing {
        Some(existing) if existing.database_alias == database_alias => {
            validate_existing_ownership(control_db, database_alias).await?;
            existing.clone()
        }
        Some(existing) => {
            change_empty_shard_placement(database_router, control_db, existing, database_alias)
                .await?
        }
        None => {
            let mut control_tx = control_db.begin().await?;
            validate_new_ownership_target(&mut control_tx, database_alias).await?;
            let placement = shard_placements::insert_active_shard_placement(
                &mut *control_tx,
                run_id,
                run_shard,
                database_alias,
            )
            .await?;
            control_tx.commit().await?;
            placement
        }
    };
    database_router
        .invalidate_execution_placement(run_id, run_shard)
        .await;

    Ok(ShardPlacementSetOutcome {
        placement,
        previous_database_alias,
        changed,
    })
}

async fn change_empty_shard_placement(
    database_router: &database::DatabaseRouter,
    control_db: &PgPool,
    expected: &ShardPlacement,
    target_database_alias: &str,
) -> anyhow::Result<ShardPlacement> {
    let source_db = database_router
        .execution_database(&expected.database_alias)
        .await?;
    let target_db = database_router
        .execution_target_database(target_database_alias)
        .await?;
    let source_is_control = expected.database_alias == database_router.control_database_alias();
    let mut source_tx = source_db.begin().await?;
    crate::db::shard_write_fence::lock_exclusive(
        &mut source_tx,
        expected.run_id,
        expected.run_shard,
    )
    .await?;

    let current = if source_is_control {
        shard_placements::select_shard_placement_with(
            &mut *source_tx,
            expected.run_id,
            expected.run_shard,
        )
        .await?
    } else {
        shard_placements::select_shard_placement(control_db, expected.run_id, expected.run_shard)
            .await?
    };
    if current
        .as_ref()
        .is_none_or(|current| !expected.same_route_fence(current))
    {
        anyhow::bail!(
            "run {} shard {} route changed while its empty placement was being updated",
            expected.run_id,
            expected.run_shard
        );
    }

    let row_count =
        count_shard_owned_rows_with(&mut *source_tx, expected.run_id, expected.run_shard).await?;
    if row_count > 0 {
        anyhow::bail!(
            "run {} shard {} already has {} shard-owned row(s) on {}; use the shard move workflow to change its database placement",
            expected.run_id,
            expected.run_shard,
            row_count,
            expected.database_alias
        );
    }

    let next_write_epoch = expected.write_epoch + 1;
    transition_local_shard_admission(
        &mut *source_tx,
        LocalShardAdmissionDraft {
            run_id: expected.run_id,
            run_shard: expected.run_shard,
            database_alias: expected.database_alias.clone(),
            write_epoch: next_write_epoch,
            state: LocalShardAdmissionState::Closed,
            redirect_database_alias: Some(target_database_alias.to_string()),
            move_fence: None,
        },
        &[LocalShardAdmissionState::Closed],
    )
    .await?;

    let placement = if source_is_control {
        validate_new_ownership_target(&mut source_tx, target_database_alias).await?;
        let placement = shard_placements::change_empty_active_shard_placement(
            &mut *source_tx,
            expected.run_id,
            expected.run_shard,
            &expected.database_alias,
            expected.route_version,
            target_database_alias,
        )
        .await?;
        source_tx.commit().await?;
        placement
    } else {
        source_tx.commit().await?;
        let mut control_tx = control_db.begin().await?;
        validate_new_ownership_target(&mut control_tx, target_database_alias).await?;
        let placement = shard_placements::change_empty_active_shard_placement(
            &mut *control_tx,
            expected.run_id,
            expected.run_shard,
            &expected.database_alias,
            expected.route_version,
            target_database_alias,
        )
        .await?;
        control_tx.commit().await?;
        placement
    }
    .ok_or_else(|| {
        anyhow::anyhow!(
            "run {} shard {} route changed while its empty placement was being updated",
            expected.run_id,
            expected.run_shard
        )
    })?;
    transition_local_admission_with_fence(
        &target_db,
        LocalShardAdmissionDraft {
            run_id: expected.run_id,
            run_shard: expected.run_shard,
            database_alias: target_database_alias.to_string(),
            write_epoch: placement.write_epoch,
            state: LocalShardAdmissionState::Open,
            redirect_database_alias: None,
            move_fence: None,
        },
        &[
            LocalShardAdmissionState::Prepared,
            LocalShardAdmissionState::Closed,
            LocalShardAdmissionState::Open,
        ],
    )
    .await?;
    Ok(placement)
}

/// Creates a persisted rebalance plan or returns the same plan without writing.
///
/// The plan records only intended moves. Applying the plan still uses
/// `move_shard_placement` for each item, so copy, verification, and route
/// fencing remain centralized in the single-shard move workflow.
pub(crate) async fn plan_shard_rebalance(
    database_router: &database::DatabaseRouter,
    options: ShardRebalancePlanOptions,
) -> anyhow::Result<ShardRebalancePlanOutcome> {
    validate_non_empty(&options.target_database_alias, "target database alias")?;
    if options.max_items == 0 {
        anyhow::bail!("max_items must be greater than zero");
    }

    let control_db = database_router.control().await?;
    validate_target_placement(control_db, &options.target_database_alias).await?;
    if let Some(source_alias) = &options.source_database_alias {
        validate_source_placement(control_db, source_alias).await?;
        if source_alias == &options.target_database_alias {
            anyhow::bail!("source and target database aliases must differ");
        }
    }

    let active_aliases = database_router
        .active_shard_capable_database_aliases()
        .await?
        .into_iter()
        .filter(|alias| options.source_database_alias.as_ref() != Some(alias))
        .collect::<Vec<_>>();
    let candidates = select_rebalance_candidates(
        list_active_rebalance_candidates(control_db).await?,
        &active_aliases,
        options.source_database_alias.as_deref(),
        &options.target_database_alias,
        options.max_items,
    );
    let items = candidates
        .into_iter()
        .map(|candidate| PlannedShardRebalanceItem {
            run_id: candidate.run_id,
            run_shard: candidate.run_shard,
            source_database_alias: candidate.source_database_alias,
            target_database_alias: options.target_database_alias.clone(),
            planned_route_version: candidate.route_version,
        })
        .collect::<Vec<_>>();

    if options.dry_run {
        return Ok(ShardRebalancePlanOutcome {
            operation: None,
            items,
            dry_run: true,
        });
    }

    let strategy = if options.source_database_alias.is_some() {
        REBALANCE_STRATEGY_DRAIN_SOURCE
    } else {
        REBALANCE_STRATEGY_FILL_TARGET
    };
    let operation = insert_rebalance_operation(
        control_db,
        strategy,
        options.source_database_alias.as_deref(),
        &options.target_database_alias,
        &items,
    )
    .await?;

    Ok(ShardRebalancePlanOutcome {
        operation: Some(operation),
        items,
        dry_run: false,
    })
}

/// Applies up to `max_items` leased moves from a persisted rebalance plan.
///
/// Each move is claimed immediately before it starts. Re-running apply resumes
/// pending items and running items whose prior claim expired. An item whose
/// shard is still draining is released back to pending. All settlement writes
/// are fenced by the opaque token returned with the claim.
pub(crate) async fn apply_shard_rebalance(
    database_router: &database::DatabaseRouter,
    operation_id: Uuid,
    options: ShardRebalanceApplyOptions,
) -> anyhow::Result<ShardRebalanceApplyOutcome> {
    if options.max_items == 0 {
        anyhow::bail!("max_items must be greater than zero");
    }
    if options.lease_seconds <= 0 {
        anyhow::bail!("lease_seconds must be greater than zero");
    }

    let control_db = database_router.control().await?;
    let operation = select_rebalance_operation(control_db, operation_id)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!("shard rebalance operation {} was not found", operation_id)
        })?;

    if matches!(
        operation.status.as_str(),
        REBALANCE_OPERATION_STATUS_COMPLETED
            | REBALANCE_OPERATION_STATUS_CANCELLED
            | REBALANCE_OPERATION_STATUS_FAILED
    ) {
        anyhow::bail!(
            "shard rebalance operation {} has terminal status {}",
            operation_id,
            operation.status
        );
    }

    let claimed_by = Uuid::now_v7();
    let mut processed_items = Vec::with_capacity(options.max_items);

    for _ in 0..options.max_items {
        let Some(claim) = claim_next_rebalance_apply_item(
            control_db,
            operation_id,
            claimed_by,
            options.lease_seconds,
        )
        .await?
        else {
            break;
        };
        let item = claim.item;
        let claim_token = item.claim_token.ok_or_else(|| {
            anyhow::anyhow!(
                "claimed rebalance item {} shard {} did not return a claim token",
                item.run_id,
                item.run_shard
            )
        })?;

        if claim.reclaimed {
            info!(
                operation_id = %operation_id,
                sequence_no = item.sequence_no,
                run_id = %item.run_id,
                run_shard = item.run_shard,
                claim_token = %claim_token,
                claimed_by = %claimed_by,
                claimed_until = ?item.claimed_until,
                "re-claimed expired shard rebalance apply item"
            );
        } else {
            info!(
                operation_id = %operation_id,
                sequence_no = item.sequence_no,
                run_id = %item.run_id,
                run_shard = item.run_shard,
                claim_token = %claim_token,
                claimed_by = %claimed_by,
                claimed_until = ?item.claimed_until,
                "claimed shard rebalance apply item"
            );
        }

        if rebalance_operation_is_cancelled(control_db, operation_id).await? {
            let settled = mark_rebalance_item_cancelled(control_db, &item, claim_token).await?;
            record_rebalance_item_settlement(
                &mut processed_items,
                settled,
                &item,
                claim_token,
                claimed_by,
                REBALANCE_ITEM_STATUS_CANCELLED,
            );
            continue;
        }

        let result = apply_rebalance_item(database_router, &item, options.force).await;
        match result {
            Ok(()) => {
                let settled = mark_rebalance_item_completed(control_db, &item, claim_token).await?;
                record_rebalance_item_settlement(
                    &mut processed_items,
                    settled,
                    &item,
                    claim_token,
                    claimed_by,
                    REBALANCE_ITEM_STATUS_COMPLETED,
                );
            }
            Err(error) => {
                if let Some(catch_up) = error.downcast_ref::<ShardMoveCatchUpPending>() {
                    let settled =
                        defer_rebalance_item(control_db, &item, claim_token, &error.to_string())
                            .await?;
                    if settled.is_some() {
                        info!(
                            operation_id = %operation_id,
                            sequence_no = item.sequence_no,
                            run_id = %catch_up.run_id,
                            run_shard = catch_up.run_shard,
                            remaining_dirty_key_count = catch_up.remaining_dirty_key_count,
                            claim_token = %claim_token,
                            claimed_by = %claimed_by,
                            "deferred shard rebalance item for another online catch-up cycle"
                        );
                    }
                    record_rebalance_item_settlement(
                        &mut processed_items,
                        settled,
                        &item,
                        claim_token,
                        claimed_by,
                        REBALANCE_ITEM_STATUS_PENDING,
                    );
                    break;
                }
                if let Some(drain) = error.downcast_ref::<ShardMoveDrainPending>() {
                    let settled =
                        defer_rebalance_item(control_db, &item, claim_token, &error.to_string())
                            .await?;
                    if settled.is_some() {
                        info!(
                            operation_id = %operation_id,
                            sequence_no = item.sequence_no,
                            run_id = %drain.run_id,
                            run_shard = drain.run_shard,
                            active_work_count = drain.active_work_count,
                            claim_token = %claim_token,
                            claimed_by = %claimed_by,
                            "deferred shard rebalance item until active work drains"
                        );
                    }
                    record_rebalance_item_settlement(
                        &mut processed_items,
                        settled,
                        &item,
                        claim_token,
                        claimed_by,
                        REBALANCE_ITEM_STATUS_PENDING,
                    );
                    break;
                }
                if let Some(stale) = error.downcast_ref::<StaleRebalancePlanError>() {
                    warn!(
                        operation_id = %operation_id,
                        sequence_no = item.sequence_no,
                        run_id = %item.run_id,
                        run_shard = item.run_shard,
                        claim_token = %claim_token,
                        claimed_by = %claimed_by,
                        expected_database_alias = %stale.expected_alias,
                        expected_route_version = stale.expected_version,
                        actual_database_alias = %stale.actual_alias,
                        actual_route_status = %stale.actual_status,
                        actual_route_version = stale.actual_version,
                        "shard rebalance apply item failed stale-plan check"
                    );
                }
                let settled =
                    mark_rebalance_item_failed(control_db, &item, claim_token, &error.to_string())
                        .await?;
                record_rebalance_item_settlement(
                    &mut processed_items,
                    settled,
                    &item,
                    claim_token,
                    claimed_by,
                    REBALANCE_ITEM_STATUS_FAILED,
                );
            }
        }
    }

    let operation = refresh_rebalance_operation_status(control_db, operation_id).await?;

    Ok(ShardRebalanceApplyOutcome {
        operation,
        processed_items,
    })
}

pub(crate) async fn verify_shard_rebalance(
    database_router: &database::DatabaseRouter,
    operation_id: Uuid,
    max_items: usize,
) -> anyhow::Result<ShardRebalanceVerifyOutcome> {
    if max_items == 0 {
        anyhow::bail!("max_items must be greater than zero");
    }

    let control_db = database_router.control().await?;
    let operation = select_rebalance_operation(control_db, operation_id)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!("shard rebalance operation {} was not found", operation_id)
        })?;
    let items = list_rebalance_items_by_status(
        control_db,
        operation_id,
        REBALANCE_ITEM_STATUS_COMPLETED,
        max_items,
    )
    .await?;
    let mut verified_items = Vec::with_capacity(items.len());

    for item in items {
        let source_db = database_router
            .execution_database(&item.source_database_alias)
            .await?;
        let target_db = database_router
            .execution_database(&item.target_database_alias)
            .await?;
        let reports = verify_move_tables(
            &source_db,
            &target_db,
            item.run_id,
            item.run_shard,
            &BTreeMap::new(),
        )
        .await?;
        let verified = reports.iter().all(|report| report.verified);
        verified_items.push(ShardRebalanceVerifyItem {
            item,
            verified,
            tables: reports,
        });
    }

    Ok(ShardRebalanceVerifyOutcome {
        operation,
        items: verified_items,
    })
}

pub(crate) async fn cancel_shard_rebalance(
    database_router: &database::DatabaseRouter,
    operation_id: Uuid,
) -> anyhow::Result<ShardRebalanceOperation> {
    let control_db = database_router.control().await?;
    let mut tx = control_db.begin().await?;
    let operation = select_rebalance_operation_for_update(&mut tx, operation_id)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!("shard rebalance operation {} was not found", operation_id)
        })?;

    if matches!(
        operation.status.as_str(),
        REBALANCE_OPERATION_STATUS_COMPLETED | REBALANCE_OPERATION_STATUS_FAILED
    ) {
        tx.commit().await?;
        return Ok(operation);
    }

    cancel_rebalance_operation(&mut tx, operation_id).await?;

    tx.commit().await?;
    refresh_rebalance_operation_status(control_db, operation_id).await
}

/// Backfills, drains, and moves one shard under fenced write admission.
///
/// `copying` stays dispatchable while durable pages and captured mutations are
/// replayed. `draining` rejects new work while admitted leases finish.
/// `moving` holds the exclusive source fence only for final replay and
/// activation.
/// The persisted operation, monotonic target claimant fence, checkpoints, and
/// route versions make retries resumable and reject stale target writers.
pub(crate) async fn move_shard_placement(
    database_router: &database::DatabaseRouter,
    run_id: Uuid,
    run_shard: i16,
    target_database_alias: &str,
    options: ShardMoveOptions,
) -> anyhow::Result<ShardMoveOutcome> {
    validate_run_shard(run_shard)?;
    validate_non_empty(target_database_alias, "target database alias")?;

    if options.dry_run && options.verify_only {
        anyhow::bail!("--dry-run and --verify-only cannot be used together");
    }

    let control_db = database_router.control().await?;
    ensure_run_creation_is_inactive(control_db, run_id).await?;
    let current = shard_placements::select_shard_placement(control_db, run_id, run_shard)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "shard placement for run {} shard {} was not found",
                run_id,
                run_shard
            )
        })?;

    let completed_operation = if !options.dry_run
        && !options.verify_only
        && current.database_alias == target_database_alias
        && current.status == SHARD_PLACEMENT_STATUS_ACTIVE
    {
        select_latest_move_to_target(control_db, run_id, run_shard, target_database_alias).await?
    } else {
        None
    };
    let retrying_completed_move = completed_operation.is_some();

    if options.verify_only {
        validate_source_placement(control_db, target_database_alias).await?;
    } else if !retrying_completed_move {
        validate_target_placement(control_db, target_database_alias).await?;
    }

    if current.database_alias == target_database_alias
        && !options.verify_only
        && !retrying_completed_move
    {
        anyhow::bail!(
            "run {} shard {} already routes to database placement {}",
            run_id,
            run_shard,
            target_database_alias
        );
    }

    let source_db = database_router
        .execution_database(&current.database_alias)
        .await?;
    let target_db = if options.verify_only || retrying_completed_move {
        database_router
            .execution_database(target_database_alias)
            .await?
    } else {
        database_router
            .execution_target_database(target_database_alias)
            .await?
    };
    let copied_rows_by_table = std::collections::BTreeMap::<&'static str, u64>::new();

    if options.dry_run || options.verify_only {
        let active_work_count = count_active_shard_work(&source_db, run_id, run_shard).await?;
        let reports = verify_move_tables(
            &source_db,
            &target_db,
            run_id,
            run_shard,
            &copied_rows_by_table,
        )
        .await?;

        return Ok(ShardMoveOutcome {
            run_id,
            run_shard,
            source_database_alias: current.database_alias.clone(),
            target_database_alias: target_database_alias.to_string(),
            dry_run: options.dry_run,
            verify_only: options.verify_only,
            forced: options.force,
            active_work_count,
            moved: false,
            placement: current,
            tables: reports,
        });
    }

    if retrying_completed_move {
        let operation = completed_operation.expect("completed move retry has an operation");
        let old_source = database_router
            .execution_database(&operation.source_database_alias)
            .await?;
        let mut identity_tx = old_source.begin().await?;
        let same_database = databases_share_identity(&mut identity_tx, &target_db).await?;
        identity_tx.commit().await?;
        if !same_database {
            transition_local_admission_with_fence(
                &old_source,
                LocalShardAdmissionDraft {
                    run_id,
                    run_shard,
                    database_alias: operation.source_database_alias.clone(),
                    write_epoch: current.write_epoch,
                    state: LocalShardAdmissionState::Closed,
                    redirect_database_alias: Some(target_database_alias.to_string()),
                    move_fence: None,
                },
                &[LocalShardAdmissionState::Closed],
            )
            .await?;
        }
        transition_local_admission_with_fence(
            &target_db,
            LocalShardAdmissionDraft {
                run_id,
                run_shard,
                database_alias: target_database_alias.to_string(),
                write_epoch: current.write_epoch,
                state: LocalShardAdmissionState::Open,
                redirect_database_alias: None,
                move_fence: None,
            },
            &[
                LocalShardAdmissionState::Prepared,
                LocalShardAdmissionState::Closed,
                LocalShardAdmissionState::Open,
            ],
        )
        .await?;
        if operation.status == "active" {
            let mut cleanup_tx = old_source.begin().await?;
            crate::db::shard_write_fence::lock_exclusive(&mut cleanup_tx, run_id, run_shard)
                .await?;
            delete_move_capture(&mut cleanup_tx, operation.id).await?;
            cleanup_tx.commit().await?;
            settle_completed_move_operation(control_db, operation.id, None).await?;
        }
        let reports = checkpoint_move_reports(control_db, operation.id).await?;
        let active_work_count = count_active_shard_work(&source_db, run_id, run_shard).await?;
        return Ok(ShardMoveOutcome {
            run_id,
            run_shard,
            source_database_alias: operation.source_database_alias,
            target_database_alias: target_database_alias.to_string(),
            dry_run: false,
            verify_only: false,
            forced: options.force,
            active_work_count,
            moved: false,
            placement: current,
            tables: reports,
        });
    }

    if !matches!(
        current.status.as_str(),
        SHARD_PLACEMENT_STATUS_ACTIVE
            | SHARD_PLACEMENT_STATUS_COPYING
            | SHARD_PLACEMENT_STATUS_DRAINING
            | SHARD_PLACEMENT_STATUS_MOVING
    ) {
        anyhow::bail!(
            "run {} shard {} has placement status {}, which cannot be moved",
            run_id,
            run_shard,
            current.status
        );
    }
    if current.status != SHARD_PLACEMENT_STATUS_ACTIVE
        && current.move_target_database_alias.as_deref() != Some(target_database_alias)
    {
        anyhow::bail!(
            "run {} shard {} is already {} toward database placement {}; retry with the persisted target",
            run_id,
            run_shard,
            current.status,
            current
                .move_target_database_alias
                .as_deref()
                .unwrap_or("<missing>")
        );
    }

    let claim_token = Uuid::now_v7();
    let operation =
        claim_shard_move_operation(control_db, &current, target_database_alias, claim_token)
            .await?;
    let prepared = match prepare_online_shard_move(
        database_router,
        &source_db,
        &target_db,
        current,
        &operation,
        claim_token,
    )
    .await
    {
        Ok(prepared) => prepared,
        Err(error) => {
            release_shard_move_claim(control_db, operation.id, claim_token).await?;
            return Err(error);
        }
    };
    let current = prepared.route;
    let initial_route = current.clone();
    let source_database_alias = current.database_alias.clone();
    let source_is_control = source_database_alias == database_router.control_database_alias();
    let active_work_count = count_active_shard_work(&source_db, run_id, run_shard).await?;
    if active_work_count > 0 {
        release_shard_move_claim(control_db, operation.id, claim_token).await?;
        return Err(ShardMoveDrainPending {
            run_id,
            run_shard,
            active_work_count,
        }
        .into());
    }
    let mut source_tx = source_db.begin().await?;
    crate::db::shard_write_fence::lock_exclusive(&mut source_tx, run_id, run_shard).await?;

    // Re-read after acquiring the barrier. Another mover may have completed
    // while this caller waited for previously admitted writes to finish.
    let current = if source_is_control {
        shard_placements::select_shard_placement_with(&mut *source_tx, run_id, run_shard).await?
    } else {
        shard_placements::select_shard_placement(control_db, run_id, run_shard).await?
    }
    .ok_or_else(|| {
        anyhow::anyhow!(
            "shard placement for run {} shard {} was not found",
            run_id,
            run_shard
        )
    })?;

    if current.database_alias == target_database_alias
        && current.status == SHARD_PLACEMENT_STATUS_ACTIVE
    {
        let active_work_count =
            count_active_shard_work_with(&mut *source_tx, run_id, run_shard).await?;
        let reports = verify_move_tables_with_source(
            &mut source_tx,
            &target_db,
            run_id,
            run_shard,
            &copied_rows_by_table,
        )
        .await?;
        if !reports.iter().all(|report| report.verified) {
            anyhow::bail!(
                "run {} shard {} reached target {} while the completed move could not be verified",
                run_id,
                run_shard,
                target_database_alias
            );
        }
        source_tx.commit().await?;

        return Ok(ShardMoveOutcome {
            run_id,
            run_shard,
            source_database_alias,
            target_database_alias: target_database_alias.to_string(),
            dry_run: false,
            verify_only: false,
            forced: options.force,
            active_work_count,
            moved: false,
            placement: current,
            tables: reports,
        });
    }

    if !initial_route.same_route_fence(&current) {
        anyhow::bail!(
            "run {} shard {} route changed while waiting for move admission; expected {} status {} version {}, found {} status {} version {}",
            run_id,
            run_shard,
            initial_route.database_alias,
            initial_route.status,
            initial_route.route_version,
            current.database_alias,
            current.status,
            current.route_version
        );
    }

    if current.database_alias != source_database_alias
        || !matches!(
            current.status.as_str(),
            SHARD_PLACEMENT_STATUS_ACTIVE
                | SHARD_PLACEMENT_STATUS_COPYING
                | SHARD_PLACEMENT_STATUS_DRAINING
                | SHARD_PLACEMENT_STATUS_MOVING
        )
    {
        anyhow::bail!(
            "run {} shard {} route changed while waiting for move admission; found {} status {} version {}",
            run_id,
            run_shard,
            current.database_alias,
            current.status,
            current.route_version
        );
    }

    if current.status != SHARD_PLACEMENT_STATUS_ACTIVE
        && current.move_target_database_alias.as_deref() != Some(target_database_alias)
    {
        anyhow::bail!(
            "run {} shard {} is already {} toward database placement {}; retry with the persisted target",
            run_id,
            run_shard,
            current.status,
            current
                .move_target_database_alias
                .as_deref()
                .unwrap_or("<missing>")
        );
    }

    let draining = current;
    database_router
        .invalidate_execution_placement(run_id, run_shard)
        .await;

    let active_work_count =
        count_active_shard_work_with(&mut *source_tx, run_id, run_shard).await?;
    if active_work_count > 0 {
        source_tx.commit().await?;
        release_shard_move_claim(control_db, operation.id, claim_token).await?;
        return Err(ShardMoveDrainPending {
            run_id,
            run_shard,
            active_work_count,
        }
        .into());
    }

    let moving = if draining.status == SHARD_PLACEMENT_STATUS_DRAINING {
        let placement = if source_is_control {
            shard_placements::mark_shard_placement_moving(
                &mut *source_tx,
                run_id,
                run_shard,
                &source_database_alias,
                draining.route_version,
                target_database_alias,
            )
            .await?
        } else {
            shard_placements::mark_shard_placement_moving(
                control_db,
                run_id,
                run_shard,
                &source_database_alias,
                draining.route_version,
                target_database_alias,
            )
            .await?
        };
        placement.ok_or_else(|| {
            anyhow::anyhow!(
                "run {} shard {} route changed before it could be frozen for copying",
                run_id,
                run_shard
            )
        })?
    } else {
        draining
    };
    database_router
        .invalidate_execution_placement(run_id, run_shard)
        .await;

    if update_claimed_move_phase(control_db, operation.id, claim_token, "cutover").await? != 1 {
        anyhow::bail!("shard move {} lost its claim before cutover", operation.id);
    }
    let remaining_dirty = if let Some(target_fence) = prepared.target_fence.as_ref() {
        replay_dirty_shard_keys(
            &source_db,
            &target_db,
            target_fence,
            operation.id,
            usize::MAX,
        )
        .await?
    } else {
        0
    };
    if remaining_dirty != 0 {
        anyhow::bail!(
            "run {} shard {} final replay retained {} dirty key(s)",
            run_id,
            run_shard,
            remaining_dirty
        );
    }
    let reports = checkpoint_move_reports(control_db, operation.id).await?;

    let next_write_epoch = moving.write_epoch + 1;
    transition_local_shard_admission(
        &mut *source_tx,
        LocalShardAdmissionDraft {
            run_id,
            run_shard,
            database_alias: source_database_alias.clone(),
            write_epoch: next_write_epoch,
            state: LocalShardAdmissionState::Closed,
            redirect_database_alias: Some(target_database_alias.to_string()),
            move_fence: None,
        },
        &[LocalShardAdmissionState::Closed],
    )
    .await?;

    let placement = if source_is_control {
        let placement = activate_moved_shard_placement_on_target_with(
            &mut source_tx,
            run_id,
            run_shard,
            &source_database_alias,
            moving.route_version,
            target_database_alias,
        )
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "run {} shard {} route changed before target activation",
                run_id,
                run_shard
            )
        })?;
        delete_move_capture(&mut source_tx, operation.id).await?;
        source_tx.commit().await?;
        placement
    } else {
        delete_move_capture(&mut source_tx, operation.id).await?;
        source_tx.commit().await?;
        activate_moved_shard_placement_on_target(
            control_db,
            run_id,
            run_shard,
            &source_database_alias,
            moving.route_version,
            target_database_alias,
        )
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "run {} shard {} route changed before target activation",
                run_id,
                run_shard
            )
        })?
    };
    if placement.write_epoch != next_write_epoch {
        anyhow::bail!(
            "run {} shard {} activated write epoch {}, expected {}",
            run_id,
            run_shard,
            placement.write_epoch,
            next_write_epoch
        );
    }
    transition_local_admission_with_fence(
        &target_db,
        LocalShardAdmissionDraft {
            run_id,
            run_shard,
            database_alias: target_database_alias.to_string(),
            write_epoch: placement.write_epoch,
            state: LocalShardAdmissionState::Open,
            redirect_database_alias: None,
            move_fence: None,
        },
        &[
            LocalShardAdmissionState::Prepared,
            LocalShardAdmissionState::Closed,
            LocalShardAdmissionState::Open,
        ],
    )
    .await?;
    settle_completed_move_operation(control_db, operation.id, Some(claim_token)).await?;
    database_router
        .invalidate_execution_placement(run_id, run_shard)
        .await;

    Ok(ShardMoveOutcome {
        run_id,
        run_shard,
        source_database_alias,
        target_database_alias: target_database_alias.to_string(),
        dry_run: false,
        verify_only: false,
        forced: options.force,
        active_work_count,
        moved: true,
        placement,
        tables: reports,
    })
}

/// Restores an in-progress shard move to active ownership on its source.
///
/// Partial target rows remain non-authoritative. The source admission lock and
/// route-version compare-and-swap ensure the abort cannot race a copy or route
/// activation, and stale movers cannot restart after the route fence advances.
pub(crate) async fn abort_shard_move(
    database_router: &database::DatabaseRouter,
    run_id: Uuid,
    run_shard: i16,
    source_database_alias: &str,
    target_database_alias: &str,
) -> anyhow::Result<ShardMoveAbortOutcome> {
    validate_run_shard(run_shard)?;
    validate_non_empty(source_database_alias, "source database alias")?;
    validate_non_empty(target_database_alias, "target database alias")?;
    if source_database_alias == target_database_alias {
        anyhow::bail!("source and target database aliases must differ");
    }

    let control_db = database_router.control().await?;
    ensure_run_creation_is_inactive(control_db, run_id).await?;
    let initial = shard_placements::select_shard_placement(control_db, run_id, run_shard)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "shard placement for run {} shard {} was not found",
                run_id,
                run_shard
            )
        })?;
    let prepared_move_id = select_active_move_id(
        control_db,
        run_id,
        run_shard,
        source_database_alias,
        target_database_alias,
    )
    .await?;
    if prepared_move_id.is_none()
        && let Some(outcome) = completed_abort_outcome(
            &initial,
            run_id,
            run_shard,
            source_database_alias,
            target_database_alias,
        )?
    {
        return Ok(outcome);
    }

    let source_db = database_router
        .execution_database(source_database_alias)
        .await?;
    let target_db = database_router
        .execution_database(target_database_alias)
        .await?;
    let source_is_control = source_database_alias == database_router.control_database_alias();
    let mut source_tx = source_db.begin().await?;
    crate::db::shard_write_fence::lock_exclusive(&mut source_tx, run_id, run_shard).await?;
    let same_database = databases_share_identity(&mut source_tx, &target_db).await?;

    let current = if source_is_control {
        shard_placements::select_shard_placement_with(&mut *source_tx, run_id, run_shard).await?
    } else {
        shard_placements::select_shard_placement(control_db, run_id, run_shard).await?
    }
    .ok_or_else(|| {
        anyhow::anyhow!(
            "shard placement for run {} shard {} was not found",
            run_id,
            run_shard
        )
    })?;
    if prepared_move_id.is_none()
        && let Some(outcome) = completed_abort_outcome(
            &current,
            run_id,
            run_shard,
            source_database_alias,
            target_database_alias,
        )?
    {
        source_tx.commit().await?;
        return Ok(outcome);
    }
    if !initial.same_route_fence(&current) {
        anyhow::bail!(
            "run {} shard {} route changed while waiting for move-abort admission; expected {} status {} version {}, found {} status {} version {}",
            run_id,
            run_shard,
            initial.database_alias,
            initial.status,
            initial.route_version,
            current.database_alias,
            current.status,
            current.route_version
        );
    }
    if current.status == SHARD_PLACEMENT_STATUS_ACTIVE {
        let placement = if source_is_control {
            shard_placements::fence_active_shard_placement(
                &mut *source_tx,
                run_id,
                run_shard,
                source_database_alias,
                current.route_version,
            )
            .await?
        } else {
            shard_placements::fence_active_shard_placement(
                control_db,
                run_id,
                run_shard,
                source_database_alias,
                current.route_version,
            )
            .await?
        }
        .ok_or_else(|| {
            anyhow::anyhow!(
                "run {} shard {} route changed before its prepared move could be aborted",
                run_id,
                run_shard
            )
        })?;
        transition_local_shard_admission(
            &mut *source_tx,
            LocalShardAdmissionDraft {
                run_id,
                run_shard,
                database_alias: source_database_alias.to_string(),
                write_epoch: placement.write_epoch,
                state: LocalShardAdmissionState::Open,
                redirect_database_alias: None,
                move_fence: None,
            },
            &[LocalShardAdmissionState::Open],
        )
        .await?;
        let move_id = prepared_move_id.expect("active abort requires a prepared move");
        delete_move_capture(&mut source_tx, move_id).await?;
        source_tx.commit().await?;
        if !same_database {
            transition_local_admission_with_fence(
                &target_db,
                LocalShardAdmissionDraft {
                    run_id,
                    run_shard,
                    database_alias: target_database_alias.to_string(),
                    write_epoch: placement.write_epoch + 1,
                    state: LocalShardAdmissionState::Closed,
                    redirect_database_alias: Some(source_database_alias.to_string()),
                    move_fence: None,
                },
                &[
                    LocalShardAdmissionState::Prepared,
                    LocalShardAdmissionState::Closed,
                ],
            )
            .await?;
        }
        settle_aborted_move_operation(control_db, move_id).await?;
        database_router
            .invalidate_execution_placement(run_id, run_shard)
            .await;
        return Ok(ShardMoveAbortOutcome {
            run_id,
            run_shard,
            source_database_alias: source_database_alias.to_string(),
            target_database_alias: target_database_alias.to_string(),
            aborted: true,
            placement,
        });
    }

    validate_abort_route(
        &current,
        run_id,
        run_shard,
        source_database_alias,
        target_database_alias,
    )?;

    let placement = if source_is_control {
        shard_placements::abort_shard_placement_move(
            &mut *source_tx,
            run_id,
            run_shard,
            source_database_alias,
            current.route_version,
            target_database_alias,
        )
        .await?
    } else {
        shard_placements::abort_shard_placement_move(
            control_db,
            run_id,
            run_shard,
            source_database_alias,
            current.route_version,
            target_database_alias,
        )
        .await?
    }
    .ok_or_else(|| {
        anyhow::anyhow!(
            "run {} shard {} route changed before its move could be aborted",
            run_id,
            run_shard
        )
    })?;
    transition_local_shard_admission(
        &mut *source_tx,
        LocalShardAdmissionDraft {
            run_id,
            run_shard,
            database_alias: source_database_alias.to_string(),
            write_epoch: placement.write_epoch,
            state: LocalShardAdmissionState::Open,
            redirect_database_alias: None,
            move_fence: None,
        },
        &[
            LocalShardAdmissionState::Open,
            LocalShardAdmissionState::Draining,
            LocalShardAdmissionState::Closed,
        ],
    )
    .await?;
    if let Some(move_id) = prepared_move_id {
        delete_move_capture(&mut source_tx, move_id).await?;
    }
    source_tx.commit().await?;
    if !same_database {
        transition_local_admission_with_fence(
            &target_db,
            LocalShardAdmissionDraft {
                run_id,
                run_shard,
                database_alias: target_database_alias.to_string(),
                write_epoch: current.write_epoch + 1,
                state: LocalShardAdmissionState::Closed,
                redirect_database_alias: Some(source_database_alias.to_string()),
                move_fence: None,
            },
            &[
                LocalShardAdmissionState::Prepared,
                LocalShardAdmissionState::Closed,
            ],
        )
        .await?;
    }
    if let Some(move_id) = prepared_move_id {
        settle_aborted_move_operation(control_db, move_id).await?;
    }
    database_router
        .invalidate_execution_placement(run_id, run_shard)
        .await;

    Ok(ShardMoveAbortOutcome {
        run_id,
        run_shard,
        source_database_alias: source_database_alias.to_string(),
        target_database_alias: target_database_alias.to_string(),
        aborted: true,
        placement,
    })
}

fn completed_abort_outcome(
    placement: &ShardPlacement,
    run_id: Uuid,
    run_shard: i16,
    source_database_alias: &str,
    target_database_alias: &str,
) -> anyhow::Result<Option<ShardMoveAbortOutcome>> {
    if placement.status != SHARD_PLACEMENT_STATUS_ACTIVE {
        return Ok(None);
    }
    if placement.database_alias == target_database_alias {
        anyhow::bail!(
            "run {} shard {} move already completed on database placement {}",
            run_id,
            run_shard,
            target_database_alias
        );
    }
    if placement.database_alias != source_database_alias
        || placement.move_target_database_alias.is_some()
    {
        anyhow::bail!(
            "run {} shard {} has no matching move from {} to {}; found {} status {} version {}",
            run_id,
            run_shard,
            source_database_alias,
            target_database_alias,
            placement.database_alias,
            placement.status,
            placement.route_version
        );
    }

    Ok(Some(ShardMoveAbortOutcome {
        run_id,
        run_shard,
        source_database_alias: source_database_alias.to_string(),
        target_database_alias: target_database_alias.to_string(),
        aborted: false,
        placement: placement.clone(),
    }))
}

fn validate_abort_route(
    placement: &ShardPlacement,
    run_id: Uuid,
    run_shard: i16,
    source_database_alias: &str,
    target_database_alias: &str,
) -> anyhow::Result<()> {
    if placement.database_alias != source_database_alias
        || !matches!(
            placement.status.as_str(),
            SHARD_PLACEMENT_STATUS_COPYING
                | SHARD_PLACEMENT_STATUS_DRAINING
                | SHARD_PLACEMENT_STATUS_MOVING
        )
        || placement.move_target_database_alias.as_deref() != Some(target_database_alias)
    {
        anyhow::bail!(
            "run {} shard {} has no matching move from {} to {}; found {} status {} target {} version {}",
            run_id,
            run_shard,
            source_database_alias,
            target_database_alias,
            placement.database_alias,
            placement.status,
            placement
                .move_target_database_alias
                .as_deref()
                .unwrap_or("<none>"),
            placement.route_version
        );
    }

    Ok(())
}

fn select_rebalance_candidates(
    candidates: Vec<ShardRebalanceCandidate>,
    active_shard_aliases: &[String],
    source_database_alias: Option<&str>,
    target_database_alias: &str,
    max_items: usize,
) -> Vec<ShardRebalanceCandidate> {
    if max_items == 0 {
        return Vec::new();
    }

    let mut counts = active_shard_aliases
        .iter()
        .map(|alias| (alias.as_str(), 0usize))
        .collect::<BTreeMap<_, _>>();
    for candidate in &candidates {
        if let Some(count) = counts.get_mut(candidate.source_database_alias.as_str()) {
            *count += 1;
        }
    }

    let mut candidates = candidates
        .into_iter()
        .filter(|candidate| candidate.source_database_alias != target_database_alias)
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        left.source_database_alias
            .cmp(&right.source_database_alias)
            .then_with(|| left.run_id.cmp(&right.run_id))
            .then_with(|| left.run_shard.cmp(&right.run_shard))
    });

    if let Some(source_alias) = source_database_alias {
        return candidates
            .into_iter()
            .filter(|candidate| candidate.source_database_alias == source_alias)
            .take(max_items)
            .collect();
    }

    let alias_count = active_shard_aliases.len();
    if alias_count < 2 {
        return Vec::new();
    }

    let target_count = counts
        .get(target_database_alias)
        .copied()
        .unwrap_or_default();
    let total = counts.values().sum::<usize>();
    let desired_per_alias = total.div_ceil(alias_count);
    let needed = desired_per_alias.saturating_sub(target_count);
    if needed == 0 {
        return Vec::new();
    }

    candidates
        .into_iter()
        .filter(|candidate| {
            counts
                .get(candidate.source_database_alias.as_str())
                .is_some_and(|count| *count > desired_per_alias)
        })
        .take(max_items.min(needed))
        .collect()
}

#[cfg(test)]
fn rebalance_item_is_claim_eligible(
    status: &str,
    claimed_until: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> bool {
    status == "pending"
        || (status == "running" && claimed_until.is_some_and(|deadline| deadline <= now))
}

async fn mark_rebalance_item_completed(
    db: &PgPool,
    item: &ShardRebalanceItem,
    claim_token: Uuid,
) -> anyhow::Result<Option<ShardRebalanceItem>> {
    mark_rebalance_item_status(db, item, claim_token, REBALANCE_ITEM_STATUS_COMPLETED, None).await
}

async fn mark_rebalance_item_failed(
    db: &PgPool,
    item: &ShardRebalanceItem,
    claim_token: Uuid,
    error_message: &str,
) -> anyhow::Result<Option<ShardRebalanceItem>> {
    mark_rebalance_item_status(
        db,
        item,
        claim_token,
        REBALANCE_ITEM_STATUS_FAILED,
        Some(error_message),
    )
    .await
}

async fn mark_rebalance_item_cancelled(
    db: &PgPool,
    item: &ShardRebalanceItem,
    claim_token: Uuid,
) -> anyhow::Result<Option<ShardRebalanceItem>> {
    mark_rebalance_item_status(db, item, claim_token, REBALANCE_ITEM_STATUS_CANCELLED, None).await
}

fn record_rebalance_item_settlement(
    processed_items: &mut Vec<ShardRebalanceItem>,
    settled: Option<ShardRebalanceItem>,
    claimed_item: &ShardRebalanceItem,
    claim_token: Uuid,
    claimed_by: Uuid,
    status: &str,
) {
    let Some(settled) = settled else {
        warn!(
            operation_id = %claimed_item.operation_id,
            sequence_no = claimed_item.sequence_no,
            run_id = %claimed_item.run_id,
            run_shard = claimed_item.run_shard,
            claim_token = %claim_token,
            claimed_by = %claimed_by,
            attempted_status = status,
            "stale shard rebalance apply worker was fenced from settling item"
        );
        return;
    };

    match status {
        REBALANCE_ITEM_STATUS_COMPLETED => info!(
            operation_id = %settled.operation_id,
            sequence_no = settled.sequence_no,
            run_id = %settled.run_id,
            run_shard = settled.run_shard,
            claim_token = %claim_token,
            claimed_by = %claimed_by,
            "completed shard rebalance apply item"
        ),
        REBALANCE_ITEM_STATUS_FAILED => warn!(
            operation_id = %settled.operation_id,
            sequence_no = settled.sequence_no,
            run_id = %settled.run_id,
            run_shard = settled.run_shard,
            claim_token = %claim_token,
            claimed_by = %claimed_by,
            error = ?settled.error_message,
            "failed shard rebalance apply item"
        ),
        REBALANCE_ITEM_STATUS_CANCELLED => info!(
            operation_id = %settled.operation_id,
            sequence_no = settled.sequence_no,
            run_id = %settled.run_id,
            run_shard = settled.run_shard,
            claim_token = %claim_token,
            claimed_by = %claimed_by,
            "cancelled claimed shard rebalance apply item"
        ),
        _ => {}
    }
    processed_items.push(settled);
}

async fn apply_rebalance_item(
    database_router: &database::DatabaseRouter,
    item: &ShardRebalanceItem,
    force: bool,
) -> anyhow::Result<()> {
    let control_db = database_router.control().await?;
    let current = shard_placements::select_shard_placement(control_db, item.run_id, item.run_shard)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "shard placement for run {} shard {} was not found",
                item.run_id,
                item.run_shard
            )
        })?;

    if current.database_alias == item.target_database_alias
        && current.status == SHARD_PLACEMENT_STATUS_ACTIVE
    {
        return Ok(());
    }

    let planned_route_is_current = current.database_alias == item.source_database_alias
        && current.status == SHARD_PLACEMENT_STATUS_ACTIVE
        && current.route_version == item.planned_route_version;
    let copying_move_is_resumable = current.database_alias == item.source_database_alias
        && current.status == SHARD_PLACEMENT_STATUS_COPYING
        && current.move_target_database_alias.as_deref() == Some(&item.target_database_alias)
        && item.planned_route_version.checked_add(1) == Some(current.route_version);
    let draining_move_is_resumable = current.database_alias == item.source_database_alias
        && current.status == SHARD_PLACEMENT_STATUS_DRAINING
        && current.move_target_database_alias.as_deref() == Some(&item.target_database_alias)
        && item.planned_route_version.checked_add(2) == Some(current.route_version);
    let moving_move_is_resumable = current.database_alias == item.source_database_alias
        && current.status == SHARD_PLACEMENT_STATUS_MOVING
        && current.move_target_database_alias.as_deref() == Some(&item.target_database_alias)
        && item.planned_route_version.checked_add(3) == Some(current.route_version);

    if !planned_route_is_current
        && !copying_move_is_resumable
        && !draining_move_is_resumable
        && !moving_move_is_resumable
    {
        return Err(StaleRebalancePlanError {
            run_id: item.run_id,
            run_shard: item.run_shard,
            expected_alias: item.source_database_alias.clone(),
            expected_version: item.planned_route_version,
            actual_alias: current.database_alias,
            actual_status: current.status,
            actual_version: current.route_version,
        }
        .into());
    }

    move_shard_placement(
        database_router,
        item.run_id,
        item.run_shard,
        &item.target_database_alias,
        ShardMoveOptions {
            dry_run: false,
            verify_only: false,
            force,
        },
    )
    .await?;

    Ok(())
}

async fn validate_target_placement(db: &PgPool, alias: &str) -> anyhow::Result<()> {
    let target = database_placements::select_database_placement(db, alias)
        .await?
        .ok_or_else(|| anyhow::anyhow!("database placement alias {} was not found", alias))?;

    if target.status != DATABASE_PLACEMENT_STATUS_ACTIVE {
        anyhow::bail!(
            "database placement alias {} has status {}, which cannot receive moved shard rows",
            target.alias,
            target.status
        );
    }

    if !target.is_shard_capable() {
        anyhow::bail!(
            "database placement alias {} has role {}, which is not shard-capable",
            target.alias,
            target.role
        );
    }

    Ok(())
}

/// Reserves an active move target while persisting the durable target reference.
///
/// Database drain and disable lock the same placement row `FOR UPDATE` before
/// checking target references. Holding `FOR SHARE` through this route update
/// makes either the move reservation or the lifecycle transition win first.
/// Holds a shared lifecycle lock until the caller commits its ownership write.
///
/// Drain and disable take `FOR UPDATE` on the same row, so either admission
/// commits first or the lifecycle transition wins and admission observes the
/// non-active status.
async fn validate_new_ownership_target(
    tx: &mut Transaction<'_, Postgres>,
    alias: &str,
) -> anyhow::Result<()> {
    let target = database_placements::select_database_placement_for_share(tx, alias)
        .await?
        .ok_or_else(|| anyhow::anyhow!("database placement alias {} was not found", alias))?;

    if !target.accepts_new_shards() {
        anyhow::bail!(
            "database placement alias {} has status {}, which cannot receive new shard ownership",
            target.alias,
            target.status
        );
    }

    if !target.is_shard_capable() {
        anyhow::bail!(
            "database placement alias {} has role {}, which is not shard-capable",
            target.alias,
            target.role
        );
    }

    Ok(())
}

async fn validate_existing_ownership(db: &PgPool, alias: &str) -> anyhow::Result<()> {
    let placement = database_placements::select_database_placement(db, alias)
        .await?
        .ok_or_else(|| anyhow::anyhow!("database placement alias {} was not found", alias))?;

    if !placement.can_serve_owned_shards() {
        anyhow::bail!(
            "database placement alias {} has status {}, which cannot serve existing shard ownership",
            placement.alias,
            placement.status
        );
    }
    if !placement.is_shard_capable() {
        anyhow::bail!(
            "database placement alias {} has role {}, which is not shard-capable",
            placement.alias,
            placement.role
        );
    }

    Ok(())
}

async fn activate_moved_shard_placement_on_target(
    db: &PgPool,
    run_id: Uuid,
    run_shard: i16,
    expected_database_alias: &str,
    expected_route_version: i64,
    target_database_alias: &str,
) -> anyhow::Result<Option<ShardPlacement>> {
    let mut tx = db.begin().await?;
    let placement = activate_moved_shard_placement_on_target_with(
        &mut tx,
        run_id,
        run_shard,
        expected_database_alias,
        expected_route_version,
        target_database_alias,
    )
    .await?;
    tx.commit().await?;
    Ok(placement)
}

async fn activate_moved_shard_placement_on_target_with(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    run_shard: i16,
    expected_database_alias: &str,
    expected_route_version: i64,
    target_database_alias: &str,
) -> anyhow::Result<Option<ShardPlacement>> {
    validate_new_ownership_target(tx, target_database_alias).await?;
    shard_placements::activate_moved_shard_placement(
        &mut **tx,
        run_id,
        run_shard,
        expected_database_alias,
        expected_route_version,
        target_database_alias,
    )
    .await
}

async fn validate_source_placement(db: &PgPool, alias: &str) -> anyhow::Result<()> {
    let source = database_placements::select_database_placement(db, alias)
        .await?
        .ok_or_else(|| anyhow::anyhow!("database placement alias {} was not found", alias))?;

    if !source.is_shard_capable() {
        anyhow::bail!(
            "database placement alias {} has role {}, which is not shard-capable",
            source.alias,
            source.role
        );
    }
    if !source.can_serve_owned_shards() {
        anyhow::bail!(
            "database placement alias {} has status {}, which cannot serve source shard ownership",
            source.alias,
            source.status
        );
    }

    Ok(())
}

const SHARD_MOVE_CLAIM_SECONDS: i32 = 300;

async fn reserve_active_move_target_and_mark_copying(
    db: &PgPool,
    run_id: Uuid,
    run_shard: i16,
    expected_database_alias: &str,
    expected_route_version: i64,
    target_database_alias: &str,
) -> anyhow::Result<Option<ShardPlacement>> {
    let mut tx = db.begin().await?;
    validate_new_ownership_target(&mut tx, target_database_alias).await?;
    let placement = shard_placements::mark_shard_placement_copying(
        &mut *tx,
        run_id,
        run_shard,
        expected_database_alias,
        expected_route_version,
        target_database_alias,
    )
    .await?;
    tx.commit().await?;
    Ok(placement)
}

fn move_table_key_columns(table: &str) -> anyhow::Result<&'static [&'static str]> {
    match table {
        "case_blobs" => Ok(&["case_hash"]),
        "dataset_versions" => Ok(&["dataset_version_id"]),
        "runs" => Ok(&["id"]),
        _ => SHARD_TABLES
            .iter()
            .find(|candidate| candidate.name == table)
            .map(|candidate| candidate.key_columns)
            .ok_or_else(|| anyhow::anyhow!("unsupported shard move table {}", table)),
    }
}

fn move_table_names() -> impl Iterator<Item = &'static str> {
    PREREQUISITE_TABLES
        .iter()
        .copied()
        .chain(SHARD_TABLES.iter().map(|table| table.name))
}

fn move_key_expression(table_alias: &str, key_columns: &[&str]) -> String {
    let columns = key_columns
        .iter()
        .map(|column| format!("to_jsonb({table_alias}.{column})"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("jsonb_build_array({columns})::text")
}

async fn install_target_move_write_fence(
    target_db: &PgPool,
    fence: &TargetMoveWriteFence,
) -> anyhow::Result<()> {
    let mut tx = target_db.begin().await?;
    crate::db::shard_write_fence::lock_exclusive(&mut tx, fence.run_id, fence.run_shard).await?;
    install_local_shard_move_fence(
        &mut *tx,
        LocalShardAdmissionDraft {
            run_id: fence.run_id,
            run_shard: fence.run_shard,
            database_alias: fence.database_alias.clone(),
            write_epoch: fence.write_epoch,
            state: LocalShardAdmissionState::Prepared,
            redirect_database_alias: None,
            move_fence: Some(fence.move_fence),
        },
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

async fn validate_target_move_write(
    tx: &mut Transaction<'_, Postgres>,
    fence: &TargetMoveWriteFence,
) -> anyhow::Result<()> {
    validate_local_shard_move_fence(
        &mut **tx,
        fence.run_id,
        fence.run_shard,
        &fence.database_alias,
        fence.write_epoch,
        fence.move_fence,
    )
    .await?;
    Ok(())
}

async fn reset_target_move_rows(
    target_db: &PgPool,
    fence: &TargetMoveWriteFence,
) -> anyhow::Result<()> {
    let mut tx = target_db.begin().await?;
    crate::db::shard_write_fence::lock_exclusive(&mut tx, fence.run_id, fence.run_shard).await?;
    validate_target_move_write(&mut tx, fence).await?;
    reset_target_shard_rows(&mut tx, fence.run_id, fence.run_shard).await?;
    tx.commit().await?;
    Ok(())
}

async fn copy_target_move_rows(
    target_db: &PgPool,
    fence: &TargetMoveWriteFence,
    table: &str,
    rows: Vec<Value>,
) -> anyhow::Result<u64> {
    let mut tx = target_db.begin().await?;
    crate::db::shard_write_fence::lock_shared(&mut tx, fence.run_id, fence.run_shard).await?;
    validate_target_move_write(&mut tx, fence).await?;
    let copied = copy_json_rows(&mut tx, table, rows).await?;
    tx.commit().await?;
    Ok(copied)
}

async fn upsert_target_move_rows(
    target_db: &PgPool,
    fence: &TargetMoveWriteFence,
    table: &str,
    key_columns: &[&str],
    rows: Vec<Value>,
) -> anyhow::Result<u64> {
    let mut tx = target_db.begin().await?;
    crate::db::shard_write_fence::lock_shared(&mut tx, fence.run_id, fence.run_shard).await?;
    validate_target_move_write(&mut tx, fence).await?;
    let copied = upsert_json_rows(&mut tx, table, key_columns, rows).await?;
    tx.commit().await?;
    Ok(copied)
}

async fn delete_target_move_rows(
    target_db: &PgPool,
    fence: &TargetMoveWriteFence,
    table: &str,
    key_columns: &[&str],
    keys: &[Value],
) -> anyhow::Result<()> {
    let mut tx = target_db.begin().await?;
    crate::db::shard_write_fence::lock_shared(&mut tx, fence.run_id, fence.run_shard).await?;
    validate_target_move_write(&mut tx, fence).await?;
    delete_target_rows_for_dirty_keys(&mut tx, table, key_columns, keys).await?;
    tx.commit().await?;
    Ok(())
}

async fn copy_and_checkpoint_move_page(
    context: &ShardMoveCopyContext<'_>,
    table: &str,
    page_number: i64,
    previous_cursor: Option<&str>,
    rows: &[MoveSourceRow],
) -> anyhow::Result<()> {
    let payload = rows.iter().map(|row| row.row.clone()).collect::<Vec<_>>();
    if PREREQUISITE_TABLES.contains(&table) {
        copy_target_move_rows(context.target_db, context.target_fence, table, payload).await?;
    } else {
        upsert_target_move_rows(
            context.target_db,
            context.target_fence,
            table,
            move_table_key_columns(table)?,
            payload,
        )
        .await?;
    }
    record_completed_move_page(
        context.control_db,
        context.target_fence.move_fence.move_id,
        context.target_fence.move_fence.claim_token,
        table,
        page_number,
        previous_cursor,
        rows,
    )
    .await?;
    Ok(())
}

async fn backfill_shard_move(context: &ShardMoveCopyContext<'_>) -> anyhow::Result<()> {
    for table in move_table_names() {
        let (mut page_number, mut cursor) = select_last_move_page(
            context.control_db,
            context.target_fence.move_fence.move_id,
            table,
        )
        .await?;
        loop {
            let rows = select_move_source_page(
                context.source_db,
                table,
                context.target_fence.run_id,
                context.target_fence.run_shard,
                cursor.as_deref(),
            )
            .await?;
            if rows.is_empty() {
                break;
            }
            copy_and_checkpoint_move_page(context, table, page_number, cursor.as_deref(), &rows)
                .await?;
            cursor = rows.last().map(|row| row.row_key.clone());
            page_number += 1;
        }
    }
    let updated = update_claimed_move_phase(
        context.control_db,
        context.target_fence.move_fence.move_id,
        context.target_fence.move_fence.claim_token,
        "catch_up",
    )
    .await?;
    if updated != 1 {
        anyhow::bail!(
            "shard move {} lost its claim after backfill",
            context.target_fence.move_fence.move_id
        );
    }
    Ok(())
}

fn key_join_predicate(table_alias: &str, key_alias: &str, key_columns: &[&str]) -> String {
    key_columns
        .iter()
        .map(|column| {
            let cast = if *column == "run_shard" {
                "smallint"
            } else if *column == "case_hash" {
                "text"
            } else {
                "uuid"
            };
            format!("{table_alias}.{column} = ({key_alias}.row_key->>'{column}')::{cast}")
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

async fn replay_dirty_shard_keys(
    source_db: &PgPool,
    target_db: &PgPool,
    target_fence: &TargetMoveWriteFence,
    move_id: Uuid,
    max_batches: usize,
) -> anyhow::Result<i64> {
    let mut completed_batches = 0;
    for table in SHARD_TABLES {
        while completed_batches < max_batches {
            let keys = select_dirty_shard_keys_for_table(
                source_db,
                move_id,
                table.name,
                table.key_columns,
                true,
                SHARD_MOVE_COPY_BATCH_SIZE,
            )
            .await?;
            if keys.is_empty() {
                break;
            }
            let current =
                select_current_rows_for_dirty_keys(source_db, table.name, table.key_columns, &keys)
                    .await?;
            let rows = current.values().cloned().collect::<Vec<_>>();
            upsert_target_move_rows(target_db, target_fence, table.name, table.key_columns, rows)
                .await?;
            let replayed = keys
                .into_iter()
                .filter(|key| current.contains_key(&key.row_key.to_string()))
                .collect::<Vec<_>>();
            settle_replayed_dirty_keys(source_db, move_id, &replayed).await?;
            completed_batches += 1;
        }
    }

    for table in SHARD_TABLES.iter().rev() {
        while completed_batches < max_batches {
            let keys = select_dirty_shard_keys_for_table(
                source_db,
                move_id,
                table.name,
                table.key_columns,
                false,
                SHARD_MOVE_COPY_BATCH_SIZE,
            )
            .await?;
            if keys.is_empty() {
                break;
            }
            let missing = keys
                .iter()
                .map(|key| key.row_key.clone())
                .collect::<Vec<_>>();
            delete_target_move_rows(
                target_db,
                target_fence,
                table.name,
                table.key_columns,
                &missing,
            )
            .await?;
            settle_replayed_dirty_keys(source_db, move_id, &keys).await?;
            completed_batches += 1;
        }
    }

    count_dirty_move_keys(source_db, move_id).await
}

async fn ensure_move_target_reset(
    control_db: &PgPool,
    target_db: &PgPool,
    target_fence: &TargetMoveWriteFence,
    operation: &ShardMoveOperation,
    claim_token: Uuid,
) -> anyhow::Result<()> {
    if operation.target_reset_at.is_some() {
        return Ok(());
    }
    reset_target_move_rows(target_db, target_fence).await?;
    let updated = mark_move_target_reset(control_db, operation.id, claim_token).await?;
    if updated != 1 {
        anyhow::bail!(
            "shard move {} lost its claim during target reset",
            operation.id
        );
    }
    Ok(())
}

async fn prepare_online_shard_move(
    database_router: &database::DatabaseRouter,
    source_db: &PgPool,
    target_db: &PgPool,
    current: ShardPlacement,
    operation: &ShardMoveOperation,
    claim_token: Uuid,
) -> anyhow::Result<PreparedShardMove> {
    let control_db = database_router.control().await?;
    let mut identity_tx = source_db.begin().await?;
    let same_database = databases_share_identity(&mut identity_tx, target_db).await?;
    identity_tx.commit().await?;
    let mut route = current;
    if route.status != SHARD_PLACEMENT_STATUS_MOVING {
        let state = if route.status == SHARD_PLACEMENT_STATUS_DRAINING {
            LocalShardAdmissionState::Draining
        } else {
            LocalShardAdmissionState::Open
        };
        reconcile_source_admission_with_fence(
            source_db,
            LocalShardAdmissionDraft {
                run_id: operation.run_id,
                run_shard: operation.run_shard,
                database_alias: operation.source_database_alias.clone(),
                write_epoch: route.write_epoch,
                state,
                redirect_database_alias: Some(operation.target_database_alias.clone()),
                move_fence: None,
            },
            route.status.as_str(),
        )
        .await?;
    }
    let authoritative =
        shard_placements::select_shard_placement(control_db, operation.run_id, operation.run_shard)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "run {} shard {} route disappeared before online copy",
                    operation.run_id,
                    operation.run_shard
                )
            })?;
    if !route.same_route_fence(&authoritative) {
        let still_source = authoritative.database_alias == operation.source_database_alias;
        let state = if !still_source {
            LocalShardAdmissionState::Closed
        } else if authoritative.status == SHARD_PLACEMENT_STATUS_DRAINING {
            LocalShardAdmissionState::Draining
        } else {
            LocalShardAdmissionState::Open
        };
        transition_local_admission_with_fence(
            source_db,
            LocalShardAdmissionDraft {
                run_id: operation.run_id,
                run_shard: operation.run_shard,
                database_alias: operation.source_database_alias.clone(),
                write_epoch: authoritative.write_epoch,
                state,
                redirect_database_alias: (!still_source)
                    .then(|| authoritative.database_alias.clone()),
                move_fence: None,
            },
            &[
                LocalShardAdmissionState::Open,
                LocalShardAdmissionState::Draining,
                LocalShardAdmissionState::Closed,
            ],
        )
        .await?;
        anyhow::bail!(
            "run {} shard {} route changed while waiting for move admission; expected {} status {} version {}, found {} status {} version {}",
            operation.run_id,
            operation.run_shard,
            route.database_alias,
            route.status,
            route.route_version,
            authoritative.database_alias,
            authoritative.status,
            authoritative.route_version
        );
    }
    if route.status == SHARD_PLACEMENT_STATUS_ACTIVE {
        if !same_database {
            enable_shard_move_capture(
                source_db,
                operation.id,
                operation.run_id,
                operation.run_shard,
            )
            .await?;
        }
        reserve_active_move_target_and_mark_copying(
            control_db,
            operation.run_id,
            operation.run_shard,
            &operation.source_database_alias,
            route.route_version,
            &operation.target_database_alias,
        )
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "run {} shard {} route changed before online copy reservation",
                operation.run_id,
                operation.run_shard
            )
        })?;
        database_router
            .invalidate_execution_placement(operation.run_id, operation.run_shard)
            .await;
    } else if !same_database {
        enable_shard_move_capture(
            source_db,
            operation.id,
            operation.run_id,
            operation.run_shard,
        )
        .await?;
    }

    if same_database {
        if mark_same_database_move_ready(control_db, operation.id, claim_token).await? != 1 {
            anyhow::bail!(
                "shard move {} lost its claim during preparation",
                operation.id
            );
        }
    } else {
        let target_fence =
            TargetMoveWriteFence::from_operation(operation, route.write_epoch + 1, claim_token)?;
        install_target_move_write_fence(target_db, &target_fence).await?;
        ensure_move_target_reset(control_db, target_db, &target_fence, operation, claim_token)
            .await?;
        if !matches!(
            operation.phase.as_str(),
            "catch_up" | "draining" | "cutover"
        ) {
            backfill_shard_move(&ShardMoveCopyContext {
                control_db,
                source_db,
                target_db,
                target_fence: &target_fence,
            })
            .await?;
        }
        let remaining_dirty = replay_dirty_shard_keys(
            source_db,
            target_db,
            &target_fence,
            operation.id,
            SHARD_MOVE_ONLINE_REPLAY_BATCHES,
        )
        .await?;
        if remaining_dirty > SHARD_MOVE_FINAL_DIRTY_KEY_LIMIT {
            return Err(ShardMoveCatchUpPending {
                run_id: operation.run_id,
                run_shard: operation.run_shard,
                remaining_dirty_key_count: remaining_dirty,
            }
            .into());
        }
    }
    renew_shard_move_claim(control_db, operation.id, claim_token).await?;

    route =
        shard_placements::select_shard_placement(control_db, operation.run_id, operation.run_shard)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "run {} shard {} route disappeared during online copy",
                    operation.run_id,
                    operation.run_shard
                )
            })?;
    if route.status == SHARD_PLACEMENT_STATUS_COPYING {
        transition_local_admission_with_fence(
            source_db,
            LocalShardAdmissionDraft {
                run_id: operation.run_id,
                run_shard: operation.run_shard,
                database_alias: operation.source_database_alias.clone(),
                write_epoch: route.write_epoch,
                state: LocalShardAdmissionState::Draining,
                redirect_database_alias: Some(operation.target_database_alias.clone()),
                move_fence: None,
            },
            &[
                LocalShardAdmissionState::Open,
                LocalShardAdmissionState::Draining,
            ],
        )
        .await?;
        route = shard_placements::mark_shard_placement_draining(
            control_db,
            operation.run_id,
            operation.run_shard,
            &operation.source_database_alias,
            route.route_version,
            &operation.target_database_alias,
        )
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "run {} shard {} route changed before drain cutover",
                operation.run_id,
                operation.run_shard
            )
        })?;
        database_router
            .invalidate_execution_placement(operation.run_id, operation.run_shard)
            .await;
    }
    if update_claimed_move_phase(control_db, operation.id, claim_token, "draining").await? != 1 {
        anyhow::bail!("shard move {} lost its claim before draining", operation.id);
    }
    let target_fence = (!same_database)
        .then(|| {
            TargetMoveWriteFence::from_operation(operation, route.write_epoch + 1, claim_token)
        })
        .transpose()?;
    Ok(PreparedShardMove {
        route,
        target_fence,
    })
}

async fn transition_local_admission_with_fence(
    db: &PgPool,
    draft: LocalShardAdmissionDraft,
    allowed_same_epoch_states: &[LocalShardAdmissionState],
) -> anyhow::Result<()> {
    let mut tx = db.begin().await?;
    crate::db::shard_write_fence::lock_exclusive(&mut tx, draft.run_id, draft.run_shard).await?;
    transition_local_shard_admission(&mut *tx, draft, allowed_same_epoch_states).await?;
    tx.commit().await?;
    Ok(())
}

async fn reconcile_source_admission_with_fence(
    db: &PgPool,
    mut draft: LocalShardAdmissionDraft,
    route_status: &str,
) -> anyhow::Result<()> {
    let mut tx = db.begin().await?;
    crate::db::shard_write_fence::lock_exclusive(&mut tx, draft.run_id, draft.run_shard).await?;
    let current = select_local_shard_admission(&mut *tx, draft.run_id, draft.run_shard).await?;
    // A crash can persist the local drain before the control route advances
    // from copying. Preserve the stricter local state; retry advances control.
    if route_status == SHARD_PLACEMENT_STATUS_COPYING
        && current.as_ref().is_some_and(|current| {
            current.database_alias == draft.database_alias
                && current.write_epoch == draft.write_epoch
                && current.parsed_state().ok() == Some(LocalShardAdmissionState::Draining)
        })
    {
        draft.state = LocalShardAdmissionState::Draining;
    }
    let allowed_same_epoch_states = if draft.state == LocalShardAdmissionState::Draining {
        &[
            LocalShardAdmissionState::Open,
            LocalShardAdmissionState::Draining,
        ][..]
    } else {
        &[LocalShardAdmissionState::Open][..]
    };
    transition_local_shard_admission(&mut *tx, draft, allowed_same_epoch_states).await?;
    tx.commit().await?;
    Ok(())
}

async fn count_active_shard_work(db: &PgPool, run_id: Uuid, run_shard: i16) -> anyhow::Result<i64> {
    count_active_shard_work_with(db, run_id, run_shard).await
}

fn normalized_prerequisite_row(table: &str, mut row: Value) -> anyhow::Result<Value> {
    let object = row
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("{} prerequisite row is not an object", table))?;
    let ignored: &[&str] = match table {
        "case_blobs" => &["created_at"],
        "dataset_versions" => &["created_at", "updated_at"],
        "runs" => &[
            "status",
            "gate_status",
            "coordinator_id",
            "coordinator_leased_until",
            "coordinator_heartbeat_at",
            "terminal_execution_count",
            "passed_execution_count",
            "failed_execution_count",
            "errored_execution_count",
            "summary",
            "error_message",
            "created_at",
            "started_at",
            "dispatched_at",
            "finalized_at",
            "completed_at",
            "updated_at",
        ],
        _ => anyhow::bail!("unsupported prerequisite table {}", table),
    };
    for field in ignored {
        object.remove(*field);
    }
    Ok(row)
}

async fn verify_move_tables(
    source_db: &PgPool,
    target_db: &PgPool,
    run_id: Uuid,
    run_shard: i16,
    copied_rows_by_table: &std::collections::BTreeMap<&'static str, u64>,
) -> anyhow::Result<Vec<ShardMoveTableReport>> {
    let mut source_tx = source_db.begin().await?;
    let reports = verify_move_tables_with_source(
        &mut source_tx,
        target_db,
        run_id,
        run_shard,
        copied_rows_by_table,
    )
    .await?;
    source_tx.commit().await?;
    Ok(reports)
}

async fn verify_move_tables_with_source(
    source_tx: &mut Transaction<'_, Postgres>,
    target_db: &PgPool,
    run_id: Uuid,
    run_shard: i16,
    copied_rows_by_table: &std::collections::BTreeMap<&'static str, u64>,
) -> anyhow::Result<Vec<ShardMoveTableReport>> {
    let mut reports = Vec::with_capacity(PREREQUISITE_TABLES.len() + SHARD_TABLES.len());

    for table in PREREQUISITE_TABLES {
        let source =
            prerequisite_table_fingerprint_with(&mut **source_tx, table, run_id, run_shard).await?;
        let target = prerequisite_table_fingerprint(target_db, table, run_id, run_shard).await?;
        let verified = source.row_count == target.row_count && source.checksum == target.checksum;
        reports.push(ShardMoveTableReport {
            table,
            source_row_count: Some(source.row_count),
            target_row_count: Some(target.row_count),
            copied_row_count: copied_rows_by_table.get(table).copied().unwrap_or_default(),
            source_checksum: Some(source.checksum),
            target_checksum: Some(target.checksum),
            verification_mode: "full_fingerprint",
            verified,
        });
    }

    for table in SHARD_TABLES {
        let source =
            table_fingerprint_with(&mut **source_tx, table.name, run_id, run_shard).await?;
        let target = table_fingerprint(target_db, table.name, run_id, run_shard).await?;
        let verified = source.row_count == target.row_count && source.checksum == target.checksum;
        reports.push(ShardMoveTableReport {
            table: table.name,
            source_row_count: Some(source.row_count),
            target_row_count: Some(target.row_count),
            copied_row_count: copied_rows_by_table
                .get(table.name)
                .copied()
                .unwrap_or_default(),
            source_checksum: Some(source.checksum),
            target_checksum: Some(target.checksum),
            verification_mode: "full_fingerprint",
            verified,
        });
    }

    Ok(reports)
}

async fn prerequisite_table_fingerprint(
    db: &PgPool,
    table: &str,
    run_id: Uuid,
    run_shard: i16,
) -> anyhow::Result<TableFingerprint> {
    prerequisite_table_fingerprint_with(db, table, run_id, run_shard).await
}

async fn table_fingerprint(
    db: &PgPool,
    table: &str,
    run_id: Uuid,
    run_shard: i16,
) -> anyhow::Result<TableFingerprint> {
    table_fingerprint_with(db, table, run_id, run_shard).await
}

async fn fingerprint_ordered_rows<'a>(
    mut rows: impl futures_util::TryStream<Ok = String, Error = sqlx::Error> + Unpin + 'a,
) -> anyhow::Result<TableFingerprint> {
    let mut hasher = blake3::Hasher::new();
    let mut row_count = 0_i64;

    while let Some(row) = rows.try_next().await? {
        let bytes = row.as_bytes();
        hasher.update(&(bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
        row_count += 1;
    }

    Ok(TableFingerprint {
        row_count,
        checksum: hasher.finalize().to_hex().to_string(),
    })
}

fn validate_run_shard(run_shard: i16) -> anyhow::Result<()> {
    if !(0..RUN_SHARD_COUNT).contains(&run_shard) {
        anyhow::bail!(
            "run_shard {} is outside the supported range 0..{}",
            run_shard,
            RUN_SHARD_COUNT
        );
    }

    Ok(())
}

fn validate_non_empty(value: &str, label: &str) -> anyhow::Result<()> {
    if value.trim().is_empty() {
        anyhow::bail!("{} must not be empty", label);
    }

    Ok(())
}

#[cfg(test)]
#[path = "shard_admin/postgres_tests.rs"]
mod postgres_tests;
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn move_page_is_bounded_by_rows_and_bytes() {
        assert_eq!(bounded_page_len(&[4, 4, 4, 4], 3, 100), 3);
        assert_eq!(bounded_page_len(&[4, 4, 4, 4], 10, 9), 2);
    }

    #[test]
    fn move_page_allows_one_oversized_row_to_make_progress() {
        assert_eq!(bounded_page_len(&[20, 2], 10, 10), 1);
        assert_eq!(bounded_page_len(&[], 10, 10), 0);
    }

    #[test]
    fn move_page_checkpoint_is_stable_and_counts_nonnegative_bytes() {
        let rows = [
            MoveSourceRow {
                row: serde_json::json!({"id": 1}),
                row_key: "one".to_string(),
                row_bytes: -1,
            },
            MoveSourceRow {
                row: serde_json::json!({"id": 2}),
                row_key: "two".to_string(),
                row_bytes: 12,
            },
        ];

        let first = move_page_checkpoint(&rows).unwrap();
        let repeated = move_page_checkpoint(&rows).unwrap();

        assert_eq!(first, repeated);
        assert_eq!(first.end_key, "two");
        assert_eq!(first.row_count, 2);
        assert_eq!(first.byte_count, 12);
    }

    #[test]
    fn move_page_checkpoint_rejects_empty_pages_and_detects_order() {
        assert!(move_page_checkpoint(&[]).is_err());
        let mut rows = [
            MoveSourceRow {
                row: serde_json::json!({"id": 1}),
                row_key: "one".to_string(),
                row_bytes: 1,
            },
            MoveSourceRow {
                row: serde_json::json!({"id": 2}),
                row_key: "two".to_string(),
                row_bytes: 1,
            },
        ];
        let checksum = move_page_checkpoint(&rows).unwrap().checksum;
        rows.reverse();

        assert_ne!(move_page_checkpoint(&rows).unwrap().checksum, checksum);
    }

    #[test]
    fn move_table_metadata_covers_prerequisites_and_shard_rows() {
        assert_eq!(move_table_key_columns("case_blobs").unwrap(), ["case_hash"]);
        assert_eq!(move_table_key_columns("runs").unwrap(), ["id"]);
        assert_eq!(
            move_table_key_columns("run_chunks").unwrap(),
            ["run_id", "run_shard", "id"]
        );
        assert!(move_table_key_columns("unsupported").is_err());

        let names = move_table_names().collect::<Vec<_>>();
        assert_eq!(names.len(), PREREQUISITE_TABLES.len() + SHARD_TABLES.len());
        assert_eq!(&names[..PREREQUISITE_TABLES.len()], PREREQUISITE_TABLES);
    }

    #[test]
    fn move_key_sql_uses_stable_keys_and_column_types() {
        assert_eq!(
            move_key_expression("row", &["run_id", "run_shard"]),
            "jsonb_build_array(to_jsonb(row.run_id), to_jsonb(row.run_shard))::text"
        );
        assert_eq!(
            key_join_predicate("row", "dirty", &["run_id", "run_shard", "case_hash"]),
            "row.run_id = (dirty.row_key->>'run_id')::uuid AND \
             row.run_shard = (dirty.row_key->>'run_shard')::smallint AND \
             row.case_hash = (dirty.row_key->>'case_hash')::text"
        );
    }

    #[test]
    fn prerequisite_normalization_ignores_only_mutable_fields() {
        let normalized = normalized_prerequisite_row(
            "runs",
            serde_json::json!({
                "id": Uuid::nil(),
                "status": "running",
                "summary": {"local": true},
                "updated_at": "now",
                "config_snapshot": {"stable": true}
            }),
        )
        .unwrap();

        assert_eq!(
            normalized,
            serde_json::json!({
                "id": Uuid::nil(),
                "config_snapshot": {"stable": true}
            })
        );
        assert!(normalized_prerequisite_row("runs", serde_json::json!([])).is_err());
        assert!(normalized_prerequisite_row("unsupported", serde_json::json!({})).is_err());
    }

    #[test]
    fn shard_admin_identifiers_reject_empty_or_whitespace_values() {
        assert!(validate_non_empty("primary", "database alias").is_ok());
        assert!(validate_non_empty("", "database alias").is_err());
        assert!(validate_non_empty("  \t", "database alias").is_err());
    }

    #[test]
    fn abort_route_accepts_each_in_progress_move_state() {
        for status in [
            SHARD_PLACEMENT_STATUS_COPYING,
            SHARD_PLACEMENT_STATUS_DRAINING,
            SHARD_PLACEMENT_STATUS_MOVING,
        ] {
            let placement = shard_placement("source", status, Some("target"));
            assert!(
                validate_abort_route(&placement, placement.run_id, 4, "source", "target").is_ok()
            );
        }
    }

    #[test]
    fn abort_route_rejects_stale_or_mismatched_placement() {
        for placement in [
            shard_placement("source", SHARD_PLACEMENT_STATUS_ACTIVE, Some("target")),
            shard_placement("other", SHARD_PLACEMENT_STATUS_COPYING, Some("target")),
            shard_placement("source", SHARD_PLACEMENT_STATUS_COPYING, Some("other")),
        ] {
            assert!(
                validate_abort_route(&placement, placement.run_id, 4, "source", "target").is_err()
            );
        }
    }

    #[test]
    fn completed_abort_reports_idempotent_source_placement() {
        let placement = shard_placement("source", SHARD_PLACEMENT_STATUS_ACTIVE, None);

        let outcome = completed_abort_outcome(
            &placement,
            placement.run_id,
            placement.run_shard,
            "source",
            "target",
        )
        .unwrap()
        .unwrap();

        assert!(!outcome.aborted);
        assert_eq!(outcome.source_database_alias, "source");
        assert_eq!(outcome.target_database_alias, "target");
    }

    #[test]
    fn completed_abort_distinguishes_in_progress_and_completed_moves() {
        let in_progress = shard_placement("source", SHARD_PLACEMENT_STATUS_COPYING, Some("target"));
        assert!(
            completed_abort_outcome(
                &in_progress,
                in_progress.run_id,
                in_progress.run_shard,
                "source",
                "target"
            )
            .unwrap()
            .is_none()
        );

        let completed = shard_placement("target", SHARD_PLACEMENT_STATUS_ACTIVE, None);
        assert!(
            completed_abort_outcome(
                &completed,
                completed.run_id,
                completed.run_shard,
                "source",
                "target"
            )
            .is_err()
        );
    }

    #[test]
    fn rebalance_claim_eligibility_distinguishes_fresh_and_expired_running_items() {
        let now = Utc::now();

        assert!(rebalance_item_is_claim_eligible("pending", None, now));
        assert!(!rebalance_item_is_claim_eligible(
            "running",
            Some(now + chrono::Duration::seconds(1)),
            now,
        ));
        assert!(rebalance_item_is_claim_eligible(
            "running",
            Some(now - chrono::Duration::seconds(1)),
            now,
        ));
        assert!(!rebalance_item_is_claim_eligible("completed", None, now));
    }

    #[test]
    fn validate_run_shard_rejects_out_of_range_values() {
        assert!(validate_run_shard(0).is_ok());
        assert!(validate_run_shard(127).is_ok());
        assert!(validate_run_shard(-1).is_err());
        assert!(validate_run_shard(128).is_err());
    }

    #[test]
    fn rebalance_candidates_drain_source_in_route_order() {
        let run_a = Uuid::now_v7();
        let run_b = Uuid::now_v7();
        let candidates = vec![
            rebalance_candidate(run_b, 2, "primary", 1),
            rebalance_candidate(run_a, 1, "primary", 1),
            rebalance_candidate(run_a, 0, "shard_001", 1),
        ];

        let selected = select_rebalance_candidates(
            candidates,
            &["primary".to_string(), "shard_001".to_string()],
            Some("primary"),
            "shard_001",
            10,
        );

        assert_eq!(
            selected,
            vec![
                rebalance_candidate(run_a, 1, "primary", 1),
                rebalance_candidate(run_b, 2, "primary", 1),
            ]
        );
    }

    #[test]
    fn rebalance_candidates_fill_target_to_even_distribution() {
        let run_id = Uuid::now_v7();
        let candidates = vec![
            rebalance_candidate(run_id, 0, "primary", 1),
            rebalance_candidate(run_id, 1, "primary", 1),
            rebalance_candidate(run_id, 2, "primary", 1),
            rebalance_candidate(run_id, 3, "primary", 1),
            rebalance_candidate(run_id, 4, "shard_001", 1),
        ];

        let selected = select_rebalance_candidates(
            candidates,
            &["primary".to_string(), "shard_001".to_string()],
            None,
            "shard_001",
            10,
        );

        assert_eq!(
            selected,
            vec![
                rebalance_candidate(run_id, 0, "primary", 1),
                rebalance_candidate(run_id, 1, "primary", 1),
            ]
        );
    }

    #[test]
    fn rebalance_candidates_respect_capacity_and_topology_limits() {
        let run_id = Uuid::nil();
        let candidates = vec![
            rebalance_candidate(run_id, 0, "primary", 1),
            rebalance_candidate(run_id, 1, "primary", 1),
            rebalance_candidate(run_id, 2, "primary", 1),
        ];

        assert!(
            select_rebalance_candidates(
                candidates.clone(),
                &["primary".to_string()],
                None,
                "target",
                10
            )
            .is_empty()
        );
        assert!(
            select_rebalance_candidates(
                candidates.clone(),
                &["primary".to_string(), "target".to_string()],
                None,
                "target",
                0
            )
            .is_empty()
        );
        assert_eq!(
            select_rebalance_candidates(
                candidates,
                &["primary".to_string(), "target".to_string()],
                None,
                "target",
                1
            )
            .len(),
            1
        );
    }

    fn rebalance_candidate(
        run_id: Uuid,
        run_shard: i16,
        source_database_alias: &str,
        route_version: i64,
    ) -> ShardRebalanceCandidate {
        ShardRebalanceCandidate {
            run_id,
            run_shard,
            source_database_alias: source_database_alias.to_string(),
            route_version,
        }
    }

    fn shard_placement(
        database_alias: &str,
        status: &str,
        move_target_database_alias: Option<&str>,
    ) -> ShardPlacement {
        let timestamp = DateTime::<Utc>::from_timestamp(0, 0).unwrap();
        ShardPlacement {
            run_id: Uuid::nil(),
            run_shard: 4,
            database_alias: database_alias.to_string(),
            status: status.to_string(),
            move_target_database_alias: move_target_database_alias.map(ToOwned::to_owned),
            route_version: 2,
            write_epoch: 1,
            created_at: timestamp,
            updated_at: timestamp,
        }
    }
}
