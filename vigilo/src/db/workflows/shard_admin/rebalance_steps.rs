//! Bounded operator planning and application over the existing rebalance ledger.
//!
//! Planning owns one control transaction and advances a persisted cursor. A
//! target fill observes capacity in separate bounded placement and route pages.
//! Application leases one item at a time and spends a shared batch budget on
//! the existing fenced single-shard mover. No control lock spans remote I/O.

use serde::Deserialize;

use super::*;

mod queries;
use queries::*;

const ROUTE_PAGE_SIZE: usize = 256;
const PLACEMENT_PAGE_SIZE: usize = 128;
const APPLY_LEASE_SECONDS: i32 = SHARD_MOVE_CLAIM_SECONDS + 60;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct RouteCursor {
    run_id: Uuid,
    run_shard: i16,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct PlacementCursor {
    alias: Option<String>,
    // A route cursor continues within alias. Otherwise seek the next alias.
    route: Option<RouteCursor>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "stage", rename_all = "snake_case")]
enum PlanningState {
    Drain {
        route: Option<RouteCursor>,
    },
    Catalog {
        after: Option<String>,
        count: i64,
    },
    Count {
        cursor: PlacementCursor,
        placements: i64,
        total: i64,
    },
    Fill {
        cursor: PlacementCursor,
        desired: i64,
        limit: i32,
        donor_remaining: i64,
    },
}

pub(crate) struct PlanRequest<'a> {
    pub(crate) operation_id: Uuid,
    pub(crate) source: Option<&'a str>,
    pub(crate) target: &'a str,
    pub(crate) max_items: i32,
}

pub(crate) struct PlanOutcome {
    pub(crate) operation: ShardRebalanceOperation,
    pub(crate) items: Vec<PlannedShardRebalanceItem>,
}

/// Each invocation observes one page of catalog or routes in read committed
/// isolation. Stored route versions certify only that observation. Application
/// always checks them against current ownership.
pub(crate) async fn plan(db: &PgPool, request: PlanRequest<'_>) -> anyhow::Result<PlanOutcome> {
    let mut tx = db
        .begin_with("BEGIN ISOLATION LEVEL READ COMMITTED")
        .await?;
    let existing = lock_plan(&mut tx, request.operation_id).await?;
    let checked_placements = existing.is_none();
    let PlanRow {
        mut operation,
        planning_state,
    } = if let Some(existing) = existing {
        existing
    } else {
        validate_placements(&mut tx, &request).await?;
        let state = if request.source.is_some() {
            PlanningState::Drain { route: None }
        } else {
            PlanningState::Catalog {
                after: None,
                count: 0,
            }
        };
        if let Some(operation) = start_plan(&mut tx, &request, state).await? {
            operation
        } else {
            lock_plan(&mut tx, request.operation_id)
                .await?
                .ok_or(PlacementConflict(
                    "rebalance plan disappeared during reservation",
                ))?
        }
    };

    if operation.library_max_items != Some(request.max_items)
        || operation.source_database_alias.as_deref() != request.source
        || operation.target_database_alias != request.target
    {
        return Err(PlacementConflict("rebalance ID already belongs to another request").into());
    }

    if let Some(Json(state)) = planning_state {
        if !checked_placements {
            validate_placements(&mut tx, &request).await?;
        }
        let (next, items) = advance_planning(&mut tx, &operation, request.max_items, state).await?;
        append_rebalance_items(&mut tx, &operation, operation.planned_item_count, &items).await?;
        operation = checkpoint_plan(
            &mut tx,
            operation.id,
            next,
            operation.planned_item_count + items.len() as i32,
        )
        .await?;
    }

    let items = if operation.status == "planning" {
        Vec::new()
    } else {
        read_planned_items(&mut tx, operation.id, request.max_items).await?
    };
    tx.commit().await?;
    Ok(PlanOutcome { operation, items })
}

async fn validate_placements(
    tx: &mut Transaction<'_, Postgres>,
    request: &PlanRequest<'_>,
) -> anyhow::Result<()> {
    validate_new_ownership_target(tx, request.target).await?;
    if let Some(alias) = request.source {
        let placement = database_placements::select_database_placement_for_share(tx, alias)
            .await?
            .ok_or(PlacementConflict("rebalance source is missing"))?;
        if !placement.is_shard_capable() || !placement.can_serve_owned_shards() {
            return Err(PlacementConflict("rebalance source cannot serve shard ownership").into());
        }
    }
    Ok(())
}

async fn advance_planning(
    tx: &mut Transaction<'_, Postgres>,
    operation: &ShardRebalanceOperation,
    max_items: i32,
    state: PlanningState,
) -> anyhow::Result<(Option<PlanningState>, Vec<PlannedShardRebalanceItem>)> {
    let operation_id = operation.id;
    let target = operation.target_database_alias.as_str();
    match state {
        PlanningState::Drain { route } => {
            let source = operation
                .source_database_alias
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("drain plan has no source alias"))?;
            let page = candidate_page(tx, source, route, None).await?;
            let items = page.items(source, target, max_items - operation.planned_item_count);
            let next = page
                .next
                .filter(|_| operation.planned_item_count + (items.len() as i32) < max_items)
                .map(|route| PlanningState::Drain { route: Some(route) });
            Ok((next, items))
        }
        PlanningState::Catalog { after, count } => {
            let aliases = catalog_page(tx, after.as_deref()).await?;
            record_observed_placements(tx, operation_id, &aliases).await?;
            let count = count + aliases.len() as i64;
            let next = if aliases.len() == PLACEMENT_PAGE_SIZE {
                PlanningState::Catalog {
                    after: aliases.last().cloned(),
                    count,
                }
            } else {
                PlanningState::Count {
                    cursor: PlacementCursor::default(),
                    placements: count,
                    total: 0,
                }
            };
            Ok((Some(next), Vec::new()))
        }
        PlanningState::Count {
            cursor,
            placements,
            total,
        } => {
            let Some(source) = count_source(tx, operation_id, &cursor).await? else {
                let wanted = if placements < 2 {
                    0
                } else {
                    let desired = total / placements + i64::from(total % placements != 0);
                    let target_count = observed_count(tx, operation_id, target).await?.ok_or(
                        PlacementConflict("rebalance target changed during catalog observation"),
                    )?;
                    desired
                        .saturating_sub(target_count)
                        .clamp(0, i64::from(max_items))
                };
                let next = (wanted > 0).then(|| PlanningState::Fill {
                    cursor: PlacementCursor::default(),
                    desired: total / placements + i64::from(total % placements != 0),
                    limit: wanted as i32,
                    donor_remaining: 0,
                });
                return Ok((next, Vec::new()));
            };
            let page = candidate_page(tx, &source, cursor.route, None).await?;
            let eligible = page.rows.iter().filter(|row| row.eligible).count() as i64;
            increment_observed_count(tx, operation_id, &source, eligible).await?;
            let cursor = PlacementCursor {
                alias: Some(source),
                route: page.next,
            };
            Ok((
                Some(PlanningState::Count {
                    cursor,
                    placements,
                    total: total + eligible,
                }),
                Vec::new(),
            ))
        }
        PlanningState::Fill {
            cursor,
            desired,
            limit,
            donor_remaining,
        } => advance_fill(tx, operation, cursor, desired, limit, donor_remaining).await,
    }
}

async fn advance_fill(
    tx: &mut Transaction<'_, Postgres>,
    operation: &ShardRebalanceOperation,
    cursor: PlacementCursor,
    desired: i64,
    limit: i32,
    donor_remaining: i64,
) -> anyhow::Result<(Option<PlanningState>, Vec<PlannedShardRebalanceItem>)> {
    let target = operation.target_database_alias.as_str();
    let (source, donor_remaining) = if cursor.route.is_some() {
        (
            cursor
                .alias
                .ok_or_else(|| anyhow::anyhow!("donor route cursor has no alias"))?,
            donor_remaining,
        )
    } else {
        let aliases = observed_placement_page(
            tx,
            operation.id,
            cursor.alias.as_deref(),
            PLACEMENT_PAGE_SIZE,
        )
        .await?;
        if let Some((source, count)) = aliases
            .iter()
            .find(|(alias, count)| alias != target && *count > desired)
        {
            (source.clone(), count - desired)
        } else {
            let next = (aliases.len() == PLACEMENT_PAGE_SIZE).then(|| PlanningState::Fill {
                cursor: PlacementCursor {
                    alias: aliases.last().map(|(alias, _)| alias.clone()),
                    route: None,
                },
                desired,
                limit,
                donor_remaining: 0,
            });
            return Ok((next, Vec::new()));
        }
    };
    let page = candidate_page(tx, &source, cursor.route, Some(operation.id)).await?;
    let remaining = limit - operation.planned_item_count;
    let items = page.items(
        &source,
        target,
        i64::from(remaining).min(donor_remaining) as i32,
    );
    let donor_remaining = donor_remaining - items.len() as i64;
    let next = ((items.len() as i32) < remaining).then(|| PlanningState::Fill {
        cursor: PlacementCursor {
            alias: Some(source),
            route: page.next.filter(|_| donor_remaining > 0),
        },
        desired,
        limit,
        donor_remaining,
    });
    Ok((next, items))
}

async fn count_source(
    tx: &mut Transaction<'_, Postgres>,
    operation_id: Uuid,
    cursor: &PlacementCursor,
) -> anyhow::Result<Option<String>> {
    if cursor.route.is_some() {
        return cursor
            .alias
            .clone()
            .map(Some)
            .ok_or_else(|| anyhow::anyhow!("counting cursor has no placement alias"));
    }
    Ok(
        observed_placement_page(tx, operation_id, cursor.alias.as_deref(), 1)
            .await?
            .into_iter()
            .next()
            .map(|(alias, _)| alias),
    )
}

#[derive(sqlx::FromRow)]
struct CandidateRow {
    run_id: Uuid,
    run_shard: i16,
    route_version: i64,
    eligible: bool,
}

struct CandidatePage {
    rows: Vec<CandidateRow>,
    next: Option<RouteCursor>,
}

impl CandidatePage {
    fn items(&self, source: &str, target: &str, limit: i32) -> Vec<PlannedShardRebalanceItem> {
        self.rows
            .iter()
            .filter(|row| row.eligible)
            .take(limit as usize)
            .map(|row| PlannedShardRebalanceItem {
                run_id: row.run_id,
                run_shard: row.run_shard,
                source_database_alias: source.to_owned(),
                target_database_alias: target.to_owned(),
                planned_route_version: row.route_version,
            })
            .collect()
    }
}

pub(crate) struct ApplyRequest {
    pub(crate) operation_id: Uuid,
    pub(crate) after_sequence: i32,
    pub(crate) max_items: u16,
    pub(crate) max_batches: u16,
}

pub(crate) struct ApplyOutcome {
    pub(crate) operation: ShardRebalanceOperation,
    pub(crate) items: Vec<AppliedItem>,
    pub(crate) batches_processed: u16,
    pub(crate) next_after_sequence: Option<i32>,
}

pub(crate) struct AppliedItem {
    pub(crate) item: ShardRebalanceItem,
    pub(crate) move_operation_id: Uuid,
    pub(crate) progress: ItemProgress,
}

pub(crate) enum ItemProgress {
    Move(ShardMoveProgress),
    StaleRoute,
    Cancelled,
}

/// Move claims may remain leased after a lost call. The item lease outlives any
/// move claim renewed during the timed apply call so a reclaimer can resume both.
pub(crate) async fn apply(
    router: &database::DatabaseRouter,
    bindings: &BTreeMap<String, database::OperatorDatabaseBinding>,
    request: ApplyRequest,
) -> anyhow::Result<ApplyOutcome> {
    let db = router.control().await?;
    let operation = select_rebalance_operation(db, request.operation_id)
        .await?
        .ok_or(PlacementConflict("rebalance plan was not found"))?;
    apply_known(router, Some(bindings), operation, request).await
}

pub(super) async fn apply_known(
    router: &database::DatabaseRouter,
    bindings: Option<&BTreeMap<String, database::OperatorDatabaseBinding>>,
    operation: ShardRebalanceOperation,
    request: ApplyRequest,
) -> anyhow::Result<ApplyOutcome> {
    if operation.library_max_items.is_none() {
        return Err(PlacementConflict("bounded apply requires a bounded rebalance plan").into());
    }
    if operation.status == "planning" {
        return Err(PlacementConflict("rebalance planning is still in progress").into());
    }
    let mut outcome = ApplyOutcome {
        operation,
        items: Vec::with_capacity(usize::from(request.max_items)),
        batches_processed: 0,
        next_after_sequence: None,
    };
    if !matches!(outcome.operation.status.as_str(), "planned" | "running") {
        return Ok(outcome);
    }

    let db = router.control().await?;
    let worker = Uuid::now_v7();
    let mut after = request.after_sequence;
    for _ in 0..request.max_items {
        let remaining = request.max_batches - outcome.batches_processed;
        if remaining == 0 {
            break;
        }
        let Some(claim) = claim_next_rebalance_item_after(
            db,
            request.operation_id,
            worker,
            APPLY_LEASE_SECONDS,
            after,
        )
        .await?
        else {
            outcome.next_after_sequence = None;
            break;
        };
        let item = claim.item;
        after = item.sequence_no;
        outcome.next_after_sequence = Some(after);
        let token = item
            .claim_token
            .ok_or_else(|| anyhow::anyhow!("rebalance claim has no token"))?;
        let move_operation_id = item
            .move_operation_id
            .ok_or_else(|| anyhow::anyhow!("bounded rebalance item has no move identity"))?;
        let (settled, progress) = if rebalance_operation_is_cancelled(db, request.operation_id)
            .await?
        {
            (
                mark_rebalance_item_cancelled(db, &item, token).await?,
                ItemProgress::Cancelled,
            )
        } else {
            let identity = ShardMoveIdentity {
                operation_id: move_operation_id,
                starting_route_version: item.planned_route_version,
                expected_source: Some(&item.source_database_alias),
            };
            match advance_shard_move(
                router,
                bindings,
                item.run_id,
                item.run_shard,
                &item.target_database_alias,
                identity,
                remaining,
            )
            .await
            {
                Ok((progress, used)) => {
                    outcome.batches_processed += used;
                    let settled = if progress == ShardMoveProgress::Completed {
                        mark_rebalance_item_completed(db, &item, token).await?
                    } else {
                        defer_rebalance_item(db, &item, token, &progress.to_string()).await?
                    };
                    (settled, ItemProgress::Move(progress))
                }
                Err(error) if error.is::<StaleShardMoveRoute>() => {
                    let settled =
                        mark_rebalance_item_failed(db, &item, token, &error.to_string()).await?;
                    (settled, ItemProgress::StaleRoute)
                }
                Err(error) => {
                    // Preserve the stable move ID and prior pages. Stop this
                    // call because the failed attempt may have spent its budget.
                    if rebalance_operation_is_cancelled(db, request.operation_id).await? {
                        mark_rebalance_item_cancelled(db, &item, token).await?;
                    } else {
                        defer_rebalance_item(db, &item, token, "shard move can be retried").await?;
                    }
                    return Err(error);
                }
            }
        };
        let settled = settled.ok_or(PlacementConflict("rebalance item lost its claim"))?;
        let progress = if settled.status == REBALANCE_ITEM_STATUS_CANCELLED {
            ItemProgress::Cancelled
        } else {
            progress
        };
        outcome.items.push(AppliedItem {
            item: settled,
            move_operation_id,
            progress,
        });
    }
    outcome.operation = refresh_rebalance_operation_status(db, request.operation_id).await?;
    if !matches!(outcome.operation.status.as_str(), "planned" | "running") {
        outcome.next_after_sequence = None;
    }
    Ok(outcome)
}
