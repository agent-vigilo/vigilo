//! Typed, resumable rebalance calls on the operator's control and move bindings.

use super::*;
use crate::db::workflows::shard_admin::rebalance_steps;

const MAX_PLAN_ITEMS: u16 = 256;
const MAX_APPLY_ITEMS: u16 = 64;
const REBALANCE_TIMEOUT: Duration = Duration::from_secs(30);

/// Starts or continues a bounded rebalance plan. Keep the same fields on retry.
pub struct PlanShardRebalanceRequest {
    /// Caller-generated ID, retained before the first call.
    pub operation_id: Uuid,
    /// Drain this source. When omitted, observe active placements and fill the target.
    pub source_database_alias: Option<String>,
    pub target_database_alias: String,
    /// Maximum total items in the plan, from 1 through 256.
    pub max_items: u16,
}

/// Advances one sealed library plan. A caller may resume from the start or
/// continue after a returned sequence to visit later items.
pub struct ApplyShardRebalanceRequest {
    pub operation_id: Uuid,
    pub after_sequence: Option<u16>,
    /// Maximum item claims in this call, from 1 through 64.
    pub max_items: u16,
    /// Shared move batch budget for the call, from 1 through 64.
    pub max_batches: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ShardRebalanceStatus {
    Planning,
    Planned,
    Running,
    Completed,
    Cancelled,
    Failed,
}

/// Durable progress retained in the control store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardRebalanceSummary {
    pub operation_id: Uuid,
    pub status: ShardRebalanceStatus,
    pub planned_items: u16,
    pub completed_items: u16,
    pub failed_items: u16,
    pub cancelled_items: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRebalanceItem {
    pub run_id: Uuid,
    pub run_shard: i16,
    pub source_database_alias: String,
    pub target_database_alias: String,
    pub expected_route_version: i64,
}

/// A planning receipt. Items are returned once planning has sealed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRebalancePlan {
    pub summary: ShardRebalanceSummary,
    pub items: Vec<ShardRebalanceItem>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ShardRebalanceItemProgress {
    Move(RunShardMoveProgress),
    StaleRoute,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedShardRebalanceItem {
    pub sequence: u16,
    pub move_operation_id: Uuid,
    pub item: ShardRebalanceItem,
    pub progress: ShardRebalanceItemProgress,
}

/// Items settled or yielded in this invocation and the latest durable totals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRebalanceApplyOutcome {
    pub summary: ShardRebalanceSummary,
    pub items: Vec<AppliedShardRebalanceItem>,
    pub batches_processed: u16,
    /// Exclusive continuation for later items. A scan reaching the end returns
    /// None. Retry from the beginning to revisit earlier deferred items.
    pub next_after_sequence: Option<u16>,
}

impl PlacementClient {
    /// Reads at most one route window and one placement window per call.
    /// A fill-target plan accumulates observations across calls. Apply always
    /// revalidates each planned route. Call with the same ID until planning
    /// seals. A call has a 30-second deadline.
    pub async fn plan_shard_rebalance(
        &self,
        request: &PlanShardRebalanceRequest,
    ) -> Result<ShardRebalancePlan, Error> {
        if request.operation_id.is_nil()
            || !valid_name(&request.target_database_alias)
            || request
                .source_database_alias
                .as_ref()
                .is_some_and(|source| {
                    !valid_name(source) || *source == request.target_database_alias
                })
            || !(1..=MAX_PLAN_ITEMS).contains(&request.max_items)
        {
            return Err(Error::InvalidInput(
                "invalid rebalance plan identity, placement, or item limit",
            ));
        }
        let result = tokio::time::timeout(REBALANCE_TIMEOUT, async {
            let db = self.client.database.control().await?;
            rebalance_steps::plan(
                db,
                rebalance_steps::PlanRequest {
                    operation_id: request.operation_id,
                    source: request.source_database_alias.as_deref(),
                    target: &request.target_database_alias,
                    max_items: i32::from(request.max_items),
                },
            )
            .await
        })
        .await
        .map_err(rebalance_timeout)?
        .map_err(placement_error)?;

        Ok(ShardRebalancePlan {
            summary: summary(result.operation)?,
            items: result
                .items
                .into_iter()
                .map(|item| ShardRebalanceItem {
                    run_id: item.run_id,
                    run_shard: item.run_shard,
                    source_database_alias: item.source_database_alias,
                    target_database_alias: item.target_database_alias,
                    expected_route_version: item.planned_route_version,
                })
                .collect(),
        })
    }

    /// Applies one shared batch budget across leased items from a library plan.
    /// Stale items are recorded individually. A dependency failure leaves its
    /// move resumable on retry. An interrupted call can leave its current item
    /// leased for up to six minutes. Each call has a 30-second deadline.
    pub async fn apply_shard_rebalance(
        &self,
        request: &ApplyShardRebalanceRequest,
    ) -> Result<ShardRebalanceApplyOutcome, Error> {
        if request.operation_id.is_nil()
            || !(1..=MAX_APPLY_ITEMS).contains(&request.max_items)
            || !(1..=MAX_MOVE_BATCHES).contains(&request.max_batches)
        {
            return Err(Error::InvalidInput(
                "invalid rebalance apply identity or work budget",
            ));
        }
        let result = tokio::time::timeout(
            REBALANCE_TIMEOUT,
            rebalance_steps::apply(
                &self.client.database,
                &self.bindings,
                rebalance_steps::ApplyRequest {
                    operation_id: request.operation_id,
                    after_sequence: request.after_sequence.map_or(-1, i32::from),
                    max_items: request.max_items,
                    max_batches: request.max_batches,
                },
            ),
        )
        .await
        .map_err(rebalance_timeout)?
        .map_err(placement_error)?;

        let items = result
            .items
            .into_iter()
            .map(|entry| {
                use rebalance_steps::ItemProgress;
                let item = entry.item;
                AppliedShardRebalanceItem {
                    sequence: item.sequence_no as u16,
                    move_operation_id: entry.move_operation_id,
                    item: ShardRebalanceItem {
                        run_id: item.run_id,
                        run_shard: item.run_shard,
                        source_database_alias: item.source_database_alias,
                        target_database_alias: item.target_database_alias,
                        expected_route_version: item.planned_route_version,
                    },
                    progress: match entry.progress {
                        ItemProgress::Move(progress) => {
                            ShardRebalanceItemProgress::Move(progress.into())
                        }
                        ItemProgress::StaleRoute => ShardRebalanceItemProgress::StaleRoute,
                        ItemProgress::Cancelled => ShardRebalanceItemProgress::Cancelled,
                    },
                }
            })
            .collect();
        Ok(ShardRebalanceApplyOutcome {
            summary: summary(result.operation)?,
            items,
            batches_processed: result.batches_processed,
            next_after_sequence: result.next_after_sequence.map(|sequence| sequence as u16),
        })
    }
}

fn summary(
    operation: shard_admin::ShardRebalanceOperation,
) -> Result<ShardRebalanceSummary, Error> {
    let status = match operation.status.as_str() {
        "planning" => ShardRebalanceStatus::Planning,
        "planned" => ShardRebalanceStatus::Planned,
        "running" => ShardRebalanceStatus::Running,
        "completed" => ShardRebalanceStatus::Completed,
        "cancelled" => ShardRebalanceStatus::Cancelled,
        "failed" => ShardRebalanceStatus::Failed,
        _ => {
            return Err(Error::Dependency(anyhow::anyhow!(
                "unknown rebalance status"
            )));
        }
    };
    Ok(ShardRebalanceSummary {
        operation_id: operation.id,
        status,
        planned_items: operation.planned_item_count as u16,
        completed_items: operation.completed_item_count as u16,
        failed_items: operation.failed_item_count as u16,
        cancelled_items: operation.cancelled_item_count as u16,
    })
}

fn rebalance_timeout(error: tokio::time::error::Elapsed) -> Error {
    Error::Dependency(anyhow::Error::new(error).context("rebalance call timed out"))
}
