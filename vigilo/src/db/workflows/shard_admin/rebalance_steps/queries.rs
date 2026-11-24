//! Control-store queries for a single rebalance planning page.

use super::*;

#[derive(sqlx::FromRow)]
pub(super) struct PlanRow {
    #[sqlx(flatten)]
    pub(super) operation: ShardRebalanceOperation,
    pub(super) planning_state: Option<Json<PlanningState>>,
}

pub(super) async fn lock_plan(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
) -> anyhow::Result<Option<PlanRow>> {
    Ok(
        sqlx::query_as("SELECT * FROM shard_rebalance_operations WHERE id=$1 FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut **tx)
            .await?,
    )
}

pub(super) async fn start_plan(
    tx: &mut Transaction<'_, Postgres>,
    request: &PlanRequest<'_>,
    state: PlanningState,
) -> anyhow::Result<Option<PlanRow>> {
    let operation = sqlx::query_as(
        "INSERT INTO shard_rebalance_operations
           (id,strategy,source_database_alias,target_database_alias,status,library_max_items,planning_state)
         VALUES($1,$2,$3,$4,'planning',$5,$6)
         ON CONFLICT (id) DO NOTHING RETURNING *",
    ).bind(request.operation_id).bind(if request.source.is_some() { REBALANCE_STRATEGY_DRAIN_SOURCE } else { REBALANCE_STRATEGY_FILL_TARGET })
        .bind(request.source).bind(request.target).bind(request.max_items).bind(Json(state))
        .fetch_optional(&mut **tx).await?;
    Ok(operation)
}

pub(super) async fn checkpoint_plan(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
    next: Option<PlanningState>,
    item_count: i32,
) -> anyhow::Result<ShardRebalanceOperation> {
    let status = if next.is_some() {
        "planning"
    } else if item_count == 0 {
        REBALANCE_OPERATION_STATUS_COMPLETED
    } else {
        "planned"
    };
    Ok(sqlx::query_as(
        "UPDATE shard_rebalance_operations
         SET planning_state=$2,planned_item_count=$3,status=$4,updated_at=now(),
             completed_at=CASE WHEN $4='completed' THEN now() ELSE NULL END
         WHERE id=$1 RETURNING *",
    )
    .bind(id)
    .bind(next.map(Json))
    .bind(item_count)
    .bind(status)
    .fetch_one(&mut **tx)
    .await?)
}

pub(super) async fn read_planned_items(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
    limit: i32,
) -> anyhow::Result<Vec<PlannedShardRebalanceItem>> {
    Ok(sqlx::query_as(
        "SELECT run_id,run_shard,source_database_alias,target_database_alias,planned_route_version
         FROM shard_rebalance_items WHERE operation_id=$1 ORDER BY sequence_no LIMIT $2",
    )
    .bind(id)
    .bind(i64::from(limit))
    .fetch_all(&mut **tx)
    .await?)
}

pub(super) async fn catalog_page(
    tx: &mut Transaction<'_, Postgres>,
    after: Option<&str>,
) -> anyhow::Result<Vec<String>> {
    Ok(sqlx::query_scalar(
        "SELECT alias FROM database_placements
         WHERE status='active' AND role IN ('shard','control_and_shard') AND alias > $1
         ORDER BY alias LIMIT $2",
    )
    .bind(after.unwrap_or(""))
    .bind(PLACEMENT_PAGE_SIZE as i64)
    .fetch_all(&mut **tx)
    .await?)
}

pub(super) async fn record_observed_placements(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
    aliases: &[String],
) -> anyhow::Result<()> {
    if !aliases.is_empty() {
        sqlx::query(
            "INSERT INTO shard_rebalance_plan_placements(operation_id,database_alias)
             SELECT $1,alias FROM unnest($2::text[]) AS placement(alias)",
        )
        .bind(id)
        .bind(aliases)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

pub(super) async fn observed_placement_page(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
    after: Option<&str>,
    limit: usize,
) -> anyhow::Result<Vec<(String, i64)>> {
    Ok(sqlx::query_as(
        "SELECT database_alias,eligible_count FROM shard_rebalance_plan_placements
         WHERE operation_id=$1 AND database_alias > $2 ORDER BY database_alias LIMIT $3",
    )
    .bind(id)
    .bind(after.unwrap_or(""))
    .bind(limit as i64)
    .fetch_all(&mut **tx)
    .await?)
}

pub(super) async fn observed_count(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
    alias: &str,
) -> anyhow::Result<Option<i64>> {
    Ok(sqlx::query_scalar(
        "SELECT eligible_count FROM shard_rebalance_plan_placements
         WHERE operation_id=$1 AND database_alias=$2",
    )
    .bind(id)
    .bind(alias)
    .fetch_optional(&mut **tx)
    .await?)
}

pub(super) async fn increment_observed_count(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
    alias: &str,
    eligible: i64,
) -> anyhow::Result<()> {
    if eligible != 0 {
        sqlx::query(
            "UPDATE shard_rebalance_plan_placements SET eligible_count=eligible_count+$3
             WHERE operation_id=$1 AND database_alias=$2",
        )
        .bind(id)
        .bind(alias)
        .bind(eligible)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

/// Limit raw index entries before evaluating whether a run is still creating.
/// A sparse eligible population advances the raw cursor without a long scan.
pub(super) async fn candidate_page(
    tx: &mut Transaction<'_, Postgres>,
    alias: &str,
    cursor: Option<RouteCursor>,
    skip_planned: Option<Uuid>,
) -> anyhow::Result<CandidatePage> {
    let mut select = sqlx::QueryBuilder::<Postgres>::new(
        "WITH page AS MATERIALIZED (
           SELECT run_id,run_shard,route_version FROM shard_placements
           WHERE status='active' AND database_alias=",
    );
    select.push_bind(alias);
    if let Some(cursor) = cursor {
        select
            .push(" AND (run_id,run_shard)>(")
            .push_bind(cursor.run_id)
            .push(",")
            .push_bind(cursor.run_shard)
            .push(")");
    }
    select
        .push(" ORDER BY run_id,run_shard LIMIT ")
        .push_bind(ROUTE_PAGE_SIZE as i64);
    select.push(
        ") SELECT page.*, COALESCE((SELECT r.status <> 'creating' FROM runs r WHERE r.id=page.run_id),false)",
    );
    // A route may have moved to a different donor since an earlier page. Keep
    // the first plan fence and advance past that identity without reinserting.
    if let Some(id) = skip_planned {
        select
            .push(" AND NOT EXISTS (SELECT 1 FROM shard_rebalance_items i WHERE i.operation_id=")
            .push_bind(id)
            .push(" AND i.run_id=page.run_id AND i.run_shard=page.run_shard)");
    }
    select.push(" AS eligible FROM page ORDER BY run_id,run_shard");
    let rows: Vec<CandidateRow> = select.build_query_as().fetch_all(&mut **tx).await?;
    let next = if rows.len() == ROUTE_PAGE_SIZE {
        rows.last().map(|row| RouteCursor {
            run_id: row.run_id,
            run_shard: row.run_shard,
        })
    } else {
        None
    };
    Ok(CandidatePage { rows, next })
}
