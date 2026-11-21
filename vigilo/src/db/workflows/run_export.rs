//! Routed run export read helpers.
//!
//! Export reads execution-owned rows from the database placement that owns each
//! `run_id + run_shard` route. The CLI owns output formatting; this module owns
//! routing and SQL pagination.

use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    context::database,
    models::{
        evaluator_result::{
            EvaluatorDiagnostic,
            EvaluatorResult,
        },
        execution::Execution,
        execution_aggregate::ExecutionAggregate,
        execution_attempt::ExecutionAttempt,
    },
};

mod bounded;
mod stream;
pub(crate) use stream::{
    next_execution,
    streams_for_window,
};
#[cfg(test)]
mod queries;
#[cfg(test)]
pub(crate) use bounded::ChildPager;
pub(crate) use bounded::{
    ChildPages,
    Kind,
    load_children,
};

#[derive(Debug)]
pub(crate) struct RunExportBatch {
    pub(crate) executions: Vec<Execution>,
    pub(crate) attempts: Vec<ExecutionAttempt>,
    pub(crate) aggregates: Vec<ExecutionAggregate>,
    pub(crate) evaluator_results: Vec<EvaluatorResult>,
    pub(crate) evaluator_diagnostics: Vec<EvaluatorDiagnostic>,
}

#[derive(Debug, Clone)]
pub(crate) struct RunExportRoute {
    run_shard: i16,
    database_alias: String,
    db: PgPool,
}

/// Logical shards sharing one physical reader within a bounded window. The sequence
/// remains in logical order; no physical alias is stored in a public cursor.
#[derive(Clone)]
pub(crate) struct RunExportGroup {
    shards: Vec<i16>,
    database_alias: String,
    db: PgPool,
}
impl RunExportGroup {
    pub(crate) fn database_alias(&self) -> &str {
        &self.database_alias
    }
    fn single(route: &RunExportRoute) -> Self {
        Self {
            shards: vec![route.run_shard],
            database_alias: route.database_alias.clone(),
            db: route.db.clone(),
        }
    }
}

/// Fixed windows bound partition planning and simultaneously buffered readers.
/// Original route frames remain separate from physical grouping for empty shards.
pub(crate) struct RunExportWindow {
    pub(crate) routes: Vec<(i16, String)>,
    pub(crate) groups: Vec<RunExportGroup>,
}

/// Coalesce each bounded logical range by physical reader; streaming callers
/// merge the readers to preserve global logical order.
pub(crate) fn export_windows(routes: &[RunExportRoute]) -> Vec<RunExportWindow> {
    const SHARDS_PER_WINDOW: usize = 32;
    const SHARDS_PER_READER: usize = 16;
    const READERS_PER_WINDOW: usize = 4;
    let mut windows = Vec::new();
    let mut window = RunExportWindow {
        routes: Vec::new(),
        groups: Vec::new(),
    };
    let mut indices = std::collections::BTreeMap::<String, usize>::new();
    for route in routes {
        let needs_reader = indices
            .get(&route.database_alias)
            .is_none_or(|&index| window.groups[index].shards.len() == SHARDS_PER_READER);
        if window.routes.len() == SHARDS_PER_WINDOW
            || (needs_reader && window.groups.len() == READERS_PER_WINDOW)
        {
            windows.push(window);
            window = RunExportWindow {
                routes: Vec::new(),
                groups: Vec::new(),
            };
            indices.clear();
        }
        window
            .routes
            .push((route.run_shard, route.database_alias.clone()));
        if let Some(&index) = indices
            .get(&route.database_alias)
            .filter(|&&index| window.groups[index].shards.len() < SHARDS_PER_READER)
        {
            window.groups[index].shards.push(route.run_shard);
        } else {
            indices.insert(route.database_alias.clone(), window.groups.len());
            window.groups.push(RunExportGroup::single(route));
        }
    }
    if !window.routes.is_empty() {
        windows.push(window);
    }
    windows
}

#[cfg(test)]
pub(crate) fn group_routes(routes: &[RunExportRoute]) -> Vec<RunExportGroup> {
    export_windows(routes)
        .into_iter()
        .flat_map(|window| window.groups)
        .collect()
}

pub(crate) async fn select_run_export_routes(
    database_router: &database::DatabaseRouter,
    run_id: Uuid,
) -> anyhow::Result<Vec<RunExportRoute>> {
    let routes = database_router
        .execution_read_routes_for_run(run_id)
        .await?
        .into_iter()
        .map(|(run_shard, database_alias, db)| RunExportRoute {
            run_shard,
            database_alias,
            db,
        })
        .collect();

    Ok(routes)
}

pub(crate) struct ExecutionPage {
    pub(crate) executions: Vec<Execution>,
    pub(crate) has_more: bool,
    pub(crate) cursor: Option<serde_json::Value>,
}

/// Reads a root buffer ordered by shard and execution identity; its cursor
/// belongs only to this in-process operator export.
pub(crate) async fn select_group_execution_page(
    group: &RunExportGroup,
    run_id: Uuid,
    cursor: Option<&serde_json::Value>,
    limit: i64,
) -> anyhow::Result<ExecutionPage> {
    let (executions, cursor, complete) =
        bounded::page(group, run_id, &[], Kind::Execution, cursor, limit).await?;
    Ok(ExecutionPage {
        executions,
        cursor,
        has_more: !complete,
    })
}

#[cfg(test)]
pub(crate) async fn select_execution_batch(
    route: &RunExportRoute,
    run_id: Uuid,
    after_execution_id: Option<Uuid>,
    limit: i64,
) -> anyhow::Result<Vec<Execution>> {
    let cursor = after_execution_id.map(|id| serde_json::json!([route.run_shard, id]));
    Ok(select_group_execution_page(
        &RunExportGroup::single(route),
        run_id,
        cursor.as_ref(),
        limit,
    )
    .await?
    .executions)
}

/// Uses one live route discovery for the operator export header and data stream.
/// Summary arithmetic and missing-summary handling remain in their own workflow.
pub(crate) async fn select_summary(
    routes: &[RunExportRoute],
    run_id: Uuid,
) -> anyhow::Result<super::run_results::RunResultsSummary> {
    let routes = routes
        .iter()
        .map(|r| (r.run_shard, r.database_alias.clone(), r.db.clone()))
        .collect::<Vec<_>>();
    Ok(
        super::run_results::select_results_from_routes(run_id, &routes)
            .await?
            .summary,
    )
}

/// Materializes the JSON operator document from bounded child reads. JSONL uses
/// the same pagers directly and never collects complete child families.
pub(crate) async fn select_batch_for_group(
    route: &RunExportGroup,
    run_id: Uuid,
    executions: Vec<Execution>,
) -> anyhow::Result<RunExportBatch> {
    if executions.is_empty() {
        return Ok(RunExportBatch {
            executions,
            attempts: Vec::new(),
            aggregates: Vec::new(),
            evaluator_results: Vec::new(),
            evaluator_diagnostics: Vec::new(),
        });
    }
    let keys = executions
        .iter()
        .map(|e| (e.run_shard, e.id))
        .collect::<Vec<_>>();
    let pages = load_children(route, run_id, &keys, 250).await?;
    let attempts = pages.attempts.collect_all().await?;
    let aggregates = pages
        .aggregates
        .collect_all()
        .await?
        .into_iter()
        .map(|row| row.aggregate)
        .collect();
    let mut evaluator_results = pages.results.collect_all().await?;
    let mut evaluator_diagnostics = pages.diagnostics.collect_all().await?;
    // JSON materializes families and keeps their SQL document ordering. JSONL
    // consumes the pagers directly in execution/attempt/result order instead.
    evaluator_results.sort_unstable_by(|left, right| {
        (
            left.run_shard,
            left.execution_id,
            left.attempt_id,
            &left.binding_id,
            left.created_at,
            left.id,
        )
            .cmp(&(
                right.run_shard,
                right.execution_id,
                right.attempt_id,
                &right.binding_id,
                right.created_at,
                right.id,
            ))
    });
    evaluator_diagnostics
        .sort_unstable_by_key(|row| (row.run_shard, row.evaluator_result_id, row.diagnostic_index));
    Ok(RunExportBatch {
        executions,
        attempts,
        aggregates,
        evaluator_results,
        evaluator_diagnostics,
    })
}

#[cfg(test)]
pub(crate) async fn select_batch_for_executions(
    route: &RunExportRoute,
    run_id: Uuid,
    executions: Vec<Execution>,
) -> anyhow::Result<RunExportBatch> {
    select_batch_for_group(&RunExportGroup::single(route), run_id, executions).await
}

/// Independent materialized-field oracle for bounded operator projection tests.
#[cfg(test)]
pub(crate) async fn select_materialized_oracle(
    route: &RunExportRoute,
    run_id: Uuid,
    executions: Vec<Execution>,
) -> anyhow::Result<RunExportBatch> {
    if executions.is_empty() {
        return Ok(RunExportBatch {
            executions,
            attempts: Vec::new(),
            aggregates: Vec::new(),
            evaluator_results: Vec::new(),
            evaluator_diagnostics: Vec::new(),
        });
    }

    let execution_ids = executions
        .iter()
        .map(|execution| execution.id)
        .collect::<Vec<_>>();

    let attempts =
        queries::select_attempts(&route.db, run_id, route.run_shard, &execution_ids).await?;
    let aggregates =
        queries::select_aggregates(&route.db, run_id, route.run_shard, &execution_ids).await?;

    let attempt_ids = attempts
        .iter()
        .map(|attempt| attempt.id)
        .collect::<Vec<_>>();

    let evaluator_results = if attempt_ids.is_empty() {
        Vec::new()
    } else {
        queries::select_evaluator_results(&route.db, run_id, route.run_shard, &attempt_ids).await?
    };
    let result_ids = evaluator_results
        .iter()
        .map(|row| row.id)
        .collect::<Vec<_>>();
    let evaluator_diagnostics = if result_ids.is_empty() {
        Vec::new()
    } else {
        queries::select_evaluator_diagnostics(&route.db, run_id, route.run_shard, &result_ids)
            .await?
    };

    Ok(RunExportBatch {
        executions,
        attempts,
        aggregates,
        evaluator_results,
        evaluator_diagnostics,
    })
}

#[cfg(test)]
mod tests {
    use sqlx::postgres::PgPoolOptions;

    use super::*;

    #[tokio::test]
    async fn reader_windows_preserve_every_route_and_bound_interleaved_aliases() {
        let closed = closed_route().await;
        for aliases in [1, 2, 7, 128] {
            let routes = (0..128)
                .map(|shard| RunExportRoute {
                    run_shard: shard,
                    database_alias: format!("db{}", shard % aliases),
                    db: closed.db.clone(),
                })
                .collect::<Vec<_>>();
            let windows = export_windows(&routes);
            let actual = windows
                .iter()
                .flat_map(|w| w.routes.iter().cloned())
                .collect::<Vec<_>>();
            let expected = routes
                .iter()
                .map(|r| (r.run_shard, r.database_alias.clone()))
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
            for window in &windows {
                assert!(window.routes.len() <= 32 && window.groups.len() <= 4);
                assert!(window.groups.iter().all(|g| g.shards.len() <= 16));
                let aliases = window
                    .groups
                    .iter()
                    .map(|g| &g.database_alias)
                    .collect::<std::collections::BTreeSet<_>>();
                assert_eq!(
                    streams_for_window(window.groups.clone()).len(),
                    aliases.len()
                );
                let mut grouped = window
                    .groups
                    .iter()
                    .flat_map(|g| g.shards.iter().map(|s| (*s, g.database_alias.clone())))
                    .collect::<Vec<_>>();
                grouped.sort();
                assert_eq!(grouped, window.routes);
            }
            if aliases == 2 {
                assert!(
                    windows[0]
                        .groups
                        .iter()
                        .any(|g| g.shards.windows(2).any(|s| s[1] - s[0] > 1)),
                    "alternating routes must share physical readers"
                );
            }
        }
    }

    #[tokio::test]
    async fn empty_execution_batch_does_not_touch_the_route_database() {
        let route = closed_route().await;

        let batch = select_batch_for_executions(&route, Uuid::nil(), Vec::new())
            .await
            .unwrap();

        assert!(batch.executions.is_empty());
        assert!(batch.attempts.is_empty());
        assert!(batch.aggregates.is_empty());
        assert!(batch.evaluator_results.is_empty());
        assert!(batch.evaluator_diagnostics.is_empty());
        assert_eq!(route.run_shard, 7);
        assert_eq!(route.database_alias.as_str(), "unavailable");
    }

    #[tokio::test]
    async fn execution_page_propagates_an_unavailable_database() {
        let route = closed_route().await;

        let error = select_execution_batch(&route, Uuid::nil(), None, 100)
            .await
            .unwrap_err();

        assert!(matches!(
            error.downcast_ref::<sqlx::Error>(),
            Some(sqlx::Error::PoolClosed)
        ));
    }

    async fn closed_route() -> RunExportRoute {
        let db = PgPoolOptions::new()
            .connect_lazy("postgres://localhost/vigilo")
            .unwrap();
        db.close().await;
        RunExportRoute {
            run_shard: 7,
            database_alias: "unavailable".to_string(),
            db,
        }
    }
}
