//! Run results summary command implementation.
//!
//! Loads a run and aggregate result counts without materializing every
//! execution. This module is for summary-only inspection; detailed execution,
//! attempt, aggregate, and evaluator-result data belongs in `run::export`.

use super::*;

pub(super) fn run_results_payload(
    run: &Run,
    summary: &RunResultsSummary,
    scorecard: Option<&Value>,
) -> Value {
    json!({
        "data": {
            "run": {
                "run_id": run.id,
                "run_key": run.run_key,
                "status": run.status,
                "gate_status": run.gate_status,
                "expected_execution_count": run.expected_execution_count,
                "terminal_execution_count": run.terminal_execution_count,
                "passed_execution_count": run.passed_execution_count,
                "failed_execution_count": run.failed_execution_count,
                "errored_execution_count": run.errored_execution_count,
                "summary": run.summary,
                "error_message": run.error_message,
                "created_at": run.created_at,
                "completed_at": run.completed_at,
                "updated_at": run.updated_at,
            },
            "results": {
                "execution_count": summary.execution_count,
                "aggregate_count": summary.aggregate_count,
                "missing_aggregate_count": summary.missing_aggregate_count,
                "missing_execution_count": summary.missing_execution_count,
                "unresolved_execution_count": summary.unresolved_execution_count,
                "planned_chunk_count": summary.planned_chunk_count,
                "settled_chunk_count": summary.settled_chunk_count,
                "sealed_shard_count": summary.sealed_shard_count,
                "status_counts": {
                    "passed": summary.passed_execution_count,
                    "failed": summary.failed_execution_count,
                    "error": summary.error_execution_count,
                    "skipped": summary.skipped_execution_count,
                },
                "score": {
                    "average": summary.average_score,
                    "min": summary.min_score,
                    "max": summary.max_score,
                },
                "evaluator_result_count": summary.evaluator_result_count,
                "blocking_failure_count": summary.blocking_failure_count,
                "scorecard": scorecard,
            },
        },
        "meta": {
            "summary_only": true,
        }
    })
}

pub(super) async fn exec(context: Context, run_id: String) -> anyhow::Result<()> {
    let run_id = parse_run_id(&run_id)?;
    let database_router = context.dbr().await?;
    let db = database_router.control().await?;
    let out = context.out().await?;
    let run = select_existing_run(db, run_id).await?;
    let summary = run_results_workflow::select_run_results_summary(database_router, run_id).await?;
    let scorecard = run_results_workflow::select_run_scorecard(db, run_id).await?;
    let payload = run_results_payload(&run, &summary, scorecard.as_ref());

    out.write_value(&payload)?;
    Ok(())
}
