//! Typed cancellation projection shared by CLI and embedding hosts.
//! The workflow owns durable transitions and routed cleanup; interfaces own
//! authentication and rendering. Persistence rows never cross the public boundary.
use chrono::{
    DateTime,
    Utc,
};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use super::Error;
use crate::{
    context::database::DatabaseRouter,
    db::workflows::run_cancel,
};

#[derive(Clone, Debug, Serialize)]
pub struct CancellationDetails {
    pub run_id: Uuid,
    pub run_key: String,
    pub status: String,
    pub gate_status: String,
    pub expected_execution_count: i32,
    pub terminal_execution_count: i32,
    pub passed_execution_count: i32,
    pub failed_execution_count: i32,
    pub errored_execution_count: i32,
    pub summary: Value,
    pub error_message: Option<String>,
    pub completed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}
#[derive(Clone, Debug, Serialize)]
pub struct CancellationMetadata {
    pub cancelled: bool,
    pub already_cancelled: bool,
    pub terminal: bool,
    pub chunks_cancelled: i64,
    pub executions_cancelled: i64,
    pub attempts_cancelled: i64,
    pub outbox_events_enqueued: i64,
}
/// Cancellation observation. A nonterminal response leaves durable cleanup for
/// coordinator retry; a database acknowledgment alone is not final settlement.
#[derive(Clone, Debug, Serialize)]
pub struct Cancellation {
    pub data: CancellationDetails,
    pub meta: CancellationMetadata,
}
impl From<run_cancel::CancelRunOutcome> for Cancellation {
    fn from(o: run_cancel::CancelRunOutcome) -> Self {
        let r = o.run;
        Self {
            data: CancellationDetails {
                run_id: r.id,
                run_key: r.run_key,
                status: r.status,
                gate_status: r.gate_status,
                expected_execution_count: r.expected_execution_count,
                terminal_execution_count: r.terminal_execution_count,
                passed_execution_count: r.passed_execution_count,
                failed_execution_count: r.failed_execution_count,
                errored_execution_count: r.errored_execution_count,
                summary: r.summary,
                error_message: r.error_message,
                completed_at: r.completed_at,
                updated_at: r.updated_at,
            },
            meta: CancellationMetadata {
                cancelled: o.cancelled,
                already_cancelled: o.already_cancelled,
                terminal: true,
                chunks_cancelled: o.chunks_cancelled,
                executions_cancelled: o.executions_cancelled,
                attempts_cancelled: o.attempts_cancelled,
                outbox_events_enqueued: o.outbox_events_enqueued,
            },
        }
    }
}
pub(crate) async fn cancel(database: &DatabaseRouter, id: Uuid) -> Result<Cancellation, Error> {
    let pool = database.control().await.map_err(Error::Dependency)?;
    let before = crate::db::tables::runs::select_run_by_id(pool, id)
        .await
        .map_err(Error::Dependency)?
        .ok_or(Error::NotFound(id))?;
    if matches!(before.status.as_str(), "completed" | "failed" | "cancelled") {
        return run_cancel::cancel_run_routed(database, id)
            .await
            .map_err(Error::Dependency)?
            .map(Into::into)
            .ok_or(Error::NotFound(id));
    }
    crate::db::workflows::run_control::request(pool, id, super::ExecutionControl::Revoked)
        .await
        .map_err(Error::Dependency)?;
    if let Some(outcome) = crate::db::workflows::run_control::deliver(database, Some(id))
        .await
        .map_err(Error::Dependency)?
        .into_iter()
        .find(|outcome| outcome.run.id == id)
    {
        return Ok(outcome.into());
    }
    let run = crate::db::tables::runs::select_run_by_id(pool, id)
        .await
        .map_err(Error::Dependency)?
        .ok_or(Error::NotFound(id))?;
    let terminal = false; // Only a returned settlement outcome confirms cleanup here.
    let mut result = Cancellation::from(run_cancel::CancelRunOutcome {
        run,
        cancelled: terminal,
        already_cancelled: false,
        chunks_cancelled: 0,
        executions_cancelled: 0,
        attempts_cancelled: 0,
        outbox_events_enqueued: 0,
    });
    result.meta.terminal = terminal;
    Ok(result)
}
