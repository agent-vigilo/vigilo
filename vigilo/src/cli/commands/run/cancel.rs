//! Run cancellation command implementation.
//!
//! Cancels an active run through the workflow layer and reports the terminal
//! run snapshot plus counts of closed chunks, executions, attempts, and outbox
//! events. Run identifiers must be UUIDs; cancellation behavior must remain
//! idempotent and workflow-owned.

use super::*;

#[cfg(test)]
pub(super) fn run_cancel_payload(outcome: &run_cancel::CancelRunOutcome) -> Value {
    serde_json::to_value(crate::operations::Cancellation::from(outcome.clone()))
        .expect("JSON projection")
}

pub(super) async fn exec(context: Context, run_id: String) -> anyhow::Result<()> {
    let run_id = parse_run_id(&run_id)?;
    let database_router = context.dbr().await?;
    let out = context.out().await?;
    let outcome = crate::operations::cancellation::cancel(database_router, run_id).await?;
    out.write_value(&serde_json::to_value(outcome)?)?;
    Ok(())
}
