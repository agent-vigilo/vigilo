//! Run status command implementation.
//!
//! Returns a single run progress snapshot using the same payload shape as
//! watch updates. This command should avoid side effects and should validate
//! the run id before initializing database-backed command work.

use super::*;

pub(super) async fn exec(context: Context, run_id: String) -> anyhow::Result<()> {
    let run_id = parse_run_id(&run_id)?;
    let database_router = context.dbr().await?;
    let out = context.out().await?;
    let status = crate::operations::status::read(database_router, run_id).await?;
    let payload = super::status_payload(&status);

    out.write_value(&payload)?;
    Ok(())
}
