//! State-change CLI adapter; the shared workflow enforces terminal removal.
use super::*;
pub(super) async fn exec(
    context: Context,
    evaluator: String,
    state: EvaluatorState,
    state_reason: Option<String>,
) -> anyhow::Result<()> {
    let client =
        crate::operations::evaluators::EvaluatorClient::operator(context, DEFAULT_NAMESPACE)
            .await?;
    client
        .set_state(
            &crate::operations::evaluators::EvaluatorId::parse(&evaluator)?,
            state.into(),
            state_reason,
        )
        .await?;
    Ok(())
}
