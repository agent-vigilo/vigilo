//! Evaluator inspection presentation; shared operations own bounded registry reads.
use super::*;
pub(super) async fn exec(context: Context, evaluator: String) -> anyhow::Result<()> {
    let out = context.out().await?;
    let client = crate::operations::evaluators::EvaluatorClient::operator(
        context.clone(),
        DEFAULT_NAMESPACE,
    )
    .await?;
    let id = crate::operations::evaluators::EvaluatorId::parse(&evaluator)?;
    out.write_value(&json!({"data":client.inspect(&id).await?}))?;
    Ok(())
}
