//! Evaluator search presentation; the library owns bounded metadata-only queries.
use super::*;
pub(super) async fn exec(
    context: Context,
    namespace: String,
    limit: i64,
    query: Option<String>,
) -> anyhow::Result<()> {
    let out = context.out().await?;
    let client = crate::operations::evaluators::EvaluatorClient::operator(
        context.clone(),
        DEFAULT_NAMESPACE,
    )
    .await?;
    let mut values = Vec::new();
    let mut cursor = None;
    loop {
        let page = client
            .search(
                &namespace,
                query.as_deref(),
                crate::operations::evaluators::PageLimits::new(
                    (limit as usize - values.len()) as u32,
                    8 * 1024 * 1024,
                )?,
                cursor.as_ref(),
            )
            .await?;
        values.extend(page.items.into_iter().map(|e|json!({"namespace":e.namespace,"name":e.name,"version":e.version,"description":e.description,"tags":e.tags,"metadata":e.metadata,"state":e.state,"state_reason":e.state_reason})));
        cursor = page.next;
        if values.len() == limit as usize || cursor.is_none() {
            break;
        }
    }
    out.write_value(&json!({"data":values,"meta":{"namespace":namespace,"query":query,"limit":limit,"count":values.len()}}))?;
    Ok(())
}
