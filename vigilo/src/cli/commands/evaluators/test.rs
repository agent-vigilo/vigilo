//! Local test CLI input/output; library operations own bounded isolated execution.
//! Input files are trusted CLI paths; the public test operation never opens paths.
use super::*;
pub(super) async fn exec(
    context: Context,
    evaluator: String,
    input: Option<String>,
    input_file: Option<PathBuf>,
) -> anyhow::Result<()> {
    use std::io::Read;
    let raw = match (input, input_file) {
        (Some(raw), None) => raw,
        (None, Some(path)) => {
            let file = fs::File::open(path)?;
            anyhow::ensure!(
                file.metadata()?.is_file(),
                "test input must be a regular file"
            );
            let mut raw = String::new();
            file.take(32 * 1024 * 1024 + 1).read_to_string(&mut raw)?;
            raw
        }
        _ => anyhow::bail!("exactly one of --input or --input-file must be provided"),
    };
    let input = crate::operations::evaluators::TestInput::from_json(&raw)?;
    let out = context.out().await?;
    let client = crate::operations::evaluators::EvaluatorClient::operator(
        context.clone(),
        DEFAULT_NAMESPACE,
    )
    .await?;
    let result = client
        .test(
            &crate::operations::evaluators::EvaluatorId::parse(&evaluator)?,
            input,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await?;
    out.write_value(&json!({"data":result}))?;
    Ok(())
}
