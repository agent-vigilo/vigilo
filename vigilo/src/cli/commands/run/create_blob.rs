//! Dataset-reference run preparation at the CLI boundary.
//!
//! Reads bounded input files and adapts signals/output around shared run operations.
//! Those operations freeze evaluator artifacts and run identity before publication.
//! The caller retries with the same run ID and profile after interruption; no
//! background process needs access to its local configuration file. Preparation
//! is explicit and non-dispatchable until execution admission verifies placements.
use anyhow::{
    Context as _,
    ensure,
};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::db::workflows::dataset_upload;
pub(crate) use crate::operations::creation::Preparation;

/// A bounded profile source; input configuration cannot become a dataset-sized
/// allocation merely because a wrong file was supplied to plan preparation.
pub(super) fn profile_input(
    inline: Option<String>,
    file: Option<PathBuf>,
) -> anyhow::Result<Value> {
    use std::io::Read;
    let raw = match (inline, file) {
        (Some(raw), None) => raw,
        (None, Some(path)) => {
            let mut bytes = Vec::new();
            std::fs::File::open(path)?
                .take(8 * 1024 * 1024 + 1)
                .read_to_end(&mut bytes)?;
            String::from_utf8(bytes)?
        }
        _ => anyhow::bail!("exactly one profile source is required"),
    };
    ensure!(raw.len() <= 8 * 1024 * 1024, "profile exceeds 8 MiB");
    let value = canonical_json(&crate::dataset_ingestion::input::yaml(raw.as_bytes())?);
    ensure!(
        serde_json::to_vec(&value)?.len() <= 8 * 1024 * 1024,
        "profile exceeds encoded 8 MiB limit"
    );
    Ok(value)
}
#[allow(clippy::too_many_arguments)]
pub(super) async fn exec(
    context: Context,
    config: crate::db::workflows::run_creation::Config,
    profile: Option<String>,
    profile_file: Option<PathBuf>,
    upload_id: Uuid,
    storage_config: PathBuf,
    run_id: Option<Uuid>,
    prepare_only: bool,
) -> anyhow::Result<()> {
    let id = run_id.unwrap_or_else(Uuid::now_v7);
    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    let task = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal.cancel();
        }
    });
    let result = async {
        let payload = profile_input(profile, profile_file)?;
        let _: RunProfile = serde_json::from_value(payload.clone())?;
        let db = context.dbr().await?.control().await?;
        let upload = dataset_upload::load(db, upload_id).await?;
        let catalog = dataset_upload::catalog(db, upload_id)
            .await?
            .context("dataset is not finalized")?;
        let inputs = Preparation::new(payload, upload, catalog, None)?;
        prepare(
            context,
            config,
            inputs,
            storage_config,
            id,
            prepare_only,
            &cancel,
        )
        .await
    }
    .await
    .with_context(|| format!("run plan {id}; retry with --run-id {id} and the same inputs"));
    task.abort();
    result
}
/// Adapts existing ingestion inputs to the shared library workflow.
pub(super) async fn prepare(
    context: Context,
    config: crate::db::workflows::run_creation::Config,
    inputs: Preparation,
    storage_config: PathBuf,
    id: Uuid,
    prepare_only: bool,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let owner = super::super::dataset::owner(&storage_config)?;
    let client = crate::operations::RunClient::from_context(context.clone(), owner, config, None);
    let prepared = client.prepare_inputs(inputs, id, cancel).await?;
    let result = if prepare_only {
        client.describe(&prepared).await?
    } else {
        client.accept_run(prepared, cancel).await?
    };
    context
        .out()
        .await?
        .write_value(&serde_json::to_value(result)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;
    use crate::cli::App;
    #[test]
    fn blob_run_sources_require_owner_and_allow_optional_preparation() {
        let id = "018f1111-1111-7111-8111-111111111111";
        let base = [
            "vigilo",
            "--database-url",
            "postgresql://unused",
            "run",
            "create",
            "--profile",
            "{}",
            "--dataset-upload-id",
            id,
            "--storage-config",
            "storage.toml",
            "--prepare-only",
        ];
        assert!(App::try_parse_from(base).is_ok());
        assert!(App::try_parse_from(&base[..base.len() - 1]).is_ok());
        assert!(
            App::try_parse_from([
                "vigilo",
                "--database-url",
                "postgresql://unused",
                "run",
                "create",
                "--profile",
                "{}",
                "--dataset",
                "{}",
                "--storage-config",
                "storage.toml"
            ])
            .is_ok()
        );
        let mut conflicting = base.to_vec();
        conflicting.extend(["--dataset", "{}"]);
        assert!(App::try_parse_from(conflicting).is_err());
        assert!(
            App::try_parse_from([
                "vigilo",
                "--database-url",
                "postgresql://unused",
                "run",
                "create",
                "--profile",
                "{}",
                "--dataset",
                "{}",
                "--prepare-only"
            ])
            .is_err()
        );
        assert!(
            App::try_parse_from([
                "vigilo",
                "--database-url",
                "postgresql://unused",
                "run",
                "create",
                "--profile",
                "{}",
                "--dataset-upload-id",
                id,
                "--prepare-only"
            ])
            .is_err()
        );
    }
    #[test]
    fn bounded_profile_source_rejects_missing_conflicting_and_oversized_inputs() {
        assert!(profile_input(None, None).is_err());
        assert!(profile_input(Some("{}".into()), Some("missing".into())).is_err());
        assert!(profile_input(Some(" ".repeat(8 * 1024 * 1024 + 1)), None).is_err());
        assert!(profile_input(Some("not: [valid".into()), None).is_err());
        assert!(profile_input(Some("duplicate: 1\nduplicate: 2".into()), None).is_err());
        assert_eq!(
            profile_input(Some("b: 2\na: 1".into()), None).unwrap(),
            json!({"a":1,"b":2})
        );
    }
}
