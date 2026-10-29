//! Bounded inline/file admission through immutable dataset ingestion.
//!
//! Existing YAML/JSON inputs retain their logical identities. A private bounded
//! spool feeds the same resumable ingestion and plan workflow as finalized
//! dataset references; no alternate SQL case-materialization path exists here.
use anyhow::Context as _;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::{
    blob_storage::config::OwnerConfig,
    contracts::case_storage::FormatLimits,
    dataset_ingestion::{
        self,
        input::{
            InputFormat,
            prepare,
        },
    },
    db::workflows::dataset_upload,
};

#[allow(clippy::too_many_arguments)]
pub(super) async fn exec(
    context: Context,
    config: crate::db::workflows::run_creation::Config,
    profile: Option<String>,
    profile_file: Option<PathBuf>,
    dataset: Option<String>,
    dataset_file: Option<PathBuf>,
    storage_config: PathBuf,
    run_id: Option<Uuid>,
    prepare_only: bool,
) -> anyhow::Result<()> {
    let id = run_id.unwrap_or_else(Uuid::now_v7);
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    let signal = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            token.cancel();
        }
    });
    let result = async {
        let owner = OwnerConfig::load(&storage_config)?;
        let limits = FormatLimits::default();
        owner.validate(&limits)?;
        let db = context.dbr().await?.control().await?;
        // Validate bounded profile/evaluator metadata before uploading input.
        let payload = create_blob::profile_input(profile, profile_file)?;
        let _: RunProfile = serde_json::from_value(payload.clone())?;
        let inline = match dataset {
            Some(raw) => {
                anyhow::ensure!(raw.len() <= 8 * 1024 * 1024, "dataset exceeds 8 MiB");
                use std::io::Write;
                let mut file = tempfile::NamedTempFile::new()?;
                file.write_all(raw.as_bytes())?;
                Some(file)
            }
            None => None,
        };
        let path = dataset_file
            .or_else(|| inline.as_ref().map(|f| f.path().to_owned()))
            .context("dataset input is required")?;
        let format = if path.extension().is_some_and(|ext| ext == "jsonl") {
            InputFormat::Jsonl
        } else {
            InputFormat::Yaml
        };
        let prepared = prepare(path, format, limits, &cancel).await?;
        let binding = owner.binding_hash()?;
        let upload_id = identity_upload_id(id)?;
        let existing = crate::db::workflows::run_plan::load(db, id).await?;
        let catalog = if let Some(plan) = &existing {
            dataset_upload::catalog(db, plan.dataset_upload_id)
                .await?
                .context("run dataset is not finalized")?
        } else if let Some(row) = dataset_upload::catalog(db, upload_id).await? {
            row
        } else if let Some(row) = dataset_upload::reusable(
            db,
            owner.alias(),
            &binding,
            &prepared.identity,
            &prepared.limits,
        )
        .await?
        {
            row
        } else {
            let upload = dataset_upload::begin(
                db,
                upload_id,
                owner.alias(),
                &binding,
                &prepared.identity,
                &prepared.limits,
            )
            .await?;
            let lease = dataset_upload::claim(db, upload_id, false).await?;
            let result = async {
                let store =
                    dataset_ingestion::connect(db, &upload, lease, &owner, false, &cancel).await?;
                dataset_ingestion::ingest(db, store.as_ref(), &upload, lease, &prepared, &cancel)
                    .await
            }
            .await;
            let release = dataset_upload::release(db, lease).await;
            let catalog = result?;
            release?;
            catalog
        };
        let upload = dataset_upload::load(db, catalog.id).await?;
        anyhow::ensure!(
            upload.identity.0 == prepared.identity,
            "dataset changed while resuming run"
        );
        let inputs = create_blob::Preparation::new(payload, upload, catalog, existing)?;
        create_blob::prepare(
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
    .with_context(|| format!("run {id}; retry with --run-id {id} and the same inputs"));
    signal.abort();
    result
}
fn identity_upload_id(run_id: Uuid) -> anyhow::Result<Uuid> {
    uuid_from_json(&json!({"run_id":run_id,"purpose":"dataset-upload"}))
}
