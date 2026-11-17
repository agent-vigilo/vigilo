//! Shared foreground orchestration; adapters own parsing paths and formatting.
//!
//! A validated source is admitted before storage. Each ledger call commits its
//! short transaction before I/O, and explicit cancellation attempts lease yield.
//! Publication/reuse keep the existing immutable identity and exact-key protocol.
use anyhow::{
    Context,
    Result,
    ensure,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::*;
use crate::dataset_ingestion as ingestion;

pub(crate) struct Published {
    pub catalog: ledger::Catalog,
    pub identity: DatasetIdentity,
    pub reused: bool,
}
impl Published {
    pub(crate) fn public(&self) -> Publication {
        Publication {
            reference: reference(&self.catalog, self.identity.clone()),
            reused: self.reused,
        }
    }
}
fn reference(catalog: &ledger::Catalog, dataset: DatasetIdentity) -> DatasetReference {
    DatasetReference {
        upload_id: catalog.id,
        dataset,
        manifest_digest: catalog.manifest.digest.clone(),
    }
}
pub(crate) fn error(error: anyhow::Error, id: Option<Uuid>, cancel: &CancellationToken) -> Error {
    if cancel.is_cancelled() {
        return Error::Cancelled;
    }
    if let Some(id) = id
        && error
            .downcast_ref::<sqlx::Error>()
            .is_some_and(|e| matches!(e, sqlx::Error::RowNotFound))
    {
        return Error::DatasetNotFound(id);
    }
    match error.downcast::<Error>() {
        Ok(e) => e,
        Err(e) => Error::Dependency(e),
    }
}
pub(crate) async fn inspect(
    db: Access<'_>,
    id: Uuid,
) -> Result<(ledger::Upload, Option<ledger::Catalog>)> {
    ensure!(!id.is_nil(), "dataset upload ID must be nonnil");
    db.inspect(id).await
}
pub(super) async fn status(db: Access<'_>, id: Uuid) -> Result<DatasetStatus, Error> {
    if id.is_nil() {
        return Err(Error::InvalidInput("dataset upload ID must be nonnil"));
    }
    let result = async {
        let (row, catalog) = inspect(db, id).await?;
        let status = match row.status.as_str() {
            "staging" => UploadStatus::Staging,
            "finalizing" => UploadStatus::Finalizing,
            "finalized" => UploadStatus::Finalized,
            "aborted" => UploadStatus::Aborted,
            "expired" => UploadStatus::Expired,
            _ => anyhow::bail!("invalid dataset status"),
        };
        let identity = DatasetIdentity::from(&row.identity.0);
        Ok(DatasetStatus {
            upload_id: id,
            status,
            dataset: identity.clone(),
            persisted_case_count: row.case_cursor,
            batch_count: row.batch_count,
            expires_at: row.expires_at,
            cleanup_after: row.cleanup_after,
            cleaned_at: row.cleaned_at,
            reference: catalog.map(|c| reference(&c, identity)),
        })
    }
    .await;
    result.map_err(|e| error(e, Some(id), &CancellationToken::new()))
}
pub(super) async fn abort(db: Access<'_>, id: Uuid) -> Result<Aborted, Error> {
    if id.is_nil() {
        return Err(Error::InvalidInput("dataset upload ID must be nonnil"));
    }
    ledger::abort(db, id)
        .await
        .map_err(|e| error(e, Some(id), &CancellationToken::new()))?;
    Ok(Aborted {
        upload_id: id,
        status: UploadStatus::Aborted,
    })
}
pub(crate) async fn upload(
    client: &DatasetClient,
    db: Access<'_>,
    prepared: PreparedDataset,
    upload_id: Option<Uuid>,
    cancel: &CancellationToken,
) -> Result<Published> {
    ensure!(!cancel.is_cancelled(), "dataset upload cancelled");
    ensure!(
        upload_id.is_none_or(|id| !id.is_nil()),
        "dataset upload ID must be nonnil"
    );
    client.owner.validate(&prepared.inner.limits)?;
    let binding = client.owner.binding_hash()?;
    let identity = DatasetIdentity::from(&prepared.inner.identity);
    if upload_id.is_none()
        && let Some(catalog) = ledger::reusable(
            db,
            client.owner.alias(),
            &binding,
            &prepared.inner.identity,
            &prepared.inner.limits,
        )
        .await?
    {
        return Ok(Published {
            catalog,
            identity,
            reused: true,
        });
    }
    let id = upload_id.unwrap_or_else(Uuid::now_v7);
    let result = async {
        let row = ledger::begin(
            db,
            id,
            client.owner.alias(),
            &binding,
            &prepared.inner.identity,
            &prepared.inner.limits,
        )
        .await?;
        if row.status == "finalized" {
            return ledger::catalog_for(db, id)
                .await?
                .context("finalized upload has no catalog reference");
        }
        let lease = ledger::claim(db, id, false).await?;
        let result = async {
            let owner = client.storage(&row, DatasetStoragePurpose::Upload)?;
            let store = ingestion::connect(db, &row, lease, &owner, false, cancel).await?;
            ingestion::ingest(db, store.as_ref(), &row, lease, &prepared.inner, cancel).await
        }
        .await;
        let release = ledger::release(db, lease).await;
        let catalog = result?;
        release?;
        Ok(catalog)
    }
    .await
    .with_context(|| {
        format!("dataset upload {id}; resume using this upload ID and the original input")
    })?;
    Ok(Published {
        catalog: result,
        identity,
        reused: false,
    })
}
pub(super) async fn cleanup(
    client: &DatasetClient,
    db: Access<'_>,
    id: Uuid,
    limit: u32,
    cancel: &CancellationToken,
) -> Result<bool> {
    ensure!(!cancel.is_cancelled(), "dataset cleanup cancelled");
    let row = db.load(id).await?;
    ensure!(
        client.owner.alias() == row.storage_alias
            && client.owner.binding_hash()? == row.binding_hash,
        "storage owner binding changed"
    );
    let lease = ledger::claim(db, id, true).await?;
    let result = async {
        let owner = client.storage(&row, DatasetStoragePurpose::Cleanup)?;
        let store = ingestion::connect(db, &row, lease, &owner, true, cancel).await?;
        ingestion::cleanup(db, store.as_ref(), lease, i64::from(limit), cancel).await
    }
    .await;
    let release = ledger::release(db, lease).await;
    let complete = result?;
    release?;
    Ok(complete)
}
