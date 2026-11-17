//! Typed foreground dataset operations shared by CLI and embedding hosts.
//!
//! Input is validated in bounded private spools before remote work. Host roots
//! bind storage and fixed user pools once, then derive caller-bound handles.
//! Ledger transactions are short and never span storage/credential I/O. Durable
//! leases, exact object records and quarantine preserve resume and cleanup after
//! cancellation or a dropped future. No interface formatting or signals install.
pub(crate) mod workflow;

use std::sync::Arc;

use chrono::{
    DateTime,
    Utc,
};
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    Error,
    UserClient,
    UserContext,
    resources::{
        DatasetRequest,
        DatasetStoragePurpose,
        Resolver,
    },
};
use crate::{
    blob_storage::config::OwnerConfig,
    contracts::case_storage::FormatLimits,
    dataset_ingestion::input,
    db::workflows::dataset_upload::{
        self as ledger,
        access::Access,
    },
};

/// Explicit input encoding. JSONL uses the documented versioned source header.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputFormat {
    Json,
    Yaml,
    Jsonl,
}
impl InputFormat {
    fn internal(self) -> input::InputFormat {
        match self {
            Self::Json => input::InputFormat::Json,
            Self::Yaml => input::InputFormat::Yaml,
            Self::Jsonl => input::InputFormat::Jsonl,
        }
    }
}
/// Physical file batching, independent of logical identity and execution chunks.
#[derive(Clone, Copy, Debug)]
pub struct Batching {
    cases: u32,
    bytes: u32,
}
impl Default for Batching {
    fn default() -> Self {
        Self {
            cases: 100,
            bytes: 8 * 1024 * 1024,
        }
    }
}
impl Batching {
    /// Accepts 1–65,536 cases and 256 bytes–64 MiB per stored batch.
    pub fn new(cases: u32, bytes: u32) -> Result<Self, Error> {
        let value = Self { cases, bytes };
        value.limits()?;
        Ok(value)
    }
    fn limits(self) -> Result<FormatLimits, Error> {
        let mut limits = FormatLimits::default();
        limits.batch.max_cases = self.cases as usize;
        limits.batch.max_bytes = self.bytes as usize;
        limits.resident_batch_bytes = limits
            .batch
            .max_bytes
            .saturating_mul(limits.resident_batches);
        limits
            .validate()
            .map_err(|_| Error::InvalidInput("invalid dataset batching limits"))?;
        Ok(limits)
    }
}
/// Verified ordered source identity; batching and storage locations are excluded.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct DatasetIdentity {
    pub dataset_id: Uuid,
    pub dataset_version: Option<String>,
    pub dataset_version_id: Uuid,
    pub dataset_hash: String,
    pub case_count: i32,
}
impl From<&crate::contracts::case_storage::identity::DatasetIdentity> for DatasetIdentity {
    fn from(value: &crate::contracts::case_storage::identity::DatasetIdentity) -> Self {
        Self {
            dataset_id: value.dataset_id,
            dataset_version: value.dataset_version.clone(),
            dataset_version_id: value.dataset_version_id,
            dataset_hash: value.dataset_hash.clone(),
            case_count: value.case_count,
        }
    }
}
/// Sealed preparation owns canonical records, duplicate-ID proof and admission.
/// Drop releases private scratch files and the process-wide preparation permit.
pub struct PreparedDataset {
    pub(crate) inner: input::Prepared,
}
impl PreparedDataset {
    /// Receives bounded bytes, validates the complete source and detects duplicate
    /// IDs before returning. JSON/YAML are capped at 8 MiB; JSONL at 8 GiB.
    /// The caller owns the overall deadline and cancellation. No database, storage
    /// credential, path or process environment is selected by the input stream.
    pub async fn prepare<R: tokio::io::AsyncRead + Unpin + Send>(
        reader: R,
        format: InputFormat,
        batching: Batching,
        cancel: &CancellationToken,
    ) -> Result<Self, Error> {
        let result =
            input::prepare_reader(reader, format.internal(), batching.limits()?, cancel).await;
        result.map(|inner| Self { inner }).map_err(|e| {
            if cancel.is_cancelled() {
                Error::Cancelled
            } else {
                Error::Dependency(e)
            }
        })
    }
    pub fn identity(&self) -> DatasetIdentity {
        (&self.inner.identity).into()
    }
    pub(crate) async fn file(
        path: std::path::PathBuf,
        format: input::InputFormat,
        limits: FormatLimits,
        cancel: &CancellationToken,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            inner: input::prepare(path, format, limits, cancel).await?,
        })
    }
}
/// Immutable reference usable by run preparation; it is not an access grant.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct DatasetReference {
    pub upload_id: Uuid,
    pub dataset: DatasetIdentity,
    pub manifest_digest: String,
}
/// Publication outcome; a reused reference performs no new blob writes.
#[derive(Clone, Debug, Serialize)]
pub struct Publication {
    pub reference: DatasetReference,
    pub reused: bool,
}
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum UploadStatus {
    Staging,
    Finalizing,
    Finalized,
    Aborted,
    Expired,
}
/// Compact live progress. Persisted cases are a contiguous acknowledged prefix;
/// a completed prefix does not imply final manifest publication.
#[derive(Clone, Debug, Serialize)]
pub struct DatasetStatus {
    pub upload_id: Uuid,
    pub status: UploadStatus,
    pub dataset: DatasetIdentity,
    pub persisted_case_count: i32,
    pub batch_count: i32,
    pub expires_at: DateTime<Utc>,
    pub cleanup_after: Option<DateTime<Utc>>,
    pub cleaned_at: Option<DateTime<Utc>>,
    pub reference: Option<DatasetReference>,
}
#[derive(Clone, Debug, Serialize)]
pub struct Aborted {
    pub upload_id: Uuid,
    pub status: UploadStatus,
}
#[derive(Clone, Debug, Serialize)]
pub struct Cleanup {
    pub upload_id: Uuid,
    pub cleanup_complete: bool,
}

#[derive(Clone)]
enum Database {
    Router(Arc<crate::context::database::DatabaseRouter>),
    Pool(sqlx::PgPool),
}
#[derive(Clone)]
struct HostStorage {
    resolver: Arc<dyn Resolver>,
    permits: Arc<tokio::sync::Semaphore>,
}
/// Trusted host factory sharing fixed-role pools and a host-owned storage binding.
/// Keep it at the authenticated host boundary; derive a caller handle per request.
#[derive(Clone)]
pub struct DatasetService {
    client: DatasetClient,
}
impl DatasetService {
    pub fn user(&self, caller: UserContext) -> DatasetClient {
        let mut client = self.client.clone();
        client.caller = Some(caller);
        client
    }
    pub(crate) fn with_permits(mut self, permits: Arc<tokio::sync::Semaphore>) -> Self {
        self.client
            .host
            .as_mut()
            .expect("host dataset factory")
            .permits = permits;
        self
    }
}
/// Foreground dataset commands with fixed storage and optional bound caller.
/// Clones share pools and admission; commands never change process configuration.
#[derive(Clone)]
pub struct DatasetClient {
    database: Database,
    owner: OwnerConfig,
    caller: Option<UserContext>,
    host: Option<HostStorage>,
}
impl DatasetClient {
    pub(crate) fn operator(pool: sqlx::PgPool, owner: OwnerConfig) -> Self {
        Self {
            database: Database::Pool(pool),
            owner,
            caller: None,
            host: None,
        }
    }
    async fn pool(&self) -> Result<&sqlx::PgPool, Error> {
        match &self.database {
            Database::Pool(p) => Ok(p),
            Database::Router(r) => r.control().await.map_err(Error::Dependency),
        }
    }
    fn access<'a>(&self, pool: &'a sqlx::PgPool) -> Access<'a> {
        match self.caller {
            Some(c) => Access::scoped(pool, c),
            None => pool.into(),
        }
    }
    fn storage(
        &self,
        row: &ledger::Upload,
        purpose: DatasetStoragePurpose,
    ) -> anyhow::Result<OwnerConfig> {
        anyhow::ensure!(
            self.owner.alias() == row.storage_alias
                && self.owner.binding_hash()? == row.binding_hash,
            "storage owner binding changed"
        );
        if let Some(host) = &self.host {
            let caller = self
                .caller
                .ok_or_else(|| anyhow::anyhow!("dataset caller is required"))?;
            anyhow::ensure!(caller.tenant_id() == row.tenant_id, "dataset owner changed");
            crate::context::resources::dataset_owner(
                &self.owner,
                host.resolver.clone(),
                host.permits.clone(),
                DatasetRequest::new(caller, row.id, row.binding_hash.clone(), purpose),
            )
        } else {
            Ok(self.owner.clone())
        }
    }
    /// Publish validated input or replay the exact upload ID. No transaction spans
    /// remote work. Cancellation yields the lease when possible; abrupt future
    /// drop leaves its bounded lease and registered keys available for recovery.
    pub async fn upload(
        &self,
        prepared: PreparedDataset,
        upload_id: Option<Uuid>,
        cancel: &CancellationToken,
    ) -> Result<Publication, Error> {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if upload_id.is_some_and(|id| id.is_nil()) {
            return Err(Error::InvalidInput("dataset upload ID must be nonnil"));
        }
        self.upload_inner(prepared, upload_id, cancel)
            .await
            .map(|p| p.public())
            .map_err(|e| workflow::error(e, upload_id, cancel))
    }
    pub(crate) async fn upload_inner(
        &self,
        prepared: PreparedDataset,
        id: Option<Uuid>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<workflow::Published> {
        let pool = self.pool().await?;
        // Keep the composed storage/SQL future out of every embedding caller’s
        // async frame; SDK polling also needs stack headroom on ordinary threads.
        Box::pin(workflow::upload(
            self,
            self.access(pool),
            prepared,
            id,
            cancel,
        ))
        .await
    }
    /// Rebuild the original source under its recorded batching policy, then replay
    /// only missing writes. Changed source or storage binding fails closed.
    pub async fn resume<R: tokio::io::AsyncRead + Unpin + Send>(
        &self,
        id: Uuid,
        reader: R,
        format: InputFormat,
        cancel: &CancellationToken,
    ) -> Result<Publication, Error> {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if id.is_nil() {
            return Err(Error::InvalidInput("dataset upload ID must be nonnil"));
        }
        let result = async {
            let pool = self.pool().await?;
            let row = self.access(pool).load(id).await?;
            self.owner.validate(&row.limits)?;
            anyhow::ensure!(
                self.owner.binding_hash()? == row.binding_hash,
                "storage owner binding changed"
            );
            let prepared = PreparedDataset {
                inner: input::prepare_reader(reader, format.internal(), row.limits.0, cancel)
                    .await?,
            };
            self.upload_inner(prepared, Some(id), cancel).await
        }
        .await;
        result
            .map(|p| p.public())
            .map_err(|e| workflow::error(e, Some(id), cancel))
    }
    pub(crate) async fn resume_file(
        &self,
        id: Uuid,
        path: std::path::PathBuf,
        format: input::InputFormat,
        cancel: &CancellationToken,
    ) -> anyhow::Result<workflow::Published> {
        let row = self.access(self.pool().await?).load(id).await?;
        self.owner.validate(&row.limits)?;
        let prepared = PreparedDataset::file(path, format, row.limits.0, cancel).await?;
        self.upload_inner(prepared, Some(id), cancel).await
    }
    /// Deletes at most 1–256 exact data keys plus bounded probe reconciliation.
    /// Cleanup requires an aborted/expired upload and the existing quarantine.
    pub async fn cleanup(
        &self,
        id: Uuid,
        limit: u32,
        cancel: &CancellationToken,
    ) -> Result<Cleanup, Error> {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if id.is_nil() || !(1..=256).contains(&limit) {
            return Err(Error::InvalidInput("invalid dataset cleanup ID or limit"));
        }
        let result = async {
            let pool = self.pool().await?;
            Box::pin(workflow::cleanup(
                self,
                self.access(pool),
                id,
                limit,
                cancel,
            ))
            .await
        }
        .await;
        result
            .map(|cleanup_complete| Cleanup {
                upload_id: id,
                cleanup_complete,
            })
            .map_err(|e| workflow::error(e, Some(id), cancel))
    }
}
fn owner(toml: &str, injected: bool) -> Result<OwnerConfig, Error> {
    if toml.len() > 64 * 1024 {
        return Err(Error::InvalidConfiguration("storage config exceeds 64 KiB"));
    }
    let owner: OwnerConfig = toml::from_str(toml)
        .map_err(|_| Error::InvalidConfiguration("invalid storage configuration"))?;
    owner
        .validate(&Default::default())
        .map_err(|_| Error::InvalidConfiguration("invalid storage binding"))?;
    if injected {
        owner
            .validate_injected()
            .map_err(|_| Error::InvalidConfiguration("host storage cannot use ambient bindings"))?;
    }
    Ok(owner)
}
impl super::Client {
    /// Bind operator-owned storage once, outside ordinary upload command input.
    pub fn datasets(&self, storage_toml: &str) -> Result<DatasetClient, Error> {
        Ok(DatasetClient {
            database: Database::Router(self.database.clone()),
            owner: owner(storage_toml, false)?,
            caller: None,
            host: None,
        })
    }
    pub async fn dataset_status(&self, id: Uuid) -> Result<DatasetStatus, Error> {
        if id.is_nil() {
            return Err(Error::InvalidInput("dataset upload ID must be nonnil"));
        }
        let pool = self.database.control().await.map_err(Error::Dependency)?;
        workflow::status(pool.into(), id).await
    }
    pub async fn abort_dataset_upload(&self, id: Uuid) -> Result<Aborted, Error> {
        if id.is_nil() {
            return Err(Error::InvalidInput("dataset upload ID must be nonnil"));
        }
        let pool = self.database.control().await.map_err(Error::Dependency)?;
        workflow::abort(pool.into(), id).await
    }
    /// Trusted maintenance only: expire one existing bounded page of uploads.
    pub async fn expire_dataset_uploads(&self) -> Result<Vec<Uuid>, Error> {
        let pool = self.database.control().await.map_err(Error::Dependency)?;
        ledger::expire(pool).await.map_err(Error::Dependency)
    }
}
impl UserClient {
    /// Host-only factory. The binding is nonsecret and credentials must come from
    /// the injected resolver. No operator pool or environment fallback is used.
    pub fn datasets(
        &self,
        storage_toml: &str,
        resolver: Arc<dyn Resolver>,
    ) -> Result<DatasetService, Error> {
        Ok(DatasetService {
            client: DatasetClient {
                database: Database::Pool(self.control.clone()),
                owner: owner(storage_toml, true)?,
                caller: None,
                host: Some(HostStorage {
                    resolver,
                    permits: Arc::new(tokio::sync::Semaphore::new(64)),
                }),
            },
        })
    }
    pub async fn dataset_status(
        &self,
        caller: UserContext,
        id: Uuid,
    ) -> Result<DatasetStatus, Error> {
        workflow::status(Access::scoped(&self.control, caller), id).await
    }
    pub async fn abort_dataset_upload(
        &self,
        caller: UserContext,
        id: Uuid,
    ) -> Result<Aborted, Error> {
        workflow::abort(Access::scoped(&self.control, caller), id).await
    }
}

#[cfg(test)]
mod tests;
