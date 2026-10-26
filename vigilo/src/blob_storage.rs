//! Bounded immutable object I/O, independent of provider and ingestion policy.
//!
//! A publishing store proves conditional creation and immediate visibility using
//! an exactly owned probe. Runtime readers connect without write probes. Each operation shares admission and disk reservations,
//! validates references before I/O, and stages bytes in private RAII spools.
//! Only fully hashed files escape this boundary. Provider retries are disabled;
//! one request counter and deadline cover admission, transfer and reconciliation.
//! Publication leases and durable orphan ownership belong to ingestion, not here.

#![allow(dead_code)] // Runtime ingestion consumes this boundary independently.

use std::{
    future::Future,
    path::Path,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use tokio::{
    io::{
        AsyncRead,
        AsyncReadExt,
        AsyncWriteExt,
    },
    sync::{
        OwnedSemaphorePermit,
        Semaphore,
    },
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use crate::contracts::case_storage::{
    FormatLimits,
    ObjectRef,
    StorageFormat,
};

#[path = "blob_storage/config.rs"]
pub(crate) mod config;
#[path = "blob_storage/s3.rs"]
pub(crate) mod s3;
#[cfg(test)]
#[path = "blob_storage/tests.rs"]
mod tests;

pub(crate) const DEFAULT_OPERATION_SECONDS: u64 = 120;

type Result<T> = std::result::Result<T, StorageError>;
pub(crate) type Reader = Box<dyn AsyncRead + Unpin + Send>;

/// Stable categories; provider response bodies and signed URLs never enter errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ErrorKind {
    Invalid,
    Missing,
    Unauthorized,
    Conflict,
    Corrupt,
    Unsupported,
    Transient,
    Timeout,
    Cancelled,
    Exhausted,
    Local,
}

/// Exact operation/owner/key context, safe provider status, and write uncertainty.
/// An uncertain key remains owned reconciliation work; it is never blindly deleted.
#[derive(Debug, thiserror::Error)]
#[error("{operation} {alias}/{key}: {kind:?} (status {status:?}, uncertain create: {uncertain})")]
pub(crate) struct StorageError {
    pub kind: ErrorKind,
    pub operation: &'static str,
    pub alias: String,
    pub key: String,
    pub status: Option<u16>,
    pub uncertain: bool,
    #[source]
    pub source: Option<std::io::Error>,
}
impl StorageError {
    fn new(kind: ErrorKind) -> Self {
        Self {
            kind,
            operation: "storage",
            alias: String::new(),
            key: String::new(),
            status: None,
            uncertain: false,
            source: None,
        }
    }
    fn io(error: std::io::Error) -> Self {
        let mut e = Self::new(ErrorKind::Local);
        e.source = Some(error);
        e
    }
}

/// Finite total budget, including queueing, staging, retries and verification.
#[derive(Clone, Copy)]
pub(crate) struct Budget {
    pub deadline: Instant,
    pub requests: u32,
    pub no_progress: Duration,
}
impl Budget {
    pub fn standard() -> Self {
        Self {
            deadline: Instant::now() + Duration::from_secs(DEFAULT_OPERATION_SECONDS),
            requests: 3,
            no_progress: Duration::from_secs(15),
        }
    }
    fn validate(&self) -> Result<()> {
        if self.requests == 0
            || self.requests > 32
            || self.no_progress.is_zero()
            || self.no_progress > Duration::from_secs(120)
            || self.deadline <= Instant::now()
            || self.deadline.saturating_duration_since(Instant::now()) > Duration::from_secs(600)
        {
            return Err(StorageError::new(ErrorKind::Invalid));
        }
        Ok(())
    }
}

/// Share one instance across aliases in a process; verified spools retain their
/// byte reservation until dropped. No dataset-size-dependent memory allocation.
pub(crate) struct Resources {
    streams: Arc<Semaphore>,
    disk: Arc<Semaphore>,
    buffer: usize,
    capacity: u32,
}
impl Resources {
    /// Admit 1–64 concurrent operations, up to 1 GiB of shared spool bytes and
    /// 1 KiB–1 MiB buffers. Invalid limits fail before allocation or remote I/O.
    pub fn new(concurrency: usize, spool_bytes: u32, buffer: usize) -> Result<Arc<Self>> {
        if !(1..=64).contains(&concurrency)
            || !(1024..=1024 * 1024).contains(&buffer)
            || spool_bytes == 0
            || spool_bytes > 1024 * 1024 * 1024
        {
            return Err(StorageError::new(ErrorKind::Invalid));
        }
        Ok(Arc::new(Self {
            streams: Arc::new(Semaphore::new(concurrency)),
            disk: Arc::new(Semaphore::new(spool_bytes as usize)),
            buffer,
            capacity: spool_bytes,
        }))
    }
    pub fn standard() -> Arc<Self> {
        Self::new(4, 256 * 1024 * 1024, 256 * 1024).expect("valid defaults")
    }
}

/// Verified bytes live on disk; callers explicitly decode and validate content.
/// The private path cannot be handed out for mutation. Dropping removes the file
/// and releases the shared byte reservation, including on cancellation/unwind.
pub(crate) struct VerifiedFile {
    file: tempfile::NamedTempFile,
    reference: ObjectRef,
    _disk: OwnedSemaphorePermit,
}
impl VerifiedFile {
    pub fn reference(&self) -> &ObjectRef {
        &self.reference
    }
    pub async fn reader(&self) -> Result<impl AsyncRead + Unpin + Send + '_> {
        tokio::fs::File::open(self.file.path())
            .await
            .map_err(StorageError::io)
    }
}

/// Provider-neutral application boundary. Dropping the future or cancelling the
/// token aborts active work; uncertain creates retain exact ownership in errors.
#[allow(
    clippy::double_must_use,
    reason = "async_trait adds must_use to its already-must-use boxed futures"
)]
#[async_trait]
pub(crate) trait BlobStorage: Send + Sync {
    async fn create(
        &self,
        reference: &ObjectRef,
        input: Reader,
        budget: Budget,
        cancel: &CancellationToken,
    ) -> Result<ObjectRef>;
    async fn read(
        &self,
        reference: &ObjectRef,
        budget: Budget,
        cancel: &CancellationToken,
    ) -> Result<VerifiedFile>;
    /// Delete only an exact, durably owned unpublished object. The caller holds
    /// cleanup authority and proves that no admitted writer remains in flight.
    /// Repeated deletion is safe; this API never lists a bucket or prefix.
    async fn remove_owned(
        &self,
        reference: &ObjectRef,
        budget: Budget,
        cancel: &CancellationToken,
    ) -> Result<()>;
}

/// A narrow provider transport. Only this layer sees SDK types. Every method
/// performs exactly one request; callers own all retries and cleanup decisions.
#[allow(
    clippy::double_must_use,
    reason = "async_trait adds must_use to its already-must-use boxed futures"
)]
#[async_trait]
pub(crate) trait Transport: Send + Sync {
    async fn put(
        &self,
        reference: &ObjectRef,
        path: &Path,
        buffer: usize,
        no_progress: Duration,
    ) -> Result<()>;
    async fn get(&self, reference: &ObjectRef, no_progress: Duration) -> Result<Download>;
    async fn delete_object(&self, key: &str) -> Result<()>;
}
pub(crate) struct Download {
    pub reader: Reader,
    pub length: u64,
    pub revision: Option<String>,
}

pub(crate) struct Store<T> {
    transport: T,
    alias: String,
    limits: FormatLimits,
    resources: Arc<Resources>,
}

struct Operation<'a> {
    budget: Budget,
    cancel: &'a CancellationToken,
    remaining: u32,
    retry: u32,
}
impl<'a> Operation<'a> {
    fn new(budget: Budget, cancel: &'a CancellationToken) -> Result<Self> {
        budget.validate()?;
        Ok(Self {
            budget,
            cancel,
            remaining: budget.requests,
            retry: 0,
        })
    }
    async fn wait<V>(&self, future: impl Future<Output = Result<V>>) -> Result<V> {
        tokio::select! { biased;
            _ = self.cancel.cancelled() => Err(StorageError::new(ErrorKind::Cancelled)),
            _ = tokio::time::sleep_until(self.budget.deadline) => Err(StorageError::new(ErrorKind::Timeout)),
            result = future => result,
        }
    }
    async fn request<V>(&mut self, future: impl Future<Output = Result<V>>) -> Result<V> {
        if self.remaining == 0 {
            return Err(StorageError::new(ErrorKind::Exhausted));
        }
        self.remaining -= 1;
        let start = Instant::now();
        let result = self.wait(future).await;
        tracing::debug!(target: "vigilo::blob_storage", requests_remaining=self.remaining, elapsed_ms=start.elapsed().as_millis(), error_kind=?result.as_ref().err().map(|e| e.kind), "object request");
        result
    }
    async fn backoff(&mut self) -> Result<()> {
        if self.remaining == 0 {
            return Err(StorageError::new(ErrorKind::Exhausted));
        }
        let cap = (250_u64 << self.retry.min(3)).min(2000);
        self.retry += 1;
        let delay = Duration::from_millis(rand::random_range(0..=cap));
        self.wait(async {
            tokio::time::sleep(delay).await;
            Ok(())
        })
        .await
    }
}

impl<T: Transport> Store<T> {
    /// Prove required endpoint behavior before admitting application writes.
    /// The caller records its unique probe key before calling, retaining cleanup
    /// ownership even if this future is dropped or the process stops. A conflict
    /// on the initial create never authorizes deletion of the existing object.
    /// Probe cleanup has a separate bounded deadline and never scans a prefix.
    pub async fn connect(
        transport: T,
        alias: String,
        limits: FormatLimits,
        resources: Arc<Resources>,
        budget: Budget,
        cancel: &CancellationToken,
        probe_key: String,
    ) -> Result<Self> {
        let store = Self {
            transport,
            alias,
            limits,
            resources,
        };
        let key = probe_key;
        if !key.starts_with("capability-probes/")
            || store.resources.capacity as usize > store.limits.staging_spool_bytes
        {
            return Err(StorageError::new(ErrorKind::Invalid));
        }
        let reference = ObjectRef::for_bytes(
            store.alias.clone(),
            key.clone(),
            StorageFormat::DatasetManifest,
            b"probe-a",
            &store.limits,
        )
        .map_err(|_| StorageError::new(ErrorKind::Invalid))?;
        store.validate(&reference)?;
        let mut op = Operation::new(budget, cancel)?;
        // Admission covers the probe just like application traffic.
        let _slot = op
            .wait(async {
                store
                    .resources
                    .streams
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| StorageError::new(ErrorKind::Local))
            })
            .await?;
        let mut touched = false;
        let result = async {
            let file = store
                .stage(&reference, Box::new(&b"probe-a"[..]), &op)
                .await?;
            touched = true;
            if let Err(e) = op
                .request(store.transport.put(
                    &reference,
                    file.file.path(),
                    store.resources.buffer,
                    op.budget.no_progress,
                ))
                .await
            {
                if matches!(
                    e.kind,
                    ErrorKind::Conflict
                        | ErrorKind::Unauthorized
                        | ErrorKind::Unsupported
                        | ErrorKind::Invalid
                        | ErrorKind::Local
                ) {
                    touched = false;
                }
                return Err(e);
            }
            drop(file);
            let mut second = reference.clone();
            second.digest = blake3::hash(b"probe-b").to_hex().to_string();
            let different = store.stage(&second, Box::new(&b"probe-b"[..]), &op).await?;
            match op
                .request(store.transport.put(
                    &second,
                    different.file.path(),
                    store.resources.buffer,
                    op.budget.no_progress,
                ))
                .await
            {
                Err(e) if e.kind == ErrorKind::Conflict => {}
                Err(e) => return Err(e),
                Ok(()) => return Err(StorageError::new(ErrorKind::Unsupported)),
            }
            drop(different);
            let download = op
                .request(store.transport.get(&reference, op.budget.no_progress))
                .await?;
            store
                .verify_download(&reference, download, None, &op)
                .await?;
            Ok(())
        }
        .await;
        if touched {
            // Even cancellation cannot skip exact probe cleanup. A failure names
            // the one key the operator must reconcile; no live data is deleted.
            let cleanup =
                tokio::time::timeout(Duration::from_secs(30), store.transport.delete_object(&key))
                    .await;
            match cleanup {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(store.context(e, "cleanup probe", &reference, true)),
                Err(_) => {
                    return Err(store.context(
                        StorageError::new(ErrorKind::Timeout),
                        "cleanup probe",
                        &reference,
                        true,
                    ));
                }
            }
        }
        result.map_err(|e| store.context(e, "validate endpoint", &reference, false))?;
        Ok(store)
    }
    fn context(
        &self,
        mut e: StorageError,
        operation: &'static str,
        reference: &ObjectRef,
        uncertain: bool,
    ) -> StorageError {
        e.operation = operation;
        e.alias = if e.kind == ErrorKind::Invalid {
            "<invalid owner>".into()
        } else {
            self.alias.clone()
        };
        // Invalid caller keys can contain secrets/URLs; do not echo them.
        e.key = if e.kind == ErrorKind::Invalid {
            "<invalid reference>".into()
        } else {
            reference.key.clone()
        };
        e.uncertain |= uncertain;
        e
    }
    fn validate(&self, reference: &ObjectRef) -> Result<()> {
        reference
            .validate(&self.limits)
            .map_err(|_| StorageError::new(ErrorKind::Invalid))?;
        if reference.storage_alias != self.alias
            || reference.byte_count > u64::from(self.resources.capacity)
        {
            return Err(StorageError::new(ErrorKind::Invalid));
        }
        Ok(())
    }
    async fn stage(
        &self,
        reference: &ObjectRef,
        mut input: Reader,
        op: &Operation<'_>,
    ) -> Result<VerifiedFile> {
        let disk = op
            .wait(async {
                self.resources
                    .disk
                    .clone()
                    .acquire_many_owned(reference.byte_count as u32)
                    .await
                    .map_err(|_| StorageError::new(ErrorKind::Local))
            })
            .await?;
        let file = tempfile::NamedTempFile::new().map_err(StorageError::io)?;
        let mut output = tokio::fs::File::from_std(file.reopen().map_err(StorageError::io)?);
        op.wait(copy_verified(
            &mut input,
            Some(&mut output),
            reference,
            self.resources.buffer,
            op.budget.no_progress,
        ))
        .await?;
        op.wait(async { output.flush().await.map_err(StorageError::io) })
            .await?;
        Ok(VerifiedFile {
            file,
            reference: reference.clone(),
            _disk: disk,
        })
    }
    async fn verify_download(
        &self,
        reference: &ObjectRef,
        download: Download,
        output: Option<&mut tokio::fs::File>,
        op: &Operation<'_>,
    ) -> Result<()> {
        if download.length != reference.byte_count
            || reference
                .revision
                .as_ref()
                .is_some_and(|r| Some(r) != download.revision.as_ref())
        {
            return Err(StorageError::new(ErrorKind::Corrupt));
        }
        let mut reader = download.reader;
        op.wait(copy_verified(
            &mut reader,
            output,
            reference,
            self.resources.buffer,
            op.budget.no_progress,
        ))
        .await
    }
    async fn get_retry(&self, reference: &ObjectRef, op: &mut Operation<'_>) -> Result<Download> {
        loop {
            match op
                .request(self.transport.get(reference, op.budget.no_progress))
                .await
            {
                Err(e) if e.kind == ErrorKind::Transient => op.backoff().await?,
                result => return result,
            }
        }
    }
}

#[async_trait]
impl<T: Transport> BlobStorage for Store<T> {
    async fn create(
        &self,
        reference: &ObjectRef,
        input: Reader,
        budget: Budget,
        cancel: &CancellationToken,
    ) -> Result<ObjectRef> {
        let mut uncertain = false;
        let result = async {
            self.validate(reference)?;
            // A recorded provider revision is a read identity, never a new write.
            if reference.revision.is_some() {
                return Err(StorageError::new(ErrorKind::Invalid));
            }
            let mut op = Operation::new(budget, cancel)?;
            let _slot = op
                .wait(async {
                    self.resources
                        .streams
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|_| StorageError::new(ErrorKind::Local))
                })
                .await?;
            let file = self.stage(reference, input, &op).await?;
            loop {
                uncertain = true;
                let put = op
                    .request(self.transport.put(
                        reference,
                        file.file.path(),
                        self.resources.buffer,
                        op.budget.no_progress,
                    ))
                    .await;
                let put_failed = put.is_err();
                let existing = put.as_ref().is_err_and(|e| e.kind == ErrorKind::Conflict);
                match put {
                    Ok(()) => {}
                    Err(e) if e.kind == ErrorKind::Conflict || e.kind == ErrorKind::Transient => {}
                    Err(e) => {
                        if e.kind == ErrorKind::Unauthorized || e.kind == ErrorKind::Unsupported {
                            uncertain = false;
                        }
                        return Err(e);
                    }
                }
                // Also reconcile a lost response before another conditional PUT.
                match self.get_retry(reference, &mut op).await {
                    Ok(download) => {
                        let verification =
                            self.verify_download(reference, download, None, &op).await;
                        uncertain = false;
                        return match verification {
                            Ok(()) => Ok(reference.clone()),
                            Err(mut e) => {
                                if e.kind == ErrorKind::Corrupt && existing {
                                    e.kind = ErrorKind::Conflict;
                                }
                                Err(e)
                            }
                        };
                    }
                    Err(e) if e.kind == ErrorKind::Missing && put_failed => {
                        uncertain = false;
                        op.backoff().await?;
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        .await;
        result.map_err(|e| self.context(e, "create", reference, uncertain))
    }
    async fn remove_owned(
        &self,
        reference: &ObjectRef,
        budget: Budget,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let result = async {
            self.validate(reference)?;
            let mut op = Operation::new(budget, cancel)?;
            let _slot = op
                .wait(async {
                    self.resources
                        .streams
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|_| StorageError::new(ErrorKind::Local))
                })
                .await?;
            loop {
                match op
                    .request(self.transport.delete_object(&reference.key))
                    .await
                {
                    Err(e) if e.kind == ErrorKind::Transient => op.backoff().await?,
                    Err(e) if e.kind == ErrorKind::Missing => return Ok(()),
                    result => return result,
                }
            }
        }
        .await;
        result.map_err(|e| self.context(e, "remove owned object", reference, false))
    }
    async fn read(
        &self,
        reference: &ObjectRef,
        budget: Budget,
        cancel: &CancellationToken,
    ) -> Result<VerifiedFile> {
        let result = async {
            self.validate(reference)?;
            let mut op = Operation::new(budget, cancel)?;
            let _slot = op
                .wait(async {
                    self.resources
                        .streams
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|_| StorageError::new(ErrorKind::Local))
                })
                .await?;
            let disk = op
                .wait(async {
                    self.resources
                        .disk
                        .clone()
                        .acquire_many_owned(reference.byte_count as u32)
                        .await
                        .map_err(|_| StorageError::new(ErrorKind::Local))
                })
                .await?;
            let file = tempfile::NamedTempFile::new().map_err(StorageError::io)?;
            let mut output = tokio::fs::File::from_std(file.reopen().map_err(StorageError::io)?);
            loop {
                let download = self.get_retry(reference, &mut op).await?;
                match self
                    .verify_download(reference, download, Some(&mut output), &op)
                    .await
                {
                    Err(e) if e.kind == ErrorKind::Transient => {
                        op.backoff().await?;
                        output.set_len(0).await.map_err(StorageError::io)?;
                        tokio::io::AsyncSeekExt::rewind(&mut output)
                            .await
                            .map_err(StorageError::io)?;
                    }
                    result => {
                        result?;
                        break;
                    }
                }
            }
            op.wait(async { output.flush().await.map_err(StorageError::io) })
                .await?;
            Ok(VerifiedFile {
                file,
                reference: reference.clone(),
                _disk: disk,
            })
        }
        .await;
        result.map_err(|e| self.context(e, "read", reference, false))
    }
}

/// Hash through a fixed buffer; read one bounded extra byte to reject excess
/// content. EOF, including the final read, is subject to no-progress timeout.
async fn copy_verified(
    input: &mut Reader,
    mut output: Option<&mut tokio::fs::File>,
    reference: &ObjectRef,
    buffer: usize,
    no_progress: Duration,
) -> Result<()> {
    let mut bytes = vec![0; buffer];
    let mut count = 0_u64;
    let mut hash = blake3::Hasher::new();
    loop {
        let limit = (reference.byte_count.saturating_sub(count).saturating_add(1))
            .min(buffer as u64) as usize;
        let n = tokio::time::timeout(no_progress, input.read(&mut bytes[..limit]))
            .await
            .map_err(|_| StorageError::new(ErrorKind::Timeout))?
            .map_err(|e| {
                let kind = if count > 0 || e.kind() == std::io::ErrorKind::UnexpectedEof {
                    ErrorKind::Corrupt
                } else {
                    ErrorKind::Transient
                };
                let mut error = StorageError::new(kind);
                error.source = Some(e);
                error
            })?;
        if n == 0 {
            break;
        }
        count += n as u64;
        if count > reference.byte_count {
            return Err(StorageError::new(ErrorKind::Corrupt));
        }
        hash.update(&bytes[..n]);
        if let Some(writer) = output.as_mut() {
            tokio::time::timeout(no_progress, writer.write_all(&bytes[..n]))
                .await
                .map_err(|_| StorageError::new(ErrorKind::Timeout))?
                .map_err(StorageError::io)?;
        }
    }
    if count != reference.byte_count || hash.finalize().to_hex().as_str() != reference.digest {
        return Err(StorageError::new(ErrorKind::Corrupt));
    }
    Ok(())
}
