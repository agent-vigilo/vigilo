//! Bounded upload checkpoints over the existing canonical preparation spool.
//!
//! A group retains only exact references and source offsets, never all file bytes.
//! After durable registration each missing file is reconstructed and hash-checked
//! individually. SQL acknowledges a verified prefix after 30 seconds of work or
//! the end of the group. Each admission mints its 150-second outer deadline
//! before checking authority; failures retain exact keys for resume and cleanup.
use std::time::Duration;

use anyhow::{
    Context,
    Result,
    ensure,
};
use sqlx::PgPool;
use tokio::{
    io::{
        AsyncReadExt,
        AsyncSeekExt,
    },
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use super::{
    Lease,
    Object,
    Prepared,
    Upload,
};
use crate::{
    blob_storage::{
        BlobStorage,
        Budget,
    },
    db::workflows::dataset_upload::{
        self as ledger,
        batches as checkpoint,
    },
};

const CHECKPOINT_AFTER: Duration = Duration::from_secs(30);
const RENEW_AFTER: Duration = Duration::from_secs(60);
const ADMISSION_SECONDS: u64 =
    CHECKPOINT_AFTER.as_secs() + crate::blob_storage::DEFAULT_OPERATION_SECONDS;
const _: () = assert!(ADMISSION_SECONDS < 300);

/// Preserve each file's normal 120-second allowance, capped by authority's
/// pre-registration window. Files start within the first 30 seconds; a paused
/// task cannot obtain an unbounded new deadline after resumption.
fn transfer_budget(admitted_deadline: Instant) -> Budget {
    let mut budget = Budget::standard();
    budget.deadline = budget.deadline.min(admitted_deadline);
    budget
}

struct Pending {
    object: Object,
    offset: u64,
    header: Vec<u8>,
}

/// One ingestion owns one bounded metadata group and reuses its prepared spool.
pub(super) struct Uploader<'a> {
    db: &'a PgPool,
    store: &'a dyn BlobStorage,
    upload: &'a Upload,
    lease: Lease,
    prepared: &'a Prepared,
    cancel: &'a CancellationToken,
    pending: Vec<Pending>,
    payload_bytes: u64,
    metadata_bytes: usize,
}

impl<'a> Uploader<'a> {
    pub(super) fn new(
        db: &'a PgPool,
        store: &'a dyn BlobStorage,
        upload: &'a Upload,
        lease: Lease,
        prepared: &'a Prepared,
        cancel: &'a CancellationToken,
    ) -> Self {
        Self {
            db,
            store,
            upload,
            lease,
            prepared,
            cancel,
            pending: Vec::new(),
            payload_bytes: 0,
            metadata_bytes: 2,
        }
    }

    /// Retain only a small canonical header and source offset; drop the encoded
    /// payload before any flush. Bound metadata, queued payload bytes and count.
    pub(super) async fn push(
        &mut self,
        object: Object,
        offset: u64,
        encoded: Vec<u8>,
    ) -> Result<()> {
        let header_end = encoded
            .iter()
            .take(1024)
            .position(|b| *b == b'\n')
            .context("batch header missing or oversized")?
            + 1;
        let header = encoded[..header_end].to_vec();
        drop(encoded);
        let size = checkpoint::metadata_bytes(&object)? + header.len() + std::mem::size_of::<u64>();
        ensure!(
            object.reference.byte_count <= checkpoint::MAX_PAYLOAD_BYTES
                && size + 2 <= checkpoint::MAX_METADATA_BYTES,
            "file exceeds checkpoint bounds"
        );
        if !fits(
            self.pending.len(),
            self.payload_bytes,
            self.metadata_bytes,
            object.reference.byte_count,
            size,
        ) {
            self.flush().await?;
        }
        self.payload_bytes += object.reference.byte_count;
        self.metadata_bytes += size;
        self.pending.push(Pending {
            object,
            offset,
            header,
        });
        if self.pending.len() == checkpoint::MAX_FILES {
            self.flush().await?;
        }
        Ok(())
    }

    /// Every remote request starts after group ownership is committed. A failed
    /// file leaves its entire unacknowledged prefix replayable at the same keys.
    pub(super) async fn flush(&mut self) -> Result<()> {
        let files = std::mem::take(&mut self.pending);
        self.payload_bytes = 0;
        self.metadata_bytes = 2;
        let objects: Vec<_> = files.iter().map(|f| f.object.clone()).collect();
        let mut first = 0;
        while first < files.len() {
            ensure!(!self.cancel.is_cancelled(), "dataset upload cancelled");
            let started = Instant::now();
            let admitted_deadline = started + Duration::from_secs(ADMISSION_SECONDS);
            let acknowledged = checkpoint::plan(self.db, self.lease, &objects[first..]).await?;
            let mut end = first;
            let mut changed = false;
            for (file, acknowledged) in files[first..].iter().zip(acknowledged) {
                if !acknowledged {
                    let budget = transfer_budget(admitted_deadline);
                    let rebuild = self.rebuild(file);
                    let bytes = tokio::select! { biased;
                        _=self.cancel.cancelled()=>anyhow::bail!("dataset upload cancelled"),
                        _=tokio::time::sleep_until(budget.deadline)=>anyhow::bail!("batch reconstruction deadline exceeded"),
                        result=rebuild=>result?,
                    };
                    // create verifies the exact complete remote bytes, including
                    // reconciliation after a lost response or conditional conflict.
                    self.store
                        .create(
                            &file.object.reference,
                            Box::new(std::io::Cursor::new(bytes)),
                            budget,
                            self.cancel,
                        )
                        .await?;
                    changed = true;
                }
                end += 1;
                if started.elapsed() >= CHECKPOINT_AFTER {
                    break;
                }
            }
            ensure!(!self.cancel.is_cancelled(), "dataset upload cancelled");
            if changed {
                checkpoint::acknowledge(self.db, self.lease, &objects[first..end]).await?;
            }
            first = end;
        }
        Ok(())
    }

    /// Rebuild exactly one batch without adding another disk spool. Digest
    /// comparison catches source mutation before the storage boundary sees bytes.
    async fn rebuild(&self, file: &Pending) -> Result<Vec<u8>> {
        let mut input = tokio::fs::File::open(self.prepared.records.path()).await?;
        input.seek(std::io::SeekFrom::Start(file.offset)).await?;
        let body_bytes = file
            .object
            .reference
            .byte_count
            .checked_sub(file.header.len() as u64)
            .context("batch header exceeds file")?;
        let mut bytes = Vec::with_capacity(usize::try_from(file.object.reference.byte_count)?);
        bytes.extend_from_slice(&file.header);
        // Prepared records already have exactly the canonical JSONL encoding
        // validated by the batch builder. Copy its bounded span, then prove the
        // reconstructed object still matches before admitting remote I/O.
        input.take(body_bytes).read_to_end(&mut bytes).await?;
        file.object.reference.verify(&bytes, &self.upload.limits)?;
        Ok(bytes)
    }
}

fn fits(
    count: usize,
    payload: u64,
    metadata: usize,
    next_payload: u64,
    next_metadata: usize,
) -> bool {
    count < checkpoint::MAX_FILES
        && payload
            .checked_add(next_payload)
            .is_some_and(|n| n <= checkpoint::MAX_PAYLOAD_BYTES)
        && metadata
            .checked_add(next_metadata)
            .is_some_and(|n| n <= checkpoint::MAX_METADATA_BYTES)
}

/// Read-only verification renews by elapsed time. The 60-second renewal cadence
/// plus a 120-second operation deadline stays inside the 300-second lease.
/// The first operation renews explicitly rather than assuming a previous call's
/// lease timestamp; a paused task cannot invent fresh authority on resumption.
pub(super) struct ReadLease {
    next_renewal: Instant,
}
impl ReadLease {
    pub(super) fn new() -> Self {
        Self {
            next_renewal: Instant::now(),
        }
    }
    pub(super) async fn budget(&mut self, db: &PgPool, lease: Lease) -> Result<Budget> {
        let budget = Budget::standard();
        let now = Instant::now();
        if now >= self.next_renewal {
            ledger::renew(db, lease, false).await?;
            self.next_renewal = now + RENEW_AFTER;
        }
        Ok(budget)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn file_deadlines_preserve_normal_allowance_and_cap_paused_admissions() {
        let now = Instant::now();
        let ordinary = transfer_budget(now + Duration::from_secs(150));
        assert!(ordinary.deadline >= now + Duration::from_secs(120));
        assert!(ordinary.deadline <= Instant::now() + Duration::from_secs(120));
        let remaining = now + Duration::from_secs(20);
        assert_eq!(transfer_budget(remaining).deadline, remaining);
        let expired = now - Duration::from_secs(1);
        assert_eq!(transfer_budget(expired).deadline, expired);
    }

    #[test]
    fn groups_bound_count_payload_metadata_and_overflow_independently() {
        assert!(fits(63, 0, 2, checkpoint::MAX_PAYLOAD_BYTES, 1));
        assert!(!fits(64, 0, 2, 1, 1));
        assert!(!fits(1, checkpoint::MAX_PAYLOAD_BYTES, 2, 1, 1));
        assert!(!fits(1, 0, checkpoint::MAX_METADATA_BYTES, 1, 1));
        assert!(!fits(1, u64::MAX, 2, 1, 1));
        assert!(!fits(1, 0, usize::MAX, 1, 1));
        assert!(RENEW_AFTER.as_secs() + crate::blob_storage::DEFAULT_OPERATION_SECONDS < 300);
    }
}

#[cfg(test)]
#[path = "batches/postgres_tests.rs"]
mod postgres_tests;
