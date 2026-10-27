//! Bounded, dependency-ordered publication of immutable plan files.
//!
//! References are deterministic before upload. The writer buffers at most one
//! bounded group, records every exact key, then creates/verifies files in caller
//! order before acknowledging a verified prefix. Tree levels flush before their
//! parents are built; gate chains are supplied tail first. The root is published
//! only after a complete flush. No payload is stored in SQL and no transaction
//! spans remote work. A failed group is reconstructed on the next preparation.
use std::time::Duration;

use anyhow::{
    Result,
    ensure,
};
use sqlx::types::Json;
use tokio::time::Instant;

use super::{
    Object,
    ObjectRef,
    Session,
    StorageDocument,
    StorageFormat,
    encode,
    read_bytes,
};
use crate::{
    blob_storage::Budget,
    db::workflows::run_plan::batches,
};

const CHECKPOINT_AFTER: Duration = Duration::from_secs(30);
const ADMISSION_SECONDS: u64 = 30 + crate::blob_storage::DEFAULT_OPERATION_SECONDS;
const _: () = assert!(ADMISSION_SECONDS < 300);

struct File {
    object: Object,
    bytes: Vec<u8>,
}

pub(super) struct Writer<'a, 's> {
    pub session: &'a Session<'s>,
    pending: Vec<File>,
    payload_bytes: u64,
    metadata_bytes: usize,
}

impl<'a, 's> Writer<'a, 's> {
    pub fn new(session: &'a Session<'s>) -> Self {
        Self {
            session,
            pending: Vec::new(),
            payload_bytes: 0,
            metadata_bytes: 2,
        }
    }

    pub async fn document<T: StorageDocument>(
        &mut self,
        kind: &str,
        level: u8,
        start: i32,
        end: i32,
        doc: &T,
    ) -> Result<ObjectRef> {
        self.put(
            kind,
            level,
            start,
            end,
            T::SCHEMA,
            encode(doc, self.session.limits)?,
        )
        .await
    }

    /// Return a deterministic reference; callers must flush before consuming
    /// SQL children or publishing a parent root. Large admitted files stand alone.
    #[allow(clippy::too_many_arguments)]
    pub async fn put(
        &mut self,
        kind: &str,
        level: u8,
        start: i32,
        end: i32,
        schema: StorageFormat,
        bytes: Vec<u8>,
    ) -> Result<ObjectRef> {
        let digest = blake3::hash(&bytes).to_hex().to_string();
        let reference = ObjectRef::for_bytes(
            self.session.alias.into(),
            format!(
                "runs/{}/{kind}/{level}/{start}-{end}/{digest}",
                self.session.lease.run_id
            ),
            schema,
            &bytes,
            self.session.limits,
        )?;
        let object = Object {
            object_key: reference.key.clone(),
            kind: kind.into(),
            level: i16::from(level),
            range_start: Some(start),
            range_end: Some(end),
            reference: Json(reference.clone()),
            acknowledged: false,
        };
        let metadata = serde_json::to_vec(&object)?.len() + 1;
        ensure!(
            metadata + 2 <= batches::MAX_METADATA_BYTES,
            "plan reference exceeds metadata group bound"
        );
        if !self.pending.is_empty()
            && (self.pending.len() == batches::MAX_FILES
                || self.payload_bytes + reference.byte_count > batches::GROUP_BYTES
                || self.metadata_bytes + metadata > batches::MAX_METADATA_BYTES)
        {
            self.flush().await?;
        }
        self.payload_bytes += reference.byte_count;
        self.metadata_bytes += metadata;
        self.pending.push(File { object, bytes });
        if self.pending.len() == batches::MAX_FILES || self.payload_bytes >= batches::GROUP_BYTES {
            self.flush().await?;
        }
        Ok(reference)
    }

    /// Own all remaining keys before any PUT, verify every complete file, and
    /// checkpoint after 30 seconds or group completion. Admission deadlines are
    /// minted before the SQL fence; a paused caller cannot invent fresh authority.
    pub async fn flush(&mut self) -> Result<()> {
        let mut files = std::mem::take(&mut self.pending);
        self.payload_bytes = 0;
        self.metadata_bytes = 2;
        let objects: Vec<_> = files.iter().map(|f| f.object.clone()).collect();
        let mut first = 0;
        while first < files.len() {
            ensure!(
                !self.session.cancel.is_cancelled(),
                "run planning cancelled"
            );
            let started = Instant::now();
            let deadline = started + Duration::from_secs(ADMISSION_SECONDS);
            let acknowledged =
                batches::plan(self.session.db, self.session.lease, &objects[first..]).await?;
            let mut end = first;
            let mut changed = false;
            for (file, acknowledged) in files[first..].iter_mut().zip(acknowledged) {
                let mut budget = Budget::standard();
                budget.deadline = budget.deadline.min(deadline);
                if acknowledged {
                    file.bytes = Vec::new();
                    read_bytes(
                        self.session.store,
                        &file.object.reference,
                        budget,
                        self.session.cancel,
                    )
                    .await?;
                } else {
                    self.session
                        .store
                        .create(
                            &file.object.reference,
                            Box::new(std::io::Cursor::new(std::mem::take(&mut file.bytes))),
                            budget,
                            self.session.cancel,
                        )
                        .await?;
                    changed = true;
                }
                end += 1;
                if started.elapsed() >= CHECKPOINT_AFTER {
                    break;
                }
            }
            ensure!(
                !self.session.cancel.is_cancelled(),
                "run planning cancelled"
            );
            if changed {
                batches::acknowledge(self.session.db, self.session.lease, &objects[first..end])
                    .await?;
            }
            first = end;
        }
        Ok(())
    }
}
