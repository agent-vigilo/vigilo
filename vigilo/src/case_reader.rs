//! Verified, bounded reads of immutable execution inputs.
//!
//! Readers follow only manifest-owned references, check every tree edge and
//! consume complete case files before releasing a requested range. Scheduling
//! chunks and stored batches have independent boundaries. No case membership or
//! payload is written to SQL, and storage I/O never holds a database transaction.
use std::sync::Arc;

use anyhow::{
    Context as _,
    Result,
    ensure,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    blob_storage::{
        BlobStorage,
        Budget,
    },
    contracts::case_storage::*,
    db::workflows::{
        case_projection,
        chunk_processing::WorkerCaseBatchItem,
    },
    models::{
        run_chunk::RunChunk as ClaimedChunk,
        run_shard_case::RunShardCaseDraft,
    },
};

/// An admitted run's verified immutable roots and configuration. The worker's
/// bounded run-context cache owns this handle; it never caches all case files.
pub(crate) struct RunReader {
    pub reference: ObjectRef,
    pub store: Arc<dyn BlobStorage>,
    pub limits: FormatLimits,
    pub manifest: RunPlanManifest,
    pub dataset: DatasetManifest,
    pub configuration: RunConfiguration,
}

impl std::fmt::Debug for RunReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunReader")
            .field("run_id", &self.manifest.run_id)
            .finish_non_exhaustive()
    }
}

/// Reads one metadata file and checks byte integrity and semantic invariants.
pub(crate) async fn document<T: StorageDocument>(
    store: &dyn BlobStorage,
    reference: &ObjectRef,
    limits: &FormatLimits,
    budget: Budget,
    cancel: &CancellationToken,
) -> Result<T> {
    let bytes = crate::dataset_ingestion::read_bytes(store, reference, budget, cancel).await?;
    let document: T = decode(&reference.verify(&bytes, limits)?)?;
    document.validate(limits)?;
    Ok(document)
}

impl RunReader {
    pub async fn open(
        store: Arc<dyn BlobStorage>,
        reference: &ObjectRef,
        limits: FormatLimits,
        run_id: Uuid,
        cancel: &CancellationToken,
    ) -> Result<Self> {
        let budget = Budget::standard();
        let manifest: RunPlanManifest =
            document(store.as_ref(), reference, &limits, budget, cancel).await?;
        ensure!(manifest.run_id == run_id, "run-plan identity mismatch");
        ensure!(
            manifest.dataset.storage_alias == reference.storage_alias,
            "dataset owner mismatch"
        );
        let configuration: RunConfiguration = document(
            store.as_ref(),
            &manifest.configuration,
            &limits,
            budget,
            cancel,
        )
        .await?;
        let dataset: DatasetManifest =
            document(store.as_ref(), &manifest.dataset, &limits, budget, cancel).await?;
        let snapshot = &configuration.snapshot;
        ensure!(
            snapshot["chunk_size"] == 100
                && snapshot["profile_hash"] == manifest.profile_hash
                && snapshot["aggregation_policy_hash"] == manifest.aggregation_policy_hash
                && snapshot["execution_plan_hash"] == manifest.execution_plan_hash
                && snapshot["dataset_hash"] == dataset.dataset_hash
                && snapshot["dataset_version_id"]
                    == serde_json::to_value(dataset.dataset_version_id)?
                && snapshot["dataset_ref"]["dataset_id"]
                    == serde_json::to_value(dataset.dataset_id)?
                && snapshot["dataset_ref"]["case_count"] == dataset.case_count
                && manifest
                    .shards
                    .iter()
                    .map(|s| i64::from(s.case_count))
                    .sum::<i64>()
                    == i64::from(dataset.case_count),
            "run configuration/manifest identity mismatch"
        );
        let chunks = (i64::from(dataset.case_count) + 99) / 100;
        ensure!(
            i64::from(manifest.chunk_count) == chunks,
            "run plan differs from execution chunk policy"
        );
        for shard in &manifest.shards {
            let number = i64::from(shard.run_shard);
            ensure!(number < chunks, "run plan contains an unused shard");
            let count = (chunks - 1 - number) / 128 + 1;
            let last = number + (count - 1) * 128;
            let final_size = if last == chunks - 1 {
                i64::from(dataset.case_count) - last * 100
            } else {
                100
            };
            ensure!(
                i64::from(shard.chunk_count) == count
                    && i64::from(shard.case_count) == (count - 1) * 100 + final_size,
                "run plan shard totals differ from execution chunk policy"
            );
        }
        Ok(Self {
            reference: reference.clone(),
            store,
            limits,
            manifest,
            dataset,
            configuration,
        })
    }

    /// Loads exactly the claimed chunk's cases. A corrupt tail outside the
    /// requested slice still rejects the whole file, before execution begins.
    pub async fn prepare(
        &self,
        chunk: &ClaimedChunk,
        cancel: &CancellationToken,
    ) -> Result<PreparedCases<'_>> {
        ensure!(
            chunk.run_id == self.manifest.run_id
                && chunk.dataset_version_id == self.dataset.dataset_version_id,
            "claimed chunk belongs to another run or dataset"
        );
        ensure!(
            chunk.ordinal_start >= 0
                && chunk.ordinal_end > chunk.ordinal_start
                && chunk.ordinal_end <= self.dataset.case_count
                && chunk.ordinal_end - chunk.ordinal_start <= 100,
            "invalid claimed chunk range"
        );
        let budget = Budget::standard();
        let index = chunk.ordinal_start / 100;
        let mut chunks = IndexCursor::new(
            self.manifest.chunks.clone(),
            IndexKind::RunChunks,
            index,
            index + 1,
        )?;
        let (entry, bytes) = chunks
            .next(self.store.as_ref(), &self.limits, budget, cancel)
            .await?
            .context("missing chunk file")?;
        let page: RunChunkPage = decode(&entry.object.verify(&bytes, &self.limits)?)?;
        page.validate(&self.limits)?;
        ensure!(
            page.run_id == chunk.run_id
                && page.chunk_index_start == entry.range_start
                && page.chunk_index_end == entry.range_end,
            "chunk file/index mismatch"
        );
        let planned = page
            .chunks
            .iter()
            .find(|c| c.chunk_index == index)
            .context("missing planned chunk")?;
        ensure!(
            planned.chunk_id == chunk.id
                && planned.run_shard == chunk.run_shard
                && planned.ordinal_start == chunk.ordinal_start
                && planned.ordinal_end == chunk.ordinal_end
                && planned.profile_group_id == chunk.profile_group_id,
            "claimed chunk differs from immutable plan"
        );
        let mut batches = IndexCursor::new(
            self.dataset.batches.clone(),
            IndexKind::DatasetBatches,
            chunk.ordinal_start,
            chunk.ordinal_end,
        )?;
        let mut cases = Vec::with_capacity((chunk.ordinal_end - chunk.ordinal_start) as usize);
        let mut seen = 0usize;
        let mut retained = 0usize;
        let mut cached = true;
        let cache_budget = self
            .limits
            .resident_batch_bytes
            .saturating_sub(self.limits.batch.max_bytes);
        let mut hash = blake3::Hasher::new();
        hash.update(b"vigilo/run-shard-cases/v1");
        while let Some((entry, bytes)) = batches
            .next(self.store.as_ref(), &self.limits, budget, cancel)
            .await?
        {
            let verified = entry.object.verify(&bytes, &self.limits)?;
            let reader = decode_case_batch(&verified)?.validate(&self.limits)?;
            ensure!(
                reader.header().dataset_id == self.dataset.dataset_id
                    && reader.header().ordinal_start == entry.range_start
                    && reader.header().ordinal_end == entry.range_end,
                "case file/index mismatch"
            );
            for record in reader {
                let record = record?;
                if record.case_ordinal < chunk.ordinal_start
                    || record.case_ordinal >= chunk.ordinal_end
                {
                    continue;
                }
                ensure!(
                    record.case_ordinal == chunk.ordinal_start + i32::try_from(seen)?,
                    "assigned cases have a gap or overlap"
                );
                let case = record.validate()?;
                case_projection::update_projection_hash(
                    &mut hash,
                    &RunShardCaseDraft {
                        run_id: chunk.run_id,
                        run_shard: chunk.run_shard,
                        dataset_version_id: chunk.dataset_version_id,
                        case_id: case.id,
                        case_ordinal: record.case_ordinal,
                        case_hash: record.case_hash.clone(),
                    },
                );
                seen += 1;
                let bytes = serde_json::to_vec(&record)?.len();
                if cached && retained.saturating_add(bytes) <= cache_budget {
                    retained += bytes;
                    cases.push(worker_case(record, case)?);
                } else {
                    cached = false;
                    cases.clear();
                }
            }
        }
        ensure!(
            seen == (chunk.ordinal_end - chunk.ordinal_start) as usize
                && hash.finalize().to_hex().as_str() == planned.case_projection_hash,
            "assigned case membership differs from immutable plan"
        );
        Ok(PreparedCases {
            reader: self,
            chunk: chunk.clone(),
            cached: cached.then_some(cases),
            cursor: if cached {
                None
            } else {
                Some(IndexCursor::new(
                    self.dataset.batches.clone(),
                    IndexKind::DatasetBatches,
                    chunk.ordinal_start,
                    chunk.ordinal_end,
                )?)
            },
        })
    }

    #[cfg(test)]
    pub(crate) async fn cases(
        &self,
        chunk: &ClaimedChunk,
        cancel: &CancellationToken,
    ) -> Result<Vec<WorkerCaseBatchItem>> {
        let mut prepared = self.prepare(chunk, cancel).await?;
        let mut cases = Vec::new();
        while let Some(batch) = prepared.next(cancel).await? {
            cases.extend(batch);
        }
        Ok(cases)
    }
}

/// A fully verified assignment. Small inputs retain one bounded case vector;
/// larger inputs reread immutable batches one at a time after the membership
/// proof, preserving the memory budget without staging another payload copy.
pub(crate) struct PreparedCases<'a> {
    reader: &'a RunReader,
    chunk: ClaimedChunk,
    cached: Option<Vec<WorkerCaseBatchItem>>,
    cursor: Option<IndexCursor>,
}
impl PreparedCases<'_> {
    pub async fn next(
        &mut self,
        cancel: &CancellationToken,
    ) -> Result<Option<Vec<WorkerCaseBatchItem>>> {
        ensure!(!cancel.is_cancelled(), "case read cancelled");
        if let Some(cases) = self.cached.take() {
            return Ok(Some(cases));
        }
        let Some(cursor) = self.cursor.as_mut() else {
            return Ok(None);
        };
        let reader = self.reader;
        let Some((entry, bytes)) = cursor
            .next(
                reader.store.as_ref(),
                &reader.limits,
                Budget::standard(),
                cancel,
            )
            .await?
        else {
            return Ok(None);
        };
        let verified = entry.object.verify(&bytes, &reader.limits)?;
        let records = decode_case_batch(&verified)?.validate(&reader.limits)?;
        ensure!(
            records.header().dataset_id == reader.dataset.dataset_id
                && records.header().ordinal_start == entry.range_start
                && records.header().ordinal_end == entry.range_end,
            "case file/index mismatch"
        );
        let mut cases = Vec::new();
        for record in records {
            let record = record?;
            if (self.chunk.ordinal_start..self.chunk.ordinal_end).contains(&record.case_ordinal) {
                let case = record.validate()?;
                cases.push(worker_case(record, case)?);
            }
        }
        ensure!(
            !cases.is_empty(),
            "assigned file contains no selected cases"
        );
        Ok(Some(cases))
    }
}
fn worker_case(
    record: CaseRecord,
    case: crate::contracts::run::DatasetCase,
) -> Result<WorkerCaseBatchItem> {
    // Execution snapshots retain normalized tag ordering, independently of the
    // original source order preserved by immutable dataset identity.
    let mut tags = case.tags;
    tags.sort();
    Ok(WorkerCaseBatchItem {
        case_id: case.id,
        case_hash: record.case_hash,
        case_ordinal: record.case_ordinal,
        task_type: case.task_type,
        case_group: case.case_group,
        input_payload: case.input,
        expected_output: case.expected.unwrap_or_default(),
        context_payload: case.context.unwrap_or_default(),
        tags: serde_json::to_value(tags)?,
        metadata: serde_json::to_value(case.metadata)?,
    })
}

/// Depth-first range traversal with an explicit 4 MiB encoded index working set.
/// Each edge decreases height and owns contiguous ranges, bounding work and
/// excluding cycles without retaining a graph-wide visited set.
pub(crate) struct IndexCursor {
    root: Option<IndexDescriptor>,
    kind: IndexKind,
    stack: Vec<(IndexPage, usize, u64)>,
    resident: u64,
    start: i32,
    end: i32,
}
impl IndexCursor {
    pub fn new(root: IndexDescriptor, kind: IndexKind, start: i32, end: i32) -> Result<Self> {
        ensure!(
            start >= root.range_start && end <= root.range_end && start < end,
            "index request outside root range"
        );
        Ok(Self {
            root: Some(root),
            kind,
            stack: Vec::new(),
            resident: 0,
            start,
            end,
        })
    }
    pub async fn next(
        &mut self,
        store: &dyn BlobStorage,
        limits: &FormatLimits,
        budget: Budget,
        cancel: &CancellationToken,
    ) -> Result<Option<(IndexEntry, Vec<u8>)>> {
        if let Some(root) = self.root.take() {
            ensure!(
                root.object.byte_count <= 4 * 1024 * 1024,
                "index working set exceeds 4 MiB"
            );
            let page: IndexPage = document(store, &root.object, limits, budget, cancel).await?;
            root.validate_page(&page, self.kind.clone(), limits)?;
            self.resident = root.object.byte_count;
            self.stack.push((page, 0, root.object.byte_count));
        }
        while let Some((page, index, size)) = self.stack.last_mut() {
            if *index == page.entries.len() {
                self.resident -= *size;
                self.stack.pop();
                continue;
            }
            let entry = page.entries[*index].clone();
            *index += 1;
            if entry.range_end <= self.start || entry.range_start >= self.end {
                continue;
            }
            if page.level == 0 {
                let bytes =
                    crate::dataset_ingestion::read_bytes(store, &entry.object, budget, cancel)
                        .await?;
                return Ok(Some((entry, bytes)));
            }
            ensure!(
                self.resident + entry.object.byte_count <= 4 * 1024 * 1024,
                "index working set exceeds 4 MiB"
            );
            let child: IndexPage = document(store, &entry.object, limits, budget, cancel).await?;
            page.validate_child(*index - 1, &child, limits)?;
            self.resident += entry.object.byte_count;
            self.stack.push((child, 0, entry.object.byte_count));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests;
