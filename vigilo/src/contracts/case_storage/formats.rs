//! Strict metadata shapes and local invariants for immutable object graphs.
//!
//! A verified object still needs semantic validation. These types check schema,
//! integer/range bounds, references, and bounded page membership. Link validation
//! binds a child to its parent's range, kind, level, and storage owner without
//! fetching objects or retaining the full graph.

use std::{
    collections::BTreeSet,
    fmt,
};

use anyhow::{
    Result,
    ensure,
};
use serde::{
    Deserialize,
    Deserializer,
    Serialize,
};
use serde_json::Value;
use uuid::Uuid;

use super::FormatLimits;

/// Known stored-object formats; unknown identifiers fail during deserialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum StorageFormat {
    #[serde(rename = "vigilo/case-batch/v1")]
    CaseBatch,
    #[serde(rename = "vigilo/dataset-source-fields/v1")]
    DatasetSourceFields,
    #[serde(rename = "vigilo/dataset-manifest/v1")]
    DatasetManifest,
    #[serde(rename = "vigilo/ordinal-index/v1")]
    OrdinalIndex,
    #[serde(rename = "vigilo/run-chunks/v1")]
    RunChunks,
    #[serde(rename = "vigilo/run-plan-manifest/v1")]
    RunPlanManifest,
    #[serde(rename = "vigilo/run-configuration/v1")]
    RunConfiguration,
    #[serde(rename = "vigilo/gate-counts/v1")]
    GateCounts,
}

impl StorageFormat {
    pub(crate) fn byte_limit(self, limits: &FormatLimits) -> usize {
        match self {
            Self::CaseBatch => limits.batch.max_bytes,
            Self::DatasetSourceFields | Self::RunConfiguration => limits.source_bytes,
            Self::DatasetManifest | Self::RunPlanManifest => limits.root_bytes,
            Self::OrdinalIndex | Self::RunChunks | Self::GateCounts => limits.page_bytes,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum HashScheme {
    #[serde(rename = "blake3-256")]
    Blake3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum IdentityScheme {
    #[serde(rename = "vigilo/dataset-json/v1")]
    DatasetJson,
}

/// Checks the exact digest representation; provider ETags are not digests.
pub(crate) fn validate_digest(digest: &str) -> Result<()> {
    ensure!(
        digest.len() == 64
            && digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "digest must contain 64 lowercase hexadecimal characters"
    );
    Ok(())
}

/// Durable, non-secret object identity. Location credentials stay in configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObjectRef {
    pub(crate) storage_alias: String,
    pub(crate) key: String,
    pub(crate) schema: StorageFormat,
    pub(crate) hash_scheme: HashScheme,
    pub(crate) digest: String,
    pub(crate) byte_count: u64,
    #[serde(deserialize_with = "required_nullable")]
    pub(crate) revision: Option<String>,
}

/// Byte-integrity proof bound to immutable bytes and a fixed reference snapshot.
/// Only successful reference verification can construct it. Borrowing avoids a
/// payload copy and prevents mutation of the bytes while the proof is in use.
/// This proves neither valid JSON nor semantic correctness of its contents.
pub(crate) struct VerifiedObject<'a> {
    reference: ObjectRef,
    bytes: &'a [u8],
}

impl VerifiedObject<'_> {
    pub(crate) fn reference(&self) -> &ObjectRef {
        &self.reference
    }
}

impl<'a> VerifiedObject<'a> {
    pub(crate) fn bytes(&self) -> &'a [u8] {
        self.bytes
    }
}

impl fmt::Debug for VerifiedObject<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Case payloads must not leak through diagnostic formatting.
        formatter
            .debug_struct("VerifiedObject")
            .field("reference", &self.reference)
            .field("byte_count", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

/// An explicit null is part of the stored format; an absent field is an error.
fn required_nullable<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> std::result::Result<Option<T>, D::Error> {
    Option::deserialize(d)
}

impl ObjectRef {
    pub(crate) fn validate(&self, limits: &FormatLimits) -> Result<()> {
        ensure!(
            !self.storage_alias.is_empty()
                && self.storage_alias.len() <= 128
                && self
                    .storage_alias
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b)),
            "invalid storage alias"
        );
        ensure!(
            !self.key.is_empty()
                && self.key.len() <= 1024
                && !self.key.starts_with('/')
                && !self.key.contains([':', '?', '#', '\\'])
                && !self.key.chars().any(char::is_control)
                && self
                    .key
                    .split('/')
                    .all(|p| !p.is_empty() && p != "." && p != ".."),
            "invalid logical object key"
        );
        validate_digest(&self.digest)?;
        ensure!(
            self.byte_count > 0 && self.byte_count <= self.schema.byte_limit(limits) as u64,
            "object byte_count exceeds schema limit or is zero"
        );
        if let Some(revision) = &self.revision {
            ensure!(
                !revision.is_empty()
                    && revision.len() <= 1024
                    && !revision.chars().any(char::is_control),
                "invalid object revision"
            );
        }
        Ok(())
    }

    /// Verifies limits, reference shape, exact byte count, and BLAKE3 integrity.
    /// Returns a proof for decoding without parsing or validating the contents.
    pub(crate) fn verify<'a>(
        &self,
        bytes: &'a [u8],
        limits: &FormatLimits,
    ) -> Result<VerifiedObject<'a>> {
        limits.validate()?;
        self.validate(limits)?;
        ensure!(
            bytes.len() as u64 == self.byte_count,
            "object byte_count mismatch"
        );
        ensure!(
            blake3::hash(bytes).to_hex().as_str() == self.digest,
            "object digest mismatch"
        );
        Ok(VerifiedObject {
            reference: self.clone(),
            bytes,
        })
    }

    /// Constructs a reference only for completely encoded, admitted bytes.
    pub(crate) fn for_bytes(
        storage_alias: String,
        key: String,
        schema: StorageFormat,
        bytes: &[u8],
        limits: &FormatLimits,
    ) -> Result<Self> {
        let reference = Self {
            storage_alias,
            key,
            schema,
            hash_scheme: HashScheme::Blake3,
            digest: blake3::hash(bytes).to_hex().to_string(),
            byte_count: bytes.len() as u64,
            revision: None,
        };
        limits.validate()?;
        reference.validate(limits)?;
        Ok(reference)
    }

    fn require(&self, schema: StorageFormat, limits: &FormatLimits) -> Result<()> {
        self.validate(limits)?;
        ensure!(
            self.schema == schema,
            "reference schema mismatch: expected {schema:?}"
        );
        Ok(())
    }
}

/// Serialization, stored-format identity, and validation for metadata objects.
pub(crate) trait StorageDocument: Serialize + for<'de> Deserialize<'de> {
    const SCHEMA: StorageFormat;
    fn validate(&self, limits: &FormatLimits) -> Result<()>;
}

fn schema(actual: StorageFormat, expected: StorageFormat) -> Result<()> {
    ensure!(actual == expected, "schema mismatch: expected {expected:?}");
    Ok(())
}

pub(crate) fn range(start: i32, end: i32) -> Result<()> {
    ensure!(
        start >= 0 && end > start,
        "invalid nonempty half-open range"
    );
    Ok(())
}

fn shard(value: i16) -> Result<()> {
    ensure!((0..128).contains(&value), "run_shard must be in 0..128");
    Ok(())
}

/// Original top-level dataset fields except cases, plus whether cases was present.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DatasetSourceFields {
    pub(crate) schema: StorageFormat,
    pub(crate) cases_present: bool,
    pub(crate) fields: Value,
}

impl StorageDocument for DatasetSourceFields {
    const SCHEMA: StorageFormat = StorageFormat::DatasetSourceFields;
    fn validate(&self, _: &FormatLimits) -> Result<()> {
        schema(self.schema, Self::SCHEMA)?;
        ensure!(
            self.fields.is_object() && self.fields.get("cases").is_none(),
            "source fields must be an object without cases"
        );
        let _: super::super::run::RunDataset = serde_json::from_value(self.fields.clone())?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum IndexKind {
    DatasetBatches,
    RunChunks,
}

impl IndexKind {
    fn leaf_schema(&self) -> StorageFormat {
        match self {
            Self::DatasetBatches => StorageFormat::CaseBatch,
            Self::RunChunks => StorageFormat::RunChunks,
        }
    }
}

/// One start-inclusive, end-exclusive interval in the containing index's unit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IndexEntry {
    pub(crate) range_start: i32,
    pub(crate) range_end: i32,
    pub(crate) object: ObjectRef,
}

/// A bounded interval-tree root or interior edge.
/// Range units follow the index kind supplied by the owning manifest or parent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IndexDescriptor {
    pub(crate) level: u8,
    pub(crate) range_start: i32,
    pub(crate) range_end: i32,
    pub(crate) object: ObjectRef,
}

impl IndexDescriptor {
    pub(crate) fn validate(&self, limits: &FormatLimits) -> Result<()> {
        range(self.range_start, self.range_end)?;
        ensure!(
            self.level < limits.index_height,
            "index depth exceeds limit"
        );
        self.object.require(StorageFormat::OrdinalIndex, limits)
    }

    /// The child must already have passed byte integrity and local validation.
    pub(crate) fn validate_page(
        &self,
        page: &IndexPage,
        kind: IndexKind,
        limits: &FormatLimits,
    ) -> Result<()> {
        self.validate(limits)?;
        page.validate(limits)?;
        ensure!(
            page.level == self.level
                && page.kind == kind
                && page.range_start == self.range_start
                && page.range_end == self.range_end,
            "index descriptor/child mismatch"
        );
        ensure!(
            page.entries
                .iter()
                .all(|entry| entry.object.storage_alias == self.object.storage_alias),
            "index child changes storage owner"
        );
        Ok(())
    }
}

/// An index page whose kind identifies whether ranges count cases or chunks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IndexPage {
    pub(crate) schema: StorageFormat,
    pub(crate) kind: IndexKind,
    pub(crate) level: u8,
    pub(crate) range_start: i32,
    pub(crate) range_end: i32,
    pub(crate) entries: Vec<IndexEntry>,
}

impl StorageDocument for IndexPage {
    const SCHEMA: StorageFormat = StorageFormat::OrdinalIndex;
    fn validate(&self, limits: &FormatLimits) -> Result<()> {
        schema(self.schema, Self::SCHEMA)?;
        range(self.range_start, self.range_end)?;
        ensure!(
            self.level < limits.index_height,
            "index depth exceeds limit"
        );
        ensure!(
            !self.entries.is_empty() && self.entries.len() <= limits.page_entries,
            "invalid index entry count"
        );
        let child_schema = if self.level == 0 {
            self.kind.leaf_schema()
        } else {
            StorageFormat::OrdinalIndex
        };
        let mut next = self.range_start;
        for entry in &self.entries {
            range(entry.range_start, entry.range_end)?;
            ensure!(
                entry.range_start == next,
                "index ranges have a gap or overlap"
            );
            entry.object.require(child_schema, limits)?;
            next = entry.range_end;
        }
        ensure!(
            next == self.range_end,
            "index entries do not cover parent range"
        );
        Ok(())
    }
}

impl IndexPage {
    /// Binds a verified leaf batch to its advertised case interval and dataset.
    pub(crate) fn validate_batch(
        &self,
        entry_index: usize,
        header: &CaseBatchHeader,
        dataset_id: Uuid,
        limits: &FormatLimits,
    ) -> Result<()> {
        self.validate(limits)?;
        ensure!(
            self.level == 0 && self.kind == IndexKind::DatasetBatches,
            "expected dataset leaf index"
        );
        let entry = self
            .entries
            .get(entry_index)
            .ok_or_else(|| anyhow::anyhow!("missing batch index entry"))?;
        ensure!(
            header.schema == StorageFormat::CaseBatch
                && header.dataset_id == dataset_id
                && header.ordinal_start == entry.range_start
                && header.ordinal_end == entry.range_end,
            "batch/index membership mismatch"
        );
        Ok(())
    }

    /// Run index intervals count chunks, independently of their case intervals.
    pub(crate) fn validate_chunks(
        &self,
        entry_index: usize,
        page: &RunChunkPage,
        run_id: Uuid,
        limits: &FormatLimits,
    ) -> Result<()> {
        self.validate(limits)?;
        page.validate(limits)?;
        ensure!(
            self.level == 0 && self.kind == IndexKind::RunChunks,
            "expected run leaf index"
        );
        let entry = self
            .entries
            .get(entry_index)
            .ok_or_else(|| anyhow::anyhow!("missing chunk index entry"))?;
        ensure!(
            page.run_id == run_id
                && page.chunk_index_start == entry.range_start
                && page.chunk_index_end == entry.range_end,
            "chunk page/index membership mismatch"
        );
        Ok(())
    }

    /// Checks one tree edge; decreasing levels also make tree cycles impossible.
    pub(crate) fn validate_child(
        &self,
        entry_index: usize,
        child: &IndexPage,
        limits: &FormatLimits,
    ) -> Result<()> {
        self.validate(limits)?;
        ensure!(self.level > 0, "leaf index cannot contain another index");
        let entry = self
            .entries
            .get(entry_index)
            .ok_or_else(|| anyhow::anyhow!("missing index entry"))?;
        IndexDescriptor {
            level: self.level - 1,
            range_start: entry.range_start,
            range_end: entry.range_end,
            object: entry.object.clone(),
        }
        .validate_page(child, self.kind.clone(), limits)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DatasetManifest {
    pub(crate) schema: StorageFormat,
    pub(crate) dataset_id: Uuid,
    #[serde(deserialize_with = "required_nullable")]
    pub(crate) dataset_version: Option<String>,
    pub(crate) dataset_version_id: Uuid,
    pub(crate) dataset_hash: String,
    pub(crate) identity_scheme: IdentityScheme,
    pub(crate) case_count: i32,
    pub(crate) batch_limits: super::BatchLimits,
    pub(crate) source_fields: ObjectRef,
    pub(crate) batches: IndexDescriptor,
}

impl StorageDocument for DatasetManifest {
    const SCHEMA: StorageFormat = StorageFormat::DatasetManifest;
    fn validate(&self, limits: &FormatLimits) -> Result<()> {
        schema(self.schema, Self::SCHEMA)?;
        self.batch_limits.validate()?;
        ensure!(
            self.batch_limits.max_bytes <= limits.batch.max_bytes
                && self.batch_limits.max_cases <= limits.batch.max_cases,
            "manifest batching policy exceeds reader limits"
        );
        validate_digest(&self.dataset_hash)?;
        ensure!(
            self.case_count > 0
                && self.batches.range_start == 0
                && self.batches.range_end == self.case_count,
            "dataset index must cover every case"
        );
        self.source_fields
            .require(StorageFormat::DatasetSourceFields, limits)?;
        self.batches.validate(limits)?;
        ensure!(
            self.source_fields.storage_alias == self.batches.object.storage_alias,
            "dataset references disagree on storage owner"
        );
        Ok(())
    }
}

/// First record of one case-batch file, identifying its dataset and case range.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CaseBatchHeader {
    pub(crate) schema: StorageFormat,
    pub(crate) dataset_id: Uuid,
    pub(crate) ordinal_start: i32,
    pub(crate) ordinal_end: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CaseRecord {
    pub(crate) case_ordinal: i32,
    pub(crate) case_hash: String,
    pub(crate) source: Value,
}

impl CaseRecord {
    /// Validates typed case shape without replacing original source fields/defaults.
    pub(crate) fn validate(&self) -> Result<super::super::run::DatasetCase> {
        ensure!(self.case_ordinal >= 0, "negative case ordinal");
        validate_digest(&self.case_hash)?;
        let case: super::super::run::DatasetCase = serde_json::from_value(self.source.clone())?;
        ensure!(!case.task_type.trim().is_empty(), "case task_type is empty");
        ensure!(
            super::identity::case_hash(&case)? == self.case_hash,
            "normalized case hash mismatch"
        );
        Ok(case)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunChunk {
    pub(crate) chunk_index: i32,
    pub(crate) chunk_id: Uuid,
    pub(crate) run_shard: i16,
    pub(crate) ordinal_start: i32,
    pub(crate) ordinal_end: i32,
    pub(crate) profile_group_id: String,
    pub(crate) case_projection_hash: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunChunkPage {
    pub(crate) schema: StorageFormat,
    pub(crate) run_id: Uuid,
    pub(crate) chunk_index_start: i32,
    pub(crate) chunk_index_end: i32,
    pub(crate) chunks: Vec<RunChunk>,
}

impl StorageDocument for RunChunkPage {
    const SCHEMA: StorageFormat = StorageFormat::RunChunks;
    fn validate(&self, limits: &FormatLimits) -> Result<()> {
        schema(self.schema, Self::SCHEMA)?;
        range(self.chunk_index_start, self.chunk_index_end)?;
        ensure!(
            !self.chunks.is_empty()
                && self.chunks.len() <= limits.page_entries
                && self.chunk_index_end - self.chunk_index_start
                    == i32::try_from(self.chunks.len())?,
            "invalid chunk page count"
        );
        let mut ids = BTreeSet::new();
        let mut previous_end = None;
        for (index, chunk) in self.chunks.iter().enumerate() {
            ensure!(
                chunk.chunk_index == self.chunk_index_start + i32::try_from(index)?,
                "nonconsecutive chunk indexes"
            );
            range(chunk.ordinal_start, chunk.ordinal_end)?;
            ensure!(
                chunk.ordinal_end - chunk.ordinal_start <= 100,
                "execution chunk exceeds 100 cases"
            );
            ensure!(
                previous_end.is_none_or(|end| end == chunk.ordinal_start),
                "chunk case ranges have a gap or overlap"
            );
            previous_end = Some(chunk.ordinal_end);
            shard(chunk.run_shard)?;
            ensure!(ids.insert(chunk.chunk_id), "duplicate chunk ID");
            ensure!(
                !chunk.profile_group_id.trim().is_empty() && chunk.profile_group_id.len() <= 1024,
                "invalid profile group ID"
            );
            validate_digest(&chunk.case_projection_hash)?;
        }
        Ok(())
    }
}

/// Expected case count for one gate on the containing logical shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GateCount {
    pub(crate) gate_id: String,
    pub(crate) planned_count: i32,
}

/// One stored file of expected gate counts for a logical shard.
/// The next reference continues the same shard's list when more entries remain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GateCountPage {
    pub(crate) schema: StorageFormat,
    pub(crate) run_shard: i16,
    pub(crate) entries: Vec<GateCount>,
    #[serde(deserialize_with = "required_nullable")]
    pub(crate) next: Option<ObjectRef>,
}

impl StorageDocument for GateCountPage {
    const SCHEMA: StorageFormat = StorageFormat::GateCounts;
    fn validate(&self, limits: &FormatLimits) -> Result<()> {
        schema(self.schema, Self::SCHEMA)?;
        shard(self.run_shard)?;
        ensure!(
            !self.entries.is_empty() && self.entries.len() <= limits.page_entries,
            "invalid gate page entry count"
        );
        let mut previous = None;
        for entry in &self.entries {
            ensure!(
                !entry.gate_id.trim().is_empty()
                    && entry.gate_id.len() <= 1024
                    && entry.planned_count >= 0,
                "invalid gate count"
            );
            ensure!(
                previous.is_none_or(|id: &str| id < entry.gate_id.as_str()),
                "gate IDs are not strictly ordered"
            );
            previous = Some(entry.gate_id.as_str());
        }
        if let Some(next) = &self.next {
            next.require(Self::SCHEMA, limits)?;
        }
        Ok(())
    }
}

/// Manifest reference to the first count file and exact file/entry totals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GateCountDescriptor {
    #[serde(deserialize_with = "required_nullable")]
    pub(crate) first: Option<ObjectRef>,
    pub(crate) page_count: i32,
    pub(crate) entry_count: i32,
}

impl GateCountDescriptor {
    fn validate(&self, limits: &FormatLimits) -> Result<()> {
        ensure!(
            self.page_count >= 0 && self.entry_count >= 0,
            "negative gate descriptor count"
        );
        ensure!(
            (self.page_count == 0) == (self.entry_count == 0)
                && self.first.is_none() == (self.page_count == 0),
            "inconsistent empty gate descriptor"
        );
        ensure!(
            self.page_count <= self.entry_count
                && i64::from(self.entry_count)
                    <= i64::from(self.page_count) * i64::try_from(limits.page_entries)?,
            "impossible gate page/entry counts"
        );
        if let Some(first) = &self.first {
            first.require(StorageFormat::GateCounts, limits)?;
        }
        Ok(())
    }
}

/// Validates a logical shard's linked gate-count files; this state is not stored.
/// Exact counts bound traversal.
/// Strict cross-page ordering and terminal/count checks reject cycles without a
/// chain-wide reference set. Callers verify each expected reference's bytes first.
#[derive(Debug)]
pub(crate) struct GateCountChainValidator {
    next: Option<ObjectRef>,
    remaining_pages: i32,
    remaining_entries: i32,
    run_shard: i16,
    case_count: i32,
    last_gate: Option<String>,
}

impl GateCountChainValidator {
    pub(crate) fn new(
        descriptor: &GateCountDescriptor,
        run_shard: i16,
        case_count: i32,
        limits: &FormatLimits,
    ) -> Result<Self> {
        limits.validate()?;
        descriptor.validate(limits)?;
        shard(run_shard)?;
        ensure!(case_count > 0, "invalid shard case count");
        Ok(Self {
            next: descriptor.first.clone(),
            remaining_pages: descriptor.page_count,
            remaining_entries: descriptor.entry_count,
            run_shard,
            case_count,
            last_gate: None,
        })
    }

    pub(crate) fn next(&self) -> Option<&ObjectRef> {
        self.next.as_ref()
    }

    /// Advances only after every check succeeds; a failed page leaves state intact.
    pub(crate) fn accept(
        &mut self,
        reference: &ObjectRef,
        page: &GateCountPage,
        limits: &FormatLimits,
    ) -> Result<()> {
        page.validate(limits)?;
        ensure!(
            self.next.as_ref() == Some(reference) && page.run_shard == self.run_shard,
            "unexpected gate page or shard"
        );
        ensure!(
            self.last_gate
                .as_ref()
                .is_none_or(|last| last < &page.entries[0].gate_id),
            "gate chain repeats or reorders IDs"
        );
        ensure!(
            page.entries
                .iter()
                .all(|entry| entry.planned_count <= self.case_count),
            "gate denominator exceeds shard case count"
        );
        let pages = self
            .remaining_pages
            .checked_sub(1)
            .ok_or_else(|| anyhow::anyhow!("gate page count overflow"))?;
        let entries = self
            .remaining_entries
            .checked_sub(i32::try_from(page.entries.len())?)
            .ok_or_else(|| anyhow::anyhow!("gate entry count overflow"))?;
        ensure!(
            pages >= 0
                && entries >= 0
                && (pages == 0) == page.next.is_none()
                && (pages == 0) == (entries == 0),
            "gate chain count/termination mismatch"
        );
        if let Some(next) = &page.next {
            ensure!(
                next.storage_alias == reference.storage_alias && next != reference,
                "gate cycle or owner mismatch"
            );
        }
        self.remaining_pages = pages;
        self.remaining_entries = entries;
        self.last_gate = page.entries.last().map(|entry| entry.gate_id.clone());
        self.next = page.next.clone();
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<()> {
        ensure!(
            self.next.is_none() && self.remaining_pages == 0 && self.remaining_entries == 0,
            "incomplete gate chain"
        );
        Ok(())
    }
}

/// One logical shard's planned case/chunk totals, membership hash, and gate counts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunPlanShard {
    pub(crate) run_shard: i16,
    pub(crate) case_count: i32,
    pub(crate) chunk_count: i32,
    pub(crate) planned_chunk_hash: String,
    pub(crate) gate_counts: GateCountDescriptor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunPlanManifest {
    pub(crate) schema: StorageFormat,
    pub(crate) run_id: Uuid,
    pub(crate) dataset: ObjectRef,
    pub(crate) profile_hash: String,
    pub(crate) aggregation_policy_hash: String,
    pub(crate) execution_plan_hash: String,
    pub(crate) configuration: ObjectRef,
    pub(crate) chunk_count: i32,
    pub(crate) chunks: IndexDescriptor,
    pub(crate) shards: Vec<RunPlanShard>,
}

impl StorageDocument for RunPlanManifest {
    const SCHEMA: StorageFormat = StorageFormat::RunPlanManifest;
    fn validate(&self, limits: &FormatLimits) -> Result<()> {
        schema(self.schema, Self::SCHEMA)?;
        self.dataset
            .require(StorageFormat::DatasetManifest, limits)?;
        self.configuration
            .require(StorageFormat::RunConfiguration, limits)?;
        self.chunks.validate(limits)?;
        ensure!(
            self.chunk_count > 0
                && self.chunks.range_start == 0
                && self.chunks.range_end == self.chunk_count,
            "run index must cover every chunk"
        );
        for digest in [
            &self.profile_hash,
            &self.aggregation_policy_hash,
            &self.execution_plan_hash,
        ] {
            validate_digest(digest)?;
        }
        ensure!(
            !self.shards.is_empty() && self.shards.len() <= 128,
            "invalid plan shard count"
        );
        let mut previous = None;
        let mut cases = 0_i32;
        let mut chunks = 0_i32;
        for item in &self.shards {
            shard(item.run_shard)?;
            ensure!(
                previous.is_none_or(|value| value < item.run_shard),
                "plan shards must be strictly ordered"
            );
            previous = Some(item.run_shard);
            ensure!(
                item.case_count > 0
                    && item.chunk_count > 0
                    && item.chunk_count <= item.case_count
                    && i64::from(item.case_count) <= i64::from(item.chunk_count) * 100,
                "invalid shard case/chunk counts"
            );
            validate_digest(&item.planned_chunk_hash)?;
            item.gate_counts.validate(limits)?;
            cases = cases
                .checked_add(item.case_count)
                .ok_or_else(|| anyhow::anyhow!("plan case count overflow"))?;
            chunks = chunks
                .checked_add(item.chunk_count)
                .ok_or_else(|| anyhow::anyhow!("plan chunk count overflow"))?;
            if let Some(first) = &item.gate_counts.first {
                ensure!(
                    first.storage_alias == self.configuration.storage_alias,
                    "gate storage owner mismatch"
                );
            }
        }
        ensure!(
            chunks == self.chunk_count,
            "plan shard chunk totals disagree"
        );
        ensure!(
            self.configuration.storage_alias == self.chunks.object.storage_alias,
            "plan storage owner mismatch"
        );
        Ok(())
    }
}

/// Configuration contents retain the existing application snapshot vocabulary.
/// Profile/evaluator semantics are checked by run planning, not the object codec.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunConfiguration {
    pub(crate) schema: StorageFormat,
    pub(crate) snapshot: Value,
}

/// Compact logical dataset identity inside the configuration snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LogicalDatasetRef {
    pub(crate) dataset_id: Uuid,
    #[serde(deserialize_with = "required_nullable")]
    pub(crate) dataset_version: Option<String>,
    pub(crate) dataset_version_id: Uuid,
    pub(crate) dataset_hash: String,
    pub(crate) case_count: i32,
}

impl StorageDocument for RunConfiguration {
    const SCHEMA: StorageFormat = StorageFormat::RunConfiguration;
    fn validate(&self, _: &FormatLimits) -> Result<()> {
        schema(self.schema, Self::SCHEMA)?;
        let fields = self
            .snapshot
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("configuration snapshot must be an object"))?;
        const FIELDS: &[&str] = &[
            "profile",
            "agent",
            "dataset_ref",
            "dataset_version_id",
            "profile_version_id",
            "profile_hash",
            "dataset_hash",
            "aggregation_policy_hash",
            "execution_plan_hash",
            "execution_plan",
            "chunk_size",
            "executability",
        ];
        ensure!(
            fields.len() == FIELDS.len() && FIELDS.iter().all(|field| fields.contains_key(*field)),
            "unexpected or missing configuration snapshot fields"
        );
        ensure!(
            self.snapshot["chunk_size"]
                .as_u64()
                .is_some_and(|size| (1..=100).contains(&size)),
            "invalid execution chunk size"
        );
        ensure!(
            self.snapshot["profile"].is_object()
                && self.snapshot["agent"].is_object()
                && self.snapshot["dataset_ref"].is_object()
                && self.snapshot["execution_plan"].is_object()
                && self.snapshot["executability"].is_object(),
            "invalid configuration snapshot shape"
        );
        for field in [
            "profile_hash",
            "dataset_hash",
            "aggregation_policy_hash",
            "execution_plan_hash",
        ] {
            validate_digest(
                self.snapshot[field]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("missing configuration hash {field}"))?,
            )?;
        }
        let dataset: LogicalDatasetRef =
            serde_json::from_value(self.snapshot["dataset_ref"].clone())?;
        ensure!(
            dataset.case_count > 0 && dataset.dataset_hash == self.snapshot["dataset_hash"],
            "configuration dataset identity mismatch"
        );
        let version: Uuid = serde_json::from_value(self.snapshot["dataset_version_id"].clone())?;
        ensure!(
            version == dataset.dataset_version_id,
            "configuration dataset version mismatch"
        );
        ensure!(
            self.snapshot["profile_version_id"]
                .as_str()
                .is_some_and(|id| !id.trim().is_empty()),
            "invalid profile version identity"
        );
        let profile: crate::contracts::run::RunProfile =
            serde_json::from_value(self.snapshot["profile"].clone())?;
        let agent: crate::contracts::run::AgentProfile =
            serde_json::from_value(self.snapshot["agent"].clone())?;
        ensure!(
            serde_json::to_value(&profile.agent)? == serde_json::to_value(agent)?,
            "profile/agent configuration mismatch"
        );
        let plan: crate::contracts::evaluator_abi::EvaluatorExecutionPlan =
            serde_json::from_value(self.snapshot["execution_plan"].clone())?;
        ensure!(
            plan.version == 1
                && plan.aggregation_policy_hash == self.snapshot["aggregation_policy_hash"]
                && plan.hash()? == self.snapshot["execution_plan_hash"],
            "configuration execution plan identity mismatch"
        );
        ensure!(
            super::identity::hash_serialized_json(&super::identity::canonical_json(
                &self.snapshot["profile"]
            ))? == self.snapshot["profile_hash"],
            "configuration profile hash mismatch"
        );
        let summary = self.snapshot["executability"]
            .as_object()
            .expect("validated object");
        const COUNTS: &[&str] = &[
            "expected_execution_count",
            "expected_evaluator_execution_count",
            "unique_evaluator_ref_count",
            "runnable_evaluator_ref_count",
        ];
        ensure!(
            summary.len() == COUNTS.len()
                && COUNTS.iter().all(|field| summary
                    .get(*field)
                    .is_some_and(|count| count.as_u64().is_some())),
            "invalid executability count fields"
        );
        // Per-case lists belong only in case batches, never configuration snapshots.
        ensure!(
            self.snapshot["profile"].get("cases").is_none()
                && self.snapshot["dataset_ref"].get("cases").is_none(),
            "case payloads are forbidden in configuration"
        );
        Ok(())
    }
}
