//! Canonical case identity and incremental ordered dataset identities.
//!
//! Source hashing preserves original fields, nulls, tags, and order. Case hashing
//! normalizes optional fields and tags. Dataset-version hashing uses normalized
//! membership. Hash preimages match the CLI's golden fixtures exactly; envelopes,
//! batching settings, and provider locations never enter logical identity.

use std::collections::BTreeMap;

use anyhow::{
    Result,
    ensure,
};
use serde_json::{
    Value,
    json,
};
use uuid::Uuid;

use super::{
    CaseRecord,
    DatasetSourceFields,
    FormatLimits,
    StorageDocument,
};
use crate::contracts::run::DatasetCase;

/// Logical identity binding a resumable upload to its complete ordered source.
/// Physical batching, storage locations and lease generations do not enter it.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DatasetIdentity {
    pub dataset_id: Uuid,
    pub dataset_version: Option<String>,
    pub dataset_version_id: Uuid,
    pub dataset_hash: String,
    pub case_count: i32,
}
/// Sorts object keys recursively while preserving array order and field presence.
pub(crate) fn canonical_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let sorted: BTreeMap<_, _> = map
                .iter()
                .map(|(key, value)| (key.clone(), canonical_json(value)))
                .collect();
            Value::Object(sorted.into_iter().collect())
        }
        Value::Array(items) => Value::Array(items.iter().map(canonical_json).collect()),
        _ => value.clone(),
    }
}

/// Serializes a JSON value compactly and returns the BLAKE3 digest of those bytes.
/// Callers prepare canonical ordering and normalization; this helper adds neither
/// normalization nor a trailing newline.
pub(crate) fn hash_serialized_json(value: &Value) -> Result<String> {
    Ok(blake3::hash(&serde_json::to_vec(value)?)
        .to_hex()
        .to_string())
}

/// Keeps the pre-release version-8 UUID encoding fixed across all producers.
pub(crate) fn uuid_from_json(value: &Value) -> Result<Uuid> {
    Ok(uuid_from_digest(blake3::hash(&serde_json::to_vec(value)?)))
}

fn uuid_from_digest(digest: blake3::Hash) -> Uuid {
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

/// Builds normalized case content for hashing and CLI persistence; excludes the case ID.
pub(crate) fn normalized_case_payload(case: &DatasetCase) -> Result<Value> {
    let mut tags = case.tags.clone();
    tags.sort();
    Ok(json!({
        "task_type": case.task_type,
        "case_group": case.case_group,
        "input": canonical_json(&case.input),
        "expected_output": canonical_json(case.expected.as_ref().unwrap_or(&Value::Null)),
        "context": canonical_json(case.context.as_ref().unwrap_or(&Value::Null)),
        "tags": tags,
        "metadata": canonical_json(&serde_json::to_value(&case.metadata)?),
    }))
}

pub(crate) fn case_hash(case: &DatasetCase) -> Result<String> {
    hash_serialized_json(&normalized_case_payload(case)?)
}

/// Incrementally builds the source hash, dataset-version UUID, and case count.
/// Keeps running hash state and bounded top-level fields without retaining cases.
/// Callers independently detect duplicate case IDs with bounded disk sorting.
/// A failed append never advances either hash or the expected ordinal.
#[derive(Debug)]
pub(crate) struct DatasetIdentityBuilder {
    record_bytes: usize,
    dataset_id: Uuid,
    dataset_version: Option<String>,
    source: blake3::Hasher,
    membership: blake3::Hasher,
    suffix: Vec<u8>,
    ordinal: i32,
}

impl DatasetIdentityBuilder {
    pub(crate) fn new(fields: &DatasetSourceFields, limits: &FormatLimits) -> Result<Self> {
        // Admission applies even when callers construct types without decoding.
        super::encode(fields, limits)?;
        fields.validate(limits)?;
        ensure!(
            fields.cases_present,
            "nonempty datasets require cases_present"
        );
        let dataset: crate::contracts::run::RunDataset =
            serde_json::from_value(fields.fields.clone())?;
        let prefix = serde_json::to_vec(
            &json!({ "dataset_id": dataset.dataset_id, "dataset_version": dataset.dataset_version }),
        )?;
        let mut membership = blake3::Hasher::new();
        membership.update(&prefix[..prefix.len() - 1]);
        membership.update(b",\"membership\":[");
        let mut source = blake3::Hasher::new();
        source.update(b"{");
        let mut before = true;
        let mut separator = false;
        let mut suffix = Vec::new();
        for (key, value) in canonical_json(&fields.fields)
            .as_object()
            .expect("validated fields")
        {
            if before && key.as_str() > "cases" {
                if separator {
                    source.update(b",");
                }
                source.update(b"\"cases\":[");
                before = false;
            }
            let encoded = serde_json::to_vec(&canonical_json(value))?;
            let key = serde_json::to_vec(key)?;
            if before {
                if separator {
                    source.update(b",");
                }
                source.update(&key).update(b":").update(&encoded);
                separator = true;
            } else {
                suffix.push(b',');
                suffix.extend_from_slice(&key);
                suffix.push(b':');
                suffix.extend_from_slice(&encoded);
            }
        }
        if before {
            if separator {
                source.update(b",");
            }
            source.update(b"\"cases\":[");
        }
        suffix.push(b'}');
        Ok(Self {
            record_bytes: limits.batch.max_bytes,
            dataset_id: dataset.dataset_id,
            dataset_version: dataset.dataset_version,
            source,
            membership,
            suffix,
            ordinal: 0,
        })
    }

    /// Appends one verified source record in submitted order without retaining it.
    pub(crate) fn push(&mut self, record: &CaseRecord) -> Result<()> {
        ensure!(
            record.case_ordinal == self.ordinal,
            "dataset identity ordinal gap or overlap"
        );
        let next = self
            .ordinal
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("dataset case count exceeds i32"))?;
        // Bound caller-constructed records before typed validation clones values.
        super::codec::compact(record, self.record_bytes)?;
        let case = record.validate()?;
        let source = serde_json::to_vec(&canonical_json(&record.source))?;
        let membership = serde_json::to_vec(
            &json!({ "case_id": case.id, "case_ordinal": record.case_ordinal, "case_hash": record.case_hash }),
        )?;
        if self.ordinal > 0 {
            self.source.update(b",");
            self.membership.update(b",");
        }
        self.source.update(&source);
        self.membership.update(&membership);
        self.ordinal = next;
        Ok(())
    }

    /// Returns source hash, normalized dataset-version UUID, and exact case count.
    pub(crate) fn finish(mut self) -> Result<(String, Uuid, i32)> {
        ensure!(self.ordinal > 0, "dataset must contain at least one case");
        self.source.update(b"]").update(&self.suffix);
        self.membership.update(b"]}");
        Ok((
            self.source.finalize().to_hex().to_string(),
            uuid_from_digest(self.membership.finalize()),
            self.ordinal,
        ))
    }

    /// Checks logical identity independently of the manifest's object digest.
    /// This does not replace external duplicate-ID or descendant verification.
    pub(crate) fn verify(
        self,
        manifest: &super::DatasetManifest,
        limits: &FormatLimits,
    ) -> Result<()> {
        manifest.validate(limits)?;
        ensure!(
            self.dataset_id == manifest.dataset_id
                && self.dataset_version == manifest.dataset_version,
            "manifest/source dataset identity mismatch"
        );
        let (hash, version, count) = self.finish()?;
        ensure!(
            hash == manifest.dataset_hash
                && version == manifest.dataset_version_id
                && count == manifest.case_count,
            "manifest/source hash, version, or case count mismatch"
        );
        Ok(())
    }
}
