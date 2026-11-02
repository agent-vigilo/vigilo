//! Durable response evidence shared by all evaluators of one attempt.
//!
//! Capture packs original and decoded bytes once, replacing invocation handles
//! with checked byte ranges. Metadata freezes mapping and execution identities;
//! full retention keeps bytes, while summary explicitly records their omission.
//! Database workflows commit this packet with the authoritative attempt transition.
//! Reconstruction validates the packet before returning any evidence. Artifact
//! availability is a separate check: a recorded digest alone never proves replay.

use std::{
    collections::BTreeMap,
    sync::Arc,
};

use anyhow::{
    Context,
    ensure,
};
use serde::{
    Deserialize,
    Serialize,
};
use uuid::Uuid;
use vigilo::{
    response::{
        CapturedBody,
        CompletionStatus,
        ContentPart,
        OutputItem,
        ResponseConfiguration,
        StopReason,
    },
    response_mapping::{
        IMPLEMENTATION_ID,
        MappedResponse,
        MappedSource,
    },
};

use super::{
    evaluator_abi::EvaluatorExecutionPlan,
    run::PersistenceMode,
};

const MAX_PAYLOAD: usize = 20 * 1024 * 1024;
const MAX_METADATA: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Provenance {
    pub(crate) run_id: Uuid,
    pub(crate) execution_id: Uuid,
    pub(crate) attempt_id: Uuid,
    pub(crate) case_hash: String,
    pub(crate) profile_hash: String,
    /// Resolves to the immutable plan already stored in the run snapshot.
    pub(crate) execution_plan_hash: String,
    /// Digest of the exact executable, not its human-readable package version.
    pub(crate) runtime_artifact_digest: String,
    /// External calls cannot be reproduced unless their responses are retained.
    pub(crate) external_dependencies: ExternalDependencies,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExternalDependencies {
    None,
    NotRetained,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) enum Version {
    #[serde(rename = "1.0.0")]
    V1,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ByteRange {
    offset: u64,
    byte_length: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "representation", rename_all = "snake_case", deny_unknown_fields)]
enum StoredSource {
    InlineText { range: ByteRange },
    OriginalRange { offset: u64, byte_length: u64 },
    Decoded { range: ByteRange },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Metadata {
    schema_version: Version,
    provenance: Provenance,
    retention: PersistenceMode,
    mapping_implementation: String,
    configuration: Option<ResponseConfiguration>,
    configuration_hash: String,
    original_length: Option<u64>,
    original_hash: Option<String>,
    content_type: Option<String>,
    items: Option<Vec<OutputItem<StoredSource>>>,
    status: Option<CompletionStatus>,
    stop_reason: Option<StopReason>,
    /// Safe classification only, never parser excerpts or response text.
    mapping_error: Option<String>,
    payload_hash: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    metadata_hash: String,
}

/// Validated packet. Byte contents are intentionally absent from Debug output.
#[derive(Clone)]
pub(crate) struct RetainedResponse {
    metadata: Arc<Metadata>,
    payload: Arc<Vec<u8>>,
}
impl std::fmt::Debug for RetainedResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetainedResponse")
            .field("attempt_id", &self.metadata.provenance.attempt_id)
            .field("payload_bytes", &self.payload.len())
            .finish_non_exhaustive()
    }
}

/// Compact authority checked by SQL without copying the profile out of blob
/// storage. Hash the typed configuration exactly as capture does, including null.
pub(crate) fn policy_identity(
    profile: &super::run::RunProfile,
) -> anyhow::Result<serde_json::Value> {
    Ok(serde_json::json!({
        "retention": profile.persistence.mode,
        "configuration_hash": configuration_hash(profile.agent.response.as_ref())?,
    }))
}

fn configuration_hash(configuration: Option<&ResponseConfiguration>) -> anyhow::Result<String> {
    Ok(hash(&bounded_json(&configuration)?))
}

impl RetainedResponse {
    /// Builds evidence without I/O. Mapping failure keeps raw evidence; no partial
    /// mapped output is accepted. Summary mode retains identities but no content.
    pub(crate) fn capture(
        provenance: Provenance,
        retention: PersistenceMode,
        body: Option<&CapturedBody>,
        configuration: Option<&ResponseConfiguration>,
        mapping: Option<Result<&MappedResponse, &vigilo::response_mapping::MappingError>>,
    ) -> anyhow::Result<Self> {
        let full = matches!(retention, PersistenceMode::Full);
        ensure!(
            body.is_none_or(|body| body.bytes().len() <= 4 * 1024 * 1024),
            "original response exceeds retention limit"
        );
        let configuration_hash = configuration_hash(configuration)?;
        let mut payload = if full {
            body.map(|body| body.bytes().to_vec()).unwrap_or_default()
        } else {
            Vec::new()
        };
        let mut metadata = Metadata {
            schema_version: Version::V1,
            provenance,
            retention,
            mapping_implementation: IMPLEMENTATION_ID.into(),
            configuration: full.then(|| configuration.cloned()).flatten(),
            configuration_hash,
            original_length: body.map(|body| body.bytes().len() as u64),
            original_hash: body.map(|body| hash(body.bytes())),
            content_type: if full {
                body.and_then(|body| body.content_type().map(str::to_owned))
            } else {
                None
            },
            items: None,
            status: None,
            stop_reason: None,
            mapping_error: None,
            payload_hash: String::new(),
            metadata_hash: String::new(),
        };
        match mapping {
            Some(Ok(mapped)) => {
                ensure!(
                    body.is_some_and(|body| body.bytes() == mapped.original.bytes()
                        && body.content_type() == mapped.original.content_type()),
                    "mapped response does not belong to original evidence"
                );
                if full {
                    let mut resources = BTreeMap::<String, ByteRange>::new();
                    if let Some(items) = &mapped.items {
                        ensure!(items.len() <= 4096, "retained item limit exceeded");
                        let mut parts = 0usize;
                        let mut convert = |source: &MappedSource| -> anyhow::Result<StoredSource> {
                            parts += 1;
                            ensure!(parts <= 8192, "retained content limit exceeded");
                            Ok(match source {
                                MappedSource::InlineText { text } => {
                                    ensure!(
                                        payload
                                            .len()
                                            .checked_add(text.len())
                                            .is_some_and(|size| size <= MAX_PAYLOAD),
                                        "retained payload limit exceeded"
                                    );
                                    let range = ByteRange {
                                        offset: payload.len() as u64,
                                        byte_length: text.len() as u64,
                                    };
                                    payload.extend_from_slice(text.as_bytes());
                                    StoredSource::InlineText { range }
                                }
                                MappedSource::BodyRange {
                                    offset,
                                    byte_length,
                                } => {
                                    checked_range(mapped.original.bytes(), *offset, *byte_length)?;
                                    StoredSource::OriginalRange {
                                        offset: *offset,
                                        byte_length: *byte_length,
                                    }
                                }
                                MappedSource::Resource { body } => {
                                    let digest = hash(body.bytes());
                                    let range = if let Some(range) = resources.get(&digest) {
                                        range.clone()
                                    } else {
                                        ensure!(
                                            resources.len() < 63,
                                            "retained resource limit exceeded"
                                        );
                                        ensure!(
                                            payload
                                                .len()
                                                .checked_add(body.bytes().len())
                                                .is_some_and(|size| size <= MAX_PAYLOAD),
                                            "retained payload limit exceeded"
                                        );
                                        let range = ByteRange {
                                            offset: payload.len() as u64,
                                            byte_length: body.bytes().len() as u64,
                                        };
                                        payload.extend_from_slice(body.bytes());
                                        resources.insert(digest, range.clone());
                                        range
                                    };
                                    StoredSource::Decoded { range }
                                }
                            })
                        };
                        metadata.items = Some(
                            items
                                .iter()
                                .map(|item| transform(item, &mut convert))
                                .collect::<anyhow::Result<_>>()?,
                        );
                    }
                    metadata.status = mapped.status;
                    metadata.stop_reason = mapped.stop_reason.clone();
                }
            }
            Some(Err(error)) => metadata.mapping_error = Some(format!("{:?}", error.kind)),
            None => {}
        }
        metadata.payload_hash = hash(&payload);
        metadata.metadata_hash = hash(&bounded_json(&metadata)?);
        let packet = Self {
            metadata: Arc::new(metadata),
            payload: Arc::new(payload),
        };
        packet.validate()?;
        Ok(packet)
    }
    pub(crate) fn validate_owner(
        &self,
        run_id: Uuid,
        execution_id: Uuid,
        attempt_id: Uuid,
    ) -> anyhow::Result<()> {
        let owner = &self.metadata.provenance;
        ensure!(
            (owner.run_id, owner.execution_id, owner.attempt_id)
                == (run_id, execution_id, attempt_id),
            "retained response owner mismatch"
        );
        Ok(())
    }
    pub(crate) fn metadata_json(&self) -> anyhow::Result<serde_json::Value> {
        Ok(serde_json::from_slice(&bounded_json(
            self.metadata.as_ref(),
        )?)?)
    }
    pub(crate) fn payload(&self) -> &[u8] {
        &self.payload
    }
    pub(crate) fn from_storage(
        metadata: serde_json::Value,
        payload: Vec<u8>,
    ) -> anyhow::Result<Self> {
        ensure!(
            payload.len() <= MAX_PAYLOAD,
            "retained payload limit exceeded"
        );
        let metadata = serde_json::from_slice(&bounded_json(&metadata)?)
            .context("invalid retained response metadata")?;
        let packet = Self {
            metadata: Arc::new(metadata),
            payload: Arc::new(payload),
        };
        packet.validate()?;
        Ok(packet)
    }
    fn validate(&self) -> anyhow::Result<()> {
        bounded_json(self.metadata.as_ref())?;
        let mut content = self.metadata.as_ref().clone();
        content.metadata_hash.clear();
        ensure!(
            hash(&bounded_json(&content)?) == self.metadata.metadata_hash,
            "retained metadata integrity failure"
        );
        ensure!(
            self.payload.len() <= MAX_PAYLOAD && hash(&self.payload) == self.metadata.payload_hash,
            "retained payload integrity failure"
        );
        ensure!(
            self.metadata.provenance.runtime_artifact_digest.len() == 64
                && self
                    .metadata
                    .provenance
                    .runtime_artifact_digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "runtime artifact identity is required"
        );
        if matches!(self.metadata.retention, PersistenceMode::Summary) {
            ensure!(
                self.payload.is_empty()
                    && self.metadata.items.is_none()
                    && self.metadata.configuration.is_none()
                    && self.metadata.content_type.is_none(),
                "summary retention contains response content"
            );
        } else {
            ensure!(
                hash(&bounded_json(&self.metadata.configuration)?)
                    == self.metadata.configuration_hash,
                "retained configuration integrity failure"
            );
            if let Some(length) = self.metadata.original_length {
                ensure!(
                    length <= 4 * 1024 * 1024,
                    "original response exceeds retention limit"
                );
                ensure!(
                    self.metadata.original_hash.as_deref()
                        == Some(hash(checked_range(&self.payload, 0, length)?).as_str()),
                    "original response integrity failure"
                );
            } else {
                ensure!(
                    self.payload.is_empty() && self.metadata.items.is_none(),
                    "response data has no original body"
                );
            }
            ensure!(
                self.metadata.mapping_error.is_none() || self.metadata.items.is_none(),
                "failed mapping contains partial output"
            );
            let mut ranges = std::collections::BTreeSet::new();
            let mut decoded_ranges = std::collections::BTreeSet::new();
            let mut content_bytes = 0u64;
            let mut parts = 0;
            if let Some(items) = &self.metadata.items {
                ensure!(items.len() <= 4096, "retained item limit exceeded");
                for item in items {
                    let content = match item {
                        OutputItem::Message { content, .. }
                        | OutputItem::ToolResult { content, .. } => content.as_slice(),
                        OutputItem::ToolCall { arguments, .. } => std::slice::from_ref(arguments),
                    };
                    parts += content.len();
                    ensure!(parts <= 8192, "retained content limit exceeded");
                    for part in content {
                        let length = match &part.source {
                            StoredSource::InlineText { range }
                            | StoredSource::Decoded { range } => range.byte_length,
                            StoredSource::OriginalRange { byte_length, .. } => *byte_length,
                        };
                        content_bytes = content_bytes
                            .checked_add(length)
                            .context("retained content size overflow")?;
                        ensure!(
                            content_bytes <= 16 * 1024 * 1024,
                            "retained logical content limit exceeded"
                        );
                        match &part.source {
                            StoredSource::InlineText { range } => {
                                std::str::from_utf8(checked_range(
                                    &self.payload,
                                    range.offset,
                                    range.byte_length,
                                )?)?;
                                ranges.insert((range.offset, range.byte_length));
                            }
                            StoredSource::OriginalRange {
                                offset,
                                byte_length,
                            } => {
                                checked_range(
                                    checked_range(
                                        &self.payload,
                                        0,
                                        self.metadata
                                            .original_length
                                            .context("original body missing")?,
                                    )?,
                                    *offset,
                                    *byte_length,
                                )?;
                            }
                            StoredSource::Decoded { range } => {
                                decoded_ranges.insert((range.offset, range.byte_length));
                                ensure!(
                                    decoded_ranges.len() <= 63,
                                    "retained resource limit exceeded"
                                );
                                checked_range(&self.payload, range.offset, range.byte_length)?;
                                ranges.insert((range.offset, range.byte_length));
                            }
                        }
                    }
                }
            }
            ensure!(ranges.len() <= 8192, "retained content limit exceeded");
            let mut end = self.metadata.original_length.unwrap_or(0);
            for (offset, length) in ranges {
                ensure!(offset == end, "retained resource layout is not contiguous");
                end = offset
                    .checked_add(length)
                    .context("retained range overflow")?;
            }
            ensure!(
                end == self.payload.len() as u64,
                "retained payload has unreferenced bytes"
            );
        }
        Ok(())
    }
    /// Recreates immutable content only; callers must bind fresh host handles.
    /// This does not claim evaluator/runtime artifacts or external calls exist.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Internal reconstruction is tested; no public replay command is provided"
        )
    )]
    pub(crate) fn reconstruct(&self) -> anyhow::Result<MappedResponse> {
        ensure!(
            matches!(self.metadata.retention, PersistenceMode::Full),
            "response evidence omitted by summary retention"
        );
        ensure!(
            self.metadata.mapping_implementation == IMPLEMENTATION_ID,
            "unsupported response mapping implementation"
        );
        let length = self
            .metadata
            .original_length
            .context("original response unavailable")?;
        let original = CapturedBody::new(
            checked_range(&self.payload, 0, length)?.to_vec(),
            self.metadata.content_type.clone(),
        );
        let mut resources = BTreeMap::<(u64, u64), CapturedBody>::new();
        let mut convert = |source: &StoredSource| -> anyhow::Result<MappedSource> {
            Ok(match source {
                StoredSource::InlineText { range } => MappedSource::InlineText {
                    text: std::str::from_utf8(checked_range(
                        &self.payload,
                        range.offset,
                        range.byte_length,
                    )?)?
                    .to_owned(),
                },
                StoredSource::OriginalRange {
                    offset,
                    byte_length,
                } => {
                    checked_range(original.bytes(), *offset, *byte_length)?;
                    MappedSource::BodyRange {
                        offset: *offset,
                        byte_length: *byte_length,
                    }
                }
                StoredSource::Decoded { range } => {
                    ensure!(
                        range.offset >= length,
                        "decoded resource overlaps original body"
                    );
                    let bytes = checked_range(&self.payload, range.offset, range.byte_length)?;
                    let body = resources
                        .entry((range.offset, range.byte_length))
                        .or_insert_with(|| CapturedBody::new(bytes.to_vec(), None))
                        .clone();
                    ensure!(resources.len() <= 63, "retained resource limit exceeded");
                    MappedSource::Resource { body }
                }
            })
        };
        let items = self
            .metadata
            .items
            .as_ref()
            .map(|items| {
                items
                    .iter()
                    .map(|item| transform(item, &mut convert))
                    .collect()
            })
            .transpose()?;
        Ok(MappedResponse {
            original,
            items,
            status: self.metadata.status,
            stop_reason: self.metadata.stop_reason.clone(),
        })
    }
    /// Identifies missing dependencies using caller-verified artifact digests.
    /// No database or network lookup occurs here, and nondeterminism is not hidden.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Internal reconstruction is tested; no public replay command is provided"
        )
    )]
    pub(crate) fn missing_dependencies(
        &self,
        plan: &EvaluatorExecutionPlan,
        available: &std::collections::BTreeSet<String>,
    ) -> anyhow::Result<Vec<String>> {
        ensure!(plan.version == 1, "unsupported evaluator execution plan");
        ensure!(
            plan.hash()? == self.metadata.provenance.execution_plan_hash,
            "retained execution plan identity mismatch"
        );
        let mut missing = Vec::new();
        if matches!(self.metadata.retention, PersistenceMode::Summary) {
            missing.push("response_evidence:summary_retention".into());
        }
        if self.metadata.original_length.is_none() {
            missing.push("original_response".into());
        }
        if !available.contains(&self.metadata.provenance.runtime_artifact_digest) {
            missing.push(format!(
                "runtime:{}",
                self.metadata.provenance.runtime_artifact_digest
            ));
        }
        for evaluator in &plan.evaluators {
            if !available.contains(&evaluator.content_hash) {
                missing.push(format!("evaluator:{}", evaluator.content_hash));
            }
        }
        if self.metadata.provenance.external_dependencies == ExternalDependencies::NotRetained {
            missing.push("external_dependency_responses".into());
        }
        Ok(missing)
    }
}
fn hash(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}
fn checked_range(bytes: &[u8], offset: u64, length: u64) -> anyhow::Result<&[u8]> {
    let start = usize::try_from(offset)?;
    let end = usize::try_from(
        offset
            .checked_add(length)
            .context("response range overflow")?,
    )?;
    bytes
        .get(start..end)
        .context("response range out of bounds")
}
fn bounded_json(value: &impl Serialize) -> anyhow::Result<Vec<u8>> {
    struct Writer(Vec<u8>);
    impl std::io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self
                .0
                .len()
                .checked_add(bytes.len())
                .is_none_or(|size| size > MAX_METADATA)
            {
                return Err(std::io::Error::other("response metadata limit exceeded"));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer(Vec::new());
    serde_json::to_writer(&mut writer, value)?;
    Ok(writer.0)
}
fn transform<S, T>(
    item: &OutputItem<S>,
    convert: &mut impl FnMut(&S) -> anyhow::Result<T>,
) -> anyhow::Result<OutputItem<T>> {
    let mut part = |part: &ContentPart<S>| -> anyhow::Result<ContentPart<T>> {
        Ok(ContentPart {
            media_type: part.media_type.clone(),
            source: convert(&part.source)?,
        })
    };
    Ok(match item {
        OutputItem::Message { id, role, content } => OutputItem::Message {
            id: id.clone(),
            role: role.clone(),
            content: content
                .iter()
                .map(&mut part)
                .collect::<anyhow::Result<_>>()?,
        },
        OutputItem::ToolCall {
            call_id,
            name,
            arguments,
        } => OutputItem::ToolCall {
            call_id: call_id.clone(),
            name: name.clone(),
            arguments: part(arguments)?,
        },
        OutputItem::ToolResult {
            call_id,
            content,
            is_error,
        } => OutputItem::ToolResult {
            call_id: call_id.clone(),
            content: content
                .iter()
                .map(&mut part)
                .collect::<anyhow::Result<_>>()?,
            is_error: *is_error,
        },
    })
}

#[cfg(test)]
pub(crate) mod tests;
