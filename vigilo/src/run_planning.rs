//! Bounded, resumable preparation of dataset-backed execution plans.
//!
//! Traverses the finalized dataset's verified index with a height-bounded stack,
//! checks routing and logical identity in one case pass, and publishes immutable
//! chunk/index/configuration/gate files. SQL holds exact file ownership plus at
//! most 128 shard totals; it never receives case data. Deterministic chunk IDs and
//! immutable range slots make interrupted preparation replayable under a new
//! creation lease. Verified plan readiness does not authorize worker dispatch.
use anyhow::{
    Context,
    Result,
    ensure,
};
use serde_json::{
    Value,
    json,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    blob_storage::{
        BlobStorage,
        Budget,
        config::OwnerConfig,
    },
    contracts::{
        case_storage::{
            identity::DatasetIdentityBuilder,
            *,
        },
        run::RunProfile,
    },
    dataset_ingestion::read_bytes,
    db::workflows::{
        dataset_upload::{
            Object,
            Upload,
        },
        run_create::RunShardPlacementAssignment,
        run_plan::{
            self as ledger,
            Lease,
            Plan,
        },
    },
};
mod accumulator;
mod dataset;
mod files;
use accumulator::Accumulator;
use dataset::DatasetCursor;
use files::Writer;

/// One caller's authority and admitted storage policy. Budgets are minted before
/// renewing the lease so expired callers cannot begin fresh remote operations.
pub(crate) struct Session<'a> {
    pub db: &'a sqlx::PgPool,
    pub store: &'a dyn BlobStorage,
    pub lease: Lease,
    pub limits: &'a FormatLimits,
    pub cancel: &'a CancellationToken,
    pub alias: &'a str,
    pub last_renewed: tokio::sync::Mutex<tokio::time::Instant>,
}
impl Session<'_> {
    async fn read(&self, reference: &ObjectRef) -> Result<Vec<u8>> {
        ensure!(!self.cancel.is_cancelled(), "run planning cancelled");
        let budget = Budget::standard();
        self.keep_alive().await?;
        read_bytes(self.store, reference, budget, self.cancel).await
    }
    /// Read-only traversal does not acquire write authority per case file.
    /// Renew periodically, well inside the 300-second lease; all writes still
    /// perform their own durable authority check before remote mutation.
    async fn keep_alive(&self) -> Result<()> {
        let mut last = self.last_renewed.lock().await;
        if last.elapsed() >= std::time::Duration::from_secs(30) {
            ledger::renew(self.db, self.lease).await?;
            *last = tokio::time::Instant::now();
        }
        Ok(())
    }
}

/// Durably own capability probes before opening an adapter; recover exact probes
/// from a killed process without deleting any dataset or published plan objects.
pub(crate) async fn connect(
    db: &sqlx::PgPool,
    plan: &Plan,
    lease: Lease,
    owner: &OwnerConfig,
    cancel: &CancellationToken,
) -> Result<Box<dyn BlobStorage>> {
    ensure!(
        owner.binding_hash()? == plan.storage_binding_hash,
        "storage owner binding changed"
    );
    let key = format!("capability-probes/{}/{}", plan.run_id, Uuid::now_v7());
    let object = Object {
        object_key: key.clone(),
        kind: "probe".into(),
        level: 0,
        range_start: None,
        range_end: None,
        reference: sqlx::types::Json(ObjectRef::for_bytes(
            owner.alias().into(),
            key.clone(),
            StorageFormat::DatasetManifest,
            b"probe-a",
            &plan.limits,
        )?),
        acknowledged: false,
    };
    let budget = Budget::standard();
    ledger::plan(db, lease, &object).await?;
    let store = owner
        .open(key, plan.limits.0.clone(), budget, cancel)
        .await?;
    ledger::acknowledge(db, lease, &object).await?;
    let probes = ledger::pending_probes(db, plan.run_id).await?;
    for probe in probes {
        let budget = Budget::standard();
        ledger::renew(db, lease).await?;
        store.remove_owned(&probe.reference, budget, cancel).await?;
        ledger::acknowledge(db, lease, &probe).await?;
    }
    Ok(store)
}

pub(crate) struct Request<'a> {
    pub plan: &'a Plan,
    pub upload: &'a Upload,
    pub dataset: &'a ObjectRef,
    pub profile: &'a RunProfile,
    pub profile_payload: &'a Value,
    pub assignments: &'a [RunShardPlacementAssignment],
    pub snapshot: Value,
}
/// Verify every case and exact chunk/shard/gate coverage before sealing a plan.
/// No transaction spans remote I/O; acknowledged files are verified on replay.
pub(crate) async fn prepare(session: &Session<'_>, request: Request<'_>) -> Result<ObjectRef> {
    let Request {
        plan,
        upload,
        dataset,
        profile,
        profile_payload,
        assignments,
        mut snapshot,
    } = request;
    ensure!(
        assignments == plan.assignments.0
            && plan.run_id == session.lease.run_id
            && plan.dataset_upload_id == upload.id
            && plan.storage_binding_hash == upload.binding_hash
            && dataset.storage_alias == upload.storage_alias,
        "run dataset or owner changed"
    );
    let bytes = session.read(dataset).await?;
    let manifest: DatasetManifest = decode(&dataset.verify(&bytes, session.limits)?)?;
    manifest.validate(session.limits)?;
    ensure!(
        manifest.dataset_id == upload.identity.dataset_id
            && manifest.dataset_version == upload.identity.dataset_version
            && manifest.dataset_version_id == upload.identity.dataset_version_id
            && manifest.dataset_hash == upload.identity.dataset_hash
            && manifest.case_count == upload.identity.case_count
            && manifest.batch_limits == session.limits.batch,
        "catalog and dataset manifest disagree"
    );
    let bytes = session.read(&manifest.source_fields).await?;
    let fields: DatasetSourceFields =
        decode(&manifest.source_fields.verify(&bytes, session.limits)?)?;
    fields.validate(session.limits)?;
    let mut identity = DatasetIdentityBuilder::new(&fields, session.limits)?;
    let mut cursor = DatasetCursor::new(&manifest);
    let mut accumulator = Accumulator::new(
        plan.run_id,
        manifest.dataset_version_id,
        profile,
        assignments,
    )?;
    let mut writer = Writer::new(session);
    let mut page = ChunkBuffer {
        chunks: Vec::new(),
        estimated_bytes: 256,
    };
    while let Some((entry, bytes)) = cursor.next(session).await? {
        let proof = entry.object.verify(&bytes, session.limits)?;
        let reader = decode_case_batch(&proof)?.validate(session.limits)?;
        ensure!(
            reader.header().dataset_id == manifest.dataset_id
                && reader.header().ordinal_start == entry.range_start
                && reader.header().ordinal_end == entry.range_end,
            "batch/index membership mismatch"
        );
        for record in reader {
            ensure!(!session.cancel.is_cancelled(), "run planning cancelled");
            let record = record?;
            identity.push(&record)?;
            if let Some(chunk) = accumulator.push(&record)? {
                push_chunk(&mut writer, &mut page, chunk).await?;
            }
        }
    }
    if let Some(chunk) = accumulator.finish_chunk()? {
        push_chunk(&mut writer, &mut page, chunk).await?;
    }
    if !page.chunks.is_empty() {
        write_chunks(&mut writer, std::mem::take(&mut page.chunks)).await?;
    }
    identity.verify(&manifest, session.limits)?;
    accumulator.validate_complete(manifest.case_count)?;
    let chunks = build_index(&mut writer).await?;
    let mut shards = Vec::new();
    let mut metadata = Vec::new();
    for (shard, state) in &accumulator.shards {
        let entries = state
            .gates
            .iter()
            .map(|(id, count)| GateCount {
                gate_id: id.clone(),
                planned_count: *count,
            })
            .collect::<Vec<_>>();
        let gate_counts = write_gates(&mut writer, *shard, entries).await?;
        shards.push(RunPlanShard {
            run_shard: *shard,
            case_count: state.plan.case_count,
            chunk_count: state.plan.chunk_count,
            planned_chunk_hash: state.plan.hash.clone(),
            gate_counts,
        });
        metadata.push(ledger::Shard {
            run_shard: *shard,
            database_alias: state.alias.clone(),
            case_count: state.plan.case_count,
            chunk_count: state.plan.chunk_count,
            planned_chunk_hash: state.plan.hash.clone(),
            case_projection_hash: state.projection.finalize().to_hex().to_string(),
        });
    }
    snapshot["profile"] = profile_payload.clone();
    snapshot["agent"] = serde_json::to_value(&profile.agent)?;
    snapshot["execution_plan"] = serde_json::to_value(&plan.execution_plan.0)?;
    snapshot["execution_plan_hash"] = json!(plan.execution_plan.0.hash()?);
    snapshot["executability"] = json!({"expected_execution_count":manifest.case_count,"expected_evaluator_execution_count":accumulator.evaluator_count,"unique_evaluator_ref_count":plan.execution_plan.evaluators.len(),"runnable_evaluator_ref_count":plan.execution_plan.evaluators.len()});
    // Location/binding belongs to the manifest and control ledger, not the
    // strict provider-neutral configuration document.
    let fields = snapshot
        .as_object_mut()
        .context("configuration must be an object")?;
    fields.remove("dataset_manifest");
    fields.remove("storage_binding_hash");
    let configuration = writer
        .document(
            "configuration",
            0,
            0,
            1,
            &RunConfiguration {
                schema: StorageFormat::RunConfiguration,
                snapshot,
            },
        )
        .await?;
    let root = RunPlanManifest {
        schema: StorageFormat::RunPlanManifest,
        run_id: plan.run_id,
        dataset: dataset.clone(),
        profile_hash: crate::contracts::case_storage::identity::hash_serialized_json(
            profile_payload,
        )?,
        aggregation_policy_hash: plan.execution_plan.aggregation_policy_hash.clone(),
        execution_plan_hash: plan.execution_plan.hash()?,
        configuration,
        chunk_count: accumulator.chunk_count,
        chunks,
        shards,
    };
    let reference = writer.document("manifest", 0, 0, 1, &root).await?;
    writer.flush().await?;
    ledger::publish(session.db, session.lease, &root, &reference, &metadata).await?;
    Ok(reference)
}
/// Accounts once per chunk, so page assembly stays linear in chunk count.
struct ChunkBuffer {
    chunks: Vec<RunChunk>,
    estimated_bytes: usize,
}
async fn push_chunk(
    writer: &mut Writer<'_, '_>,
    page: &mut ChunkBuffer,
    chunk: RunChunk,
) -> Result<()> {
    let bytes = serde_json::to_vec(&chunk)?.len() + 1;
    ensure!(
        256 + bytes <= writer.session.limits.page_bytes,
        "chunk cannot fit admitted page"
    );
    if !page.chunks.is_empty()
        && (page.chunks.len() == writer.session.limits.page_entries
            || page.estimated_bytes + bytes > writer.session.limits.page_bytes)
    {
        write_chunks(writer, std::mem::take(&mut page.chunks)).await?;
        page.estimated_bytes = 256;
    }
    writer.session.keep_alive().await?;
    page.estimated_bytes += bytes;
    page.chunks.push(chunk);
    Ok(())
}

async fn write_chunks(writer: &mut Writer<'_, '_>, chunks: Vec<RunChunk>) -> Result<()> {
    let start = chunks.first().context("empty chunk page")?.chunk_index;
    let end = chunks.last().context("empty chunk page")?.chunk_index + 1;
    writer
        .document(
            "chunks",
            0,
            start,
            end,
            &RunChunkPage {
                schema: StorageFormat::RunChunks,
                run_id: writer.session.lease.run_id,
                chunk_index_start: start,
                chunk_index_end: end,
                chunks,
            },
        )
        .await?;
    Ok(())
}
async fn write_gates(
    writer: &mut Writer<'_, '_>,
    shard: i16,
    entries: Vec<GateCount>,
) -> Result<GateCountDescriptor> {
    // Profiles admit at most 64 gates, so this collection is independently bounded.
    let total = i32::try_from(entries.len())?;
    let mut next = None;
    let mut pages = 0i32;
    let mut end = entries.len();
    while end > 0 {
        let mut start = end.saturating_sub(writer.session.limits.page_entries);
        let (doc, bytes) = loop {
            let doc = GateCountPage {
                schema: StorageFormat::GateCounts,
                run_shard: shard,
                entries: entries[start..end].to_vec(),
                next: next.clone(),
            };
            match encode(&doc, writer.session.limits) {
                Ok(bytes) => break (doc, bytes),
                Err(error) if start + 1 < end => {
                    start += 1;
                    let _ = error;
                }
                Err(error) => return Err(error),
            }
        };
        let _ = doc;
        next = Some(
            writer
                .put(
                    "gates",
                    0,
                    i32::from(shard) * 65 + i32::try_from(start)?,
                    i32::from(shard) * 65 + i32::try_from(end)?,
                    StorageFormat::GateCounts,
                    bytes,
                )
                .await?,
        );
        pages += 1;
        end = start;
    }
    Ok(GateCountDescriptor {
        first: next,
        page_count: pages,
        entry_count: total,
    })
}
async fn build_index(writer: &mut Writer<'_, '_>) -> Result<IndexDescriptor> {
    for level in 0..writer.session.limits.index_height {
        writer.flush().await?;
        let kind = if level == 0 { "chunks" } else { "index" };
        let child_level = if level == 0 { 0 } else { i16::from(level) - 1 };
        let mut after = -1;
        let mut entries = Vec::new();
        let mut size = 256;
        let mut count = 0;
        let mut last = None;
        loop {
            let rows = ledger::objects(
                writer.session.db,
                writer.session.lease.run_id,
                kind,
                child_level,
                after,
            )
            .await?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                let entry = IndexEntry {
                    range_start: row.range_start.context("missing child start")?,
                    range_end: row.range_end.context("missing child end")?,
                    object: row.reference.0,
                };
                after = entry.range_start;
                let bytes = serde_json::to_vec(&entry)?.len() + 1;
                if !entries.is_empty()
                    && (entries.len() == writer.session.limits.page_entries
                        || size + bytes > writer.session.limits.page_bytes)
                {
                    last = Some(write_index(writer, level, std::mem::take(&mut entries)).await?);
                    count += 1;
                    size = 256;
                }
                ensure!(
                    size + bytes <= writer.session.limits.page_bytes,
                    "index entry cannot fit admitted page"
                );
                size += bytes;
                entries.push(entry);
            }
        }
        if !entries.is_empty() {
            last = Some(write_index(writer, level, entries).await?);
            count += 1;
        }
        ensure!(count > 0, "empty run chunk index");
        if count == 1 {
            writer.flush().await?;
            return last.context("missing root");
        }
    }
    anyhow::bail!("run plan exceeds admitted index height")
}
async fn write_index(
    writer: &mut Writer<'_, '_>,
    level: u8,
    entries: Vec<IndexEntry>,
) -> Result<IndexDescriptor> {
    let start = entries.first().context("empty index")?.range_start;
    let end = entries.last().context("empty index")?.range_end;
    let doc = IndexPage {
        schema: StorageFormat::OrdinalIndex,
        kind: IndexKind::RunChunks,
        level,
        range_start: start,
        range_end: end,
        entries,
    };
    let object = writer.document("index", level, start, end, &doc).await?;
    Ok(IndexDescriptor {
        level,
        range_start: start,
        range_end: end,
        object,
    })
}
