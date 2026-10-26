//! Resumable dataset publication, independent of run creation and worker policy.
//!
//! Preparation validates bounded inputs and duplicate IDs before remote work.
//! A foreground caller claims one upload, records each exact object before PUT,
//! and acknowledges only completed verified bytes. Recovery rebuilds deterministic
//! batches from the same logical input, skips acknowledged writes, and re-verifies
//! all membership before generation-fenced catalog publication. Explicit CLI
//! resume/cleanup invocations own scheduling; no local file is delegated to a
//! background coordinator. Cleanup never deletes finalized or shared objects.
use anyhow::{
    Context,
    Result,
    ensure,
};
use sqlx::PgPool;
use tokio::io::{
    AsyncBufReadExt,
    AsyncReadExt,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    blob_storage::{
        BlobStorage,
        Budget,
        config::OwnerConfig,
    },
    contracts::case_storage::{
        identity::DatasetIdentityBuilder,
        *,
    },
    db::workflows::dataset_upload::{
        self as ledger,
        Catalog,
        Lease,
        Object,
        Upload,
    },
};

pub(crate) mod input;
use input::Prepared;

fn object(
    reference: ObjectRef,
    kind: &str,
    level: u8,
    start: Option<i32>,
    end: Option<i32>,
) -> Object {
    Object {
        object_key: reference.key.clone(),
        kind: kind.into(),
        level: i16::from(level),
        range_start: start,
        range_end: end,
        reference: sqlx::types::Json(reference),
        acknowledged: false,
    }
}
pub(crate) fn reference(
    upload: &Upload,
    kind: &str,
    level: u8,
    start: i32,
    end: i32,
    schema: StorageFormat,
    bytes: &[u8],
) -> Result<Object> {
    let digest = blake3::hash(bytes).to_hex().to_string();
    let key = format!(
        "uploads/{}/{kind}/{level}/{start}-{end}/{digest}",
        upload.id
    );
    Ok(object(
        ObjectRef::for_bytes(
            upload.storage_alias.clone(),
            key,
            schema,
            bytes,
            &upload.limits,
        )?,
        kind,
        level,
        Some(start),
        Some(end),
    ))
}

/// Persist ownership of connection probes before the adapter can issue a PUT.
/// Old probes are reconciled exactly after acquiring the new generation.
pub(crate) async fn connect(
    db: &PgPool,
    upload: &Upload,
    lease: Lease,
    owner: &OwnerConfig,
    cleanup: bool,
    cancel: &CancellationToken,
) -> Result<Box<dyn BlobStorage>> {
    ensure!(
        owner.alias() == upload.storage_alias && owner.binding_hash()? == upload.binding_hash,
        "storage configuration no longer resolves the recorded owner"
    );
    let budget = Budget::standard();
    let key = format!("capability-probes/{}/{}", upload.id, Uuid::now_v7());
    let probe = object(
        ObjectRef::for_bytes(
            upload.storage_alias.clone(),
            key.clone(),
            StorageFormat::DatasetManifest,
            b"probe-a",
            &upload.limits,
        )?,
        "probe",
        0,
        None,
        None,
    );
    ledger::plan(db, lease, &probe, cleanup).await?;
    let store = owner
        .open(key, upload.limits.0.clone(), budget, cancel)
        .await?;
    ledger::acknowledge(db, lease, &probe, cleanup).await?;
    // This page is bounded. More outstanding probes are handled on later resume
    // or cleanup invocations rather than performing unbounded startup work.
    for old in ledger::pending_cleanup(db, upload.id, true, 64).await? {
        let budget = Budget::standard();
        ledger::renew(db, lease, cleanup).await?;
        store.remove_owned(&old.reference, budget, cancel).await?;
        ledger::cleaned(db, lease, &old.object_key, cleanup).await?;
    }
    Ok(store)
}
pub(crate) async fn read_bytes(
    store: &dyn BlobStorage,
    reference: &ObjectRef,
    budget: Budget,
    cancel: &CancellationToken,
) -> Result<Vec<u8>> {
    let read = async {
        let file = store.read(reference, budget, cancel).await?;
        let mut bytes = Vec::with_capacity(usize::try_from(reference.byte_count)?);
        file.reader()
            .await?
            .take(reference.byte_count + 1)
            .read_to_end(&mut bytes)
            .await?;
        ensure!(
            bytes.len() as u64 == reference.byte_count
                && blake3::hash(&bytes).to_hex().as_str() == reference.digest,
            "verified spool changed before consumption"
        );
        Ok(bytes)
    };
    tokio::select! { biased;
        _=cancel.cancelled()=>anyhow::bail!("dataset object read cancelled"),
        _=tokio::time::sleep_until(budget.deadline)=>anyhow::bail!("dataset object read deadline exceeded"),
        result=read=>result,
    }
}
/// Register/replay one deterministic object. Acknowledged batch writes are skipped;
/// metadata is verified on replay and all batches are read in finalization.
async fn put(
    db: &PgPool,
    store: &dyn BlobStorage,
    lease: Lease,
    object: &Object,
    bytes: Vec<u8>,
    cancel: &CancellationToken,
) -> Result<()> {
    let budget = Budget::standard();
    let acknowledged = ledger::plan(db, lease, object, false).await?;
    if !acknowledged {
        store
            .create(
                &object.reference,
                Box::new(std::io::Cursor::new(bytes)),
                budget,
                cancel,
            )
            .await?;
        ledger::acknowledge(db, lease, object, false).await?;
    } else if object.kind != "batch" {
        read_bytes(store, &object.reference, budget, cancel).await?;
    }
    Ok(())
}

pub(crate) async fn ingest(
    db: &PgPool,
    store: &dyn BlobStorage,
    upload: &Upload,
    lease: Lease,
    prepared: &Prepared,
    cancel: &CancellationToken,
) -> Result<Catalog> {
    ensure!(
        prepared.identity == upload.identity.0 && prepared.limits == upload.limits.0,
        "resume source or batching changed"
    );
    let limits = &prepared.limits;
    let fields_bytes = encode(&prepared.fields, limits)?;
    let source = reference(
        upload,
        "source",
        0,
        0,
        1,
        StorageFormat::DatasetSourceFields,
        &fields_bytes,
    )?;
    put(db, store, lease, &source, fields_bytes, cancel).await?;
    let mut input =
        tokio::io::BufReader::new(tokio::fs::File::open(prepared.records.path()).await?);
    let mut start = 0;
    let mut end = 0;
    let mut builder = CaseBatchBuilder::new(prepared.identity.dataset_id, 0, limits)?;
    loop {
        ensure!(!cancel.is_cancelled(), "dataset upload cancelled");
        let mut line = Vec::new();
        (&mut input)
            .take(limits.batch.max_bytes as u64 + 1)
            .read_until(b'\n', &mut line)
            .await?;
        ensure!(
            line.len() <= limits.batch.max_bytes,
            "prepared record exceeds byte limit"
        );
        if line.is_empty() {
            break;
        }
        let record: CaseRecord = serde_json::from_slice(&line)?;
        if !builder.try_push(&record)? {
            let bytes = builder.finish()?;
            let batch = reference(
                upload,
                "batch",
                0,
                start,
                end,
                StorageFormat::CaseBatch,
                &bytes,
            )?;
            put(db, store, lease, &batch, bytes, cancel).await?;
            start = end;
            builder = CaseBatchBuilder::new(prepared.identity.dataset_id, start, limits)?;
            ensure!(
                builder.try_push(&record)?,
                "single case cannot fit in a batch"
            );
        }
        end = record
            .case_ordinal
            .checked_add(1)
            .context("case count overflow")?;
    }
    let bytes = builder.finish()?;
    let batch = reference(
        upload,
        "batch",
        0,
        start,
        end,
        StorageFormat::CaseBatch,
        &bytes,
    )?;
    put(db, store, lease, &batch, bytes, cancel).await?;
    ledger::finalizing(db, lease).await?;
    // Re-read actual remote membership, not just ledger counters or the local
    // source. Acknowledged corruption/missing objects cannot activate a dataset.
    let mut identity = DatasetIdentityBuilder::new(&prepared.fields, limits)?;
    let mut after = -1;
    let mut next = 0;
    loop {
        let page = ledger::objects(db, upload.id, "batch", 0, after).await?;
        if page.is_empty() {
            break;
        }
        for batch in page {
            ensure!(
                batch.range_start == Some(next),
                "stored batch membership gap or overlap"
            );
            let budget = Budget::standard();
            ledger::renew(db, lease, false).await?;
            let bytes = read_bytes(store, &batch.reference, budget, cancel).await?;
            let proof = batch.reference.verify(&bytes, limits)?;
            let decoded = decode_case_batch(&proof)?;
            let reader = decoded.validate(limits)?;
            ensure!(
                reader.header().dataset_id == prepared.identity.dataset_id
                    && reader.header().ordinal_start == next
                    && Some(reader.header().ordinal_end) == batch.range_end,
                "batch header disagrees with acknowledged membership"
            );
            for record in reader {
                identity.push(&record?)?;
            }
            after = batch.range_start.context("missing batch range")?;
            next = batch.range_end.context("missing batch range")?;
        }
    }
    ensure!(
        next == prepared.identity.case_count,
        "incomplete dataset membership"
    );
    let batches = build_index(db, store, upload, lease, cancel).await?;
    let manifest = DatasetManifest {
        schema: StorageFormat::DatasetManifest,
        dataset_id: prepared.identity.dataset_id,
        dataset_version: prepared.identity.dataset_version.clone(),
        dataset_version_id: prepared.identity.dataset_version_id,
        dataset_hash: prepared.identity.dataset_hash.clone(),
        identity_scheme: IdentityScheme::DatasetJson,
        case_count: next,
        batch_limits: limits.batch,
        source_fields: source.reference.0,
        batches,
    };
    identity.verify(&manifest, limits)?;
    let bytes = encode(&manifest, limits)?;
    let root = reference(
        upload,
        "manifest",
        0,
        0,
        next,
        StorageFormat::DatasetManifest,
        &bytes,
    )?;
    put(db, store, lease, &root, bytes, cancel).await?;
    ledger::publish(db, lease, &manifest, &root.reference).await
}

/// Build one level at a time from bounded SQL pages. Only one encoded page and
/// its child references are resident; tree height and fanout remain admitted limits.
async fn build_index(
    db: &PgPool,
    store: &dyn BlobStorage,
    upload: &Upload,
    lease: Lease,
    cancel: &CancellationToken,
) -> Result<IndexDescriptor> {
    let limits = &upload.limits;
    for level in 0..limits.index_height {
        let kind = if level == 0 { "batch" } else { "index" };
        let child_level = if level == 0 { 0 } else { i16::from(level) - 1 };
        let mut after = -1;
        let mut entries = Vec::new();
        let mut encoded_size = 256_usize;
        let mut roots = 0;
        let mut last = None;
        loop {
            let rows = ledger::objects(db, upload.id, kind, child_level, after).await?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                let entry = IndexEntry {
                    range_start: row.range_start.context("index child range missing")?,
                    range_end: row.range_end.context("index child range missing")?,
                    object: row.reference.0,
                };
                after = entry.range_start;
                let bytes = serde_json::to_vec(&entry)?.len() + 1;
                if !entries.is_empty()
                    && (entries.len() == limits.page_entries
                        || encoded_size + bytes > limits.page_bytes)
                {
                    last = Some(
                        index_page(
                            db,
                            store,
                            upload,
                            lease,
                            level,
                            std::mem::take(&mut entries),
                            cancel,
                        )
                        .await?,
                    );
                    roots += 1;
                    encoded_size = 256;
                }
                ensure!(
                    encoded_size + bytes <= limits.page_bytes,
                    "index entry cannot fit in admitted page"
                );
                encoded_size += bytes;
                entries.push(entry);
            }
        }
        if !entries.is_empty() {
            last = Some(index_page(db, store, upload, lease, level, entries, cancel).await?);
            roots += 1;
        }
        ensure!(roots > 0, "empty dataset index");
        if roots == 1 {
            return last.context("missing index root");
        }
    }
    anyhow::bail!("dataset exceeds admitted index height")
}
async fn index_page(
    db: &PgPool,
    store: &dyn BlobStorage,
    upload: &Upload,
    lease: Lease,
    level: u8,
    entries: Vec<IndexEntry>,
    cancel: &CancellationToken,
) -> Result<IndexDescriptor> {
    let start = entries.first().context("empty index")?.range_start;
    let end = entries.last().context("empty index")?.range_end;
    let page = IndexPage {
        schema: StorageFormat::OrdinalIndex,
        kind: IndexKind::DatasetBatches,
        level,
        range_start: start,
        range_end: end,
        entries,
    };
    let bytes = encode(&page, &upload.limits)?;
    let object = reference(
        upload,
        "index",
        level,
        start,
        end,
        StorageFormat::OrdinalIndex,
        &bytes,
    )?;
    put(db, store, lease, &object, bytes, cancel).await?;
    Ok(IndexDescriptor {
        level,
        range_start: start,
        range_end: end,
        object: object.reference.0,
    })
}

/// One exact-key cleanup page per invocation. The authority check follows creation
/// of the monotonic storage budget, so stale paused cleanup cannot race publication.
pub(crate) async fn cleanup(
    db: &PgPool,
    store: &dyn BlobStorage,
    lease: Lease,
    limit: i64,
    cancel: &CancellationToken,
) -> Result<bool> {
    for object in ledger::pending_cleanup(db, lease.id, false, limit).await? {
        let budget = Budget::standard();
        ledger::renew(db, lease, true).await?;
        store
            .remove_owned(&object.reference, budget, cancel)
            .await?;
        ledger::cleaned(db, lease, &object.object_key, true).await?;
    }
    ledger::finish_cleanup(db, lease).await
}
