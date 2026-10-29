//! PostgreSQL authority, progress, replay and publication regression scenarios.
//! Runtime recovery uses a deterministic in-memory transport here; signed storage
//! and process interruption are verified separately by the named live CLI tier.
use std::{
    collections::HashMap,
    sync::{
        Arc,
        Mutex,
    },
};

use tokio_util::sync::CancellationToken;

use super::*;
#[path = "postgres_tests/batches.rs"]
mod batch_tests;
use crate::{
    blob_storage::{
        Budget,
        Download,
        ErrorKind,
        Resources,
        StorageError,
        Store,
        Transport,
    },
    contracts::case_storage::StorageFormat,
    dataset_ingestion::{
        self as ingestion,
        input::{
            InputFormat,
            prepare,
        },
    },
};

async fn fixture(db: &PgPool) -> (Upload, Lease) {
    let identity = DatasetIdentity {
        dataset_id: Uuid::now_v7(),
        dataset_version: None,
        dataset_version_id: Uuid::now_v7(),
        dataset_hash: "1".repeat(64),
        case_count: 3,
    };
    let row = begin(
        db,
        Uuid::now_v7(),
        "cases",
        &"2".repeat(64),
        &identity,
        &FormatLimits::default(),
    )
    .await
    .unwrap();
    let lease = claim(db, row.id, false).await.unwrap();
    (row, lease)
}
fn batch(row: &Upload, start: i32, end: i32) -> Object {
    let reference = ObjectRef::for_bytes(
        "cases".into(),
        format!("uploads/{}/batch/{start}", row.id),
        StorageFormat::CaseBatch,
        b"batch",
        &row.limits,
    )
    .unwrap();
    Object {
        object_key: reference.key.clone(),
        kind: "batch".into(),
        level: 0,
        range_start: Some(start),
        range_end: Some(end),
        reference: Json(reference),
        acknowledged: false,
    }
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn leases_fence_stale_owners_and_acknowledgements_are_contiguous(db: PgPool) {
    let (row, lease) = fixture(&db).await;
    assert!(claim(&db, row.id, false).await.is_err());
    let status = load(&db, row.id).await.unwrap();
    assert_eq!(status.lease_owner, Some(lease.owner));
    assert!(
        status
            .leased_until
            .is_some_and(|until| until > chrono::Utc::now())
    );
    assert!(status.expires_at > chrono::Utc::now());
    let first = batch(&row, 0, 1);
    let gap = batch(&row, 2, 3);
    assert!(!plan(&db, lease, &first, false).await.unwrap());
    plan(&db, lease, &gap, false).await.unwrap();
    assert!(acknowledge(&db, lease, &gap, false).await.is_err());
    acknowledge(&db, lease, &first, false).await.unwrap();
    acknowledge(&db, lease, &first, false).await.unwrap();
    let progress = load(&db, row.id).await.unwrap();
    assert_eq!((progress.case_cursor, progress.batch_count), (1, 1));
    let mut changed = first.clone();
    changed.reference.digest = "3".repeat(64);
    assert!(plan(&db, lease, &changed, false).await.is_err());
    sqlx::query("UPDATE dataset_uploads SET leased_until=now()-interval '1 second' WHERE id=$1")
        .bind(row.id)
        .execute(&db)
        .await
        .unwrap();
    let replacement = claim(&db, row.id, false).await.unwrap();
    assert!(replacement.generation > lease.generation);
    assert!(acknowledge(&db, lease, &gap, false).await.is_err());
    assert!(renew(&db, lease, false).await.is_err());
    assert!(finalizing(&db, replacement).await.is_err());
    assert!(catalog(&db, row.id).await.unwrap().is_none());
}
#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn changed_input_owner_limits_and_unowned_keys_cannot_replay(db: PgPool) {
    let (row, lease) = fixture(&db).await;
    assert!(
        begin(
            &db,
            row.id,
            "other",
            &row.binding_hash,
            &row.identity,
            &row.limits
        )
        .await
        .is_err()
    );
    let mut changed = row.identity.0.clone();
    changed.dataset_hash = "4".repeat(64);
    assert!(
        begin(
            &db,
            row.id,
            &row.storage_alias,
            &row.binding_hash,
            &changed,
            &row.limits
        )
        .await
        .is_err()
    );
    let mut limits = row.limits.0.clone();
    limits.batch.max_cases = 1;
    assert!(
        begin(
            &db,
            row.id,
            &row.storage_alias,
            &row.binding_hash,
            &row.identity,
            &limits
        )
        .await
        .is_err()
    );
    let mut unowned = batch(&row, 0, 1);
    unowned.reference.key = "uploads/someone-else/file".into();
    unowned.object_key = unowned.reference.key.clone();
    assert!(plan(&db, lease, &unowned, false).await.is_err());
    abort(&db, row.id).await.unwrap();
    abort(&db, row.id).await.unwrap();
    assert!(plan(&db, lease, &batch(&row, 0, 1), false).await.is_err());
    assert!(claim(&db, row.id, true).await.is_err());
    sqlx::query("UPDATE dataset_uploads SET cleanup_after=now()-interval '1 second' WHERE id=$1")
        .bind(row.id)
        .execute(&db)
        .await
        .unwrap();
    let cleanup = claim(&db, row.id, true).await.unwrap();
    assert!(finish_cleanup(&db, cleanup).await.unwrap());
    assert!(renew(&db, cleanup, true).await.is_err());
}
#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn expiry_is_bounded_and_revokes_authority(db: PgPool) {
    let (row, lease) = fixture(&db).await;
    sqlx::query("UPDATE dataset_uploads SET expires_at=now()-interval '1 second' WHERE id=$1")
        .bind(row.id)
        .execute(&db)
        .await
        .unwrap();
    assert!(renew(&db, lease, false).await.is_err());
    assert_eq!(expire(&db).await.unwrap(), vec![row.id]);
    assert_eq!(load(&db, row.id).await.unwrap().status, "expired");
    assert!(claim(&db, row.id, false).await.is_err());
    assert!(claim(&db, row.id, true).await.is_err());
}

#[derive(Default)]
struct State {
    objects: HashMap<String, Vec<u8>>,
    creates: HashMap<String, u32>,
    stop_after_batch: bool,
    stop_after_index: bool,
    fail_delete: bool,
    observe_upload: Option<(PgPool, Uuid)>,
    registrations_at_batch_put: Vec<i64>,
    stop_at_batch: Option<usize>,
    delay_batch_once: Option<std::time::Duration>,
    abort_after_batch: bool,
}
#[derive(Clone, Default)]
struct Memory(Arc<Mutex<State>>);
fn error(kind: ErrorKind) -> StorageError {
    StorageError {
        kind,
        operation: "fixture",
        alias: String::new(),
        key: String::new(),
        status: None,
        uncertain: false,
        source: None,
    }
}
#[async_trait::async_trait]
impl Transport for Memory {
    async fn put(
        &self,
        r: &ObjectRef,
        path: &std::path::Path,
        _: usize,
        _: std::time::Duration,
    ) -> std::result::Result<(), StorageError> {
        if r.key.contains("/batch/") {
            let delay = self.0.lock().unwrap().delay_batch_once.take();
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            let observe = self.0.lock().unwrap().observe_upload.clone();
            if let Some((db, upload)) = observe {
                let count: i64 = sqlx::query_scalar("SELECT count(*) FROM dataset_upload_objects WHERE upload_id=$1 AND kind='batch'")
                    .bind(upload).fetch_one(&db).await.unwrap();
                let abort_now = self.0.lock().unwrap().abort_after_batch;
                if abort_now {
                    abort(&db, upload).await.unwrap();
                }
                self.0
                    .lock()
                    .unwrap()
                    .registrations_at_batch_put
                    .push(count);
            }
        }
        let bytes = tokio::fs::read(path).await.unwrap();
        let mut s = self.0.lock().unwrap();
        if s.objects.contains_key(&r.key) {
            return Err(error(ErrorKind::Conflict));
        }
        s.objects.insert(r.key.clone(), bytes);
        *s.creates.entry(r.key.clone()).or_default() += 1;
        if r.key.contains("/batch/") && s.stop_at_batch == Some(s.registrations_at_batch_put.len())
        {
            s.stop_at_batch = None;
            return Err(error(ErrorKind::Cancelled));
        }
        if r.key.contains("/batch/") && s.stop_after_batch {
            s.stop_after_batch = false;
            return Err(error(ErrorKind::Cancelled));
        }
        if r.key.contains("/index/") && s.stop_after_index {
            s.stop_after_index = false;
            return Err(error(ErrorKind::Cancelled));
        }
        Ok(())
    }
    async fn get(
        &self,
        r: &ObjectRef,
        _: std::time::Duration,
    ) -> std::result::Result<Download, StorageError> {
        let s = self.0.lock().unwrap();
        let bytes = s
            .objects
            .get(&r.key)
            .ok_or_else(|| error(ErrorKind::Missing))?
            .clone();
        Ok(Download {
            length: bytes.len() as u64,
            revision: None,
            reader: Box::new(std::io::Cursor::new(bytes)),
        })
    }
    async fn delete_object(&self, key: &str) -> std::result::Result<(), StorageError> {
        let mut state = self.0.lock().unwrap();
        if state.fail_delete {
            state.fail_delete = false;
            return Err(error(ErrorKind::Unauthorized));
        }
        state.objects.remove(key);
        Ok(())
    }
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn batch_group_registers_before_io_and_partial_upload_keeps_atomic_progress(db: PgPool) {
    let input = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        input.path(),
        include_bytes!("../../../cli/commands/run/fixtures/dataset-identity.json"),
    )
    .unwrap();
    let mut limits = FormatLimits::default();
    limits.batch.max_cases = 1;
    let cancel = CancellationToken::new();
    let prepared = prepare(
        input.path().to_owned(),
        InputFormat::Json,
        limits.clone(),
        &cancel,
    )
    .await
    .unwrap();
    let row = begin(
        &db,
        Uuid::now_v7(),
        "cases",
        &"2".repeat(64),
        &prepared.identity,
        &limits,
    )
    .await
    .unwrap();
    let lease = claim(&db, row.id, false).await.unwrap();
    let memory = Memory::default();
    let store = Store::connect(
        memory.clone(),
        "cases".into(),
        limits,
        Resources::standard(),
        Budget::standard(),
        &cancel,
        "capability-probes/fixture".into(),
    )
    .await
    .unwrap();
    {
        let mut state = memory.0.lock().unwrap();
        state.observe_upload = Some((db.clone(), row.id));
        state.stop_at_batch = Some(2);
    }
    assert!(
        ingestion::ingest(&db, &store, &row, lease, &prepared, &cancel)
            .await
            .is_err()
    );
    assert_eq!(
        memory.0.lock().unwrap().registrations_at_batch_put,
        vec![3, 3],
        "all exact group keys must be durable before any group PUT"
    );
    let progress = load(&db, row.id).await.unwrap();
    assert_eq!(
        (progress.case_cursor, progress.batch_count),
        (0, 0),
        "partial group must not advance progress"
    );
    assert!(catalog(&db, row.id).await.unwrap().is_none());
    release(&db, lease).await.unwrap();
    let resumed = claim(&db, row.id, false).await.unwrap();
    ingestion::ingest(&db, &store, &row, resumed, &prepared, &cancel)
        .await
        .unwrap();
    let progress = load(&db, row.id).await.unwrap();
    assert_eq!((progress.case_cursor, progress.batch_count), (3, 3));
    assert!(
        memory
            .0
            .lock()
            .unwrap()
            .creates
            .values()
            .all(|&count| count == 1),
        "uncertain writes must reconcile, not overwrite"
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn object_commit_before_ack_recovers_exactly_and_finalizes_multilevel_index(db: PgPool) {
    let input = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        input.path(),
        include_bytes!("../../../cli/commands/run/fixtures/dataset-identity.json"),
    )
    .unwrap();
    let mut limits = FormatLimits::default();
    limits.batch.max_cases = 1;
    limits.page_entries = 2;
    let cancel = CancellationToken::new();
    let prepared = prepare(
        input.path().to_owned(),
        InputFormat::Json,
        limits.clone(),
        &cancel,
    )
    .await
    .unwrap();
    let row = begin(
        &db,
        Uuid::now_v7(),
        "cases",
        &"2".repeat(64),
        &prepared.identity,
        &limits,
    )
    .await
    .unwrap();
    let lease = claim(&db, row.id, false).await.unwrap();
    let memory = Memory::default();
    let store = Store::connect(
        memory.clone(),
        "cases".into(),
        limits,
        Resources::standard(),
        Budget::standard(),
        &cancel,
        "capability-probes/fixture".into(),
    )
    .await
    .unwrap();
    memory.0.lock().unwrap().stop_after_batch = true;
    assert!(
        ingestion::ingest(&db, &store, &row, lease, &prepared, &cancel)
            .await
            .is_err()
    );
    assert_eq!(load(&db, row.id).await.unwrap().case_cursor, 0);
    assert!(catalog(&db, row.id).await.unwrap().is_none());
    release(&db, lease).await.unwrap();
    let interrupted = claim(&db, row.id, false).await.unwrap();
    memory.0.lock().unwrap().stop_after_index = true;
    assert!(
        ingestion::ingest(&db, &store, &row, interrupted, &prepared, &cancel)
            .await
            .is_err()
    );
    let progress = load(&db, row.id).await.unwrap();
    assert_eq!(progress.status, "finalizing");
    assert_eq!(progress.case_cursor, 3);
    assert!(catalog(&db, row.id).await.unwrap().is_none());
    release(&db, interrupted).await.unwrap();
    let resumed = claim(&db, row.id, false).await.unwrap();
    let published = ingestion::ingest(&db, &store, &row, resumed, &prepared, &cancel)
        .await
        .unwrap();
    assert_eq!(load(&db, row.id).await.unwrap().status, "finalized");
    assert!(published.manifest.key.contains("/manifest/"));
    assert!(abort(&db, row.id).await.is_err());
    assert!(claim(&db, row.id, true).await.is_err());
    assert!(memory.0.lock().unwrap().creates.values().all(|&v| v == 1));
    assert!(
        reusable(&db, "cases", &row.binding_hash, &row.identity, &row.limits)
            .await
            .unwrap()
            .is_some()
    );
    // A sealed catalog is immutable; even the original lease cannot republish.
    assert!(renew(&db, resumed, false).await.is_err());
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn corrupt_acknowledged_batch_never_publishes_and_abort_cleanup_is_exact(db: PgPool) {
    let input = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        input.path(),
        include_bytes!("../../../cli/commands/run/fixtures/dataset-identity.json"),
    )
    .unwrap();
    let cancel = CancellationToken::new();
    let prepared = prepare(
        input.path().to_owned(),
        InputFormat::Json,
        FormatLimits::default(),
        &cancel,
    )
    .await
    .unwrap();
    let row = begin(
        &db,
        Uuid::now_v7(),
        "cases",
        &"2".repeat(64),
        &prepared.identity,
        &prepared.limits,
    )
    .await
    .unwrap();
    let lease = claim(&db, row.id, false).await.unwrap();
    let memory = Memory::default();
    let store = Store::connect(
        memory.clone(),
        "cases".into(),
        prepared.limits.clone(),
        Resources::standard(),
        Budget::standard(),
        &cancel,
        "capability-probes/fixture".into(),
    )
    .await
    .unwrap();
    // Seed an acknowledged batch whose physical content is then altered.
    let mut builder = crate::contracts::case_storage::CaseBatchBuilder::new(
        prepared.identity.dataset_id,
        0,
        &prepared.limits,
    )
    .unwrap();
    for line in std::fs::read_to_string(prepared.records.path())
        .unwrap()
        .lines()
    {
        builder
            .try_push(&serde_json::from_str(line).unwrap())
            .unwrap();
    }
    let bytes = builder.finish().unwrap();
    let b = ingestion::reference(&row, "batch", 0, 0, 3, StorageFormat::CaseBatch, &bytes).unwrap();
    plan(&db, lease, &b, false).await.unwrap();
    memory
        .0
        .lock()
        .unwrap()
        .objects
        .insert(b.object_key.clone(), b"changed".to_vec());
    acknowledge(&db, lease, &b, false).await.unwrap();
    assert!(
        ingestion::ingest(&db, &store, &row, lease, &prepared, &cancel)
            .await
            .is_err()
    );
    assert!(catalog(&db, row.id).await.unwrap().is_none());
    memory
        .0
        .lock()
        .unwrap()
        .objects
        .insert("unrelated/published".into(), b"keep".to_vec());
    abort(&db, row.id).await.unwrap();
    sqlx::query("UPDATE dataset_uploads SET cleanup_after=now()-interval '1 second' WHERE id=$1")
        .bind(row.id)
        .execute(&db)
        .await
        .unwrap();
    let cleaner = claim(&db, row.id, true).await.unwrap();
    memory.0.lock().unwrap().fail_delete = true;
    assert!(
        ingestion::cleanup(&db, &store, cleaner, 1, &cancel)
            .await
            .is_err()
    );
    assert!(
        !pending_cleanup(&db, row.id, false, 1)
            .await
            .unwrap()
            .is_empty()
    );
    let mut complete = false;
    for _ in 0..10 {
        complete = ingestion::cleanup(&db, &store, cleaner, 1, &cancel)
            .await
            .unwrap();
        if complete {
            break;
        }
    }
    assert!(complete);
    assert_eq!(memory.0.lock().unwrap().objects.len(), 1);
    assert!(
        memory
            .0
            .lock()
            .unwrap()
            .objects
            .contains_key("unrelated/published")
    );
    assert!(claim(&db, row.id, true).await.is_err());
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn lease_expiring_while_waiting_for_row_lock_cannot_be_resurrected(db: PgPool) {
    let (row, lease) = fixture(&db).await;
    sqlx::query("UPDATE dataset_uploads SET leased_until=clock_timestamp()+interval '2 seconds' WHERE id=$1").bind(row.id).execute(&db).await.unwrap();
    let mut locked = db.begin().await.unwrap();
    sqlx::query("SELECT id FROM dataset_uploads WHERE id=$1 FOR UPDATE")
        .bind(row.id)
        .fetch_one(&mut *locked)
        .await
        .unwrap();
    let pool = db.clone();
    let b = batch(&row, 0, 1);
    let waiter = tokio::spawn(async move { plan(&pool, lease, &b, false).await });
    tokio::time::timeout(std::time::Duration::from_secs(1),async{
        loop{let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%dataset_uploads%' AND query LIKE '%FOR UPDATE%')").fetch_one(&db).await.unwrap();if waiting{break;}tokio::task::yield_now().await;}
    }).await.unwrap();
    sqlx::query("SELECT pg_sleep(2.1)")
        .execute(&mut *locked)
        .await
        .unwrap();
    locked.commit().await.unwrap();
    assert!(
        waiter.await.unwrap().is_err(),
        "expired lease was renewed after waiting for a row lock"
    );
}
