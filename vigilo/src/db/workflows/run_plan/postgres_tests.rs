//! Database-backed preparation recovery, authority and fail-closed publication.
//! Memory transport isolates exact crash windows; the CLI service tier verifies
//! actual signing and process restart against Ceph.
use std::{
    collections::HashMap,
    sync::{
        Arc,
        Mutex,
    },
};

use serde_json::json;
use tokio_util::sync::CancellationToken;

use super::*;
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
    contracts::{
        case_storage::identity::{
            canonical_json,
            hash_serialized_json,
        },
        run::RunProfile,
    },
    dataset_ingestion::{
        self,
        input::{
            InputFormat,
            prepare as prepare_input,
        },
    },
    db::workflows::{
        dataset_upload,
        run_create::RunShardPlacementAssignment,
    },
    run_planning::{
        self,
        Request,
        Session,
    },
};
#[derive(Default)]
struct State {
    objects: HashMap<String, Vec<u8>>,
    creates: HashMap<String, u32>,
    reads: usize,
    blocked_read: Option<(String, Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
    delayed_put: Option<(String, std::time::Duration)>,
    stop: Option<String>,
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
        let delay = {
            let mut state = self.0.lock().unwrap();
            if state
                .delayed_put
                .as_ref()
                .is_some_and(|(key, _)| r.key.contains(key))
            {
                state.delayed_put.take().map(|(_, delay)| delay)
            } else {
                None
            }
        };
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        let bytes = tokio::fs::read(path).await.unwrap();
        let mut s = self.0.lock().unwrap();
        if s.objects.contains_key(&r.key) {
            return Err(error(ErrorKind::Conflict));
        }
        s.objects.insert(r.key.clone(), bytes);
        *s.creates.entry(r.key.clone()).or_default() += 1;
        if s.stop.as_ref().is_some_and(|stop| r.key.contains(stop)) {
            s.stop = None;
            return Err(error(ErrorKind::Cancelled));
        }
        Ok(())
    }
    async fn get(
        &self,
        r: &ObjectRef,
        _: std::time::Duration,
    ) -> std::result::Result<Download, StorageError> {
        let block = self.0.lock().unwrap().blocked_read.clone();
        if let Some((key, entered, resume)) = block
            && r.key == key
        {
            entered.notify_one();
            resume.notified().await;
        }
        let mut state = self.0.lock().unwrap();
        state.reads += 1;
        let bytes = state
            .objects
            .get(&r.key)
            .cloned()
            .ok_or_else(|| error(ErrorKind::Missing))?;
        Ok(Download {
            length: bytes.len() as u64,
            revision: None,
            reader: Box::new(std::io::Cursor::new(bytes)),
        })
    }
    async fn delete_object(&self, key: &str) -> std::result::Result<(), StorageError> {
        self.0.lock().unwrap().objects.remove(key);
        Ok(())
    }
}
struct Fixture {
    upload: dataset_upload::Upload,
    dataset: ObjectRef,
    plan: Plan,
    draft: RunDraft,
    profile: RunProfile,
    payload: serde_json::Value,
    memory: Memory,
    store: Store<Memory>,
    cancel: CancellationToken,
}
impl Fixture {
    async fn new(db: &PgPool) -> Self {
        Self::with_size(db, 205, 7, 2).await
    }
    async fn with_size(
        db: &PgPool,
        case_count: usize,
        batch_cases: usize,
        page_entries: usize,
    ) -> Self {
        let file = tempfile::NamedTempFile::new().unwrap();
        let dataset_id = Uuid::now_v7();
        let mut source = format!(
            "{}\n",
            json!({"schema":"vigilo/dataset-upload/v1","fields":{"dataset_id":dataset_id,"dataset_version":"fixture"}})
        );
        for ordinal in 0..case_count {
            source.push_str(&format!("{}\n",json!({"id":Uuid::from_u128((ordinal+1) as u128),"task_type":"classification","input":{"text":"hello"},"tags":if ordinal%2==0{vec!["selected"]}else{vec![]}})));
        }
        std::fs::write(file.path(), source).unwrap();
        let mut limits = FormatLimits::default();
        limits.batch.max_cases = batch_cases;
        limits.page_entries = page_entries;
        let cancel = CancellationToken::new();
        let prepared = prepare_input(
            file.path().into(),
            InputFormat::Jsonl,
            limits.clone(),
            &cancel,
        )
        .await
        .unwrap();
        let upload = dataset_upload::begin(
            db,
            Uuid::now_v7(),
            "cases",
            &"2".repeat(64),
            &prepared.identity,
            &limits,
        )
        .await
        .unwrap();
        let lease = dataset_upload::claim(db, upload.id, false).await.unwrap();
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
        let catalog = dataset_ingestion::ingest(db, &store, &upload, lease, &prepared, &cancel)
            .await
            .unwrap();
        drop(prepared);
        let mut payload = canonical_json(
            &serde_yaml::from_str::<serde_json::Value>(include_str!(
                "../../../../../example/profile.yaml"
            ))
            .unwrap(),
        );
        let gate = payload["scorecard"]["gates"][0].clone();
        payload["scorecard"]["gates"] = serde_json::Value::Array(
            (0..5)
                .map(|index| {
                    let mut gate = gate.clone();
                    gate["id"] = json!(format!("gate_{index}"));
                    gate
                })
                .collect(),
        );
        let profile: RunProfile = serde_json::from_value(payload.clone()).unwrap();
        // This fixture tests persistence/planning, not evaluator resolution. The
        // CLI tier resolves registry metadata; the distributed runtime tier
        // separately executes real Wasm fixtures.
        let execution = EvaluatorExecutionPlan::new("3".repeat(64), Vec::new());
        let id = Uuid::now_v7();
        let identity = &upload.identity;
        let snapshot = json!({"dataset_ref":identity.0,"dataset_manifest":catalog.manifest.0,"dataset_version_id":identity.dataset_version_id,"dataset_hash":identity.dataset_hash,"profile_version_id":"fixture/1","profile_hash":hash_serialized_json(&payload).unwrap(),"aggregation_policy_hash":execution.aggregation_policy_hash,"execution_plan_hash":execution.hash().unwrap(),"storage_binding_hash":upload.binding_hash,"chunk_size":100});
        let draft = RunDraft {
            run_key: id.to_string(),
            name: None,
            description: None,
            dataset_id,
            dataset_version: "fixture".into(),
            dataset_version_id: identity.dataset_version_id,
            evaluation_profile_id: profile.profile_id.clone(),
            evaluation_profile_version: profile.profile_version.clone(),
            profile_version_id: "fixture/1".into(),
            profile_hash: hash_serialized_json(&payload).unwrap(),
            aggregation_policy_id: "policy".into(),
            aggregation_policy_version: "v5".into(),
            aggregation_policy_hash: execution.aggregation_policy_hash.clone(),
            agent_provider: profile.agent.provider.clone(),
            agent_name: profile.agent.name.clone(),
            agent_version: None,
            prompt_config_id: "default".into(),
            prompt_config_version: "v1".into(),
            config_snapshot: snapshot,
            expected_execution_count: case_count as i32,
        };
        let assignments = (0..case_count.div_ceil(100).min(128) as i16)
            .map(|run_shard| RunShardPlacementAssignment {
                run_shard,
                database_alias: "primary".into(),
            })
            .collect::<Vec<_>>();
        let plan = begin(
            db,
            id,
            upload.id,
            &upload.binding_hash,
            &upload.limits,
            &draft,
            &execution,
            &assignments,
        )
        .await
        .unwrap();
        Self {
            upload,
            dataset: catalog.manifest.0,
            plan,
            draft,
            profile,
            payload,
            memory,
            store,
            cancel,
        }
    }
    async fn prepare(&self, db: &PgPool, lease: Lease) -> Result<ObjectRef> {
        run_planning::prepare(
            &Session {
                db,
                store: &self.store,
                lease,
                limits: &self.plan.limits,
                cancel: &self.cancel,
                alias: "cases",
                last_renewed: tokio::sync::Mutex::new(tokio::time::Instant::now()),
            },
            Request {
                plan: &self.plan,
                upload: &self.upload,
                dataset: &self.dataset,
                profile: &self.profile,
                profile_payload: &self.payload,
                assignments: &self.plan.assignments,
                snapshot: self.draft.config_snapshot.clone(),
            },
        )
        .await
    }
}
#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn interrupted_chunk_and_manifest_writes_resume_exactly_without_control_case_rows(
    db: PgPool,
) {
    let f = Fixture::new(&db).await;
    let first = claim(&db, f.plan.run_id).await.unwrap();
    f.memory.0.lock().unwrap().stop = Some("/chunks/".into());
    assert!(f.prepare(&db, first).await.is_err());
    assert_eq!(
        load(&db, f.plan.run_id)
            .await
            .unwrap()
            .unwrap()
            .chunk_cursor,
        0
    );
    release(&db, first, Some("interrupted chunks"))
        .await
        .unwrap();
    let second = claim(&db, f.plan.run_id).await.unwrap();
    assert!(second.generation > first.generation);
    assert!(renew(&db, first).await.is_err());
    f.memory.0.lock().unwrap().stop = Some("/manifest/".into());
    assert!(f.prepare(&db, second).await.is_err());
    let progress = load(&db, f.plan.run_id).await.unwrap().unwrap();
    assert_eq!(progress.chunk_cursor, 3);
    assert!(progress.manifest.is_none());
    release(&db, second, None).await.unwrap();
    let last = claim(&db, f.plan.run_id).await.unwrap();
    sqlx::query("CREATE FUNCTION reject_ready_plan() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.status='ready' THEN RAISE EXCEPTION 'injected plan publication failure'; END IF; RETURN NEW; END $$").execute(&db).await.unwrap();
    sqlx::query("CREATE TRIGGER reject_ready_plan BEFORE UPDATE ON run_plans FOR EACH ROW EXECUTE FUNCTION reject_ready_plan()").execute(&db).await.unwrap();
    assert!(f.prepare(&db, last).await.is_err());
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM run_plan_shards WHERE run_id=$1")
        .bind(f.plan.run_id)
        .fetch_one(&db)
        .await
        .unwrap();
    assert_eq!(rows, 0);
    assert!(
        load(&db, f.plan.run_id)
            .await
            .unwrap()
            .unwrap()
            .manifest
            .is_none()
    );
    sqlx::query("DROP TRIGGER reject_ready_plan ON run_plans")
        .execute(&db)
        .await
        .unwrap();
    let root = f.prepare(&db, last).await.unwrap();
    let saved = load(&db, f.plan.run_id).await.unwrap().unwrap();
    assert_eq!(saved.status, "ready");
    assert_eq!(saved.manifest.unwrap().0, root);
    assert!(claim(&db, f.plan.run_id).await.is_err());
    let replay = begin(
        &db,
        f.plan.run_id,
        f.upload.id,
        &f.upload.binding_hash,
        &f.plan.limits,
        &f.draft,
        &f.plan.execution_plan,
        &f.plan.assignments,
    )
    .await
    .unwrap();
    assert_eq!(replay.status, "ready");
    let (status,cursors,shards):(String,i64,i64)=sqlx::query_as("SELECT status::text,(SELECT count(*) FROM run_shard_dispatch_cursors),(SELECT count(*) FROM run_plan_shards WHERE run_id=$1) FROM runs WHERE id=$1").bind(f.plan.run_id).fetch_one(&db).await.unwrap();
    assert_eq!((status.as_str(), cursors, shards), ("creating", 0, 3));
    let projection: serde_json::Value =
        sqlx::query_scalar("SELECT config_snapshot->'settlement_plan' FROM runs WHERE id=$1")
            .bind(f.plan.run_id)
            .fetch_one(&db)
            .await
            .unwrap();
    let shards:Vec<(i16,i32,i32,String)>=sqlx::query_as("SELECT run_shard,case_count,chunk_count,planned_chunk_hash FROM run_plan_shards WHERE run_id=$1 ORDER BY run_shard").bind(f.plan.run_id).fetch_all(&db).await.unwrap();
    assert_eq!(projection["version"], 1);
    for (shard, cases, chunks, hash) in shards {
        assert_eq!(
            projection["shards"][shard.to_string()],
            json!({"hash":hash,"case_count":cases,"chunk_count":chunks})
        );
    }
    let old_tables:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name=ANY(ARRAY['case_blobs','dataset_version_cases','run_shard_cases','run_creation_chunks','run_creation_placements'])").fetch_one(&db).await.unwrap();
    assert_eq!(old_tables, 0);
    assert!(
        f.memory
            .0
            .lock()
            .unwrap()
            .creates
            .values()
            .all(|&count| count == 1)
    );
    let bytes = f.memory.0.lock().unwrap().objects[&root.key].clone();
    let manifest: RunPlanManifest = decode(&root.verify(&bytes, &f.plan.limits).unwrap()).unwrap();
    manifest.validate(&f.plan.limits).unwrap();
    assert_eq!(manifest.chunk_count, 3);
    for shard in &manifest.shards {
        assert_eq!(shard.gate_counts.page_count, 3);
        assert_eq!(shard.gate_counts.entry_count, 5);
        let mut chain = GateCountChainValidator::new(
            &shard.gate_counts,
            shard.run_shard,
            shard.case_count,
            &f.plan.limits,
        )
        .unwrap();
        while let Some(reference) = chain.next().cloned() {
            let bytes = f.memory.0.lock().unwrap().objects[&reference.key].clone();
            let page: GateCountPage =
                decode(&reference.verify(&bytes, &f.plan.limits).unwrap()).unwrap();
            assert!(
                page.entries
                    .iter()
                    .all(|entry| entry.planned_count == shard.case_count)
            );
            chain.accept(&reference, &page, &f.plan.limits).unwrap();
        }
        chain.finish().unwrap();
    }

    assert_eq!(
        manifest.shards.iter().map(|s| s.case_count).sum::<i32>(),
        205
    );
    let bytes = f.memory.0.lock().unwrap().objects[&manifest.configuration.key].clone();
    let configuration: RunConfiguration = decode(
        &manifest
            .configuration
            .verify(&bytes, &f.plan.limits)
            .unwrap(),
    )
    .unwrap();
    configuration.validate(&f.plan.limits).unwrap();
    assert_eq!(
        configuration.snapshot["executability"]["expected_evaluator_execution_count"],
        205
    );
}
#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn competing_expired_cancelled_and_changed_replays_fail_closed(db: PgPool) {
    let f = Fixture::new(&db).await;
    let lease = claim(&db, f.plan.run_id).await.unwrap();
    assert!(claim(&db, f.plan.run_id).await.is_err());
    let mut draft = f.draft.clone();
    draft.profile_hash = "f".repeat(64);
    assert!(
        begin(
            &db,
            f.plan.run_id,
            f.upload.id,
            &f.upload.binding_hash,
            &f.plan.limits,
            &draft,
            &f.plan.execution_plan,
            &f.plan.assignments
        )
        .await
        .is_err()
    );
    let mut wrong_dataset = f.draft.clone();
    wrong_dataset.dataset_id = Uuid::now_v7();
    assert!(
        begin(
            &db,
            f.plan.run_id,
            f.upload.id,
            &f.upload.binding_hash,
            &f.plan.limits,
            &wrong_dataset,
            &f.plan.execution_plan,
            &f.plan.assignments
        )
        .await
        .is_err()
    );
    let mut assignments = f.plan.assignments.0.clone();
    assignments[0].database_alias = "wrong".into();
    assert!(
        begin(
            &db,
            f.plan.run_id,
            f.upload.id,
            &f.upload.binding_hash,
            &f.plan.limits,
            &f.draft,
            &f.plan.execution_plan,
            &assignments
        )
        .await
        .is_err()
    );
    let mut limits = f.plan.limits.0.clone();
    limits.page_entries = 3;
    assert!(
        begin(
            &db,
            f.plan.run_id,
            f.upload.id,
            &f.upload.binding_hash,
            &limits,
            &f.draft,
            &f.plan.execution_plan,
            &f.plan.assignments
        )
        .await
        .is_err()
    );
    sqlx::query("UPDATE runs SET coordinator_leased_until=clock_timestamp()-interval '1 second' WHERE id=$1").bind(f.plan.run_id).execute(&db).await.unwrap();
    assert!(renew(&db, lease).await.is_err());
    let replacement = claim(&db, f.plan.run_id).await.unwrap();
    assert!(renew(&db, lease).await.is_err());
    sqlx::query("UPDATE runs SET status='cancelled' WHERE id=$1")
        .bind(f.plan.run_id)
        .execute(&db)
        .await
        .unwrap();
    assert!(renew(&db, replacement).await.is_err());
    assert!(f.prepare(&db, replacement).await.is_err());
    assert!(
        load(&db, f.plan.run_id)
            .await
            .unwrap()
            .unwrap()
            .manifest
            .is_none()
    );
}
#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn corrupted_or_missing_dataset_cannot_publish_run_plan(db: PgPool) {
    let f = Fixture::new(&db).await;
    let lease = claim(&db, f.plan.run_id).await.unwrap();
    let key = f
        .memory
        .0
        .lock()
        .unwrap()
        .objects
        .keys()
        .find(|key| key.contains("/batch/"))
        .unwrap()
        .clone();
    let original = f.memory.0.lock().unwrap().objects.remove(&key).unwrap();
    assert!(f.prepare(&db, lease).await.is_err());
    assert!(
        load(&db, f.plan.run_id)
            .await
            .unwrap()
            .unwrap()
            .manifest
            .is_none()
    );
    let mut changed = original.clone();
    changed[0] ^= 1;
    f.memory
        .0
        .lock()
        .unwrap()
        .objects
        .insert(key.clone(), changed);
    assert!(f.prepare(&db, lease).await.is_err());
    f.memory.0.lock().unwrap().objects.insert(key, original);
    f.prepare(&db, lease).await.unwrap();
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn run_plan_ranges_and_owned_keys_are_immutable_and_acknowledged_contiguously(db: PgPool) {
    let f = Fixture::new(&db).await;
    let lease = claim(&db, f.plan.run_id).await.unwrap();
    let object = |start, end| {
        let reference = ObjectRef::for_bytes(
            "cases".into(),
            format!("runs/{}/chunks/{start}-{end}", f.plan.run_id),
            StorageFormat::RunChunks,
            b"chunk",
            &f.plan.limits,
        )
        .unwrap();
        Object {
            object_key: reference.key.clone(),
            kind: "chunks".into(),
            level: 0,
            range_start: Some(start),
            range_end: Some(end),
            reference: Json(reference),
            acknowledged: false,
        }
    };
    let first = object(0, 1);
    let gap = object(2, 3);
    assert!(!plan(&db, lease, &first).await.unwrap());
    plan(&db, lease, &gap).await.unwrap();
    assert!(acknowledge(&db, lease, &gap).await.is_err());
    acknowledge(&db, lease, &first).await.unwrap();
    acknowledge(&db, lease, &first).await.unwrap();
    assert_eq!(
        load(&db, f.plan.run_id)
            .await
            .unwrap()
            .unwrap()
            .chunk_cursor,
        1
    );
    let mut changed = first.clone();
    changed.reference.digest = "f".repeat(64);
    changed.object_key.push_str("-changed");
    changed.reference.key = changed.object_key.clone();
    assert!(plan(&db, lease, &changed).await.is_err());
    let mut unowned = object(1, 2);
    unowned.object_key = "runs/someone-else/chunks/1-2".into();
    unowned.reference.key = unowned.object_key.clone();
    assert!(plan(&db, lease, &unowned).await.is_err());
    let mut owner = object(1, 2);
    owner.reference.storage_alias = "elsewhere".into();
    assert!(plan(&db, lease, &owner).await.is_err());
}
#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn plan_lease_expiring_during_row_lock_wait_cannot_be_renewed(db: PgPool) {
    let f = Fixture::new(&db).await;
    let lease = claim(&db, f.plan.run_id).await.unwrap();
    sqlx::query("UPDATE runs SET coordinator_leased_until=clock_timestamp()+interval '2 seconds' WHERE id=$1").bind(f.plan.run_id).execute(&db).await.unwrap();
    let mut lock = db.begin().await.unwrap();
    sqlx::query("SELECT id FROM runs WHERE id=$1 FOR UPDATE")
        .bind(f.plan.run_id)
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    let pool = db.clone();
    let renew = tokio::spawn(async move { renew(&pool, lease).await });
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))",
            )
            .bind(blocker)
            .fetch_one(&db)
            .await
            .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
    lock.commit().await.unwrap();
    assert!(
        renew.await.unwrap().is_err(),
        "expired authority was resurrected after waiting for the row lock"
    );
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn reused_dataset_identity_does_not_require_a_row_write_lock(db: PgPool) {
    let f = Fixture::new(&db).await;
    let id = Uuid::now_v7();
    let mut draft = f.draft.clone();
    draft.run_key = id.to_string();
    let mut lock = db.begin().await.unwrap();
    sqlx::query(
        "SELECT dataset_version_id FROM dataset_versions WHERE dataset_version_id=$1 FOR NO KEY UPDATE",
    )
    .bind(draft.dataset_version_id)
    .fetch_one(&mut *lock)
    .await
    .unwrap();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        begin(
            &db,
            id,
            f.upload.id,
            &f.upload.binding_hash,
            &f.plan.limits,
            &draft,
            &f.plan.execution_plan,
            &f.plan.assignments,
        ),
    )
    .await;
    lock.rollback().await.unwrap();
    assert!(
        result.is_ok(),
        "reusing immutable dataset identity waited for a row write lock"
    );
    assert!(result.unwrap().is_ok());
}

fn runtime_context(db: &PgPool) -> crate::context::Context {
    use sqlx::ConnectOptions;

    use crate::context::{
        database,
        output::OutputFormat,
        wasm,
    };
    crate::context::Context::new(
        database::Config::new(
            db.connect_options().to_url_lossy().to_string(),
            5,
            std::time::Duration::from_secs(10),
            database::CircuitBreakerConfig::default(),
            database::PlacementConfig::default_single_database(),
        ),
        None,
        wasm::Config::default(),
        OutputFormat::Json,
    )
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn blob_admission_resumes_bounded_pages_and_reads_exact_cross_file_chunks(db: PgPool) {
    let f = Fixture::new(&db).await;
    let lease = claim(&db, f.plan.run_id).await.unwrap();
    let root = f.prepare(&db, lease).await.unwrap();
    let plan = load(&db, f.plan.run_id).await.unwrap().unwrap();
    let context = runtime_context(&db);
    let memory = f.memory.clone();
    let store: Arc<dyn crate::blob_storage::BlobStorage> = Arc::new(f.store);
    assert!(
        !crate::run_admission::resume(
            &context,
            &plan,
            store.clone(),
            crate::db::workflows::run_creation::Config {
                case_page_budget: 1,
                ..Default::default()
            },
            &f.cancel
        )
        .await
        .unwrap()
    );
    let counts:(i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM run_chunks WHERE run_id=$1),(SELECT count(*) FROM run_shard_dispatch_cursors WHERE run_id=$1)").bind(plan.run_id).fetch_one(&db).await.unwrap();
    assert_eq!(counts, (2, 0));
    assert!(
        crate::run_admission::resume(
            &context,
            &plan,
            store.clone(),
            crate::db::workflows::run_creation::Config {
                case_page_budget: 1,
                ..Default::default()
            },
            &f.cancel
        )
        .await
        .unwrap()
    );
    let reader = crate::case_reader::RunReader::open(
        store,
        &root,
        plan.limits.0.clone(),
        plan.run_id,
        &f.cancel,
    )
    .await
    .unwrap();
    let chunks: Vec<crate::models::run_chunk::RunChunk> =
        sqlx::query_as("SELECT * FROM run_chunks WHERE run_id=$1 ORDER BY ordinal_start")
            .bind(plan.run_id)
            .fetch_all(&db)
            .await
            .unwrap();
    assert_eq!(chunks.len(), 3);
    for chunk in &chunks {
        let cases = reader.cases(chunk, &f.cancel).await.unwrap();
        assert_eq!(
            cases.len(),
            (chunk.ordinal_end - chunk.ordinal_start) as usize
        );
        assert_eq!(cases[0].case_ordinal, chunk.ordinal_start);
        assert_eq!(cases.last().unwrap().case_ordinal, chunk.ordinal_end - 1);
        assert!(
            cases
                .iter()
                .all(|c| c.expected_output.is_null() && c.context_payload.is_null())
        );
    }
    let mut wrong = chunks[0].clone();
    wrong.id = Uuid::now_v7();
    assert!(reader.cases(&wrong, &f.cancel).await.is_err());
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(reader.cases(&chunks[0], &cancelled).await.is_err());
    let key = memory
        .0
        .lock()
        .unwrap()
        .objects
        .keys()
        .find(|key| key.contains("/batch/"))
        .unwrap()
        .clone();
    memory.0.lock().unwrap().objects.get_mut(&key).unwrap()[0] ^= 1;
    let mut rejected = false;
    for chunk in &chunks {
        rejected |= reader.cases(chunk, &f.cancel).await.is_err();
    }
    assert!(rejected, "corrupt assigned case file was accepted");
    let snapshot: serde_json::Value = sqlx::query_scalar(
        "SELECT config_snapshot FROM run_snapshots WHERE run_id=$1 AND run_shard=0",
    )
    .bind(plan.run_id)
    .fetch_one(&db)
    .await
    .unwrap();
    assert!(snapshot["profile"].get("case_groups").is_none());
    assert_eq!(snapshot["gate_planned_counts"]["gate_0"], 100);
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn admission_cancellation_fences_old_owner_and_partial_local_metadata(db: PgPool) {
    let f = Fixture::new(&db).await;
    let lease = claim(&db, f.plan.run_id).await.unwrap();
    f.prepare(&db, lease).await.unwrap();
    let plan = load(&db, f.plan.run_id).await.unwrap().unwrap();
    let context = runtime_context(&db);
    let store: Arc<dyn crate::blob_storage::BlobStorage> = Arc::new(f.store);
    assert!(
        !crate::run_admission::resume(
            &context,
            &plan,
            store.clone(),
            crate::db::workflows::run_creation::Config {
                case_page_budget: 1,
                ..Default::default()
            },
            &f.cancel
        )
        .await
        .unwrap()
    );
    let old = admission::claim(&db, plan.run_id).await.unwrap();
    let result = crate::db::workflows::run_cancel::cancel_run_routed(
        context.dbr().await.unwrap(),
        plan.run_id,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.run.status, "cancelled");
    assert!(admission::renew(&db, old).await.is_err());
    assert!(
        crate::run_admission::resume(
            &context,
            &plan,
            store,
            crate::db::workflows::run_creation::Config {
                case_page_budget: 1,
                ..Default::default()
            },
            &f.cancel
        )
        .await
        .is_err()
    );
    let states: Vec<String> =
        sqlx::query_scalar("SELECT state FROM local_shard_admissions WHERE run_id=$1")
            .bind(plan.run_id)
            .fetch_all(&db)
            .await
            .unwrap();
    assert_eq!(states.len(), 3);
    assert!(states.iter().all(|s| s == "closed"));
    let dispatch: i64 =
        sqlx::query_scalar("SELECT count(*) FROM run_shard_dispatch_cursors WHERE run_id=$1")
            .bind(plan.run_id)
            .fetch_one(&db)
            .await
            .unwrap();
    assert_eq!(dispatch, 0);
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn preparation_only_is_held_until_execution_is_requested(db: PgPool) {
    let f = Fixture::new(&db).await;
    let lease = claim(&db, f.plan.run_id).await.unwrap();
    f.prepare(&db, lease).await.unwrap();
    assert!(admission::claim(&db, f.plan.run_id).await.is_err());
    let context = runtime_context(&db);
    let stats = crate::run_admission::recover(
        &context,
        1,
        crate::db::workflows::run_creation::Config::default(),
    )
    .await
    .unwrap();
    assert_eq!(stats.claimed_runs, 0);
    admission::request(&db, f.plan.run_id).await.unwrap();
    let lease = admission::claim(&db, f.plan.run_id).await.unwrap();
    assert!(
        admission::activate(&db, lease, &std::collections::BTreeMap::new())
            .await
            .is_err()
    );
    release(&db, lease, None).await.unwrap();
    let cursors: i64 =
        sqlx::query_scalar("SELECT count(*) FROM run_shard_dispatch_cursors WHERE run_id=$1")
            .bind(f.plan.run_id)
            .fetch_one(&db)
            .await
            .unwrap();
    assert_eq!(cursors, 0);
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn admission_rejects_changed_local_membership_after_acknowledgement(db: PgPool) {
    let f = Fixture::new(&db).await;
    let lease = claim(&db, f.plan.run_id).await.unwrap();
    f.prepare(&db, lease).await.unwrap();
    let plan = load(&db, f.plan.run_id).await.unwrap().unwrap();
    let context = runtime_context(&db);
    let store: Arc<dyn crate::blob_storage::BlobStorage> = Arc::new(f.store);
    let config = crate::db::workflows::run_creation::Config {
        case_page_budget: 1,
        ..Default::default()
    };
    assert!(
        !crate::run_admission::resume(&context, &plan, store.clone(), config, &f.cancel)
            .await
            .unwrap()
    );
    sqlx::query("UPDATE run_chunks SET id=$2 WHERE run_id=$1 AND run_shard=0")
        .bind(plan.run_id)
        .bind(Uuid::now_v7())
        .execute(&db)
        .await
        .unwrap();
    assert!(
        crate::run_admission::resume(&context, &plan, store, config, &f.cancel)
            .await
            .is_err()
    );
    let status: String = sqlx::query_scalar("SELECT status::text FROM runs WHERE id=$1")
        .bind(plan.run_id)
        .fetch_one(&db)
        .await
        .unwrap();
    assert_eq!(status, "failed");
    let cursors: i64 =
        sqlx::query_scalar("SELECT count(*) FROM run_shard_dispatch_cursors WHERE run_id=$1")
            .bind(plan.run_id)
            .fetch_one(&db)
            .await
            .unwrap();
    assert_eq!(cursors, 0);
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn placement_admission_batches_snapshot_statements_without_changing_shard_snapshots(
    db: PgPool,
) {
    let f = Fixture::new(&db).await;
    let lease = claim(&db, f.plan.run_id).await.unwrap();
    f.prepare(&db, lease).await.unwrap();
    sqlx::raw_sql("CREATE TABLE snapshot_statement_sizes(n bigint NOT NULL); CREATE FUNCTION observe_snapshot_statement() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO snapshot_statement_sizes SELECT count(*) FROM inserted; RETURN NULL; END $$; CREATE TRIGGER observe_snapshot_statement AFTER INSERT ON run_snapshots REFERENCING NEW TABLE AS inserted FOR EACH STATEMENT EXECUTE FUNCTION observe_snapshot_statement();").execute(&db).await.unwrap();
    let plan = load(&db, f.plan.run_id).await.unwrap().unwrap();
    assert!(
        crate::run_admission::resume(
            &runtime_context(&db),
            &plan,
            Arc::new(f.store),
            crate::db::workflows::run_creation::Config::default(),
            &f.cancel
        )
        .await
        .unwrap()
    );
    let sizes: Vec<i64> = sqlx::query_scalar("SELECT n FROM snapshot_statement_sizes ORDER BY n")
        .fetch_all(&db)
        .await
        .unwrap();
    assert_eq!(
        sizes,
        vec![1, 2],
        "each bounded placement page should insert its shard snapshots together"
    );
    let rows: Vec<(i16, i32, serde_json::Value)> = sqlx::query_as("SELECT run_shard,expected_execution_count,config_snapshot FROM run_snapshots WHERE run_id=$1 ORDER BY run_shard").bind(plan.run_id).fetch_all(&db).await.unwrap();
    assert_eq!(rows.len(), 3);
    for (shard, count, snapshot) in rows {
        assert_eq!(count, if shard == 2 { 5 } else { 100 });
        assert_eq!(snapshot["gate_planned_counts"]["gate_0"], count);
        assert_eq!(
            snapshot["settlement_plan"]["shards"]
                .as_object()
                .unwrap()
                .len(),
            1
        );
        assert!(
            snapshot["settlement_plan"]["shards"]
                .get(shard.to_string())
                .is_some()
        );
    }
}

#[path = "postgres_tests/admission.rs"]
mod batched_admission;

#[path = "postgres_tests/metadata.rs"]
mod metadata;
