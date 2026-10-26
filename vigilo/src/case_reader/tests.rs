//! Service-free worker input verification with deterministic in-memory objects.
use std::{
    collections::BTreeMap,
    sync::Mutex,
    time::Duration,
};

use serde_json::json;

use super::*;
use crate::blob_storage::{
    Download,
    ErrorKind,
    Resources,
    StorageError,
    Store,
    Transport,
};

#[derive(Clone, Default)]
struct Memory(Arc<Mutex<BTreeMap<String, Vec<u8>>>>);
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
        _: Duration,
    ) -> std::result::Result<(), StorageError> {
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|_| error(ErrorKind::Local))?;
        let mut files = self.0.lock().unwrap();
        if files.contains_key(&r.key) {
            return Err(error(ErrorKind::Conflict));
        }
        files.insert(r.key.clone(), bytes);
        Ok(())
    }
    async fn get(&self, r: &ObjectRef, _: Duration) -> std::result::Result<Download, StorageError> {
        let bytes = self
            .0
            .lock()
            .unwrap()
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
        self.0.lock().unwrap().remove(key);
        Ok(())
    }
}
struct Fixture {
    reader: RunReader,
    chunks: Vec<ClaimedChunk>,
    memory: Memory,
    root: ObjectRef,
    batch: ObjectRef,
}
fn put(memory: &Memory, limits: &FormatLimits, kind: StorageFormat, bytes: Vec<u8>) -> ObjectRef {
    let key = format!("files/{}", blake3::hash(&bytes).to_hex());
    let r = ObjectRef::for_bytes("cases".into(), key, kind, &bytes, limits).unwrap();
    memory.0.lock().unwrap().insert(r.key.clone(), bytes);
    r
}
fn metadata<T: StorageDocument>(memory: &Memory, limits: &FormatLimits, value: &T) -> ObjectRef {
    put(memory, limits, T::SCHEMA, encode(value, limits).unwrap())
}
impl Fixture {
    async fn new() -> Self {
        let limits = FormatLimits {
            batch: BatchLimits {
                max_cases: 128,
                ..Default::default()
            },
            ..Default::default()
        };
        let memory = Memory::default();
        let cancel = CancellationToken::new();
        let store: Arc<dyn BlobStorage> = Arc::new(
            Store::connect(
                memory.clone(),
                "cases".into(),
                limits.clone(),
                Resources::standard(),
                Budget::standard(),
                &cancel,
                "capability-probes/test".into(),
            )
            .await
            .unwrap(),
        );
        let run = Uuid::from_u128(1);
        let dataset_id = Uuid::from_u128(2);
        let dataset_version_id = Uuid::from_u128(3);
        let records=(0..101).map(|n|{
            let source=json!({"id":Uuid::from_u128(n+10),"task_type":"classification","input":{"text":format!("case {n}")},"expected":null,"context":null,"tags":["zeta","release","alpha","release"],"metadata":{"position":n}});
            let typed=serde_json::from_value(source.clone()).unwrap();
            CaseRecord{case_ordinal:n as i32,case_hash:identity::case_hash(&typed).unwrap(),source}
        }).collect::<Vec<_>>();
        let batch = put(
            &memory,
            &limits,
            StorageFormat::CaseBatch,
            encode_batch(
                &CaseBatchHeader {
                    schema: StorageFormat::CaseBatch,
                    dataset_id,
                    ordinal_start: 0,
                    ordinal_end: 101,
                },
                &records,
                &limits,
            )
            .unwrap(),
        );
        let batches = metadata(
            &memory,
            &limits,
            &IndexPage {
                schema: StorageFormat::OrdinalIndex,
                kind: IndexKind::DatasetBatches,
                level: 0,
                range_start: 0,
                range_end: 101,
                entries: vec![IndexEntry {
                    range_start: 0,
                    range_end: 101,
                    object: batch.clone(),
                }],
            },
        );
        let fields = metadata(
            &memory,
            &limits,
            &DatasetSourceFields {
                schema: StorageFormat::DatasetSourceFields,
                cases_present: true,
                fields: json!({"dataset_id":dataset_id}),
            },
        );
        let dataset = DatasetManifest {
            schema: StorageFormat::DatasetManifest,
            dataset_id,
            dataset_version: None,
            dataset_version_id,
            dataset_hash: "0".repeat(64),
            identity_scheme: IdentityScheme::DatasetJson,
            case_count: 101,
            batch_limits: limits.batch,
            source_fields: fields,
            batches: IndexDescriptor {
                level: 0,
                range_start: 0,
                range_end: 101,
                object: batches,
            },
        };
        let dataset_ref = metadata(&memory, &limits, &dataset);
        let mut chunks = Vec::new();
        let mut plans = Vec::new();
        for (index, (start, end)) in [(0, 100), (100, 101)].into_iter().enumerate() {
            let id = Uuid::from_u128(index as u128 + 1000);
            let shard = index as i16;
            let mut hash = blake3::Hasher::new();
            hash.update(b"vigilo/run-shard-cases/v1");
            for record in &records[start as usize..end as usize] {
                case_projection::update_projection_hash(
                    &mut hash,
                    &RunShardCaseDraft {
                        run_id: run,
                        run_shard: shard,
                        dataset_version_id,
                        case_id: record.validate().unwrap().id,
                        case_ordinal: record.case_ordinal,
                        case_hash: record.case_hash.clone(),
                    },
                );
            }
            plans.push(crate::contracts::case_storage::RunChunk {
                chunk_index: index as i32,
                chunk_id: id,
                run_shard: shard,
                ordinal_start: start,
                ordinal_end: end,
                profile_group_id: "default".into(),
                case_projection_hash: hash.finalize().to_hex().to_string(),
            });
            chunks.push(ClaimedChunk {
                write_epoch: 1,
                id,
                run_id: run,
                run_shard: shard,
                dataset_version_id,
                profile_group_id: "default".into(),
                ordinal_start: start,
                ordinal_end: end,
                status: "leased".into(),
                lease_token: Some(Uuid::nil()),
                leased_until: None,
                created_at: chrono::DateTime::UNIX_EPOCH,
                updated_at: chrono::DateTime::UNIX_EPOCH,
            });
        }
        let chunk_file = metadata(
            &memory,
            &limits,
            &RunChunkPage {
                schema: StorageFormat::RunChunks,
                run_id: run,
                chunk_index_start: 0,
                chunk_index_end: 2,
                chunks: plans,
            },
        );
        let index = metadata(
            &memory,
            &limits,
            &IndexPage {
                schema: StorageFormat::OrdinalIndex,
                kind: IndexKind::RunChunks,
                level: 0,
                range_start: 0,
                range_end: 2,
                entries: vec![IndexEntry {
                    range_start: 0,
                    range_end: 2,
                    object: chunk_file,
                }],
            },
        );
        let profile: serde_json::Value =
            serde_yaml::from_str(include_str!("../../../example/profile.yaml")).unwrap();
        let execution =
            crate::contracts::evaluator_abi::EvaluatorExecutionPlan::new("1".repeat(64), vec![]);
        let profile_hash =
            identity::hash_serialized_json(&identity::canonical_json(&profile)).unwrap();
        let configuration = RunConfiguration {
            schema: StorageFormat::RunConfiguration,
            snapshot: json!({"profile":profile,"agent":profile["agent"],"dataset_ref":{"dataset_id":dataset_id,"dataset_version":null,"dataset_version_id":dataset_version_id,"dataset_hash":dataset.dataset_hash,"case_count":101},"dataset_version_id":dataset_version_id,"profile_version_id":"fixture/1","profile_hash":profile_hash,"dataset_hash":dataset.dataset_hash,"aggregation_policy_hash":execution.aggregation_policy_hash,"execution_plan_hash":execution.hash().unwrap(),"execution_plan":execution,"chunk_size":100,"executability":{"expected_execution_count":101,"expected_evaluator_execution_count":101,"unique_evaluator_ref_count":1,"runnable_evaluator_ref_count":1}}),
        };
        let config = metadata(&memory, &limits, &configuration);
        let manifest = RunPlanManifest {
            schema: StorageFormat::RunPlanManifest,
            run_id: run,
            dataset: dataset_ref,
            profile_hash,
            aggregation_policy_hash: execution.aggregation_policy_hash.clone(),
            execution_plan_hash: execution.hash().unwrap(),
            configuration: config,
            chunk_count: 2,
            chunks: IndexDescriptor {
                level: 0,
                range_start: 0,
                range_end: 2,
                object: index,
            },
            shards: (0..2)
                .map(|n| RunPlanShard {
                    run_shard: n,
                    case_count: if n == 0 { 100 } else { 1 },
                    chunk_count: 1,
                    planned_chunk_hash: "2".repeat(64),
                    gate_counts: GateCountDescriptor {
                        first: None,
                        page_count: 0,
                        entry_count: 0,
                    },
                })
                .collect(),
        };
        let root = metadata(&memory, &limits, &manifest);
        let reader = RunReader::open(store, &root, limits, run, &cancel)
            .await
            .unwrap();
        Self {
            reader,
            chunks,
            memory,
            root,
            batch,
        }
    }
}
#[tokio::test]
async fn assigned_ranges_preserve_order_nulls_and_metadata_across_file_boundaries() {
    let f = Fixture::new().await;
    for (i, chunk) in f.chunks.iter().enumerate() {
        let cases = f
            .reader
            .cases(chunk, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(cases.len(), if i == 0 { 100 } else { 1 });
        for (offset, c) in cases.iter().enumerate() {
            assert_eq!(c.case_ordinal, chunk.ordinal_start + offset as i32);
            assert_eq!(c.metadata["position"], c.case_ordinal);
            assert!(c.expected_output.is_null() && c.context_payload.is_null());
            assert_eq!(c.tags, json!(["alpha", "release", "release", "zeta"]));
        }
    }
}
#[tokio::test]
async fn changed_claim_missing_corrupt_and_cancelled_objects_fail_closed() {
    let f = Fixture::new().await;
    for field in 0..5 {
        let mut chunk = f.chunks[0].clone();
        match field {
            0 => chunk.id = Uuid::nil(),
            1 => chunk.run_id = Uuid::nil(),
            2 => chunk.dataset_version_id = Uuid::nil(),
            3 => chunk.ordinal_end = 99,
            _ => chunk.run_shard = 7,
        }
        assert!(
            f.reader
                .cases(&chunk, &CancellationToken::new())
                .await
                .is_err()
        );
    }
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(f.reader.cases(&f.chunks[0], &cancelled).await.is_err());
    let original = f.memory.0.lock().unwrap().remove(&f.batch.key).unwrap();
    assert!(
        f.reader
            .cases(&f.chunks[0], &CancellationToken::new())
            .await
            .is_err()
    );
    let mut changed = original;
    changed[0] ^= 1;
    f.memory
        .0
        .lock()
        .unwrap()
        .insert(f.batch.key.clone(), changed);
    assert!(
        f.reader
            .cases(&f.chunks[0], &CancellationToken::new())
            .await
            .is_err()
    );
    assert!(
        RunReader::open(
            f.reader.store.clone(),
            &f.root,
            f.reader.limits.clone(),
            Uuid::nil(),
            &CancellationToken::new()
        )
        .await
        .is_err()
    );
}
#[tokio::test]
async fn an_unrequested_invalid_tail_rejects_the_whole_case_file() {
    let mut f = Fixture::new().await;
    let original = f.memory.0.lock().unwrap().remove(&f.batch.key).unwrap();
    let mut lines = String::from_utf8(original)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let mut tail: serde_json::Value = serde_json::from_str(lines.last().unwrap()).unwrap();
    tail["case_ordinal"] = json!(999);
    *lines.last_mut().unwrap() = serde_json::to_string(&identity::canonical_json(&tail)).unwrap();
    let batch = put(
        &f.memory,
        &f.reader.limits,
        StorageFormat::CaseBatch,
        format!("{}\n", lines.join("\n")).into_bytes(),
    );
    let index = metadata(
        &f.memory,
        &f.reader.limits,
        &IndexPage {
            schema: StorageFormat::OrdinalIndex,
            kind: IndexKind::DatasetBatches,
            level: 0,
            range_start: 0,
            range_end: 101,
            entries: vec![IndexEntry {
                range_start: 0,
                range_end: 101,
                object: batch,
            }],
        },
    );
    f.reader.dataset.batches.object = index;
    assert!(
        f.reader
            .cases(&f.chunks[0], &CancellationToken::new())
            .await
            .unwrap_err()
            .to_string()
            .contains("ordinal")
    );
}

#[tokio::test]
async fn memory_budget_falls_back_to_verified_batch_rereads_without_losing_cases() {
    let mut f = Fixture::new().await;
    f.reader.limits.resident_batches = 1;
    f.reader.limits.resident_batch_bytes = f.reader.limits.batch.max_bytes;
    for chunk in &f.chunks {
        let mut prepared = f
            .reader
            .prepare(chunk, &CancellationToken::new())
            .await
            .unwrap();
        assert!(prepared.cached.is_none());
        let cases = prepared
            .next(&CancellationToken::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            cases.len(),
            (chunk.ordinal_end - chunk.ordinal_start) as usize
        );
        assert!(
            prepared
                .next(&CancellationToken::new())
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn incorrect_shard_distribution_is_rejected_before_admission() {
    let f = Fixture::new().await;
    let mut manifest = f.reader.manifest.clone();
    manifest.shards[0].case_count = 1;
    manifest.shards[1].case_count = 100;
    let changed = metadata(&f.memory, &f.reader.limits, &manifest);
    let error = RunReader::open(
        f.reader.store.clone(),
        &changed,
        f.reader.limits.clone(),
        manifest.run_id,
        &CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("shard totals"));
}
