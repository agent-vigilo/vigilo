//! Observable cache isolation, bounded retention and fail-closed read contracts.
use super::*;
use crate::case_reader::cache::Scope;

fn scope() -> Scope {
    Scope {
        cache: Arc::new(ReadCache::default()),
        binding: "owner-a".into(),
    }
}
async fn read(scope: &Scope, f: &Fixture, reference: &ObjectRef) -> Result<Vec<u8>> {
    scope
        .read(
            f.reader.store.as_ref(),
            reference,
            &f.reader.limits,
            Budget::standard(),
            &CancellationToken::new(),
        )
        .await
}

#[tokio::test]
async fn warm_hits_preserve_cancellation_deadlines_limits_and_budgets() {
    let f = Fixture::new().await;
    let scope = scope();
    read(&scope, &f, &f.batch).await.unwrap();
    f.memory.1.lock().unwrap().clear();
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(
        scope
            .read(
                f.reader.store.as_ref(),
                &f.batch,
                &f.reader.limits,
                Budget::standard(),
                &cancelled
            )
            .await
            .is_err()
    );
    for budget in [
        Budget {
            requests: 0,
            ..Budget::standard()
        },
        Budget {
            deadline: tokio::time::Instant::now(),
            ..Budget::standard()
        },
    ] {
        assert!(
            scope
                .read(
                    f.reader.store.as_ref(),
                    &f.batch,
                    &f.reader.limits,
                    budget,
                    &CancellationToken::new()
                )
                .await
                .is_err()
        );
    }
    let mut limits = f.reader.limits.clone();
    limits.batch.max_bytes = 1;
    assert!(
        scope
            .read(
                f.reader.store.as_ref(),
                &f.batch,
                &limits,
                Budget::standard(),
                &CancellationToken::new()
            )
            .await
            .is_err()
    );
    assert!(f.memory.1.lock().unwrap().is_empty());
    read(&scope, &f, &f.batch).await.unwrap();
}

#[tokio::test]
async fn every_reference_field_and_physical_binding_isolated() {
    let f = Fixture::new().await;
    let scope = scope();
    let original = read(&scope, &f, &f.batch).await.unwrap();
    f.memory.1.lock().unwrap().clear();
    // Changing the physical binding must fetch, even for an identical reference.
    let other = Scope {
        cache: scope.cache.clone(),
        binding: "owner-b".into(),
    };
    assert_eq!(read(&other, &f, &f.batch).await.unwrap(), original);
    assert_eq!(f.memory.1.lock().unwrap().len(), 1);
    // Another key with the same bytes is a distinct object.
    let mut key = f.batch.clone();
    key.key.push_str("-other");
    f.memory
        .0
        .lock()
        .unwrap()
        .insert(key.key.clone(), original.clone());
    read(&scope, &f, &key).await.unwrap();
    assert_eq!(f.memory.1.lock().unwrap().len(), 2);
    // Format changes cannot reuse the previous proof.
    let mut schema = f.batch.clone();
    schema.schema = StorageFormat::RunConfiguration;
    read(&scope, &f, &schema).await.unwrap();
    assert_eq!(f.memory.1.lock().unwrap().len(), 3);
    let mut alias = f.batch.clone();
    alias.storage_alias = "other".into();
    assert!(read(&scope, &f, &alias).await.is_err());
    let mut revision = f.batch.clone();
    revision.revision = Some("different-revision".into());
    assert!(read(&scope, &f, &revision).await.is_err());
    let mut digest = f.batch.clone();
    digest.digest = "0".repeat(64);
    assert!(read(&scope, &f, &digest).await.is_err());
    let mut size = f.batch.clone();
    size.byte_count += 1;
    assert!(read(&scope, &f, &size).await.is_err());
    // HashScheme has only one accepted variant; unknown schemes fail at decode.
    assert_eq!(read(&scope, &f, &f.batch).await.unwrap(), original);
}

#[tokio::test]
async fn missing_and_corrupt_reads_do_not_poison_the_cache() {
    let f = Fixture::new().await;
    let scope = scope();
    let original = f.memory.0.lock().unwrap().remove(&f.batch.key).unwrap();
    assert!(read(&scope, &f, &f.batch).await.is_err());
    let mut corrupt = original.clone();
    corrupt[0] ^= 1;
    f.memory
        .0
        .lock()
        .unwrap()
        .insert(f.batch.key.clone(), corrupt);
    assert!(read(&scope, &f, &f.batch).await.is_err());
    f.memory
        .0
        .lock()
        .unwrap()
        .insert(f.batch.key.clone(), original.clone());
    assert_eq!(read(&scope, &f, &f.batch).await.unwrap(), original);
    f.memory.0.lock().unwrap().remove(&f.batch.key);
    assert_eq!(read(&scope, &f, &f.batch).await.unwrap(), original);
}

#[tokio::test]
async fn entry_bound_evicts_least_recently_used_and_rechecks_remote_bytes() {
    let f = Fixture::new().await;
    let scope = scope();
    let mut refs = Vec::new();
    for n in 0..256 {
        let r = put(
            &f.memory,
            &f.reader.limits,
            StorageFormat::CaseBatch,
            format!("file-{n}").into_bytes(),
        );
        read(&scope, &f, &r).await.unwrap();
        refs.push(r);
    }
    // Refresh the oldest entry; the next insertion must evict entry 1 instead.
    read(&scope, &f, &refs[0]).await.unwrap();
    let next = put(
        &f.memory,
        &f.reader.limits,
        StorageFormat::CaseBatch,
        b"extra".to_vec(),
    );
    read(&scope, &f, &next).await.unwrap();
    f.memory
        .0
        .lock()
        .unwrap()
        .insert(refs[1].key.clone(), b"corrupt".to_vec());
    f.memory.1.lock().unwrap().clear();
    read(&scope, &f, &refs[0]).await.unwrap();
    assert!(f.memory.1.lock().unwrap().is_empty());
    assert!(read(&scope, &f, &refs[1]).await.is_err());
    assert_eq!(f.memory.1.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn byte_bound_evicts_and_oversize_files_bypass_retention() {
    let f = Fixture::new().await;
    let scope = scope();
    let mut refs = Vec::new();
    for n in 0..8 {
        let r = put(
            &f.memory,
            &f.reader.limits,
            StorageFormat::CaseBatch,
            vec![n; 1024 * 1024],
        );
        read(&scope, &f, &r).await.unwrap();
        refs.push(r);
    }
    // Eight 1-MiB payloads plus identity overhead exceed the byte cap.
    f.memory.1.lock().unwrap().clear();
    read(&scope, &f, &refs[7]).await.unwrap();
    assert!(f.memory.1.lock().unwrap().is_empty());
    read(&scope, &f, &refs[0]).await.unwrap();
    assert_eq!(f.memory.1.lock().unwrap().len(), 1);
    let big = put(
        &f.memory,
        &f.reader.limits,
        StorageFormat::CaseBatch,
        vec![42; 1024 * 1024 + 1],
    );
    f.memory.1.lock().unwrap().clear();
    for _ in 0..2 {
        read(&scope, &f, &big).await.unwrap();
    }
    assert_eq!(f.memory.1.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn concurrent_readers_and_cancelled_caller_remain_independent() {
    let f = Fixture::new().await;
    let scope = scope();
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let (a, b, c) = tokio::join!(
        read(&scope, &f, &f.batch),
        read(&scope, &f, &f.batch),
        scope.read(
            f.reader.store.as_ref(),
            &f.batch,
            &f.reader.limits,
            Budget::standard(),
            &cancelled
        )
    );
    assert_eq!(a.unwrap(), b.unwrap());
    assert!(c.is_err());
    f.memory.1.lock().unwrap().clear();
    read(&scope, &f, &f.batch).await.unwrap();
    assert!(f.memory.1.lock().unwrap().is_empty());
}

#[derive(Clone)]
struct PausedRead {
    memory: Memory,
    key: String,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}
#[async_trait::async_trait]
impl Transport for PausedRead {
    async fn put(
        &self,
        r: &ObjectRef,
        path: &std::path::Path,
        size: usize,
        deadline: Duration,
    ) -> std::result::Result<(), StorageError> {
        self.memory.put(r, path, size, deadline).await
    }
    async fn get(
        &self,
        r: &ObjectRef,
        deadline: Duration,
    ) -> std::result::Result<Download, StorageError> {
        if r.key == self.key {
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.memory.get(r, deadline).await
    }
    async fn delete_object(&self, key: &str) -> std::result::Result<(), StorageError> {
        self.memory.delete_object(key).await
    }
}

#[tokio::test]
async fn cancelling_an_inflight_miss_releases_it_without_blocking_other_keys() {
    let f = Fixture::new().await;
    let gate = PausedRead {
        memory: f.memory.clone(),
        key: f.batch.key.clone(),
        entered: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let store: Arc<dyn BlobStorage> = Arc::new(
        Store::connect(
            gate.clone(),
            "cases".into(),
            f.reader.limits.clone(),
            Resources::standard(),
            Budget::standard(),
            &CancellationToken::new(),
            "capability-probes/paused".into(),
        )
        .await
        .unwrap(),
    );
    let scope = scope();
    let cancel = CancellationToken::new();
    let (s, storage, reference, limits, c) = (
        scope.clone(),
        store.clone(),
        f.batch.clone(),
        f.reader.limits.clone(),
        cancel.clone(),
    );
    let pending = tokio::spawn(async move {
        s.read(
            storage.as_ref(),
            &reference,
            &limits,
            Budget::standard(),
            &c,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(2),
        scope.read(
            store.as_ref(),
            &f.root,
            &f.reader.limits,
            Budget::standard(),
            &CancellationToken::new(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    cancel.cancel();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), pending)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    gate.release.notify_one();
    f.memory.1.lock().unwrap().clear();
    scope
        .read(
            store.as_ref(),
            &f.batch,
            &f.reader.limits,
            Budget::standard(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(f.memory.1.lock().unwrap().len(), 1);
}
