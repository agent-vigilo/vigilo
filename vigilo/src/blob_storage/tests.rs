//! Service-free storage contract tests with deterministic in-process transport.
//! The same application operations are exercised against signed S3 in the named
//! service tier; fake failures here prove budgeting without network dependence.
use std::{
    collections::{
        HashMap,
        VecDeque,
    },
    pin::Pin,
    sync::Mutex,
    task::{
        Context,
        Poll,
    },
};

use tokio::io::ReadBuf;

use super::*;

#[derive(Default)]
struct State {
    objects: HashMap<String, Vec<u8>>,
    calls: Vec<String>,
    faults: VecDeque<ErrorKind>,
    ignore_condition: bool,
    lost_response: bool,
    bad_download: Option<Vec<u8>>,
    wrong_length: bool,
    revision: Option<String>,
    stall: bool,
    cleanup_error: bool,
}
#[derive(Clone, Default)]
struct Fake(Arc<Mutex<State>>);
#[async_trait]
impl Transport for Fake {
    async fn put(&self, reference: &ObjectRef, path: &Path, _: usize, _: Duration) -> Result<()> {
        let bytes = tokio::fs::read(path).await.map_err(StorageError::io)?;
        let mut s = self.0.lock().unwrap();
        s.calls.push(format!("put:{}", reference.key));
        if let Some(kind) = s.faults.pop_front() {
            return Err(StorageError::new(kind));
        }
        if s.objects.contains_key(&reference.key) && !s.ignore_condition {
            return Err(StorageError::new(ErrorKind::Conflict));
        }
        s.objects.insert(reference.key.clone(), bytes);
        if s.lost_response {
            s.lost_response = false;
            return Err(StorageError::new(ErrorKind::Transient));
        }
        Ok(())
    }
    async fn get(&self, reference: &ObjectRef, _: Duration) -> Result<Download> {
        let mut s = self.0.lock().unwrap();
        s.calls.push(format!("get:{}", reference.key));
        if let Some(kind) = s.faults.pop_front() {
            return Err(StorageError::new(kind));
        }
        let bytes = s
            .objects
            .get(&reference.key)
            .cloned()
            .ok_or_else(|| StorageError::new(ErrorKind::Missing))?;
        let length = bytes.len() as u64 + u64::from(s.wrong_length);
        let reader: Reader = if s.stall {
            Box::new(Pending)
        } else {
            Box::new(std::io::Cursor::new(
                s.bad_download.clone().unwrap_or(bytes),
            ))
        };
        Ok(Download {
            reader,
            length,
            revision: s.revision.clone(),
        })
    }
    async fn delete_object(&self, key: &str) -> Result<()> {
        let mut s = self.0.lock().unwrap();
        s.calls.push(format!("delete:{key}"));
        if s.cleanup_error {
            return Err(StorageError::new(ErrorKind::Unauthorized));
        }
        s.objects.remove(key);
        Ok(())
    }
}
struct Pending;
impl AsyncRead for Pending {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }
}
fn reference(bytes: &[u8]) -> ObjectRef {
    ObjectRef::for_bytes(
        "tests".into(),
        "cases/test".into(),
        StorageFormat::CaseBatch,
        bytes,
        &FormatLimits::default(),
    )
    .unwrap()
}
fn input(bytes: &[u8]) -> Reader {
    Box::new(std::io::Cursor::new(bytes.to_vec()))
}
async fn store(fake: Fake) -> Store<Fake> {
    Store::connect(
        fake,
        "tests".into(),
        FormatLimits::default(),
        Resources::standard(),
        Budget::standard(),
        &CancellationToken::new(),
        format!("capability-probes/{}", uuid::Uuid::now_v7()),
    )
    .await
    .unwrap()
}
async fn bytes(file: &VerifiedFile) -> Vec<u8> {
    let mut result = Vec::new();
    file.reader()
        .await
        .unwrap()
        .read_to_end(&mut result)
        .await
        .unwrap();
    result
}
fn kind<T>(result: Result<T>) -> StorageError {
    match result {
        Ok(_) => panic!("expected error"),
        Err(e) => e,
    }
}

#[tokio::test]
async fn create_read_replay_conflict_and_missing() {
    let fake = Fake::default();
    let store = store(fake.clone()).await;
    let cancel = CancellationToken::new();
    let r = reference(b"hello");
    assert!(fake.0.lock().unwrap().objects.is_empty()); // exact capability cleanup
    store
        .create(&r, input(b"hello"), Budget::standard(), &cancel)
        .await
        .unwrap();
    let file = store.read(&r, Budget::standard(), &cancel).await.unwrap();
    assert_eq!(bytes(&file).await, b"hello");
    assert_eq!(file.reference(), &r);
    store
        .create(&r, input(b"hello"), Budget::standard(), &cancel)
        .await
        .unwrap();
    let changed = reference(b"other");
    assert_eq!(
        kind(
            store
                .create(&changed, input(b"other"), Budget::standard(), &cancel)
                .await
        )
        .kind,
        ErrorKind::Conflict
    );
    assert_eq!(fake.0.lock().unwrap().objects[&r.key], b"hello");
    let mut missing = r.clone();
    missing.key = "missing".into();
    assert_eq!(
        kind(store.read(&missing, Budget::standard(), &cancel).await).kind,
        ErrorKind::Missing
    );
}
#[tokio::test]
async fn invalid_input_never_reaches_remote_and_releases_reservations() {
    let fake = Fake::default();
    let store = store(fake.clone()).await;
    fake.0.lock().unwrap().calls.clear();
    let cancel = CancellationToken::new();
    let r = reference(b"hello");
    for bad in [b"hell".as_slice(), b"helloo", b"other"] {
        assert_eq!(
            kind(
                store
                    .create(&r, input(bad), Budget::standard(), &cancel)
                    .await
            )
            .kind,
            ErrorKind::Corrupt
        );
    }
    let mut owner = r.clone();
    owner.storage_alias = "unknown".into();
    let mut digest = r.clone();
    digest.digest = "bad".into();
    let mut zero = r.clone();
    zero.byte_count = 0;
    let mut large = r.clone();
    large.byte_count = 64 * 1024 * 1024 + 1;
    for bad in [owner, digest, zero, large] {
        assert_eq!(
            kind(store.read(&bad, Budget::standard(), &cancel).await).kind,
            ErrorKind::Invalid
        );
    }
    let mut bad = r.clone();
    bad.key = "https://secret@host?token=password".into();
    let e = kind(store.read(&bad, Budget::standard(), &cancel).await);
    assert!(!format!("{e:?}").contains("password"));
    let mut bad = Budget::standard();
    bad.requests = 0;
    assert_eq!(
        kind(store.read(&r, bad, &cancel).await).kind,
        ErrorKind::Invalid
    );
    assert!(fake.0.lock().unwrap().calls.is_empty());
    assert_eq!(
        store.resources.disk.available_permits(),
        store.resources.capacity as usize
    );
}
#[tokio::test]
async fn corrupt_truncated_excess_length_and_revision_fail_closed() {
    let fake = Fake::default();
    let store = store(fake.clone()).await;
    let cancel = CancellationToken::new();
    let r = reference(b"hello");
    fake.0
        .lock()
        .unwrap()
        .objects
        .insert(r.key.clone(), b"hello".to_vec());
    for bad in [b"hell".as_slice(), b"helloo", b"other"] {
        fake.0.lock().unwrap().bad_download = Some(bad.to_vec());
        assert_eq!(
            kind(store.read(&r, Budget::standard(), &cancel).await).kind,
            ErrorKind::Corrupt
        );
    }
    fake.0.lock().unwrap().bad_download = None;
    fake.0.lock().unwrap().wrong_length = true;
    assert_eq!(
        kind(store.read(&r, Budget::standard(), &cancel).await).kind,
        ErrorKind::Corrupt
    );
    fake.0.lock().unwrap().wrong_length = false;
    let mut revision = r.clone();
    revision.revision = Some("v1".into());
    assert_eq!(
        kind(store.read(&revision, Budget::standard(), &cancel).await).kind,
        ErrorKind::Corrupt
    );
    fake.0.lock().unwrap().revision = Some("v1".into());
    assert_eq!(
        bytes(
            &store
                .read(&revision, Budget::standard(), &cancel)
                .await
                .unwrap()
        )
        .await,
        b"hello"
    );
}
#[tokio::test]
async fn retries_are_one_finite_request_budget_and_auth_is_not_retried() {
    let fake = Fake::default();
    let store = store(fake.clone()).await;
    let cancel = CancellationToken::new();
    let r = reference(b"hello");
    fake.0
        .lock()
        .unwrap()
        .objects
        .insert(r.key.clone(), b"hello".to_vec());
    fake.0.lock().unwrap().calls.clear();
    fake.0
        .lock()
        .unwrap()
        .faults
        .extend([ErrorKind::Transient, ErrorKind::Transient]);
    store.read(&r, Budget::standard(), &cancel).await.unwrap();
    assert_eq!(fake.0.lock().unwrap().calls.len(), 3);
    fake.0.lock().unwrap().calls.clear();
    fake.0
        .lock()
        .unwrap()
        .faults
        .extend([ErrorKind::Transient; 3]);
    assert_eq!(
        kind(store.read(&r, Budget::standard(), &cancel).await).kind,
        ErrorKind::Exhausted
    );
    assert_eq!(fake.0.lock().unwrap().calls.len(), 3);
    fake.0.lock().unwrap().calls.clear();
    fake.0
        .lock()
        .unwrap()
        .faults
        .push_back(ErrorKind::Unauthorized);
    assert_eq!(
        kind(store.read(&r, Budget::standard(), &cancel).await).kind,
        ErrorKind::Unauthorized
    );
    assert_eq!(fake.0.lock().unwrap().calls.len(), 1);
}
#[tokio::test]
async fn lost_create_response_is_reconciled_without_a_second_write() {
    let fake = Fake::default();
    let store = store(fake.clone()).await;
    let cancel = CancellationToken::new();
    let r = reference(b"hello");
    fake.0.lock().unwrap().lost_response = true;
    fake.0.lock().unwrap().calls.clear();
    store
        .create(&r, input(b"hello"), Budget::standard(), &cancel)
        .await
        .unwrap();
    assert_eq!(
        fake.0.lock().unwrap().calls,
        ["put:cases/test", "get:cases/test"]
    );
}
#[tokio::test]
async fn no_progress_deadline_and_cancellation_release_spools() {
    let fake = Fake::default();
    let store = store(fake.clone()).await;
    let r = reference(b"hello");
    let cancel = CancellationToken::new();
    let mut budget = Budget::standard();
    budget.no_progress = Duration::from_millis(5);
    assert_eq!(
        kind(store.create(&r, Box::new(Pending), budget, &cancel).await).kind,
        ErrorKind::Timeout
    );
    assert_eq!(
        store.resources.disk.available_permits(),
        store.resources.capacity as usize
    );
    let token = cancel.clone();
    let future = store.create(&r, Box::new(Pending), Budget::standard(), &cancel);
    let (result, ()) = tokio::join!(future, async {
        tokio::task::yield_now().await;
        token.cancel();
    });
    assert_eq!(kind(result).kind, ErrorKind::Cancelled);
    let cancel = CancellationToken::new();
    let permit = store
        .resources
        .streams
        .clone()
        .acquire_many_owned(4)
        .await
        .unwrap();
    let mut budget = Budget::standard();
    budget.deadline = Instant::now() + Duration::from_millis(5);
    assert_eq!(
        kind(store.read(&r, budget, &cancel).await).kind,
        ErrorKind::Timeout
    );
    drop(permit);
    fake.0
        .lock()
        .unwrap()
        .objects
        .insert(r.key.clone(), b"hello".to_vec());
    fake.0.lock().unwrap().stall = true;
    assert_eq!(
        kind(store.read(&r, budget_with_short_idle(), &cancel).await).kind,
        ErrorKind::Timeout
    );
    assert_eq!(
        store.resources.disk.available_permits(),
        store.resources.capacity as usize
    );
}
fn budget_with_short_idle() -> Budget {
    let mut b = Budget::standard();
    b.no_progress = Duration::from_millis(5);
    b
}
#[tokio::test]
async fn verified_spools_hold_quota_until_drop() {
    let fake = Fake::default();
    let store = store(fake.clone()).await;
    let r = reference(b"hello");
    let cancel = CancellationToken::new();
    fake.0
        .lock()
        .unwrap()
        .objects
        .insert(r.key.clone(), b"hello".to_vec());
    let file = store.read(&r, Budget::standard(), &cancel).await.unwrap();
    let path = file.file.path().to_owned();
    assert_eq!(
        store.resources.disk.available_permits(),
        store.resources.capacity as usize - 5
    );
    drop(file);
    assert!(!path.exists());
    assert_eq!(
        store.resources.disk.available_permits(),
        store.resources.capacity as usize
    );
}
#[tokio::test]
async fn unsupported_endpoint_and_failed_cleanup_fail_closed() {
    let fake = Fake::default();
    fake.0.lock().unwrap().ignore_condition = true;
    let error = kind(
        Store::connect(
            fake.clone(),
            "tests".into(),
            FormatLimits::default(),
            Resources::standard(),
            Budget::standard(),
            &CancellationToken::new(),
            format!("capability-probes/{}", uuid::Uuid::now_v7()),
        )
        .await,
    );
    assert_eq!(error.kind, ErrorKind::Unsupported);
    assert!(fake.0.lock().unwrap().objects.is_empty());
    fake.0.lock().unwrap().ignore_condition = false;
    fake.0.lock().unwrap().cleanup_error = true;
    let error = kind(
        Store::connect(
            fake.clone(),
            "tests".into(),
            FormatLimits::default(),
            Resources::standard(),
            Budget::standard(),
            &CancellationToken::new(),
            format!("capability-probes/{}", uuid::Uuid::now_v7()),
        )
        .await,
    );
    assert_eq!(error.operation, "cleanup probe");
    assert!(error.uncertain);
    assert!(error.key.starts_with("capability-probes/"));
    assert_eq!(fake.0.lock().unwrap().objects.len(), 1);
}
#[test]
fn finite_resource_and_request_limits() {
    assert!(Resources::new(0, 1, 1024).is_err());
    assert!(Resources::new(65, 1, 1024).is_err());
    assert!(Resources::new(1, 0, 1024).is_err());
    assert!(Resources::new(1, 1, 0).is_err());
    let mut b = Budget::standard();
    b.requests = 33;
    assert!(b.validate().is_err());
    b.requests = 1;
    b.no_progress = Duration::ZERO;
    assert!(b.validate().is_err());
}

struct Truncated {
    sent: bool,
}
impl AsyncRead for Truncated {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.sent {
            return Poll::Ready(Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)));
        }
        self.sent = true;
        out.put_slice(b"hel");
        Poll::Ready(Ok(()))
    }
}
#[tokio::test]
async fn truncated_transport_body_is_corruption() {
    let mut reader: Reader = Box::new(Truncated { sent: false });
    let e = kind(
        copy_verified(
            &mut reader,
            None,
            &reference(b"hello"),
            1024,
            Duration::from_secs(1),
        )
        .await,
    );
    assert_eq!(e.kind, ErrorKind::Corrupt);
}

#[tokio::test]
async fn capability_probe_never_deletes_an_existing_key() {
    let fake = Fake::default();
    let key = "capability-probes/already-owned".to_owned();
    fake.0
        .lock()
        .unwrap()
        .objects
        .insert(key.clone(), b"existing".to_vec());
    let e = kind(
        Store::connect(
            fake.clone(),
            "tests".into(),
            FormatLimits::default(),
            Resources::standard(),
            Budget::standard(),
            &CancellationToken::new(),
            key.clone(),
        )
        .await,
    );
    assert_eq!(e.kind, ErrorKind::Conflict);
    assert_eq!(fake.0.lock().unwrap().objects[&key], b"existing");
    assert_eq!(fake.0.lock().unwrap().calls.len(), 1);
}

#[tokio::test]
async fn exhausted_create_keeps_exact_uncertain_key_and_is_replayable() {
    let fake = Fake::default();
    let store = store(fake.clone()).await;
    let r = reference(b"hello");
    let cancel = CancellationToken::new();
    let mut budget = Budget::standard();
    budget.requests = 1;
    let e = kind(store.create(&r, input(b"hello"), budget, &cancel).await);
    assert_eq!(e.kind, ErrorKind::Exhausted);
    assert!(e.uncertain);
    assert_eq!(e.key, r.key);
    store
        .create(&r, input(b"hello"), Budget::standard(), &cancel)
        .await
        .unwrap();
}

struct Measured {
    remaining: usize,
    largest: Arc<std::sync::atomic::AtomicUsize>,
}
impl AsyncRead for Measured {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.largest
            .fetch_max(out.remaining(), std::sync::atomic::Ordering::SeqCst);
        let count = out.remaining().min(self.remaining);
        out.put_slice(&vec![b'x'; count]);
        self.remaining -= count;
        Poll::Ready(Ok(()))
    }
}
#[tokio::test]
async fn streaming_input_never_requests_more_than_the_fixed_buffer() {
    let fake = Fake::default();
    let store = store(fake).await;
    let bytes = vec![b'x'; 1024 * 1024 + 17];
    let r = reference(&bytes);
    let largest = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let input = Box::new(Measured {
        remaining: bytes.len(),
        largest: largest.clone(),
    });
    store
        .create(&r, input, Budget::standard(), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        largest.load(std::sync::atomic::Ordering::SeqCst),
        256 * 1024
    );
}

#[tokio::test]
async fn shared_disk_admission_times_out_without_another_remote_read() {
    let fake = Fake::default();
    let resources = Resources::new(2, 7, 1024).unwrap();
    let store = Store::connect(
        fake.clone(),
        "tests".into(),
        FormatLimits::default(),
        resources.clone(),
        Budget::standard(),
        &CancellationToken::new(),
        "capability-probes/quota".into(),
    )
    .await
    .unwrap();
    let r = reference(b"1234567");
    fake.0
        .lock()
        .unwrap()
        .objects
        .insert(r.key.clone(), b"1234567".to_vec());
    let cancel = CancellationToken::new();
    let file = store.read(&r, Budget::standard(), &cancel).await.unwrap();
    let calls = fake.0.lock().unwrap().calls.len();
    let mut short = Budget::standard();
    short.deadline = Instant::now() + Duration::from_millis(5);
    assert_eq!(
        kind(store.read(&r, short, &cancel).await).kind,
        ErrorKind::Timeout
    );
    assert_eq!(calls, fake.0.lock().unwrap().calls.len());
    drop(file);
    store.read(&r, Budget::standard(), &cancel).await.unwrap();
    assert_eq!(resources.disk.available_permits(), 7);
}

#[tokio::test]
async fn owned_deletion_is_exact_idempotent_and_budgeted() {
    let fake = Fake::default();
    let store = store(fake.clone()).await;
    let r = reference(b"hello");
    fake.0
        .lock()
        .unwrap()
        .objects
        .insert(r.key.clone(), b"hello".to_vec());
    fake.0
        .lock()
        .unwrap()
        .objects
        .insert("unrelated".into(), b"keep".to_vec());
    let cancel = CancellationToken::new();
    store
        .remove_owned(&r, Budget::standard(), &cancel)
        .await
        .unwrap();
    store
        .remove_owned(&r, Budget::standard(), &cancel)
        .await
        .unwrap();
    assert_eq!(fake.0.lock().unwrap().objects.len(), 1);
    assert!(fake.0.lock().unwrap().objects.contains_key("unrelated"));
    let mut bad = r;
    bad.storage_alias = "other".into();
    let calls = fake.0.lock().unwrap().calls.len();
    assert_eq!(
        kind(store.remove_owned(&bad, Budget::standard(), &cancel).await).kind,
        ErrorKind::Invalid
    );
    assert_eq!(calls, fake.0.lock().unwrap().calls.len());
}
