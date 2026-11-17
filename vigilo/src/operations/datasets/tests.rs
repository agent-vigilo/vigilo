//! Service-free contracts for sealed preparation, admission and local validation.
use super::*;
const SOURCE: &str = include_str!("../../cli/commands/run/fixtures/dataset-identity.json");
const EXPECTED: &str =
    include_str!("../../cli/commands/run/fixtures/dataset-identity.expected.json");
const OWNER: &str = "provider='s3'\nalias='cases'\nbucket='cases'\nprefix='inputs'\nregion='us-east-1'\nendpoint='https://objects.example'\n";
fn client() -> super::super::Client {
    super::super::Client::new(super::super::Config::new(
        "postgres://fixture@localhost/unused".into(),
    ))
    .unwrap()
}

#[tokio::test]
async fn reader_and_file_preparation_keep_exact_ordered_identity_and_unknown_fields() {
    let cancel = CancellationToken::new();
    let expected: serde_json::Value = serde_json::from_str(EXPECTED).unwrap();
    let prepared = PreparedDataset::prepare(
        SOURCE.as_bytes(),
        InputFormat::Json,
        Batching::default(),
        &cancel,
    )
    .await
    .unwrap();
    let identity = prepared.identity();
    assert_eq!(
        identity.dataset_hash,
        expected["dataset_hash"].as_str().unwrap()
    );
    assert_eq!(
        identity.dataset_version_id.to_string(),
        expected["dataset_version_id"].as_str().unwrap()
    );
    assert_eq!(identity.case_count, 3);
    drop(prepared);
    let file = tempfile::NamedTempFile::new().unwrap();
    tokio::fs::write(file.path(), SOURCE).await.unwrap();
    let prepared = PreparedDataset::file(
        file.path().to_owned(),
        input::InputFormat::Json,
        FormatLimits::default(),
        &cancel,
    )
    .await
    .unwrap();
    assert_eq!(prepared.identity(), identity);
    drop(prepared);
    let yaml = include_str!("../../cli/commands/run/fixtures/dataset-identity.yaml");
    let prepared = PreparedDataset::prepare(
        yaml.as_bytes(),
        InputFormat::Yaml,
        Batching::new(2, 1024 * 1024).unwrap(),
        &cancel,
    )
    .await
    .unwrap();
    assert_eq!(
        prepared.identity(),
        identity,
        "physical batching must not change logical identity"
    );
}

#[tokio::test]
async fn malformed_duplicate_and_oversized_sources_release_admission() {
    let cancel = CancellationToken::new();
    for source in [
        "{}",
        "{broken",
        r#"{"dataset_id":"00000000-0000-0000-0000-000000000001","cases":[]}"#,
    ] {
        assert!(
            PreparedDataset::prepare(
                source.as_bytes(),
                InputFormat::Json,
                Batching::default(),
                &cancel
            )
            .await
            .is_err()
        );
    }
    let mut duplicate: serde_json::Value = serde_json::from_str(SOURCE).unwrap();
    duplicate["cases"][1]["id"] = duplicate["cases"][0]["id"].clone();
    assert!(
        PreparedDataset::prepare(
            std::io::Cursor::new(serde_json::to_vec(&duplicate).unwrap()),
            InputFormat::Json,
            Batching::default(),
            &cancel
        )
        .await
        .is_err()
    );
    let result = PreparedDataset::prepare(
        tokio::io::repeat(b'x'),
        InputFormat::Json,
        Batching::default(),
        &cancel,
    )
    .await;
    assert!(matches!(result,Err(Error::Dependency(e)) if e.to_string().contains("size limit")));
    assert!(
        PreparedDataset::prepare(
            SOURCE.as_bytes(),
            InputFormat::Json,
            Batching::default(),
            &cancel
        )
        .await
        .is_ok()
    );
}

struct WaitingReader {
    entered: Arc<tokio::sync::Notify>,
    dropped: Arc<std::sync::atomic::AtomicBool>,
}
impl tokio::io::AsyncRead for WaitingReader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        _: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        self.entered.notify_one();
        std::task::Poll::Pending
    }
}
impl Drop for WaitingReader {
    fn drop(&mut self) {
        self.dropped
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}
#[tokio::test]
async fn cancellation_during_receive_releases_reader_and_process_admission() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    let reader = WaitingReader {
        entered: entered.clone(),
        dropped: dropped.clone(),
    };
    let mut task = tokio::spawn(async move {
        PreparedDataset::prepare(reader, InputFormat::Jsonl, Batching::default(), &token).await
    });
    if tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .is_err()
    {
        task.abort();
        let _ = task.await;
        panic!("reader was not admitted within fixture budget");
    }
    cancel.cancel();
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), &mut task).await;
    if result.is_err() {
        task.abort();
        let _ = task.await;
        panic!("cancelled preparation did not release admission");
    }
    assert!(matches!(result.unwrap().unwrap(), Err(Error::Cancelled)));
    assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
    assert!(
        PreparedDataset::prepare(
            SOURCE.as_bytes(),
            InputFormat::Json,
            Batching::default(),
            &CancellationToken::new()
        )
        .await
        .is_ok()
    );
}

#[tokio::test]
async fn invalid_limits_ids_and_precancelled_commands_do_no_service_io() {
    for (cases, bytes) in [(0, 1024), (65537, 1024), (1, 255), (1, 67108865)] {
        assert!(Batching::new(cases, bytes).is_err());
    }
    let root = client();
    let service = root.datasets(OWNER).unwrap();
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(matches!(
        service
            .resume(
                Uuid::from_u128(1),
                SOURCE.as_bytes(),
                InputFormat::Json,
                &cancel
            )
            .await,
        Err(Error::Cancelled)
    ));
    assert!(matches!(
        service.cleanup(Uuid::from_u128(1), 1, &cancel).await,
        Err(Error::Cancelled)
    ));
    assert!(matches!(
        service
            .cleanup(Uuid::from_u128(1), 0, &CancellationToken::new())
            .await,
        Err(Error::InvalidInput(_))
    ));
    assert!(matches!(
        root.dataset_status(Uuid::nil()).await,
        Err(Error::InvalidInput(_))
    ));
    assert!(matches!(
        root.abort_dataset_upload(Uuid::nil()).await,
        Err(Error::InvalidInput(_))
    ));
    assert!(matches!(
        service
            .resume(
                Uuid::nil(),
                SOURCE.as_bytes(),
                InputFormat::Json,
                &CancellationToken::new()
            )
            .await,
        Err(Error::InvalidInput(_))
    ));
    let source = PreparedDataset::prepare(
        SOURCE.as_bytes(),
        InputFormat::Json,
        Batching::default(),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(matches!(
        service
            .upload(source, Some(Uuid::nil()), &CancellationToken::new())
            .await,
        Err(Error::InvalidInput(_))
    ));
}

struct Denied;
impl Resolver for Denied {
    fn bindings(
        &self,
        _: super::super::resources::Request,
    ) -> super::super::resources::Resolution<'_, super::super::resources::Bindings> {
        Box::pin(async { Err(super::super::resources::Error::Denied) })
    }
}
#[tokio::test]
async fn trusted_storage_factory_rejects_ambient_bindings_without_opening_a_connection() {
    let client = UserClient::new(super::super::UserConfig {
        control: super::super::Config::new("postgres://unused@127.0.0.1:1/unused".into()),
        database_role: "fixture".into(),
        execution_databases: Default::default(),
    })
    .unwrap();
    assert!(client.datasets(OWNER, Arc::new(Denied)).is_ok());
    for config in [
        format!("{OWNER}read_endpoint_variable='UNUSED_ENDPOINT'\n"),
        format!(
            "{OWNER}[credential_variables]\naccess_key='UNUSED_ACCESS'\nsecret_key='UNUSED_SECRET'\n"
        ),
        "x".repeat(64 * 1024 + 1),
        OWNER.replace("https://objects.example", "ftp://objects.example"),
    ] {
        assert!(matches!(
            client.datasets(&config, Arc::new(Denied)),
            Err(Error::InvalidConfiguration(_))
        ));
    }
}

#[tokio::test]
async fn public_dataset_futures_leave_stack_headroom_for_storage_polling() {
    let root = client();
    let dataset = root.datasets(OWNER).unwrap();
    let cancel = CancellationToken::new();
    let prepared = PreparedDataset::prepare(
        SOURCE.as_bytes(),
        InputFormat::Json,
        Batching::default(),
        &cancel,
    )
    .await
    .unwrap();
    let upload = dataset.upload(prepared, Some(Uuid::now_v7()), &cancel);
    let resume = dataset.resume(
        Uuid::now_v7(),
        SOURCE.as_bytes(),
        InputFormat::Json,
        &cancel,
    );
    let cleanup = dataset.cleanup(Uuid::now_v7(), 1, &cancel);
    for size in [
        std::mem::size_of_val(&upload),
        std::mem::size_of_val(&resume),
        std::mem::size_of_val(&cleanup),
    ] {
        assert!(
            size <= 64 * 1024,
            "public dataset future grew to {size} bytes"
        );
    }
}
