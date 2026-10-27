//! Opt-in signed S3 API acceptance against an explicitly selected disposable
//! namespace. The endpoint may be Ceph RGW or AWS S3. No missing-variable skips:
//! callers provide credentials, bucket and endpoint selection. Cleanup deletes
//! only this invocation's exact keys, including after an assertion error.
#![allow(dead_code)] // Reuse the binary's contracts without publishing a library API.
#[path = "../src/blob_storage.rs"]
mod blob_storage;
#[path = "support/storage_contracts.rs"]
mod contracts;

use aws_sdk_s3::{
    Client,
    config::{
        BehaviorVersion,
        Credentials,
        Region,
        retry::RetryConfig,
    },
    primitives::ByteStream,
};
use blob_storage::{
    BlobStorage,
    Budget,
    ErrorKind,
    Resources,
    s3::{
        S3Config,
        S3Storage,
    },
};
use contracts::case_storage::{
    FormatLimits,
    ObjectRef,
    StorageFormat,
};
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;

static SERVICE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn variable(name: &str) -> anyhow::Result<String> {
    std::env::var(name)
        .map_err(|_| anyhow::anyhow!("required service-test variable {name} is missing"))
}
fn config(prefix: &str) -> anyhow::Result<S3Config> {
    let endpoint = variable("VIGILO_BLOB_TEST_ENDPOINT")?;
    Ok(S3Config {
        alias: "acceptance".into(),
        probe_key: format!("capability-probes/{}", uuid::Uuid::now_v7()),
        bucket: variable("VIGILO_BLOB_TEST_BUCKET")?,
        prefix: prefix.into(),
        region: std::env::var("VIGILO_BLOB_TEST_REGION").unwrap_or_else(|_| "us-east-1".into()),
        endpoint: if endpoint == "aws" {
            None
        } else {
            Some(endpoint)
        },
        allow_http: true,
        path_style: true,
        credential_variables: Some((
            "VIGILO_BLOB_TEST_ACCESS_KEY_ID".into(),
            "VIGILO_BLOB_TEST_SECRET_ACCESS_KEY".into(),
            std::env::var("VIGILO_BLOB_TEST_SESSION_TOKEN")
                .ok()
                .map(|_| "VIGILO_BLOB_TEST_SESSION_TOKEN".into()),
        )),
    })
}
async fn raw(config: &S3Config) -> anyhow::Result<Client> {
    let credentials = Credentials::new(
        variable("VIGILO_BLOB_TEST_ACCESS_KEY_ID")?,
        variable("VIGILO_BLOB_TEST_SECRET_ACCESS_KEY")?,
        std::env::var("VIGILO_BLOB_TEST_SESSION_TOKEN").ok(),
        None,
        "acceptance",
    );
    let mut b = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(config.region.clone()))
        .credentials_provider(credentials)
        .retry_config(RetryConfig::disabled())
        .timeout_config(
            aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                .connect_timeout(std::time::Duration::from_secs(2))
                .read_timeout(std::time::Duration::from_secs(5))
                .operation_timeout(std::time::Duration::from_secs(10))
                .build(),
        )
        .force_path_style(true);
    if let Some(endpoint) = &config.endpoint {
        b = b.endpoint_url(endpoint);
    }
    Ok(Client::from_conf(b.build()))
}
fn input(bytes: &[u8]) -> blob_storage::Reader {
    Box::new(std::io::Cursor::new(bytes.to_vec()))
}
fn expect_kind<T>(
    result: std::result::Result<T, blob_storage::StorageError>,
    expected: ErrorKind,
) -> anyhow::Result<()> {
    match result {
        Err(e) if e.kind == expected => Ok(()),
        Err(e) => Err(e.into()),
        Ok(_) => anyhow::bail!("expected {expected:?}"),
    }
}

#[tokio::test]
async fn signed_storage_contract() -> anyhow::Result<()> {
    // Restart acceptance cannot overlap another test using this endpoint.
    let _service = SERVICE_LOCK.lock().await;
    let prefix = format!("adapter-acceptance/{}", uuid::Uuid::now_v7());
    let cfg = config(&prefix)?;
    let client = raw(&cfg).await?;
    let cancel = CancellationToken::new();
    let resources = Resources::standard();
    let limits = FormatLimits::default();
    let connection = config(&prefix)?;
    let probe = format!("{prefix}/{}", connection.probe_key);
    let reconnect = config(&prefix)?;
    let reconnect_probe = format!("{prefix}/{}", reconnect.probe_key);
    let mut denied = config(&prefix)?;
    let denied_probe = format!("{prefix}/{}", denied.probe_key);
    variable("VIGILO_BLOB_TEST_INVALID_SECRET_ACCESS_KEY")?;
    denied.credential_variables = Some((
        "VIGILO_BLOB_TEST_ACCESS_KEY_ID".into(),
        "VIGILO_BLOB_TEST_INVALID_SECRET_ACCESS_KEY".into(),
        None,
    ));
    let data = vec![b'x'; 1024 * 1024 + 17]; // Cross multiple fixed-size stream buffers.
    let r = ObjectRef::for_bytes(
        "acceptance".into(),
        "cases/data".into(),
        StorageFormat::CaseBatch,
        &data,
        &limits,
    )?;
    let key = format!("{prefix}/{}", r.key);
    let result: anyhow::Result<()> = async {
        let store = S3Storage::open(
            connection,
            limits.clone(),
            resources.clone(),
            Budget::standard(),
            &cancel,
        )
        .await?;
        expect_kind(
            S3Storage::open(
                denied,
                limits.clone(),
                resources.clone(),
                Budget::standard(),
                &cancel,
            )
            .await,
            ErrorKind::Unauthorized,
        )?;
        store
            .create(&r, input(&data), Budget::standard(), &cancel)
            .await?;
        store
            .create(&r, input(&data), Budget::standard(), &cancel)
            .await?;
        let mut changed = r.clone();
        changed.digest = blake3::hash(&vec![b'y'; data.len()]).to_hex().to_string();
        expect_kind(
            store
                .create(
                    &changed,
                    input(&vec![b'y'; data.len()]),
                    Budget::standard(),
                    &cancel,
                )
                .await,
            ErrorKind::Conflict,
        )?;
        let mut missing = r.clone();
        missing.key = "cases/missing".into();
        expect_kind(
            store.read(&missing, Budget::standard(), &cancel).await,
            ErrorKind::Missing,
        )?;
        // Both invalid signing and valid signing with the configured credentials
        // pass through the provider's real authentication path.
        let bad = Client::from_conf(
            client
                .config()
                .to_builder()
                .credentials_provider(Credentials::new(
                    "invalid-key",
                    "invalid-secret",
                    None,
                    None,
                    "negative-acceptance",
                ))
                .build(),
        );
        let error = bad
            .get_object()
            .bucket(&cfg.bucket)
            .key(&key)
            .send()
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("invalid credentials accepted"))?;
        anyhow::ensure!(
            error
                .raw_response()
                .is_some_and(|r| r.status().as_u16() == 403),
            "unexpected unauthorized response"
        );
        drop(store);
        if let Ok(container) = std::env::var("VIGILO_BLOB_TEST_RESTART_CONTAINER") {
            let status = tokio::time::timeout(
                std::time::Duration::from_secs(60),
                tokio::process::Command::new("docker")
                    .args(["restart", "--timeout", "45", &container])
                    .kill_on_drop(true)
                    .status(),
            )
            .await??;
            anyhow::ensure!(status.success(), "selected Ceph restart failed");
            // Bounded readiness loop; each probe has its own finite deadline.
            let mut ready = false;
            for _ in 0..60 {
                if tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    client.head_object().bucket(&cfg.bucket).key(&key).send(),
                )
                .await
                .is_ok_and(|r| r.is_ok())
                {
                    ready = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            anyhow::ensure!(ready, "object not readable after service restart");
        }
        let reopened =
            S3Storage::open(reconnect, limits, resources, Budget::standard(), &cancel).await?;
        let file = reopened.read(&r, Budget::standard(), &cancel).await?;
        let mut actual = Vec::new();
        file.reader().await?.read_to_end(&mut actual).await?;
        anyhow::ensure!(actual == data, "restart changed bytes");
        let mut wrong = r.clone();
        wrong.digest = "0".repeat(64);
        expect_kind(
            reopened.read(&wrong, Budget::standard(), &cancel).await,
            ErrorKind::Corrupt,
        )?;
        // Simulate external corruption using only this fixture's exact owned key.
        client
            .put_object()
            .bucket(&cfg.bucket)
            .key(&key)
            .body(ByteStream::from_static(b"truncated"))
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("fixture corruption upload failed"))?;
        expect_kind(
            reopened.read(&r, Budget::standard(), &cancel).await,
            ErrorKind::Corrupt,
        )?;
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        expect_kind(
            reopened.read(&r, Budget::standard(), &cancelled).await,
            ErrorKind::Cancelled,
        )?;
        Ok(())
    }
    .await;
    for owned in [&key, &probe, &reconnect_probe, &denied_probe] {
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            client.delete_object().bucket(&cfg.bucket).key(owned).send(),
        )
        .await?
        .map_err(|_| {
            anyhow::anyhow!("exact fixture cleanup failed: {owned}; acceptance: {result:?}")
        })?;
    }
    result
}

#[path = "support/storage_proxy.rs"]
mod storage_proxy;

/// Faults are injected on signed requests forwarded to the selected local Ceph
/// service, rather than substituting an application-level fake adapter.
#[tokio::test]
async fn ceph_fault_contract() -> anyhow::Result<()> {
    let _service = SERVICE_LOCK.lock().await;
    use std::time::Duration;
    let upstream = variable("VIGILO_BLOB_TEST_ENDPOINT")?;
    anyhow::ensure!(
        upstream.starts_with("http://127.0.0.1:"),
        "fault acceptance requires the disposable local Ceph endpoint"
    );
    let proxy = storage_proxy::Proxy::start(upstream).await?;
    let prefix = format!("adapter-faults/{}", uuid::Uuid::now_v7());
    let cfg = config(&prefix)?;
    let client = raw(&cfg).await?;
    let mut connection = config(&prefix)?;
    connection.endpoint = Some(proxy.endpoint.clone());
    let probe = format!("{prefix}/{}", connection.probe_key);
    let cancel = CancellationToken::new();
    let limits = FormatLimits::default();
    let r = ObjectRef::for_bytes(
        "acceptance".into(),
        "cases/faults".into(),
        StorageFormat::CaseBatch,
        &vec![b'x'; 1024 * 1024],
        &limits,
    )?;
    let key = format!("{prefix}/{}", r.key);
    let interrupted = format!("{prefix}/cases/interrupted");
    let result: anyhow::Result<()> = async {
        let store = S3Storage::open(
            connection,
            limits,
            Resources::standard(),
            Budget::standard(),
            &cancel,
        )
        .await?;
        proxy.set(2);
        store
            .create(
                &r,
                input(&vec![b'x'; 1024 * 1024]),
                Budget::standard(),
                &cancel,
            )
            .await?;
        anyhow::ensure!(
            proxy.count() == 2,
            "lost PUT response was not reconciled by one GET"
        );
        proxy.set(1);
        expect_kind(
            store.read(&r, Budget::standard(), &cancel).await,
            ErrorKind::Exhausted,
        )?;
        anyhow::ensure!(
            proxy.count() == 3,
            "SDK retries multiplied the three-request budget"
        );
        proxy.set(3);
        let mut budget = Budget::standard();
        budget.no_progress = Duration::from_millis(50);
        expect_kind(store.read(&r, budget, &cancel).await, ErrorKind::Timeout)?;
        let _ = tokio::time::timeout(Duration::from_millis(1), proxy.entered.notified()).await;
        proxy.set(3);
        let token = CancellationToken::new();
        let stop = token.clone();
        let (result, ()) = tokio::join!(store.read(&r, Budget::standard(), &token), async {
            let _ = tokio::time::timeout(Duration::from_secs(5), proxy.entered.notified()).await;
            stop.cancel();
        });
        expect_kind(result, ErrorKind::Cancelled)?;
        anyhow::ensure!(
            proxy.count() == 1,
            "read cancellation did not reach transport"
        );
        let _ = tokio::time::timeout(Duration::from_millis(1), proxy.entered.notified()).await;
        proxy.set(4);
        let mut other = r.clone();
        other.key = "cases/interrupted".into();
        let token = CancellationToken::new();
        let stop = token.clone();
        let (result, ()) = tokio::join!(
            store.create(
                &other,
                input(&vec![b'x'; 1024 * 1024]),
                Budget::standard(),
                &token
            ),
            async {
                let _ =
                    tokio::time::timeout(Duration::from_secs(5), proxy.entered.notified()).await;
                stop.cancel();
            }
        );
        match result {
            Err(e) => {
                anyhow::ensure!(
                    e.kind == ErrorKind::Cancelled && e.uncertain && e.key == other.key,
                    "cancelled PUT lost exact uncertainty context"
                );
            }
            Ok(_) => anyhow::bail!("cancelled PUT succeeded"),
        }
        let absent = client
            .head_object()
            .bucket(&cfg.bucket)
            .key(&interrupted)
            .send()
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("incomplete upload became visible"))?;
        anyhow::ensure!(
            absent
                .raw_response()
                .is_some_and(|r| r.status().as_u16() == 404),
            "unexpected incomplete-upload status"
        );
        proxy.set(0);
        store.read(&r, Budget::standard(), &cancel).await?;
        Ok(())
    }
    .await;
    // Keep the proxy alive until in-flight partial uploads close, then remove
    // only the exact fixture keys; no bucket or prefix deletion is allowed.
    drop(proxy);
    for owned in [&key, &interrupted, &probe] {
        tokio::time::timeout(
            Duration::from_secs(30),
            client.delete_object().bucket(&cfg.bucket).key(owned).send(),
        )
        .await?
        .map_err(|_| {
            anyhow::anyhow!("exact fixture cleanup failed: {owned}; acceptance: {result:?}")
        })?;
    }
    result
}
