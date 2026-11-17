//! Signed storage through the public caller-bound dataset API and restricted SQL.
use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    sync::{
        Arc,
        Mutex,
        atomic::{
            AtomicBool,
            AtomicUsize,
            Ordering,
        },
    },
};

use tokio_util::sync::CancellationToken;
use vigilo::operations::{
    self,
    datasets::{
        Batching,
        InputFormat,
        PreparedDataset,
    },
    resources,
};

use super::*;

struct Host {
    owner: String,
    caller: operations::UserContext,
    allowed: BTreeSet<Uuid>,
    deny: AtomicBool,
    calls: AtomicUsize,
    purposes: Mutex<Vec<resources::DatasetStoragePurpose>>,
    access: String,
    secret: String,
}
impl resources::Resolver for Host {
    fn bindings(&self, _: resources::Request) -> resources::Resolution<'_, resources::Bindings> {
        Box::pin(async { Err(resources::Error::Denied) })
    }
    fn dataset_storage_credentials(
        &self,
        request: resources::DatasetRequest,
    ) -> resources::Resolution<'_, resources::StorageCredentials<resources::DatasetRequest>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.deny.load(Ordering::SeqCst)
                || request.caller() != self.caller
                || !self.allowed.contains(&request.upload_id())
            {
                return Err(resources::Error::Denied);
            }
            request.validate_storage_binding(&self.owner)?;
            self.purposes.lock().unwrap().push(request.purpose());
            Ok(resources::StorageCredentials {
                request,
                authentication: resources::StorageAuthentication::S3 {
                    access_key: self.access.clone(),
                    secret_key: self.secret.clone(),
                    session_token: None,
                },
                expires_at: std::time::SystemTime::now() + Duration::from_secs(60),
            })
        })
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn hosted_upload_resume_reuse_and_cleanup_use_verified_dataset_credentials(
    db: sqlx::PgPool,
) -> Result<()> {
    use futures_util::FutureExt;
    let role = format!("dataset_host_{}", Uuid::now_v7().simple());
    let caller = operations::UserContext::new(Uuid::now_v7(), Uuid::now_v7()).unwrap();
    let other = operations::UserContext::new(Uuid::now_v7(), Uuid::now_v7()).unwrap();
    sqlx::raw_sql(&format!(
        "CREATE ROLE {role} LOGIN PASSWORD 'fixture'; GRANT USAGE ON SCHEMA public TO {role}"
    ))
    .execute(&db)
    .await?;
    for table in [
        "dataset_uploads",
        "dataset_upload_objects",
        "dataset_catalog",
    ] {
        sqlx::raw_sql(&format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY; GRANT SELECT,INSERT,UPDATE ON {table} TO {role}; CREATE POLICY caller ON {table} TO {role} USING (tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND current_setting('vigilo.actor_id',true)='{}')",caller.actor_id())).execute(&db).await?;
    }
    use sqlx::ConnectOptions;
    let mut url = db
        .connect_options()
        .as_ref()
        .clone()
        .username(&role)
        .password("fixture")
        .to_url_lossy();
    url.set_query(None);
    let mut config = operations::Config::new(url.to_string());
    config.max_connections = 1;
    config.acquire_timeout = Duration::from_secs(2);
    let client = operations::UserClient::new(operations::UserConfig {
        control: config,
        database_role: role.clone(),
        execution_databases: BTreeMap::new(),
    })?;
    let upload = Uuid::now_v7();
    let abandoned = Uuid::now_v7();
    let endpoint = env("VIGILO_BLOB_TEST_ENDPOINT")?;
    ensure!(
        endpoint.starts_with("http://127.0.0.1:"),
        "requires disposable local Ceph"
    );
    let owner = format!(
        "provider='s3'\nalias='cases'\nbucket='{}'\nprefix='hosted-dataset/{}'\nregion='us-east-1'\nendpoint='{endpoint}'\nallow_http=true\npath_style=true\n",
        env("VIGILO_BLOB_TEST_BUCKET")?,
        Uuid::now_v7()
    );
    let host = Arc::new(Host {
        owner: owner.clone(),
        caller,
        allowed: [upload, abandoned].into(),
        deny: AtomicBool::new(true),
        calls: AtomicUsize::new(0),
        purposes: Mutex::new(Vec::new()),
        access: env("VIGILO_BLOB_TEST_ACCESS_KEY_ID")?,
        secret: env("VIGILO_BLOB_TEST_SECRET_ACCESS_KEY")?,
    });
    let service = client.datasets(&owner, host.clone())?;
    let datasets = service.user(caller);
    let source = include_bytes!("../../src/cli/commands/run/fixtures/dataset-identity.json");
    let cancel = CancellationToken::new();
    let outcome = std::panic::AssertUnwindSafe(async {
        let prepared = PreparedDataset::prepare(
            source.as_slice(),
            InputFormat::Json,
            Batching::new(1, 1024 * 1024)?,
            &cancel,
        )
        .await?;
        ensure!(
            datasets
                .upload(prepared, Some(upload), &cancel)
                .await
                .is_err(),
            "denied credentials were bypassed"
        );
        let status = client.dataset_status(caller, upload).await?;
        ensure!(
            status.persisted_case_count == 0 && status.reference.is_none(),
            "failed upload published progress"
        );
        host.deny.store(false, Ordering::SeqCst);
        let published = datasets
            .resume(upload, source.as_slice(), InputFormat::Json, &cancel)
            .await?;
        ensure!(
            !published.reused && published.reference.upload_id == upload,
            "resume identity changed"
        );
        let calls = host.calls.load(Ordering::SeqCst);
        let prepared = PreparedDataset::prepare(
            source.as_slice(),
            InputFormat::Json,
            Batching::new(1, 1024 * 1024)?,
            &cancel,
        )
        .await?;
        let reused = datasets.upload(prepared, None, &cancel).await?;
        ensure!(
            reused.reused && reused.reference == published.reference,
            "finalized reuse changed identity"
        );
        ensure!(
            host.calls.load(Ordering::SeqCst) == calls,
            "reuse requested new storage credentials"
        );
        let mut changed: Value = serde_json::from_slice(source)?;
        changed["additional_source_field"] = json!(true);
        let changed = serde_json::to_vec(&changed)?;
        ensure!(
            datasets
                .resume(upload, changed.as_slice(), InputFormat::Json, &cancel)
                .await
                .is_err(),
            "changed source reused publication"
        );
        let moved = client
            .datasets(
                &owner.replace("prefix='hosted-dataset/", "prefix='moved/"),
                host.clone(),
            )?
            .user(caller);
        ensure!(
            moved
                .resume(upload, source.as_slice(), InputFormat::Json, &cancel)
                .await
                .is_err(),
            "changed namespace redirected resume"
        );
        ensure!(
            host.calls.load(Ordering::SeqCst) == calls,
            "rejected replay reached credentials"
        );
        for wrong in [
            other,
            operations::UserContext::new(caller.tenant_id(), other.actor_id()).unwrap(),
        ] {
            ensure!(
                client.dataset_status(wrong, upload).await.is_err(),
                "foreign status escaped policy"
            );
            ensure!(
                client.abort_dataset_upload(wrong, upload).await.is_err(),
                "foreign abort escaped policy"
            );
            ensure!(
                service
                    .user(wrong)
                    .resume(upload, source.as_slice(), InputFormat::Json, &cancel)
                    .await
                    .is_err(),
                "foreign resume escaped policy"
            );
        }
        ensure!(
            host.calls.load(Ordering::SeqCst) == calls,
            "foreign work reached credential provider"
        );
        let status = serde_json::to_value(client.dataset_status(caller, upload).await?)?;
        for field in ["lease_owner", "binding_hash", "manifest", "object_key"] {
            ensure!(status.get(field).is_none(), "status leaked {field}");
        }
        ensure!(
            client.abort_dataset_upload(caller, upload).await.is_err(),
            "finalized upload aborted"
        );
        host.deny.store(true, Ordering::SeqCst);
        let prepared = PreparedDataset::prepare(
            source.as_slice(),
            InputFormat::Json,
            Batching::default(),
            &cancel,
        )
        .await?;
        ensure!(
            datasets
                .upload(prepared, Some(abandoned), &cancel)
                .await
                .is_err(),
            "expected interrupted staging"
        );
        client.abort_dataset_upload(caller, abandoned).await?;
        ensure!(
            datasets.cleanup(abandoned, 1, &cancel).await.is_err(),
            "quarantine was bypassed"
        );
        sqlx::query(
            "UPDATE dataset_uploads SET cleanup_after=now()-interval '1 second' WHERE id=$1",
        )
        .bind(abandoned)
        .execute(&db)
        .await?;
        host.deny.store(false, Ordering::SeqCst);
        ensure!(
            datasets
                .cleanup(abandoned, 1, &cancel)
                .await?
                .cleanup_complete,
            "exact cleanup incomplete"
        );
        ensure!(
            host.purposes
                .lock()
                .unwrap()
                .contains(&resources::DatasetStoragePurpose::Cleanup),
            "cleanup did not bind its purpose"
        );
        ensure!(
            client.dataset_status(caller, upload).await?.reference == Some(published.reference),
            "cleanup damaged finalized data"
        );
        Ok::<_, anyhow::Error>(())
    })
    .catch_unwind()
    .await;
    // Exact owned keys only; the fixture owns this fresh database and prefix.
    let keys: Vec<String> =
        sqlx::query_scalar("SELECT object_key FROM dataset_upload_objects WHERE upload_id=ANY($1)")
            .bind(vec![upload, abandoned])
            .fetch_all(&db)
            .await?;
    let s3 = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::new()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .endpoint_url(endpoint)
            .force_path_style(true)
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                env("VIGILO_BLOB_TEST_ACCESS_KEY_ID")?,
                env("VIGILO_BLOB_TEST_SECRET_ACCESS_KEY")?,
                None,
                None,
                "fixture",
            ))
            .timeout_config(
                aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                    .operation_timeout(Duration::from_secs(10))
                    .build(),
            )
            .build(),
    );
    for key in keys {
        s3.delete_object()
            .bucket(env("VIGILO_BLOB_TEST_BUCKET")?)
            .key(key)
            .send()
            .await?;
    }
    drop(datasets);
    drop(service);
    drop(client);
    sqlx::raw_sql(&format!("DROP OWNED BY {role}; DROP ROLE {role}"))
        .execute(&db)
        .await?;
    match outcome {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
