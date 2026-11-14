//! Public-library hosted acceptance on actual restricted PostgreSQL roles,
//! RabbitMQ, object storage and a real evaluator. Fixtures own exact databases,
//! roles, broker namespace, object keys and tasks; no production policy is installed.
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::Ordering,
    },
};

use anyhow::Context;
use futures_util::FutureExt;
use tokio_util::sync::CancellationToken;
use vigilo::operations::{
    BrokerConfig,
    HostedConfig,
    HostedRuntime,
    HostedUser,
    PrepareRun,
    UserContext,
};

use super::*;

#[path = "hosted/agent.rs"]
mod agent;
#[path = "hosted/broker.rs"]
mod broker;
#[path = "hosted/database.rs"]
mod database;
#[path = "hosted/failures.rs"]
mod failures;
#[path = "hosted/measurement.rs"]
mod measurement;
use super::postgres_probe;
#[path = "hosted/resources.rs"]
mod resources;

struct Fixture {
    db: database::Databases,
    root: tempfile::TempDir,
    agent: agent::Agent,
    host: Arc<resources::Host>,
    runtime: Option<HostedRuntime>,
    stop: CancellationToken,
    tasks: Vec<tokio::task::JoinHandle<Result<(), vigilo::operations::Error>>>,
    owners: BTreeMap<Uuid, toml::Value>,
    namespace: String,
    object_owners: BTreeMap<Uuid, Uuid>,
    standalone: bool,
    children: Vec<tokio::process::Child>,
    probes: Vec<postgres_probe::Probe>,
    coordinator_count: usize,
    worker_handles: Vec<vigilo::operations::Worker>,
    coordinator_handles: Vec<vigilo::operations::Coordinator>,
}
impl Fixture {
    async fn new() -> anyhow::Result<Self> {
        Ok(Self {
            db: database::Databases::new().await?,
            root: tempfile::tempdir()?,
            agent: agent::Agent::new().await?,
            host: Arc::new(Default::default()),
            runtime: None,
            stop: CancellationToken::new(),
            tasks: vec![],
            owners: BTreeMap::new(),
            namespace: format!("hosted-{}", Uuid::now_v7()),
            object_owners: BTreeMap::new(),
            standalone: false,
            children: vec![],
            probes: vec![],
            coordinator_count: 0,
            worker_handles: vec![],
            coordinator_handles: vec![],
        })
    }
    async fn setup(&mut self) -> anyhow::Result<()> {
        self.db.setup(!self.standalone).await?;
        let original: toml::Value = toml::from_str(
            &tokio::fs::read_to_string(std::env::var("VIGILO_STORAGE_CONFIG")?).await?,
        )?;
        let mut host = resources::Host {
            origin: self
                .agent
                .url
                .rsplit_once('/')
                .context("agent origin")?
                .0
                .into(),
            ..Default::default()
        };
        for _ in 0..2 {
            let tenant = Uuid::now_v7();
            let mut owner = original.clone();
            owner["prefix"] = toml::Value::String(format!("hosted/{}/{tenant}", self.namespace));
            self.owners.insert(tenant, owner.clone());
            owner
                .as_table_mut()
                .context("owner table")?
                .remove("credential_variables");
            host.owners.insert(tenant, toml::to_string(&owner)?);
        }
        self.host = Arc::new(host);
        if !self.standalone {
            self.construct_runtime().await?;
        }
        Ok(())
    }
    async fn construct_runtime(&mut self) -> anyhow::Result<()> {
        let mut broker = BrokerConfig::new(std::env::var("MESSAGING_URL")?);
        broker.namespace = Some(self.namespace.clone());
        let mut config = HostedConfig::new(
            self.db.pools()?,
            self.host.clone(),
            broker,
            SHARD_ALIAS.into(),
            "single-default".into(),
        );
        config.agent_credentials_allow_http_loopback = true;
        config.coordinator.tick_seconds = 1;
        config.worker_max_inflight_chunks = 2;
        self.runtime = Some(HostedRuntime::new(config).await?);
        Ok(())
    }
    fn user(&self, tenant: Uuid) -> HostedUser {
        self.runtime
            .as_ref()
            .unwrap()
            .user(UserContext::new(tenant, tenant).unwrap())
    }
    async fn upload(&mut self, tenant: Uuid, count: usize) -> anyhow::Result<Uuid> {
        let owner = self.root.path().join(format!("{tenant}.toml"));
        let source = self.root.path().join(format!("{tenant}.jsonl"));
        tokio::fs::write(&owner, toml::to_string(&self.owners[&tenant])?).await?;
        let upload_id = Uuid::now_v7();
        self.object_owners.insert(upload_id, tenant);
        let dataset = Uuid::now_v7();
        let mut content = format!(
            "{}\n",
            json!({"schema":"vigilo/dataset-upload/v1","fields":{"dataset_id":dataset,"dataset_version":"1.0.0"}})
        );
        for i in 0..count {
            content += &format!(
                "{}\n",
                json!({"id":Uuid::from_u128(i as u128+1),"task_type":"classification","input":{"owner":tenant.to_string(),"user_message":"good safe"},"expected":{"label":"positive"}})
            );
        }
        tokio::fs::write(&source, content).await?;
        let mut url = url::Url::parse(&self.db.urls[0])?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-c vigilo.tenant_id={tenant}"));
        let output = tokio::time::timeout(
            Duration::from_secs(90),
            tokio::process::Command::new(env!("CARGO_BIN_EXE_vigilo"))
                .kill_on_drop(true)
                .env("DATABASE_URL", url.as_str())
                .args([
                    "-q",
                    "dataset",
                    "upload",
                    "--upload-id",
                    &upload_id.to_string(),
                    "--input-file",
                    source.to_str().context("source path")?,
                    "--input-format",
                    "jsonl",
                    "--storage-config",
                    owner.to_str().context("owner path")?,
                    "--batch-cases",
                    "100",
                ])
                .output(),
        )
        .await??;
        anyhow::ensure!(
            output.status.success(),
            "upload: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let result: Value = serde_json::from_slice(&output.stdout)?;
        let upload = Uuid::parse_str(result["data"]["id"].as_str().context("upload ID")?)?;
        if self.standalone {
            return Ok(upload);
        }
        // Input ingestion is a bootstrap fixture, not a hosted ingestion API.
        // Standalone deliberately owns nil context. Import its exact immutable
        // catalog/file references into the fixture's owner before run submission.
        let mut tx = self.db.admin[0].begin().await?;
        let catalog: Value = sqlx::query_scalar(
            "DELETE FROM dataset_catalog WHERE id=$1 RETURNING to_jsonb(dataset_catalog)",
        )
        .bind(upload)
        .fetch_one(&mut *tx)
        .await?;
        let objects: Vec<Value> = sqlx::query_scalar(
            "SELECT to_jsonb(o) FROM dataset_upload_objects o WHERE upload_id=$1 LIMIT 129",
        )
        .bind(upload)
        .fetch_all(&mut *tx)
        .await?;
        anyhow::ensure!(objects.len() <= 128, "fixture upload ledger exceeded bound");
        sqlx::query("DELETE FROM dataset_upload_objects WHERE upload_id=$1")
            .bind(upload)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE dataset_uploads SET tenant_id=$2 WHERE id=$1")
            .bind(upload)
            .bind(tenant)
            .execute(&mut *tx)
            .await?;
        for (table, mut row) in [
            ("dataset_catalog", vec![catalog]),
            ("dataset_upload_objects", objects),
        ] {
            for object in &mut row {
                object["tenant_id"] = json!(tenant);
                sqlx::query(&format!(
                    "INSERT INTO {table} SELECT (jsonb_populate_record(NULL::{table},$1)).*"
                ))
                .bind(&*object)
                .execute(&mut *tx)
                .await?;
            }
        }
        tx.commit().await?;
        Ok(upload)
    }
    fn input(&self, run: Uuid, upload: Uuid) -> anyhow::Result<PrepareRun> {
        let mut profile: Value = serde_yaml::from_str(&profile_yaml(
            "hosted",
            &self.agent.url,
            SENTIMENT_EVALUATOR_REF,
        ))?;
        profile["agent"]["http"]["credential"] = json!("agent-api");
        Ok(PrepareRun {
            run_id: run,
            dataset_upload_id: upload,
            profile,
        })
    }
    async fn await_terminal(
        &self,
        owner: Uuid,
        run: Uuid,
    ) -> anyhow::Result<vigilo::operations::RunStatus> {
        tokio::time::timeout(Duration::from_secs(90), async {
            loop {
                let status = self.user(owner).run_status(run).await?;
                if status.terminal {
                    return anyhow::Ok(status);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .context("hosted completion deadline")?
    }
    async fn exercise(&mut self) -> anyhow::Result<()> {
        self.setup().await?;
        let owners: Vec<_> = self.owners.keys().copied().collect();
        let mut runs = Vec::new();
        let mut uploads = BTreeMap::new();
        for &owner in &owners {
            let upload = self.upload(owner, 101).await?;
            uploads.insert(owner, upload);
            let run = Uuid::now_v7();
            self.object_owners.insert(run, owner);
            let user = self.user(owner);
            let wrong = self
                .runtime
                .as_ref()
                .unwrap()
                .user(UserContext::new(owner, Uuid::now_v7()).unwrap());
            let before = self.host.calls.lock().unwrap().len();
            assert!(
                wrong
                    .runs()
                    .prepare_run(self.input(run, upload)?, &self.stop)
                    .await
                    .is_err()
            );
            assert_eq!(
                self.host.calls.lock().unwrap().len(),
                before,
                "unauthorized input reached resource resolver"
            );
            self.host.deny.store(true, Ordering::SeqCst);
            assert!(
                user.runs()
                    .prepare_run(self.input(run, upload)?, &self.stop)
                    .await
                    .is_err()
            );
            self.host.deny.store(false, Ordering::SeqCst);
            let prepared = user
                .runs()
                .prepare_run(self.input(run, upload)?, &self.stop)
                .await?;
            self.runtime.as_ref().unwrap().coordinator().once().await?;
            let chunks: i64 = sqlx::query_scalar("SELECT count(*) FROM run_chunks WHERE run_id=$1")
                .bind(run)
                .fetch_one(&self.db.admin[1])
                .await?;
            assert_eq!(chunks, 0, "preparation must not authorize work");
            user.runs().accept_run(prepared, &self.stop).await?;
            runs.push((owner, run));
        }
        for &(owner, run) in &runs {
            let other = *owners.iter().find(|&&id| id != owner).unwrap();
            assert!(matches!(
                self.user(other).run_status(run).await,
                Err(vigilo::operations::Error::NotFound(_))
            ));
            assert!(
                self.user(other)
                    .control_run(run, vigilo::operations::ExecutionControl::Revoked)
                    .await
                    .is_err()
            );
        }
        self.forged_messages().await?;
        self.coordinator_count = 2;
        for _ in 0..2 {
            let coordinator = self.runtime.as_ref().unwrap().coordinator();
            self.coordinator_handles.push(coordinator.clone());
            let stopped = self.stop.clone();
            self.tasks
                .push(tokio::spawn(async move { coordinator.run(stopped).await }));
        }
        self.start_worker();
        self.start_worker();
        for (owner, run) in runs {
            let status = self.await_terminal(owner, run).await?;
            anyhow::ensure!(status.gate_passed, "hosted run failed: {status:?}");
            assert_eq!(status.details.terminal_execution_count, 101);
            let count:i64=sqlx::query_scalar("SELECT count(*) FROM executions WHERE tenant_id=$1 AND run_id=$2 AND status='completed'").bind(owner).bind(run).fetch_one(&self.db.admin[1]).await?;
            assert_eq!(count, 101);
        }
        assert_eq!(self.agent.calls.load(Ordering::SeqCst), 202);
        assert!(matches!(
            self.worker_handles[0].once().await,
            Err(vigilo::operations::Error::Conflict(_))
        ));
        assert!(matches!(
            self.coordinator_handles[0].once().await,
            Err(vigilo::operations::Error::Conflict(_))
        ));
        let workers: i64 =
            sqlx::query_scalar("SELECT count(DISTINCT worker_id) FROM execution_attempts")
                .fetch_one(&self.db.admin[1])
                .await?;
        assert!(workers >= 2, "both shared-client workers must execute work");
        self.failure_scenarios(owners[0], uploads[&owners[0]])
            .await?;
        Ok(())
    }
    async fn cleanup_objects(&self) -> anyhow::Result<()> {
        let Some(control) = self.db.admin.first() else {
            return Ok(());
        };
        let client = resources::storage_client().await?;
        let mut cursor = (Uuid::nil(), Uuid::nil(), String::new());
        for _ in 0..64 {
            let rows:Result<Vec<(Uuid,Uuid,String)>,_>=sqlx::query_as("SELECT * FROM (SELECT tenant_id,upload_id AS entity,object_key FROM dataset_upload_objects UNION SELECT tenant_id,run_id AS entity,object_key FROM run_plan_objects) objects WHERE (tenant_id,entity,object_key)>($1,$2,$3) ORDER BY tenant_id,entity,object_key LIMIT 128").bind(cursor.0).bind(cursor.1).bind(&cursor.2).fetch_all(control).await;
            let rows = match rows {
                Ok(rows) => rows,
                Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("42P01") => {
                    return Ok(());
                }
                Err(e) => return Err(e.into()),
            };
            if rows.is_empty() {
                return Ok(());
            }
            for (tenant, entity, key) in rows {
                let owner = if tenant.is_nil() {
                    *self
                        .object_owners
                        .get(&entity)
                        .context("owned bootstrap entity")?
                } else {
                    tenant
                };
                let prefix = self
                    .owners
                    .get(&owner)
                    .and_then(|v| v["prefix"].as_str())
                    .context("owned storage prefix")?;
                client
                    .delete_object()
                    .bucket(
                        std::env::var("DEV_S3_BUCKET")
                            .or_else(|_| std::env::var("VIGILO_BLOB_TEST_BUCKET"))?,
                    )
                    .key(format!("{prefix}/{key}"))
                    .send()
                    .await?;
                cursor = (tenant, entity, key);
            }
        }
        anyhow::bail!("fixture cleanup exceeded ledger page limit")
    }
    async fn cleanup(&mut self) -> anyhow::Result<()> {
        let services = self.stop_services().await;
        let runtime = tokio::time::timeout(Duration::from_secs(20), async {
            if let Some(runtime) = self.runtime.take() {
                runtime.close().await;
            }
        })
        .await;
        for probe in self.probes.drain(..) {
            let _ = probe.finish();
        }
        let agent = self.agent.close().await;
        let storage = tokio::time::timeout(Duration::from_secs(30), self.cleanup_objects()).await;
        let broker = tokio::time::timeout(Duration::from_secs(15), self.cleanup_broker()).await;
        let database = tokio::time::timeout(Duration::from_secs(60), self.db.cleanup()).await;
        services?;
        runtime.context("runtime cleanup deadline")?;
        agent?;
        storage.context("object cleanup deadline")??;
        broker.context("broker cleanup deadline")??;
        database.context("database cleanup deadline")??;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restricted_lifecycle_uses_injected_resources_and_shared_workers() -> anyhow::Result<()> {
    if std::env::var(E2E_ENABLED_ENV).ok().as_deref() != Some("1") {
        return Ok(());
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("vigilo=warn,lapin=error,sqlx=error")
        .with_test_writer()
        .try_init();
    let mut fixture = Fixture::new().await?;
    let scenario = std::env::var("VIGILO_E2E_HOSTING_SAMPLE").ok();
    let result = std::panic::AssertUnwindSafe(async {
        if let Some(config) = scenario {
            fixture.measurement(serde_json::from_str(&config)?).await
        } else {
            fixture.exercise().await
        }
    })
    .catch_unwind()
    .await;
    let cleanup = fixture.cleanup().await;
    match result {
        Ok(result) => {
            if let Err(error) = result {
                return Err(error.context(format!("fixture cleanup: {cleanup:?}")));
            }
            cleanup?;
        }
        Err(panic) => {
            cleanup?;
            std::panic::resume_unwind(panic)
        }
    };
    Ok(())
}
