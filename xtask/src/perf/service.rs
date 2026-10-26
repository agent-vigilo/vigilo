//! Run-scoped PostgreSQL, RabbitMQ, object storage and deterministic agent services.
//!
//! Docker Compose resources are created under a unique project and carry the
//! campaign marker as a live label. Destructive database, vhost, and topology
//! operations require names derived from that marker. Teardown verifies the
//! exact recorded container and volume inventory before invoking Compose.

use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    fs,
    io::{
        Read,
        Write,
    },
    net::{
        TcpListener,
        TcpStream,
    },
    path::{
        Path,
        PathBuf,
    },
    process::Command,
    sync::{
        Arc,
        Mutex,
        RwLock,
        atomic::{
            AtomicBool,
            AtomicU64,
            Ordering,
        },
    },
    thread::{
        self,
        JoinHandle,
    },
    time::{
        Duration,
        Instant,
    },
};

use anyhow::{
    Context,
    Result,
    bail,
};
use postgres::{
    Client as PgClient,
    NoTls,
};
use reqwest::blocking::Client as HttpClient;
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::{
    Value,
    json,
};
use url::Url;

use super::{
    artifact::{
        atomic_json,
        atomic_text,
    },
    model::{
        ExternalMeasurements,
        QueryDiagnostic,
        TableWrites,
    },
};

const OWNERSHIP_LABEL: &str = "io.vigilo.run-id";
const PERFORMANCE_LABEL: &str = "io.vigilo.performance";
const COMPOSE_PROJECT_LABEL: &str = "com.docker.compose.project";
const POSTGRES_PASSWORD: &str = "vigilo_perf";
const POSTGRES_USER: &str = "vigilo_perf";
const RABBIT_PASSWORD: &str = "vigilo_perf";
const RABBIT_USER: &str = "vigilo_perf";
const RABBIT_MANAGEMENT_READY_TIMEOUT: Duration = Duration::from_secs(60);
const RABBIT_MANAGEMENT_OPERATION_TIMEOUT: Duration = Duration::from_secs(10);
const RABBIT_MANAGEMENT_RETRY_DELAY: Duration = Duration::from_millis(250);
const DATABASE_IDENTIFIER_MAX_BYTES: usize = 63;
const DATABASE_MARKER_PREFIX_BYTES: usize = 7;
const DATABASE_MARKER_HASH_HEX_CHARS: usize = 16;
const WAL_BYTES_QUERY: &str =
    "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), $1::text::pg_lsn)::bigint";

/// Exercises live service ownership, collector reset, and exact cleanup.
pub fn integration_self_test(root: &Path) -> Result<()> {
    let run_id = format!("service-test-{}", uuid::Uuid::now_v7());
    let run_dir = root.join("target/perf/service-tests").join(&run_id);
    fs::create_dir_all(&run_dir)?;
    let mut harness = ServiceHarness::start(root, &run_dir, &run_id, "good", 256)?;
    harness.storage_reader()?.probe()?;
    let database_url = harness.create_database("sentinel")?;
    let scope = harness.create_sample_scope(database_url, "sentinel")?;

    let first = harness.begin_measurement(&scope)?;
    let response = HttpClient::new()
        .post(harness.agent_url())
        .json(&json!({"sentinel": true}))
        .send()?;
    if !response.status().is_success() {
        bail!(
            "deterministic agent sentinel returned {}",
            response.status()
        );
    }
    let mut client = PgClient::connect(&scope.database_url, NoTls)?;
    client.batch_execute("CREATE TABLE collector_rows (id integer PRIMARY KEY); INSERT INTO collector_rows VALUES (1); UPDATE collector_rows SET id = 2; DELETE FROM collector_rows; SELECT pg_stat_force_next_flush()")?;
    client.simple_query("SELECT 42")?;
    drop(client);
    let first_measurement =
        harness.finish_measurement(&scope, first, BTreeMap::from([("sentinel".into(), 1)]))?;
    if first_measurement.table_writes.get("collector_rows")
        != Some(&TableWrites {
            inserted: 1,
            updated: 1,
            deleted: 1,
        })
    {
        bail!(
            "table mutation sentinel differs: {:?}",
            first_measurement.table_writes
        );
    }
    if first_measurement.http_requests != Some(1)
        || first_measurement.durable_counts.get("sentinel") != Some(&1)
        || !first_measurement
            .query_diagnostics
            .iter()
            .any(|diagnostic| diagnostic.plans > 0 && diagnostic.total_plan_time_ms >= 0.0)
    {
        bail!("first collector sentinel or query-planning diagnostic was not observed");
    }

    let second = harness.begin_measurement(&scope)?;
    let second_measurement = harness.finish_measurement(&scope, second, BTreeMap::new())?;
    if second_measurement.table_writes.get("collector_rows") != Some(&TableWrites::default()) {
        bail!("table mutation delta retained prior sample writes");
    }
    if second_measurement.http_requests != Some(0)
        || second_measurement.durable_counts.contains_key("sentinel")
    {
        bail!("sample collector reset retained the prior sentinel");
    }
    harness.restart_rabbitmq()?;
    harness.wait_for_queue_counts(&scope, 0, 0, Duration::from_secs(30))?;
    harness.wait_for_worker_deliveries(&scope, 0, Duration::from_secs(30))?;
    harness.release_scope(scope)?;
    harness.stop()?;
    if !labelled_volumes(root, &run_id)?.is_empty() {
        bail!("service self-test cleanup left run-owned volumes");
    }
    if !labelled_resources(root, "network", &run_id)?.is_empty() {
        bail!("service self-test cleanup left a run-owned network");
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct OwnedResource {
    id: String,
    name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ServiceManifest {
    schema_id: String,
    run_id: String,
    project: String,
    compose_file: String,
    environment_file: String,
    postgres_admin_url: String,
    rabbit_management_url: String,
    agent_url: String,
    containers: Vec<OwnedResource>,
    networks: Vec<OwnedResource>,
    volumes: Vec<OwnedResource>,
}

/// Immutable observations taken immediately before a measured command.
pub struct MeasurementBaseline {
    wal_lsn: String,
    databases: Vec<DatabaseBaseline>,
    service_sampler: ServiceSampler,
}

struct DatabaseBaseline {
    url: String,
    database_bytes: i64,
    table_writes: BTreeMap<String, TableWrites>,
}

#[derive(Default)]
struct ServiceStats {
    memory_bytes: Option<u64>,
    cpu_percent: Option<f64>,
}

struct ServiceSampler {
    stop: Arc<AtomicBool>,
    stats: Arc<Mutex<ServiceStats>>,
    thread: Option<JoinHandle<()>>,
}

/// Exact durable counts returned by a workload-specific oracle.
pub type DurableCounts = BTreeMap<String, i64>;

/// One fresh database and RabbitMQ vhost used by a sample.
pub struct SampleScope {
    /// PostgreSQL URL containing the run ownership marker.
    pub database_url: String,
    /// RabbitMQ URL containing the run-owned vhost.
    pub messaging_url: String,
    /// Namespace applied to all Vigilo RabbitMQ topology names.
    pub mq_namespace: String,
    database_name: String,
    vhost: String,
}

/// Run-scoped service topology and collectors.
pub struct ServiceHarness {
    root: PathBuf,
    run_dir: PathBuf,
    run_id: String,
    marker: String,
    project: String,
    compose_file: PathBuf,
    environment_file: PathBuf,
    storage_endpoint: String,
    storage_config: PathBuf,
    postgres_admin_url: String,
    rabbit_management_url: String,
    rabbit_amqp_port: u16,
    http: HttpClient,
    manifest: ServiceManifest,
    databases: BTreeSet<String>,
    vhosts: BTreeSet<String>,
    sequence: u64,
    agent: DeterministicAgent,
    stopped: bool,
}

impl ServiceHarness {
    /// Starts an isolated Compose project and verifies its ownership inventory.
    pub fn start(
        root: &Path,
        run_dir: &Path,
        run_id: &str,
        agent_response: &str,
        agent_payload_bytes: usize,
    ) -> Result<Self> {
        let marker = safe_name(run_id);
        if marker.is_empty() {
            bail!("performance run ID produced an empty ownership marker");
        }
        let project = truncate_name(&format!("vigilo-perf-{marker}"), 56);
        let compose_file = root.join("infra/performance/compose.yml");
        if !compose_file.is_file() {
            bail!(
                "performance Compose file is missing: {}",
                compose_file.display()
            );
        }
        command_output(
            root,
            "docker",
            &["version", "--format", "{{.Server.Version}}"],
        )
        .context("Docker engine is required for service-backed performance workloads")?;
        command_output(root, "docker", &["compose", "version"])?;

        let environment_file = run_dir.join("service-compose.env");
        atomic_text(
            &environment_file,
            &format!("VIGILO_PERF_PROJECT={project}\nVIGILO_PERF_RUN_ID={run_id}\n"),
        )?;
        let compose_args = compose_args(&compose_file, &environment_file, &project);
        let mut up_args = compose_args.clone();
        up_args.extend(["up".into(), "--detach".into(), "--wait".into()]);
        if let Err(error) = command_output_owned(root, "docker", &up_args) {
            return match cleanup_started_topology(root, run_id, &project, &compose_args) {
                Ok(()) => Err(error.context("start performance topology")),
                Err(cleanup) => Err(error.context(format!(
                    "topology start failed and rollback refused or failed: {cleanup:#}"
                ))),
            };
        }
        let initialized = (|| {
            let agent = DeterministicAgent::start(run_id, agent_response, agent_payload_bytes)?;
            let storage_port = compose_port(root, &compose_args, "ceph", 3900)?;
            let storage_endpoint = format!("http://127.0.0.1:{storage_port}");
            let storage_config = run_dir.join("storage.toml");
            atomic_text(
                &storage_config,
                &format!(
                    "provider='s3'\nalias='cases'\nbucket='vigilo-perf'\nprefix='{project}'\nregion='us-east-1'\nendpoint='{storage_endpoint}'\nallow_http=true\npath_style=true\n[credential_variables]\naccess_key='VIGILO_PERF_S3_ACCESS_KEY'\nsecret_key='VIGILO_PERF_S3_SECRET_KEY'\n"
                ),
            )?;
            let postgres_port = compose_port(root, &compose_args, "postgres", 5432)?;
            let rabbit_amqp_port = compose_port(root, &compose_args, "rabbitmq", 5672)?;
            let rabbit_management_port = compose_port(root, &compose_args, "rabbitmq", 15672)?;
            let postgres_admin_url = format!(
                "postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@127.0.0.1:{postgres_port}/postgres"
            );
            let rabbit_management_url = format!("http://127.0.0.1:{rabbit_management_port}");
            let http = HttpClient::builder()
                .timeout(Duration::from_secs(10))
                .build()?;
            wait_for_rabbit_management(&http, &rabbit_management_url)?;
            let containers = project_resources(root, "container", &project)?;
            let networks = project_resources(root, "network", &project)?;
            let volumes = project_resources(root, "volume", &project)?;
            verify_resource_labels(root, run_id, &project, "container", &containers)?;
            verify_resource_labels(root, run_id, &project, "network", &networks)?;
            verify_resource_labels(root, run_id, &project, "volume", &volumes)?;
            validate_inventory(run_id, &containers, &networks, &volumes)?;
            let manifest = ServiceManifest {
                schema_id: "service-topology/v1".into(),
                run_id: run_id.into(),
                project: project.clone(),
                compose_file: compose_file.display().to_string(),
                environment_file: environment_file.display().to_string(),
                postgres_admin_url: redact_url(&postgres_admin_url),
                rabbit_management_url: rabbit_management_url.clone(),
                agent_url: agent.url().to_owned(),
                containers,
                networks,
                volumes,
            };
            atomic_json(&run_dir.join("services.json"), &manifest)?;
            Ok::<_, anyhow::Error>((
                storage_endpoint,
                storage_config,
                agent,
                rabbit_amqp_port,
                postgres_admin_url,
                rabbit_management_url,
                http,
                manifest,
            ))
        })();
        let (
            storage_endpoint,
            storage_config,
            agent,
            rabbit_amqp_port,
            postgres_admin_url,
            rabbit_management_url,
            http,
            manifest,
        ) =
            match initialized {
                Ok(initialized) => initialized,
                Err(error) => {
                    return match cleanup_started_topology(root, run_id, &project, &compose_args) {
                        Ok(()) => Err(error.context("initialize performance topology")),
                        Err(cleanup) => Err(error
                            .context(format!("topology rollback refused or failed: {cleanup:#}"))),
                    };
                }
            };

        Ok(Self {
            root: root.to_path_buf(),
            run_dir: run_dir.to_path_buf(),
            run_id: run_id.into(),
            marker,
            project,
            compose_file,
            environment_file,
            storage_endpoint,
            storage_config,
            postgres_admin_url,
            rabbit_management_url,
            rabbit_amqp_port,
            http,
            manifest,
            databases: BTreeSet::new(),
            vhosts: BTreeSet::new(),
            sequence: 0,
            agent,
            stopped: false,
        })
    }

    /// Child storage configuration points only into this owned campaign bucket.
    pub fn storage_env(&self) -> Vec<(String, String)> {
        vec![
            (
                "VIGILO_STORAGE_CONFIG".into(),
                self.storage_config.display().to_string(),
            ),
            ("VIGILO_PERF_S3_ACCESS_KEY".into(), "vigilo_perf".into()),
            (
                "VIGILO_PERF_S3_SECRET_KEY".into(),
                "vigilo_perf_storage_secret".into(),
            ),
        ]
    }
    pub(super) fn storage_reader(&self) -> Result<super::storage::Reader> {
        super::storage::Reader::new(&self.storage_endpoint, &self.project)
    }

    /// Returns the deterministic run-scoped agent endpoint.
    pub fn agent_url(&self) -> &str {
        self.agent.url()
    }

    /// Selects deterministic response size and latency before a sample begins.
    pub fn configure_agent(
        &self,
        response_text: &str,
        payload_bytes: usize,
        delay: Duration,
    ) -> Result<()> {
        self.agent.configure(response_text, payload_bytes, delay)
    }

    /// Restarts the run-owned RabbitMQ application and waits for management readiness.
    ///
    /// The container and its ephemeral host ports remain stable while the real
    /// broker application closes client connections and reopens from its durable
    /// volume. This lets resident clients exercise reconnect behavior without
    /// redirecting them to a newly allocated port.
    pub fn restart_rabbitmq(&self) -> Result<()> {
        let container = self
            .manifest
            .containers
            .iter()
            .find(|container| container.name.ends_with("-rabbitmq-1"))
            .context("service manifest omitted its RabbitMQ container")?;
        verify_resource_labels(
            &self.root,
            &self.run_id,
            &self.project,
            "container",
            std::slice::from_ref(container),
        )?;
        command_output(
            &self.root,
            "docker",
            &["exec", &container.id, "rabbitmqctl", "stop_app"],
        )
        .context("stop RabbitMQ application")?;
        command_output(
            &self.root,
            "docker",
            &["exec", &container.id, "rabbitmqctl", "start_app"],
        )
        .context("start RabbitMQ application")?;
        wait_for_rabbit_management(&self.http, &self.rabbit_management_url)
            .context("wait for RabbitMQ after restart")?;
        let containers = project_resources(&self.root, "container", &self.project)?;
        verify_resource_labels(
            &self.root,
            &self.run_id,
            &self.project,
            "container",
            &containers,
        )?;
        if containers != self.manifest.containers {
            bail!("RabbitMQ restart changed the run-owned container inventory");
        }
        Ok(())
    }

    /// Creates an empty database owned by this campaign.
    pub fn create_database(&mut self, purpose: &str) -> Result<String> {
        self.sequence += 1;
        let name = database_name(&self.marker, purpose, self.sequence);
        self.require_owned_database(&name)?;
        let mut admin = PgClient::connect(&self.postgres_admin_url, NoTls)?;
        admin.batch_execute(&format!("CREATE DATABASE {}", quoted_identifier(&name)))?;
        self.databases.insert(name.clone());
        database_url(&self.postgres_admin_url, &name)
    }

    /// Clones a prepared template database into a fresh sample database.
    pub fn clone_database(&mut self, template: &str, purpose: &str) -> Result<String> {
        self.require_owned_database(template)?;
        self.sequence += 1;
        let name = database_name(&self.marker, purpose, self.sequence);
        self.require_owned_database(&name)?;
        let mut admin = PgClient::connect(&self.postgres_admin_url, NoTls)?;
        admin.batch_execute(&format!(
            "CREATE DATABASE {} TEMPLATE {}",
            quoted_identifier(&name),
            quoted_identifier(template)
        ))?;
        self.databases.insert(name.clone());
        database_url(&self.postgres_admin_url, &name)
    }

    /// Returns the database name from a validated run-owned URL.
    pub fn owned_database_name(&self, database_url: &str) -> Result<String> {
        let parsed = Url::parse(database_url)?;
        require_loopback_url(&parsed)?;
        let name = parsed.path().trim_start_matches('/').to_owned();
        self.require_owned_database(&name)?;
        Ok(name)
    }

    /// Creates a fresh RabbitMQ vhost and namespace for one sample.
    pub fn create_sample_scope(
        &mut self,
        database_url: String,
        purpose: &str,
    ) -> Result<SampleScope> {
        self.sequence += 1;
        let suffix = truncate_name(&safe_name(purpose), 20);
        let vhost = truncate_name(
            &format!("perf-{}-{suffix}-{}", self.marker, self.sequence),
            63,
        );
        self.require_owned_vhost(&vhost)?;
        let vhost_url = format!("{}/api/vhosts/{vhost}", self.rabbit_management_url);
        self.rabbit(self.http.put(&vhost_url))?;
        let permission_url = format!(
            "{}/api/permissions/{vhost}/{RABBIT_USER}",
            self.rabbit_management_url
        );
        self.rabbit(self.http.put(&permission_url).json(&json!({
            "configure": ".*",
            "write": ".*",
            "read": ".*"
        })))?;
        self.vhosts.insert(vhost.clone());
        let messaging_url = format!(
            "amqp://{RABBIT_USER}:{RABBIT_PASSWORD}@127.0.0.1:{}/{vhost}",
            self.rabbit_amqp_port
        );
        let database_name = self.owned_database_name(&database_url)?;
        Ok(SampleScope {
            database_url,
            messaging_url,
            mq_namespace: truncate_name(&format!("perf-{}-{}", self.marker, self.sequence), 63),
            database_name,
            vhost,
        })
    }

    /// Resets all scoped collectors immediately before the measured command.
    pub fn begin_measurement(&self, scope: &SampleScope) -> Result<MeasurementBaseline> {
        self.begin_measurement_with_databases(scope, &[])
    }

    /// Resets collectors for the control database and every routed database.
    pub fn begin_measurement_with_databases(
        &self,
        scope: &SampleScope,
        routed_database_urls: &[String],
    ) -> Result<MeasurementBaseline> {
        self.require_scope(scope)?;
        self.agent.reset()?;
        let urls = std::iter::once(scope.database_url.clone())
            .chain(routed_database_urls.iter().cloned())
            .collect::<Vec<_>>();
        let mut databases = Vec::with_capacity(urls.len());
        let mut wal_lsn = None;
        for url in urls {
            self.owned_database_name(&url)?;
            let mut client = PgClient::connect(&url, NoTls)?;
            client.batch_execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")?;
            if wal_lsn.is_none() {
                wal_lsn = Some(
                    client
                        .query_one("SELECT pg_current_wal_lsn()::text", &[])?
                        .get(0),
                );
            }
            let database_bytes: i64 = client
                .query_one("SELECT pg_database_size(current_database())", &[])?
                .get(0);
            let table_writes = table_write_counts(&mut client)?;
            let _ = client.simple_query("SELECT pg_stat_statements_reset()")?;
            databases.push(DatabaseBaseline {
                url,
                database_bytes,
                table_writes,
            });
        }
        let service_sampler = ServiceSampler::start(&self.root, self.container_ids())?;
        Ok(MeasurementBaseline {
            wal_lsn: wal_lsn.context("measurement scope omitted its control database")?,
            databases,
            service_sampler,
        })
    }

    /// Collects scoped database, RabbitMQ, HTTP, and container measurements.
    pub fn finish_measurement(
        &self,
        scope: &SampleScope,
        mut baseline: MeasurementBaseline,
        durable_counts: DurableCounts,
    ) -> Result<ExternalMeasurements> {
        self.require_scope(scope)?;
        let services = baseline.service_sampler.finish()?;
        let mut control = PgClient::connect(&scope.database_url, NoTls)?;
        let wal_bytes: i64 = control
            .query_one(WAL_BYTES_QUERY, &[&baseline.wal_lsn])?
            .get(0);
        let mut sql_calls = 0_u64;
        let mut sql_time_ms = 0.0_f64;
        let mut sql_rows = 0_u64;
        let mut database_bytes_delta = 0_i64;
        let mut table_writes = BTreeMap::<String, TableWrites>::new();
        let mut diagnostics = Vec::new();
        for database in baseline.databases {
            let mut client = PgClient::connect(&database.url, NoTls)?;
            diagnostics.extend(query_diagnostics(&mut client)?);
            let stats = client.query_one(
                r#"
                SELECT
                    COALESCE(SUM(calls), 0)::bigint,
                    COALESCE(SUM(total_exec_time), 0)::double precision,
                    COALESCE(SUM(rows), 0)::bigint
                FROM pg_stat_statements
                WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                  AND query NOT LIKE '%pg_stat_statements%'
                "#,
                &[],
            )?;
            sql_calls = sql_calls.saturating_add(nonnegative_u64(stats.get::<_, i64>(0))?);
            sql_time_ms += stats.get::<_, f64>(1);
            sql_rows = sql_rows.saturating_add(nonnegative_u64(stats.get::<_, i64>(2))?);
            let database_bytes: i64 = client
                .query_one("SELECT pg_database_size(current_database())", &[])?
                .get(0);
            database_bytes_delta = database_bytes_delta
                .saturating_add(database_bytes.saturating_sub(database.database_bytes));
            for (table, current) in table_write_counts(&mut client)? {
                let delta = table_write_delta(&current, database.table_writes.get(&table))?;
                let total = table_writes.entry(table).or_default();
                total.inserted = total
                    .inserted
                    .checked_add(delta.inserted)
                    .context("insert counter overflow")?;
                total.updated = total
                    .updated
                    .checked_add(delta.updated)
                    .context("update counter overflow")?;
                total.deleted = total
                    .deleted
                    .checked_add(delta.deleted)
                    .context("delete counter overflow")?;
            }
        }
        diagnostics.sort_by(|left, right| {
            right
                .total_exec_time_ms
                .total_cmp(&left.total_exec_time_ms)
                .then_with(|| left.query_digest.cmp(&right.query_digest))
        });
        diagnostics.truncate(20);
        let queue = self.queue_counts(&scope.vhost)?;
        let http = self.agent.snapshot();
        Ok(ExternalMeasurements {
            sql_calls: Some(sql_calls),
            sql_time_ms: Some(sql_time_ms),
            sql_rows: Some(sql_rows),
            table_writes,
            wal_bytes: Some(nonnegative_u64(wal_bytes)?),
            database_bytes_delta: Some(database_bytes_delta),
            http_requests: Some(http.requests),
            http_bytes: Some(http.bytes),
            http_peak_concurrency: Some(http.peak_concurrency),
            queue_ready: Some(queue.0),
            queue_unacked: Some(queue.1),
            service_memory_bytes: services.memory_bytes,
            service_cpu_percent: services.cpu_percent,
            durable_counts,
            query_diagnostics: diagnostics,
        })
    }

    /// Purges every queue in one owned sample vhost during fixture preparation.
    pub fn purge_scope_queues(&self, scope: &SampleScope) -> Result<()> {
        self.require_scope(scope)?;
        let response = self
            .http
            .get(format!(
                "{}/api/queues/{}",
                self.rabbit_management_url, scope.vhost
            ))
            .basic_auth(RABBIT_USER, Some(RABBIT_PASSWORD))
            .send()?;
        if !response.status().is_success() {
            bail!("RabbitMQ queue inventory failed with {}", response.status());
        }
        let queues: Vec<Value> = response.json()?;
        for queue in queues {
            let name = queue["name"]
                .as_str()
                .context("RabbitMQ queue omitted name")?;
            let endpoint = format!(
                "{}/api/queues/{}/{}/contents",
                self.rabbit_management_url, scope.vhost, name
            );
            self.rabbit(self.http.delete(endpoint))?;
        }
        Ok(())
    }

    /// Waits for RabbitMQ management statistics to expose an exact settled state.
    pub fn settled_queue_counts(
        &self,
        scope: &SampleScope,
        expected_ready: u64,
        expected_unacked: u64,
    ) -> Result<(u64, u64)> {
        self.require_scope(scope)?;
        let mut observed = (0, 0);
        retry_until_success(
            "RabbitMQ queue settlement",
            RABBIT_MANAGEMENT_OPERATION_TIMEOUT,
            RABBIT_MANAGEMENT_RETRY_DELAY,
            || {
                observed = self.queue_counts(&scope.vhost)?;
                if observed != (expected_ready, expected_unacked) {
                    bail!(
                        "RabbitMQ counts are ready={}, unacked={}; expected {expected_ready}, {expected_unacked}",
                        observed.0,
                        observed.1
                    );
                }
                Ok(())
            },
        )?;
        Ok(observed)
    }

    /// Waits within an explicit recovery bound for one exact queue state.
    pub fn wait_for_queue_counts(
        &self,
        scope: &SampleScope,
        expected_ready: u64,
        expected_unacked: u64,
        timeout: Duration,
    ) -> Result<(u64, u64)> {
        self.require_scope(scope)?;
        let mut observed = (0, 0);
        retry_until_success(
            "RabbitMQ recovery queue settlement",
            timeout,
            RABBIT_MANAGEMENT_RETRY_DELAY,
            || {
                observed = self.queue_counts(&scope.vhost)?;
                if observed != (expected_ready, expected_unacked) {
                    bail!(
                        "RabbitMQ counts are ready={}, unacked={}; expected {expected_ready}, {expected_unacked}",
                        observed.0,
                        observed.1
                    );
                }
                Ok(())
            },
        )?;
        Ok(observed)
    }

    /// Waits for the run-owned worker queue to expose one exact delivery total.
    pub fn wait_for_worker_deliveries(
        &self,
        scope: &SampleScope,
        expected: u64,
        timeout: Duration,
    ) -> Result<u64> {
        self.require_scope(scope)?;
        let mut observed = 0;
        retry_until_success(
            "RabbitMQ worker delivery count",
            timeout,
            RABBIT_MANAGEMENT_RETRY_DELAY,
            || {
                observed = self.worker_delivery_count(&scope.vhost)?;
                if observed != expected {
                    bail!("RabbitMQ worker deliveries are {observed}; expected {expected}");
                }
                Ok(())
            },
        )?;
        Ok(observed)
    }

    /// Removes one sample's database and RabbitMQ vhost after artifacts are captured.
    pub fn release_scope(&mut self, scope: SampleScope) -> Result<()> {
        self.require_scope(&scope)?;
        self.delete_vhost(&scope.vhost)?;
        self.drop_database(&scope.database_name)?;
        Ok(())
    }

    /// Drops a prepared template database after all dependent samples finish.
    pub fn release_database(&mut self, database_url: &str) -> Result<()> {
        let name = self.owned_database_name(database_url)?;
        self.drop_database(&name)
    }

    /// Drops a set of sample-owned routed databases, attempting every cleanup.
    pub fn release_databases(&mut self, database_urls: &[String]) -> Result<()> {
        let mut failures = Vec::new();
        for database_url in database_urls {
            if let Err(error) = self.release_database(database_url) {
                failures.push(format!("{error:#}"));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            bail!("release routed databases: {}", failures.join("; "))
        }
    }

    /// Stops the topology after verifying the recorded live ownership inventory.
    pub fn stop(&mut self) -> Result<()> {
        if self.stopped {
            return Ok(());
        }
        let mut cleanup_failures = Vec::new();
        for vhost in self.vhosts.clone() {
            if let Err(error) = self.delete_vhost(&vhost) {
                cleanup_failures.push(format!("delete vhost {vhost}: {error:#}"));
            }
        }
        for database in self.databases.clone() {
            if let Err(error) = self.drop_database(&database) {
                cleanup_failures.push(format!("drop database {database}: {error:#}"));
            }
        }
        let compose_args = compose_args(&self.compose_file, &self.environment_file, &self.project);
        let live_containers = project_resources(&self.root, "container", &self.project)?;
        let live_networks = project_resources(&self.root, "network", &self.project)?;
        let live_volumes = project_resources(&self.root, "volume", &self.project)?;
        verify_resource_labels(
            &self.root,
            &self.run_id,
            &self.project,
            "container",
            &live_containers,
        )?;
        verify_resource_labels(
            &self.root,
            &self.run_id,
            &self.project,
            "network",
            &live_networks,
        )?;
        verify_resource_labels(
            &self.root,
            &self.run_id,
            &self.project,
            "volume",
            &live_volumes,
        )?;
        validate_cleanup_inventory(
            &self.run_id,
            &self.manifest,
            &live_containers,
            &live_networks,
            &live_volumes,
        )?;
        remove_topology(&self.root, &self.run_id, &self.project, &compose_args)?;
        atomic_text(&self.run_dir.join("services.stopped"), &self.run_id)?;
        self.stopped = true;
        if cleanup_failures.is_empty() {
            Ok(())
        } else {
            bail!(
                "topology was removed after scoped cleanup failures: {}",
                cleanup_failures.join("; ")
            )
        }
    }

    /// Fences database operations to names derived from this campaign marker.
    fn require_owned_database(&self, name: &str) -> Result<()> {
        let prefix = database_prefix(&self.marker);
        if !name.starts_with(&prefix)
            || !name.chars().all(|character| {
                character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
            })
        {
            bail!("refusing database operation without matching ownership marker: {name}");
        }
        Ok(())
    }

    /// Fences RabbitMQ operations to vhosts derived from this campaign marker.
    fn require_owned_vhost(&self, vhost: &str) -> Result<()> {
        if !vhost.starts_with(&format!("perf-{}-", self.marker)) {
            bail!("refusing RabbitMQ operation without matching ownership marker: {vhost}");
        }
        Ok(())
    }

    /// Revalidates ownership and loopback isolation before using a sample scope.
    fn require_scope(&self, scope: &SampleScope) -> Result<()> {
        self.require_owned_database(&scope.database_name)?;
        self.require_owned_vhost(&scope.vhost)?;
        let database = Url::parse(&scope.database_url)?;
        let messaging = Url::parse(&scope.messaging_url)?;
        require_loopback_url(&database)?;
        require_loopback_url(&messaging)
    }

    /// Executes an authenticated RabbitMQ management mutation and requires success.
    fn rabbit(&self, request: reqwest::blocking::RequestBuilder) -> Result<()> {
        retry_until_success(
            "RabbitMQ management mutation",
            RABBIT_MANAGEMENT_OPERATION_TIMEOUT,
            RABBIT_MANAGEMENT_RETRY_DELAY,
            || {
                let response = request
                    .try_clone()
                    .context("RabbitMQ management request body cannot be retried")?
                    .basic_auth(RABBIT_USER, Some(RABBIT_PASSWORD))
                    .send()?;
                if !response.status().is_success() {
                    bail!(
                        "RabbitMQ management request failed with {}",
                        response.status()
                    );
                }
                Ok(())
            },
        )
    }

    /// Sums ready and unacknowledged messages across queues in one owned vhost.
    fn queue_counts(&self, vhost: &str) -> Result<(u64, u64)> {
        self.require_owned_vhost(vhost)?;
        let response = self
            .http
            .get(format!("{}/api/queues/{vhost}", self.rabbit_management_url))
            .basic_auth(RABBIT_USER, Some(RABBIT_PASSWORD))
            .send()?;
        if !response.status().is_success() {
            bail!("RabbitMQ queue collector failed with {}", response.status());
        }
        let queues: Vec<Value> = response.json()?;
        let (ready, unacked, _) = worker_queue_metrics(&queues);
        Ok((ready, unacked))
    }

    fn worker_delivery_count(&self, vhost: &str) -> Result<u64> {
        self.require_owned_vhost(vhost)?;
        let response = self
            .http
            .get(format!("{}/api/queues/{vhost}", self.rabbit_management_url))
            .basic_auth(RABBIT_USER, Some(RABBIT_PASSWORD))
            .send()?;
        if !response.status().is_success() {
            bail!(
                "RabbitMQ delivery collector failed with {}",
                response.status()
            );
        }
        let queues: Vec<Value> = response.json()?;
        Ok(worker_queue_metrics(&queues).2)
    }

    fn container_ids(&self) -> Vec<String> {
        self.manifest
            .containers
            .iter()
            .map(|resource| resource.id.clone())
            .collect()
    }

    /// Idempotently deletes a vhost tracked as owned by this harness instance.
    fn delete_vhost(&mut self, vhost: &str) -> Result<()> {
        self.require_owned_vhost(vhost)?;
        if !self.vhosts.contains(vhost) {
            return Ok(());
        }
        let response = self
            .http
            .delete(format!("{}/api/vhosts/{vhost}", self.rabbit_management_url))
            .basic_auth(RABBIT_USER, Some(RABBIT_PASSWORD))
            .send()?;
        if !response.status().is_success() && response.status().as_u16() != 404 {
            bail!("RabbitMQ vhost cleanup failed with {}", response.status());
        }
        self.vhosts.remove(vhost);
        Ok(())
    }

    /// Idempotently drops a tracked database after terminating its connections.
    fn drop_database(&mut self, name: &str) -> Result<()> {
        self.require_owned_database(name)?;
        if !self.databases.contains(name) {
            return Ok(());
        }
        let mut admin = PgClient::connect(&self.postgres_admin_url, NoTls)?;
        admin.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
            &[&name],
        )?;
        admin.batch_execute(&format!(
            "DROP DATABASE IF EXISTS {}",
            quoted_identifier(name)
        ))?;
        self.databases.remove(name);
        Ok(())
    }
}

fn worker_queue_metrics(queues: &[Value]) -> (u64, u64, u64) {
    queues
        .iter()
        .filter(|queue| {
            queue["name"]
                .as_str()
                .is_some_and(|name| name == "vigilo.worker" || name.ends_with(".worker"))
        })
        .fold((0_u64, 0_u64, 0_u64), |totals, queue| {
            (
                totals
                    .0
                    .saturating_add(queue["messages_ready"].as_u64().unwrap_or_default()),
                totals.1.saturating_add(
                    queue["messages_unacknowledged"]
                        .as_u64()
                        .unwrap_or_default(),
                ),
                totals.2.saturating_add(
                    queue["message_stats"]["deliver_get"]
                        .as_u64()
                        .unwrap_or_default(),
                ),
            )
        })
}

/// Reads all public-table counters after the measured child has closed its connections.
///
/// PostgreSQL flushes backend counters at connection exit. Fresh collector
/// transactions avoid retaining a statistics snapshot from baseline collection.
fn table_write_counts(client: &mut PgClient) -> Result<BTreeMap<String, TableWrites>> {
    client.query(
        "SELECT relname, n_tup_ins, n_tup_upd, n_tup_del FROM pg_stat_user_tables WHERE schemaname = 'public' ORDER BY relname",
        &[],
    )?.into_iter().map(|row| Ok((row.get(0), TableWrites {
        inserted: nonnegative_u64(row.get(1))?,
        updated: nonnegative_u64(row.get(2))?,
        deleted: nonnegative_u64(row.get(3))?,
    }))).collect()
}

/// Rejects resets rather than silently reporting an invalid zero-write sample.
fn table_write_delta(current: &TableWrites, baseline: Option<&TableWrites>) -> Result<TableWrites> {
    let zero = TableWrites::default();
    let baseline = baseline.unwrap_or(&zero);
    Ok(TableWrites {
        inserted: current
            .inserted
            .checked_sub(baseline.inserted)
            .context("insert counters reset during sample")?,
        updated: current
            .updated
            .checked_sub(baseline.updated)
            .context("update counters reset during sample")?,
        deleted: current
            .deleted
            .checked_sub(baseline.deleted)
            .context("delete counters reset during sample")?,
    })
}

/// Captures normalized statement, planning, buffer, and WAL evidence after timing.
fn query_diagnostics(client: &mut PgClient) -> Result<Vec<QueryDiagnostic>> {
    client
        .query(
            r#"
            SELECT
                query,
                calls::bigint,
                plans::bigint,
                rows::bigint,
                total_plan_time::double precision,
                total_exec_time::double precision,
                shared_blks_hit::bigint,
                shared_blks_read::bigint,
                (temp_blks_read + temp_blks_written)::bigint,
                wal_records::bigint,
                wal_fpi::bigint,
                wal_bytes::numeric::text
            FROM pg_stat_statements
            WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
              AND query NOT LIKE '%pg_stat_statements%'
            ORDER BY total_exec_time DESC
            LIMIT 25
            "#,
            &[],
        )?
        .into_iter()
        .map(|row| {
            let query: String = row.get(0);
            let wal_bytes = row
                .get::<_, String>(11)
                .split_once('.')
                .map_or_else(|| row.get::<_, String>(11), |(whole, _)| whole.into())
                .parse::<u64>()?;
            Ok(QueryDiagnostic {
                query_digest: blake3::hash(query.as_bytes()).to_hex().to_string(),
                query: bounded_text(&query, 512),
                calls: nonnegative_u64(row.get(1))?,
                plans: nonnegative_u64(row.get(2))?,
                rows: nonnegative_u64(row.get(3))?,
                total_plan_time_ms: row.get(4),
                total_exec_time_ms: row.get(5),
                shared_blocks_hit: nonnegative_u64(row.get(6))?,
                shared_blocks_read: nonnegative_u64(row.get(7))?,
                temporary_blocks: nonnegative_u64(row.get(8))?,
                wal_records: nonnegative_u64(row.get(9))?,
                wal_full_page_images: nonnegative_u64(row.get(10))?,
                wal_bytes,
            })
        })
        .collect()
}

fn bounded_text(value: &str, max_chars: usize) -> String {
    let mut characters = value.chars();
    let bounded = characters.by_ref().take(max_chars).collect::<String>();
    if characters.next().is_some() {
        format!("{bounded}...")
    } else {
        bounded
    }
}

/// Waits for authenticated RabbitMQ management requests to complete successfully.
fn wait_for_rabbit_management(http: &HttpClient, management_url: &str) -> Result<()> {
    let overview_url = format!("{management_url}/api/overview");
    retry_until_success(
        "RabbitMQ management API",
        RABBIT_MANAGEMENT_READY_TIMEOUT,
        RABBIT_MANAGEMENT_RETRY_DELAY,
        || {
            let response = http
                .get(&overview_url)
                .basic_auth(RABBIT_USER, Some(RABBIT_PASSWORD))
                .send()
                .context("send authenticated RabbitMQ readiness probe")?;
            if !response.status().is_success() {
                bail!("RabbitMQ readiness probe failed with {}", response.status());
            }
            Ok(())
        },
    )
}

/// Repeats a setup operation until it succeeds or its bounded deadline expires.
fn retry_until_success(
    operation: &str,
    timeout: Duration,
    retry_delay: Duration,
    mut probe: impl FnMut() -> Result<()>,
) -> Result<()> {
    let started = Instant::now();
    loop {
        match probe() {
            Ok(()) => return Ok(()),
            Err(error) if started.elapsed() >= timeout => {
                return Err(error.context(format!(
                    "{operation} did not succeed within {} ms",
                    timeout.as_millis()
                )));
            }
            Err(_) => {
                let remaining = timeout.saturating_sub(started.elapsed());
                thread::sleep(retry_delay.min(remaining));
            }
        }
    }
}

impl Drop for ServiceHarness {
    fn drop(&mut self) {
        if let Err(error) = self.stop() {
            eprintln!(
                "performance topology cleanup refused or failed; inspect {}: {error:#}",
                self.run_dir.join("services.json").display()
            );
        }
    }
}

impl ServiceSampler {
    /// Starts peak CPU and memory sampling for the exact recorded containers.
    fn start(root: &Path, container_ids: Vec<String>) -> Result<Self> {
        let initial = collect_container_stats(root, &container_ids)?;
        let stop = Arc::new(AtomicBool::new(false));
        let stats = Arc::new(Mutex::new(initial));
        let thread_root = root.to_path_buf();
        let thread_stop = stop.clone();
        let thread_stats = stats.clone();
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                if let Ok(observation) = collect_container_stats(&thread_root, &container_ids)
                    && let Ok(mut aggregate) = thread_stats.lock()
                {
                    aggregate.memory_bytes =
                        maximum(aggregate.memory_bytes, observation.memory_bytes);
                    aggregate.cpu_percent =
                        maximum_f64(aggregate.cpu_percent, observation.cpu_percent);
                }
                thread::sleep(Duration::from_millis(50));
            }
        });
        Ok(Self {
            stop,
            stats,
            thread: Some(thread),
        })
    }

    /// Stops the collector thread and returns peak observations.
    fn finish(&mut self) -> Result<ServiceStats> {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| anyhow::anyhow!("service collector thread panicked"))?;
        }
        let stats = self
            .stats
            .lock()
            .map_err(|_| anyhow::anyhow!("service collector state was poisoned"))?;
        Ok(ServiceStats {
            memory_bytes: stats.memory_bytes,
            cpu_percent: stats.cpu_percent,
        })
    }
}

impl Drop for ServiceSampler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Collects one aggregate Docker CPU and memory observation for owned containers.
fn collect_container_stats(root: &Path, ids: &[String]) -> Result<ServiceStats> {
    if ids.is_empty() {
        return Ok(ServiceStats::default());
    }
    let mut args = vec![
        "stats".into(),
        "--no-stream".into(),
        "--format".into(),
        "{{json .}}".into(),
    ];
    args.extend(ids.iter().cloned());
    let output = command_output_owned(root, "docker", &args)?;
    let mut memory = 0u64;
    let mut cpu = 0.0;
    for line in output.lines().filter(|line| !line.trim().is_empty()) {
        let value: Value = serde_json::from_str(line)?;
        if let Some(usage) = value["MemUsage"].as_str().and_then(parse_memory_usage) {
            memory = memory.saturating_add(usage);
        }
        if let Some(percent) = value["CPUPerc"].as_str().and_then(parse_percent) {
            cpu += percent;
        }
    }
    Ok(ServiceStats {
        memory_bytes: Some(memory),
        cpu_percent: Some(cpu),
    })
}

fn maximum(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    left.into_iter().chain(right).max()
}

fn maximum_f64(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    left.into_iter().chain(right).reduce(f64::max)
}

#[derive(Default)]
struct AgentCounters {
    requests: AtomicU64,
    bytes: AtomicU64,
    active: AtomicU64,
    peak: AtomicU64,
}

#[derive(Debug)]
struct AgentSnapshot {
    requests: u64,
    bytes: u64,
    peak_concurrency: u64,
}

struct DeterministicAgent {
    url: String,
    counters: Arc<AgentCounters>,
    behavior: Arc<RwLock<AgentBehavior>>,
    shutdown: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

#[derive(Clone)]
struct AgentBehavior {
    body: Vec<u8>,
    delay: Duration,
}

impl DeterministicAgent {
    /// Starts a loopback HTTP fixture with a stable response and request counters.
    fn start(run_id: &str, response_text: &str, payload_bytes: usize) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let path = format!("/perf/{}/invoke", safe_name(run_id));
        let url = format!("http://{address}{path}");
        let counters = Arc::new(AgentCounters::default());
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_counters = counters.clone();
        let thread_shutdown = shutdown.clone();
        let behavior = Arc::new(RwLock::new(AgentBehavior {
            body: agent_body(response_text, payload_bytes),
            delay: Duration::ZERO,
        }));
        let thread_behavior = behavior.clone();
        let thread = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let counters = thread_counters.clone();
                        let behavior = thread_behavior.clone();
                        thread::spawn(move || {
                            let behavior = behavior.read().map(|behavior| behavior.clone());
                            if let Ok(behavior) = behavior {
                                let _ = handle_agent(stream, &behavior, &counters);
                            }
                        });
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });
        Ok(Self {
            url,
            counters,
            behavior,
            shutdown,
            thread: Some(thread),
        })
    }

    fn url(&self) -> &str {
        &self.url
    }

    /// Reconfigures the fixture only when no request can observe a partial change.
    fn configure(&self, response_text: &str, payload_bytes: usize, delay: Duration) -> Result<()> {
        if self.counters.active.load(Ordering::Acquire) != 0 {
            bail!("cannot configure HTTP agent while a request is active");
        }
        let mut behavior = self
            .behavior
            .write()
            .map_err(|_| anyhow::anyhow!("HTTP agent behavior lock was poisoned"))?;
        *behavior = AgentBehavior {
            body: agent_body(response_text, payload_bytes),
            delay,
        };
        Ok(())
    }

    /// Clears counters at a measurement boundary when no request is active.
    fn reset(&self) -> Result<()> {
        if self.counters.active.load(Ordering::Acquire) != 0 {
            bail!("cannot reset HTTP collector while a request is active");
        }
        self.counters.requests.store(0, Ordering::Release);
        self.counters.bytes.store(0, Ordering::Release);
        self.counters.peak.store(0, Ordering::Release);
        Ok(())
    }

    /// Captures request, byte, and peak-concurrency counts for one sample.
    fn snapshot(&self) -> AgentSnapshot {
        AgentSnapshot {
            requests: self.counters.requests.load(Ordering::Acquire),
            bytes: self.counters.bytes.load(Ordering::Acquire),
            peak_concurrency: self.counters.peak.load(Ordering::Acquire),
        }
    }
}

impl Drop for DeterministicAgent {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Serves one complete HTTP request and accounts for request and response bytes.
fn handle_agent(
    mut stream: TcpStream,
    behavior: &AgentBehavior,
    counters: &AgentCounters,
) -> Result<()> {
    // Accepted sockets inherit nonblocking mode on Windows even though the
    // portable handler below performs bounded blocking reads.
    stream.set_nonblocking(false)?;
    let active = counters.active.fetch_add(1, Ordering::AcqRel) + 1;
    counters.peak.fetch_max(active, Ordering::AcqRel);
    let result = (|| {
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        let mut request = Vec::new();
        let mut buffer = [0u8; 8192];
        loop {
            let read = stream.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..read]);
            if let Some(header_end) = find_bytes(&request, b"\r\n\r\n") {
                let content_length = parse_content_length(&request[..header_end]).unwrap_or(0);
                if request.len() >= header_end + 4 + content_length {
                    break;
                }
            }
            if request.len() > 2 * 1024 * 1024 {
                bail!("agent request exceeded 2 MiB");
            }
        }
        counters.requests.fetch_add(1, Ordering::AcqRel);
        counters.bytes.fetch_add(
            (request.len() + behavior.body.len()) as u64,
            Ordering::AcqRel,
        );
        thread::sleep(behavior.delay);
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            behavior.body.len()
        )?;
        stream.write_all(&behavior.body)?;
        Ok(())
    })();
    counters.active.fetch_sub(1, Ordering::AcqRel);
    result
}

/// Builds valid agent JSON padded to the requested size when structurally possible.
fn agent_body(response_text: &str, payload_bytes: usize) -> Vec<u8> {
    let minimum = json!({"text": response_text}).to_string();
    if minimum.len() >= payload_bytes {
        return minimum.into_bytes();
    }
    let empty_padded = json!({"text": response_text, "padding": ""}).to_string();
    if empty_padded.len() > payload_bytes {
        return minimum.into_bytes();
    }
    let padding = "x".repeat(payload_bytes - empty_padded.len());
    json!({"text": response_text, "padding": padding})
        .to_string()
        .into_bytes()
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn parse_content_length(headers: &[u8]) -> Option<usize> {
    for line in String::from_utf8_lossy(headers).lines() {
        let lowercase = line.to_ascii_lowercase();
        if let Some(value) = lowercase.strip_prefix("content-length:") {
            return value.trim().parse().ok();
        }
    }
    None
}

/// Builds the common Compose argument prefix for one isolated project.
fn compose_args(compose: &Path, environment: &Path, project: &str) -> Vec<String> {
    vec![
        "compose".into(),
        "--file".into(),
        compose.display().to_string(),
        "--env-file".into(),
        environment.display().to_string(),
        "--project-name".into(),
        project.into(),
    ]
}

/// Resolves a dynamically assigned service port and requires a loopback binding.
fn compose_port(root: &Path, compose_args: &[String], service: &str, port: u16) -> Result<u16> {
    let mut args = compose_args.to_vec();
    args.extend(["port".into(), service.into(), port.to_string()]);
    let output = command_output_owned(root, "docker", &args)?;
    let address = output
        .lines()
        .next()
        .context("Docker Compose returned no port")?
        .trim();
    let (host, port) = address
        .rsplit_once(':')
        .context("invalid Docker Compose port output")?;
    if host.trim_matches(['[', ']']) != "127.0.0.1" {
        bail!("performance service was not bound to loopback: {address}");
    }
    Ok(port.parse()?)
}

/// Returns every sorted Docker resource owned by one Compose project.
fn project_resources(root: &Path, kind: &str, project: &str) -> Result<Vec<OwnedResource>> {
    let mut args = match kind {
        "container" => vec!["container".into(), "ls".into(), "--all".into()],
        "network" | "volume" => vec![kind.into(), "ls".into()],
        _ => bail!("unsupported Compose resource kind: {kind}"),
    };
    args.extend([
        "--quiet".into(),
        "--filter".into(),
        format!("label={COMPOSE_PROJECT_LABEL}={project}"),
    ]);
    let ids = command_output_owned(root, "docker", &args)?;
    let mut resources = Vec::new();
    for id in ids.lines().map(str::trim).filter(|id| !id.is_empty()) {
        let inspected = command_output(
            root,
            "docker",
            &["inspect", "--type", kind, "--format", "{{.Name}}", id],
        )?;
        resources.push(OwnedResource {
            id: id.into(),
            name: inspected.trim_start_matches('/').into(),
        });
    }
    resources.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(resources)
}

fn labelled_volumes(root: &Path, run_id: &str) -> Result<Vec<OwnedResource>> {
    labelled_resources(root, "volume", run_id)
}

/// Lists sorted Docker resources carrying both performance ownership labels.
fn labelled_resources(root: &Path, kind: &str, run_id: &str) -> Result<Vec<OwnedResource>> {
    let mut args = match kind {
        "container" => vec!["container".into(), "ls".into(), "--all".into()],
        "network" | "volume" => vec![kind.into(), "ls".into()],
        _ => bail!("unsupported labelled resource kind: {kind}"),
    };
    args.extend([
        "--quiet".into(),
        "--filter".into(),
        format!("label={OWNERSHIP_LABEL}={run_id}"),
        "--filter".into(),
        format!("label={PERFORMANCE_LABEL}=true"),
    ]);
    let output = command_output_owned(root, "docker", &args)?;
    let mut resources = output
        .lines()
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(|name| OwnedResource {
            id: name.into(),
            name: name.into(),
        })
        .collect::<Vec<_>>();
    resources.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(resources)
}

/// Rechecks ownership and fixed Compose component labels before destructive work.
fn verify_resource_labels(
    root: &Path,
    run_id: &str,
    project: &str,
    kind: &str,
    resources: &[OwnedResource],
) -> Result<()> {
    let (labels_template, component_label, expected_components): (&str, &str, &[&str]) = match kind
    {
        "container" => (
            "{{json .Config.Labels}}",
            "com.docker.compose.service",
            &["postgres", "rabbitmq", "ceph"],
        ),
        "network" => (
            "{{json .Labels}}",
            "com.docker.compose.network",
            &["default"],
        ),
        "volume" => (
            "{{json .Labels}}",
            "com.docker.compose.volume",
            &["postgres-data", "rabbitmq-data", "ceph-data"],
        ),
        _ => bail!("unsupported Compose resource kind: {kind}"),
    };
    let mut observed_components = BTreeSet::new();
    for resource in resources {
        let output = command_output(
            root,
            "docker",
            &[
                "inspect",
                "--type",
                kind,
                "--format",
                labels_template,
                &resource.id,
            ],
        )?;
        let labels: BTreeMap<String, String> = serde_json::from_str(&output)
            .with_context(|| format!("parse labels for {kind} {}", resource.name))?;
        let component = labels.get(component_label).cloned();
        if labels.get(OWNERSHIP_LABEL).map(String::as_str) != Some(run_id)
            || labels.get(PERFORMANCE_LABEL).map(String::as_str) != Some("true")
            || labels.get(COMPOSE_PROJECT_LABEL).map(String::as_str) != Some(project)
            || !component
                .as_deref()
                .is_some_and(|value| expected_components.contains(&value))
        {
            bail!(
                "performance {kind} {} has unexpected ownership or component labels",
                resource.name
            );
        }
        if !observed_components.insert(component.expect("component was validated")) {
            bail!("performance topology has duplicate {kind} component labels");
        }
    }
    Ok(())
}

/// Requires the exact three-container, one-network, three-volume service topology.
fn validate_inventory(
    run_id: &str,
    containers: &[OwnedResource],
    networks: &[OwnedResource],
    volumes: &[OwnedResource],
) -> Result<()> {
    if containers.len() != 3 || networks.len() != 1 || volumes.len() != 3 {
        bail!(
            "performance topology inventory mismatch for {run_id}: expected 3 containers, 1 network, and 3 volumes; found {}, {}, and {}",
            containers.len(),
            networks.len(),
            volumes.len()
        );
    }
    Ok(())
}

/// Bounds resources that Compose may have created before a failed startup.
fn validate_partial_inventory(
    run_id: &str,
    containers: &[OwnedResource],
    networks: &[OwnedResource],
    volumes: &[OwnedResource],
) -> Result<()> {
    if containers.len() > 3 || networks.len() > 1 || volumes.len() > 3 {
        bail!(
            "partial performance topology inventory exceeded bounds for {run_id}: expected at most 3 containers, 1 network, and 3 volumes; found {}, {}, and {}",
            containers.len(),
            networks.len(),
            volumes.len()
        );
    }
    Ok(())
}

/// Refuses cleanup unless live resources exactly match the persisted authority.
fn validate_cleanup_inventory(
    run_id: &str,
    manifest: &ServiceManifest,
    live_containers: &[OwnedResource],
    live_networks: &[OwnedResource],
    live_volumes: &[OwnedResource],
) -> Result<()> {
    if manifest.run_id != run_id
        || manifest.containers != live_containers
        || manifest.networks != live_networks
        || manifest.volumes != live_volumes
    {
        bail!("refusing topology cleanup because the live ownership inventory changed");
    }
    Ok(())
}

/// Rolls back a partially initialized topology only after live ownership checks pass.
fn cleanup_started_topology(
    root: &Path,
    run_id: &str,
    project: &str,
    compose_args: &[String],
) -> Result<()> {
    let containers = project_resources(root, "container", project)?;
    let networks = project_resources(root, "network", project)?;
    let volumes = project_resources(root, "volume", project)?;
    verify_resource_labels(root, run_id, project, "container", &containers)?;
    verify_resource_labels(root, run_id, project, "network", &networks)?;
    verify_resource_labels(root, run_id, project, "volume", &volumes)?;
    validate_partial_inventory(run_id, &containers, &networks, &volumes)?;
    remove_topology(root, run_id, project, compose_args)
}

/// Removes one validated Compose project and proves that no owned resources remain.
fn remove_topology(
    root: &Path,
    run_id: &str,
    project: &str,
    compose_args: &[String],
) -> Result<()> {
    let mut down_args = compose_args.to_vec();
    down_args.extend(["down".into(), "--volumes".into(), "--remove-orphans".into()]);
    command_output_owned(root, "docker", &down_args)?;
    let project_resources_remain = ["container", "network", "volume"]
        .into_iter()
        .map(|kind| project_resources(root, kind, project))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .any(|resources| !resources.is_empty());
    let run_resources_remain = ["container", "network", "volume"]
        .into_iter()
        .map(|kind| labelled_resources(root, kind, run_id))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .any(|resources| !resources.is_empty());
    if project_resources_remain || run_resources_remain {
        bail!("performance topology cleanup left project-owned or run-owned resources");
    }
    Ok(())
}

/// Builds a PostgreSQL identifier while retaining its complete unique sequence suffix.
fn database_name(marker: &str, purpose: &str, sequence: u64) -> String {
    let prefix = database_prefix(marker);
    let sequence = base36(sequence);
    let available = DATABASE_IDENTIFIER_MAX_BYTES
        .checked_sub(prefix.len() + sequence.len() + 2)
        .expect("database naming constants must leave room for a purpose");
    let purpose = truncate_name(&safe_name(purpose).replace('-', "_"), available);
    format!("{prefix}_{purpose}_{sequence}")
}

/// Produces a compact ownership prefix that hashes the complete, untruncated marker.
fn database_prefix(marker: &str) -> String {
    let readable = truncate_name(
        &safe_name(marker).replace('-', "_"),
        DATABASE_MARKER_PREFIX_BYTES,
    );
    let digest = blake3::hash(marker.as_bytes()).to_hex();
    format!(
        "vigilo_perf_{readable}_{}",
        &digest[..DATABASE_MARKER_HASH_HEX_CHARS]
    )
}

/// Encodes the full `u64` sequence in at most thirteen identifier-safe characters.
fn base36(mut value: u64) -> String {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut encoded = [0_u8; 13];
    let mut position = encoded.len();
    loop {
        position -= 1;
        encoded[position] = DIGITS[(value % 36) as usize];
        value /= 36;
        if value == 0 {
            break;
        }
    }
    String::from_utf8_lossy(&encoded[position..]).into_owned()
}

fn database_url(admin_url: &str, name: &str) -> Result<String> {
    let mut url = Url::parse(admin_url)?;
    require_loopback_url(&url)?;
    url.set_path(&format!("/{name}"));
    Ok(url.to_string())
}

fn require_loopback_url(url: &Url) -> Result<()> {
    if !matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "::1")) {
        bail!("performance endpoint must use a loopback host: {url}");
    }
    Ok(())
}

fn quoted_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn safe_name(value: &str) -> String {
    value
        .chars()
        .filter_map(|character| {
            if character.is_ascii_alphanumeric() {
                Some(character.to_ascii_lowercase())
            } else if matches!(character, '-' | '_' | '.') {
                Some('-')
            } else {
                None
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_owned()
}

fn truncate_name(value: &str, maximum: usize) -> String {
    value.chars().take(maximum).collect()
}

fn redact_url(value: &str) -> String {
    Url::parse(value)
        .map(|mut url| {
            let _ = url.set_password(Some("<redacted>"));
            url.to_string()
        })
        .unwrap_or_else(|_| "<invalid-url>".into())
}

fn nonnegative_u64(value: i64) -> Result<u64> {
    value
        .try_into()
        .context("collector returned a negative count")
}

fn parse_percent(value: &str) -> Option<f64> {
    value.trim().trim_end_matches('%').parse().ok()
}

fn parse_memory_usage(value: &str) -> Option<u64> {
    parse_size(value.split('/').next()?.trim())
}

fn parse_size(value: &str) -> Option<u64> {
    let split = value.find(|character: char| !character.is_ascii_digit() && character != '.')?;
    let number: f64 = value[..split].parse().ok()?;
    let multiplier = match value[split..].trim() {
        "B" => 1.0,
        "kB" | "KB" => 1_000.0,
        "KiB" => 1_024.0,
        "MB" => 1_000_000.0,
        "MiB" => 1_048_576.0,
        "GB" => 1_000_000_000.0,
        "GiB" => 1_073_741_824.0,
        _ => return None,
    };
    Some((number * multiplier) as u64)
}

fn command_output(root: &Path, program: &str, args: &[&str]) -> Result<String> {
    let args = args
        .iter()
        .map(|value| (*value).to_owned())
        .collect::<Vec<_>>();
    command_output_owned(root, program, &args)
}

fn command_output_owned(root: &Path, program: &str, args: &[String]) -> Result<String> {
    let output = Command::new(program)
        .args(args)
        .current_dir(root)
        .output()
        .with_context(|| format!("run {program} {}", args.join(" ")))?;
    if !output.status.success() {
        bail!(
            "{program} {} failed with {}: {}",
            args.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

#[cfg(test)]
mod tests {
    #[test]
    fn table_write_counters_distinguish_mutations_and_reject_resets() {
        let current = super::TableWrites {
            inserted: 8,
            updated: 5,
            deleted: 3,
        };
        let baseline = super::TableWrites {
            inserted: 7,
            updated: 3,
            deleted: 0,
        };
        assert_eq!(
            super::table_write_delta(&current, Some(&baseline)).unwrap(),
            super::TableWrites {
                inserted: 1,
                updated: 2,
                deleted: 3
            }
        );
        assert_eq!(super::table_write_delta(&current, None).unwrap(), current);
        assert_eq!(
            super::table_write_delta(&current, Some(&current)).unwrap(),
            super::TableWrites::default()
        );
        assert!(super::table_write_delta(&baseline, Some(&current)).is_err());
    }

    use super::*;

    #[test]
    fn database_and_vhost_names_embed_the_ownership_marker() {
        let prefix = database_prefix("run-123");
        assert!(prefix.starts_with("vigilo_perf_run_123_"));
        assert!(database_name("run-123", "worker", 1).starts_with(&prefix));
        assert!(format!("perf-{}-sample", safe_name("run-123")).starts_with("perf-run-123-"));
    }

    #[test]
    fn database_names_preserve_uniqueness_after_purpose_truncation() {
        let marker = "1724512345678-12345";
        let purpose = "coordinator.placement-scaling.v1";
        let first = database_name(marker, purpose, 2);
        let second = database_name(marker, purpose, 3);

        assert_ne!(first, second);
        assert!(first.len() <= DATABASE_IDENTIFIER_MAX_BYTES);
        assert!(second.len() <= DATABASE_IDENTIFIER_MAX_BYTES);
        assert!(first.ends_with("_2"));
        assert!(second.ends_with("_3"));
    }

    #[test]
    fn database_names_hash_full_markers_and_bound_maximum_sequences() {
        let shared = "service-test-00000000-0000-7000-8000-";
        let first = database_name(&format!("{shared}000000000001"), "sentinel", u64::MAX);
        let second = database_name(&format!("{shared}000000000002"), "sentinel", u64::MAX);

        assert_ne!(first, second);
        assert!(first.len() <= DATABASE_IDENTIFIER_MAX_BYTES);
        assert!(second.len() <= DATABASE_IDENTIFIER_MAX_BYTES);
        assert!(first.ends_with("_3w5e11264sgsf"));
    }

    #[test]
    fn non_loopback_endpoints_are_refused() {
        assert!(require_loopback_url(&Url::parse("postgres://localhost/db").unwrap()).is_ok());
        assert!(
            require_loopback_url(&Url::parse("postgres://db.example.com/prod").unwrap()).is_err()
        );
    }

    #[test]
    fn cleanup_requires_the_exact_recorded_inventory() {
        let resources = vec![OwnedResource {
            id: "one".into(),
            name: "one".into(),
        }];
        let manifest = ServiceManifest {
            schema_id: "service-topology/v1".into(),
            run_id: "run".into(),
            project: "project".into(),
            compose_file: "compose".into(),
            environment_file: "env".into(),
            postgres_admin_url: "postgres".into(),
            rabbit_management_url: "rabbit".into(),
            agent_url: "agent".into(),
            containers: resources.clone(),
            networks: resources.clone(),
            volumes: resources.clone(),
        };
        assert!(
            validate_cleanup_inventory("run", &manifest, &resources, &resources, &resources)
                .is_ok()
        );
        assert!(
            validate_cleanup_inventory("other", &manifest, &resources, &resources, &resources)
                .is_err()
        );
        assert!(validate_cleanup_inventory("run", &manifest, &[], &resources, &resources).is_err());
    }

    #[test]
    fn byte_and_percent_collectors_parse_docker_output() {
        assert_eq!(parse_memory_usage("12.5MiB / 1GiB"), Some(13_107_200));
        assert_eq!(parse_percent("3.25%"), Some(3.25));
    }

    #[test]
    fn deterministic_agent_body_has_the_requested_size() {
        assert_eq!(agent_body("good reliable response", 1024).len(), 1024);
        assert!(agent_body("long response", 1).len() > 1);
    }

    #[test]
    fn deterministic_agent_serves_requests_and_resets_counters() {
        let agent = DeterministicAgent::start("run-1", "good", 256).unwrap();
        agent
            .configure("changed", 512, Duration::from_millis(1))
            .unwrap();
        let url = Url::parse(agent.url()).unwrap();
        let mut stream =
            TcpStream::connect((url.host_str().unwrap(), url.port().unwrap())).unwrap();
        write!(
            stream,
            "POST {} HTTP/1.1\r\nhost: localhost\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{{}}",
            url.path()
        )
        .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).unwrap();
        assert!(find_bytes(&response, b"HTTP/1.1 200 OK").is_some());
        assert!(find_bytes(&response, b"\"text\":\"changed\"").is_some());

        let snapshot = agent.snapshot();
        assert_eq!(snapshot.requests, 1);
        assert!(snapshot.bytes >= 514);
        assert_eq!(snapshot.peak_concurrency, 1);
        agent.reset().unwrap();
        assert_eq!(agent.snapshot().requests, 0);
    }

    #[test]
    fn deterministic_agent_serves_concurrent_http_clients_without_drops() {
        let agent = DeterministicAgent::start("run-concurrent", "good", 1024).unwrap();
        let client = reqwest::blocking::Client::new();
        let requests = (0..100)
            .map(|_| {
                let client = client.clone();
                let url = agent.url().to_owned();
                thread::spawn(move || {
                    let response = client.post(url).json(&json!({ "input": "case" })).send()?;
                    anyhow::ensure!(response.status().is_success());
                    response.bytes().map(|_| ()).map_err(Into::into)
                })
            })
            .collect::<Vec<_>>();

        let failures = requests
            .into_iter()
            .filter_map(|request| request.join().unwrap().err())
            .map(|error| format!("{error:#}"))
            .collect::<Vec<_>>();
        assert!(
            failures.is_empty(),
            "served {} requests at peak {}; failures:\n{}",
            agent.snapshot().requests,
            agent.snapshot().peak_concurrency,
            failures.join("\n"),
        );
        assert_eq!(agent.snapshot().requests, 100);
    }

    #[test]
    fn collectors_and_name_helpers_cover_boundary_values() {
        assert_eq!(
            collect_container_stats(Path::new("."), &[])
                .unwrap()
                .memory_bytes,
            None
        );
        let mut sampler = ServiceSampler::start(Path::new("."), Vec::new()).unwrap();
        assert_eq!(sampler.finish().unwrap().cpu_percent, None);
        assert_eq!(maximum(Some(2), Some(3)), Some(3));
        assert_eq!(maximum(None, None), None);
        assert_eq!(maximum_f64(Some(2.0), Some(1.0)), Some(2.0));
        assert_eq!(
            parse_content_length(b"Host: local\r\nCONTENT-LENGTH: 12"),
            Some(12)
        );
        assert_eq!(parse_content_length(b"content-length: nope"), None);
        assert_eq!(find_bytes(b"abcabc", b"bca"), Some(1));
        assert_eq!(bounded_text("abc", 3), "abc");
        assert_eq!(bounded_text("abcd", 3), "abc...");
        assert_eq!(bounded_text("aé日", 2), "aé...");
        assert_eq!(parse_size("1KB"), Some(1_000));
        assert_eq!(parse_size("1KiB"), Some(1_024));
        assert_eq!(parse_size("2MB"), Some(2_000_000));
        assert_eq!(parse_size("1GiB"), Some(1_073_741_824));
        assert_eq!(parse_size("12"), None);
        assert_eq!(parse_size("1XB"), None);
        assert_eq!(nonnegative_u64(7).unwrap(), 7);
        assert!(nonnegative_u64(-1).is_err());
        assert_eq!(quoted_identifier("a\"b"), "\"a\"\"b\"");
        assert_eq!(safe_name(" A/b_c.d "), "ab-c-d");
        assert_eq!(truncate_name("abcdef", 3), "abc");
    }

    #[test]
    fn endpoint_and_compose_helpers_preserve_owned_configuration() {
        assert_eq!(
            database_url("postgres://user:secret@localhost:5432/admin", "owned")
                .unwrap()
                .as_str(),
            "postgres://user:secret@localhost:5432/owned"
        );
        assert_eq!(
            redact_url("postgres://user:secret@localhost/db"),
            "postgres://user:%3Credacted%3E@localhost/db"
        );
        assert_eq!(redact_url("not a url"), "<invalid-url>");
        let args = compose_args(Path::new("compose.yml"), Path::new("run.env"), "project");
        assert_eq!(args[0], "compose");
        assert_eq!(args.last().unwrap(), "project");

        let containers = vec![
            OwnedResource {
                id: "1".into(),
                name: "one".into(),
            },
            OwnedResource {
                id: "2".into(),
                name: "two".into(),
            },
        ];
        let network = vec![OwnedResource {
            id: "3".into(),
            name: "net".into(),
        }];
        let volumes = vec![
            OwnedResource {
                id: "4".into(),
                name: "a".into(),
            },
            OwnedResource {
                id: "5".into(),
                name: "b".into(),
            },
        ];
        let mut containers = containers;
        containers.push(OwnedResource {
            id: "ceph".into(),
            name: "ceph".into(),
        });
        let mut volumes = volumes;
        volumes.push(OwnedResource {
            id: "ceph-volume".into(),
            name: "ceph-data".into(),
        });
        assert!(validate_inventory("run", &containers, &network, &volumes).is_ok());
        assert!(validate_inventory("run", &[], &network, &volumes).is_err());
        assert!(validate_partial_inventory("run", &[], &[], &[]).is_ok());
        assert!(validate_partial_inventory("run", &containers[..1], &network, &volumes).is_ok());
        assert!(
            validate_partial_inventory(
                "run",
                &[containers.clone(), containers].concat(),
                &network,
                &volumes,
            )
            .is_err()
        );
    }

    #[test]
    fn bounded_retry_accepts_a_later_success() {
        let mut attempts = 0;
        retry_until_success("fixture", Duration::from_secs(1), Duration::ZERO, || {
            attempts += 1;
            if attempts < 3 {
                bail!("fixture is starting");
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(attempts, 3);
    }

    #[test]
    fn bounded_retry_reports_the_last_failure_at_its_deadline() {
        let mut attempts = 0;
        let error = retry_until_success("fixture", Duration::ZERO, Duration::ZERO, || {
            attempts += 1;
            bail!("connection reset")
        })
        .unwrap_err();
        let message = format!("{error:#}");
        assert_eq!(attempts, 1);
        assert!(message.contains("fixture did not succeed within 0 ms"));
        assert!(message.contains("connection reset"));
    }

    #[test]
    fn worker_queue_metrics_exclude_unrelated_queues_and_sum_exact_counters() {
        let queues = vec![
            json!({
                "name": "perf.worker",
                "messages_ready": 2,
                "messages_unacknowledged": 1,
                "message_stats": {"deliver_get": 4}
            }),
            json!({
                "name": "vigilo.worker",
                "messages_ready": 3,
                "messages_unacknowledged": 0,
                "message_stats": {"deliver_get": 5}
            }),
            json!({
                "name": "unrelated",
                "messages_ready": 100,
                "messages_unacknowledged": 100,
                "message_stats": {"deliver_get": 100}
            }),
        ];
        assert_eq!(worker_queue_metrics(&queues), (5, 1, 9));
        assert_eq!(
            worker_queue_metrics(&[json!({"name": "perf.worker"})]),
            (0, 0, 0)
        );
    }

    #[test]
    fn wal_lsn_query_binds_the_rust_string_as_text_before_casting() {
        use postgres::types::{
            ToSql,
            Type,
        };

        assert!(<String as ToSql>::accepts(&Type::TEXT));
        assert!(!<String as ToSql>::accepts(&Type::PG_LSN));
        assert!(WAL_BYTES_QUERY.contains("$1::text::pg_lsn"));
    }
}
