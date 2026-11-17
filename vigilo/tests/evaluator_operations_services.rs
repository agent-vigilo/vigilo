//! Public-library registry and HTTP testing against disposable PostgreSQL and
//! bounded loopback servers. Real Wasm uses existing fixtures, never private APIs.
use std::{
    sync::{
        Arc,
        Mutex,
        atomic::{
            AtomicBool,
            AtomicUsize,
            Ordering,
        },
    },
    time::Duration,
};

use anyhow::{
    Result,
    ensure,
};
use serde_json::{
    Value,
    json,
};
use sqlx::{
    ConnectOptions,
    PgPool,
};
use tokio::io::{
    AsyncReadExt,
    AsyncWriteExt,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use vigilo::{
    evaluator_http::{
        Action,
        Rule,
        transport::Credentials,
    },
    operations::{
        self,
        evaluators::*,
    },
};

fn fixture(name: &str, artifact: &str) -> Result<Vec<u8>> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../target/wasm32-wasip2/release")
        .join(artifact);
    let mut wasm = std::fs::read(&path)?;
    fn leb(mut n: usize) -> Vec<u8> {
        let mut b = Vec::new();
        loop {
            let mut v = (n & 127) as u8;
            n >>= 7;
            if n > 0 {
                v |= 128;
            }
            b.push(v);
            if n == 0 {
                return b;
            }
        }
    }
    let section = "vigilo.package";
    let mut custom = leb(section.len());
    custom.extend_from_slice(section.as_bytes());
    custom.extend_from_slice(
        json!({"name":name,"version":"0.1.0"})
            .to_string()
            .as_bytes(),
    );
    wasm.push(0);
    wasm.extend(leb(custom.len()));
    wasm.extend(custom);
    let manifest = format!(
        "format='vigilo/evaluator-package/v1'\n[package]\nname='{name}'\nversion='0.1.0'\n[artifact]\npath='evaluator.wasm'\nblake3='{}'\n[wit]\npackage='vigilo:evaluator'\nworld='evaluator-world'\ninterface='evaluator'\nversion='1.0.0'\ncontract_hash='{}'\n",
        blake3::hash(&wasm).to_hex(),
        blake3::hash(include_bytes!("../wit/evaluator/v1.0.0/evaluator.wit")).to_hex()
    );
    let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    let mut archive = tar::Builder::new(encoder);
    for (name, bytes) in [
        ("Vigilo.toml", manifest.as_bytes()),
        ("evaluator.wasm", wasm.as_slice()),
    ] {
        let mut header = tar::Header::new_ustar();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        archive.append_data(&mut header, name, bytes)?;
    }
    Ok(archive.into_inner()?.finish()?)
}
fn input(requests: Value) -> TestInput {
    TestInput::from_json(&json!({"run_id":"fixture","execution_id":"fixture","attempt_id":"fixture","case":{"id":"case","task_type":"text","input":{}},"actual":{},"evaluator_config":requests}).to_string()).unwrap()
}
struct HeaderCredential {
    calls: Arc<AtomicUsize>,
    value: &'static str,
}
#[async_trait::async_trait]
impl Credentials for HeaderCredential {
    async fn headers(&self, _: &str, _: &str) -> Result<reqwest::header::HeaderMap, &'static str> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut h = reqwest::header::HeaderMap::new();
        h.insert(
            "authorization",
            reqwest::header::HeaderValue::from_static(self.value),
        );
        Ok(h)
    }
}
struct Host {
    database: PgPool,
    role: Mutex<Option<String>>,
    uri: String,
    allowed: Mutex<Option<(operations::UserContext, Uuid, String)>>,
    seen: Mutex<Vec<TestRequest>>,
    denied: AtomicBool,
    pending: AtomicBool,
    credentials: Arc<AtomicUsize>,
}
impl TestResolver for Host {
    fn resolve(
        &self,
        request: TestRequest,
    ) -> operations::resources::Resolution<'_, TestAuthority> {
        Box::pin(async move {
            let role = self
                .role
                .lock()
                .unwrap()
                .clone()
                .ok_or(operations::resources::Error::Invalid)?;
            let active:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename=$1 AND state='idle in transaction'").bind(role).fetch_one(&self.database).await.map_err(|_|operations::resources::Error::Unavailable)?;
            assert_eq!(
                active, 0,
                "registry transaction spanned external authority resolution"
            );
            self.seen.lock().unwrap().push(request.clone());
            if self.pending.load(Ordering::SeqCst) {
                return std::future::pending().await;
            }
            if self.denied.load(Ordering::SeqCst) {
                return Err(operations::resources::Error::Denied);
            }
            let allowed = self
                .allowed
                .lock()
                .unwrap()
                .clone()
                .ok_or(operations::resources::Error::Denied)?;
            if request.caller() != Some(allowed.0)
                || request.evaluator_id() != allowed.1
                || request.artifact_digest() != allowed.2
            {
                return Err(operations::resources::Error::Denied);
            }
            TestAuthority::new(
                &request,
                vec![Rule {
                    uri: self.uri.clone(),
                    method: "POST".into(),
                    action: Action::Allow,
                    networks: vec!["127.0.0.0/8".into()],
                }],
                Arc::new(HeaderCredential {
                    calls: self.credentials.clone(),
                    value: "Bearer host-only",
                }),
            )
        })
    }
}
async fn judge() -> Result<(String, tokio::task::JoinHandle<Result<String>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let uri = format!("http://{}/judge", listener.local_addr()?);
    let task = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(30),async move{
  let(mut socket,_)=listener.accept().await?;let mut bytes=Vec::new();loop{let mut b=[0;1024];let n=socket.read(&mut b).await?;ensure!(n>0,"incomplete request");bytes.extend_from_slice(&b[..n]);ensure!(bytes.len()<32768,"request exceeds fixture bound");if let Some(end)=bytes.windows(4).position(|b|b==b"\r\n\r\n"){let h=String::from_utf8_lossy(&bytes[..end]);let len:usize=h.lines().find_map(|l|l.to_ascii_lowercase().strip_prefix("content-length:").map(str::trim).map(str::to_owned)).unwrap_or("0".into()).parse()?;if bytes.len()>=end+4+len{break;}}}
  socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nContent-Type: text/plain\r\nX-Secret: hidden\r\nConnection: close\r\n\r\njudge").await?;Ok(String::from_utf8(bytes)?)
 }).await?
    });
    Ok((uri, task))
}

#[sqlx::test(migrations = "./migrations")]
async fn public_registry_preserves_artifacts_and_injects_only_verified_test_authority(
    pool: PgPool,
) -> Result<()> {
    use futures_util::FutureExt;
    let (uri, server) = judge().await?;
    let host = Arc::new(Host {
        database: pool.clone(),
        role: Mutex::new(None),
        uri: uri.clone(),
        allowed: Mutex::new(None),
        seen: Mutex::new(Vec::new()),
        denied: AtomicBool::new(false),
        pending: AtomicBool::new(false),
        credentials: Arc::new(AtomicUsize::new(0)),
    });
    let cancel = CancellationToken::new();
    let operator = operations::Client::new(operations::Config::new(
        pool.connect_options().to_url_lossy().to_string(),
    ))?;
    let offline = operator.evaluators("fixture", Options::default())?;
    let package = fixture("http-request", "http_request_fixture.wasm")?;
    let prepared = offline.prepare_package(package.as_slice(), &cancel).await?;
    let hash = prepared.content_hash().to_owned();
    let publication = offline.publish(prepared).await?;
    let id = EvaluatorId::parse("fixture/http-request:0.1.0")?;
    let replay = offline
        .publish(offline.prepare_package(package.as_slice(), &cancel).await?)
        .await?;
    ensure!(
        replay.reused && replay.evaluator.id == publication.evaluator.id,
        "idempotent replay changed artifact"
    );
    let mut forged = package.clone();
    let last = forged.len() - 1;
    forged[last] ^= 1;
    ensure!(
        offline
            .prepare_package(forged.as_slice(), &cancel)
            .await
            .is_err(),
        "corrupt package accepted"
    );
    let denied = offline
        .test(&id, input(json!([{"uri":uri}])), &cancel)
        .await?;
    ensure!(
        denied.output.metadata[0]["error"].is_string(),
        "offline evaluator gained HTTP"
    );
    let official = operator.evaluators("vigilo", Options::default())?;
    let official_package = fixture("sentiment-basic-en", "sentiment_basic_en.wasm")?;
    let published = official
        .publish(
            official
                .prepare_package(official_package.as_slice(), &cancel)
                .await?,
        )
        .await?;
    ensure!(
        published.evaluator.namespace == "vigilo"
            && published.evaluator.id != publication.evaluator.id,
        "publisher namespaces were rewritten"
    );
    let role = format!("registry_host_{}", Uuid::now_v7().simple());
    *host.role.lock().unwrap() = Some(role.clone());
    let actor = operations::UserContext::new(Uuid::now_v7(), Uuid::now_v7()).unwrap();
    let other = operations::UserContext::new(Uuid::now_v7(), Uuid::now_v7()).unwrap();
    sqlx::raw_sql(&format!("CREATE ROLE {role} LOGIN PASSWORD 'fixture';GRANT USAGE ON SCHEMA public TO {role};ALTER TABLE evaluators ENABLE ROW LEVEL SECURITY;GRANT SELECT,INSERT,UPDATE ON evaluators TO {role};CREATE POLICY caller ON evaluators TO {role} USING(current_setting('vigilo.actor_id',true)='{}' AND current_setting('vigilo.tenant_id',true)='{}')",actor.actor_id(),actor.tenant_id())).execute(&pool).await?;
    let mut url = pool
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
    let users = operations::UserClient::new(operations::UserConfig {
        control: config,
        database_role: role.clone(),
        execution_databases: Default::default(),
    })?;
    let service = users
        .evaluators(
            "fixture",
            Options {
                test_authority: Some(host.clone()),
                ..Default::default()
            },
        )
        .await?;
    *host.allowed.lock().unwrap() = Some((actor, publication.evaluator.id, hash.clone()));
    let client = service.user(actor);
    let result = std::panic::AssertUnwindSafe(async {
        ensure!(
            service.user(other).inspect(&id).await.is_err(),
            "foreign caller read artifact"
        );
        ensure!(
            service
                .user(other)
                .test(&id, input(json!([])), &cancel)
                .await
                .is_err(),
            "foreign caller executed artifact"
        );
        ensure!(
            host.seen.lock().unwrap().is_empty(),
            "unauthorized caller reached host"
        );
        sqlx::raw_sql(&format!("ALTER POLICY caller ON evaluators USING ((current_setting('vigilo.actor_id',true)='{}' AND current_setting('vigilo.tenant_id',true)='{}') OR (current_setting('vigilo.actor_id',true)='{}' AND current_setting('vigilo.tenant_id',true)='{}'))",actor.actor_id(),actor.tenant_id(),other.actor_id(),other.tenant_id())).execute(&pool).await?;
        let shared=service.user(other).inspect(&id).await?;
        ensure!(shared.id==publication.evaluator.id && shared.content_hash==hash,"shared reference copied or relabelled artifact");
        ensure!(service.user(other).test(&id,input(json!([])),&cancel).await.is_err(),"shared artifact inherited another caller's HTTP grant");
        // Reset observed denied callback before asserting only granted caller work.
        host.seen.lock().unwrap().clear();
        sqlx::raw_sql(&format!("ALTER POLICY caller ON evaluators USING (current_setting('vigilo.actor_id',true)='{}' AND current_setting('vigilo.tenant_id',true)='{}')",actor.actor_id(),actor.tenant_id())).execute(&pool).await?;
        let before=host.seen.lock().unwrap().len();
        sqlx::query("UPDATE evaluators SET content_hash=$2 WHERE id=$1").bind(publication.evaluator.id).bind("f".repeat(64)).execute(&pool).await?;
        ensure!(client.test(&id,input(json!([])),&cancel).await.is_err(),"corrupt stored artifact was executed");
        sqlx::query("UPDATE evaluators SET content_hash=$2,interface_version='9.0.0' WHERE id=$1").bind(publication.evaluator.id).bind(&hash).execute(&pool).await?;
        ensure!(client.test(&id,input(json!([])),&cancel).await.is_err(),"unsupported ABI was executed");
        ensure!(host.seen.lock().unwrap().len()==before,"unverified artifact reached host authority");
        sqlx::query("UPDATE evaluators SET interface_version='1.0.0' WHERE id=$1").bind(publication.evaluator.id).execute(&pool).await?;
        let held=client.prepare_package(package.as_slice(),&cancel).await?;
        let official_caller=service.for_namespace("vigilo")?.user(actor);
        ensure!(tokio::time::timeout(Duration::from_millis(20),official_caller.prepare_package(official_package.as_slice(),&cancel)).await.is_err(),"namespace factory lost shared preparation admission");
        ensure!(client.publish(held).await?.reused,"scoped replay changed identity");
        ensure!(official_caller.publish(official_caller.prepare_package(official_package.as_slice(),&cancel).await?).await?.reused,"shared namespace import changed existing artifact");
        let tested = client
            .test(&id, input(json!([{"uri":uri}])), &cancel)
            .await?;
        ensure!(
            tested.output.metadata[0]["status"] == 200,
            "judge response missing: {:?}",
            tested.output.metadata
        );
        ensure!(
            tested.output.metadata[0]["headers"]
                .as_array()
                .unwrap()
                .iter()
                .all(|h| h[0] != "x-secret"),
            "unsafe response header exposed"
        );
        ensure!(
            host.credentials.load(Ordering::SeqCst) == 1,
            "credential lookup count changed"
        );
        host.denied.store(true, Ordering::SeqCst);
        ensure!(
            client.test(&id, input(json!([])), &cancel).await.is_err(),
            "denied authority fell back"
        );
        host.denied.store(false, Ordering::SeqCst);
        let blocked = client
            .test(
                &id,
                input(json!([{"uri":"http://127.0.0.1:1/other"}])),
                &cancel,
            )
            .await?;
        ensure!(
            blocked.output.metadata[0]["error"].is_string(),
            "ungranted destination allowed"
        );
        ensure!(
            host.credentials.load(Ordering::SeqCst) == 1,
            "denied destination resolved credentials"
        );
        client.set_state(&id, State::Disabled, None).await?;
        let calls = host.seen.lock().unwrap().len();
        ensure!(
            client.test(&id, input(json!([])), &cancel).await.is_err(),
            "disabled evaluator tested"
        );
        ensure!(
            host.seen.lock().unwrap().len() == calls,
            "disabled artifact reached host"
        );
        client.set_state(&id, State::Active, None).await?;
        host.pending.store(true, Ordering::SeqCst);
        let token = CancellationToken::new();
        let task = tokio::spawn({
            let c = client.clone();
            let id = id.clone();
            let token = token.clone();
            async move { c.test(&id, input(json!([])), &token).await }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if host.seen.lock().unwrap().len() > calls {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        token.cancel();
        ensure!(
            matches!(task.await?, Err(operations::Error::Cancelled)),
            "pending host request did not cancel"
        );
        host.pending.store(false, Ordering::SeqCst);
        let final_result = client.test(&id, input(json!([])), &cancel).await?;
        ensure!(
            final_result.output.metadata == json!([]),
            "cancelled invocation contaminated later test"
        );
        let requests = host.seen.lock().unwrap();
        let ids: std::collections::BTreeSet<_> =
            requests.iter().map(TestRequest::test_id).collect();
        ensure!(ids.len() == requests.len(), "test IDs reused");
        ensure!(
            requests.iter().all(|r| r.caller() == Some(actor)
                && r.evaluator_id() == publication.evaluator.id
                && r.artifact_digest() == hash),
            "authority identity changed"
        );
        Ok::<_, anyhow::Error>(())
    })
    .catch_unwind()
    .await;
    drop(client);
    drop(service);
    drop(users);
    sqlx::raw_sql(&format!("DROP OWNED BY {role};DROP ROLE {role}"))
        .execute(&pool)
        .await?;
    if result.as_ref().is_err() || result.as_ref().is_ok_and(|r| r.is_err()) {
        server.abort();
        let _ = server.await;
    } else {
        let received = server.await??;
        ensure!(
            received
                .to_ascii_lowercase()
                .contains("authorization: bearer host-only"),
            "host credential not used"
        );
    }
    match result {
        Ok(r) => r,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

async fn cli(pool: &PgPool, args: &[&str]) -> Result<Value> {
    let out = tokio::time::timeout(
        Duration::from_secs(30),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_vigilo"))
            .arg("-q")
            .args(args)
            .env(
                "DATABASE_URL",
                pool.connect_options().to_url_lossy().as_str(),
            )
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    ensure!(
        out.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    if out.stdout.is_empty() {
        Ok(Value::Null)
    } else {
        Ok(serde_json::from_slice(&out.stdout)?)
    }
}
#[sqlx::test(migrations = "./migrations")]
async fn cli_payloads_and_large_search_limits_match_library_metadata(pool: PgPool) -> Result<()> {
    let root = operations::Client::new(operations::Config::new(
        pool.connect_options().to_url_lossy().to_string(),
    ))?;
    let client = root.evaluators("fixture", Options::default())?;
    let cancel = CancellationToken::new();
    let package = fixture("http-request", "http_request_fixture.wasm")?;
    let published = client
        .publish(client.prepare_package(package.as_slice(), &cancel).await?)
        .await?;
    let id = EvaluatorId::parse("fixture/http-request:0.1.0")?;
    let show = cli(&pool, &["evaluator", "show", &id.to_string()]).await?;
    ensure!(
        show["data"] == serde_json::to_value(&published.evaluator)?,
        "inspection JSON changed"
    );
    let expected = client.test(&id, input(json!([])), &cancel).await?;
    let raw=json!({"run_id":"fixture","execution_id":"fixture","attempt_id":"fixture","case":{"id":"case","task_type":"text","input":{}},"actual":{},"evaluator_config":[]}).to_string();
    let tested = cli(
        &pool,
        &["evaluator", "test", &id.to_string(), "--input", &raw],
    )
    .await?;
    ensure!(
        tested["data"] == serde_json::to_value(expected)?,
        "test JSON changed"
    );
    ensure!(
        cli(
            &pool,
            &[
                "evaluator",
                "set-state",
                &id.to_string(),
                "yanked",
                "--state-reason",
                "withdrawn"
            ]
        )
        .await?
        .is_null(),
        "state command gained a payload"
    );
    ensure!(
        client.inspect(&id).await?.state == State::Yanked,
        "state command skipped shared workflow"
    );
    for i in 0..20 {
        sqlx::query("INSERT INTO evaluators(namespace,name,version,content_hash,wasm_bytes,wasm_size_bytes,interface_name,interface_version,wit_world,abi_contract_hash,abi_adapter,runtime,runtime_version,runtime_fingerprint,metadata) VALUES('bulk',$1,'0.1.0',$2,''::bytea,0,'vigilo:evaluator/evaluator','1.0.0','evaluator-world',$3,'vigilo-evaluator-v1@1','wasmtime','fixture','fixture',jsonb_build_object('large',repeat('x',524288)))")
   .bind(format!("item{i:02}")).bind(format!("{i:064x}")).bind("a".repeat(64)).execute(&pool).await?;
    }
    let search = cli(
        &pool,
        &[
            "evaluator",
            "search",
            "--namespace",
            "bulk",
            "--limit",
            "20",
        ],
    )
    .await?;
    ensure!(
        search["meta"] == json!({"namespace":"bulk","query":null,"limit":20,"count":20}),
        "large search truncated its CLI limit"
    );
    let values = search["data"].as_array().unwrap();
    ensure!(values.len() == 20, "large search lost results");
    let expected_keys: std::collections::BTreeSet<_> = [
        "namespace",
        "name",
        "version",
        "description",
        "tags",
        "metadata",
        "state",
        "state_reason",
    ]
    .into_iter()
    .collect();
    for (i, value) in values.iter().enumerate() {
        ensure!(
            value["name"] == format!("item{i:02}"),
            "search order changed"
        );
        let keys: std::collections::BTreeSet<_> = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        ensure!(
            keys == expected_keys,
            "search leaked descriptor-only fields"
        );
    }
    Ok(())
}
