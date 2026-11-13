//! Real Wasm and provider integration against bounded loopback servers.
use serde_json::{
    Value,
    json,
};
use tokio::{
    io::{
        AsyncReadExt,
        AsyncWriteExt,
    },
    net::TcpListener,
};

use super::*;
use crate::{
    context::wasm::{
        Config as WasmConfig,
        Wasm,
    },
    contracts::evaluator::EvaluatorInput,
    evaluator_abi::{
        PreparedEvaluator,
        current_identity,
    },
};

fn execution() -> Execution {
    Execution {
        resources: None,
        run_id: Uuid::new_v4(),
        execution_id: Uuid::new_v4(),
        attempt_id: Uuid::new_v4(),
    }
}
fn policy(id: Uuid, uri: &str, allow: bool) -> Arc<Snapshot> {
    Arc::new(
        Snapshot::new(
            id,
            vec![vigilo::evaluator_http::Rule {
                uri: uri.into(),
                method: "POST".into(),
                action: if allow {
                    vigilo::evaluator_http::Action::Allow
                } else {
                    vigilo::evaluator_http::Action::Deny
                },
                networks: vec!["127.0.0.0/8".into()],
            }],
        )
        .unwrap(),
    )
}
async fn read_request(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    loop {
        let mut buf = [0; 1024];
        let n = socket.read(&mut buf).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buf[..n]);
        assert!(bytes.len() < 32768);
        if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..end]).to_lowercase();
            let len: usize = headers
                .lines()
                .find_map(|l| l.strip_prefix("content-length: "))
                .unwrap_or("0")
                .parse()
                .unwrap();
            if bytes.len() >= end + 4 + len {
                return bytes;
            }
        }
    }
}
async fn execute(invocation: Arc<Invocation>, policy: Arc<Snapshot>, requests: Value) -> Value {
    tokio::time::timeout(Duration::from_secs(20), tokio::task::spawn_blocking(move || {
        let runtime = Wasm::new(WasmConfig { timeout_ms: 0, ..Default::default() }).unwrap();
        let bytes = std::fs::read(Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/wasm32-wasip2/release/http_request_fixture.wasm")).expect("build http-request-fixture for wasm32-wasip2 first");
        let component = PreparedEvaluator::compile(&runtime, &bytes, &current_identity()).unwrap();
        let mut input: EvaluatorInput = serde_json::from_value(json!({"run_id":"run", "execution_id":"execution", "attempt_id":"attempt", "case":{"id":"case", "task_type":"text", "input":"hello"}, "actual":{}, "evaluator_config":requests})).unwrap();
        input.http_policy = Some(policy);
        input.http_invocation = Some(invocation);
        runtime.test_evaluator_component(&component, input).unwrap().metadata
    })).await.unwrap().unwrap()
}

#[tokio::test]
async fn wasm_returns_http_errors_and_preserves_sequential_budget() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}/judge", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_request(&mut socket).await;
        socket.write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 5\r\nSet-Cookie: secret\r\nConnection: close\r\n\r\nerror").await.unwrap();
        request
    });
    let policy = policy(Uuid::new_v4(), &uri, true);
    let service = Service::configured(Config {
        requests: 1,
        ..Default::default()
    })
    .unwrap();
    let (invocation, _guard) = service
        .invocation(policy.clone(), Arc::new(Provider::default()), execution())
        .unwrap();
    let output = execute(invocation, policy, json!([{"uri":uri},{"uri":uri}])).await;
    assert_eq!(output[0]["status"], 503);
    assert_eq!(output[0]["body"], json!(b"error".to_vec()));
    assert_eq!(output[0]["headers"], json!([]));
    assert_eq!(output[1]["error"], "evaluator HTTP budget exhausted");
    server.await.unwrap();
}

#[tokio::test]
async fn wasm_policy_denial_and_cancelled_invocations_never_connect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}/judge", listener.local_addr().unwrap());
    for deny in [true, false] {
        let policy = policy(Uuid::new_v4(), &uri, !deny);
        let (invocation, guard) = Service::default()
            .invocation(policy.clone(), Arc::new(Provider::default()), execution())
            .unwrap();
        if deny {
            let output = execute(invocation, policy, json!([{"uri":uri}])).await;
            assert_eq!(output[0]["error"], "evaluator HTTP explicitly denied");
            drop(guard);
        } else {
            drop(guard);
            let output = execute(invocation, policy, json!([{"uri":uri}])).await;
            assert_eq!(output[0]["error"], "evaluator HTTP invocation cancelled");
        }
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn wasm_timeout_exhausts_budget_and_releases_shared_permit() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}/judge", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), socket.read_to_end(&mut bytes))
            .await
            .unwrap()
            .unwrap();
    });
    let policy = policy(Uuid::new_v4(), &uri, true);
    let service = Service::configured(Config {
        total_time_ms: 100,
        concurrency: 1,
        ..Default::default()
    })
    .unwrap();
    let (invocation, _guard) = service
        .invocation(policy.clone(), Arc::new(Provider::default()), execution())
        .unwrap();
    let output = execute(invocation, policy, json!([{"uri":uri},{"uri":uri}])).await;
    assert_eq!(output[0]["error"], "evaluator HTTP deadline exceeded");
    assert_eq!(output[1]["error"], "evaluator HTTP budget exhausted");
    assert_eq!(service.permits.available_permits(), 1);
    server.await.unwrap();
}

#[tokio::test]
async fn wasm_large_response_is_an_error_not_partial_evidence() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}/judge", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\n123456")
            .await
            .unwrap();
    });
    let policy = policy(Uuid::new_v4(), &uri, true);
    let service = Service::configured(Config {
        response_bytes: 5,
        ..Default::default()
    })
    .unwrap();
    let (invocation, _guard) = service
        .invocation(policy.clone(), Arc::new(Provider::default()), execution())
        .unwrap();
    let output = execute(invocation, policy, json!([{"uri":uri}])).await;
    assert_eq!(output[0]["error"], "evaluator HTTP response limit exceeded");
    server.await.unwrap();
}

struct Files(std::path::PathBuf);
impl Files {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("vigilo-evaluator-http-{}", Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Files {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn local_provider(uri: &str, secret: &Path) -> Arc<Provider> {
    let url = reqwest::Url::parse(uri).unwrap();
    Arc::new(Provider::new(serde_json::from_value(json!({"version":1,"allow_http_loopback":true,"provider":{"type":"local","bindings":{"judge":{"allowed_origin":url.origin().ascii_serialization(),"header":"x-judge-key","source":{"type":"file","path":secret}}}}})).unwrap()).unwrap())
}
fn credential_service(id: Uuid, uri: &str) -> Service {
    Service::configured(Config {
        bindings: vec![Binding {
            evaluator_id: id,
            method: "POST".into(),
            uri: uri.into(),
            credential: "judge".into(),
        }],
        ..Default::default()
    })
    .unwrap()
}

#[tokio::test]
async fn wasm_host_injects_credentials_caches_per_invocation_and_rejects_override() {
    let files = Files::new();
    let secret = files.0.join("secret");
    std::fs::write(&secret, "first-secret").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}/judge", listener.local_addr().unwrap());
    let changed_secret = secret.clone();
    let server = tokio::spawn(async move {
        for expected in ["first-secret", "first-secret", "second-secret"] {
            let (mut socket, _) = tokio::time::timeout(Duration::from_secs(20), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let request = String::from_utf8(read_request(&mut socket).await).unwrap();
            assert!(request.contains(&format!("x-judge-key: {expected}\r\n")));
            std::fs::write(&changed_secret, "second-secret").unwrap();
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nX-Private: first-secret\r\nConnection: close\r\n\r\nok").await.unwrap();
        }
    });
    let policy = policy(Uuid::new_v4(), &uri, true);
    let service = credential_service(policy.evaluator_id(), &uri);
    let provider = local_provider(&uri, &secret);
    let (invocation, _guard) = service
        .invocation(policy.clone(), provider.clone(), execution())
        .unwrap();
    let output = execute(
        invocation,
        policy.clone(),
        json!([{"uri":uri},{"uri":uri},{"uri":uri,"headers":[["x-judge-key","forged"]]}]),
    )
    .await;
    assert_eq!(output[0]["status"], 200);
    assert_eq!(output[1]["status"], 200);
    assert_eq!(
        output[2]["error"],
        "evaluator overrides host-managed header"
    );
    assert!(!output.to_string().contains("secret"));
    let (invocation, _guard) = service
        .invocation(policy.clone(), provider, execution())
        .unwrap();
    assert_eq!(
        execute(invocation, policy, json!([{"uri":uri}])).await[0]["status"],
        200
    );
    server.await.unwrap();
}

#[tokio::test]
async fn wasm_missing_credential_fails_closed_and_wrong_evaluator_has_no_binding() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}/judge", listener.local_addr().unwrap());
    let policy = policy(Uuid::new_v4(), &uri, true);
    let service = credential_service(policy.evaluator_id(), &uri);
    let (invocation, _guard) = service
        .invocation(policy.clone(), Arc::new(Provider::default()), execution())
        .unwrap();
    assert_eq!(
        execute(invocation, policy, json!([{"uri":uri}])).await[0]["error"],
        "evaluator credential resolution failed"
    );
    let foreign = policy_fn(Uuid::new_v4(), &uri);
    let (invocation, _guard) = service
        .invocation(foreign, Arc::new(Provider::default()), execution())
        .unwrap();
    assert!(
        invocation
            .resolver
            .headers("POST", &uri)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        invocation
            .resolver
            .headers("POST", &format!("{uri}/different"))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}
fn policy_fn(id: Uuid, uri: &str) -> Arc<Snapshot> {
    policy(id, uri, true)
}

#[tokio::test]
async fn service_v1_receives_evaluator_authority_and_expired_cache_refresh_fails_closed() {
    let files = Files::new();
    let execution = execution();
    let secret = files.0.join("token");
    std::fs::write(&secret, "service-secret").unwrap();
    std::fs::write(
        files.0.join(execution.run_id.to_string()),
        "execution-grant",
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let policy = policy(Uuid::new_v4(), "https://judge.example.test/judge", true);
    let expected_id = policy.evaluator_id();
    let expected_revision = policy.revision().to_owned();
    let server = tokio::spawn(async move {
        for attempt in 0..2 {
            let (mut socket, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let request = read_request(&mut socket).await;
            let end = request.windows(4).position(|b| b == b"\r\n\r\n").unwrap();
            let body: Value = serde_json::from_slice(&request[end + 4..]).unwrap();
            assert_eq!(body["version"], 1);
            assert_eq!(
                body["authority"],
                json!({"kind":"evaluator","evaluator_id":expected_id,"policy_revision":expected_revision})
            );
            assert_eq!(body["attempt_id"], execution.attempt_id.to_string());
            assert_eq!(body["credential"], "judge");
            if attempt == 0 {
                let body = json!({"version":1,"authority":body["authority"],"allowed_origin":"https://judge.example.test","header":"x-judge-key","value":"resolved-secret","expires_at":(chrono::Utc::now()+chrono::Duration::minutes(1)).to_rfc3339()}).to_string();
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            } else {
                socket
                    .write_all(
                        b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await
                    .unwrap();
            }
        }
    });
    let provider = Arc::new(Provider::new(serde_json::from_value(json!({"version":1,"allow_http_loopback":true,"provider":{"type":"service","endpoint":endpoint,"token":{"type":"file","path":secret},"grants_directory":files.0}})).unwrap()).unwrap());
    let service = credential_service(expected_id, "https://judge.example.test/judge");
    let (invocation, _guard) = service.invocation(policy, provider, execution).unwrap();
    let headers = invocation
        .resolver
        .headers("POST", "https://judge.example.test/judge")
        .await
        .unwrap();
    assert_eq!(headers["x-judge-key"], "resolved-secret");
    assert_eq!(
        invocation
            .resolver
            .headers("POST", "https://judge.example.test/judge")
            .await
            .unwrap()["x-judge-key"],
        "resolved-secret"
    );
    // Expire the cached value deterministically, without racing a wall clock.
    invocation
        .resolver
        .cache
        .lock()
        .await
        .get_mut(&0)
        .unwrap()
        .expire_for_test();
    assert_eq!(
        invocation
            .resolver
            .headers("POST", "https://judge.example.test/judge")
            .await
            .err(),
        Some("evaluator credential resolution failed")
    );
    assert!(invocation.resolver.cache.lock().await.is_empty());
    server.await.unwrap();
}

#[tokio::test]
async fn cancellation_interrupts_wasm_host_io_and_releases_permit() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}/judge", listener.local_addr().unwrap());
    let policy = policy(Uuid::new_v4(), &uri, true);
    let service = Service::configured(Config {
        concurrency: 1,
        ..Default::default()
    })
    .unwrap();
    let (invocation, guard) = service
        .invocation(policy.clone(), Arc::new(Provider::default()), execution())
        .unwrap();
    let work = tokio::spawn(execute(invocation, policy, json!([{"uri":uri}])));
    let (mut socket, _) = tokio::time::timeout(Duration::from_secs(20), listener.accept())
        .await
        .unwrap()
        .unwrap();
    read_request(&mut socket).await;
    drop(guard);
    let output = tokio::time::timeout(Duration::from_secs(5), work)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output[0]["error"], "evaluator HTTP invocation cancelled");
    assert_eq!(service.permits.available_permits(), 1);
    let mut remaining = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), socket.read_to_end(&mut remaining))
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn binding_views_isolate_same_evaluator_credentials_and_share_process_limits() {
    use vigilo::evaluator_http::transport::BlockingTransport;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let uri = format!("{origin}/judge");
    let id = Uuid::new_v4();
    let policy = policy(id, &uri, true);
    let directory = tempfile::tempdir().unwrap();
    let mut local = BTreeMap::new();
    for name in ["alpha", "beta"] {
        let path = directory.path().join(name);
        tokio::fs::write(&path, format!("{name}-fixture"))
            .await
            .unwrap();
        local.insert(
            name.into(),
            credentials::Binding {
                source: credentials::Source::File { path },
                allowed_origin: origin.clone(),
                header: "authorization".into(),
                prefix: "Bearer ".into(),
            },
        );
    }
    let provider = Arc::new(
        Provider::new(credentials::Config {
            version: 1,
            allow_http_loopback: true,
            provider: credentials::ProviderConfig::Local { bindings: local },
        })
        .unwrap(),
    );
    let base = Service::configured(Config {
        concurrency: 1,
        requests: 1,
        request_timeout_ms: 5000,
        total_time_ms: 10000,
        ..Default::default()
    })
    .unwrap();
    let view = |name| {
        Service::from_bindings_json(
            &json!([{"evaluator_id":id,"method":"POST","uri":uri,"credential":name}]).to_string(),
        )
        .unwrap()
        .bind_to(&base)
    };
    let (alpha, _alpha_guard) = view("alpha")
        .invocation(policy.clone(), provider.clone(), execution())
        .unwrap();
    let (beta, _beta_guard) = view("beta")
        .invocation(policy, provider, execution())
        .unwrap();
    let request = || Request {
        method: "POST".into(),
        uri: uri.clone(),
        headers: vec![],
        body: vec![],
        timeout: None,
    };
    let a = alpha.clone();
    let ar = request();
    let work_a = tokio::task::spawn_blocking(move || a.send(ar));
    let (mut first, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let headers = String::from_utf8(read_request(&mut first).await)
        .unwrap()
        .to_ascii_lowercase();
    assert!(headers.contains("authorization: bearer alpha-fixture"));
    let (started, ready) = tokio::sync::oneshot::channel();
    let br = request();
    let work_b = tokio::task::spawn_blocking(move || {
        let _ = started.send(());
        beta.send(br)
    });
    ready.await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err(),
        "binding view multiplied the process request limit"
    );
    first
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
        .await
        .unwrap();
    assert_eq!(work_a.await.unwrap().unwrap().status, 200);
    let (mut second, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let headers = String::from_utf8(read_request(&mut second).await)
        .unwrap()
        .to_ascii_lowercase();
    assert!(headers.contains("authorization: bearer beta-fixture"));
    second
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
        .await
        .unwrap();
    assert_eq!(work_b.await.unwrap().unwrap().status, 200);
    let ar = request();
    assert_eq!(
        tokio::task::spawn_blocking(move || alpha.send(ar))
            .await
            .unwrap()
            .err()
            .unwrap(),
        "evaluator HTTP budget exhausted"
    );
}
