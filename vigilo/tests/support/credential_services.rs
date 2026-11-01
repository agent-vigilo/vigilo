//! Real loopback tests for the open credential protocol and standalone injection.
use std::{
    collections::BTreeMap,
    time::Duration,
};

use agent_client::credentials::{
    Binding,
    Config,
    Execution,
    Provider,
    ProviderConfig,
    Source,
};

use super::*;

#[derive(Clone)]
struct Capture(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
impl std::io::Write for Capture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct Server {
    url: String,
    task: Option<tokio::task::JoinHandle<String>>,
}
impl Drop for Server {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
impl Server {
    async fn new(status: u16, body: String, extra: &str, stall: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let extra = extra.to_owned();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            loop {
                let mut block = [0; 4096];
                let n = socket.read(&mut block).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&block[..n]);
                assert!(bytes.len() <= 65_536);
                if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&bytes[..end]).to_ascii_lowercase();
                    let length = head
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length: "))
                        .unwrap()
                        .parse::<usize>()
                        .unwrap();
                    if bytes.len() >= end + 4 + length {
                        break;
                    }
                }
            }
            if stall {
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            let response = format!(
                "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n{extra}\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
            String::from_utf8(bytes).unwrap()
        });
        Self {
            url,
            task: Some(task),
        }
    }
    async fn request(&mut self) -> String {
        tokio::time::timeout(Duration::from_secs(5), self.task.as_mut().unwrap())
            .await
            .unwrap()
            .unwrap()
    }
}
fn execution() -> Execution {
    Execution {
        run_id: Uuid::nil(),
        execution_id: Uuid::from_u128(1),
        attempt_id: Uuid::from_u128(2),
    }
}
async fn service_provider(dir: &std::path::Path, endpoint: &str) -> Provider {
    tokio::fs::write(dir.join("token"), "workload-canary")
        .await
        .unwrap();
    tokio::fs::write(dir.join(Uuid::nil().to_string()), "grant-canary")
        .await
        .unwrap();
    let config = serde_json::json!({
        "version":1, "allow_http_loopback":true,
        "provider": { "type":"service", "endpoint":endpoint,
        "token":{"type":"file", "path":dir.join("token")}, "grants_directory":dir },
    });
    let path = dir.join("provider.json");
    tokio::fs::write(&path, serde_json::to_vec(&config).unwrap())
        .await
        .unwrap();
    Provider::load(&path).await.unwrap()
}
fn response(origin: &str) -> serde_json::Value {
    serde_json::json!({"version":1, "allowed_origin":origin, "header":"Authorization", "value":"Bearer agent-canary", "expires_at":chrono::Utc::now() + chrono::Duration::minutes(1)})
}
async fn invoke(
    provider: &Provider,
    p: &RunProfile,
) -> anyhow::Result<contracts::evaluator::AgentOutput> {
    agent_client::invoke(
        agent_client::Services {
            client: &reqwest::Client::new(),
            limits: Limits::default(),
            credentials: provider,
        },
        Uuid::nil(),
        Uuid::from_u128(1),
        Uuid::from_u128(2),
        p,
        &case(),
    )
    .await
    .map(|invocation| invocation.output)
}

#[tokio::test]
async fn service_protocol_resolves_for_runtime_scope_without_enterprise_dependencies() {
    let mut agent = Server::new(200, "hello".into(), "", false).await;
    let mut service = Server::new(200, response(&agent.url).to_string(), "", false).await;
    let dir = tempfile::tempdir().unwrap();
    let provider = service_provider(dir.path(), &service.url).await;
    let mut p = profile(&agent.url);
    p.agent.http.credential = Some("agent-api".into());
    p.agent
        .http
        .headers
        .insert("X-Request-Label".into(), "ordinary".into());
    let before = serde_json::to_value(&p).unwrap();
    let capture = Capture(std::sync::Arc::new(std::sync::Mutex::new(Vec::new())));
    let writer = capture.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(move || writer.clone())
        .finish();
    use tracing::instrument::WithSubscriber;
    let output = invoke(&provider, &p)
        .with_subscriber(subscriber)
        .await
        .unwrap();
    let logs = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    assert_eq!(output.text.as_deref(), Some("hello"));
    assert_eq!(before, serde_json::to_value(&p).unwrap());
    for canary in ["agent-canary", "grant-canary", "workload-canary"] {
        assert!(!logs.contains(canary));
        assert!(!serde_json::to_string(&p).unwrap().contains(canary));
        assert!(!serde_json::to_string(&output).unwrap().contains(canary));
    }
    let request = service.request().await;
    assert!(request.contains("authorization: Bearer workload-canary"));
    let body: serde_json::Value =
        serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap();
    assert_eq!(
        body,
        serde_json::json!({"version":1,"credential":"agent-api","destination":format!("{}/", agent.url),"run_id":Uuid::nil(),"execution_id":Uuid::from_u128(1),"attempt_id":Uuid::from_u128(2),"grant":"grant-canary"})
    );
    let sent = agent.request().await;
    assert!(sent.contains("authorization: Bearer agent-canary"));
    assert!(sent.contains("x-request-label: ordinary"));
    assert!(!sent.contains("grant-canary"));
    assert!(!sent.contains("workload-canary"));
    assert!(
        !sent
            .split_once("\r\n\r\n")
            .unwrap()
            .1
            .contains("agent-canary")
    );
}

#[tokio::test]
async fn standalone_injects_without_a_service_and_omits_authenticated_error_excerpts() {
    let mut agent = Server::new(401, "echo agent-canary".into(), "", false).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("token");
    tokio::fs::write(&path, "agent-canary").await.unwrap();
    let provider = Provider::new(Config {
        version: 1,
        allow_http_loopback: true,
        provider: ProviderConfig::Local {
            bindings: BTreeMap::from([(
                "agent-api".into(),
                Binding {
                    source: Source::File { path },
                    allowed_origin: agent.url.clone(),
                    header: "X-Api-Key".into(),
                    prefix: String::new(),
                },
            )]),
        },
    })
    .unwrap();
    let mut p = profile(&agent.url);
    p.agent.http.credential = Some("agent-api".into());
    let err = invoke(&provider, &p).await.unwrap_err();
    assert!(!format!("{err:#?}").contains("agent-canary"));
    let failure = err
        .downcast_ref::<agent_client::failure::Failure>()
        .unwrap();
    assert_eq!(failure.kind, agent_client::failure::Kind::Authentication);
    assert!(
        !serde_json::to_string(failure)
            .unwrap()
            .contains("agent-canary")
    );
    assert!(agent.request().await.contains("x-api-key: agent-canary"));
}

#[tokio::test]
async fn denied_malformed_expired_wrong_destination_and_oversized_resolutions_fail_closed() {
    let mut expired = response("https://agent.example.com");
    expired["expires_at"] = serde_json::json!(chrono::Utc::now() - chrono::Duration::seconds(1));
    let mut version = response("https://agent.example.com");
    version["version"] = 2.into();
    for (status, body, retryable) in [
        (403, "denied secret-canary".into(), false),
        (401, "secret-canary".into(), false),
        (503, "secret-canary".into(), true),
        (429, "secret-canary".into(), true),
        (200, "secret-canary".into(), false),
        (200, expired.to_string(), false),
        (200, version.to_string(), false),
        (
            200,
            response("https://other-tenant.example.com").to_string(),
            false,
        ),
        (200, "x".repeat(65_537), false),
    ] {
        let mut service = Server::new(status, body, "", false).await;
        let dir = tempfile::tempdir().unwrap();
        let provider = service_provider(dir.path(), &service.url).await;
        let err = provider
            .resolve(
                "agent-api",
                "https://agent.example.com",
                &BTreeMap::new(),
                execution(),
            )
            .await
            .err()
            .unwrap();
        assert_eq!(err.retryable, retryable);
        assert!(!format!("{err:?}").contains("canary"));
        service.request().await;
    }
}

#[tokio::test]
async fn redirects_never_forward_workload_or_agent_credentials() {
    let sink = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let location = format!(
        "Location: http://{}/capture\r\n",
        sink.local_addr().unwrap()
    );
    let mut service = Server::new(307, String::new(), &location, false).await;
    let dir = tempfile::tempdir().unwrap();
    let provider = service_provider(dir.path(), &service.url).await;
    assert!(
        provider
            .resolve(
                "agent-api",
                "https://agent.example.com",
                &BTreeMap::new(),
                execution()
            )
            .await
            .is_err()
    );
    service.request().await;
    let mut agent = Server::new(307, String::new(), &location, false).await;
    let mut service = Server::new(200, response(&agent.url).to_string(), "", false).await;
    let provider = service_provider(dir.path(), &service.url).await;
    let mut p = profile(&agent.url);
    p.agent.http.credential = Some("agent-api".into());
    assert!(invoke(&provider, &p).await.is_err());
    service.request().await;
    agent.request().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), sink.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn grant_is_required_and_service_deadline_is_bounded() {
    let mut service = Server::new(200, String::new(), "", true).await;
    let dir = tempfile::tempdir().unwrap();
    let provider = service_provider(dir.path(), &service.url).await;
    let unauthorized = Execution {
        run_id: Uuid::from_u128(999),
        ..execution()
    };
    let err = provider
        .resolve(
            "agent-api",
            "https://agent.example.com",
            &BTreeMap::new(),
            unauthorized,
        )
        .await
        .err()
        .unwrap();
    assert!(!err.retryable);
    let err = tokio::time::timeout(
        Duration::from_secs(15),
        provider.resolve(
            "agent-api",
            "https://agent.example.com",
            &BTreeMap::new(),
            execution(),
        ),
    )
    .await
    .unwrap()
    .err()
    .unwrap();
    assert!(err.retryable);
    let task = service.task.take().unwrap();
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn service_resolution_bounds_concurrency_and_cancellation_releases_capacity() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let dir = tempfile::tempdir().unwrap();
    let provider = std::sync::Arc::new(service_provider(dir.path(), &endpoint).await);
    let mut calls = tokio::task::JoinSet::new();
    for _ in 0..65 {
        let provider = provider.clone();
        calls.spawn(async move {
            provider
                .resolve(
                    "agent-api",
                    "https://agent.example.com",
                    &BTreeMap::new(),
                    execution(),
                )
                .await
                .is_ok()
        });
    }
    let mut sockets = Vec::new();
    for _ in 0..64 {
        sockets.push(
            tokio::time::timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap()
                .0,
        );
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err()
    );
    calls.abort_all();
    while calls.join_next().await.is_some() {}
    drop(sockets);
    let next_provider = provider.clone();
    calls.spawn(async move {
        next_provider
            .resolve(
                "agent-api",
                "https://agent.example.com",
                &BTreeMap::new(),
                execution(),
            )
            .await
            .is_ok()
    });
    // A new transport request proves cancellation returned resolution capacity.
    let _socket = tokio::time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap()
        .0;
    calls.abort_all();
    while calls.join_next().await.is_some() {}
}

#[tokio::test]
async fn usage_is_adapter_specific_and_correlation_cannot_be_overridden() {
    let body = serde_json::json!({"choices":[{"message":{"content":"hello"}}],"usage":{"prompt_tokens":2,"completion_tokens":0,"total_tokens":2},"secret":"response-canary"}).to_string();
    for chat in [false, true] {
        let mut server = Server::new(
            200,
            body.clone(),
            "x-request-id: untrusted-canary\r\n",
            false,
        )
        .await;
        let mut p = profile(&server.url);
        if chat {
            p.agent.model = Some("fixture".into());
            p.agent.config =
                serde_json::json!({"request_format":"openai_compatible_chat_completions"});
        }
        let invocation = agent_client::invoke(
            agent_client::Services {
                client: &reqwest::Client::new(),
                limits: Limits::default(),
                credentials: &Provider::default(),
            },
            Uuid::nil(),
            Uuid::nil(),
            Uuid::nil(),
            &p,
            &case(),
        )
        .await
        .unwrap();
        assert_eq!(invocation.diagnostics.usage.is_some(), chat);
        if let Some(usage) = &invocation.diagnostics.usage {
            assert_eq!(usage.completion_tokens, Some(0));
        }
        let safe = serde_json::to_string(&invocation.diagnostics).unwrap();
        assert!(!safe.contains("canary"));
        assert!(safe.len() < 1024);
        assert!(server.request().await.contains("x-vigilo-attempt-id:"));
    }
    let mut p = profile("http://127.0.0.1:1");
    p.agent
        .http
        .headers
        .insert("X-Vigilo-Attempt-Id".into(), "forged".into());
    let error = invoke(&Provider::default(), &p).await.unwrap_err();
    let failure = error
        .downcast_ref::<agent_client::failure::Failure>()
        .unwrap();
    assert_eq!(failure.kind, agent_client::failure::Kind::Configuration);
    assert!(failure.invocation.as_ref().unwrap().latency_ms.is_none());
}
