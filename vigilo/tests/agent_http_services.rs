//! Opt-in loopback HTTP tests for real agent invocation limits and wire behavior.
#![allow(dead_code)]
#[path = "../src/agent_client.rs"]
mod agent_client;
#[path = "support/agent_contracts.rs"]
mod contracts;

use agent_client::limits::Limits;
use contracts::{
    evaluator::TestCase,
    run::RunProfile,
};
use tokio::io::{
    AsyncReadExt,
    AsyncWriteExt,
};
use uuid::Uuid;

fn profile(url: &str) -> RunProfile {
    serde_json::from_value(serde_json::json!({
        "profile_id":"test", "profile_version":"1", "description":"",
        "defaults":{"max_attempts":1,"request_timeout_secs":2,"fail_on_any_blocking_failure":true,"min_execution_score":0.5},
        "agent":{"provider":"test","name":"test","http":{"url":url}},
        "persistence":{"mode":"full","persist_raw_outputs":"all","persist_evaluator_evidence":true},
        "case_groups":[]
    })).unwrap()
}
fn case() -> TestCase {
    serde_json::from_value(serde_json::json!({"id":"case", "task_type":"text", "input":"hello", "tags":[], "metadata":{}})).unwrap()
}

async fn call(
    response: &'static str,
    limit: u64,
) -> anyhow::Result<contracts::evaluator::AgentOutput> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let mut server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut chunk = [0; 4096];
        loop {
            let n = stream.read(&mut chunk).await.unwrap();
            if n == 0 {
                break;
            }
            request.extend_from_slice(&chunk[..n]);
            if let Some(end) = request.windows(4).position(|v| v == b"\r\n\r\n") {
                let header = String::from_utf8_lossy(&request[..end]).to_lowercase();
                let length: usize = header
                    .lines()
                    .find_map(|l| l.strip_prefix("content-length: "))
                    .unwrap()
                    .parse()
                    .unwrap();
                if request.len() >= end + 4 + length {
                    assert!(header.contains("content-type: application/json"));
                    let body: serde_json::Value =
                        serde_json::from_slice(&request[end + 4..]).unwrap();
                    assert_eq!(body["input"], "hello");
                    assert!(body["case"].get("expected").is_none());
                    break;
                }
            }
            assert!(request.len() < 16_384);
        }
        let _ = stream.write_all(response.as_bytes()).await;
    });
    let result = agent_client::invoke(
        agent_client::Services {
            client: &reqwest::Client::new(),
            credentials: &agent_client::credentials::Provider::default(),
            limits: Limits {
                max_request_bytes: 4096,
                max_response_bytes: limit,
            },
        },
        Uuid::nil(),
        Uuid::nil(),
        Uuid::nil(),
        &profile(&url),
        &case(),
    )
    .await;
    match tokio::time::timeout(std::time::Duration::from_secs(3), &mut server).await {
        Ok(joined) => joined.expect("HTTP fixture failed"),
        Err(_) => {
            server.abort();
            let _ = server.await;
            panic!("HTTP fixture did not finish within its deadline");
        }
    }
    result
}

#[tokio::test]
async fn response_bounds_cover_success_error_chunked_and_missing_length() {
    let output = call("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello", 5)
        .await
        .unwrap();
    assert_eq!(output.text.as_deref(), Some("hello"));
    for response in [
        "HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nhello!",
        "HTTP/1.1 500 Error\r\nContent-Length: 6\r\n\r\nhello!",
        "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nhello!",
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nhel\r\n3\r\nlo!\r\n0\r\n\r\n",
    ] {
        assert!(
            call(response, 5)
                .await
                .unwrap_err()
                .to_string()
                .contains("response limit exceeded")
        );
    }
    assert!(
        call("HTTP/1.1 500 Error\r\nContent-Length: 3\r\n\r\nbad", 5)
            .await
            .unwrap_err()
            .to_string()
            .contains("status 500")
    );
    assert!(
        call("HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\nshort", 10)
            .await
            .unwrap_err()
            .to_string()
            .contains("read agent HTTP response")
    );
}

#[tokio::test]
async fn oversized_request_and_profile_fail_before_connection() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut p = profile(&format!("http://{}", listener.local_addr().unwrap()));
    let limits = Limits {
        max_request_bytes: 1,
        max_response_bytes: 10,
    };
    let result = agent_client::invoke(
        agent_client::Services {
            client: &reqwest::Client::new(),
            limits,
            credentials: &agent_client::credentials::Provider::default(),
        },
        Uuid::nil(),
        Uuid::nil(),
        Uuid::nil(),
        &p,
        &case(),
    )
    .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("request serialization")
    );
    p.agent.http.max_response_bytes = Some(11);
    let result = agent_client::invoke(
        agent_client::Services {
            client: &reqwest::Client::new(),
            limits,
            credentials: &agent_client::credentials::Provider::default(),
        },
        Uuid::nil(),
        Uuid::nil(),
        Uuid::nil(),
        &p,
        &case(),
    )
    .await;
    assert!(result.unwrap_err().to_string().contains("worker ceiling"));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn response_timeout_remains_enforced_during_body_read() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut p = profile(&format!("http://{}", listener.local_addr().unwrap()));
    p.agent.http.timeout_secs = Some(1);
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut bytes = [0; 4096];
        assert!(stream.read(&mut bytes).await.unwrap() > 0);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nh")
            .await
            .unwrap();
        std::future::pending::<()>().await;
    });
    let result = agent_client::invoke(
        agent_client::Services {
            client: &reqwest::Client::new(),
            limits: Limits::default(),
            credentials: &agent_client::credentials::Provider::default(),
        },
        Uuid::nil(),
        Uuid::nil(),
        Uuid::nil(),
        &p,
        &case(),
    )
    .await;
    server.abort();
    let _ = server.await;
    let error = result.unwrap_err();
    assert!(error.to_string().contains("read agent HTTP response body"));
    let failure = error
        .downcast_ref::<agent_client::failure::Failure>()
        .unwrap();
    assert_eq!(failure.kind, agent_client::failure::Kind::Timeout);
    assert!(failure.retryable);
}

#[tokio::test]
async fn failures_preserve_type_and_retry_advice_without_client_retries() {
    use agent_client::failure::{
        Failure,
        Kind,
    };
    for (response, kind, retryable) in [
        (
            "HTTP/1.1 401 Unauthorized\r\nContent-Length: 3\r\n\r\nbad",
            Kind::Authentication,
            false,
        ),
        (
            "HTTP/1.1 429 Busy\r\nRetry-After: 60\r\nContent-Length: 3\r\n\r\nbad",
            Kind::RateLimited,
            true,
        ),
        (
            "HTTP/1.1 503 Busy\r\nRetry-After: 601\r\nContent-Length: 3\r\n\r\nbad",
            Kind::RetryDelayExceeded,
            false,
        ),
        (
            "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n{",
            Kind::InvalidResponse,
            false,
        ),
        (
            "HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\nshort",
            Kind::Transport,
            true,
        ),
        (
            "HTTP/1.1 403 Forbidden\r\nContent-Length: 9\r\n\r\nshort",
            Kind::Authentication,
            false,
        ),
    ] {
        let before = chrono::Utc::now();
        let error = call(response, 10).await.unwrap_err();
        let failure = error
            .downcast_ref::<Failure>()
            .expect("typed invocation failure");
        assert_eq!(failure.kind, kind);
        assert_eq!(failure.retryable, retryable);
        if kind == Kind::RateLimited {
            assert!(failure.retry_not_before.unwrap() >= before + chrono::Duration::seconds(60));
        }
    }
}

#[tokio::test]
async fn credential_reference_without_provider_fails_closed() {
    let mut value = serde_json::to_value(profile("http://127.0.0.1:1")).unwrap();
    value["agent"]["http"]["credential"] = serde_json::json!("agent-api");
    let profile = serde_json::from_value(value).unwrap();
    let err = agent_client::invoke(
        agent_client::Services {
            client: &reqwest::Client::new(),
            limits: Limits::default(),
            credentials: &agent_client::credentials::Provider::default(),
        },
        Uuid::nil(),
        Uuid::nil(),
        Uuid::nil(),
        &profile,
        &case(),
    )
    .await
    .unwrap_err();
    let failure = err
        .downcast_ref::<agent_client::failure::Failure>()
        .unwrap();
    assert_eq!(failure.kind, agent_client::failure::Kind::Configuration);
    assert!(!failure.retryable);
}

#[path = "support/credential_services.rs"]
mod credential_services;
