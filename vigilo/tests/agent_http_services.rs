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

async fn call(response: &str, limit: u64) -> anyhow::Result<contracts::evaluator::AgentOutput> {
    call_bytes(response.as_bytes(), limit).await
}

async fn call_bytes(
    response: &[u8],
    limit: u64,
) -> anyhow::Result<contracts::evaluator::AgentOutput> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let response = response.to_owned();
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
                    assert!(header.contains(&format!("x-vigilo-attempt-id: {}", Uuid::nil())));
                    let body: serde_json::Value =
                        serde_json::from_slice(&request[end + 4..]).unwrap();
                    assert_eq!(body["input"], "hello");
                    assert!(body["case"].get("expected").is_none());
                    break;
                }
            }
            assert!(request.len() < 16_384);
        }
        let _ = stream.write_all(&response).await;
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
    .await
    .map(|invocation| invocation.output);
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
    .await
    .map(|invocation| invocation.output);
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
    .await
    .map(|invocation| invocation.output);
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
    .await
    .map(|invocation| invocation.output);
    server.abort();
    let _ = server.await;
    let error = result.unwrap_err();
    assert!(error.to_string().contains("read agent HTTP response body"));
    let failure = error
        .downcast_ref::<agent_client::failure::Failure>()
        .unwrap();
    assert_eq!(failure.kind, agent_client::failure::Kind::Timeout);
    let diagnostics = failure.invocation.as_ref().unwrap();
    assert!(diagnostics.latency_ms.is_some());
    assert!(diagnostics.elapsed_ms >= diagnostics.latency_ms.unwrap());
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
    let diagnostics = failure.invocation.as_ref().unwrap();
    assert!(diagnostics.latency_ms.is_none());
    assert!(diagnostics.http_status.is_none());
}

#[path = "support/credential_services.rs"]
mod credential_services;

#[tokio::test]
async fn invocation_diagnostics_identify_success_and_failure_consistently() {
    let output = call("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok", 10)
        .await
        .unwrap();
    let data = &output.metadata["vigilo_invocation"];
    assert_eq!(data["attempt_id"], Uuid::nil().to_string());
    assert!(
        data["endpoint_ref"]
            .as_str()
            .unwrap()
            .starts_with("blake3:")
    );
    assert!(data["elapsed_ms"].is_u64());
    assert_eq!(data["http_status"], 200);
    assert!(data["failure_category"].is_null());
    let body = serde_json::json!({"text":"ok", "metadata":{"vigilo_invocation":{"attempt_id":"forged", "endpoint_ref":"forged"}}}).to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    let output = call(&response, 1024).await.unwrap();
    assert_eq!(
        output.metadata["vigilo_invocation"]["attempt_id"],
        Uuid::nil().to_string()
    );
    assert!(
        output.metadata["vigilo_invocation"]["endpoint_ref"]
            .as_str()
            .unwrap()
            .starts_with("blake3:")
    );
}

#[tokio::test]
async fn failed_http_and_response_validation_retain_safe_timing_and_status() {
    for (response, limit, status, kind) in [
        (
            "HTTP/1.1 503 Busy\r\nContent-Length: 6\r\n\r\ncanary",
            10,
            503,
            agent_client::failure::Kind::HttpStatus,
        ),
        (
            "HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\ncanary",
            5,
            200,
            agent_client::failure::Kind::BodyLimit,
        ),
        (
            "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n{",
            10,
            200,
            agent_client::failure::Kind::InvalidResponse,
        ),
    ] {
        let err = call(response, limit).await.unwrap_err();
        let failure = err
            .downcast_ref::<agent_client::failure::Failure>()
            .unwrap();
        let diagnostics = failure.invocation.as_ref().unwrap();
        assert_eq!(failure.kind, kind);
        assert_eq!(diagnostics.failure_category, Some(kind));
        assert_eq!(diagnostics.http_status, Some(status));
        assert!(diagnostics.latency_ms.is_some());
        assert!(diagnostics.elapsed_ms >= diagnostics.latency_ms.unwrap());
        assert!(
            !serde_json::to_string(diagnostics)
                .unwrap()
                .contains("canary")
        );
    }
}

fn compressed_response(body: &[u8], encoding: &str) -> Vec<u8> {
    let mut response = format!("HTTP/1.1 200 OK\r\nContent-Encoding: {encoding}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n", body.len()).into_bytes();
    response.extend_from_slice(body);
    response
}

fn gzip_bytes(bytes: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

#[tokio::test]
async fn compressed_response_limits_apply_to_decoded_bytes() {
    let encoded = gzip_bytes(b"hello");
    let response = compressed_response(&encoded, "gzip");
    let result = call_bytes(&response, 5).await.unwrap();
    assert_eq!(result.text.as_deref(), Some("hello"));
    assert!(call_bytes(&response, 4).await.is_err());
}

async fn capture_bytes(
    response: Vec<u8>,
    limit: u64,
) -> Result<vigilo::response::CapturedBody, agent_client::failure::Failure> {
    capture_bytes_with_timeout(response, limit, std::time::Duration::from_secs(2)).await
}

async fn capture_bytes_with_timeout(
    response: Vec<u8>,
    limit: u64,
    timeout: std::time::Duration,
) -> Result<vigilo::response::CapturedBody, agent_client::failure::Failure> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let mut server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        let _ = stream.read(&mut request).await;
        let _ = stream.write_all(&response).await;
    });
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .unwrap();
    let response = client.get(url).send().await.unwrap();
    let result =
        agent_client::capture::collect(response, limit, tokio::time::Instant::now() + timeout)
            .await;
    if tokio::time::timeout(std::time::Duration::from_secs(2), &mut server)
        .await
        .is_err()
    {
        server.abort();
        let _ = server.await;
        panic!("capture fixture did not complete");
    }
    result
}

#[tokio::test]
async fn capture_preserves_binary_empty_bom_and_invalid_json_without_interpreting() {
    for bytes in [
        vec![],
        vec![0, 255, 42],
        b"{invalid json".to_vec(),
        vec![239, 187, 191, b'a'],
    ] {
        let response = compressed_response(&bytes, "identity");
        let captured = capture_bytes(response, 64).await.unwrap();
        assert_eq!(captured.bytes(), bytes);
        assert_eq!(captured.content_type(), Some("text/plain"));
    }
    let captured = capture_bytes(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\na".to_vec(), 1)
        .await
        .unwrap();
    assert_eq!(captured.content_type(), None);
}

#[tokio::test]
async fn compressed_capture_validates_trailers_members_and_encoding() {
    use std::io::Write;

    use agent_client::failure::Kind;
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(b"hello").unwrap();
    let mut zlib = encoder.finish().unwrap();
    assert_eq!(
        capture_bytes(compressed_response(&zlib, "deflate"), 5)
            .await
            .unwrap()
            .bytes(),
        b"hello"
    );
    zlib.push(1);
    assert_eq!(
        capture_bytes(compressed_response(&zlib, "deflate"), 5)
            .await
            .unwrap_err()
            .kind,
        Kind::InvalidResponse
    );
    let mut concatenated = gzip_bytes(b"ab");
    concatenated.extend(gzip_bytes(b"cd"));
    assert_eq!(
        capture_bytes(compressed_response(&concatenated, "gzip"), 4)
            .await
            .unwrap()
            .bytes(),
        b"abcd"
    );
    concatenated.pop();
    assert_eq!(
        capture_bytes(compressed_response(&concatenated, "gzip"), 4)
            .await
            .unwrap_err()
            .kind,
        Kind::InvalidResponse
    );
    let mut corrupt = gzip_bytes(b"hello");
    let checksum = corrupt.len() - 8;
    corrupt[checksum] ^= 1;
    assert_eq!(
        capture_bytes(compressed_response(&corrupt, "gzip"), 5)
            .await
            .unwrap_err()
            .kind,
        Kind::InvalidResponse
    );
    for encoding in ["br", "gzip, deflate", "unknown"] {
        assert_eq!(
            capture_bytes(compressed_response(b"body", encoding), 16)
                .await
                .unwrap_err()
                .kind,
            Kind::InvalidResponse
        );
    }
}

#[tokio::test]
async fn expansion_and_encoded_overhead_are_both_bounded() {
    use agent_client::failure::Kind;
    let bomb = gzip_bytes(&vec![b'x'; 128 * 1024]);
    assert_eq!(
        capture_bytes(compressed_response(&bomb, "gzip"), 1024)
            .await
            .unwrap_err()
            .kind,
        Kind::BodyLimit
    );
    let empty = gzip_bytes(b"");
    let overhead = empty.repeat(4000);
    assert_eq!(
        capture_bytes(compressed_response(&overhead, "gzip"), 1)
            .await
            .unwrap_err()
            .kind,
        Kind::BodyLimit
    );
}

#[tokio::test]
async fn original_capture_is_shared_but_not_implicitly_serialized() {
    let output = call_bytes(
        b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n\xef\xbb\xbfX",
        4,
    )
    .await
    .unwrap();
    assert_eq!(output.text.as_deref(), Some("X"));
    let captured = output.captured_body.as_ref().unwrap();
    assert_eq!(captured.bytes(), b"\xef\xbb\xbfX");
    let clone = output.clone();
    assert_eq!(
        captured.bytes().as_ptr(),
        clone.captured_body.as_ref().unwrap().bytes().as_ptr()
    );
    assert!(
        serde_json::to_value(&output)
            .unwrap()
            .get("captured_body")
            .is_none()
    );
}

#[tokio::test]
async fn capture_rejects_ambiguous_metadata_and_expired_deadlines() {
    use agent_client::failure::Kind;
    for response in [
        b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Type: image/png\r\nContent-Length: 0\r\n\r\n".to_vec(),
        b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Encoding: deflate\r\nContent-Length: 0\r\n\r\n".to_vec(),
        format!("HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: 0\r\n\r\n", "x".repeat(1025)).into_bytes(),
    ] {
        assert_eq!(capture_bytes(response, 1).await.unwrap_err().kind, Kind::InvalidResponse);
    }
    let empty = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    assert_eq!(
        capture_bytes(empty.to_vec(), 0).await.unwrap_err().kind,
        Kind::Configuration
    );
    let error = capture_bytes_with_timeout(empty.to_vec(), 1, std::time::Duration::ZERO)
        .await
        .unwrap_err();
    assert_eq!(error.kind, Kind::Timeout);
    assert!(error.retryable);
}
