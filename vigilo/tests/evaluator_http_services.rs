//! Opt-in loopback coverage of bounded evaluator HTTP transport.
//!
//! These tests exercise real sockets without requiring remote services. ABI and
//! credential integration have separate boundaries; transport success alone does
//! not prove that a Wasm evaluator can issue an authorized request.
#[path = "../src/agent_client.rs"]
#[allow(dead_code)]
mod agent_client;
#[path = "support/evaluator_http_context.rs"]
#[allow(dead_code)]
mod context;
#[path = "support/storage_contracts.rs"]
#[allow(dead_code)]
mod contracts;
#[path = "../src/evaluator_abi.rs"]
#[allow(dead_code)]
mod evaluator_abi;
#[path = "../src/evaluator_package.rs"]
#[allow(dead_code)]
mod evaluator_package;
#[path = "../src/evaluator_requests.rs"]
#[allow(dead_code)]
mod evaluator_requests;
#[path = "../src/manifest.rs"]
#[allow(dead_code)]
mod manifest;

use std::{
    sync::Arc,
    time::Duration,
};

use reqwest::header::{
    HeaderMap,
    HeaderValue,
};
use tokio::{
    io::{
        AsyncReadExt,
        AsyncWriteExt,
    },
    net::TcpListener,
};
use uuid::Uuid;
use vigilo::evaluator_http::{
    Action,
    Rule,
    Snapshot,
    transport::{
        Limits,
        Request,
        Session,
    },
};

fn request(uri: &str) -> Request {
    Request {
        method: "POST".into(),
        uri: uri.into(),
        headers: vec![],
        body: b"input".to_vec(),
        timeout: None,
    }
}
fn session(uri: &str, limits: Limits, action: Action) -> Session {
    let id = Uuid::new_v4();
    let policy = Snapshot::new(
        id,
        vec![Rule {
            uri: uri.into(),
            method: "POST".into(),
            action,
            networks: vec!["127.0.0.0/8".into()],
        }],
    )
    .unwrap();
    Session::new(Arc::new(policy), id, limits).unwrap()
}
async fn server(response: &'static [u8]) -> (String, tokio::task::JoinHandle<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}/judge", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(5), async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            loop {
                let mut buf = [0; 1024];
                let count = socket.read(&mut buf).await.unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&buf[..count]);
                assert!(bytes.len() <= 32768);
                if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..end]).to_lowercase();
                    let size: usize = headers
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length: "))
                        .unwrap_or("0")
                        .parse()
                        .unwrap();
                    if bytes.len() >= end + 4 + size {
                        break;
                    }
                }
            }
            socket.write_all(response).await.unwrap();
            bytes
        })
        .await
        .expect("bounded test server")
    });
    (uri, task)
}

#[tokio::test]
async fn status_body_and_safe_headers_are_returned_without_retry() {
    let (uri, server) = server(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 5\r\nContent-Type: text/plain\r\nRetry-After: 2\r\nSet-Cookie: private\r\nX-Secret: private\r\nConnection: close\r\n\r\nerror").await;
    let session = session(&uri, Limits::default(), Action::Allow);
    let mut credentials = HeaderMap::new();
    credentials.insert("authorization", HeaderValue::from_static("Bearer secret"));
    let response = session.send(request(&uri), credentials).await.unwrap();
    assert_eq!(response.status, 503);
    assert_eq!(response.body, b"error");
    assert_eq!(
        response.headers,
        [
            ("content-type".into(), "text/plain".into()),
            ("retry-after".into(), "2".into())
        ]
    );
    let received = String::from_utf8(server.await.unwrap()).unwrap();
    assert!(received.contains("authorization: Bearer secret\r\n"));
    assert!(received.ends_with("input"));
}

#[tokio::test]
async fn redirect_is_returned_without_following() {
    let (uri, server) = server(b"HTTP/1.1 307 Temporary Redirect\r\nLocation: http://127.0.0.1:1/secret\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await;
    assert_eq!(
        session(&uri, Limits::default(), Action::Allow)
            .send(request(&uri), HeaderMap::new())
            .await
            .unwrap()
            .status,
        307
    );
    server.await.unwrap();
}

#[tokio::test]
async fn oversized_declared_and_chunked_bodies_fail_without_partial_results() {
    for response in [
        &b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\n123456"[..],
        &b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n3\r\n123\r\n3\r\n456\r\n0\r\n\r\n"[..],
    ] {
        let (uri, server) = server(response).await;
        let limits = Limits { response_bytes: 5, ..Limits::default() };
        assert_eq!(session(&uri, limits, Action::Allow).send(request(&uri), HeaderMap::new()).await.err(), Some("evaluator HTTP response limit exceeded"));
        server.await.unwrap();
    }
}

#[tokio::test]
async fn exact_body_limit_and_request_count_are_enforced() {
    let (uri, server) =
        server(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\n12345").await;
    let session = session(
        &uri,
        Limits {
            requests: 1,
            response_bytes: 5,
            request_bytes: 5,
            ..Limits::default()
        },
        Action::Allow,
    );
    assert_eq!(
        session
            .send(request(&uri), HeaderMap::new())
            .await
            .unwrap()
            .body,
        b"12345"
    );
    assert_eq!(
        session.send(request(&uri), HeaderMap::new()).await.err(),
        Some("evaluator HTTP budget exhausted")
    );
    server.await.unwrap();
}

#[tokio::test]
async fn denied_policy_and_header_overrides_never_connect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}/judge", listener.local_addr().unwrap());
    assert_eq!(
        session(&uri, Limits::default(), Action::Deny)
            .send(request(&uri), HeaderMap::new())
            .await
            .err(),
        Some("evaluator HTTP destination denied")
    );
    for name in [
        "Host",
        "Authorization",
        "x-forwarded-host",
        "x-custom-token",
        "content-length",
    ] {
        let session = session(&uri, Limits::default(), Action::Allow);
        let mut req = request(&uri);
        req.headers.push((name.into(), "forged".into()));
        let mut credentials = HeaderMap::new();
        credentials.insert("x-custom-token", HeaderValue::from_static("secret"));
        assert_eq!(
            session.send(req, credentials).await.err(),
            Some("evaluator overrides host-managed header")
        );
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn timeout_consumes_total_budget_and_closes_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}/judge", listener.local_addr().unwrap());
    let session = session(
        &uri,
        Limits {
            total_time: Duration::from_millis(100),
            ..Limits::default()
        },
        Action::Allow,
    );
    let operation = session.send(request(&uri), HeaderMap::new());
    let peer = async {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        socket.read_to_end(&mut bytes).await.unwrap();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(operation, peer)
    })
    .await
    .unwrap();
    assert_eq!(result.err(), Some("evaluator HTTP deadline exceeded"));
    assert_eq!(
        session.send(request(&uri), HeaderMap::new()).await.err(),
        Some("evaluator HTTP budget exhausted")
    );
}

#[tokio::test]
async fn cancellation_releases_session_and_retains_reserved_budget() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}/judge", listener.local_addr().unwrap());
    let session = session(
        &uri,
        Limits {
            total_time: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
            ..Limits::default()
        },
        Action::Allow,
    );
    {
        let operation = session.send(request(&uri), HeaderMap::new());
        tokio::pin!(operation);
        tokio::select! {
            biased;
            result = &mut operation => panic!("request completed unexpectedly: {}", result.is_ok()),
            result = tokio::time::timeout(Duration::from_secs(3), listener.accept()) => { result.unwrap().unwrap(); }
        }
        // Dropping the future must release the invocation lock and its network I/O.
    }
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        session.send(request(&uri), HeaderMap::new()),
    )
    .await
    .unwrap();
    assert_eq!(result.err(), Some("evaluator HTTP budget exhausted"));
}

#[tokio::test]
async fn resolved_private_addresses_need_explicit_approval() {
    let id = Uuid::new_v4();
    let policy = Snapshot::new(
        id,
        vec![Rule {
            uri: "https://localhost/judge".into(),
            method: "POST".into(),
            action: Action::Allow,
            networks: vec![],
        }],
    )
    .unwrap();
    let session = Session::new(Arc::new(policy), id, Limits::default()).unwrap();
    assert_eq!(
        session
            .send(request("https://localhost/judge"), HeaderMap::new())
            .await
            .err(),
        Some("evaluator HTTP address denied")
    );
}

#[tokio::test]
async fn invalid_and_oversized_requests_fail_before_connecting() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}/judge", listener.local_addr().unwrap());
    let session = session(
        &uri,
        Limits {
            request_bytes: 4,
            ..Limits::default()
        },
        Action::Allow,
    );
    assert_eq!(
        session.send(request(&uri), HeaderMap::new()).await.err(),
        Some("evaluator HTTP request limit exceeded")
    );
    let mut req = request(&uri);
    req.body.clear();
    req.headers.push(("invalid name".into(), "value".into()));
    assert_eq!(
        session.send(req, HeaderMap::new()).await.err(),
        Some("invalid evaluator HTTP header")
    );
    let mut req = request(&uri);
    req.body.clear();
    req.timeout = Some(Duration::ZERO);
    assert_eq!(
        session.send(req, HeaderMap::new()).await.err(),
        Some("invalid evaluator HTTP timeout")
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err()
    );
}
