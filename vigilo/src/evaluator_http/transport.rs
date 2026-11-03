//! Bounded asynchronous HTTP transport for an authorized evaluator invocation.
//!
//! The caller owns async execution and supplies host-managed credentials only after
//! policy authorization. Requests pin their validated DNS addresses, disable proxies,
//! redirects and retries, and never expose transport diagnostics containing secrets.
//! The synchronous ABI bridge and credential resolution remain host responsibilities.
use std::{
    sync::Arc,
    time::Duration,
};

use reqwest::header::{
    HeaderMap,
    HeaderName,
    HeaderValue,
};
use tokio::{
    sync::Mutex,
    time::Instant,
};
use uuid::Uuid;

use super::Snapshot;

/// Operator ceilings shared across every request in one evaluator invocation.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub request_bytes: usize,
    pub response_bytes: usize,
    pub request_timeout: Duration,
    pub total_time: Duration,
    pub requests: u32,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            request_bytes: 1024 * 1024,
            response_bytes: 4 * 1024 * 1024,
            request_timeout: Duration::from_secs(30),
            total_time: Duration::from_secs(60),
            requests: 8,
        }
    }
}

/// Evaluator-authored request; no credential reference can be supplied here.
pub struct Request {
    pub method: String,
    pub uri: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub timeout: Option<Duration>,
}
/// Complete bounded response. Non-success HTTP statuses are ordinary responses.
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}
/// Host bridge used by synchronous evaluator ABIs. Implementations own runtime
/// scheduling, cancellation and credentials; evaluator input cannot create one.
pub trait BlockingTransport: std::fmt::Debug + Send + Sync {
    fn send(&self, request: Request) -> Result<Response, String>;
}

/// Host-owned credential resolution. It runs after destination authorization and
/// within the same request/time budget as network I/O.
#[async_trait::async_trait]
pub trait Credentials: Send + Sync {
    #[must_use = "credential resolution executes only when awaited"]
    async fn headers(&self, method: &str, uri: &str) -> Result<HeaderMap, &'static str>;
}
struct FixedHeaders(HeaderMap);
#[async_trait::async_trait]
impl Credentials for FixedHeaders {
    async fn headers(&self, _: &str, _: &str) -> Result<HeaderMap, &'static str> {
        Ok(self.0.clone())
    }
}
struct Budget {
    calls: u32,
    remaining: Duration,
}
/// Per-invocation session. Calls serialize, matching the synchronous evaluator ABI;
/// permits and in-flight network futures are released when their caller is dropped.
pub struct Session {
    policy: Arc<Snapshot>,
    evaluator: Uuid,
    limits: Limits,
    budget: Mutex<Budget>,
    permits: Arc<tokio::sync::Semaphore>,
}
impl Session {
    pub fn new(
        policy: Arc<Snapshot>,
        evaluator: Uuid,
        limits: Limits,
    ) -> Result<Self, &'static str> {
        if limits.request_bytes == 0
            || limits.response_bytes == 0
            || limits.requests == 0
            || limits.request_timeout.is_zero()
            || limits.total_time.is_zero()
            || limits.request_bytes > 64 * 1024 * 1024
            || limits.response_bytes > 64 * 1024 * 1024
            || limits.requests > 1024
            || limits.request_timeout > Duration::from_secs(300)
            || limits.total_time > Duration::from_secs(3600)
        {
            return Err("invalid evaluator HTTP limits");
        }
        Ok(Self {
            policy,
            permits: Arc::new(tokio::sync::Semaphore::new(64)),
            evaluator,
            limits,
            budget: Mutex::new(Budget {
                calls: 0,
                remaining: limits.total_time,
            }),
        })
    }
    /// Share a worker-wide concurrency ceiling. Waiting consumes the request budget.
    pub fn with_concurrency(mut self, permits: Arc<tokio::sync::Semaphore>) -> Self {
        self.permits = permits;
        self
    }
    /// Send once using already-resolved host headers. Caller cancellation drops all
    /// I/O. Completed calls consume elapsed time, including failures. Cancelled
    /// calls consume the reserved timeout so abandonment cannot reset the budget.
    pub async fn send(
        &self,
        request: Request,
        credentials: HeaderMap,
    ) -> Result<Response, &'static str> {
        self.send_resolving(request, &FixedHeaders(credentials))
            .await
    }

    /// Resolve host credentials after authorization, under the invocation budget.
    pub async fn send_resolving(
        &self,
        request: Request,
        credentials: &dyn Credentials,
    ) -> Result<Response, &'static str> {
        let mut budget = self.budget.lock().await;
        if budget.calls >= self.limits.requests || budget.remaining.is_zero() {
            return Err("evaluator HTTP budget exhausted");
        }
        budget.calls += 1;
        let timeout = request
            .timeout
            .unwrap_or(self.limits.request_timeout)
            .min(self.limits.request_timeout)
            .min(budget.remaining);
        if timeout.is_zero() {
            return Err("invalid evaluator HTTP timeout");
        }
        budget.remaining = budget.remaining.saturating_sub(timeout);
        let started = Instant::now();
        let result = tokio::time::timeout(timeout, self.send_once(request, credentials)).await;
        // Unused reservation is refunded only after the future completes.
        budget.remaining += timeout.saturating_sub(started.elapsed());
        result.map_err(|_| "evaluator HTTP deadline exceeded")?
    }
    async fn send_once(
        &self,
        request: Request,
        credentials: &dyn Credentials,
    ) -> Result<Response, &'static str> {
        let destination = self
            .policy
            .authorize(self.evaluator, &request.method, &request.uri)
            .map_err(|_| "evaluator HTTP destination denied")?;
        if request.body.len() > self.limits.request_bytes || request.headers.len() > 64 {
            return Err("evaluator HTTP request limit exceeded");
        }
        let mut headers = HeaderMap::new();
        let mut header_bytes = 0usize;
        for (name, value) in request.headers {
            header_bytes = header_bytes
                .saturating_add(name.len())
                .saturating_add(value.len());
            if header_bytes > 16384 {
                return Err("evaluator HTTP headers exceed limit");
            }
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| "invalid evaluator HTTP header")?;
            if matches!(
                name.as_str(),
                "host"
                    | "connection"
                    | "content-length"
                    | "transfer-encoding"
                    | "upgrade"
                    | "trailer"
                    | "te"
                    | "proxy-authorization"
                    | "proxy-connection"
                    | "cookie"
                    | "authorization"
                    | "api-key"
                    | "x-http-method-override"
            ) || name.as_str().starts_with("x-forwarded-")
            {
                return Err("evaluator overrides host-managed header");
            }
            headers.append(
                name,
                HeaderValue::from_str(&value).map_err(|_| "invalid evaluator HTTP header")?,
            );
        }
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| "evaluator HTTP transport closed")?;
        let credentials = credentials.headers(&request.method, &request.uri).await?;
        if credentials.keys().any(|name| headers.contains_key(name)) {
            return Err("evaluator overrides host-managed header");
        }
        headers.extend(credentials);
        let url = destination.url();
        let host = url
            .host_str()
            .ok_or("missing HTTP host")?
            .trim_matches(['[', ']']);
        let port = url.port_or_known_default().ok_or("missing HTTP port")?;
        // OS name resolution cannot be interrupted. Keep its admission permit
        // inside the blocking job so cancellation cannot accumulate unbounded
        // resolver jobs. Numeric hosts need no blocking job at all.
        let (addresses, _permit) = if let Ok(ip) = host.parse::<std::net::IpAddr>() {
            (vec![std::net::SocketAddr::new(ip, port)], permit)
        } else {
            let hostname = host.to_owned();
            tokio::task::spawn_blocking(move || {
                use std::net::ToSocketAddrs;
                let addresses = (hostname.as_str(), port)
                    .to_socket_addrs()
                    .map_err(|_| "evaluator DNS resolution failed")?
                    .take(65)
                    .collect::<Vec<_>>();
                Ok::<_, &'static str>((addresses, permit))
            })
            .await
            .map_err(|_| "evaluator DNS resolution failed")??
        };
        if addresses.is_empty()
            || addresses.len() > 64
            || addresses
                .iter()
                .any(|addr| !destination.permits_address(addr.ip()))
        {
            return Err("evaluator HTTP address denied");
        }
        let client = reqwest::Client::builder()
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .pool_max_idle_per_host(0)
            .resolve_to_addrs(host, &addresses)
            .build()
            .map_err(|_| "evaluator HTTP client unavailable")?;
        let method = reqwest::Method::from_bytes(request.method.as_bytes())
            .map_err(|_| "invalid evaluator HTTP method")?;
        let mut response = client
            .request(method, url.clone())
            .headers(headers)
            .body(request.body)
            .send()
            .await
            .map_err(|_| "evaluator HTTP transport failed")?;
        if response
            .content_length()
            .is_some_and(|n| n > self.limits.response_bytes as u64)
        {
            return Err("evaluator HTTP response limit exceeded");
        }
        let status = response.status().as_u16();
        // Return only useful non-secret representation metadata. Never forward
        // cookies, authentication challenges or arbitrary provider headers.
        let headers = ["content-type", "content-encoding", "retry-after"]
            .into_iter()
            .filter_map(|name| {
                response
                    .headers()
                    .get(name)
                    .and_then(|value| value.to_str().ok())
                    .filter(|value| value.len() <= 4096)
                    .map(|value| (name.to_owned(), value.to_owned()))
            })
            .collect();
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| "evaluator HTTP body read failed")?
        {
            if body.len().saturating_add(chunk.len()) > self.limits.response_bytes {
                return Err("evaluator HTTP response limit exceeded");
            }
            body.extend_from_slice(&chunk);
        }
        Ok(Response {
            status,
            headers,
            body,
        })
    }
}
