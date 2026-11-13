//! Worker-owned evaluator HTTP configuration, credentials and synchronous ABI bridge.
//!
//! Operators bind verified evaluator IDs and exact destinations to credential names.
//! Each invocation freezes those bindings, caches secrets only until expiry, and
//! owns cancellation. The async transport runs on the caller's runtime while Wasm
//! executes on a blocking thread. No database lookup or provider payload mapping
//! belongs in this module.
use std::{
    collections::BTreeMap,
    path::Path,
    sync::Arc,
    time::Duration,
};

use anyhow::{
    Result,
    ensure,
};
use reqwest::header::HeaderMap;
use serde::Deserialize;
use tokio::{
    io::AsyncReadExt,
    sync::{
        Mutex,
        Semaphore,
        watch,
    },
};
use uuid::Uuid;
use vigilo::evaluator_http::{
    Snapshot,
    transport::{
        Credentials,
        Limits,
        Request,
        Response,
        Session,
    },
};

use crate::agent_client::credentials::{
    self,
    Authority,
    Execution,
    Provider,
    Resolved,
};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    evaluator_id: Uuid,
    method: String,
    uri: String,
    credential: String,
}

/// Process-local operator ceilings. The synchronous ABI permits one request at a
/// time per invocation; the shared semaphore bounds all invocations in a worker.
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Config {
    request_bytes: usize,
    response_bytes: usize,
    request_timeout_ms: u64,
    total_time_ms: u64,
    requests: u32,
    concurrency: usize,
    bindings: Vec<Binding>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            request_bytes: 1024 * 1024,
            response_bytes: 4 * 1024 * 1024,
            request_timeout_ms: 30_000,
            total_time_ms: 60_000,
            requests: 8,
            concurrency: 64,
            bindings: vec![],
        }
    }
}

pub(crate) struct Service {
    config: Config,
    permits: Arc<Semaphore>,
}
impl Default for Service {
    fn default() -> Self {
        Self {
            config: Config::default(),
            permits: Arc::new(Semaphore::new(64)),
        }
    }
}
impl Service {
    pub(crate) fn from_bindings_json(text: &str) -> Result<Self> {
        ensure!(
            text.len() <= 1024 * 1024,
            "evaluator HTTP config exceeds 1 MiB"
        );
        let bindings = serde_json::from_str(text)
            .map_err(|_| anyhow::anyhow!("invalid evaluator credential bindings"))?;
        Self::configured(Config {
            bindings,
            ..Default::default()
        })
    }
    /// Isolate binding selection while retaining process-wide ceilings and permits.
    pub(crate) fn bind_to(self, base: &Self) -> Self {
        Self {
            config: Config {
                bindings: self.config.bindings,
                request_bytes: base.config.request_bytes,
                response_bytes: base.config.response_bytes,
                request_timeout_ms: base.config.request_timeout_ms,
                total_time_ms: base.config.total_time_ms,
                requests: base.config.requests,
                concurrency: base.config.concurrency,
            },
            permits: base.permits.clone(),
        }
    }
    /// Read a bounded operator file once, before the worker begins accepting work.
    pub(crate) async fn load(path: &Path) -> Result<Self> {
        let mut bytes = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::fs::File::open(path)
                .await?
                .take(1_048_577)
                .read_to_end(&mut bytes)
                .await
        })
        .await??;
        ensure!(
            bytes.len() <= 1_048_576,
            "evaluator HTTP configuration exceeds limit"
        );
        let config: Config = serde_json::from_slice(&bytes)?;
        Self::configured(config)
    }
    fn configured(config: Config) -> Result<Self> {
        ensure!(
            (1..=64 * 1024 * 1024).contains(&config.request_bytes)
                && (1..=64 * 1024 * 1024).contains(&config.response_bytes),
            "invalid evaluator HTTP body ceiling"
        );
        ensure!(
            (1..=300_000).contains(&config.request_timeout_ms)
                && (1..=3_600_000).contains(&config.total_time_ms),
            "invalid evaluator HTTP time ceiling"
        );
        ensure!(
            (1..=1024).contains(&config.requests) && (1..=1024).contains(&config.concurrency),
            "invalid evaluator HTTP request ceiling"
        );
        ensure!(
            config.bindings.len() <= 4096,
            "too many evaluator credential bindings"
        );
        let mut seen = std::collections::BTreeSet::new();
        for binding in &config.bindings {
            credentials::validate_reference(&binding.credential)
                .map_err(|_| anyhow::anyhow!("invalid evaluator credential reference"))?;
            // Reuse the grant grammar without granting network access. Actual
            // permission comes exclusively from the attempt's policy snapshot.
            Snapshot::new(
                binding.evaluator_id,
                vec![vigilo::evaluator_http::Rule {
                    method: binding.method.clone(),
                    uri: binding.uri.clone(),
                    action: vigilo::evaluator_http::Action::Allow,
                    networks: vec!["0.0.0.0/0".into(), "::/0".into()],
                }],
            )?;
            ensure!(
                seen.insert((
                    binding.evaluator_id,
                    binding.method.clone(),
                    canonical(&binding.uri)?
                )),
                "duplicate evaluator credential binding"
            );
        }
        Ok(Self {
            permits: Arc::new(Semaphore::new(config.concurrency)),
            config,
        })
    }
    pub(crate) fn invocation(
        &self,
        policy: Arc<Snapshot>,
        provider: Arc<Provider>,
        execution: Execution,
    ) -> Result<(Arc<Invocation>, Cancellation)> {
        let limits = Limits {
            request_bytes: self.config.request_bytes,
            response_bytes: self.config.response_bytes,
            request_timeout: Duration::from_millis(self.config.request_timeout_ms),
            total_time: Duration::from_millis(self.config.total_time_ms),
            requests: self.config.requests,
        };
        let session = Session::new(policy.clone(), policy.evaluator_id(), limits)
            .map_err(anyhow::Error::msg)?
            .with_concurrency(self.permits.clone());
        let resolver = Resolver {
            bindings: self
                .config
                .bindings
                .iter()
                .filter(|b| b.evaluator_id == policy.evaluator_id())
                .cloned()
                .collect(),
            provider,
            execution,
            authority: Authority::Evaluator {
                evaluator_id: policy.evaluator_id(),
                policy_revision: policy.revision().to_owned(),
            },
            cache: Mutex::new(BTreeMap::new()),
        };
        let (cancel, cancelled) = watch::channel(false);
        Ok((
            Arc::new(Invocation {
                session,
                resolver,
                runtime: tokio::runtime::Handle::current(),
                cancelled,
            }),
            Cancellation(cancel),
        ))
    }
}
fn canonical(uri: &str) -> Result<String> {
    let mut uri = reqwest::Url::parse(uri)?;
    uri.set_query(None);
    Ok(uri.to_string())
}
struct Resolver {
    bindings: Vec<Binding>,
    provider: Arc<Provider>,
    execution: Execution,
    authority: Authority,
    cache: Mutex<BTreeMap<usize, Resolved>>,
}
#[async_trait::async_trait]
impl Credentials for Resolver {
    async fn headers(&self, method: &str, uri: &str) -> Result<HeaderMap, &'static str> {
        let destination = canonical(uri).map_err(|_| "invalid credential destination")?;
        let Some((index, binding)) =
            self.bindings.iter().enumerate().find(|(_, b)| {
                b.method == method && canonical(&b.uri).is_ok_and(|u| u == destination)
            })
        else {
            return Ok(HeaderMap::new());
        };
        let mut cache = self.cache.lock().await;
        if cache
            .get(&index)
            .is_none_or(|value| value.check_validity().is_err())
        {
            cache.remove(&index);
            let value = self
                .provider
                .resolve_authorized(
                    &binding.credential,
                    uri,
                    &BTreeMap::new(),
                    self.execution.clone(),
                    self.authority.clone(),
                )
                .await
                .map_err(|_| "evaluator credential resolution failed")?;
            cache.insert(index, value);
        }
        let value = &cache[&index];
        value
            .check_validity()
            .map_err(|_| "evaluator credential expired")?;
        let mut headers = HeaderMap::new();
        headers.insert(value.name.clone(), value.value.clone());
        Ok(headers)
    }
}
/// Dropping the owning async worker task interrupts pending host HTTP work.
pub(crate) struct Cancellation(watch::Sender<bool>);
impl Drop for Cancellation {
    fn drop(&mut self) {
        let _ = self.0.send(true);
    }
}
/// Host-only state: never serialized into evaluator input or persisted evidence.
pub(crate) struct Invocation {
    session: Session,
    resolver: Resolver,
    runtime: tokio::runtime::Handle,
    cancelled: watch::Receiver<bool>,
}
impl std::fmt::Debug for Invocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EvaluatorHttpInvocation")
    }
}
impl vigilo::evaluator_http::transport::BlockingTransport for Invocation {
    /// Called from blocking Wasm execution; scheduling uses the worker's runtime.
    fn send(&self, request: Request) -> Result<Response, String> {
        self.runtime
            .block_on(async {
                let mut cancelled = self.cancelled.clone();
                if *cancelled.borrow() {
                    return Err("evaluator HTTP invocation cancelled");
                }
                tokio::select! {
                    biased;
                    _ = cancelled.changed() => Err("evaluator HTTP invocation cancelled"),
                    result = self.session.send_resolving(request, &self.resolver) => result,
                }
            })
            .map_err(str::to_owned)
    }
}

#[cfg(all(test, feature = "evaluator-http-services"))]
#[path = "evaluator_requests/service_tests.rs"]
mod service_tests;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn operator_configuration_is_bounded_and_explicit() {
        assert!(Service::configured(Config::default()).is_ok());
        for config in [
            Config {
                requests: 0,
                ..Default::default()
            },
            Config {
                concurrency: 1025,
                ..Default::default()
            },
            Config {
                request_bytes: 0,
                ..Default::default()
            },
            Config {
                response_bytes: 64 * 1024 * 1024 + 1,
                ..Default::default()
            },
            Config {
                total_time_ms: 0,
                ..Default::default()
            },
            Config {
                request_timeout_ms: 300_001,
                ..Default::default()
            },
        ] {
            assert!(Service::configured(config).is_err());
        }
        assert!(serde_json::from_str::<Config>(r#"{"fallback":true}"#).is_err());
    }
    #[test]
    fn bindings_reject_ambiguous_destinations_and_canonical_duplicates() {
        let binding = Binding {
            evaluator_id: Uuid::nil(),
            method: "POST".into(),
            uri: "https://example.test/judge".into(),
            credential: "judge".into(),
        };
        assert!(
            Service::configured(Config {
                bindings: vec![binding.clone()],
                ..Default::default()
            })
            .is_ok()
        );
        let mut duplicate = binding.clone();
        duplicate.uri = "https://example.test:443/judge".into();
        assert!(
            Service::configured(Config {
                bindings: vec![binding.clone(), duplicate],
                ..Default::default()
            })
            .is_err()
        );
        for uri in [
            "https://example.test/judge?key=secret",
            "https://example.test/%2fjudge",
            "https://user:secret@example.test/judge",
        ] {
            let mut invalid = binding.clone();
            invalid.uri = uri.into();
            assert!(
                Service::configured(Config {
                    bindings: vec![invalid],
                    ..Default::default()
                })
                .is_err()
            );
        }
    }
}
