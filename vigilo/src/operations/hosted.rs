//! Validated hosting construction with fixed database authority and shared services.
//!
//! Startup checks every supplied database binding before returning a runtime.
//! Caller handles expose only scoped operations; scheduler and worker handles are
//! obtained separately by the trusted host. No CLI parsing, process signals,
//! tracing subscriber, credential environment lookup or policy installation occurs.
use std::{
    sync::Arc,
    time::Duration,
};

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    ControlOutcome,
    DatabasePools,
    Error,
    ExecutionControl,
    RunClient,
    RunStatus,
    UserClient,
    UserContext,
    database_bindings::Authority,
};
pub use crate::{
    agent_client::limits::Limits as AgentHttpLimits,
    cli::commands::coordinator::CoordinatorRuntimeConfig as CoordinatorOptions,
    context::wasm::Config as WasmOptions,
    db::workflows::{
        run_creation::Config as CreationOptions,
        run_shard_summary::AggregationConfig as AggregationOptions,
    },
};
use crate::{
    cli::commands::{
        coordinator,
        worker,
    },
    context::Context,
};

/// Broker settings are deployment input, never ordinary operation parameters.
/// None selects the standard topology explicitly; no environment namespace is read.
#[derive(Clone)]
pub struct BrokerConfig {
    pub uri: String,
    pub namespace: Option<String>,
}
impl BrokerConfig {
    pub fn new(uri: String) -> Self {
        Self {
            uri,
            namespace: None,
        }
    }
    fn config(&self) -> Result<crate::mq::Config, Error> {
        let invalid = || Error::InvalidConfiguration("invalid explicit broker configuration");
        let uri = reqwest::Url::parse(&self.uri).map_err(|_| invalid())?;
        if !matches!(uri.scheme(), "amqp" | "amqps")
            || uri.host_str().is_none()
            || uri.username().is_empty()
            || uri.password().is_none_or(str::is_empty)
            || uri.fragment().is_some()
        {
            return Err(invalid());
        }
        if self.namespace.as_ref().is_some_and(|s| {
            s.is_empty()
                || s.len() > 128
                || !s
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        }) {
            return Err(invalid());
        }
        Ok(crate::mq::Config::explicit(
            self.uri.clone(),
            self.namespace.as_deref(),
        ))
    }
}

/// Trusted runtime configuration. Resources are required: omission cannot select
/// standalone mode. Credentials intentionally have no Debug implementation.
pub struct HostedConfig {
    pub databases: DatabasePools,
    pub resources: Arc<dyn super::resources::Resolver>,
    pub broker: BrokerConfig,
    pub default_execution_alias: String,
    pub shard_assignment_policy: String,
    pub coordinator: CoordinatorOptions,
    pub wasm: WasmOptions,
    pub agent_http: AgentHttpLimits,
    /// Local development only; HTTPS remains required unless explicitly enabled.
    /// The exception accepts literal loopback addresses, never arbitrary HTTP hosts.
    pub agent_credentials_allow_http_loopback: bool,
    pub evaluator_http: crate::evaluator_http::transport::Limits,
    pub evaluator_http_concurrency: usize,
    pub policy_max_age_seconds: u64,
    pub worker_max_inflight_chunks: u16,
    pub worker_host: Option<String>,
}
impl HostedConfig {
    pub fn new(
        databases: DatabasePools,
        resources: Arc<dyn super::resources::Resolver>,
        broker: BrokerConfig,
        default_execution_alias: String,
        shard_assignment_policy: String,
    ) -> Self {
        Self {
            databases,
            resources,
            broker,
            default_execution_alias,
            shard_assignment_policy,
            coordinator: Default::default(),
            wasm: Default::default(),
            agent_http: Default::default(),
            agent_credentials_allow_http_loopback: false,
            evaluator_http: Default::default(),
            evaluator_http_concurrency: 64,
            policy_max_age_seconds: 30,
            worker_max_inflight_chunks: 1,
            worker_host: None,
        }
    }
}

struct Inner {
    databases: DatabasePools,
    user: Context,
    scheduler: Context,
    execution: Context,
    coordinator: CoordinatorOptions,
    max_inflight: u16,
    worker_host: Option<String>,
}
/// Trusted host root. Do not hand this service-construction authority to a user
/// request; derive a caller handle after authenticating that request instead.
#[derive(Clone)]
pub struct HostedRuntime(Arc<Inner>);
impl HostedRuntime {
    /// Validate nonsecret limits first, then open/revalidate every fixed pool.
    /// Broker connections and Wasm compilation stay lazy. Failure returns no
    /// usable runtime and does not close independently held pool clones.
    pub async fn new(config: HostedConfig) -> Result<Self, Error> {
        config.coordinator.validate()?;
        let bounded = |v: u64| (1..=64 * 1024 * 1024).contains(&v);
        if !(1..=1024).contains(&config.worker_max_inflight_chunks)
            || !(1..=3600).contains(&config.policy_max_age_seconds)
            || !bounded(config.agent_http.max_request_bytes)
            || !bounded(config.agent_http.max_response_bytes)
            || config
                .worker_host
                .as_ref()
                .is_some_and(|s| s.is_empty() || s.len() > 255 || s.chars().any(char::is_control))
        {
            return Err(Error::InvalidConfiguration(
                "invalid runtime limits or worker identity",
            ));
        }
        config
            .wasm
            .validate()
            .map_err(|_| Error::InvalidConfiguration("invalid Wasm runtime limits"))?;
        let messaging = config.broker.config()?;
        let http = crate::evaluator_requests::Service::with_limits(
            config.evaluator_http,
            config.evaluator_http_concurrency,
        )
        .map_err(|_| Error::InvalidConfiguration("invalid evaluator HTTP limits"))?;
        let router = |authority| {
            config.databases.router(
                authority,
                config.default_execution_alias.clone(),
                config.shard_assignment_policy.clone(),
            )
        };
        let user = Context::for_operations(router(Authority::User)?)
            .with_agent_credentials(
                crate::agent_client::credentials::Provider::default()
                    .with_http_loopback(config.agent_credentials_allow_http_loopback),
            )
            .with_resource_resolver(config.resources)
            .with_runtime_dependencies(messaging, config.wasm)
            .with_agent_http_limits(config.agent_http)
            .with_evaluator_http(http)
            .with_http_policy_max_age(config.policy_max_age_seconds)
            .map_err(|_| Error::InvalidConfiguration("invalid policy freshness"))?;
        let scheduler = user
            .authority_view(router(Authority::Scheduler)?, config.policy_max_age_seconds)
            .map_err(Error::Dependency)?;
        let execution = user
            .authority_view(router(Authority::Execution)?, config.policy_max_age_seconds)
            .map_err(Error::Dependency)?;
        config.databases.validate().await?;
        Ok(Self(Arc::new(Inner {
            databases: config.databases,
            user,
            scheduler,
            execution,
            coordinator: config.coordinator,
            max_inflight: config.worker_max_inflight_chunks,
            worker_host: config.worker_host,
        })))
    }
    /// Caller identity is fixed for this handle. Keep it with its prepared values
    /// until acceptance; clones share that proof identity and all runtime budgets.
    pub fn user(&self, caller: UserContext) -> HostedUser {
        HostedUser {
            status: self.0.databases.user_client(),
            runs: RunClient::from_hosted(
                self.0.user.clone(),
                self.0.coordinator.run_creation,
                caller,
            ),
            context: caller,
            control: self.0.user.clone(),
        }
    }
    pub fn coordinator(&self) -> Coordinator {
        Coordinator {
            context: self.0.scheduler.clone(),
            id: Uuid::now_v7(),
            options: self.0.coordinator.clone(),
            active: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
    pub fn worker(&self) -> Worker {
        Worker {
            service: Arc::new(worker::Service::new(
                self.0.execution.clone(),
                Uuid::now_v7(),
                self.0.worker_host.clone(),
            )),
            max_inflight: self.0.max_inflight,
            active: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
    /// Closes the supplied shared pools, including external clones. Stop and join
    /// services before closing; in-flight borrowers are allowed to return first.
    pub async fn close(&self) {
        self.0.databases.close().await;
    }
}

/// Scoped request operations. This handle contains no service authority.
#[derive(Clone)]
pub struct HostedUser {
    status: UserClient,
    runs: RunClient,
    context: UserContext,
    control: Context,
}
impl HostedUser {
    pub fn runs(&self) -> &RunClient {
        &self.runs
    }
    pub async fn run_status(&self, id: Uuid) -> Result<RunStatus, Error> {
        self.status.run_status(self.context, id).await
    }
    /// Durable intent only; scheduler delivery and quiescence are asynchronous.
    pub async fn control_run(
        &self,
        id: Uuid,
        next: ExecutionControl,
    ) -> Result<ControlOutcome, Error> {
        let pool = self
            .control
            .dbr()
            .await
            .map_err(Error::Dependency)?
            .control()
            .await
            .map_err(Error::Dependency)?;
        crate::db::workflows::run_control::request_as(pool, self.context, id, next)
            .await
            .map_err(Error::Dependency)
    }
    pub async fn run_control_status(&self, id: Uuid) -> Result<ControlOutcome, Error> {
        let pool = self
            .control
            .dbr()
            .await
            .map_err(Error::Dependency)?
            .control()
            .await
            .map_err(Error::Dependency)?;
        crate::db::workflows::run_control::progress_as(pool, self.context, id)
            .await
            .map_err(Error::Dependency)
    }
}

/// Scheduler authority held by the trusted host, never derived from a user handle.
/// Clones share one service; concurrent invocations return a conflict.
#[derive(Clone)]
pub struct Coordinator {
    context: Context,
    id: Uuid,
    options: CoordinatorOptions,
    active: Arc<tokio::sync::Mutex<()>>,
}
impl Coordinator {
    /// One bounded existing orchestration pass. Placement failures retain the
    /// existing containment/logging behavior; success is not an all-runs-complete claim.
    pub async fn once(&self) -> Result<(), Error> {
        let _active = self
            .active
            .try_lock()
            .map_err(|_| Error::Conflict("coordinator is already running"))?;
        coordinator::cycle(self.context.clone(), self.id, &self.options)
            .await
            .map_err(Error::Dependency)
    }
    /// The host owns cancellation and task joining. Stop admits no new cycles;
    /// an active bounded cycle drains before returning. No signal handlers install.
    pub async fn run(&self, cancel: CancellationToken) -> Result<(), Error> {
        let _active = self
            .active
            .try_lock()
            .map_err(|_| Error::Conflict("coordinator is already running"))?;
        let mut interval = tokio::time::interval(Duration::from_secs(self.options.tick_seconds));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { biased;
                _ = cancel.cancelled() => return Ok(()),
                _ = interval.tick() => {
                    coordinator::cycle(self.context.clone(), self.id, &self.options)
                        .await.map_err(Error::Dependency)?;
                }
            }
        }
    }
}
/// Execution authority with persistent per-service caches and worker identity.
/// Clones share one service; concurrent invocations return a conflict.
#[derive(Clone)]
pub struct Worker {
    service: Arc<worker::Service>,
    max_inflight: u16,
    active: Arc<tokio::sync::Mutex<()>>,
}
impl Worker {
    /// Process at most one available broker delivery, including retry/quarantine
    /// settlement. Zero means no delivery was available. Dropping stops lease renewal.
    pub async fn once(&self) -> Result<usize, Error> {
        let _active = self
            .active
            .try_lock()
            .map_err(|_| Error::Conflict("worker is already running"))?;
        self.service.once().await.map_err(Error::Dependency)
    }
    /// Drain admitted chunks after cancellation; no new deliveries are admitted.
    /// The host must join this future before closing shared dependencies.
    pub async fn run(&self, cancel: CancellationToken) -> Result<(), Error> {
        let _active = self
            .active
            .try_lock()
            .map_err(|_| Error::Conflict("worker is already running"))?;
        self.service
            .run(self.max_inflight, cancel)
            .await
            .map_err(Error::Dependency)
    }
}

#[cfg(test)]
mod tests;
