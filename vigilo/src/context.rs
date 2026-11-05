//! Lazily initialized process context.
//!
//! Commands receive a cloned [`Context`] and request configured services on demand:
//! database router, HTTP client, message queue client, output buffer,
//! evaluator registry cache, and Wasm runtime. Worker credentials remain in a
//! separate process-local provider shared by context clones. Service modules in `context::*`
//! should remain thin initialization boundaries; domain behavior belongs in
//! runtime, db, mq, or command modules. Message-queue configuration is present
//! only for coordinator and worker commands.

use std::sync::Arc;

pub(crate) mod database;
pub(crate) mod http;
pub(crate) mod messaging;
pub(crate) mod output;
pub(crate) mod registry;
pub(crate) mod storage;
pub(crate) mod wasm;

struct ContextInner {
    http_policies: vigilo::evaluator_http::cache::Cache,
    pub dbr: database::Context,
    pub http: http::Context,
    pub mq: messaging::Context,
    pub out: output::Context,
    pub reg: registry::Context,
    pub wasm: wasm::Context,
    pub storage: storage::Context,
}

#[derive(Clone)]
pub(crate) struct Context(
    Arc<ContextInner>,
    crate::agent_client::limits::Limits,
    Arc<crate::agent_client::credentials::Provider>,
    Arc<crate::evaluator_requests::Service>,
);

impl Context {
    pub fn new(
        database_config: database::Config,
        messaging_config: Option<crate::mq::Config>,
        wasm_config: wasm::Config,
        output_format: output::OutputFormat,
    ) -> Self {
        Self(
            Arc::new(ContextInner {
                http_policies: Default::default(),
                storage: storage::Context::default(),
                dbr: database::Context {
                    config: database_config,
                    cell: Default::default(),
                },
                http: http::Context {
                    cell: Default::default(),
                },
                mq: messaging::Context {
                    config: messaging_config,
                    cell: Default::default(),
                },
                out: output::Context {
                    cell: Default::default(),
                    format: output_format,
                },
                reg: registry::Context {
                    cell: Default::default(),
                },
                wasm: wasm::Context {
                    cell: Default::default(),
                    config: wasm_config,
                },
            }),
            crate::agent_client::limits::Limits::default(),
            Arc::new(crate::agent_client::credentials::Provider::default()),
            Arc::new(crate::evaluator_requests::Service::default()),
        )
    }

    /// Reuses the application's shared router without initializing presentation or process state.
    pub(crate) fn for_operations(router: Arc<database::DatabaseRouter>) -> Self {
        let config = database::Config::new(
            router.uri.clone(),
            router.max_connections_per_pool,
            router.acquire_timeout,
            Default::default(),
            router.placement_config.clone(),
        );
        let mut context = Self::new(config, None, Default::default(), output::OutputFormat::Json);
        Arc::get_mut(&mut context.0)
            .expect("unshared context")
            .dbr
            .cell = tokio::sync::OnceCell::new_with(Some(router));
        context
    }

    pub(crate) fn with_evaluator_http(
        mut self,
        service: crate::evaluator_requests::Service,
    ) -> Self {
        self.3 = Arc::new(service);
        self
    }
    pub(crate) fn evaluator_http(
        &self,
        policy: Arc<vigilo::evaluator_http::Snapshot>,
        execution: crate::agent_client::credentials::Execution,
    ) -> anyhow::Result<(
        Arc<crate::evaluator_requests::Invocation>,
        crate::evaluator_requests::Cancellation,
    )> {
        self.3.invocation(policy, self.2.clone(), execution)
    }

    /// Configure freshness before sharing the process context.
    pub(crate) fn with_http_policy_max_age(mut self, seconds: u64) -> anyhow::Result<Self> {
        let cache = vigilo::evaluator_http::cache::Cache::new(
            std::time::Duration::from_secs(seconds),
            256,
        )?;
        Arc::get_mut(&mut self.0)
            .ok_or_else(|| {
                anyhow::anyhow!("policy cache must be configured before sharing context")
            })?
            .http_policies = cache;
        Ok(self)
    }

    /// Shared worker cache; callers freeze returned policies for one attempt.
    pub(crate) fn http_policies(&self) -> &vigilo::evaluator_http::cache::Cache {
        &self.0.http_policies
    }

    /// Set process-local agent HTTP ceilings before dispatching the command.
    pub(crate) fn with_agent_http_limits(
        mut self,
        limits: crate::agent_client::limits::Limits,
    ) -> Self {
        self.1 = limits;
        self
    }

    /// Attach operator-owned credential resolution without changing run profiles.
    pub(crate) fn with_agent_credentials(
        mut self,
        provider: crate::agent_client::credentials::Provider,
    ) -> Self {
        self.2 = Arc::new(provider);
        self
    }

    pub(crate) fn agent_credentials(&self) -> &crate::agent_client::credentials::Provider {
        &self.2
    }

    pub(crate) fn agent_http_limits(&self) -> crate::agent_client::limits::Limits {
        self.1
    }

    pub(crate) async fn storage(
        &self,
        binding: &str,
        limits: &crate::contracts::case_storage::FormatLimits,
    ) -> anyhow::Result<Arc<dyn crate::blob_storage::BlobStorage>> {
        self.0.storage.get(binding, limits).await
    }

    pub(crate) async fn dbr(&self) -> anyhow::Result<&database::DatabaseRouter> {
        self.0.dbr.get().await
    }

    pub async fn http(&self) -> anyhow::Result<&reqwest::Client> {
        self.0.http.get().await
    }

    pub async fn out(&self) -> anyhow::Result<&output::Buffer> {
        self.0.out.get().await
    }

    pub async fn mq(&self) -> anyhow::Result<&crate::mq::Client> {
        self.0.mq.get().await
    }

    pub async fn reg(
        &self,
    ) -> anyhow::Result<&moka::future::Cache<String, crate::evaluator_abi::PreparedEvaluator>> {
        self.0.reg.get().await
    }

    pub async fn wasm(&self) -> anyhow::Result<&wasm::Wasm> {
        self.0.wasm.get().await
    }
}
