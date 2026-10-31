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
        )
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
