//! Interface-independent evaluator registry and local testing operations.
//!
//! Trusted roots bind a publication namespace, fixed pools and optional test HTTP
//! authority. Caller handles retain transaction-local identity; registry row policy
//! remains deployment-owned. Prepared packages prove bytes and ABI before insertion.
//! Bounded metadata pages never load Wasm. Blocking compilation/execution hold
//! shared admission, never SQL connections, and install no interface process state.
mod db;
mod http;
mod types;
use std::sync::Arc;

pub use http::{
    TestAuthority,
    TestRequest,
    TestResolver,
};
use tokio_util::sync::CancellationToken;
pub use types::*;
use uuid::Uuid;

use super::{
    Error,
    UserClient,
    UserContext,
};
use crate::{
    context::{
        Context,
        wasm::{
            Component,
            Wasm,
        },
    },
    models::evaluator::EvaluatorDraft,
};

/// Operator limits and host callback are fixed at service construction, not in
/// test JSON. Defaults deny HTTP; finite Wasm and transport limits apply.
#[derive(Default, Clone)]
pub struct Options {
    pub wasm: super::WasmOptions,
    pub http: crate::evaluator_http::transport::Limits,
    pub test_authority: Option<Arc<dyn TestResolver>>,
}
struct Runtime {
    options: Options,
    wasm: tokio::sync::OnceCell<Wasm>,
    cli: Option<Context>,
    prepare: Arc<tokio::sync::Semaphore>,
    http_permits: Arc<tokio::sync::Semaphore>,
}
impl Runtime {
    async fn wasm(&self) -> anyhow::Result<Wasm> {
        if let Some(c) = &self.cli {
            return Ok(c.wasm().await?.clone());
        }
        Ok(self
            .wasm
            .get_or_try_init(|| async { Wasm::new(self.options.wasm.clone()) })
            .await?
            .clone())
    }
}
#[derive(Clone)]
enum Database {
    Router(Arc<crate::context::database::DatabaseRouter>),
    Pool(sqlx::PgPool),
}
/// Reuse this trusted factory across requests; it is not an authenticated caller.
#[derive(Clone)]
pub struct EvaluatorService {
    client: EvaluatorClient,
}
impl EvaluatorService {
    /// Derive another trusted publication binding while sharing runtime admission
    /// and the fixed registry pool. This is not namespace ownership authorization.
    pub fn for_namespace(&self, publication_namespace: &str) -> Result<Self, Error> {
        namespace(publication_namespace)?;
        let mut client = self.client.clone();
        client.namespace = publication_namespace.into();
        Ok(Self { client })
    }
    pub fn user(&self, caller: UserContext) -> EvaluatorClient {
        let mut c = self.client.clone();
        c.caller = Some(caller);
        c
    }
}
/// Caller-bound registry commands. Namespace binding grants no access by itself;
/// the host and database policies own publication and visibility authorization.
#[derive(Clone)]
pub struct EvaluatorClient {
    database: Database,
    namespace: String,
    caller: Option<UserContext>,
    runtime: Arc<Runtime>,
}
/// A verified package owns preparation admission until consumed or dropped.
/// It is not permission to publish into a namespace.
pub struct PreparedPackage {
    component: Component,
    _permit: tokio::sync::OwnedSemaphorePermit,
}
impl PreparedPackage {
    pub fn name(&self) -> &str {
        &self.component.name
    }
    pub fn version(&self) -> &str {
        &self.component.version
    }
    pub fn content_hash(&self) -> &str {
        &self.component.wasm_hash
    }
}
/// Decoded local fixture with host-generated response handles. It contains no
/// outbound authority and performs no agent call.
pub struct TestInput {
    inner: crate::contracts::evaluator::EvaluatorInput,
}
impl TestInput {
    pub fn from_json(json: &str) -> Result<Self, Error> {
        if json.len() > 32 * 1024 * 1024 {
            return Err(Error::InvalidInput("evaluator test input exceeds 32 MiB"));
        }
        Ok(Self {
            inner: crate::contracts::evaluator::test_input::decode_input(json)
                .map_err(Error::Dependency)?,
        })
    }
}
fn namespace(value: &str) -> Result<(), Error> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    {
        return Err(Error::InvalidInput("invalid evaluator namespace"));
    }
    Ok(())
}
fn runtime(options: Options, cli: Option<Context>) -> Result<Arc<Runtime>, Error> {
    options
        .wasm
        .validate()
        .map_err(|_| Error::InvalidConfiguration("invalid evaluator Wasm limits"))?;
    crate::evaluator_http::transport::Session::new(
        Arc::new(
            crate::evaluator_http::Snapshot::new(Uuid::nil(), vec![]).map_err(Error::Dependency)?,
        ),
        Uuid::nil(),
        options.http,
    )
    .map_err(Error::InvalidConfiguration)?;
    Ok(Arc::new(Runtime {
        options,
        wasm: Default::default(),
        cli,
        prepare: Arc::new(tokio::sync::Semaphore::new(1)),
        http_permits: Arc::new(tokio::sync::Semaphore::new(64)),
    }))
}
/// Bound the complete short SQL operation, including pool waits and rollback.
async fn bounded<T>(
    work: impl std::future::Future<Output = anyhow::Result<T>>,
) -> Result<T, Error> {
    let result = tokio::time::timeout(std::time::Duration::from_secs(30), work)
        .await
        .map_err(|_| {
            Error::Dependency(anyhow::anyhow!(
                "evaluator database operation deadline exceeded"
            ))
        })?;
    result.map_err(|e| match e.downcast::<Error>() {
        Ok(e) => e,
        Err(e) => Error::Dependency(e),
    })
}
impl super::Client {
    pub fn evaluators(
        &self,
        publication_namespace: &str,
        options: Options,
    ) -> Result<EvaluatorClient, Error> {
        namespace(publication_namespace)?;
        Ok(EvaluatorClient {
            database: Database::Router(self.database.clone()),
            namespace: publication_namespace.into(),
            caller: None,
            runtime: runtime(options, None)?,
        })
    }
}
impl UserClient {
    /// Validates independent shared-registry row protection once at trusted startup.
    /// Clones reuse this fixed role's pools; create a caller handle after authentication.
    pub async fn evaluators(
        &self,
        publication_namespace: &str,
        options: Options,
    ) -> Result<EvaluatorService, Error> {
        namespace(publication_namespace)?;
        let runtime = runtime(options, None)?;
        bounded(db::validate(&self.control)).await?;
        Ok(EvaluatorService {
            client: EvaluatorClient {
                database: Database::Pool(self.control.clone()),
                namespace: publication_namespace.into(),
                caller: None,
                runtime,
            },
        })
    }
}
impl EvaluatorClient {
    pub(crate) async fn operator(
        context: Context,
        publication_namespace: &str,
    ) -> anyhow::Result<Self> {
        namespace(publication_namespace)?;
        let pool = context.dbr().await?.control().await?.clone();
        Ok(Self {
            database: Database::Pool(pool),
            namespace: publication_namespace.into(),
            caller: None,
            runtime: runtime(Default::default(), Some(context))?,
        })
    }
    async fn connection(&self) -> anyhow::Result<db::Connection> {
        let pool = match &self.database {
            Database::Pool(p) => p,
            Database::Router(r) => r.control().await?,
        };
        db::Connection::open(pool, self.caller).await
    }
    /// Streams at most 66 MiB into private scratch before verification/compilation.
    /// No input-selected server path is opened; cancellation releases receipt work.
    pub async fn prepare_package<R: tokio::io::AsyncRead + Unpin + Send>(
        &self,
        reader: R,
        cancel: &CancellationToken,
    ) -> Result<PreparedPackage, Error> {
        use tokio::io::{
            AsyncReadExt,
            AsyncWriteExt,
        };
        let permit = tokio::select! {biased;_=cancel.cancelled()=>return Err(Error::Cancelled),p=self.runtime.prepare.clone().acquire_owned()=>p.map_err(|_|Error::Conflict("evaluator preparation closed"))?};
        let file = tempfile::NamedTempFile::new().map_err(|e| Error::Dependency(e.into()))?;
        let mut out =
            tokio::fs::File::from_std(file.reopen().map_err(|e| Error::Dependency(e.into()))?);
        const CAP: u64 = 66 * 1024 * 1024;
        let mut reader = reader.take(CAP + 1);
        let length = tokio::select! {biased;_=cancel.cancelled()=>return Err(Error::Cancelled),r=tokio::io::copy(&mut reader,&mut out)=>r.map_err(|e|Error::Dependency(e.into()))?};
        if length > CAP {
            return Err(Error::InvalidInput("evaluator package exceeds 66 MiB"));
        }
        out.flush().await.map_err(|e| Error::Dependency(e.into()))?;
        drop(out);
        let wasm = self.runtime.wasm().await.map_err(Error::Dependency)?;
        let work = tokio::task::spawn_blocking(move || {
            let input = crate::evaluator_package::load_archive(file.reopen()?)?;
            let component = wasm.prepare_portable(input)?;
            Ok::<_, anyhow::Error>(PreparedPackage {
                component,
                _permit: permit,
            })
        });
        tokio::select! {biased;_=cancel.cancelled()=>Err(Error::Cancelled),r=work=>r.map_err(|e|Error::Dependency(e.into()))?.map_err(Error::Dependency)}
    }
    /// Publishes only this service's trusted namespace; a same-content replay leaves
    /// metadata/state unchanged. No transaction spans compilation or artifact receipt.
    pub async fn publish(&self, prepared: PreparedPackage) -> Result<Publication, Error> {
        self.publish_component(prepared.component).await
    }
    pub(crate) async fn publish_component(
        &self,
        component: Component,
    ) -> Result<Publication, Error> {
        let draft = EvaluatorDraft {
            namespace: self.namespace.clone(),
            name: component.name,
            version: component.version,
            content_hash: component.wasm_hash,
            wasm_bytes: component.wasm_bytes,
            interface_name: component.interface_name,
            interface_version: component.interface_version,
            wit_world: component.wit_world,
            abi_contract_hash: component.abi.contract_hash,
            abi_adapter: component.abi.adapter,
            runtime: component.runtime,
            runtime_version: component.runtime_version,
            runtime_fingerprint: component.runtime_fingerprint,
            description: component.description,
            tags: component.tags,
            metadata: component.metadata,
        };
        bounded(async {
            let mut c = self.connection().await?;
            let outcome = crate::evaluators::publish_draft_on(&mut c, draft).await?;
            let reused = matches!(
                outcome,
                crate::evaluators::PublishEvaluatorOutcome::Skipped(_)
            );
            let evaluator = match outcome {
                crate::evaluators::PublishEvaluatorOutcome::Inserted(e)
                | crate::evaluators::PublishEvaluatorOutcome::Skipped(e) => e,
            };
            c.finish().await?;
            Ok(Publication {
                evaluator: evaluator.into(),
                reused,
            })
        })
        .await
    }
    /// Reads at most 8 MiB of current metadata without downloading Wasm bytes.
    pub async fn inspect(&self, id: &EvaluatorId) -> Result<Descriptor, Error> {
        bounded(async {
            let mut c = self.connection().await?;
            let value = db::show(&mut c, id).await?;
            c.finish().await?;
            Ok(value)
        })
        .await
    }
    /// Reads a logical metadata page under current authority; filter text retains
    /// SQL ILIKE wildcard semantics and versions use registry text ordering.
    pub async fn search(
        &self,
        namespace_filter: &str,
        query: Option<&str>,
        limits: PageLimits,
        cursor: Option<&Cursor>,
    ) -> Result<Page, Error> {
        namespace(namespace_filter)?;
        if query.is_some_and(|q| q.len() > 4096) {
            return Err(Error::InvalidInput("evaluator search exceeds limit"));
        }
        if let Some(cursor) = cursor {
            cursor.position(namespace_filter, query)?;
        }
        bounded(async {
            let mut c = self.connection().await?;
            let page = db::page(&mut c, namespace_filter, query, limits, cursor).await?;
            c.finish().await?;
            Ok(page)
        })
        .await
    }
    /// Changes lifecycle fields only. Removed is terminal; reasons are at most
    /// 64 KiB. Hidden, missing and terminal versions return no successful mutation.
    pub async fn set_state(
        &self,
        id: &EvaluatorId,
        state: State,
        reason: Option<String>,
    ) -> Result<(), Error> {
        if reason.as_ref().is_some_and(|r| r.len() > 64 * 1024) {
            return Err(Error::InvalidInput("evaluator state reason exceeds limit"));
        }
        bounded(async {
            let mut c = self.connection().await?;
            let changed = crate::db::tables::evaluators::update_evaluator_state(
                &mut *c,
                &id.namespace,
                &id.name,
                &id.version,
                &crate::models::evaluator::EvaluatorPatch {
                    state: state.into(),
                    state_reason: reason,
                },
            )
            .await?;
            anyhow::ensure!(
                changed == 1,
                "evaluator is absent, inaccessible or permanently removed"
            );
            c.finish().await?;
            Ok(())
        })
        .await
    }

    /// Validates current access/state and pinned bytes before creating test authority.
    /// No connection is held across compilation, Wasm or host/network callbacks.
    pub async fn test(
        &self,
        id: &EvaluatorId,
        input: TestInput,
        cancel: &CancellationToken,
    ) -> Result<TestResult, Error> {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let (e, bytes) = bounded(async {
            let mut c = self.connection().await?;
            let v = db::artifact(&mut c, id).await?;
            c.finish().await?;
            Ok(v)
        })
        .await?;
        if matches!(e.state, State::Disabled | State::Removed) {
            return Err(Error::Conflict(
                "evaluator cannot be tested in its current state",
            ));
        }
        if blake3::hash(&bytes).to_hex().as_str() != e.content_hash {
            return Err(Error::Conflict("evaluator artifact digest mismatch"));
        }
        let wasm = self.runtime.wasm().await.map_err(Error::Dependency)?;
        let permit = tokio::select! {biased;_=cancel.cancelled()=>return Err(Error::Cancelled),p=wasm.acquire_evaluation_permit()=>p.map_err(Error::Dependency)?};
        let token = cancel.child_token();
        let _cancel_on_drop = token.clone().drop_guard();
        let mut input = input.inner;
        let abi = crate::contracts::evaluator_abi::EvaluatorAbiIdentity {
            package: e
                .interface_name
                .rsplit_once('/')
                .ok_or(Error::InvalidInput("invalid evaluator ABI interface"))?
                .0
                .into(),
            interface: e.interface_name.rsplit_once('/').unwrap().1.into(),
            world: e.wit_world.clone(),
            version: e.interface_version.clone(),
            contract_hash: e.abi_contract_hash.clone(),
            adapter: e.abi_adapter.clone(),
        };
        let compile_runtime = wasm.clone();
        let compile_token = token.clone();
        let compile = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            anyhow::ensure!(!compile_token.is_cancelled(), "evaluator test cancelled");
            let component = compile_runtime.compile_evaluator(&bytes, &abi)?;
            Ok::<_, anyhow::Error>((component, _permit))
        });
        let (component, permit) = tokio::select! {biased;_=cancel.cancelled()=>return Err(Error::Cancelled),r=compile=>r.map_err(|e|Error::Dependency(e.into()))?.map_err(Error::Dependency)?};
        if let Some(resolver) = &self.runtime.options.test_authority {
            let request = TestRequest::new(self.caller, &e);
            let (policy, bridge) = http::resolve(
                resolver.as_ref(),
                request,
                self.runtime.options.http,
                self.runtime.http_permits.clone(),
                &token,
            )
            .await?;
            input.http_policy = Some(policy);
            input.http_invocation = Some(bridge);
        }
        let work = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            anyhow::ensure!(!token.is_cancelled(), "evaluator test cancelled");
            let output = wasm.test_evaluator_component(&component, input)?;
            anyhow::ensure!(
                output.evaluator.namespace == e.namespace
                    && output.evaluator.name == e.name
                    && output.evaluator.version == e.version,
                "evaluator returned identity does not match registry"
            );
            anyhow::ensure!(
                output
                    .evaluator
                    .content_hash
                    .as_ref()
                    .is_none_or(|h| h == &e.content_hash)
                    && output
                        .evaluator
                        .interface_version
                        .as_ref()
                        .is_none_or(|v| v == &e.interface_version),
                "evaluator returned artifact or ABI identity mismatch"
            );
            Ok::<_, anyhow::Error>(TestResult {
                namespace: e.namespace,
                name: e.name,
                version: e.version,
                state: e.state,
                output,
            })
        });
        tokio::select! {biased;_=cancel.cancelled()=>Err(Error::Cancelled),r=work=>r.map_err(|e|Error::Dependency(e.into()))?.map_err(Error::Dependency)}
    }
}

#[cfg(test)]
mod postgres_tests;
#[cfg(test)]
mod tests;
