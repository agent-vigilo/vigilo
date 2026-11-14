//! Request-bound host resource selection and S3 credential renewal.
//!
//! Preparation and verified accepted work have distinct identities. Replies must
//! match that identity and the pinned storage binding before adapters are built.
//! Credential callbacks are bounded, redacted and never fall back to process state.
use std::{
    sync::Arc,
    time::Duration,
};

use anyhow::{
    Result,
    ensure,
};
use aws_credential_types::provider::{
    ProvideCredentials,
    SharedCredentialsProvider,
    error::CredentialsError,
    future,
};
use tokio_util::sync::CancellationToken;

use super::Context;
use crate::{
    blob_storage::{
        BlobStorage,
        Budget,
        config::OwnerConfig,
    },
    contracts::case_storage::FormatLimits,
    operations::resources::{
        Request,
        Resolver,
    },
};

const TIMEOUT: Duration = Duration::from_secs(10);

/// Runtime-wide host admission with request-local immutable identity.
#[derive(Clone)]
pub(super) struct Configuration {
    resolver: Option<Arc<dyn Resolver>>,
    request: Option<Arc<Request>>,
    permits: Arc<tokio::sync::Semaphore>,
}
impl Default for Configuration {
    fn default() -> Self {
        Self {
            resolver: None,
            request: None,
            permits: Arc::new(tokio::sync::Semaphore::new(64)),
        }
    }
}

#[derive(Clone)]
struct StorageProvider {
    resolver: Arc<dyn Resolver>,
    request: Request,
    permits: Arc<tokio::sync::Semaphore>,
}
impl std::fmt::Debug for StorageProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HostStorageCredentials")
    }
}
impl ProvideCredentials for StorageProvider {
    fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        future::ProvideCredentials::new(async {
            let invalid =
                || CredentialsError::provider_error("host storage credential resolution failed");
            let value = tokio::time::timeout(TIMEOUT, async {
                let _permit = self
                    .permits
                    .acquire()
                    .await
                    .map_err(|_| crate::operations::resources::Error::Unavailable)?;
                self.resolver
                    .storage_credentials(self.request.clone())
                    .await
            })
            .await
            .map_err(|_| invalid())?
            .map_err(|_| invalid())?;
            let crate::operations::resources::StorageAuthentication::S3 {
                access_key,
                secret_key,
                session_token,
            } = value.authentication;
            let secret_valid = |value: &str| !value.is_empty() && value.len() <= 16_384;
            if value.request != self.request
                || value.expires_at <= std::time::SystemTime::now()
                || !secret_valid(&access_key)
                || !secret_valid(&secret_key)
                || session_token.as_deref().is_some_and(|v| !secret_valid(v))
            {
                return Err(invalid());
            }
            Ok(aws_credential_types::Credentials::new(
                access_key,
                secret_key,
                session_token,
                Some(value.expires_at),
                "host-resolver",
            ))
        })
    }
}
// Erase the transport future per context construction, bounding worker task frames.
type ExecutionResources<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<(Context, Arc<dyn BlobStorage>)>> + Send + 'a>,
>;

/// Per-run bindings contain no database, queue or process context. Keeping them
/// separate avoids recursively retaining a whole runtime inside the worker cache.
#[derive(Clone)]
pub(crate) struct Services {
    credentials: Arc<crate::agent_client::credentials::Provider>,
    evaluator_http: Arc<crate::evaluator_requests::Service>,
    request: Option<Arc<Request>>,
}
impl std::fmt::Debug for Services {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BoundResourceServices")
    }
}
impl Services {
    pub(crate) fn apply(&self, base: &Context) -> Context {
        let mut context = base.clone();
        context.2 = self.credentials.clone();
        context.3 = self.evaluator_http.clone();
        context.4.request = self.request.clone();
        context
    }
}

impl Context {
    pub(crate) fn resource_services(&self) -> Services {
        Services {
            credentials: self.2.clone(),
            evaluator_http: self.3.clone(),
            request: self.4.request.clone(),
        }
    }
    /// Bind a host resolver before constructing a hosted application runtime.
    pub(crate) fn with_resource_resolver(mut self, resolver: Arc<dyn Resolver>) -> Self {
        self.4.resolver = Some(resolver);
        self
    }
    pub(crate) fn uses_injected_resources(&self) -> bool {
        self.4.resolver.is_some()
    }
    pub(crate) fn accepted_resources(&self) -> Option<Arc<Request>> {
        self.4.request.clone().filter(|r| r.plan_digest().is_some())
    }

    /// Resolve outside SQL transactions. The host sees only verified ownership
    /// and pinned metadata. A reply cannot silently switch to standalone services.
    async fn injected_resources(
        &self,
        request: &Request,
        cancel: &CancellationToken,
    ) -> Result<(Self, OwnerConfig)> {
        ensure!(
            !request.tenant_id().is_nil(),
            "injected resources require explicit ownership"
        );
        let resolver = self
            .4
            .resolver
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("host resource resolver is not configured"))?;
        let bindings = tokio::select! {
            biased;
            _ = cancel.cancelled() => anyhow::bail!("resource resolution cancelled"),
            result = tokio::time::timeout(TIMEOUT, async {
                let _permit = self.4.permits.acquire().await.map_err(|_| crate::operations::resources::Error::Unavailable)?;
                resolver.bindings(request.clone()).await
            }) => result.map_err(|_| anyhow::anyhow!("resource resolution deadline exceeded"))??,
        };
        ensure!(
            bindings.request == *request,
            "resource provider returned different ownership"
        );
        ensure!(
            bindings.owner.binding_hash()? == request.binding_hash(),
            "resource provider changed storage binding"
        );
        let owner = bindings
            .owner
            .with_credentials(SharedCredentialsProvider::new(StorageProvider {
                resolver: resolver.clone(),
                request: request.clone(),
                permits: self.4.permits.clone(),
            }))?;
        let mut context = self.clone();
        context.2 = Arc::new(self.2.injected(resolver.clone()));
        context.3 = Arc::new(bindings.evaluator_http.bind_to(&self.3));
        context.4.request = Some(Arc::new(request.clone()));
        Ok((context, owner))
    }
    pub(crate) async fn preparation_resources(
        &self,
        request: Request,
        owner: Option<&OwnerConfig>,
        cancel: &CancellationToken,
    ) -> Result<(Self, OwnerConfig)> {
        ensure!(
            request.caller().is_some() && request.plan_digest().is_none(),
            "preparation resources require caller context"
        );
        if self.4.resolver.is_some() {
            return self.injected_resources(&request, cancel).await;
        }
        Ok((
            self.clone(),
            owner
                .ok_or_else(|| anyhow::anyhow!("standalone storage configuration is required"))?
                .clone(),
        ))
    }
    pub(crate) fn execution_resources<'a>(
        &'a self,
        request: Request,
        limits: &'a FormatLimits,
        cancel: &'a CancellationToken,
    ) -> ExecutionResources<'a> {
        Box::pin(async move {
            ensure!(
                request.plan_digest().is_some(),
                "execution resources require accepted work"
            );
            if self.4.resolver.is_some() {
                let (context, owner) = self.injected_resources(&request, cancel).await?;
                let store = owner
                    .reader(limits.clone(), Budget::standard(), cancel)
                    .await?;
                return Ok((context, Arc::from(store)));
            }
            let store = self.0.storage.get(request.binding_hash(), limits).await?;
            let mut context = self.clone();
            context.4.request = Some(Arc::new(request));
            Ok((context, store))
        })
    }
}

#[cfg(test)]
mod tests;
