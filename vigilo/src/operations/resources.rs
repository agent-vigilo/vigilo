//! Trusted host bindings for immutable input storage and outbound credentials.
//!
//! Core constructs requests from scoped preparation or verified accepted work.
//! The host selects bindings and supplies live credential material; this module
//! owns neither authentication policy nor a secret store. Replies remain bound to
//! their exact request. Injected resolution has no file/environment fallback.
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    time::SystemTime,
};

use uuid::Uuid;

/// Host-verified purpose for outbound credential use.
#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[non_exhaustive]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CredentialAuthority {
    Agent,
    Evaluator {
        evaluator_id: Uuid,
        policy_revision: String,
    },
}
use crate::{
    blob_storage::config::OwnerConfig,
    db::scope::UserContext,
    evaluator_requests::Service,
};

/// A bounded asynchronous host resolution. Core imposes its own deadline.
pub type Resolution<'a, T> = Pin<Box<dyn Future<Output = Result<T, Error>> + Send + 'a>>;

/// Safe provider failures; do not put secret material in diagnostics.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("resource access denied")]
    Denied,
    #[error("resource provider unavailable")]
    Unavailable,
    #[error("invalid resource binding")]
    Invalid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Identity {
    Preparation {
        caller: UserContext,
        run_id: Uuid,
        dataset_upload_id: Uuid,
    },
    Accepted {
        tenant_id: Uuid,
        run_id: Uuid,
        plan_digest: String,
    },
}

/// Read-only host request. Construction is restricted to core workflows; the
/// identity is not deserialized from command input or a broker payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    identity: Identity,
    binding_hash: String,
}
impl Request {
    pub(crate) fn preparation(
        caller: UserContext,
        run_id: Uuid,
        dataset_upload_id: Uuid,
        binding_hash: String,
    ) -> Self {
        Self {
            identity: Identity::Preparation {
                caller,
                run_id,
                dataset_upload_id,
            },
            binding_hash,
        }
    }
    pub(crate) fn accepted(
        tenant_id: Uuid,
        run_id: Uuid,
        plan_digest: &str,
        binding_hash: String,
    ) -> Result<Self, Error> {
        if run_id.is_nil()
            || plan_digest.len() != 64
            || !plan_digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(Error::Invalid);
        }
        Ok(Self {
            identity: Identity::Accepted {
                tenant_id,
                run_id,
                plan_digest: plan_digest.into(),
            },
            binding_hash,
        })
    }
    #[doc(hidden)]
    pub fn tenant_id(&self) -> Uuid {
        match self.identity {
            Identity::Preparation { caller, .. } => caller.tenant_id(),
            Identity::Accepted { tenant_id, .. } => tenant_id,
        }
    }
    /// Authenticated caller during preparation; absent for accepted system work.
    pub fn caller(&self) -> Option<UserContext> {
        match self.identity {
            Identity::Preparation { caller, .. } => Some(caller),
            _ => None,
        }
    }
    pub fn run_id(&self) -> Uuid {
        match self.identity {
            Identity::Preparation { run_id, .. } | Identity::Accepted { run_id, .. } => run_id,
        }
    }
    /// Prepared dataset identity; never fabricated for an accepted execution.
    pub fn dataset_upload_id(&self) -> Option<Uuid> {
        match self.identity {
            Identity::Preparation {
                dataset_upload_id, ..
            } => Some(dataset_upload_id),
            _ => None,
        }
    }
    /// Exact accepted immutable plan; absent while preparing work.
    pub fn plan_digest(&self) -> Option<&str> {
        match &self.identity {
            Identity::Accepted { plan_digest, .. } => Some(plan_digest),
            _ => None,
        }
    }
    /// Physical storage identity. This hash is not permission to read objects.
    pub fn binding_hash(&self) -> &str {
        &self.binding_hash
    }
}

/// Validated nonsecret bindings. Construct against the supplied request, not a
/// cached request from another execution. Secret configuration is not accepted.
pub struct Bindings {
    pub(crate) request: Request,
    pub(crate) owner: OwnerConfig,
    pub(crate) evaluator_http: Service,
}
impl Bindings {
    /// Storage uses the documented owner TOML without credential/environment
    /// variables. Evaluator HTTP takes a JSON binding array; process ceilings remain host-owned.
    pub fn new(
        request: &Request,
        storage_toml: &str,
        evaluator_http_json: &str,
    ) -> Result<Self, Error> {
        if storage_toml.len() > 64 * 1024 {
            return Err(Error::Invalid);
        }
        let owner: OwnerConfig = toml::from_str(storage_toml).map_err(|_| Error::Invalid)?;
        owner
            .validate(&Default::default())
            .map_err(|_| Error::Invalid)?;
        owner.validate_injected().map_err(|_| Error::Invalid)?;
        if owner.binding_hash().map_err(|_| Error::Invalid)? != request.binding_hash {
            return Err(Error::Invalid);
        }
        Ok(Self {
            request: request.clone(),
            owner,
            evaluator_http: Service::from_bindings_json(evaluator_http_json)
                .map_err(|_| Error::Invalid)?,
        })
    }
    /// Explicit private/read-only route to the same physical namespace. It is
    /// bounded to 64 KiB and validated like the canonical endpoint. It is not
    /// persisted and does not change the binding hash. No environment variable is consulted.
    pub fn with_read_endpoint(mut self, endpoint: String) -> Result<Self, Error> {
        self.owner = self
            .owner
            .with_read_endpoint(endpoint)
            .map_err(|_| Error::Invalid)?;
        Ok(self)
    }
}

/// Host-verified purpose plus allocated invocation identity. No case content or
/// profile-provided identity can select the authority used here.
#[derive(Clone, PartialEq, Eq)]
pub struct CredentialRequest {
    pub resources: Arc<Request>,
    pub execution_id: Uuid,
    pub attempt_id: Uuid,
    pub authority: CredentialAuthority,
    pub reference: String,
    pub destination: String,
}

/// Short-lived header material returned only to the execution host. Deliberately
/// lacks Debug/Serialize. Core rechecks identity, header, origin and expiry before use.
/// Header, origin and value share a 64 KiB material limit; value is at most 16 KiB.
pub struct Credential {
    pub request: CredentialRequest,
    pub allowed_origin: String,
    pub header: String,
    pub value: String,
    pub expires_at: chrono::DateTime<chrono::Utc>,
}

/// Short-lived S3 signing material. Deliberately lacks Debug/Serialize. The SDK
/// refreshes it through the same request-bound resolver; no ambient chain is used.
pub struct StorageCredentials {
    pub request: Request,
    pub authentication: StorageAuthentication,
    pub expires_at: SystemTime,
}

/// Provider-specific signing material. New storage adapters add explicit variants;
/// no response field names or credential formats are inferred.
#[non_exhaustive]
pub enum StorageAuthentication {
    S3 {
        access_key: String,
        secret_key: String,
        session_token: Option<String>,
    },
}

/// Host-owned dependency boundary, shared across workers. Implementations must
/// authorize the exact supplied context and avoid returning another request's
/// cached reply. Defaults deny credentials; no environment or local file fallback
/// exists. Core bounds calls; implementations must release resources on cancellation.
pub trait Resolver: Send + Sync {
    fn bindings(&self, request: Request) -> Resolution<'_, Bindings>;
    fn credential(&self, _request: CredentialRequest) -> Resolution<'_, Credential> {
        Box::pin(async { Err(Error::Denied) })
    }
    fn storage_credentials(&self, _request: Request) -> Resolution<'_, StorageCredentials> {
        Box::pin(async { Err(Error::Denied) })
    }
}
