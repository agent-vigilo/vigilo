//! Invocation delivery for trusted library hosts. The host authenticates the
//! callback and supplies its verified routing identity. Core fences each write
//! against local shard ownership and the current attempt.

use chrono::{
    DateTime,
    Utc,
};
use uuid::Uuid;
pub use vigilo::invocation::wire::Callback;

use super::{
    Error,
    resources,
};
use crate::{
    context::Context,
    db::workflows::{
        execution_processing::{
            deferred,
            deferred_jobs,
        },
        local_shard_admission::LocalShardRouteHint,
    },
    deferred_runtime,
};

/// Verified routing identity furnished to the host at credential issuance.
/// Ingress must recover it from trusted authorization state, never the outcome
/// body. Use the current placement and write epoch if a shard has moved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Target {
    pub tenant_id: Uuid,
    pub run_id: Uuid,
    pub run_shard: i16,
    pub chunk_id: Uuid,
    pub attempt_id: Uuid,
    pub invocation_id: Uuid,
    pub database_alias: String,
    pub write_epoch: i64,
}

impl Target {
    pub(crate) fn new(key: deferred::Attempt, database_alias: &str, invocation_id: Uuid) -> Self {
        Self {
            tenant_id: key.tenant_id,
            run_id: key.run_id,
            run_shard: key.run_shard,
            chunk_id: key.chunk_id,
            attempt_id: key.attempt_id,
            invocation_id,
            database_alias: database_alias.into(),
            write_epoch: key.write_epoch,
        }
    }

    pub(crate) fn attempt(&self) -> deferred::Attempt {
        deferred::Attempt {
            tenant_id: self.tenant_id,
            run_id: self.run_id,
            run_shard: self.run_shard,
            chunk_id: self.chunk_id,
            attempt_id: self.attempt_id,
            write_epoch: self.write_epoch,
        }
    }
}

/// The host returns an HTTPS destination and an invocation-scoped bearer
/// credential that expires by the supplied deadline. Issuance must be
/// idempotent for the logical invocation across retries and placement changes.
/// Vigilo retains a request digest so changed credentials cannot alter a retry.
pub trait CallbackIssuer: Send + Sync {
    fn issue(&self, target: Target, deadline: DateTime<Utc>)
    -> resources::Resolution<'_, Callback>;
}

/// An acknowledgment of the committed result or a rejection without mutation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Completion {
    Accepted,
    Duplicate,
    Ineligible,
}

/// Trusted runtime handle for authenticated callback acceptance and scheduled
/// invocation maintenance. It exposes no reads of execution evidence.
#[derive(Clone)]
pub struct Service {
    context: Context,
}

impl Service {
    pub(crate) fn new(context: Context) -> Self {
        Self { context }
    }

    async fn database(&self, target: &Target) -> Result<sqlx::PgPool, Error> {
        if [
            target.tenant_id,
            target.run_id,
            target.chunk_id,
            target.attempt_id,
            target.invocation_id,
        ]
        .into_iter()
        .any(|id| id.is_nil())
            || !(0..crate::models::run_chunk::RUN_SHARD_COUNT).contains(&target.run_shard)
            || target.write_epoch <= 0
        {
            return Err(Error::InvalidInput("invalid invocation target"));
        }
        Ok(self
            .context
            .dbr()
            .await
            .map_err(Error::Dependency)?
            .execution_write_route(LocalShardRouteHint {
                run_id: target.run_id,
                run_shard: target.run_shard,
                database_alias: target.database_alias.clone(),
                write_epoch: target.write_epoch,
            })
            .await
            .map_err(Error::Dependency)?
            .pool)
    }

    /// Commits a final native outcome after the host verifies the bearer token,
    /// its scope and expiry. Bound the body while reading it at ingress. Retry
    /// with identical bytes after an uncertain acknowledgment.
    pub async fn complete(&self, target: &Target, body: &[u8]) -> Result<Completion, Error> {
        if body.is_empty() || body.len() > vigilo::invocation::wire::MAX_OUTCOME_BYTES {
            return Err(Error::InvalidInput("invalid invocation outcome size"));
        }
        let db = self.database(target).await?;
        match deferred::accept(
            &db,
            target.attempt(),
            target.invocation_id,
            deferred::CompletionProof::VerifiedCallback {
                invocation_id: target.invocation_id,
            },
            body,
        )
        .await
        {
            Ok(deferred::Acceptance::Accepted) => Ok(Completion::Accepted),
            Ok(deferred::Acceptance::Duplicate) => Ok(Completion::Duplicate),
            Ok(deferred::Acceptance::Ineligible) => Ok(Completion::Ineligible),
            Err(error) => Err(match error.downcast_ref::<deferred::OutcomeError>() {
                Some(deferred::OutcomeError::Invalid) => {
                    Error::InvalidInput("invalid invocation outcome")
                }
                Some(deferred::OutcomeError::Conflict) => {
                    Error::Conflict("conflicting invocation outcome")
                }
                None => Error::Dependency(error),
            }),
        }
    }

    /// Attempts one due action for this callback invocation, if any. A stale
    /// wakeup or active claim is harmless. Hosts schedule recovery at claim
    /// expiry and at the invocation deadline, and persist their wakeups.
    pub async fn advance(&self, target: &Target) -> Result<(), Error> {
        let db = self.database(target).await?;
        if let Some(job) = deferred_jobs::callback_job(&db, target.attempt(), target.invocation_id)
            .await
            .map_err(Error::Dependency)?
        {
            deferred_runtime::advance_callback(&self.context, &db, target, job)
                .await
                .map_err(Error::Dependency)?;
        }
        Ok(())
    }
}
