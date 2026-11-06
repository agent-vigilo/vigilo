//! Restricted status reads with shared pools and transaction-owned identity.
//!
//! The embedding host supplies credentials and identities. Pools are bound to a
//! fixed database role, reused across callers, and never fall back to standalone
//! credentials or environment lookup. Local policy installation is checked when
//! connections open; policy administration remains outside these operations.
use std::{
    collections::BTreeMap,
    sync::Arc,
};

use sqlx::{
    PgPool,
    postgres::{
        PgConnectOptions,
        PgPoolOptions,
    },
};
use uuid::Uuid;

use super::{
    Config,
    Error,
    RunStatus,
};
pub use crate::db::scope::UserContext;
use crate::db::{
    scope,
    tables::runs,
    workflows::{
        run_creation,
        run_shard_summary,
        run_status,
    },
};

/// Fixed credential bindings supplied by the trusted embedding host.
#[derive(Clone)]
pub struct UserConfig {
    pub control: Config,
    /// Expected login role on every bound database; never selected by a request.
    pub database_role: String,
    /// Execution placement alias to connection string. No environment fallback.
    pub execution_databases: BTreeMap<String, String>,
}

/// Shared restricted-operation pools. This does not authenticate requests or
/// install authorization policy. Each connection must satisfy the role/policy guard.
#[derive(Clone)]
pub struct UserClient {
    control: PgPool,
    control_alias: String,
    execution: Arc<BTreeMap<String, PgPool>>,
}
impl UserClient {
    pub(super) fn from_fixed_pools(
        control: PgPool,
        control_alias: String,
        execution: BTreeMap<String, PgPool>,
    ) -> Self {
        Self {
            control,
            control_alias,
            execution: Arc::new(execution),
        }
    }

    /// Creates lazy fixed-role pools inside the host's Tokio runtime. Deployment
    /// validation occurs before the first protected query on each connection.
    pub fn new(config: UserConfig) -> Result<Self, Error> {
        // Reuse the standalone configuration validation without opening its pools.
        super::Client::new(config.control.clone())?;
        if config.database_role.trim().is_empty()
            || config.database_role.trim() != config.database_role
            || config.control.control_database_alias.trim() != config.control.control_database_alias
            || config.execution_databases.len() > 128
            || config
                .execution_databases
                .contains_key(&config.control.control_database_alias)
        {
            return Err(Error::InvalidConfiguration(
                "invalid fixed database bindings",
            ));
        }
        let pool = |uri: &str| -> Result<PgPool, Error> {
            let options: PgConnectOptions = uri
                .parse()
                .map_err(|_| Error::InvalidConfiguration("invalid PostgreSQL connection string"))?;
            let role = config.database_role.clone();
            Ok(PgPoolOptions::new()
                .max_connections(config.control.max_connections)
                .acquire_timeout(config.control.acquire_timeout)
                .after_connect(move |connection, _| {
                    let role = role.clone();
                    Box::pin(async move { scope::validate_connection(connection, &role).await })
                })
                .connect_lazy_with(options))
        };
        let control = pool(&config.control.database_url)?;
        let mut execution = BTreeMap::new();
        for (alias, uri) in &config.execution_databases {
            if alias.is_empty() || alias.trim() != alias {
                return Err(Error::InvalidConfiguration("invalid placement alias"));
            }
            execution.insert(alias.clone(), pool(uri)?);
        }
        Ok(Self {
            control,
            control_alias: config.control.control_database_alias,
            execution: Arc::new(execution),
        })
    }

    /// Reads one run with current caller context. The host authorizes this bounded
    /// operation; database policies enforce the configured row restrictions.
    pub async fn run_status(&self, context: UserContext, run_id: Uuid) -> Result<RunStatus, Error> {
        self.read(context, run_id)
            .await
            .map_err(|error| match error.downcast::<Error>() {
                Ok(error) => error,
                Err(error) => Error::Dependency(error),
            })
    }

    async fn read(&self, context: UserContext, run_id: Uuid) -> anyhow::Result<RunStatus> {
        let mut tx = scope::begin(&self.control, context, run_id).await?;
        let run = runs::select_run_by_id(&mut *tx, run_id)
            .await?
            .ok_or(Error::NotFound(run_id))?;
        if run.status == run_creation::RUN_STATUS_CREATING
            || (run.status == "failed" && run.started_at.is_none() && run.dispatched_at.is_none())
        {
            let creation = run_creation::select_creation_progress(&mut *tx, run_id).await?;
            tx.commit().await?;
            let mut projection = run_status::RunStatusProjection::from_control_run(run);
            projection.creation_progress = Some(creation);
            return Ok(RunStatus::from(&projection));
        }
        let routes:Vec<(i16,String,Option<String>,Option<String>)>=sqlx::query_as("SELECT s.run_shard,s.database_alias,d.role,d.status FROM shard_placements s LEFT JOIN database_placements d ON d.alias=s.database_alias WHERE s.run_id=$1 ORDER BY s.run_shard")
            .bind(run_id).fetch_all(&mut *tx).await?;
        tx.commit().await?;
        let mut grouped: BTreeMap<String, Vec<i16>> = BTreeMap::new();
        for (shard, alias, role, status) in &routes {
            anyhow::ensure!(
                matches!(role.as_deref(), Some("shard" | "control_and_shard"))
                    && matches!(status.as_deref(), Some("active" | "draining")),
                "execution placement is unavailable"
            );
            grouped.entry(alias.clone()).or_default().push(*shard);
        }
        let mut summaries = Vec::with_capacity(routes.len());
        for (alias, shards) in grouped {
            let pool = if alias == self.control_alias {
                &self.control
            } else {
                self.execution
                    .get(&alias)
                    .ok_or(Error::InvalidConfiguration(
                        "execution placement has no credential binding",
                    ))?
            };
            let mut tx = scope::begin(pool, context, run_id).await?;
            summaries.extend(
                run_shard_summary::select_run_shard_summaries(&mut *tx, run_id, &shards).await?,
            );
            tx.commit().await?;
        }
        let mut projection = run_status::RunStatusProjection::from_control_run(run);
        projection.execution_route_count = routes.len();
        projection.shard_summary_count = summaries.len();
        if !summaries.is_empty() {
            projection.live_progress = run_status::combine_run_shard_progress(&summaries);
        }
        Ok(RunStatus::from(&projection))
    }
}

#[cfg(test)]
#[path = "user_postgres_tests.rs"]
mod postgres_tests;
