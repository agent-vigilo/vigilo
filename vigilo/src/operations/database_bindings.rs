//! Fixed database credential domains for trusted embedding hosts.
//!
//! One shared pool per placement and authority is retained across clones. The
//! host supplies every connection binding explicitly; no environment fallback or
//! request-selected database role exists. New connections validate the installed
//! scheduler functions and role protections before becoming usable. Authentication,
//! policy administration and process lifecycle remain outside this module.

mod contracts;
mod validation;

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::Duration,
};

use sqlx::{
    PgPool,
    postgres::{
        PgConnectOptions,
        PgPoolOptions,
    },
};

use super::{
    Error,
    UserClient,
};

/// Work supported by a placement. The trusted host binds this to its deployment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DatabasePurpose {
    Control,
    Execution,
    ControlAndExecution,
}

/// A fixed login and its connection string. Neither is selected by a request.
/// Debug is intentionally absent because connection strings contain secrets.
#[derive(Clone)]
pub struct DatabaseCredential {
    pub database_url: String,
    pub database_role: String,
}

/// Explicit credentials and installation identity for one database placement.
#[derive(Clone)]
pub struct DatabaseBinding {
    pub purpose: DatabasePurpose,
    pub schema: String,
    /// Expected non-login, limited owner of the installed scheduler functions.
    pub function_owner: String,
    pub scheduler: DatabaseCredential,
    pub execution: DatabaseCredential,
    pub user: DatabaseCredential,
}

/// Trusted, fixed placement bindings. Pool limits apply per authority per database.
#[derive(Clone)]
pub struct DatabaseBindings {
    pub control_alias: String,
    pub databases: BTreeMap<String, DatabaseBinding>,
    pub max_connections: u32,
    pub acquire_timeout: Duration,
    /// Budget for deriving the runtime's statement and lock deadlines on every
    /// connection. This does not impose a deadline on an entire operation future.
    pub operation_timeout: Duration,
}
impl DatabaseBindings {
    /// Uses five connections per authority, a ten-second acquisition deadline
    /// and a thirty-second budget for statement/lock deadlines. No connection opens here.
    pub fn new(control_alias: String, databases: BTreeMap<String, DatabaseBinding>) -> Self {
        Self {
            control_alias,
            databases,
            max_connections: 5,
            acquire_timeout: Duration::from_secs(10),
            operation_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Authority {
    Scheduler,
    Execution,
    User,
}

struct BoundPool {
    pool: PgPool,
    guard: Arc<validation::Guard>,
}
struct PlacementPools {
    purpose: DatabasePurpose,
    scheduler: BoundPool,
    execution: BoundPool,
    user: BoundPool,
}
struct Inner {
    max_connections: u32,
    acquire_timeout: Duration,
    operation_timeout: Duration,
    control_alias: String,
    placements: BTreeMap<String, PlacementPools>,
}

/// Shared fixed-role pools. Construct inside the host's Tokio runtime without
/// opening connections. No operator runtime or raw SQL executor is exposed.
#[derive(Clone)]
pub struct DatabasePools(Arc<Inner>);

impl DatabasePools {
    /// Validates configuration and builds lazy pools. Connections fail closed on
    /// wrong roles, unsafe installation or missing protection/grants.
    pub fn new(config: DatabaseBindings) -> Result<Self, Error> {
        let valid_name = |name: &str| {
            !name.is_empty() && name.trim() == name && name.len() <= 63 && !name.contains('\0')
        };
        if !valid_name(&config.control_alias)
            || config.databases.is_empty()
            || config.databases.len() > 129
            || !(1..=256).contains(&config.max_connections)
            || !(Duration::from_secs(1)..=Duration::from_secs(300))
                .contains(&config.acquire_timeout)
            || !(Duration::from_secs(1)..=Duration::from_secs(3600))
                .contains(&config.operation_timeout)
        {
            return Err(Error::InvalidConfiguration(
                "invalid fixed database bindings or pool limits",
            ));
        }
        let control =
            config
                .databases
                .get(&config.control_alias)
                .ok_or(Error::InvalidConfiguration(
                    "missing control database binding",
                ))?;
        if !matches!(
            control.purpose,
            DatabasePurpose::Control | DatabasePurpose::ControlAndExecution
        ) {
            return Err(Error::InvalidConfiguration(
                "control binding does not support control work",
            ));
        }
        let mut placements = BTreeMap::new();
        for (alias, binding) in &config.databases {
            if !valid_name(alias)
                || !valid_name(&binding.schema)
                || !valid_name(&binding.function_owner)
                || binding.schema.starts_with("pg_")
                || binding.schema == "information_schema"
                || (alias != &config.control_alias && binding.purpose != DatabasePurpose::Execution)
            {
                return Err(Error::InvalidConfiguration(
                    "invalid fixed database placement",
                ));
            }
            let roles = [
                &binding.scheduler.database_role,
                &binding.execution.database_role,
                &binding.user.database_role,
                &binding.function_owner,
            ];
            if roles.iter().any(|role| !valid_name(role))
                || roles
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    != roles.len()
            {
                return Err(Error::InvalidConfiguration(
                    "database authority roles must be distinct explicit names",
                ));
            }
            let build = |credential: &DatabaseCredential, authority| -> Result<BoundPool, Error> {
                let options = connection_options(credential)?;
                let guard = Arc::new(validation::Guard::new(
                    binding,
                    credential,
                    authority,
                    config.operation_timeout,
                ));
                let after = guard.clone();
                let pool = PgPoolOptions::new()
                    .max_connections(config.max_connections)
                    .acquire_timeout(config.acquire_timeout)
                    .after_connect(move |connection, _| {
                        let guard = after.clone();
                        Box::pin(async move { guard.initialize(connection).await })
                    })
                    .connect_lazy_with(options);
                Ok(BoundPool { pool, guard })
            };
            placements.insert(
                alias.clone(),
                PlacementPools {
                    purpose: binding.purpose,
                    scheduler: build(&binding.scheduler, Authority::Scheduler)?,
                    execution: build(&binding.execution, Authority::Execution)?,
                    user: build(&binding.user, Authority::User)?,
                },
            );
        }
        Ok(Self(Arc::new(Inner {
            max_connections: config.max_connections,
            acquire_timeout: config.acquire_timeout,
            operation_timeout: config.operation_timeout,
            control_alias: config.control_alias,
            placements,
        })))
    }

    /// Construct one internal authority router from these exact validated pools.
    /// Public runtime construction is separate from standalone operator clients.
    pub(super) fn router(
        &self,
        authority: Authority,
        default_execution_alias: String,
        shard_assignment_policy: String,
    ) -> Result<Arc<crate::context::database::DatabaseRouter>, Error> {
        use crate::{
            context::database::{
                Config,
                DatabaseOperationTimeoutConfig,
                DatabaseRouter,
                FixedPlacementPool,
                PlacementConfig,
            },
            models::database_placement::{
                DATABASE_PLACEMENT_ROLE_CONTROL,
                DATABASE_PLACEMENT_ROLE_CONTROL_AND_SHARD,
                DATABASE_PLACEMENT_ROLE_SHARD,
            },
        };
        let placement = PlacementConfig::new(
            self.0.control_alias.clone(),
            default_execution_alias,
            shard_assignment_policy,
        )
        .map_err(|_| {
            Error::InvalidConfiguration("invalid fixed runtime placement configuration")
        })?;
        let config = Config::new(
            String::new(),
            self.0.max_connections,
            self.0.acquire_timeout,
            Default::default(),
            placement,
        )
        .with_operation_timeout(Some(
            DatabaseOperationTimeoutConfig::new(self.0.operation_timeout)
                .map_err(Error::Dependency)?,
        ));
        let pools = self
            .0
            .placements
            .iter()
            .map(|(alias, bindings)| {
                let pool = match authority {
                    Authority::Scheduler => &bindings.scheduler.pool,
                    Authority::Execution => &bindings.execution.pool,
                    Authority::User => &bindings.user.pool,
                };
                let role = match bindings.purpose {
                    DatabasePurpose::Control => DATABASE_PLACEMENT_ROLE_CONTROL,
                    DatabasePurpose::Execution => DATABASE_PLACEMENT_ROLE_SHARD,
                    DatabasePurpose::ControlAndExecution => {
                        DATABASE_PLACEMENT_ROLE_CONTROL_AND_SHARD
                    }
                };
                (
                    alias.clone(),
                    FixedPlacementPool {
                        pool: pool.clone(),
                        role,
                    },
                )
            })
            .collect();
        DatabaseRouter::from_fixed_pools(config, pools)
            .map(Arc::new)
            .map_err(Error::Dependency)
    }

    /// Opens and explicitly revalidates every configured authority binding before
    /// serving work. This performs bounded startup I/O, never per-item validation.
    /// New physical connections also validate independently. Hosts must revalidate
    /// or replace pools after changing deployment permissions or function code.
    pub async fn validate(&self) -> Result<(), Error> {
        for placement in self.0.placements.values() {
            for bound in [&placement.scheduler, &placement.execution, &placement.user] {
                let mut connection = bound
                    .pool
                    .acquire()
                    .await
                    .map_err(|e| Error::Dependency(e.into()))?;
                bound
                    .guard
                    .validate(&mut connection)
                    .await
                    .map_err(|e| Error::Dependency(e.into()))?;
            }
        }
        Ok(())
    }

    /// Creates a status client sharing the already configured user-role pools.
    /// This does not expose scheduler or execution credentials to user requests.
    pub fn user_client(&self) -> UserClient {
        let control = self.0.placements[&self.0.control_alias].user.pool.clone();
        let execution = self
            .0
            .placements
            .iter()
            .filter(|(alias, _)| *alias != &self.0.control_alias)
            .map(|(alias, pools)| (alias.clone(), pools.user.pool.clone()))
            .collect();
        UserClient::from_fixed_pools(control, self.0.control_alias.clone(), execution)
    }

    /// Closes all shared pools, including clones, and waits for borrowed connections.
    pub async fn close(&self) {
        for placement in self.0.placements.values() {
            for bound in [&placement.scheduler, &placement.execution, &placement.user] {
                bound.pool.close().await;
            }
        }
    }
}

/// Require a complete credential URI before SQLx can consult ambient defaults.
/// SQLx has no environment-free options constructor, so reject its ambient
/// overrides rather than mutating process-global environment in a library.
fn connection_options(credential: &DatabaseCredential) -> Result<PgConnectOptions, Error> {
    const AMBIENT: &[&str] = &[
        "PGHOSTADDR",
        "PGHOST",
        "PGPORT",
        "PGUSER",
        "PGDATABASE",
        "PGPASSWORD",
        "PGSSLROOTCERT",
        "PGSSLCERT",
        "PGSSLKEY",
        "PGSSLMODE",
        "PGAPPNAME",
        "PGOPTIONS",
    ];
    if AMBIENT.iter().any(|key| std::env::var_os(key).is_some()) {
        return Err(Error::InvalidConfiguration(
            "fixed database bindings require explicit URI settings without PG environment overrides",
        ));
    }
    let url = reqwest::Url::parse(&credential.database_url)
        .map_err(|_| Error::InvalidConfiguration("invalid PostgreSQL connection string"))?;
    if !matches!(url.scheme(), "postgres" | "postgresql")
        || url.host_str().is_none()
        || url.username().is_empty()
        || url.password().is_none_or(str::is_empty)
        || url.path().trim_matches('/').is_empty()
        || url.fragment().is_some()
        || url.query_pairs().any(|(key, _)| {
            !matches!(
                key.as_ref(),
                "sslmode"
                    | "ssl-mode"
                    | "sslrootcert"
                    | "ssl-root-cert"
                    | "sslcert"
                    | "ssl-cert"
                    | "sslkey"
                    | "ssl-key"
                    | "application_name"
            )
        })
    {
        return Err(Error::InvalidConfiguration(
            "fixed database URI requires host, database, login and password with explicit transport settings",
        ));
    }
    let options: PgConnectOptions = credential
        .database_url
        .parse()
        .map_err(|_| Error::InvalidConfiguration("invalid PostgreSQL connection string"))?;
    if options.get_username() != credential.database_role {
        return Err(Error::InvalidConfiguration(
            "connection login does not match fixed database role",
        ));
    }
    Ok(options)
}

#[cfg(test)]
mod postgres_tests;
#[cfg(test)]
mod tests;
