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
    collections::{
        BTreeMap,
        BTreeSet,
    },
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

/// One runtime credential and the installation identity it must match.
/// The other two runtime role names are used only to reject role membership.
#[derive(Clone)]
pub struct RoleDatabaseBinding {
    pub purpose: DatabasePurpose,
    pub schema: String,
    pub function_owner: String,
    pub credential: DatabaseCredential,
    pub other_roles: [String; 2],
}

/// Fixed database bindings and routing for one runtime role. Pool limits apply
/// independently to each bound database.
#[derive(Clone)]
pub struct RoleDatabaseBindings {
    pub control_alias: String,
    pub default_execution_alias: String,
    pub shard_assignment_policy: String,
    pub databases: BTreeMap<String, RoleDatabaseBinding>,
    pub max_connections: u32,
    pub acquire_timeout: Duration,
    pub operation_timeout: Duration,
}
impl RoleDatabaseBindings {
    /// Uses five connections, a ten-second acquisition timeout and a thirty-second
    /// operation budget per database. Construction here performs no I/O.
    pub fn new(
        control_alias: String,
        default_execution_alias: String,
        shard_assignment_policy: String,
        databases: BTreeMap<String, RoleDatabaseBinding>,
    ) -> Self {
        Self {
            control_alias,
            default_execution_alias,
            shard_assignment_policy,
            databases,
            max_connections: 5,
            acquire_timeout: Duration::from_secs(10),
            operation_timeout: Duration::from_secs(30),
        }
    }

    pub(super) fn router(
        self,
        authority: Authority,
    ) -> Result<Arc<crate::context::database::DatabaseRouter>, Error> {
        let limits = PoolLimits {
            max_connections: self.max_connections,
            acquire_timeout: self.acquire_timeout,
            operation_timeout: self.operation_timeout,
        };
        validate_limits(&self.control_alias, self.databases.len(), limits)?;
        let pools = self
            .databases
            .into_iter()
            .map(|(alias, binding)| {
                let pool = build_pool(&alias, &self.control_alias, &binding, authority, limits)?;
                Ok((alias, fixed_pool(binding.purpose, pool.pool)))
            })
            .collect::<Result<_, Error>>()?;
        fixed_router(
            self.control_alias,
            self.default_execution_alias,
            self.shard_assignment_policy,
            limits,
            pools,
            authority,
        )
    }
}

impl DatabaseBinding {
    fn for_role(&self, authority: Authority) -> RoleDatabaseBinding {
        let (credential, peers) = match authority {
            Authority::Scheduler => (&self.scheduler, [&self.execution, &self.user]),
            Authority::Execution => (&self.execution, [&self.scheduler, &self.user]),
            Authority::User => (&self.user, [&self.scheduler, &self.execution]),
        };
        RoleDatabaseBinding {
            purpose: self.purpose,
            schema: self.schema.clone(),
            function_owner: self.function_owner.clone(),
            credential: credential.clone(),
            other_roles: peers.map(|peer| peer.database_role.clone()),
        }
    }
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

#[derive(Clone, Copy)]
struct PoolLimits {
    max_connections: u32,
    acquire_timeout: Duration,
    operation_timeout: Duration,
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
    limits: PoolLimits,
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
        let limits = PoolLimits {
            max_connections: config.max_connections,
            acquire_timeout: config.acquire_timeout,
            operation_timeout: config.operation_timeout,
        };
        validate_limits(&config.control_alias, config.databases.len(), limits)?;
        if !config.databases.contains_key(&config.control_alias) {
            return Err(Error::InvalidConfiguration(
                "missing control database binding",
            ));
        }
        let mut placements = BTreeMap::new();
        for (alias, binding) in &config.databases {
            let build = |authority| {
                build_pool(
                    alias,
                    &config.control_alias,
                    &binding.for_role(authority),
                    authority,
                    limits,
                )
            };
            placements.insert(
                alias.clone(),
                PlacementPools {
                    purpose: binding.purpose,
                    scheduler: build(Authority::Scheduler)?,
                    execution: build(Authority::Execution)?,
                    user: build(Authority::User)?,
                },
            );
        }
        Ok(Self(Arc::new(Inner {
            limits,
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
        let pools = self
            .0
            .placements
            .iter()
            .map(|(alias, binding)| {
                let pool = match authority {
                    Authority::Scheduler => &binding.scheduler.pool,
                    Authority::Execution => &binding.execution.pool,
                    Authority::User => &binding.user.pool,
                };
                (alias.clone(), fixed_pool(binding.purpose, pool.clone()))
            })
            .collect();
        fixed_router(
            self.0.control_alias.clone(),
            default_execution_alias,
            shard_assignment_policy,
            self.0.limits,
            pools,
            // Retain the composite runtime's configuration validation.
            Authority::Scheduler,
        )
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

fn valid_name(name: &str) -> bool {
    !name.is_empty() && name.trim() == name && name.len() <= 63 && !name.contains('\0')
}

fn validate_limits(control_alias: &str, count: usize, limits: PoolLimits) -> Result<(), Error> {
    if !valid_name(control_alias)
        || !(1..=129).contains(&count)
        || !(1..=256).contains(&limits.max_connections)
        || !(Duration::from_secs(1)..=Duration::from_secs(300)).contains(&limits.acquire_timeout)
        || !(Duration::from_secs(1)..=Duration::from_secs(3600)).contains(&limits.operation_timeout)
    {
        return Err(Error::InvalidConfiguration(
            "invalid fixed database bindings or pool limits",
        ));
    }
    Ok(())
}

fn build_pool(
    alias: &str,
    control_alias: &str,
    binding: &RoleDatabaseBinding,
    authority: Authority,
    limits: PoolLimits,
) -> Result<BoundPool, Error> {
    let valid_purpose = if alias == control_alias {
        matches!(
            binding.purpose,
            DatabasePurpose::Control | DatabasePurpose::ControlAndExecution
        )
    } else {
        binding.purpose == DatabasePurpose::Execution
    };
    if !valid_name(alias)
        || !valid_name(&binding.schema)
        || binding.schema.starts_with("pg_")
        || binding.schema == "information_schema"
        || !valid_purpose
    {
        return Err(Error::InvalidConfiguration(
            "invalid fixed database placement",
        ));
    }
    let roles = [
        &binding.credential.database_role,
        &binding.function_owner,
        &binding.other_roles[0],
        &binding.other_roles[1],
    ];
    if roles.iter().any(|role| !valid_name(role))
        || roles.iter().collect::<BTreeSet<_>>().len() != roles.len()
    {
        return Err(Error::InvalidConfiguration(
            "database authority roles must be distinct explicit names",
        ));
    }
    let options = connection_options(&binding.credential)?;
    let guard = Arc::new(validation::Guard::new(
        binding,
        authority,
        limits.operation_timeout,
    ));
    let after = guard.clone();
    let pool = PgPoolOptions::new()
        .max_connections(limits.max_connections)
        .acquire_timeout(limits.acquire_timeout)
        .after_connect(move |connection, _| {
            let guard = after.clone();
            Box::pin(async move { guard.initialize(connection).await })
        })
        .connect_lazy_with(options);
    Ok(BoundPool { pool, guard })
}

fn fixed_pool(
    purpose: DatabasePurpose,
    pool: PgPool,
) -> crate::context::database::FixedPlacementPool {
    use crate::models::database_placement::{
        DATABASE_PLACEMENT_ROLE_CONTROL,
        DATABASE_PLACEMENT_ROLE_CONTROL_AND_SHARD,
        DATABASE_PLACEMENT_ROLE_SHARD,
    };
    let role = match purpose {
        DatabasePurpose::Control => DATABASE_PLACEMENT_ROLE_CONTROL,
        DatabasePurpose::Execution => DATABASE_PLACEMENT_ROLE_SHARD,
        DatabasePurpose::ControlAndExecution => DATABASE_PLACEMENT_ROLE_CONTROL_AND_SHARD,
    };
    crate::context::database::FixedPlacementPool { pool, role }
}

fn fixed_router(
    control_alias: String,
    default_execution_alias: String,
    shard_assignment_policy: String,
    limits: PoolLimits,
    pools: BTreeMap<String, crate::context::database::FixedPlacementPool>,
    authority: Authority,
) -> Result<Arc<crate::context::database::DatabaseRouter>, Error> {
    use crate::context::database::{
        Config,
        DatabaseOperationTimeoutConfig,
        DatabaseRouter,
        PlacementConfig,
    };
    let placement = PlacementConfig::new(
        control_alias,
        default_execution_alias,
        shard_assignment_policy,
    )
    .map_err(|_| Error::InvalidConfiguration("invalid fixed runtime placement configuration"))?;
    let config = Config::new(
        String::new(),
        limits.max_connections,
        limits.acquire_timeout,
        Default::default(),
        placement,
    )
    .with_operation_timeout(Some(
        DatabaseOperationTimeoutConfig::new(limits.operation_timeout).map_err(Error::Dependency)?,
    ));
    if authority == Authority::User {
        DatabaseRouter::from_fixed_user_pools(config, pools)
    } else {
        DatabaseRouter::from_fixed_pools(config, pools)
    }
    .map(Arc::new)
    .map_err(Error::Dependency)
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
