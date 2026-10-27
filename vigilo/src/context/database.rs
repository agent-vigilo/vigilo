//! Lazy PostgreSQL pool router and placement configuration context.
//!
//! Commands call this module through
//! [`crate::context::Context::dbr`] when they first need database
//! access. This context initializes the database router once, then the router
//! lazily initializes its control pool and per-placement pools. New aliases are
//! discovered on first use when their secret is already in the environment;
//! existing connection parameters remain fixed for the process lifetime.
//! Runtime commands may attach one operation deadline policy; the router then
//! applies its derived statement and lock limits to every new pool connection.
//!
//! Keep topology choices behind database workflow boundaries. Thin command dispatch,
//! generic runtime code, and arbitrary domain callers should prefer workflows
//! over directly choosing the control or execution database.

mod circuit_breaker;

use std::{
    collections::HashMap,
    sync::Arc,
    time::{
        Duration,
        Instant,
    },
};

pub(crate) use circuit_breaker::{
    CircuitBreakerConfig,
    CircuitOpen,
    CircuitPermit,
    CircuitTransition,
    DatabaseCircuitBreakers,
    is_database_contention,
    is_database_unavailable,
};
use moka::future::Cache;
use sqlx::{
    PgPool,
    Postgres,
    Transaction,
    postgres::PgPoolOptions,
};
use tokio::sync::OnceCell;
use tracing::debug;
use uuid::Uuid;

use crate::{
    db::{
        tables::{
            database_placements,
            shard_placements,
        },
        workflows::local_shard_admission::{
            LocalShardAdmissionDraft,
            LocalShardAdmissionError,
            LocalShardAdmissionState,
            LocalShardRouteHint,
            LocalShardWriteKind,
            upsert_local_shard_admission,
            validate_local_shard_admission,
        },
    },
    models::{
        database_placement::{
            DATABASE_PLACEMENT_STATUS_ACTIVE,
            DATABASE_PLACEMENT_STATUS_PROVISIONING,
            DEFAULT_DATABASE_URL_ENV,
            DatabasePlacement,
        },
        shard_placement::ShardPlacement,
    },
};

const SHARD_PLACEMENT_CACHE_TTL: Duration = Duration::from_secs(5);
const SHARD_PLACEMENT_CACHE_CAPACITY: u64 = 10_000;
const MAX_LOCK_TIMEOUT: Duration = Duration::from_secs(5);

/// Deadline policy for runtime database work.
///
/// PostgreSQL limits are derived from one operator-facing value so session and
/// client deadlines cannot drift independently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DatabaseOperationTimeoutConfig {
    operation_timeout: Duration,
    statement_timeout: Duration,
    lock_timeout: Duration,
}

impl DatabaseOperationTimeoutConfig {
    pub(crate) fn new(operation_timeout: Duration) -> anyhow::Result<Self> {
        if operation_timeout.is_zero() {
            anyhow::bail!("database operation timeout must be greater than zero");
        }
        let statement_timeout = operation_timeout.mul_f64(0.8);
        let lock_timeout = statement_timeout.div_f64(4.0).min(MAX_LOCK_TIMEOUT);
        Ok(Self {
            operation_timeout,
            statement_timeout,
            lock_timeout,
        })
    }

    pub(crate) fn operation_timeout(self) -> Duration {
        self.operation_timeout
    }

    pub(crate) fn statement_timeout(self) -> Duration {
        self.statement_timeout
    }

    pub(crate) fn lock_timeout(self) -> Duration {
        self.lock_timeout
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "database operation {operation_name} timed out for placement {database_alias} after {timeout_ms}ms"
)]
pub(crate) struct DatabaseOperationTimeout {
    database_alias: String,
    operation_name: &'static str,
    timeout_ms: u128,
}

impl DatabaseOperationTimeout {
    pub(crate) fn new(
        database_alias: impl Into<String>,
        operation_name: &'static str,
        timeout: Duration,
    ) -> Self {
        Self {
            database_alias: database_alias.into(),
            operation_name,
            timeout_ms: timeout.as_millis(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ExecutionRouteError {
    #[error(
        "missing shard placement for run {run_id} shard {run_shard}; run creation must insert shard_placements rows before execution routing"
    )]
    MissingShardPlacement { run_id: Uuid, run_shard: i16 },
    #[error(
        "shard placement for run {run_id} shard {run_shard} has status {status}, which is not dispatchable"
    )]
    NonDispatchableShardPlacement {
        run_id: Uuid,
        run_shard: i16,
        status: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ShardAssignmentPolicy {
    SingleDefault,
    SpreadActive,
}

impl ShardAssignmentPolicy {
    pub(crate) fn parse(raw: &str) -> anyhow::Result<Self> {
        match normalize_alias(raw.to_string(), "shard assignment policy")?.as_str() {
            "single-default" => Ok(Self::SingleDefault),
            "spread-active" => Ok(Self::SpreadActive),
            other => anyhow::bail!(
                "unsupported shard assignment policy {}; expected single-default or spread-active",
                other
            ),
        }
    }
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::SingleDefault => "single-default",
            Self::SpreadActive => "spread-active",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacementConfig {
    pub(crate) control_database_alias: String,
    pub(crate) default_shard_database_alias: String,
    pub(crate) shard_assignment_policy: ShardAssignmentPolicy,
}

impl PlacementConfig {
    pub(crate) fn new(
        control_database_alias: String,
        default_shard_database_alias: String,
        shard_assignment_policy: String,
    ) -> anyhow::Result<Self> {
        let control_database_alias = normalize_alias(control_database_alias, "control alias")?;
        let default_shard_database_alias =
            normalize_alias(default_shard_database_alias, "default shard alias")?;
        let shard_assignment_policy = ShardAssignmentPolicy::parse(&shard_assignment_policy)?;

        Ok(Self {
            control_database_alias,
            default_shard_database_alias,
            shard_assignment_policy,
        })
    }

    #[cfg(test)]
    pub(crate) fn default_single_database() -> Self {
        Self {
            control_database_alias: crate::models::database_placement::DEFAULT_DATABASE_ALIAS
                .to_string(),
            default_shard_database_alias: crate::models::database_placement::DEFAULT_DATABASE_ALIAS
                .to_string(),
            shard_assignment_policy: ShardAssignmentPolicy::SingleDefault,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Config {
    pub(crate) uri: String,
    pub(crate) max_connections_per_pool: u32,
    pub(crate) acquire_timeout: Duration,
    pub(crate) circuit_breaker_config: CircuitBreakerConfig,
    pub(crate) placement_config: PlacementConfig,
    pub(crate) operation_timeout_config: Option<DatabaseOperationTimeoutConfig>,
}

impl Config {
    pub(crate) fn new(
        uri: String,
        max_connections_per_pool: u32,
        acquire_timeout: Duration,
        circuit_breaker_config: CircuitBreakerConfig,
        placement_config: PlacementConfig,
    ) -> Self {
        Self {
            uri,
            max_connections_per_pool,
            acquire_timeout,
            circuit_breaker_config,
            placement_config,
            operation_timeout_config: None,
        }
    }

    pub(crate) fn with_operation_timeout(
        mut self,
        operation_timeout_config: Option<DatabaseOperationTimeoutConfig>,
    ) -> Self {
        self.operation_timeout_config = operation_timeout_config;
        self
    }
}

pub(crate) struct Context {
    pub(crate) config: Config,
    pub(crate) cell: OnceCell<DatabaseRouter>,
}

impl Context {
    pub(crate) async fn get(&self) -> anyhow::Result<&DatabaseRouter> {
        self.cell
            .get_or_try_init(|| async {
                Ok::<DatabaseRouter, anyhow::Error>(DatabaseRouter::new(self.config.clone()))
            })
            .await
    }
}

/// Resolves control and execution database pools from placement metadata.
pub(crate) struct DatabaseRouter {
    pub(crate) uri: String,
    pub(crate) max_connections_per_pool: u32,
    pub(crate) acquire_timeout: Duration,
    pub(crate) placement_config: PlacementConfig,
    pub(crate) circuit_breakers: DatabaseCircuitBreakers,
    pub(crate) operation_timeout_config: Option<DatabaseOperationTimeoutConfig>,
    pub(crate) control_pool: OnceCell<PgPool>,
    #[allow(dead_code)]
    pub(crate) placement_pools: OnceCell<PlacementPools>,
    pub(crate) dynamic_placement_pools: Cache<String, Arc<PlacementPool>>,
    #[allow(dead_code)]
    pub(crate) shard_placement_cache: Cache<ShardPlacementKey, ShardPlacement>,
}

/// One serviceable execution route resolved from control metadata.
///
/// Write paths retain the control route alongside the pool; the execution
/// database validates its local write epoch before changing owned state.
#[derive(Clone)]
pub(crate) struct ExecutionRoute {
    pub(crate) placement: ShardPlacement,
    pub(crate) pool: PgPool,
}

/// Message-carried route hint paired with its process-local execution pool.
#[derive(Clone)]
pub(crate) struct ExecutionWriteRoute {
    pub(crate) hint: LocalShardRouteHint,
    pub(crate) pool: PgPool,
}

impl DatabaseRouter {
    fn new(config: Config) -> Self {
        Self {
            uri: config.uri,
            max_connections_per_pool: config.max_connections_per_pool,
            acquire_timeout: config.acquire_timeout,
            placement_config: config.placement_config,
            circuit_breakers: circuit_breaker::DatabaseCircuitBreakers::new(
                config.circuit_breaker_config,
            ),
            operation_timeout_config: config.operation_timeout_config,
            control_pool: OnceCell::new(),
            placement_pools: OnceCell::new(),
            dynamic_placement_pools: Cache::builder().max_capacity(1_000).build(),
            shard_placement_cache: new_shard_placement_cache(),
        }
    }

    /// Returns the control database pool.
    ///
    /// The pool is initialized on first use, not when the database router is
    /// retrieved from [`crate::context::Context::dbr`].
    pub async fn control(&self) -> anyhow::Result<&PgPool> {
        let started = Instant::now();
        self.control_pool
            .get_or_try_init(|| async {
                debug!("initializing postgres database connection");

                runtime_pool_options(
                    self.max_connections_per_pool,
                    self.acquire_timeout,
                    self.operation_timeout_config,
                )
                .connect(&self.uri)
                .await
                .map_err(|error| {
                    let message = format!("database connection failed: {error}");
                    anyhow::Error::new(error).context(message)
                })
            })
            .await
            .inspect(|_| {
                debug!(
                    database_alias = %self.placement_config.control_database_alias,
                    pool_acquisition_ms = started.elapsed().as_millis() as u64,
                    "resolved control database pool"
                );
            })
    }

    pub(crate) fn default_execution_database_alias(&self) -> &str {
        &self.placement_config.default_shard_database_alias
    }

    /// Applies the outer deadline to one database future while preserving its
    /// output type. Callers decide how domain errors are classified.
    pub(crate) async fn deadline_database_operation<T, Operation>(
        &self,
        database_alias: &str,
        operation_name: &'static str,
        operation: Operation,
    ) -> Result<T, DatabaseOperationTimeout>
    where
        Operation: std::future::Future<Output = T>,
    {
        let Some(config) = self.operation_timeout_config else {
            return Ok(operation.await);
        };
        tokio::time::timeout(config.operation_timeout(), operation)
            .await
            .map_err(|_| {
                DatabaseOperationTimeout::new(
                    database_alias,
                    operation_name,
                    config.operation_timeout(),
                )
            })
    }

    /// Applies the outer deadline and preserves the operation's error chain for
    /// circuit-breaker and retry classification.
    pub(crate) async fn run_database_operation<T, Operation>(
        &self,
        database_alias: &str,
        operation_name: &'static str,
        operation: Operation,
    ) -> anyhow::Result<T>
    where
        Operation: std::future::Future<Output = anyhow::Result<T>>,
    {
        self.deadline_database_operation(database_alias, operation_name, operation)
            .await
            .map_err(anyhow::Error::new)
            .and_then(std::convert::identity)
    }

    pub(crate) fn shard_assignment_policy(&self) -> &ShardAssignmentPolicy {
        &self.placement_config.shard_assignment_policy
    }

    /// Acquires process-local runtime permission to contact one execution database.
    ///
    /// Durable work remains pending when the circuit is open. Administrative
    /// workflows use the normal pool methods directly and therefore retain an
    /// explicit recovery path.
    pub(crate) fn acquire_database_operation(
        &self,
        database_alias: &str,
    ) -> Result<CircuitPermit, CircuitOpen> {
        self.circuit_breakers
            .acquire(database_alias, Instant::now())
    }

    pub(crate) fn record_database_operation_success(
        &self,
        permit: CircuitPermit,
    ) -> Option<CircuitTransition> {
        self.circuit_breakers.record_success(permit)
    }

    pub(crate) fn record_database_operation_error(
        &self,
        permit: CircuitPermit,
        error: &anyhow::Error,
    ) -> (bool, Option<CircuitTransition>) {
        let impact = circuit_breaker::classify_error(error);
        let transition = self
            .circuit_breakers
            .record_failure(permit, Instant::now(), impact);
        (
            impact == circuit_breaker::FailureImpact::Unavailable,
            transition,
        )
    }

    pub(crate) fn control_database_alias(&self) -> &str {
        &self.placement_config.control_database_alias
    }

    pub(crate) async fn active_shard_capable_database_aliases(
        &self,
    ) -> anyhow::Result<Vec<String>> {
        let db = self.control().await?;
        database_placements::list_active_shard_capable_database_aliases(db).await
    }

    pub(crate) async fn serviceable_execution_database_aliases(
        &self,
    ) -> anyhow::Result<Vec<String>> {
        let db = self.control().await?;
        database_placements::list_serviceable_shard_database_aliases(db).await
    }

    pub(crate) async fn serviceable_outbox_database_aliases(&self) -> anyhow::Result<Vec<String>> {
        let db = self.control().await?;
        database_placements::list_serviceable_database_aliases(db).await
    }

    /// Returns a pool for a placement that may finish existing work.
    ///
    /// This is an infrastructure hook for router/admin workflows. Most callers
    /// should use a domain workflow or [`Self::execution`] instead of naming a
    /// placement directly. Active and draining placements are serviceable.
    /// Status is read from the control database on every call; connection
    /// parameters and pools remain fixed for the process lifetime.
    #[allow(dead_code)]
    pub async fn placement(&self, alias: &str) -> anyhow::Result<PgPool> {
        let started = Instant::now();
        let alias = normalize_alias(alias.to_string(), "database alias")?;
        let placement = self.require_serviceable_placement(&alias).await?;

        self.resolve_placement_pool(&placement, started).await
    }

    /// Returns a pool for a shard-capable owner of existing execution data.
    ///
    /// Active and draining targets remain available because draining must not
    /// strand routed work, recovery, creation replay, or source-side movement.
    pub(crate) async fn execution_database(&self, alias: &str) -> anyhow::Result<PgPool> {
        let started = Instant::now();
        let alias = normalize_alias(alias.to_string(), "database alias")?;
        let placement = self.require_serviceable_placement(&alias).await?;
        require_shard_capable_placement(&placement)?;

        self.resolve_placement_pool(&placement, started).await
    }

    /// Returns a pool for a target that may receive new shard ownership.
    pub(crate) async fn execution_target_database(&self, alias: &str) -> anyhow::Result<PgPool> {
        let started = Instant::now();
        let alias = normalize_alias(alias.to_string(), "database alias")?;
        let placement = self.require_active_placement(&alias).await?;
        require_shard_capable_placement(&placement)?;

        self.resolve_placement_pool(&placement, started).await
    }

    /// Returns a pool for one shard placement undergoing readiness verification.
    pub(crate) async fn provisioning_database(&self, alias: &str) -> anyhow::Result<PgPool> {
        let started = Instant::now();
        let alias = normalize_alias(alias.to_string(), "database alias")?;
        let db = self.control().await?;
        let placement = database_placements::select_database_placement(db, &alias)
            .await?
            .ok_or_else(|| anyhow::anyhow!("database placement alias {} was not found", alias))?;

        if placement.status != DATABASE_PLACEMENT_STATUS_PROVISIONING {
            anyhow::bail!(
                "database placement alias {} has status {}, which is not awaiting activation",
                alias,
                placement.status
            );
        }
        require_shard_capable_placement(&placement)?;

        self.resolve_placement_pool(&placement, started).await
    }

    async fn resolve_placement_pool(
        &self,
        placement: &DatabasePlacement,
        started: Instant,
    ) -> anyhow::Result<PgPool> {
        let pools = self.placement_pools().await?;
        if placement.alias == self.placement_config.control_database_alias {
            if let Some(configured) = pools.pools_by_alias.get(&placement.alias)
                && configured.database_url_env != placement.database_url_env
            {
                anyhow::bail!(
                    "database placement alias {} changed database_url_env from {} to {}; restart Vigilo to load new connection parameters",
                    placement.alias,
                    configured.database_url_env,
                    placement.database_url_env
                );
            }
            let pool = self.control().await?;
            debug!(
                database_alias = %placement.alias,
                placement_pool_acquisition_ms = started.elapsed().as_millis() as u64,
                pool_source = "control",
                "resolved database placement pool"
            );
            return Ok(pool.clone());
        }

        if let Some(pool) = pools.pools_by_alias.get(&placement.alias) {
            if pool.database_url_env != placement.database_url_env {
                anyhow::bail!(
                    "database placement alias {} changed database_url_env from {} to {}; restart Vigilo to load new connection parameters",
                    placement.alias,
                    pool.database_url_env,
                    placement.database_url_env
                );
            }
            let database_url = self.resolve_database_url_env(&placement.database_url_env)?;
            return Ok(pool
                .get(
                    &placement.alias,
                    &database_url,
                    self.max_connections_per_pool,
                    self.acquire_timeout,
                    self.operation_timeout_config,
                )
                .await?
                .clone());
        }

        let database_url = self.resolve_database_url_env(&placement.database_url_env)?;
        let database_url_env = placement.database_url_env.clone();
        let pool = self
            .dynamic_placement_pools
            .get_with(placement.alias.clone(), async move {
                Arc::new(PlacementPool {
                    database_url_env,
                    pool: OnceCell::new(),
                })
            })
            .await;
        if pool.database_url_env != placement.database_url_env {
            anyhow::bail!(
                "database placement alias {} changed database_url_env from {} to {}; restart Vigilo to load new connection parameters",
                placement.alias,
                pool.database_url_env,
                placement.database_url_env
            );
        }
        let pool = pool
            .get(
                &placement.alias,
                &database_url,
                self.max_connections_per_pool,
                self.acquire_timeout,
                self.operation_timeout_config,
            )
            .await?;
        debug!(
            database_alias = %placement.alias,
            placement_pool_acquisition_ms = started.elapsed().as_millis() as u64,
            pool_source = "placement",
            "resolved database placement pool"
        );
        Ok(pool.clone())
    }

    /// Resolves the stored execution placement for a run shard.
    #[allow(dead_code)]
    pub async fn execution_placement(
        &self,
        run_id: Uuid,
        run_shard: i16,
    ) -> anyhow::Result<ShardPlacement> {
        let placement = self.resolve_execution_placement(run_id, run_shard).await?;
        self.validate_shard_placement_alias(&placement).await?;
        Ok(placement)
    }

    async fn resolve_execution_placement(
        &self,
        run_id: Uuid,
        run_shard: i16,
    ) -> anyhow::Result<ShardPlacement> {
        let started = Instant::now();
        validate_run_shard(run_shard)?;

        let key = ShardPlacementKey { run_id, run_shard };
        if let Some(placement) = self.shard_placement_cache.get(&key).await {
            debug!(
                run_id = %run_id,
                run_shard,
                database_alias = %placement.database_alias,
                placement_status = %placement.status,
                route_version = placement.route_version,
                route_resolution_ms = started.elapsed().as_millis() as u64,
                route_cache = "hit",
                routing_decision = "cached_execution_placement",
                "resolved execution placement from route cache"
            );
            return Ok(placement);
        }

        let placement = self
            .select_control_shard_placement(run_id, run_shard)
            .await?;

        debug!(
            run_id = %run_id,
            run_shard,
            database_alias = %placement.database_alias,
            placement_status = %placement.status,
            route_version = placement.route_version,
            route_resolution_ms = started.elapsed().as_millis() as u64,
            route_cache = "miss",
            routing_decision = "control_lookup_execution_placement",
            "resolved execution placement from control metadata"
        );
        self.shard_placement_cache
            .insert(key, placement.clone())
            .await;

        Ok(placement)
    }

    /// Returns the execution-owned database pool for a run shard.
    ///
    /// Routing is based on persisted `shard_placements` data. Callers that do
    /// not own execution data should use a workflow that hides this choice.
    #[allow(dead_code)]
    pub async fn execution(&self, run_id: Uuid, run_shard: i16) -> anyhow::Result<PgPool> {
        let placement = self.resolve_execution_placement(run_id, run_shard).await?;

        if !placement.is_dispatchable() {
            debug!(
                run_id = %run_id,
                run_shard,
                database_alias = %placement.database_alias,
                placement_status = %placement.status,
                routing_decision = "blocked_non_dispatchable",
                "execution placement is not dispatchable"
            );
            return Err(ExecutionRouteError::NonDispatchableShardPlacement {
                run_id,
                run_shard,
                status: placement.status,
            }
            .into());
        }

        debug!(
            run_id = %run_id,
            run_shard,
            database_alias = %placement.database_alias,
            placement_status = %placement.status,
            routing_decision = "dispatchable_execution_pool",
            "resolved dispatchable execution pool"
        );
        self.execution_database(&placement.database_alias).await
    }

    /// Resolves an active execution route together with its current fence.
    pub(crate) async fn execution_route(
        &self,
        run_id: Uuid,
        run_shard: i16,
    ) -> anyhow::Result<ExecutionRoute> {
        let placement = self.resolve_execution_placement(run_id, run_shard).await?;
        if !placement.is_dispatchable() {
            return Err(ExecutionRouteError::NonDispatchableShardPlacement {
                run_id,
                run_shard,
                status: placement.status,
            }
            .into());
        }

        let pool = self
            .execution_database(&placement.database_alias)
            .await?
            .clone();
        Ok(ExecutionRoute { placement, pool })
    }

    /// Resolves a queue-carried route without consulting shard control metadata.
    ///
    /// Known placement pools are process-local. A previously unseen database
    /// alias is discovered once from control metadata and cached, allowing
    /// workers to consume routes added after process startup.
    pub(crate) async fn execution_write_route(
        &self,
        hint: LocalShardRouteHint,
    ) -> anyhow::Result<ExecutionWriteRoute> {
        validate_run_shard(hint.run_shard)?;
        if hint.write_epoch <= 0 {
            anyhow::bail!("write epoch must be greater than zero");
        }
        let alias = normalize_alias(hint.database_alias.clone(), "database alias")?;
        if alias != hint.database_alias {
            anyhow::bail!("database alias in route hint must be normalized");
        }
        let pool = self.execution_database_from_hint(&alias).await?;
        Ok(ExecutionWriteRoute { hint, pool })
    }

    async fn execution_database_from_hint(&self, alias: &str) -> anyhow::Result<PgPool> {
        if alias == self.control_database_alias() {
            return Ok(self.control().await?.clone());
        }

        let pools = self.placement_pools().await?;
        if let Some(pool) = pools.pools_by_alias.get(alias) {
            let database_url = self.resolve_database_url_env(&pool.database_url_env)?;
            return Ok(pool
                .get(
                    alias,
                    &database_url,
                    self.max_connections_per_pool,
                    self.acquire_timeout,
                    self.operation_timeout_config,
                )
                .await?
                .clone());
        }
        if let Some(pool) = self.dynamic_placement_pools.get(alias).await {
            let database_url = self.resolve_database_url_env(&pool.database_url_env)?;
            return Ok(pool
                .get(
                    alias,
                    &database_url,
                    self.max_connections_per_pool,
                    self.acquire_timeout,
                    self.operation_timeout_config,
                )
                .await?
                .clone());
        }
        let placement = self.require_serviceable_placement(alias).await?;
        require_shard_capable_placement(&placement)?;
        let database_url = self.resolve_database_url_env(&placement.database_url_env)?;
        let database_url_env = placement.database_url_env;
        let pool = self
            .dynamic_placement_pools
            .get_with(alias.to_string(), async move {
                Arc::new(PlacementPool {
                    database_url_env,
                    pool: OnceCell::new(),
                })
            })
            .await;
        Ok(pool
            .get(
                alias,
                &database_url,
                self.max_connections_per_pool,
                self.acquire_timeout,
                self.operation_timeout_config,
            )
            .await?
            .clone())
    }

    /// Acquires write admission for a previously resolved route.
    ///
    /// Movement takes the exclusive form of the same advisory lock. Holding
    /// the shared lock from local epoch validation through the write closes
    /// the cutover gap after pool resolution.
    pub(crate) async fn begin_execution_admission(
        &self,
        route: &ExecutionRoute,
    ) -> anyhow::Result<Transaction<'static, Postgres>> {
        let mut tx = route.pool.begin().await?;
        crate::db::shard_write_fence::lock_shared(
            &mut tx,
            route.placement.run_id,
            route.placement.run_shard,
        )
        .await?;

        let hint = LocalShardRouteHint {
            run_id: route.placement.run_id,
            run_shard: route.placement.run_shard,
            database_alias: route.placement.database_alias.clone(),
            write_epoch: route.placement.write_epoch,
        };
        let validation =
            validate_local_shard_admission(&mut *tx, &hint, LocalShardWriteKind::NewWork).await;
        if let Err(error) = validation {
            if !matches!(
                error.downcast_ref::<LocalShardAdmissionError>(),
                Some(LocalShardAdmissionError::Missing { .. })
            ) {
                return Err(error);
            }

            // Upgrade/new-placement repair path only. The shared local fence
            // prevents movement from changing admission while control metadata
            // is re-read and the missing row is created.
            let current = self
                .select_control_shard_placement(hint.run_id, hint.run_shard)
                .await?;
            if !route.placement.same_route_fence(&current) || !current.is_dispatchable() {
                return Err(error);
            }
            upsert_local_shard_admission(
                &mut *tx,
                LocalShardAdmissionDraft {
                    run_id: hint.run_id,
                    run_shard: hint.run_shard,
                    database_alias: hint.database_alias,
                    write_epoch: hint.write_epoch,
                    state: LocalShardAdmissionState::Open,
                    redirect_database_alias: None,
                    move_fence: None,
                },
            )
            .await?;
        }

        Ok(tx)
    }

    /// Acquires local write admission for a message-carried route hint.
    pub(crate) async fn begin_execution_write_admission(
        &self,
        route: &ExecutionWriteRoute,
    ) -> anyhow::Result<Transaction<'static, Postgres>> {
        let mut tx = route.pool.begin().await?;
        crate::db::shard_write_fence::lock_shared(&mut tx, route.hint.run_id, route.hint.run_shard)
            .await?;
        validate_local_shard_admission(&mut *tx, &route.hint, LocalShardWriteKind::NewWork).await?;
        Ok(tx)
    }

    /// Acquires shared admission for cleanup across routes on one placement.
    ///
    /// Cancellation may finish cleanup while a route is `draining`, but
    /// `moving` is fully frozen for target reconciliation and copying. Locks
    /// are acquired in shard order to keep concurrent cleanup deterministic.
    pub(crate) async fn begin_execution_cleanup_admission(
        &self,
        routes: &[ExecutionRoute],
    ) -> anyhow::Result<Transaction<'static, Postgres>> {
        let Some(first) = routes.first() else {
            anyhow::bail!("execution cleanup admission requires at least one route");
        };
        if routes.iter().any(|route| {
            route.placement.run_id != first.placement.run_id
                || route.placement.database_alias != first.placement.database_alias
        }) {
            anyhow::bail!("execution cleanup admission routes must share one run and placement");
        }

        let mut ordered = routes.iter().collect::<Vec<_>>();
        ordered.sort_by_key(|route| route.placement.run_shard);
        let mut tx = first.pool.begin().await?;
        for route in &ordered {
            crate::db::shard_write_fence::lock_shared(
                &mut tx,
                route.placement.run_id,
                route.placement.run_shard,
            )
            .await?;
        }

        for route in ordered {
            if route.placement.status
                == crate::models::shard_placement::SHARD_PLACEMENT_STATUS_MOVING
            {
                return Err(ExecutionRouteError::NonDispatchableShardPlacement {
                    run_id: route.placement.run_id,
                    run_shard: route.placement.run_shard,
                    status: route.placement.status.clone(),
                }
                .into());
            }
            let hint = LocalShardRouteHint {
                run_id: route.placement.run_id,
                run_shard: route.placement.run_shard,
                database_alias: route.placement.database_alias.clone(),
                write_epoch: route.placement.write_epoch,
            };
            let validation =
                validate_local_shard_admission(&mut *tx, &hint, LocalShardWriteKind::Settlement)
                    .await;
            if let Err(error) = validation {
                if !matches!(
                    error.downcast_ref::<LocalShardAdmissionError>(),
                    Some(LocalShardAdmissionError::Missing { .. })
                ) {
                    return Err(error);
                }
                let state = if route.placement.status
                    == crate::models::shard_placement::SHARD_PLACEMENT_STATUS_DRAINING
                {
                    LocalShardAdmissionState::Draining
                } else {
                    LocalShardAdmissionState::Open
                };
                upsert_local_shard_admission(
                    &mut *tx,
                    LocalShardAdmissionDraft {
                        run_id: hint.run_id,
                        run_shard: hint.run_shard,
                        database_alias: hint.database_alias,
                        write_epoch: hint.write_epoch,
                        state,
                        redirect_database_alias: route.placement.move_target_database_alias.clone(),
                        move_fence: None,
                    },
                )
                .await?;
            }
        }

        Ok(tx)
    }

    /// Returns all dispatchable execution routes for a run.
    #[allow(dead_code)]
    pub async fn execution_routes_for_run(
        &self,
        run_id: Uuid,
    ) -> anyhow::Result<Vec<(i16, String, PgPool)>> {
        let placements = self.execution_placements_for_run(run_id).await?;
        let mut routed = Vec::with_capacity(placements.len());

        for placement in placements {
            if !placement.is_dispatchable() {
                debug!(
                    run_id = %run_id,
                    run_shard = placement.run_shard,
                    database_alias = %placement.database_alias,
                    placement_status = %placement.status,
                    routing_decision = "blocked_non_dispatchable",
                    "execution route is not dispatchable"
                );
                anyhow::bail!(
                    "shard placement for run {} shard {} has status {}, which is not dispatchable",
                    run_id,
                    placement.run_shard,
                    placement.status
                );
            }

            let pool = self
                .execution_database(&placement.database_alias)
                .await?
                .clone();
            debug!(
                run_id = %run_id,
                run_shard = placement.run_shard,
                database_alias = %placement.database_alias,
                placement_status = %placement.status,
                routing_decision = "dispatchable_execution_route",
                "resolved dispatchable execution route"
            );
            routed.push((placement.run_shard, placement.database_alias, pool));
        }

        Ok(routed)
    }

    /// Returns stored execution placement rows without resolving their pools.
    ///
    /// Placement-scoped coordinator passes use this to open and inspect each
    /// database independently, so one unavailable target does not discard
    /// successful reads from other placements.
    pub(crate) async fn execution_placements_for_run(
        &self,
        run_id: Uuid,
    ) -> anyhow::Result<Vec<ShardPlacement>> {
        let db = self.control().await?;
        shard_placements::list_shard_placements_for_run(db, run_id).await
    }

    /// Returns all readable execution routes for a run.
    ///
    /// Read paths such as results/export can read from `moving` or `draining`
    /// shard placements. Those states block new claims and dispatch, not
    /// inspection of the placement currently recorded for the shard.
    pub(crate) async fn execution_read_routes_for_run(
        &self,
        run_id: Uuid,
    ) -> anyhow::Result<Vec<(i16, String, PgPool)>> {
        let routes = self
            .execution_read_routes_with_fences_for_run(run_id)
            .await?;
        Ok(routes
            .into_iter()
            .map(|route| {
                (
                    route.placement.run_shard,
                    route.placement.database_alias,
                    route.pool,
                )
            })
            .collect())
    }

    /// Returns readable routes with their exact control-database fences.
    pub(crate) async fn execution_read_routes_with_fences_for_run(
        &self,
        run_id: Uuid,
    ) -> anyhow::Result<Vec<ExecutionRoute>> {
        let db = self.control().await?;
        let placements = shard_placements::list_shard_placements_for_run(db, run_id).await?;
        let mut routed = Vec::with_capacity(placements.len());

        for placement in placements {
            let pool = self
                .execution_database(&placement.database_alias)
                .await?
                .clone();
            debug!(
                run_id = %run_id,
                run_shard = placement.run_shard,
                database_alias = %placement.database_alias,
                placement_status = %placement.status,
                routing_decision = "readable_execution_route",
                "resolved readable execution route"
            );
            routed.push(ExecutionRoute { placement, pool });
        }

        Ok(routed)
    }

    pub(crate) async fn invalidate_execution_placement(&self, run_id: Uuid, run_shard: i16) {
        self.shard_placement_cache
            .invalidate(&ShardPlacementKey { run_id, run_shard })
            .await;
    }

    async fn select_control_shard_placement(
        &self,
        run_id: Uuid,
        run_shard: i16,
    ) -> anyhow::Result<ShardPlacement> {
        let db = self.control().await?;
        shard_placements::select_shard_placement(db, run_id, run_shard)
            .await?
            .ok_or_else(|| ExecutionRouteError::MissingShardPlacement { run_id, run_shard }.into())
    }

    pub(crate) async fn validate_placement_config(&self) -> anyhow::Result<()> {
        let db = self.control().await?;
        let placements = database_placements::list_serviceable_database_placements(db).await?;

        if placements.is_empty() {
            anyhow::bail!("database_placements has no active or draining placements");
        }

        for placement in &placements {
            self.resolve_database_url_env(&placement.database_url_env)?;
        }

        let control_placements = placements
            .iter()
            .filter(|placement| {
                placement.status == DATABASE_PLACEMENT_STATUS_ACTIVE
                    && placement.is_control_capable()
            })
            .collect::<Vec<_>>();

        let [active_control] = control_placements.as_slice() else {
            anyhow::bail!(
                "expected exactly one active control-capable database placement, found {}",
                control_placements.len()
            );
        };

        if active_control.alias != self.placement_config.control_database_alias {
            anyhow::bail!(
                "VIGILO_CONTROL_DATABASE_ALIAS={} does not match active control placement {}",
                self.placement_config.control_database_alias,
                active_control.alias
            );
        }

        let placements_by_alias = placements
            .iter()
            .map(|placement| (placement.alias.as_str(), placement))
            .collect::<HashMap<_, _>>();

        validate_shard_capable_alias(
            &placements_by_alias,
            &self.placement_config.default_shard_database_alias,
            "VIGILO_DEFAULT_SHARD_DATABASE_ALIAS",
        )?;

        let unserviceable_routes =
            database_placements::count_shard_placements_on_unserviceable_databases(db).await?;
        if unserviceable_routes > 0 {
            anyhow::bail!(
                "{} shard placement row(s) route to provisioning or disabled database placements",
                unserviceable_routes
            );
        }

        Ok(())
    }

    fn resolve_database_url_env(&self, database_url_env: &str) -> anyhow::Result<String> {
        if database_url_env == DEFAULT_DATABASE_URL_ENV {
            return Ok(self.uri.clone());
        }

        std::env::var(database_url_env).map_err(|_| {
            anyhow::anyhow!(
                "serviceable database placement references unset env var {}",
                database_url_env
            )
        })
    }

    pub(crate) fn database_url_env_is_resolved(&self, database_url_env: &str) -> bool {
        database_url_env == DEFAULT_DATABASE_URL_ENV || std::env::var_os(database_url_env).is_some()
    }

    #[allow(dead_code)]
    async fn placement_pools(&self) -> anyhow::Result<&PlacementPools> {
        self.placement_pools
            .get_or_try_init(|| async {
                let db = self.control().await?;
                let placements =
                    database_placements::list_serviceable_database_placements(db).await?;

                if placements.is_empty() {
                    anyhow::bail!("database_placements has no active or draining placements");
                }

                let mut pools_by_alias = HashMap::with_capacity(placements.len());

                for placement in placements {
                    let alias = placement.alias.clone();
                    pools_by_alias.insert(
                        alias,
                        PlacementPool {
                            database_url_env: placement.database_url_env,
                            pool: OnceCell::new(),
                        },
                    );
                }

                Ok(PlacementPools { pools_by_alias })
            })
            .await
    }

    async fn require_active_placement(&self, alias: &str) -> anyhow::Result<DatabasePlacement> {
        let db = self.control().await?;
        let placement = database_placements::select_database_placement(db, alias)
            .await?
            .ok_or_else(|| anyhow::anyhow!("database placement alias {} was not found", alias))?;

        if placement.status != DATABASE_PLACEMENT_STATUS_ACTIVE {
            anyhow::bail!(
                "database placement alias {} has status {}, which cannot receive new shard ownership",
                alias,
                placement.status
            );
        }

        Ok(placement)
    }

    async fn require_serviceable_placement(
        &self,
        alias: &str,
    ) -> anyhow::Result<DatabasePlacement> {
        let db = self.control().await?;
        let placement = database_placements::select_database_placement(db, alias)
            .await?
            .ok_or_else(|| anyhow::anyhow!("database placement alias {} was not found", alias))?;

        if !placement.can_serve_owned_shards() {
            anyhow::bail!(
                "database placement alias {} has status {}, which cannot serve existing shard ownership",
                alias,
                placement.status
            );
        }

        Ok(placement)
    }

    #[allow(dead_code)]
    async fn validate_shard_placement_alias(
        &self,
        placement: &ShardPlacement,
    ) -> anyhow::Result<()> {
        let database_placement = self
            .require_serviceable_placement(&placement.database_alias)
            .await?;

        require_shard_capable_placement(&database_placement)
    }
}

pub(crate) fn new_shard_placement_cache() -> Cache<ShardPlacementKey, ShardPlacement> {
    Cache::builder()
        .time_to_live(SHARD_PLACEMENT_CACHE_TTL)
        .max_capacity(SHARD_PLACEMENT_CACHE_CAPACITY)
        .build()
}

pub(crate) struct PlacementPools {
    #[allow(dead_code)]
    pools_by_alias: HashMap<String, PlacementPool>,
}

pub(crate) struct PlacementPool {
    database_url_env: String,
    #[allow(dead_code)]
    pool: OnceCell<PgPool>,
}

impl PlacementPool {
    #[allow(dead_code)]
    async fn get(
        &self,
        alias: &str,
        database_url: &str,
        max_connections_per_pool: u32,
        acquire_timeout: Duration,
        operation_timeout_config: Option<DatabaseOperationTimeoutConfig>,
    ) -> anyhow::Result<&PgPool> {
        self.pool
            .get_or_try_init(|| async {
                debug!(database_alias = %alias, "initializing postgres placement connection");

                runtime_pool_options(
                    max_connections_per_pool,
                    acquire_timeout,
                    operation_timeout_config,
                )
                .connect(database_url)
                .await
                .map_err(|error| {
                    let message = format!("database placement {alias} connection failed: {error}");
                    anyhow::Error::new(error).context(message)
                })
            })
            .await
    }
}

fn runtime_pool_options(
    max_connections: u32,
    acquire_timeout: Duration,
    operation_timeout_config: Option<DatabaseOperationTimeoutConfig>,
) -> PgPoolOptions {
    let options = PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(acquire_timeout);
    let Some(config) = operation_timeout_config else {
        return options;
    };

    let statement_timeout = format!("{}ms", config.statement_timeout().as_millis().max(1));
    let lock_timeout = format!("{}ms", config.lock_timeout().as_millis().max(1));
    options.after_connect(move |connection, _| {
        let statement_timeout = statement_timeout.clone();
        let lock_timeout = lock_timeout.clone();
        Box::pin(async move {
            sqlx::query("SELECT set_config('statement_timeout', $1, false)")
                .bind(statement_timeout)
                .execute(&mut *connection)
                .await?;
            sqlx::query("SELECT set_config('lock_timeout', $1, false)")
                .bind(lock_timeout)
                .execute(&mut *connection)
                .await?;
            Ok(())
        })
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ShardPlacementKey {
    run_id: Uuid,
    run_shard: i16,
}

fn normalize_alias(alias: String, label: &str) -> anyhow::Result<String> {
    let alias = alias.trim().to_string();
    if alias.is_empty() {
        anyhow::bail!("{} must not be empty", label);
    }

    Ok(alias)
}

fn validate_shard_capable_alias(
    placements_by_alias: &HashMap<&str, &crate::models::database_placement::DatabasePlacement>,
    alias: &str,
    config_name: &str,
) -> anyhow::Result<()> {
    let Some(placement) = placements_by_alias.get(alias) else {
        anyhow::bail!(
            "{}={} does not match an active or draining database placement",
            config_name,
            alias
        );
    };

    if !placement.accepts_new_shards() {
        anyhow::bail!(
            "{}={} points to placement status {}, which cannot receive new shard ownership",
            config_name,
            alias,
            placement.status
        );
    }

    if !placement.is_shard_capable() {
        anyhow::bail!(
            "{}={} points to placement role {}, which is not shard-capable",
            config_name,
            alias,
            placement.role
        );
    }

    Ok(())
}

fn require_shard_capable_placement(placement: &DatabasePlacement) -> anyhow::Result<()> {
    if !placement.is_shard_capable() {
        anyhow::bail!(
            "database placement alias {} has role {}, which is not shard-capable",
            placement.alias,
            placement.role
        );
    }

    Ok(())
}

#[allow(dead_code)]
fn validate_run_shard(run_shard: i16) -> anyhow::Result<()> {
    if !(0..crate::models::run_chunk::RUN_SHARD_COUNT).contains(&run_shard) {
        anyhow::bail!(
            "run_shard {} is outside the supported range 0..{}",
            run_shard,
            crate::models::run_chunk::RUN_SHARD_COUNT
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use sqlx::{
        PgPool,
        postgres::PgPoolOptions,
    };
    use uuid::Uuid;

    use super::*;
    use crate::models::{
        database_placement::DEFAULT_DATABASE_ALIAS,
        shard_placement::{
            SHARD_PLACEMENT_STATUS_ACTIVE,
            SHARD_PLACEMENT_STATUS_MOVING,
        },
    };

    #[test]
    fn default_config_uses_primary_aliases() {
        let config = PlacementConfig::default_single_database();

        assert_eq!(config.control_database_alias, DEFAULT_DATABASE_ALIAS);
        assert_eq!(config.default_shard_database_alias, DEFAULT_DATABASE_ALIAS);
        assert_eq!(
            config.shard_assignment_policy,
            ShardAssignmentPolicy::SingleDefault
        );
    }

    #[test]
    fn config_normalizes_aliases() {
        let config = PlacementConfig::new(
            " primary ".to_string(),
            " exec_a ".to_string(),
            " spread-active ".to_string(),
        )
        .unwrap();

        assert_eq!(config.control_database_alias, "primary");
        assert_eq!(config.default_shard_database_alias, "exec_a");
        assert_eq!(
            config.shard_assignment_policy,
            ShardAssignmentPolicy::SpreadActive
        );
    }

    #[test]
    fn config_rejects_empty_control_alias() {
        let error = PlacementConfig::new(
            " ".to_string(),
            "primary".to_string(),
            "single-default".to_string(),
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("control alias must not be empty")
        );
    }

    #[test]
    fn config_rejects_empty_default_shard_alias() {
        let error = PlacementConfig::new(
            "primary".to_string(),
            " ".to_string(),
            "single-default".to_string(),
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("default shard alias must not be empty")
        );
    }

    #[test]
    fn config_rejects_unknown_shard_assignment_policy() {
        let error = PlacementConfig::new(
            "primary".to_string(),
            "primary".to_string(),
            "unknown".to_string(),
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("unsupported shard assignment policy")
        );
    }

    #[test]
    fn operation_timeout_derives_bounded_server_timeouts() {
        let config = DatabaseOperationTimeoutConfig::new(Duration::from_secs(30)).unwrap();

        assert_eq!(config.operation_timeout(), Duration::from_secs(30));
        assert_eq!(config.statement_timeout(), Duration::from_secs(24));
        assert_eq!(config.lock_timeout(), Duration::from_secs(5));
    }

    #[test]
    fn operation_timeout_rejects_zero() {
        assert!(DatabaseOperationTimeoutConfig::new(Duration::ZERO).is_err());
    }

    #[test]
    fn validates_run_shard_range() {
        assert!(validate_run_shard(0).is_ok());
        assert!(validate_run_shard(127).is_ok());

        let low_error = validate_run_shard(-1).unwrap_err();
        assert!(
            low_error
                .to_string()
                .contains("outside the supported range")
        );

        let high_error = validate_run_shard(128).unwrap_err();
        assert!(
            high_error
                .to_string()
                .contains("outside the supported range")
        );
    }

    #[tokio::test]
    async fn database_context_get_returns_one_router_without_opening_pool() {
        let context = Context {
            config: Config {
                uri: "postgres://lazy-control-pool".to_string(),
                max_connections_per_pool: 5,
                acquire_timeout: Duration::from_secs(10),
                circuit_breaker_config: CircuitBreakerConfig::default(),
                placement_config: PlacementConfig::default_single_database(),
                operation_timeout_config: None,
            },
            cell: OnceCell::new(),
        };

        let first = context.get().await.unwrap() as *const DatabaseRouter;
        let second = context.get().await.unwrap() as *const DatabaseRouter;

        assert_eq!(first, second);
        assert!(context.cell.get().unwrap().control_pool.get().is_none());
    }

    #[sqlx::test(migrations = "../migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx router tests"]
    async fn configured_statement_timeout_cancels_slow_query_and_keeps_pool_usable(pool: PgPool) {
        let database_url = isolated_database_url(&pool).await;
        let database_router = DatabaseRouter::new(
            Config::new(
                database_url,
                1,
                Duration::from_secs(1),
                CircuitBreakerConfig::default(),
                PlacementConfig::default_single_database(),
            )
            .with_operation_timeout(Some(
                DatabaseOperationTimeoutConfig::new(Duration::from_millis(125)).unwrap(),
            )),
        );
        let db = database_router.control().await.unwrap();

        let error = sqlx::query("SELECT pg_sleep(1)")
            .execute(db)
            .await
            .unwrap_err();
        assert_eq!(
            error.as_database_error().and_then(|error| error.code()),
            Some(std::borrow::Cow::Borrowed("57014"))
        );
        assert_eq!(
            sqlx::query_scalar::<_, i32>("SELECT 1")
                .fetch_one(db)
                .await
                .unwrap(),
            1
        );

        db.close().await;
    }

    #[sqlx::test(migrations = "../migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx router tests"]
    async fn outer_database_deadline_rolls_back_owned_transaction(pool: PgPool) {
        sqlx::query("CREATE TABLE timeout_probe (value INTEGER NOT NULL)")
            .execute(&pool)
            .await
            .unwrap();
        let mut database_router = database_router_with_control_pool(pool.clone());
        database_router.operation_timeout_config =
            Some(DatabaseOperationTimeoutConfig::new(Duration::from_millis(20)).unwrap());

        let result = database_router
            .deadline_database_operation("primary", "test", async {
                let mut tx = pool.begin().await.unwrap();
                sqlx::query("INSERT INTO timeout_probe (value) VALUES (1)")
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                std::future::pending::<()>().await;
                tx.commit().await.unwrap();
            })
            .await;

        assert!(result.is_err());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM timeout_probe")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
    }

    #[sqlx::test(migrations = "../migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx router tests"]
    async fn execution_routes_active_primary_placement_to_control_pool(pool: PgPool) {
        let database_router = database_router_with_control_pool(pool);
        let run_id = Uuid::now_v7();

        insert_shard_placement(
            database_router.control().await.unwrap(),
            run_id,
            42,
            DEFAULT_DATABASE_ALIAS,
            SHARD_PLACEMENT_STATUS_ACTIVE,
        )
        .await;

        let control = database_router.control().await.unwrap();
        let routed = database_router.execution(run_id, 42).await.unwrap();
        assert_eq!(
            control.connect_options().get_database(),
            routed.connect_options().get_database()
        );

        let placement = database_router
            .execution_placement(run_id, 42)
            .await
            .unwrap();
        assert_eq!(placement.database_alias, DEFAULT_DATABASE_ALIAS);
        assert_eq!(placement.status, SHARD_PLACEMENT_STATUS_ACTIVE);
    }

    #[sqlx::test(migrations = "../migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx router tests"]
    async fn draining_placement_serves_owned_routes_but_rejects_new_ownership(pool: PgPool) {
        sqlx::query(
            r#"
            INSERT INTO database_placements (alias, database_url_env, role, status)
            VALUES ('shard_001', 'DATABASE_URL', 'shard', 'draining')
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();
        let database_url = isolated_database_url(&pool).await;
        let database_router = database_router_with_control_pool_and_uri(pool, database_url);
        let run_id = Uuid::now_v7();
        insert_shard_placement(
            database_router.control().await.unwrap(),
            run_id,
            42,
            "shard_001",
            SHARD_PLACEMENT_STATUS_ACTIVE,
        )
        .await;

        database_router.execution(run_id, 42).await.unwrap();

        let error = database_router
            .execution_target_database("shard_001")
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("cannot receive new shard ownership")
        );
    }

    #[sqlx::test(migrations = "../migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx router tests"]
    async fn execution_rejects_moving_placement(pool: PgPool) {
        let database_router = database_router_with_control_pool(pool);
        let run_id = Uuid::now_v7();

        insert_shard_placement(
            database_router.control().await.unwrap(),
            run_id,
            7,
            DEFAULT_DATABASE_ALIAS,
            SHARD_PLACEMENT_STATUS_ACTIVE,
        )
        .await;
        let draining =
            mark_test_shard_placement_draining(database_router.control().await.unwrap(), run_id, 7)
                .await;
        mark_test_shard_placement_moving(database_router.control().await.unwrap(), &draining).await;

        let error = database_router.execution(run_id, 7).await.unwrap_err();
        assert!(error.to_string().contains("not dispatchable"));
    }

    #[sqlx::test(migrations = "../migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx router tests"]
    async fn execution_placement_refreshes_after_explicit_invalidation(pool: PgPool) {
        let database_router = database_router_with_control_pool(pool);
        let run_id = Uuid::now_v7();

        insert_shard_placement(
            database_router.control().await.unwrap(),
            run_id,
            7,
            DEFAULT_DATABASE_ALIAS,
            SHARD_PLACEMENT_STATUS_ACTIVE,
        )
        .await;

        let initial = database_router
            .execution_placement(run_id, 7)
            .await
            .unwrap();
        assert_eq!(initial.status, SHARD_PLACEMENT_STATUS_ACTIVE);
        assert_eq!(initial.route_version, 1);

        let draining =
            mark_test_shard_placement_draining(database_router.control().await.unwrap(), run_id, 7)
                .await;
        mark_test_shard_placement_moving(database_router.control().await.unwrap(), &draining).await;

        let cached = database_router
            .execution_placement(run_id, 7)
            .await
            .unwrap();
        assert_eq!(cached.status, SHARD_PLACEMENT_STATUS_ACTIVE);
        assert_eq!(cached.route_version, 1);

        database_router
            .invalidate_execution_placement(run_id, 7)
            .await;
        let refreshed = database_router
            .execution_placement(run_id, 7)
            .await
            .unwrap();
        assert_eq!(refreshed.status, SHARD_PLACEMENT_STATUS_MOVING);
        assert_eq!(refreshed.route_version, 4);

        let error = database_router.execution(run_id, 7).await.unwrap_err();
        assert!(error.to_string().contains("not dispatchable"));
    }

    #[sqlx::test(migrations = "../migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx router tests"]
    async fn stale_execution_route_cannot_claim_after_move_begins(pool: PgPool) {
        let database_router = database_router_with_control_pool(pool);
        let run_id = Uuid::now_v7();
        let chunk_id =
            seed_claimable_chunk(database_router.control().await.unwrap(), run_id, 7).await;
        insert_shard_placement(
            database_router.control().await.unwrap(),
            run_id,
            7,
            DEFAULT_DATABASE_ALIAS,
            SHARD_PLACEMENT_STATUS_ACTIVE,
        )
        .await;
        let stale_route = database_router.execution_route(run_id, 7).await.unwrap();

        upsert_local_shard_admission(
            database_router.control().await.unwrap(),
            LocalShardAdmissionDraft {
                run_id,
                run_shard: 7,
                database_alias: DEFAULT_DATABASE_ALIAS.to_string(),
                write_epoch: stale_route.placement.write_epoch,
                state: LocalShardAdmissionState::Draining,
                redirect_database_alias: Some("shard_001".to_string()),
                move_fence: None,
            },
        )
        .await
        .unwrap();

        let draining =
            mark_test_shard_placement_draining(database_router.control().await.unwrap(), run_id, 7)
                .await;
        mark_test_shard_placement_moving(database_router.control().await.unwrap(), &draining).await;

        let error = crate::db::workflows::chunk_processing::claim_routed_chunk_for_processing(
            &database_router,
            &stale_route,
            run_id,
            7,
            chunk_id,
            60,
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error.downcast_ref::<LocalShardAdmissionError>(),
            Some(LocalShardAdmissionError::RejectedState { .. })
        ));
    }

    #[sqlx::test(migrations = "../migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx router tests"]
    async fn worker_stale_hint_invalidates_cache_and_claims_current_epoch(pool: PgPool) {
        let database_router = database_router_with_control_pool(pool.clone());
        let run_id = Uuid::now_v7();
        let chunk_id = seed_claimable_chunk(&pool, run_id, 7).await;
        insert_shard_placement(
            &pool,
            run_id,
            7,
            DEFAULT_DATABASE_ALIAS,
            SHARD_PLACEMENT_STATUS_ACTIVE,
        )
        .await;
        let cached = database_router
            .execution_placement(run_id, 7)
            .await
            .unwrap();
        assert_eq!(cached.write_epoch, 1);

        sqlx::query(
            r#"
            UPDATE shard_placements
            SET write_epoch = 2,
                route_version = route_version + 1,
                updated_at = now()
            WHERE run_id = $1::uuid
              AND run_shard = 7
            "#,
        )
        .bind(run_id)
        .execute(&pool)
        .await
        .unwrap();
        upsert_local_shard_admission(
            &pool,
            LocalShardAdmissionDraft {
                run_id,
                run_shard: 7,
                database_alias: DEFAULT_DATABASE_ALIAS.to_string(),
                write_epoch: 2,
                state: LocalShardAdmissionState::Open,
                redirect_database_alias: None,
                move_fence: None,
            },
        )
        .await
        .unwrap();
        let hinted_route = database_router
            .execution_write_route(LocalShardRouteHint {
                run_id,
                run_shard: 7,
                database_alias: DEFAULT_DATABASE_ALIAS.to_string(),
                write_epoch: 1,
            })
            .await
            .unwrap();

        let (_, claimed) = crate::cli::claim_hinted_chunk_with_route_refresh(
            &database_router,
            &hinted_route,
            chunk_id,
        )
        .await
        .unwrap();

        let claimed = claimed.expect("current route should claim the pending chunk");
        assert_eq!(claimed.write_epoch, 2);
        assert_eq!(claimed.status, "leased");
        assert_eq!(
            database_router
                .execution_placement(run_id, 7)
                .await
                .unwrap()
                .write_epoch,
            2
        );
    }

    #[sqlx::test(migrations = "../migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx router tests"]
    async fn missing_admission_is_repaired_for_an_unchanged_route(pool: PgPool) {
        let database_router = database_router_with_control_pool(pool.clone());
        let run_id = Uuid::now_v7();
        insert_shard_placement(
            &pool,
            run_id,
            7,
            DEFAULT_DATABASE_ALIAS,
            SHARD_PLACEMENT_STATUS_ACTIVE,
        )
        .await;
        let route = database_router.execution_route(run_id, 7).await.unwrap();

        database_router
            .begin_execution_admission(&route)
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();

        let admission = sqlx::query_as::<_, (String, i64, String)>(
            r#"
            SELECT database_alias, write_epoch, state
            FROM local_shard_admissions
            WHERE run_id = $1::uuid
              AND run_shard = 7
            "#,
        )
        .bind(run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            admission,
            (DEFAULT_DATABASE_ALIAS.to_string(), 1, "open".to_string())
        );
    }

    #[sqlx::test(migrations = "../migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx router tests"]
    async fn missing_admission_repair_rechecks_route_after_waiting_for_fence(pool: PgPool) {
        let database_router = database_router_with_control_pool(pool.clone());
        let run_id = Uuid::now_v7();
        insert_shard_placement(
            &pool,
            run_id,
            7,
            DEFAULT_DATABASE_ALIAS,
            SHARD_PLACEMENT_STATUS_ACTIVE,
        )
        .await;
        sqlx::query(
            r#"
            INSERT INTO database_placements (alias, database_url_env, role, status)
            VALUES ('shard_001', 'DATABASE_URL', 'shard', 'active')
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();
        let stale_route = database_router.execution_route(run_id, 7).await.unwrap();

        let mut move_tx = pool.begin().await.unwrap();
        crate::db::shard_write_fence::lock_exclusive(&mut move_tx, run_id, 7)
            .await
            .unwrap();
        let repair = tokio::spawn(async move {
            database_router
                .begin_execution_admission(&stale_route)
                .await
        });
        wait_for_waiting_advisory_lock(&pool, "missing-admission repair").await;

        sqlx::query(
            r#"
            UPDATE shard_placements
            SET status = 'copying',
                move_target_database_alias = 'shard_001',
                route_version = route_version + 1,
                updated_at = now()
            WHERE run_id = $1::uuid
              AND run_shard = 7
            "#,
        )
        .bind(run_id)
        .execute(&mut *move_tx)
        .await
        .unwrap();
        move_tx.commit().await.unwrap();

        let error = repair.await.unwrap().unwrap_err();
        assert!(matches!(
            error.downcast_ref::<LocalShardAdmissionError>(),
            Some(LocalShardAdmissionError::Missing { .. })
        ));
        let admission_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::bigint FROM local_shard_admissions WHERE run_id = $1::uuid AND run_shard = 7",
        )
        .bind(run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(admission_count, 0);
    }

    #[sqlx::test(migrations = "../migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx router tests"]
    async fn routed_cancellation_waits_for_shard_move_admission(pool: PgPool) {
        let database_name = sqlx::query_scalar::<_, String>("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .unwrap();
        let mut database_url = url::Url::parse(&std::env::var("DATABASE_URL").unwrap()).unwrap();
        database_url.set_path(&database_name);
        let runtime_pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url.as_str())
            .await
            .unwrap();
        let database_router = database_router_with_control_pool(runtime_pool);
        let run_id = Uuid::now_v7();
        seed_claimable_chunk(database_router.control().await.unwrap(), run_id, 7).await;
        insert_shard_placement(
            database_router.control().await.unwrap(),
            run_id,
            7,
            DEFAULT_DATABASE_ALIAS,
            SHARD_PLACEMENT_STATUS_ACTIVE,
        )
        .await;

        let mut move_tx = database_router
            .control()
            .await
            .unwrap()
            .begin()
            .await
            .unwrap();
        crate::db::shard_write_fence::lock_exclusive(&mut move_tx, run_id, 7)
            .await
            .unwrap();
        let cancellation =
            crate::db::workflows::run_cancel::cancel_run_routed(&database_router, run_id);
        tokio::pin!(cancellation);

        assert!(
            tokio::time::timeout(Duration::from_millis(500), cancellation.as_mut())
                .await
                .is_err(),
            "cancellation must not mutate source rows while movement holds exclusive admission"
        );

        move_tx.rollback().await.unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(10), cancellation.as_mut())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(outcome.cancelled);
    }

    #[sqlx::test(migrations = "../migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx router tests"]
    async fn dropped_write_transaction_releases_waiting_move_fence(pool: PgPool) {
        let database_router = database_router_with_control_pool(pool.clone());
        let run_id = Uuid::now_v7();
        insert_shard_placement(
            &pool,
            run_id,
            7,
            DEFAULT_DATABASE_ALIAS,
            SHARD_PLACEMENT_STATUS_ACTIVE,
        )
        .await;
        upsert_local_shard_admission(
            &pool,
            LocalShardAdmissionDraft {
                run_id,
                run_shard: 7,
                database_alias: DEFAULT_DATABASE_ALIAS.to_string(),
                write_epoch: 1,
                state: LocalShardAdmissionState::Open,
                redirect_database_alias: None,
                move_fence: None,
            },
        )
        .await
        .unwrap();
        let route = database_router.execution_route(run_id, 7).await.unwrap();
        let admitted_write = database_router
            .begin_execution_admission(&route)
            .await
            .unwrap();

        let move_pool = pool.clone();
        let mover = tokio::spawn(async move {
            let mut tx = move_pool.begin().await.unwrap();
            crate::db::shard_write_fence::lock_exclusive(&mut tx, run_id, 7)
                .await
                .unwrap();
            sqlx::query(
                "UPDATE local_shard_admissions SET state = 'draining' WHERE run_id = $1::uuid AND run_shard = 7",
            )
            .bind(run_id)
            .execute(&mut *tx)
            .await
            .unwrap();
            tx.commit().await.unwrap();
        });
        wait_for_waiting_advisory_lock(&pool, "move").await;

        drop(admitted_write);
        tokio::time::timeout(Duration::from_secs(5), mover)
            .await
            .expect("move fence remained blocked after admitted transaction was dropped")
            .unwrap();

        let state = sqlx::query_scalar::<_, String>(
            "SELECT state FROM local_shard_admissions WHERE run_id = $1::uuid AND run_shard = 7",
        )
        .bind(run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(state, "draining");
    }

    #[sqlx::test(migrations = "../migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx router tests"]
    async fn cleanup_admission_allows_draining_but_rejects_moving(pool: PgPool) {
        let database_router = database_router_with_control_pool(pool);
        let run_id = Uuid::now_v7();
        seed_claimable_chunk(database_router.control().await.unwrap(), run_id, 7).await;
        insert_shard_placement(
            database_router.control().await.unwrap(),
            run_id,
            7,
            DEFAULT_DATABASE_ALIAS,
            SHARD_PLACEMENT_STATUS_ACTIVE,
        )
        .await;
        let draining =
            mark_test_shard_placement_draining(database_router.control().await.unwrap(), run_id, 7)
                .await;

        let draining_routes = database_router
            .execution_read_routes_with_fences_for_run(run_id)
            .await
            .unwrap();
        database_router
            .begin_execution_cleanup_admission(&draining_routes)
            .await
            .unwrap()
            .rollback()
            .await
            .unwrap();

        mark_test_shard_placement_moving(database_router.control().await.unwrap(), &draining).await;
        let moving_routes = database_router
            .execution_read_routes_with_fences_for_run(run_id)
            .await
            .unwrap();
        let error = database_router
            .begin_execution_cleanup_admission(&moving_routes)
            .await
            .unwrap_err();

        assert!(matches!(
            error.downcast_ref::<ExecutionRouteError>(),
            Some(ExecutionRouteError::NonDispatchableShardPlacement { status, .. })
                if status == SHARD_PLACEMENT_STATUS_MOVING
        ));
    }

    #[sqlx::test(migrations = "../migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx router tests"]
    async fn execution_placement_requires_stored_shard_placement(pool: PgPool) {
        let database_router = database_router_with_control_pool(pool);
        let run_id = Uuid::now_v7();

        let error = database_router
            .execution_placement(run_id, 3)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("missing shard placement"));
        assert!(
            error
                .to_string()
                .contains("run creation must insert shard_placements rows")
        );
    }

    #[sqlx::test(migrations = "../migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx router tests"]
    async fn execution_routes_for_run_returns_routed_primary_pools(pool: PgPool) {
        let database_router = database_router_with_control_pool(pool);
        let run_id = Uuid::now_v7();

        insert_shard_placement(
            database_router.control().await.unwrap(),
            run_id,
            2,
            DEFAULT_DATABASE_ALIAS,
            SHARD_PLACEMENT_STATUS_ACTIVE,
        )
        .await;
        insert_shard_placement(
            database_router.control().await.unwrap(),
            run_id,
            9,
            DEFAULT_DATABASE_ALIAS,
            SHARD_PLACEMENT_STATUS_ACTIVE,
        )
        .await;

        let routed = database_router
            .execution_routes_for_run(run_id)
            .await
            .unwrap();

        assert_eq!(routed.len(), 2);
        assert_eq!(routed[0].0, 2);
        assert_eq!(routed[0].1, DEFAULT_DATABASE_ALIAS);
        assert_eq!(routed[1].0, 9);
        assert_eq!(routed[1].1, DEFAULT_DATABASE_ALIAS);
    }

    #[sqlx::test(migrations = "../migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx router tests"]
    async fn execution_read_routes_for_run_include_moving_placements(pool: PgPool) {
        let database_router = database_router_with_control_pool(pool);
        let run_id = Uuid::now_v7();

        insert_shard_placement(
            database_router.control().await.unwrap(),
            run_id,
            7,
            DEFAULT_DATABASE_ALIAS,
            SHARD_PLACEMENT_STATUS_ACTIVE,
        )
        .await;
        let draining =
            mark_test_shard_placement_draining(database_router.control().await.unwrap(), run_id, 7)
                .await;
        mark_test_shard_placement_moving(database_router.control().await.unwrap(), &draining).await;

        let dispatch_error = database_router
            .execution_routes_for_run(run_id)
            .await
            .unwrap_err();
        assert!(dispatch_error.to_string().contains("not dispatchable"));

        let readable = database_router
            .execution_read_routes_for_run(run_id)
            .await
            .unwrap();
        assert_eq!(readable.len(), 1);
        assert_eq!(readable[0].0, 7);
        assert_eq!(readable[0].1, DEFAULT_DATABASE_ALIAS);
    }

    #[sqlx::test(migrations = "../migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx router tests"]
    async fn placement_rejects_alias_disabled_after_pool_initialization(pool: PgPool) {
        let database_router = database_router_with_control_pool(pool);

        database_router
            .placement(DEFAULT_DATABASE_ALIAS)
            .await
            .unwrap();
        assert!(database_router.placement_pools.get().is_some());

        sqlx::query(
            r#"
            UPDATE database_placements
            SET status = 'disabled', updated_at = now()
            WHERE alias = $1
            "#,
        )
        .bind(DEFAULT_DATABASE_ALIAS)
        .execute(database_router.control().await.unwrap())
        .await
        .unwrap();

        let error = database_router
            .placement(DEFAULT_DATABASE_ALIAS)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("status disabled"));
        assert!(database_router.placement_pools.get().is_some());
    }

    #[sqlx::test(migrations = "../migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx router tests"]
    async fn execution_rejects_live_non_shard_role(pool: PgPool) {
        let database_router = database_router_with_control_pool(pool);
        let run_id = Uuid::now_v7();

        insert_shard_placement(
            database_router.control().await.unwrap(),
            run_id,
            7,
            DEFAULT_DATABASE_ALIAS,
            SHARD_PLACEMENT_STATUS_ACTIVE,
        )
        .await;
        database_router.execution(run_id, 7).await.unwrap();

        sqlx::query(
            r#"
            UPDATE database_placements
            SET role = 'control', updated_at = now()
            WHERE alias = $1
            "#,
        )
        .bind(DEFAULT_DATABASE_ALIAS)
        .execute(database_router.control().await.unwrap())
        .await
        .unwrap();

        database_router
            .placement(DEFAULT_DATABASE_ALIAS)
            .await
            .unwrap();
        let error = database_router.execution(run_id, 7).await.unwrap_err();
        assert!(error.to_string().contains("role control"));
        assert!(error.to_string().contains("not shard-capable"));
    }

    #[sqlx::test(migrations = "../migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx router tests"]
    async fn placement_discovers_alias_added_after_pool_initialization(pool: PgPool) {
        let database_url = isolated_database_url(&pool).await;
        let database_router = database_router_with_control_pool_and_uri(pool, database_url);
        database_router
            .placement(DEFAULT_DATABASE_ALIAS)
            .await
            .unwrap();

        sqlx::query(
            r#"
            INSERT INTO database_placements (alias, database_url_env, role, status)
            VALUES ('shard_late', 'DATABASE_URL', 'shard', 'active')
            "#,
        )
        .execute(database_router.control().await.unwrap())
        .await
        .unwrap();

        database_router.placement("shard_late").await.unwrap();
        assert!(
            database_router
                .dynamic_placement_pools
                .get("shard_late")
                .await
                .is_some()
        );
    }

    #[sqlx::test(migrations = "../migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx router tests"]
    async fn placement_requires_restart_after_connection_metadata_changes(pool: PgPool) {
        let database_router = database_router_with_control_pool(pool);
        database_router
            .placement(DEFAULT_DATABASE_ALIAS)
            .await
            .unwrap();

        sqlx::query(
            r#"
            UPDATE database_placements
            SET database_url_env = 'VIGILO_TEST_REPLACED_DATABASE_URL',
                updated_at = now()
            WHERE alias = $1
            "#,
        )
        .bind(DEFAULT_DATABASE_ALIAS)
        .execute(database_router.control().await.unwrap())
        .await
        .unwrap();

        let error = database_router
            .placement(DEFAULT_DATABASE_ALIAS)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("changed database_url_env"));
        assert!(error.to_string().contains("restart Vigilo"));
    }

    #[sqlx::test(migrations = "../migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx router tests"]
    async fn active_placement_with_missing_env_var_fails_clearly(pool: PgPool) {
        let database_router = database_router_with_control_pool(pool);

        sqlx::query(
            r#"
            INSERT INTO database_placements (alias, database_url_env, role, status)
            VALUES ('shard_001', 'VIGILO_TEST_MISSING_SHARD_URL', 'shard', 'active')
            "#,
        )
        .execute(database_router.control().await.unwrap())
        .await
        .unwrap();

        let error = database_router.placement("shard_001").await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("serviceable database placement references unset env var")
        );
    }

    #[sqlx::test(migrations = "../migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx router tests"]
    async fn unrelated_missing_placement_secret_does_not_block_healthy_database(pool: PgPool) {
        let database_router = database_router_with_control_pool(pool);

        sqlx::query(
            r#"
            INSERT INTO database_placements (alias, database_url_env, role, status)
            VALUES ('shard_missing', 'VIGILO_TEST_MISSING_SHARD_URL', 'shard', 'active')
            "#,
        )
        .execute(database_router.control().await.unwrap())
        .await
        .unwrap();

        database_router
            .placement(DEFAULT_DATABASE_ALIAS)
            .await
            .unwrap();
        let error = database_router
            .placement("shard_missing")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("VIGILO_TEST_MISSING_SHARD_URL"));
    }

    fn database_router_with_control_pool(pool: PgPool) -> DatabaseRouter {
        database_router_with_control_pool_and_uri(
            pool,
            "postgres://injected-control-pool".to_string(),
        )
    }

    fn database_router_with_control_pool_and_uri(pool: PgPool, uri: String) -> DatabaseRouter {
        let database_router = DatabaseRouter {
            uri,
            max_connections_per_pool: 5,
            acquire_timeout: Duration::from_secs(10),
            placement_config: PlacementConfig::default_single_database(),
            circuit_breakers: circuit_breaker::DatabaseCircuitBreakers::new(
                CircuitBreakerConfig::default(),
            ),
            operation_timeout_config: None,
            control_pool: OnceCell::new(),
            placement_pools: OnceCell::new(),
            dynamic_placement_pools: Cache::builder().max_capacity(1_000).build(),
            shard_placement_cache: new_shard_placement_cache(),
        };
        assert!(database_router.control_pool.set(pool).is_ok());
        database_router
    }

    async fn isolated_database_url(pool: &PgPool) -> String {
        let database_name = sqlx::query_scalar::<_, String>("SELECT current_database()")
            .fetch_one(pool)
            .await
            .unwrap();
        let mut database_url =
            url::Url::parse(&std::env::var(DEFAULT_DATABASE_URL_ENV).unwrap()).unwrap();
        database_url.set_path(&database_name);
        database_url.to_string()
    }

    async fn wait_for_waiting_advisory_lock(pool: &PgPool, operation: &str) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let waiting = sqlx::query_scalar::<_, bool>(
                    r#"
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_locks
                        WHERE locktype = 'advisory'
                          AND database = (
                              SELECT oid
                              FROM pg_database
                              WHERE datname = current_database()
                          )
                          AND NOT granted
                    )
                    "#,
                )
                .fetch_one(pool)
                .await
                .unwrap();
                if waiting {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{operation} did not wait behind shard admission"));
    }

    async fn insert_shard_placement(
        db: &PgPool,
        run_id: Uuid,
        run_shard: i16,
        database_alias: &str,
        status: &str,
    ) {
        sqlx::query(
            r#"
            INSERT INTO shard_placements (run_id, run_shard, database_alias, status)
            VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(run_id)
        .bind(run_shard)
        .bind(database_alias)
        .bind(status)
        .execute(db)
        .await
        .unwrap();
    }

    async fn mark_test_shard_placement_draining(
        db: &PgPool,
        run_id: Uuid,
        run_shard: i16,
    ) -> ShardPlacement {
        sqlx::query(
            r#"
            INSERT INTO database_placements (alias, database_url_env, role, status)
            VALUES ('shard_001', 'DATABASE_URL', 'shard', 'active')
            ON CONFLICT (alias) DO NOTHING
            "#,
        )
        .execute(db)
        .await
        .unwrap();

        let current = shard_placements::select_shard_placement(db, run_id, run_shard)
            .await
            .unwrap()
            .unwrap();
        let copying = shard_placements::mark_shard_placement_copying(
            db,
            run_id,
            run_shard,
            &current.database_alias,
            current.route_version,
            "shard_001",
        )
        .await
        .unwrap()
        .unwrap();
        shard_placements::mark_shard_placement_draining(
            db,
            run_id,
            run_shard,
            &copying.database_alias,
            copying.route_version,
            copying.move_target_database_alias.as_deref().unwrap(),
        )
        .await
        .unwrap()
        .unwrap()
    }

    async fn mark_test_shard_placement_moving(
        db: &PgPool,
        draining: &ShardPlacement,
    ) -> ShardPlacement {
        shard_placements::mark_shard_placement_moving(
            db,
            draining.run_id,
            draining.run_shard,
            &draining.database_alias,
            draining.route_version,
            draining.move_target_database_alias.as_deref().unwrap(),
        )
        .await
        .unwrap()
        .unwrap()
    }

    async fn seed_claimable_chunk(db: &PgPool, run_id: Uuid, run_shard: i16) -> Uuid {
        let dataset_id = Uuid::now_v7();
        let dataset_version_id = Uuid::now_v7();
        let chunk_id = Uuid::now_v7();

        sqlx::query(
            r#"
            INSERT INTO dataset_versions (dataset_version_id, dataset_id, dataset_version)
            VALUES ($1::uuid, $2::uuid, 'test')
            "#,
        )
        .bind(dataset_version_id)
        .bind(dataset_id)
        .execute(db)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO runs (
                id, run_key,
                dataset_id, dataset_version_id, dataset_version,
                evaluation_profile_id, evaluation_profile_version,
                profile_version_id, profile_hash,
                aggregation_policy_id, aggregation_policy_version, aggregation_policy_hash,
                agent_provider, agent_name,
                prompt_config_id, prompt_config_version,
                status, expected_execution_count
            )
            VALUES (
                $1::uuid, $2,
                $3::uuid, $4::uuid, 'test',
                'profile', '1', 'profile:1', 'profile-hash',
                'policy', '1', 'policy-hash',
                'test', 'agent', 'prompt', '1',
                'running'::run_status, 1
            )
            "#,
        )
        .bind(run_id)
        .bind(format!("run-{run_id}"))
        .bind(dataset_id)
        .bind(dataset_version_id)
        .execute(db)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO run_chunks (
                id, run_id, run_shard, dataset_version_id,
                profile_group_id, ordinal_start, ordinal_end, status
            )
            VALUES ($1::uuid, $2::uuid, $3, $4::uuid, 'default', 0, 1, 'pending')
            "#,
        )
        .bind(chunk_id)
        .bind(run_id)
        .bind(run_shard)
        .bind(dataset_version_id)
        .execute(db)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO run_snapshots (
                run_id, run_shard, run_key,
                dataset_id, dataset_version_id, dataset_version,
                evaluation_profile_id, evaluation_profile_version,
                profile_version_id, profile_hash,
                aggregation_policy_id, aggregation_policy_version, aggregation_policy_hash,
                agent_provider, agent_name,
                prompt_config_id, prompt_config_version,
                expected_execution_count
            )
            VALUES (
                $1::uuid, $2, $3,
                $4::uuid, $5::uuid, 'test',
                'profile', '1', 'profile:1', 'profile-hash',
                'policy', '1', 'policy-hash',
                'test', 'agent', 'prompt', '1', 1
            )
            "#,
        )
        .bind(run_id)
        .bind(run_shard)
        .bind(format!("run-{run_id}"))
        .bind(dataset_id)
        .bind(dataset_version_id)
        .execute(db)
        .await
        .unwrap();

        chunk_id
    }
}
