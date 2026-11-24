//! Trusted operator access to the control placement and run-shard catalogs.
//!
//! Reads use the standalone control pool, with a keyset limit for the global
//! catalog and the schema's shard limit for one run. Connection references are
//! resolved from explicit operator configuration. Registration creates no
//! routable ownership. Activation probes the intended schema, then commits only
//! if the catalog and ownership guards still agree with the proof.

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::Duration,
};

use sqlx::{
    ConnectOptions,
    PgConnection,
    PgPool,
};
use uuid::Uuid;

use super::{
    Client,
    Error,
    setup::DatabaseIdentity,
};
use crate::{
    context::database::{
        OperatorDatabaseBinding,
        standalone_connect_options,
    },
    db::{
        tables::database_placements,
        workflows::shard_admin,
    },
    models::{
        database_placement::{
            DEFAULT_DATABASE_URL_ENV,
            DatabasePlacement,
        },
        run_chunk::RUN_SHARD_COUNT,
        shard_placement::ShardPlacement,
    },
};

const MAX_PAGE_SIZE: u16 = 100;
const MAX_ACTIVATION_PEERS: i64 = 128;
const ACTIVATION_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_MOVE_BATCHES: u16 = 64;
const MOVE_STEP_TIMEOUT: Duration = Duration::from_secs(30);

/// Operator handle sharing the standalone client's control pool.
#[derive(Clone)]
pub struct PlacementClient {
    client: Client,
    bindings: Arc<BTreeMap<String, OperatorDatabaseBinding>>,
}

/// One catalog page in ascending alias order. Defaults to 50 rows.
pub struct ListDatabasePlacementsRequest {
    /// Maximum returned rows, from 1 through 100.
    pub page_size: u16,
    /// Exclusive alias from the preceding page. `None` starts a traversal.
    pub after_alias: Option<String>,
}

impl Default for ListDatabasePlacementsRequest {
    fn default() -> Self {
        Self {
            page_size: 50,
            after_alias: None,
        }
    }
}

/// One bounded page of placement metadata.
#[derive(Debug)]
pub struct DatabasePlacementPage {
    pub items: Vec<DatabasePlacementInfo>,
    /// Pass this value as `after_alias` to continue the traversal.
    pub next_after_alias: Option<String>,
}

/// Operator-visible catalog metadata. The connection reference is a name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabasePlacementInfo {
    pub alias: String,
    pub database_url_reference: String,
    pub role: String,
    pub status: String,
}

impl DatabasePlacementInfo {
    fn from_row(row: DatabasePlacement) -> Self {
        Self {
            alias: row.alias,
            database_url_reference: row.database_url_env,
            role: row.role,
            status: row.status,
        }
    }
}

/// Creates a shard-only registration using a configured connection reference.
pub struct RegisterDatabasePlacement {
    pub alias: String,
    pub database_url_reference: String,
}

/// Registration result. A matching retry returns current metadata with `inserted` false.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseRegistration {
    pub placement: DatabasePlacementInfo,
    pub inserted: bool,
}

/// Result of a placement activation attempt that completed successfully.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseActivation {
    pub placement: DatabasePlacementInfo,
    /// This invocation transitioned the row from provisioning to active.
    pub changed: bool,
    /// A fresh readiness probe ran in this call. An already-active fast path skips it.
    pub readiness_verified: bool,
    /// Number of matching migration fingerprints when a probe ran.
    pub migration_count: Option<usize>,
}

/// A single run's control-side ownership and fencing metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunShardPlacement {
    pub run_id: Uuid,
    pub run_shard: i16,
    pub database_alias: String,
    pub status: String,
    pub move_target_database_alias: Option<String>,
    pub route_version: i64,
    pub write_epoch: i64,
}

/// Starts or resumes a specific move. Keep the same identity fields on retry;
/// the batch budget may change on each call.
pub struct MoveRunShardRequest {
    /// Caller-generated UUID, retained before the first call.
    pub operation_id: Uuid,
    pub run_id: Uuid,
    pub run_shard: i16,
    /// Route version from the active source in [`PlacementClient::list_run_shards`].
    pub expected_route_version: i64,
    pub target_database_alias: String,
    /// Number of nonempty batches allowed in this call, from 1 through 64.
    pub max_batches: u16,
}

/// The next stage of a move. A caller can resume every unfinished stage with
/// the same request identity after waiting for work or recovering a dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RunShardMoveProgress {
    ResettingTarget,
    Copying,
    CatchingUp,
    /// Previously admitted source work must settle or recover before cutover.
    Draining,
    CuttingOver,
    /// The move completed. The route may have changed after completion.
    Completed,
}

/// Durable move identity and the work completed by this call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MoveRunShardOutcome {
    pub operation_id: Uuid,
    pub progress: RunShardMoveProgress,
    pub batches_processed: u16,
}

impl RunShardPlacement {
    fn from_row(row: ShardPlacement) -> Self {
        Self {
            run_id: row.run_id,
            run_shard: row.run_shard,
            database_alias: row.database_alias,
            status: row.status,
            move_target_database_alias: row.move_target_database_alias,
            route_version: row.route_version,
            write_epoch: row.write_epoch,
        }
    }
}

impl PlacementClient {
    /// Binds operator-owned PostgreSQL URLs to catalog connection reference names.
    /// `DATABASE_URL` belongs to the control client. Each shard URL declares
    /// its host, login, and database. Construction validates without connecting.
    pub fn new(client: Client, database_urls: BTreeMap<String, String>) -> Result<Self, Error> {
        let mut bindings = BTreeMap::new();
        for (reference, uri) in database_urls {
            if !valid_name(&reference) || reference == DEFAULT_DATABASE_URL_ENV {
                return Err(Error::InvalidConfiguration(
                    "invalid shard database reference",
                ));
            }
            let url = reqwest::Url::parse(&uri).map_err(|_| {
                Error::InvalidConfiguration("invalid shard PostgreSQL connection string")
            })?;
            if !matches!(url.scheme(), "postgres" | "postgresql")
                || url.host_str().is_none()
                || url.username().is_empty()
                || url.path().trim_matches('/').is_empty()
            {
                return Err(Error::InvalidConfiguration(
                    "shard binding requires an explicit PostgreSQL host, login, and database",
                ));
            }
            let options = standalone_connect_options(&uri).map_err(|_| {
                Error::InvalidConfiguration("invalid shard PostgreSQL connection string")
            })?;
            bindings.insert(reference, OperatorDatabaseBinding::new(options));
        }
        Ok(Self {
            client,
            bindings: Arc::new(bindings),
        })
    }

    /// Reads one page from the control catalog with one indexed data query.
    /// Each call observes committed metadata at the time of that read.
    pub async fn list_database_placements(
        &self,
        request: &ListDatabasePlacementsRequest,
    ) -> Result<DatabasePlacementPage, Error> {
        if !(1..=MAX_PAGE_SIZE).contains(&request.page_size) {
            return Err(Error::InvalidInput("placement page size must be 1–100"));
        }
        let db = self.control().await?;
        let page_size = usize::from(request.page_size);
        let mut rows = database_placements::list_database_placement_page(
            db,
            request.after_alias.as_deref(),
            i64::from(request.page_size) + 1,
        )
        .await
        .map_err(Error::Dependency)?;
        let next_after_alias = if rows.len() > page_size {
            rows.truncate(page_size);
            rows.last().map(|row| row.alias.clone())
        } else {
            None
        };
        Ok(DatabasePlacementPage {
            items: rows
                .into_iter()
                .map(DatabasePlacementInfo::from_row)
                .collect(),
            next_after_alias,
        })
    }

    /// Registers one provisioning shard on control. The same alias and reference
    /// can be retried without writing or changing the row's lifecycle state.
    pub async fn register_database_placement(
        &self,
        request: &RegisterDatabasePlacement,
    ) -> Result<DatabaseRegistration, Error> {
        if !valid_name(&request.alias) {
            return Err(Error::InvalidInput("invalid database placement alias"));
        }
        if !self.bindings.contains_key(&request.database_url_reference) {
            return Err(Error::InvalidInput("undeclared shard database reference"));
        }
        let (placement, inserted) = shard_admin::register_shard_database_placement(
            self.control().await?,
            &request.alias,
            &request.database_url_reference,
        )
        .await
        .map_err(placement_error)?;
        Ok(DatabaseRegistration {
            placement: DatabasePlacementInfo::from_row(placement),
            inserted,
        })
    }

    /// Verifies and activates a provisioning shard. Obtain the expected target
    /// from provisioning, as with [`super::setup::apply_migrations`]. The bindings
    /// need access to `pg_catalog.pg_control_system()` for duplicate detection.
    /// Each call permits up to 128 serviceable peers and has a 30-second deadline.
    /// A changed peer set can be retried. An already-active row returns a no-op.
    pub async fn activate_database_placement(
        &self,
        alias: &str,
        target: &DatabaseIdentity,
    ) -> Result<DatabaseActivation, Error> {
        if !valid_name(alias) || target.database.is_empty() || target.schema.is_empty() {
            return Err(Error::InvalidInput(
                "activation requires an alias, database, and schema",
            ));
        }
        let activation = async {
            let control = self.control().await?;
            shard_admin::activate_placement_with(
                control,
                alias,
                Some(MAX_ACTIVATION_PEERS),
                |registration, peers| async move {
                    self.verify_readiness(control, &registration, &peers, target)
                        .await
                },
            )
            .await
            .map_err(placement_error)
        };
        let outcome = tokio::time::timeout(ACTIVATION_TIMEOUT, activation)
            .await
            .map_err(|error| {
                Error::Dependency(
                    anyhow::Error::new(error).context("placement activation timed out"),
                )
            })??;
        Ok(DatabaseActivation {
            placement: DatabasePlacementInfo::from_row(outcome.placement),
            changed: outcome.changed,
            readiness_verified: outcome.readiness_verified,
            migration_count: outcome.migration_count,
        })
    }

    /// Lists up to 128 shard routes for one run, ordered by logical shard.
    /// An unknown run or a run without routes yields an empty list.
    pub async fn list_run_shards(&self, run_id: Uuid) -> Result<Vec<RunShardPlacement>, Error> {
        Ok(
            shard_admin::list_shard_placements(self.control().await?, run_id)
                .await
                .map_err(Error::Dependency)?
                .into_iter()
                .map(RunShardPlacement::from_row)
                .collect(),
        )
    }

    /// Advances one move with an explicit data batch budget and a 30-second
    /// deadline. An interrupted claim may remain leased for up to five minutes.
    /// Retry with the original ID and route version. Completed IDs return a
    /// read-only receipt, including after the shard moves again.
    pub async fn move_run_shard(
        &self,
        request: &MoveRunShardRequest,
    ) -> Result<MoveRunShardOutcome, Error> {
        if request.operation_id.is_nil()
            || request.run_id.is_nil()
            || !(0..RUN_SHARD_COUNT).contains(&request.run_shard)
            || request.expected_route_version <= 0
            || !valid_name(&request.target_database_alias)
            || !(1..=MAX_MOVE_BATCHES).contains(&request.max_batches)
        {
            return Err(Error::InvalidInput(
                "invalid shard move identity, route, or batch budget",
            ));
        }
        let (progress, batches_processed) = tokio::time::timeout(
            MOVE_STEP_TIMEOUT,
            shard_admin::advance_shard_move(
                &self.client.database,
                &self.bindings,
                request.run_id,
                request.run_shard,
                &request.target_database_alias,
                shard_admin::ShardMoveIdentity {
                    operation_id: request.operation_id,
                    starting_route_version: request.expected_route_version,
                },
                request.max_batches,
            ),
        )
        .await
        .map_err(|error| {
            Error::Dependency(anyhow::Error::new(error).context("shard move step timed out"))
        })?
        .map_err(placement_error)?;
        use shard_admin::ShardMoveProgress as Progress;
        Ok(MoveRunShardOutcome {
            operation_id: request.operation_id,
            progress: match progress {
                Progress::Resetting => RunShardMoveProgress::ResettingTarget,
                Progress::Copying => RunShardMoveProgress::Copying,
                Progress::CatchingUp => RunShardMoveProgress::CatchingUp,
                Progress::Draining => RunShardMoveProgress::Draining,
                Progress::CuttingOver => RunShardMoveProgress::CuttingOver,
                Progress::Completed => RunShardMoveProgress::Completed,
            },
            batches_processed,
        })
    }

    async fn control(&self) -> Result<&PgPool, Error> {
        self.client
            .database
            .control()
            .await
            .map_err(Error::Dependency)
    }

    async fn connect(&self, reference: &str) -> anyhow::Result<PgConnection> {
        let options = self
            .bindings
            .get(reference)
            .ok_or_else(|| anyhow::anyhow!("undeclared shard database reference {reference}"))?;
        Ok(options.options.connect().await?)
    }

    async fn verify_readiness(
        &self,
        control: &PgPool,
        registration: &DatabasePlacement,
        peers: &[DatabasePlacement],
        expected: &DatabaseIdentity,
    ) -> anyhow::Result<usize> {
        let control_alias = self.client.database.control_database_alias();
        anyhow::ensure!(
            peers.iter().any(|peer| peer.alias == control_alias
                && peer.accepts_new_shards()
                && peer.is_control_capable()),
            "configured control alias has no active control placement"
        );
        let mut target = self.connect(&registration.database_url_env).await?;
        let (identity, standby) = DatabaseIdentity::observe(&mut target).await?;
        if identity.as_ref() != Some(expected) || standby {
            return Err(shard_admin::PlacementConflict(
                "activation target does not match the expected writable database and schema",
            )
            .into());
        }
        for peer in peers {
            let (identity, _) = if peer.alias == control_alias
                || peer.database_url_env == DEFAULT_DATABASE_URL_ENV
            {
                DatabaseIdentity::observe(control).await?
            } else {
                let mut connection = self.connect(&peer.database_url_env).await?;
                DatabaseIdentity::observe(&mut connection).await?
            };
            let identity = identity.ok_or_else(|| {
                anyhow::anyhow!(
                    "serviceable placement {} has no installation schema",
                    peer.alias
                )
            })?;
            if expected.same_database(&identity) {
                return Err(shard_admin::PlacementConflict(
                    "target database is already used by a serviceable placement",
                )
                .into());
            }
        }
        expected.pin_schema(&mut target).await?;
        shard_admin::verify_activation_target(control, &mut target, &registration.alias).await
    }
}

fn valid_name(name: &str) -> bool {
    !name.is_empty() && name == name.trim() && !name.contains('\0')
}

fn placement_error(error: anyhow::Error) -> Error {
    match error.downcast::<shard_admin::PlacementConflict>() {
        Ok(conflict) => Error::Conflict(conflict.0),
        Err(error) => Error::Dependency(error),
    }
}

#[cfg(test)]
mod tests;
