//! Operator-owned provisioning for one PostgreSQL target or control catalog.
//!
//! These explicit calls reuse the standalone client's pool and never run during
//! runtime construction, submission, or execution. Migrations verify and pin the
//! target on the migration session. Placement validation reads configuration on
//! control. Evaluator installation pins the official catalog and commits one
//! validated package at a time so an interrupted installation can resume.

use std::{
    collections::BTreeSet,
    path::Path,
};

use super::{
    Client,
    Error,
};
use crate::{
    context::{
        Context,
        database,
    },
    core_evaluators,
    db::migrations,
};
pub use crate::{
    core_evaluators::EvaluatorInstallation,
    db::migrations::DatabaseIdentity,
};

/// Confirmation that migrations completed on the verified target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationOutcome {
    /// The identity checked on the session that applied pending migrations.
    pub target: DatabaseIdentity,
}

/// Applies pending migrations to the database bound to this standalone client.
///
/// Obtain the expected identity from trusted provisioning, independently of the
/// connection under test. The login needs access to `pg_catalog.pg_control_system()`
/// and installation privileges. Use a session-affine endpoint for SQLx's session
/// migration lock. `None` uses embedded migrations. A supplied directory fully
/// replaces them. Repeating with the same source checks history and applies only
/// pending migrations. An identity mismatch returns [`Error::Conflict`].
pub async fn apply_migrations(
    client: &Client,
    target: &DatabaseIdentity,
    migrations_dir: Option<&Path>,
) -> Result<MigrationOutcome, Error> {
    if target.database.is_empty() || target.schema.is_empty() {
        return Err(Error::InvalidInput(
            "migration database and schema are required",
        ));
    }
    let db = client.database.control().await.map_err(Error::Dependency)?;
    migrations::migrate_on_target(db, migrations_dir, Some(target))
        .await
        .map_err(|error| {
            if error.is::<migrations::WrongTarget>() {
                Error::Conflict(
                    "migration target does not match the expected writable database and schema",
                )
            } else {
                Error::Dependency(error)
            }
        })?;
    Ok(MigrationOutcome {
        target: target.clone(),
    })
}

/// Placement configuration supplied by the trusted operator.
pub struct PlacementValidationRequest {
    /// The active shard-capable alias used as the default execution placement.
    pub default_shard_database_alias: String,
    /// Connection reference names present in the host configuration. The
    /// built-in `DATABASE_URL` reference is supplied by the standalone client.
    pub database_url_references: BTreeSet<String>,
}

/// Summary of the catalog and binding checks completed on control.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementValidation {
    /// The single active control-capable alias.
    pub control_database_alias: String,
    /// The validated active shard-capable default alias.
    pub default_shard_database_alias: String,
    /// Number of active and draining placements checked.
    pub serviceable_placements: usize,
}

/// Checks the control catalog, default execution alias, declared references and
/// route serviceability. It queries only control. Check remote credentials,
/// reachability, and installation readiness independently.
pub async fn validate_placements(
    client: &Client,
    request: &PlacementValidationRequest,
) -> Result<PlacementValidation, Error> {
    let database = &client.database;
    let placement = database::PlacementConfig::new(
        database.control_database_alias().into(),
        request.default_shard_database_alias.clone(),
        "single-default".into(),
    )
    .map_err(|_| Error::InvalidConfiguration("invalid default shard database alias"))?;
    let serviceable_placements = database
        .validate_placement_bindings(&placement.default_shard_database_alias, |reference| {
            anyhow::ensure!(
                reference == crate::models::database_placement::DEFAULT_DATABASE_URL_ENV
                    || request.database_url_references.contains(reference),
                "serviceable database placement references an undeclared connection: {reference}"
            );
            Ok(())
        })
        .await
        .map_err(Error::Dependency)?;
    Ok(PlacementValidation {
        control_database_alias: placement.control_database_alias,
        default_shard_database_alias: placement.default_shard_database_alias,
        serviceable_placements,
    })
}

/// Exact identity of an official evaluator collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluatorCollectionPin {
    /// Canonical collection identifier `YYYY.MM.DD.N` without the `evaluators/` prefix.
    pub collection: String,
    /// Lowercase hexadecimal SHA-256 of the unmodified `catalog.json` release asset.
    pub catalog_sha256: String,
}

/// Installs the pinned official collection into the client's control database.
///
/// The optional token is sent only to the official release metadata API. The
/// original catalog bytes must match the pin before registry access. Matching
/// versions retain their lifecycle state. Each publication commits separately
/// and can be skipped on retry after a partial install.
pub async fn install_official_evaluators(
    client: &Client,
    pin: &EvaluatorCollectionPin,
    github_token: Option<&str>,
) -> Result<EvaluatorInstallation, Error> {
    core_evaluators::parse_collection(&pin.collection)
        .map_err(|_| Error::InvalidInput("invalid official evaluator collection identifier"))?;
    if !core_evaluators::valid_hash(&pin.catalog_sha256) {
        return Err(Error::InvalidInput(
            "invalid official evaluator catalog SHA-256",
        ));
    }
    core_evaluators::install(
        &Context::for_operations(client.database.clone()),
        Some(&pin.collection),
        Some(&pin.catalog_sha256),
        github_token,
    )
    .await
    .map_err(Error::Dependency)
}

#[cfg(test)]
mod tests;
