//! Database placement persistence models.
//!
//! Database placements are the routing catalog for multi-database deployments.
//! The default deployment seeds one `primary` placement that uses `DATABASE_URL`
//! for both control-database and shard-local data. Additional targets remain
//! non-routable until their readiness is verified during activation.

use chrono::{
    DateTime,
    Utc,
};
use serde::{
    Deserialize,
    Serialize,
};

pub(crate) const DEFAULT_DATABASE_ALIAS: &str = "primary";
pub(crate) const DEFAULT_DATABASE_URL_ENV: &str = "DATABASE_URL";

pub(crate) const DATABASE_PLACEMENT_ROLE_CONTROL: &str = "control";
pub(crate) const DATABASE_PLACEMENT_ROLE_SHARD: &str = "shard";
pub(crate) const DATABASE_PLACEMENT_ROLE_CONTROL_AND_SHARD: &str = "control_and_shard";
pub(crate) const DATABASE_PLACEMENT_STATUS_PROVISIONING: &str = "provisioning";
pub(crate) const DATABASE_PLACEMENT_STATUS_ACTIVE: &str = "active";
pub(crate) const DATABASE_PLACEMENT_STATUS_DRAINING: &str = "draining";
pub(crate) const DATABASE_PLACEMENT_STATUS_DISABLED: &str = "disabled";

/// Persisted database placement row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
pub(crate) struct DatabasePlacement {
    /// Stable routing alias.
    pub(crate) alias: String,
    /// Environment variable that contains this placement's database URL.
    pub(crate) database_url_env: String,
    /// Placement role: control, shard, or control_and_shard.
    pub(crate) role: String,
    /// Placement status: provisioning, active, draining, or disabled.
    pub(crate) status: String,
    /// Time this placement row was inserted.
    pub(crate) created_at: DateTime<Utc>,
    /// Time this placement row was last updated.
    pub(crate) updated_at: DateTime<Utc>,
}

impl DatabasePlacement {
    pub(crate) fn is_control_capable(&self) -> bool {
        matches!(
            self.role.as_str(),
            DATABASE_PLACEMENT_ROLE_CONTROL | DATABASE_PLACEMENT_ROLE_CONTROL_AND_SHARD
        )
    }

    pub(crate) fn is_shard_capable(&self) -> bool {
        matches!(
            self.role.as_str(),
            DATABASE_PLACEMENT_ROLE_SHARD | DATABASE_PLACEMENT_ROLE_CONTROL_AND_SHARD
        )
    }

    /// Whether this target may receive a new run shard or move activation.
    pub(crate) fn accepts_new_shards(&self) -> bool {
        self.status == DATABASE_PLACEMENT_STATUS_ACTIVE
    }

    /// Whether this target may serve ownership assigned before a drain began.
    pub(crate) fn can_serve_owned_shards(&self) -> bool {
        matches!(
            self.status.as_str(),
            DATABASE_PLACEMENT_STATUS_ACTIVE | DATABASE_PLACEMENT_STATUS_DRAINING
        )
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;

    fn shard_placement(status: &str) -> DatabasePlacement {
        DatabasePlacement {
            alias: "shard_001".to_string(),
            database_url_env: "VIGILO_SHARD_001_DATABASE_URL".to_string(),
            role: DATABASE_PLACEMENT_ROLE_SHARD.to_string(),
            status: status.to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn draining_placement_serves_existing_ownership_but_rejects_new_ownership() {
        let placement = shard_placement(DATABASE_PLACEMENT_STATUS_DRAINING);

        assert!(placement.can_serve_owned_shards());
        assert!(!placement.accepts_new_shards());
    }

    #[test]
    fn disabled_placement_serves_no_shard_ownership() {
        let placement = shard_placement(DATABASE_PLACEMENT_STATUS_DISABLED);

        assert!(!placement.can_serve_owned_shards());
        assert!(!placement.accepts_new_shards());
    }

    #[test]
    fn provisioning_placement_serves_no_shard_ownership() {
        let placement = shard_placement(DATABASE_PLACEMENT_STATUS_PROVISIONING);

        assert!(!placement.can_serve_owned_shards());
        assert!(!placement.accepts_new_shards());
    }
}
