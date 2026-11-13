//! Typed application operations for trusted hosts.
//!
//! A client owns shared, lazy database pools. Operations reuse the same workflows
//! as the CLI and return values rather than formatting output or installing process
//! state. Construction does not connect to services. The standalone client assumes
//! an authorized operator. Restricted reads use explicit trusted caller context
//! and fixed-role pools; authentication and database policy installation belong to
//! the embedding host.

pub(crate) mod cancellation;
pub(crate) mod control;
pub(crate) mod creation;
mod lifecycle;
pub use cancellation::{
    Cancellation,
    CancellationDetails,
    CancellationMetadata,
};
pub use control::{
    ControlOutcome,
    ExecutionControl,
};
pub(crate) mod status;
use std::time::Duration;

pub use lifecycle::{
    CreationDetails,
    CreationMetadata,
    PlanReference,
    PrepareRun,
    PreparedRun,
    RunClient,
    RunSubmission,
};
pub use status::{
    CreationProgress,
    LiveProgress,
    RunDetails,
    RunStatus,
};
use uuid::Uuid;

use crate::context::database;

/// Standalone database settings. Connection strings are operator-owned secrets.
/// This type does not implement Debug to avoid exposing the connection string.
#[derive(Clone)]
pub struct Config {
    /// PostgreSQL control database connection string.
    pub database_url: String,
    /// Control placement alias, defaulting to `primary`.
    pub control_database_alias: String,
    /// Maximum connections per database (1–256).
    pub max_connections: u32,
    /// Connection acquisition timeout (1–300 seconds).
    pub acquire_timeout: Duration,
}
impl Config {
    /// Uses the same pool and acquisition defaults as the CLI.
    pub fn new(database_url: String) -> Self {
        Self {
            database_url,
            control_database_alias: "primary".into(),
            max_connections: 5,
            acquire_timeout: Duration::from_secs(10),
        }
    }
}

/// Safe operation failures. Dependency internals are not included in Display.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Invalid bounded command input; the message contains no request payload.
    #[error("invalid operation input: {0}")]
    InvalidInput(&'static str),
    /// A supplied prepared value belongs to a different operation client.
    #[error("operation conflict: {0}")]
    Conflict(&'static str),
    /// The caller cancelled before committing this operation's acceptance.
    #[error("operation cancelled")]
    Cancelled,
    /// Invalid operator configuration; no service connection was attempted.
    #[error("invalid operation configuration: {0}")]
    InvalidConfiguration(&'static str),
    /// The requested run does not exist in the accessible store.
    #[error("run '{0}' was not found")]
    NotFound(Uuid),
    /// Database or routing failure. The source is for trusted diagnostics only;
    /// interfaces must not serialize its chain as a user response.
    #[error("operation dependency failed")]
    Dependency(#[source] anyhow::Error),
}

/// Standalone application client with shared, lazy pools and no output state.
#[derive(Clone)]
pub struct Client {
    database: std::sync::Arc<database::DatabaseRouter>,
}
impl Client {
    /// Validates configuration without connecting, reading CLI arguments or setting
    /// logging/runtime state. The caller supplies a Tokio runtime when invoking I/O.
    pub fn new(config: Config) -> Result<Self, Error> {
        if !(1..=256).contains(&config.max_connections) {
            return Err(Error::InvalidConfiguration("max_connections must be 1–256"));
        }
        if !(Duration::from_secs(1)..=Duration::from_secs(300)).contains(&config.acquire_timeout) {
            return Err(Error::InvalidConfiguration(
                "acquire_timeout must be 1–300 seconds",
            ));
        }
        if config.database_url.trim().is_empty() {
            return Err(Error::InvalidConfiguration(
                "database_url must not be empty",
            ));
        }
        config
            .database_url
            .parse::<sqlx::postgres::PgConnectOptions>()
            .map_err(|_| Error::InvalidConfiguration("invalid PostgreSQL connection string"))?;
        let placement = database::PlacementConfig::new(
            config.control_database_alias.clone(),
            config.control_database_alias,
            "single-default".into(),
        )
        .map_err(|_| Error::InvalidConfiguration("invalid control database alias"))?;
        Ok(Self {
            database: std::sync::Arc::new(database::DatabaseRouter::new(database::Config::new(
                config.database_url,
                config.max_connections,
                config.acquire_timeout,
                Default::default(),
                placement,
            ))),
        })
    }

    /// Binds run operations to operator-owned storage TOML, without reading a
    /// configuration path or accepting storage settings in ordinary run requests.
    pub fn runs(&self, storage_toml: &str) -> Result<RunClient, Error> {
        if storage_toml.len() > 64 * 1024 {
            return Err(Error::InvalidConfiguration("storage config exceeds 64 KiB"));
        }
        let owner: crate::blob_storage::config::OwnerConfig = toml::from_str(storage_toml)
            .map_err(|_| Error::InvalidConfiguration("invalid storage configuration"))?;
        owner
            .validate(&Default::default())
            .map_err(|_| Error::InvalidConfiguration("invalid storage binding"))?;
        Ok(RunClient::from_context(
            crate::context::Context::for_operations(self.database.clone()),
            owner,
            Default::default(),
            None,
        ))
    }

    /// Durably changes execution permission. Delivery and worker quiescence are
    /// asynchronous; a successful request is not a global stop acknowledgment.
    pub async fn control_run(
        &self,
        run_id: Uuid,
        control: ExecutionControl,
    ) -> Result<ControlOutcome, Error> {
        let db = self.database.control().await.map_err(Error::Dependency)?;
        crate::db::workflows::run_control::request(db, run_id, control)
            .await
            .map_err(Error::Dependency)
    }

    /// Reads execution-control delivery progress separately from lifecycle status.
    pub async fn run_control_status(&self, run_id: Uuid) -> Result<ControlOutcome, Error> {
        let db = self.database.control().await.map_err(Error::Dependency)?;
        crate::db::workflows::run_control::progress(db, run_id)
            .await
            .map_err(Error::Dependency)
    }

    /// Requests durable cancellation and attempts bounded routed settlement. Committed control changes survive
    /// caller disconnect; retrying the same run is idempotent.
    pub async fn cancel_run(&self, run_id: Uuid) -> Result<Cancellation, Error> {
        cancellation::cancel(&self.database, run_id).await
    }

    /// Reads bounded run progress. A snapshot may combine observations from several
    /// databases; completeness is explicit. Missing runs return [`Error::NotFound`].
    /// Dropping the future cancels the read; this operation never changes run state.
    pub async fn run_status(&self, run_id: Uuid) -> Result<RunStatus, Error> {
        status::read(&self.database, run_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn construction_is_service_free_and_validates_bounds_without_exposing_secrets() {
        assert!(
            Client::new(Config::new(
                "postgresql://operator:secret@127.0.0.1:1/vigilo".into()
            ))
            .is_ok()
        );
        let mut config = Config::new("postgresql://localhost/vigilo".into());
        config.max_connections = 0;
        assert!(matches!(
            Client::new(config),
            Err(Error::InvalidConfiguration(_))
        ));
        for timeout in [Duration::ZERO, Duration::from_secs(301)] {
            let mut config = Config::new("postgresql://localhost/vigilo".into());
            config.acquire_timeout = timeout;
            assert!(matches!(
                Client::new(config),
                Err(Error::InvalidConfiguration(_))
            ));
        }
        let mut config = Config::new("postgresql://localhost/vigilo".into());
        config.control_database_alias.clear();
        assert!(matches!(
            Client::new(config),
            Err(Error::InvalidConfiguration(_))
        ));
        assert!(matches!(
            Client::new(Config::new(String::new())),
            Err(Error::InvalidConfiguration(_))
        ));
        let error = Client::new(Config::new("secret-not-a-url".into()))
            .err()
            .unwrap();
        assert!(!error.to_string().contains("secret-not-a-url"));
    }
}

mod user;
pub use user::{
    UserClient,
    UserConfig,
    UserContext,
};

mod database_bindings;
pub use database_bindings::{
    DatabaseBinding,
    DatabaseBindings,
    DatabaseCredential,
    DatabasePools,
    DatabasePurpose,
};
