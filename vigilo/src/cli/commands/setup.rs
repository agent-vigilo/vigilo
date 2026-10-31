//! Database setup command.
//!
//! Applies embedded schema migrations (or an explicit directory override), then
//! validates placement configuration and installs the official core evaluator collection.

use std::path::PathBuf;

use async_trait::async_trait;
use clap::Args;
use tracing::info;

use super::{
    Executable,
    args::{
        PlacementOptions,
        parsers::parse_dir,
    },
};
use crate::{
    context::Context,
    db::migrations,
};

#[derive(Debug, Args)]
/// Arguments for `vigilo setup`.
///
/// This command is intended to be safe to re-run; migrations are applied using
/// the existing migration workflow and only pending migrations are executed.
pub(crate) struct Command {
    #[command(flatten)]
    pub(crate) placement: PlacementOptions,

    /// Override the embedded migrations with SQL files from this directory
    #[arg(long, value_parser = parse_dir)]
    pub migrations_dir: Option<PathBuf>,

    /// Apply database setup without downloading or installing core evaluators
    #[arg(long, conflicts_with = "evaluators")]
    pub skip_evaluators: bool,

    /// Pin the official evaluator collection (YYYY.MM.DD.N); defaults to latest compatible
    #[arg(long, value_name = "YYYY.MM.DD.N", value_parser = crate::core_evaluators::parse_collection)]
    pub evaluators: Option<String>,
}

#[async_trait]
impl Executable for Command {
    /// Applies pending migrations and validates database placement configuration.
    async fn exec(self, context: Context) -> anyhow::Result<()> {
        let db = context.dbr().await?.control().await?;

        info!("running database migrations");
        migrations::migrate(db, self.migrations_dir).await?;

        info!("validating database placement configuration");
        context.dbr().await?.validate_placement_config().await?;

        if !self.skip_evaluators {
            use anyhow::Context as _;
            crate::core_evaluators::install(&context, self.evaluators.as_deref())
                .await.context("database setup completed, but core evaluator installation failed; rerun setup to retry, or use --skip-evaluators for database-only setup")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[derive(Parser)]
    struct Arguments {
        #[command(flatten)]
        setup: Command,
    }

    #[test]
    fn default_setup_requires_no_source_directory_or_evaluator_options() {
        let args = Arguments::try_parse_from(["setup"]).unwrap();
        assert!(args.setup.migrations_dir.is_none());
        assert!(!args.setup.skip_evaluators);
        assert!(
            Arguments::try_parse_from(["setup", "--skip-evaluators"])
                .unwrap()
                .setup
                .skip_evaluators
        );
        assert_eq!(
            Arguments::try_parse_from(["setup", "--evaluators", "2026.10.28.1"])
                .unwrap()
                .setup
                .evaluators
                .as_deref(),
            Some("2026.10.28.1")
        );
        assert!(Arguments::try_parse_from(["setup", "--evaluators"]).is_err());
        assert!(Arguments::try_parse_from(["setup", "--evaluators", "latest"]).is_err());
        assert!(
            Arguments::try_parse_from([
                "setup",
                "--skip-evaluators",
                "--evaluators",
                "2026.10.28.1"
            ])
            .is_err()
        );
        assert!(
            Arguments::try_parse_from(["setup", "--wasm-fuel-per-evaluation", "1000"]).is_err()
        );
    }

    #[test]
    fn explicit_migration_directory_is_validated() {
        let dir = tempfile::tempdir().unwrap();
        let args = Arguments::try_parse_from([
            std::ffi::OsStr::new("setup"),
            std::ffi::OsStr::new("--migrations-dir"),
            dir.path().as_os_str(),
        ])
        .unwrap();
        assert_eq!(args.setup.migrations_dir.as_deref(), Some(dir.path()));
        assert!(
            Arguments::try_parse_from([
                std::ffi::OsStr::new("setup"),
                std::ffi::OsStr::new("--migrations-dir"),
                dir.path().join("missing").as_os_str(),
            ])
            .is_err()
        );
    }
}
