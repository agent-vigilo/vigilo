//! CLI application definition.
//!
//! This module defines global options, shared output selection, and the
//! `Executable` dispatch contract used by subcommands. Command implementations
//! and subsystem-specific configuration live under `cli::commands`; this root
//! owns only database-client, logging, and output options shared by every
//! command.

use async_trait::async_trait;
use clap::{
    ArgAction,
    Parser,
    crate_description,
    crate_version,
};

mod args;
mod commands;
use commands::Command;
#[cfg(test)]
pub(crate) use commands::worker::claim_hinted_chunk_with_route_refresh;

use super::context::{
    Context,
    output::OutputFormat,
};

#[async_trait]
pub(super) trait Executable {
    #[must_use = "the command executes only when this future is awaited"]
    async fn exec(self, context: Context) -> anyhow::Result<()>;
}

#[derive(Debug, Parser)]
#[command(
    name = "agent-vigilo",
    version = crate_version!(),
    about = crate_description!(),
    long_about = None,
    after_help = "Agent tip: use `-q -f toon` for compact structured output in AI agent and LLM tool-call workflows. Use `-f json` when exact JSON parsing is required."
)]
pub(crate) struct App {
    /// Database URL (connection string)
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: Option<String>,

    /// Maximum Postgres connections in each database pool
    #[arg(long = "database-max-connections", env = "DATABASE_MAX_CONNECTIONS", value_name = "COUNT_PER_DATABASE", default_value_t = 5, value_parser = clap::value_parser!(u32).range(1..=256))]
    pub database_pool_max_connections_per_target: u32,

    /// Maximum seconds to acquire a PostgreSQL connection
    #[arg(long, env = "VIGILO_DATABASE_ACQUIRE_TIMEOUT_SECONDS", default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..=300))]
    pub database_acquire_timeout_seconds: u64,

    /// Active control-capable database placement alias
    #[arg(long, env = "VIGILO_CONTROL_DATABASE_ALIAS", default_value = "primary")]
    pub control_database_alias: String,

    /// Suppress all diagnostic output and progress messages
    #[arg(global = true, short, long, default_value_t = false)]
    pub quiet: bool,

    /// Increase log verbosity (-v for DEBUG, -vv for TRACE)
    #[arg(global = true, short, long, action = ArgAction::Count)]
    pub verbose: u8,

    /// Output encoding for stdout payloads; agents should prefer `-q -f toon` for compact inspection
    #[arg(
        global = true,
        short = 'f',
        long = "output-format",
        env = "VIGILO_OUTPUT_FORMAT",
        value_name = "FORMAT",
        default_value_t = OutputFormat::Json,
        value_enum
    )]
    pub output_format: OutputFormat,

    #[command(subcommand)]
    pub command: Command,
}

impl App {
    /// Database-backed commands require a URL; local packaging never opens a pool.
    pub(crate) fn database_uri(&self) -> anyhow::Result<String> {
        match &self.database_url {
            Some(url) => Ok(url.clone()),
            None if self.command.is_evaluator_package() => Ok(String::new()),
            None => anyhow::bail!("--database-url or DATABASE_URL is required for this command"),
        }
    }
}

#[async_trait]
impl Executable for App {
    async fn exec(self, context: Context) -> anyhow::Result<()> {
        self.command.exec(context).await
    }
}

#[cfg(test)]
mod tests {
    use clap::{
        CommandFactory,
        Parser,
    };

    use super::App;

    #[test]
    fn only_local_packaging_can_omit_database_configuration() {
        let dir = tempfile::tempdir().unwrap();
        let mut package = App::try_parse_from([
            "vigilo",
            "evaluator",
            "package",
            dir.path().to_str().unwrap(),
            "--output",
            "example.tar.gz",
        ])
        .unwrap();
        package.database_url = None;
        assert_eq!(package.database_uri().unwrap(), "");
        let mut publish = App::try_parse_from([
            "vigilo",
            "evaluator",
            "publish",
            dir.path().to_str().unwrap(),
        ])
        .unwrap();
        publish.database_url = None;
        assert!(publish.database_uri().is_err());
        publish.database_url = Some("postgresql://control/vigilo".into());
        assert_eq!(
            publish.database_uri().unwrap(),
            "postgresql://control/vigilo"
        );
    }

    #[test]
    fn cli_definition_has_no_argument_conflicts() {
        App::command().debug_assert();
    }

    #[test]
    fn database_commands_do_not_require_unrelated_runtime_configuration() {
        App::try_parse_from([
            "vigilo",
            "--database-url",
            "postgres://control",
            "database",
            "list",
        ])
        .unwrap();
    }

    #[test]
    fn runtime_configuration_is_scoped_to_consuming_commands() {
        let root = App::command();
        assert!(root.get_arguments().all(|arg| {
            !matches!(
                arg.get_long(),
                Some(
                    "messaging-url"
                        | "messaging-circuit-failure-threshold"
                        | "wasm-timeout-ms"
                        | "database-circuit-failure-threshold"
                )
            )
        }));

        let coordinator = root.find_subcommand("coordinator").unwrap();
        assert!(has_long(coordinator, "messaging-url"));
        assert!(has_long(coordinator, "messaging-circuit-failure-threshold"));
        assert!(has_long(coordinator, "database-circuit-failure-threshold"));
        assert!(has_long(coordinator, "database-operation-timeout-seconds"));
        assert!(!has_long(coordinator, "wasm-timeout-ms"));

        let worker = root.find_subcommand("worker").unwrap();
        assert!(has_long(worker, "messaging-url"));
        assert!(has_long(worker, "messaging-circuit-failure-threshold"));
        assert!(has_long(worker, "wasm-timeout-ms"));
        assert!(has_long(worker, "database-circuit-failure-threshold"));
        assert!(has_long(worker, "database-operation-timeout-seconds"));

        let evaluator_test = root
            .find_subcommand("evaluator")
            .unwrap()
            .find_subcommand("test")
            .unwrap();
        assert!(has_long(evaluator_test, "wasm-timeout-ms"));

        let run_create = root
            .find_subcommand("run")
            .unwrap()
            .find_subcommand("create")
            .unwrap();
        assert!(has_long(run_create, "shard-assignment-policy"));
        assert!(has_long(run_create, "database-circuit-failure-threshold"));
        assert!(has_long(run_create, "database-operation-timeout-seconds"));
        assert!(has_long(run_create, "run-creation-case-batch-size"));
    }

    #[test]
    fn selected_command_contributes_only_its_external_service_configuration() {
        let database = App::try_parse_from([
            "vigilo",
            "--database-url",
            "postgres://control",
            "database",
            "list",
        ])
        .unwrap();
        assert!(
            database
                .command
                .context_config("primary")
                .unwrap()
                .messaging
                .is_none()
        );

        let coordinator = App::try_parse_from([
            "vigilo",
            "--database-url",
            "postgres://control",
            "coordinator",
            "--messaging-url",
            "amqp://broker",
            "start",
        ])
        .unwrap();
        let messaging = coordinator
            .command
            .context_config("primary")
            .unwrap()
            .messaging
            .unwrap();
        assert_eq!(messaging.uri, "amqp://broker");
    }

    #[test]
    fn messaging_circuit_configuration_rejects_an_invalid_open_range() {
        let app = App::try_parse_from([
            "vigilo",
            "--database-url",
            "postgres://control",
            "coordinator",
            "--messaging-url",
            "amqp://broker",
            "--messaging-circuit-initial-open-seconds",
            "20",
            "--messaging-circuit-max-open-seconds",
            "10",
            "start",
        ])
        .unwrap();

        let error = app
            .command
            .context_config("primary")
            .err()
            .expect("invalid messaging circuit range must fail");

        assert!(error.to_string().contains("maximum open duration"));
    }

    fn has_long(command: &clap::Command, name: &str) -> bool {
        command
            .get_arguments()
            .any(|arg| arg.get_long() == Some(name))
    }
}
