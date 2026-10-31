//! Root command router for the `vigilo` CLI.
//!
//! This module owns the top-level command enum used by clap, delegates execution
//! to feature-specific modules, and derives lazy context configuration from the
//! selected command. Public command groups use singular resource names and
//! follow ownership boundaries.

use async_trait::async_trait;
use clap::Subcommand;

use crate::context::{
    Context,
    database::{
        CircuitBreakerConfig,
        PlacementConfig,
    },
    wasm,
};

pub(super) mod coordinator;
pub(super) mod dataset;
pub(super) mod evaluators;
pub(super) mod run;
pub(super) mod setup;
pub(super) mod shard;
pub(super) mod worker;

use super::{
    Executable,
    args::{
        self,
        CircuitBreakerOptions,
        PlacementOptions,
        WasmOptions,
    },
};

pub(crate) struct CommandContextConfig {
    pub(crate) messaging: Option<crate::mq::Config>,
    pub(crate) wasm: wasm::Config,
    pub(crate) agent_http: crate::agent_client::limits::Limits,
    pub(crate) agent_credentials: Option<std::path::PathBuf>,
    pub(crate) circuit_breaker: CircuitBreakerConfig,
    pub(crate) database_operation_timeout:
        Option<crate::context::database::DatabaseOperationTimeoutConfig>,
    pub(crate) placement: PlacementConfig,
}

#[derive(Debug, Subcommand)]
/// Top-level CLI commands exposed by `vigilo`.
///
/// Each variant wraps a subcommand module that implements [`Executable`].
pub(crate) enum Command {
    /// Apply database migrations and validate placement configuration
    Setup(setup::Command),

    /// Manage system evaluators
    Evaluator(evaluators::CanonicalCommand),

    /// Store and manage immutable datasets
    Dataset(dataset::Command),

    /// Manage runs, inputs, results, and owned shards
    Run(run::Command),

    /// Manage registered execution databases
    Database(shard::DatabaseCommand),

    /// Plan and execute cross-run shard rebalancing
    Rebalance(shard::RebalanceCommand),

    /// Run coordinator processes
    Coordinator(coordinator::Command),

    /// Run worker processes
    Worker(worker::Command),

    /// Deprecated plural evaluator hierarchy
    #[command(hide = true)]
    Evaluators(evaluators::Command),

    /// Deprecated shard administration hierarchy
    #[command(hide = true)]
    Shard(shard::Command),
}

impl Command {
    /// Packaging is local and does not consume a database connection.
    pub(crate) fn is_evaluator_package(&self) -> bool {
        matches!(
            self,
            Self::Evaluator(evaluators::CanonicalCommand {
                command: evaluators::SubCommand::Package { .. }
            }) | Self::Evaluators(evaluators::Command {
                command: Some(evaluators::SubCommand::Package { .. })
            })
        )
    }

    pub(crate) fn context_config(
        &self,
        control_database_alias: &str,
    ) -> anyhow::Result<CommandContextConfig> {
        let mut messaging = None;
        let mut agent_credentials = None;
        let mut agent_http = crate::agent_client::limits::Limits::default();
        let mut wasm = WasmOptions::default();
        let mut circuit_breaker = CircuitBreakerOptions::default();
        let mut database_operation_timeout = None;
        let mut placement = PlacementOptions::default();

        match self {
            Self::Coordinator(command) => {
                messaging = Some(command.messaging.config()?);
                circuit_breaker = command.circuit_breaker;
                database_operation_timeout = Some(command.database_operation_timeout.config()?);
            }
            Self::Worker(command) => {
                messaging = Some(command.messaging.config()?);
                wasm = command.wasm;
                agent_http = command.agent_http.config();
                agent_credentials = command.agent_credentials.clone();
                circuit_breaker = command.circuit_breaker;
                database_operation_timeout = Some(command.database_operation_timeout.config()?);
            }
            Self::Setup(command) => {
                placement = command.placement.clone();
            }
            Self::Evaluator(command) => {
                if let Some(options) = command.command.wasm_options() {
                    wasm = options;
                }
            }
            Self::Evaluators(command) => {
                if let Some(options) = command
                    .command
                    .as_ref()
                    .and_then(evaluators::SubCommand::wasm_options)
                {
                    wasm = options;
                }
            }
            Self::Run(command) => {
                if let Some((
                    placement_options,
                    circuit_breaker_options,
                    database_operation_timeout_options,
                )) = command.create_options()
                {
                    placement = placement_options.clone();
                    circuit_breaker = circuit_breaker_options;
                    database_operation_timeout = Some(database_operation_timeout_options.config()?);
                }
            }
            Self::Dataset(_) => {
                database_operation_timeout =
                    Some(args::DatabaseOperationTimeoutOptions::default().config()?);
            }
            Self::Database(command) => {
                database_operation_timeout = Some(command.database_operation_timeout.config()?);
            }
            Self::Rebalance(_) | Self::Shard(_) => {}
        }

        Ok(CommandContextConfig {
            messaging,
            wasm: wasm.config(),
            agent_http,
            agent_credentials,
            circuit_breaker: circuit_breaker.config()?,
            database_operation_timeout,
            placement: PlacementConfig::new(
                control_database_alias.to_string(),
                placement.default_shard_database_alias,
                placement.shard_assignment_policy,
            )?,
        })
    }
}

#[async_trait]
impl Executable for Command {
    /// Dispatches the selected top-level command to its concrete handler.
    async fn exec(self, context: Context) -> anyhow::Result<()> {
        match self {
            Command::Dataset(cmd) => cmd.exec(context).await,
            Command::Coordinator(cmd) => cmd.exec(context).await,
            Command::Database(cmd) => cmd.exec(context).await,
            Command::Evaluator(cmd) => cmd.exec(context).await,
            Command::Evaluators(cmd) => cmd.exec(context).await,
            Command::Rebalance(cmd) => cmd.exec(context).await,
            Command::Run(cmd) => cmd.exec(context).await,
            Command::Shard(cmd) => cmd.exec(context).await,
            Command::Setup(cmd) => cmd.exec(context).await,
            Command::Worker(cmd) => cmd.exec(context).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::{
        CommandFactory,
        Parser,
    };

    use super::Command;

    const RUN_ID: &str = "0198f36d-f7db-7d90-998c-d60d498dd979";
    const OPERATION_ID: &str = "0198f36e-1839-7f62-b97a-e6c0c78f0fb6";

    #[derive(Debug, Parser)]
    struct TestCli {
        #[command(subcommand)]
        command: Command,
    }

    #[test]
    fn canonical_command_hierarchy_parses() {
        let commands: &[&[&str]] = &[
            &["vigilo", "evaluator", "list"],
            &[
                "vigilo",
                "evaluator",
                "test",
                "vigilo/example:1.0.0",
                "--input",
                "{}",
            ],
            &[
                "vigilo",
                "run",
                "validate",
                "--profile",
                "{}",
                "--dataset",
                "{}",
            ],
            &["vigilo", "run", "shard", "list", RUN_ID],
            &["vigilo", "run", "shard", "show", RUN_ID, "4"],
            &[
                "vigilo",
                "run",
                "shard",
                "assign",
                RUN_ID,
                "4",
                "--to",
                "shard_001",
            ],
            &[
                "vigilo",
                "run",
                "shard",
                "move",
                RUN_ID,
                "4",
                "--to",
                "shard_001",
                "--dry-run",
            ],
            &[
                "vigilo",
                "run",
                "shard",
                "abort-move",
                RUN_ID,
                "4",
                "--from",
                "primary",
                "--to",
                "shard_001",
            ],
            &["vigilo", "database", "list"],
            &[
                "vigilo",
                "database",
                "register",
                "shard_001",
                "--database-url-env",
                "VIGILO_SHARD_001_DATABASE_URL",
            ],
            &["vigilo", "database", "activate", "shard_001"],
            &["vigilo", "database", "drain", "shard_001"],
            &["vigilo", "database", "disable", "shard_001"],
            &[
                "vigilo",
                "rebalance",
                "plan",
                "--from",
                "primary",
                "--to",
                "shard_001",
            ],
            &["vigilo", "rebalance", "apply", OPERATION_ID],
            &["vigilo", "rebalance", "verify", OPERATION_ID],
            &["vigilo", "rebalance", "cancel", OPERATION_ID],
        ];

        for command in commands {
            TestCli::try_parse_from(*command)
                .unwrap_or_else(|error| panic!("failed to parse `{}`: {error}", command.join(" ")));
        }
    }

    #[test]
    fn legacy_command_hierarchy_remains_accepted() {
        let commands: &[&[&str]] = &[
            &["vigilo", "evaluators"],
            &[
                "vigilo",
                "run",
                "test",
                "--profile",
                "{}",
                "--dataset",
                "{}",
            ],
            &["vigilo", "shard", "databases", "list"],
            &[
                "vigilo",
                "shard",
                "databases",
                "add",
                "shard_001",
                "--database-url-env",
                "VIGILO_SHARD_001_DATABASE_URL",
            ],
            &["vigilo", "shard", "placements", "list", RUN_ID],
            &[
                "vigilo",
                "shard",
                "placements",
                "set",
                RUN_ID,
                "4",
                "--alias",
                "shard_001",
            ],
            &["vigilo", "shard", "route", RUN_ID, "4"],
            &[
                "vigilo",
                "shard",
                "move",
                RUN_ID,
                "4",
                "--alias",
                "shard_001",
            ],
            &[
                "vigilo",
                "shard",
                "move-abort",
                RUN_ID,
                "4",
                "--source",
                "primary",
                "--target",
                "shard_001",
            ],
            &["vigilo", "shard", "rebalance", "apply", OPERATION_ID],
        ];

        for command in commands {
            TestCli::try_parse_from(*command).unwrap_or_else(|error| {
                panic!(
                    "failed to parse legacy command `{}`: {error}",
                    command.join(" ")
                )
            });
        }
    }

    #[test]
    fn root_help_lists_only_canonical_command_groups() {
        let command = TestCli::command();
        let visible_names = command
            .get_subcommands()
            .filter(|subcommand| !subcommand.is_hide_set())
            .map(|subcommand| subcommand.get_name())
            .collect::<Vec<_>>();

        assert_eq!(
            visible_names,
            [
                "setup",
                "evaluator",
                "dataset",
                "run",
                "database",
                "rebalance",
                "coordinator",
                "worker",
            ]
        );
    }

    #[test]
    fn nested_help_follows_resource_ownership() {
        let command = TestCli::command();
        let evaluator = command.find_subcommand("evaluator").unwrap();
        let run = command.find_subcommand("run").unwrap();
        let run_shard = run.find_subcommand("shard").unwrap();
        let database = command.find_subcommand("database").unwrap();
        let rebalance = command.find_subcommand("rebalance").unwrap();

        assert_eq!(
            visible_subcommands(evaluator),
            [
                "list",
                "publish",
                "package",
                "show",
                "search",
                "test",
                "set-state"
            ]
        );
        assert_eq!(
            visible_subcommands(run_shard),
            ["list", "show", "assign", "move", "abort-move"]
        );
        assert_eq!(
            visible_subcommands(database),
            ["list", "register", "activate", "drain", "disable"]
        );
        assert_eq!(
            visible_subcommands(rebalance),
            ["plan", "apply", "verify", "cancel"]
        );

        let rebalance_plan = rebalance.find_subcommand("plan").unwrap();
        assert!(
            rebalance_plan
                .get_arguments()
                .any(|arg| arg.get_long() == Some("to"))
        );
        assert!(
            !rebalance_plan
                .get_arguments()
                .any(|arg| arg.get_long() == Some("target"))
        );
    }

    #[test]
    fn canonical_evaluator_requires_an_explicit_operation() {
        assert!(TestCli::try_parse_from(["vigilo", "evaluator"]).is_err());
        assert!(TestCli::try_parse_from(["vigilo", "evaluators"]).is_ok());
    }

    fn visible_subcommands(command: &clap::Command) -> Vec<&str> {
        command
            .get_subcommands()
            .filter(|subcommand| !subcommand.is_hide_set())
            .map(|subcommand| subcommand.get_name())
            .collect()
    }
}
