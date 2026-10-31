//! Evaluator registry commands.
//!
//! This module provides publish, search, inspect, test, and state-management
//! operations for evaluator artifacts stored in the registry.

use std::{
    fs,
    path::PathBuf,
};

use async_trait::async_trait;
use clap::{
    Args,
    Subcommand,
};
use serde_json::json;
use tracing::{
    info,
    warn,
};

use super::{
    Executable,
    args::{
        WasmOptions,
        parsers::{
            parse_dir,
            parse_filepath,
        },
    },
};
use crate::{
    context::Context,
    contracts::{
        evaluator::EvaluatorInput,
        evaluator_ref::parse_fully_qualified_evaluator,
    },
    db::tables::evaluators,
    models::evaluator::{
        EvaluatorPatch,
        EvaluatorState,
    },
};

mod publish;
mod search;
mod set_state;
mod show;
mod test;

const DEFAULT_NAMESPACE: &str = "vigilo";
const DEFAULT_SEARCH_LIMIT: i64 = 10;
const MAX_SEARCH_LIMIT: i64 = 20;

/// Resolves the build profile used when preparing an evaluator package.
///
/// `--release` always maps to `release`; otherwise an explicit profile is used
/// with `dev` fallback.
fn get_manifest_profile(release: bool, profile: Option<String>) -> String {
    match release {
        true => "release".to_string(),
        false => profile.unwrap_or_else(|| "dev".to_string()),
    }
}

#[derive(Debug, Subcommand)]
/// Evaluator command operations.
pub(crate) enum SubCommand {
    /// List evaluator versions in the default namespace
    List,

    /// Publish evaluator version
    Publish {
        #[command(flatten)]
        wasm: WasmOptions,

        /// Source directory, portable package directory, or .tar.gz archive
        #[arg(value_parser = parse_package_path)]
        evaluator_path: PathBuf,

        /// Publish evaluator built in release mode, with optimizations
        #[arg(short, long)]
        release: bool,

        /// Publish evaluator built with the specified profile
        #[arg(long, value_name = "PROFILE", conflicts_with = "release")]
        profile: Option<String>,
    },
    /// Create a portable release archive from a prebuilt source package
    Package {
        #[command(flatten)]
        wasm: WasmOptions,
        /// Evaluator source directory containing Vigilo.toml
        #[arg(value_parser = parse_dir)]
        evaluator_path: PathBuf,
        /// Destination archive; existing files are never overwritten
        #[arg(long)]
        output: PathBuf,
        #[arg(short, long)]
        release: bool,
        #[arg(long, conflicts_with = "release")]
        profile: Option<String>,
    },
    /// Show evaluator details
    Show {
        /// Fully qualified evaluator identifier (<namespace>/<name>:<version>)
        #[arg()]
        evaluator: String,
    },
    /// Search evaluators
    Search {
        /// Evaluator namespace
        #[arg(long, value_name = "NAMESPACE", default_value = DEFAULT_NAMESPACE)]
        namespace: String,

        /// Max results to return
        #[arg(long, value_name = "LIMIT", default_value_t = DEFAULT_SEARCH_LIMIT, value_parser = clap::value_parser!(i64).range(1..=MAX_SEARCH_LIMIT as i64))]
        limit: i64,

        /// Optional text query (matches name, description, tags, metadata)
        #[arg()]
        query: Option<String>,
    },
    /// Execute a single evaluator with canonical test input
    Test {
        #[command(flatten)]
        wasm: WasmOptions,

        /// Fully qualified evaluator identifier (<namespace>/<name>:<version>)
        #[arg()]
        evaluator: String,

        /// Input JSON string
        #[arg(
            long,
            value_name = "JSON",
            conflicts_with = "input_file",
            required_unless_present = "input_file",
            alias = "request"
        )]
        input: Option<String>,

        /// Path to input JSON file
        #[arg(
            long,
            value_name = "FILE",
            value_parser = parse_filepath,
            conflicts_with = "input",
            required_unless_present = "input",
            alias = "request-file"
        )]
        input_file: Option<PathBuf>,
    },
    /// Set evaluator state
    SetState {
        /// Fully qualified evaluator identifier (<namespace>/<name>:<version>)
        #[arg()]
        evaluator: String,

        /// Evaluator state
        #[arg(value_name = "STATE", value_enum)]
        state: EvaluatorState,

        /// Optional reason for setting this state
        #[arg(long, value_name = "TEXT")]
        state_reason: Option<String>,
    },
}

#[async_trait]
impl Executable for SubCommand {
    /// Executes one evaluator operation against runtime + persistence context.
    async fn exec(self, context: Context) -> anyhow::Result<()> {
        match self {
            SubCommand::List => list(context).await,
            SubCommand::Publish {
                wasm: _,
                evaluator_path,
                release,
                profile,
            } => publish::exec(context, evaluator_path, release, profile).await,
            SubCommand::Package {
                wasm: _,
                evaluator_path,
                output,
                release,
                profile,
            } => {
                let component = context
                    .wasm()
                    .await?
                    .prepare_evaluator(evaluator_path, get_manifest_profile(release, profile))?;
                crate::evaluator_package::write(
                    &output,
                    crate::evaluator_package::PackageInput {
                        name: component.name,
                        version: component.version,
                        description: component.description,
                        tags: component.tags,
                        metadata: component.metadata,
                        abi: component.abi,
                        wasm: component.wasm_bytes,
                    },
                )?;
                info!("created evaluator package: {}", output.display());
                Ok(())
            }
            SubCommand::Show { evaluator } => show::exec(context, evaluator).await,
            SubCommand::Search {
                namespace,
                limit,
                query,
            } => search::exec(context, namespace, limit, query).await,
            SubCommand::Test {
                wasm: _,
                evaluator,
                input,
                input_file,
            } => test::exec(context, evaluator, input, input_file).await,
            SubCommand::SetState {
                evaluator,
                state,
                state_reason,
            } => set_state::exec(context, evaluator, state, state_reason).await,
        }
    }
}

impl SubCommand {
    pub(crate) fn wasm_options(&self) -> Option<WasmOptions> {
        match self {
            Self::Publish { wasm, .. } | Self::Package { wasm, .. } | Self::Test { wasm, .. } => {
                Some(*wasm)
            }
            _ => None,
        }
    }
}

async fn list(context: Context) -> anyhow::Result<()> {
    let db = context.dbr().await?.control().await?;
    let evaluators = evaluators::list_evaluators(db, DEFAULT_NAMESPACE).await?;

    if evaluators.is_empty() {
        warn!("no evaluators found");
    } else {
        for evaluator in evaluators {
            info!(
                "{}/{}:{} state={:?}",
                evaluator.namespace, evaluator.name, evaluator.version, evaluator.state,
            );
        }
    }

    Ok(())
}

#[derive(Debug, Args)]
/// Arguments for `vigilo evaluator`.
pub(crate) struct CanonicalCommand {
    #[command(subcommand)]
    pub command: SubCommand,
}

#[async_trait]
impl Executable for CanonicalCommand {
    async fn exec(self, context: Context) -> anyhow::Result<()> {
        self.command.exec(context).await
    }
}

#[derive(Debug, Args)]
/// Deprecated `vigilo evaluators` compatibility command.
pub(crate) struct Command {
    #[command(subcommand)]
    pub command: Option<SubCommand>,
}

#[async_trait]
impl Executable for Command {
    async fn exec(self, context: Context) -> anyhow::Result<()> {
        warn!("`vigilo evaluators` is deprecated; use `vigilo evaluator`");
        match self.command {
            Some(subcommand) => subcommand.exec(context).await,
            None => list(context).await,
        }
    }
}

/// Accepts only an existing directory or archive file; package validation follows.
fn parse_package_path(value: &str) -> Result<PathBuf, String> {
    let path = PathBuf::from(value);
    if path.is_dir() || path.is_file() {
        Ok(path)
    } else {
        Err(format!("'{value}' is not an evaluator package path"))
    }
}
