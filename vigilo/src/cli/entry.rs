//! Binary entry point for the `agent-vigilo` CLI.
//!
//! This module wires clap argument parsing, process logging, shared runtime
//! context construction, and top-level command dispatch. Keep application
//! behavior in command/runtime modules; the entry point should stay limited to
//! process setup, error reporting, and exit-code selection.

use std::{
    io::stderr,
    process::ExitCode,
    time::Duration,
};

use clap::Parser;
use tracing::{
    Level,
    error,
};
use tracing_subscriber::{
    EnvFilter,
    Registry,
    fmt,
    prelude::*,
};

use super::{
    App,
    Executable,
};
use crate::{
    agent_client,
    context,
    context::Context,
    evaluator_requests,
};

fn init_logger(quiet: bool, verbose: u8) {
    let level = if quiet {
        Level::ERROR
    } else {
        match verbose {
            0 => Level::INFO,
            1 => Level::DEBUG,
            _ => Level::TRACE,
        }
    };

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(""))
        .add_directive(level.into());

    let subscriber = Registry::default()
        .with(fmt::layer().with_writer(stderr).with_target(false))
        .with(filter);

    let _ = subscriber.try_init();
}

pub(super) async fn run() -> ExitCode {
    let app_parse_result = App::try_parse();

    match app_parse_result {
        Ok(app) => {
            init_logger(app.quiet, app.verbose);

            let command_context = match app.command.context_config(&app.control_database_alias) {
                Ok(config) => config,
                Err(e) => {
                    error!(error = %e, "invalid command configuration");
                    return ExitCode::FAILURE;
                }
            };

            let database_url = match app.database_uri() {
                Ok(url) => url,
                Err(e) => {
                    error!(error = %e, "invalid database configuration");
                    return ExitCode::FAILURE;
                }
            };
            let context = Context::new(
                context::database::Config::new(
                    database_url,
                    app.database_pool_max_connections_per_target,
                    Duration::from_secs(app.database_acquire_timeout_seconds),
                    command_context.circuit_breaker,
                    command_context.placement,
                )
                .with_operation_timeout(command_context.database_operation_timeout),
                command_context.messaging,
                command_context.wasm,
                app.output_format,
            )
            .with_agent_http_limits(command_context.agent_http);

            let context =
                match context.with_http_policy_max_age(app.evaluator_http_policy_max_age_seconds) {
                    Ok(context) => context,
                    Err(error) => {
                        error!(%error, "invalid evaluator HTTP policy freshness");
                        return ExitCode::FAILURE;
                    }
                };

            let context = if let Some(path) = command_context.agent_credentials {
                match agent_client::credentials::Provider::load(&path).await {
                    Ok(provider) => context.with_agent_credentials(provider),
                    Err(error) => {
                        error!(%error, "invalid worker credential configuration");
                        return ExitCode::FAILURE;
                    }
                }
            } else {
                context
            };

            let context = if let Some(path) = &app.evaluator_http_config {
                match evaluator_requests::Service::load(path).await {
                    Ok(service) => context.with_evaluator_http(service),
                    Err(error) => {
                        error!(%error, "invalid evaluator HTTP configuration");
                        return ExitCode::FAILURE;
                    }
                }
            } else {
                context
            };

            match app.exec(context).await {
                Err(e) => {
                    error!(error = %format!("{e:#}"), "command execution failed");
                    ExitCode::FAILURE
                }
                Ok(()) => ExitCode::SUCCESS,
            }
        }
        Err(e) => {
            init_logger(false, 0);

            if !e.use_stderr() {
                _ = e.print();
                return ExitCode::SUCCESS;
            }

            for (kind, value) in e.context() {
                if let Some(kind) = kind.as_str()
                    && !kind.is_empty()
                {
                    error!("command failed: {}: {}", kind, value);
                }
            }

            ExitCode::from(e.exit_code() as u8)
        }
    }
}
