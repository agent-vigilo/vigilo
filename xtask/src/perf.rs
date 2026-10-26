//! External performance-harness command surface.
//!
//! The harness builds immutable release snapshots, resolves versioned workload
//! profiles, executes single-binary or counterbalanced comparison campaigns,
//! and writes machine-readable artifacts. It treats unsupported or incomplete
//! campaigns as invalid results instead of silently reducing their scope.
//!
//! # Module overview
//!
//! - `config` and `model` define the versioned input and output contracts.
//! - `build` creates immutable Vigilo snapshots and records their provenance.
//! - `schedule` and `stats` define sampling order and comparison semantics.
//! - `scaling` fits registered continuous or stepped component models, while
//!   `diagnostics` renders post-timing PostgreSQL planning, buffer, and WAL
//!   observations without changing gates.
//! - `calibration` turns canonical no-change and bounded-capacity evidence into
//!   reviewed budget/profile candidates and immutable baseline artifacts.
//! - `projection` combines bounded-capacity evidence with a named deployment
//!   input to expose resource demand, limit provenance, and confidence.
//! - `reliability` owns soak/recovery evidence and fail-closed verdicts for
//!   progress, resources, amplification, and reconnect deadlines.
//! - `process` owns bounded child execution and platform resource collection.
//! - `service`, `fixture`, and `workload` provision isolated dependencies,
//!   render deterministic inputs, execute service-backed workloads, and apply
//!   exact correctness oracles.
//! - `command` orchestrates run and comparison campaigns; `artifact` persists
//!   checkpoints, `provenance` freezes their validated input contracts, and
//!   `report` derives human-readable views from JSON results.
//! - `check` validates these contracts and isolation boundaries without
//!   provisioning benchmark services.
//!
//! `build` and campaign commands share a workspace lease. A measured sample is
//! accepted only after its process result and workload oracle are valid; timing
//! alone never makes a sample successful.

mod artifact;
mod build;
mod calibration;
mod check;
mod command;
mod config;
mod diagnostics;
mod fixture;
mod model;
mod process;
mod projection;
mod provenance;
mod reliability;
mod report;
mod scaling;
mod schedule;
mod service;
mod stats;
mod storage;
mod workload;

use std::{
    path::PathBuf,
    thread,
    time::Duration,
};

use anyhow::Result;
use clap::{
    Args,
    Subcommand,
};

/// Exit status for a completed campaign with no failing gate.
pub const EXIT_PASS: u8 = 0;
/// Exit status for a correctness failure or confirmed performance regression.
pub const EXIT_REGRESSION: u8 = 1;
/// Exit status for an unsupported, invalid, inconclusive, or incomplete result.
pub const EXIT_INVALID: u8 = 2;

/// Arguments for the `cargo perf` command family.
#[derive(Debug, Args)]
pub struct PerfArgs {
    #[command(subcommand)]
    command: PerfCommand,
}

#[derive(Debug, Subcommand)]
enum PerfCommand {
    /// Verify that the harness is safe and ready before local or CI use.
    Check(check::CheckArgs),
    /// Create a reusable, verified test subject from one Vigilo revision.
    Build(build::BuildArgs),
    /// Exercise one test subject; this does not compare revisions.
    Run(command::RunArgs),
    /// Regression-test a candidate against a baseline on the same host.
    Compare(command::CompareArgs),
    /// Recreate readable output from an existing report without rerunning workloads.
    Report(report::ReportArgs),
    /// Turn canonical stability and capacity runs into reviewed gating inputs.
    Calibrate(calibration::CalibrateArgs),
    /// Derive component cost and scaling models from a completed reference run.
    Model(scaling::ModelArgs),
    /// Estimate a named deployment's resource needs from measured capacity.
    Project(projection::ProjectArgs),
    /// Investigate captured PostgreSQL behavior without changing the run verdict.
    Diagnose(diagnostics::DiagnoseArgs),
}

/// Arguments for the hidden subprocess fixture used by lifecycle tests.
#[derive(Debug, Args)]
pub struct FixtureArgs {
    #[arg(long, default_value_t = 0)]
    delay_ms: u64,
    #[arg(long, default_value_t = 0)]
    output_bytes: usize,
    #[arg(long, default_value_t = 0)]
    exit_code: i32,
}

/// Dispatches a parsed performance-harness command and returns its process code.
pub fn run(args: PerfArgs) -> Result<u8> {
    match args.command {
        PerfCommand::Check(args) => check::execute(args),
        PerfCommand::Build(args) => build::execute(args),
        PerfCommand::Run(args) => command::run_single(args),
        PerfCommand::Compare(args) => command::compare(args),
        PerfCommand::Report(args) => report::execute(args),
        PerfCommand::Calibrate(args) => calibration::execute(args),
        PerfCommand::Model(args) => scaling::execute(args),
        PerfCommand::Project(args) => projection::execute(args),
        PerfCommand::Diagnose(args) => diagnostics::execute(args),
    }
}

/// Runs the hidden deterministic subprocess fixture used by harness self-tests.
pub fn run_fixture(args: FixtureArgs) -> Result<u8> {
    thread::sleep(Duration::from_millis(args.delay_ms));
    if args.output_bytes > 0 {
        println!("{}", "x".repeat(args.output_bytes));
    }
    Ok(args.exit_code.clamp(0, u8::MAX as i32) as u8)
}

/// Runs the opt-in live service integration fixture.
pub fn run_service_fixture() -> Result<u8> {
    let root = artifact::workspace_root()?;
    service::integration_self_test(&root)?;
    Ok(EXIT_PASS)
}

/// Returns the repository-relative directory containing campaign profiles.
fn default_profile_dir() -> PathBuf {
    PathBuf::from("performance/profiles")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_exit_codes_are_clamped_to_process_range() {
        assert_eq!(
            run_fixture(FixtureArgs {
                delay_ms: 0,
                output_bytes: 0,
                exit_code: -1,
            })
            .unwrap(),
            0
        );
        assert_eq!(
            run_fixture(FixtureArgs {
                delay_ms: 0,
                output_bytes: 0,
                exit_code: 300,
            })
            .unwrap(),
            u8::MAX
        );
        assert_eq!(default_profile_dir(), PathBuf::from("performance/profiles"));
    }
}
