//! Static and service-free validation of performance-harness contracts.
//!
//! Checks cover registry/profile consistency, audited production constants,
//! dependency and package isolation, artifact containment, endpoint ownership,
//! and subprocess cleanup. No benchmark services are provisioned here.

use std::{
    fs,
    path::Path,
    process::Command,
    time::Duration,
};

use anyhow::{
    Context,
    Result,
    bail,
};
use clap::Args;
use serde::Deserialize;
use serde_json::Value;
use serde_yaml::Value as YamlValue;

use super::{
    EXIT_PASS,
    artifact::{
        require_artifact_path,
        workspace_root,
    },
    calibration,
    config::{
        load_profile,
        load_registry,
        validate_profile_registry,
    },
    fixture,
    model::ImplementationStatus,
    process::{
        ProcessSpec,
        execute as execute_process,
    },
    projection,
    schedule,
};

const PROFILES: [&str; 16] = [
    "developer-v1",
    "pr-v1",
    "reference-v1",
    "calibration-v1",
    "capacity-v1",
    "component-reference-v1",
    "component-nightly-v1",
    "component-smoke-v1",
    "admin-nightly-v1",
    "admin-smoke-v1",
    "recovery-v1",
    "soak-v1",
    "ingestion-baseline-v1",
    "ingestion-reference-v1",
    "creation-query-v1",
    "creation-query-matrix-v1",
];

#[derive(Debug, Deserialize)]
struct CanonicalEnvironmentContract {
    schema_id: String,
    id: String,
    canonical: bool,
    provider: String,
    region: String,
    availability_zone_id: String,
    instance_type: String,
    market: String,
    os: String,
    ami_id: String,
    ami_name: String,
    architecture: String,
    vcpus: u64,
    memory_mib: u64,
    storage: String,
    root_volume_gib: u64,
    root_volume_type: String,
    root_volume_iops: u64,
    root_volume_throughput_mibps: u64,
    validity: Vec<String>,
}

/// CLI arguments for validating the performance harness.
#[derive(Debug, Args)]
pub struct CheckArgs {
    /// Optional Git base for proving that bootstrap-only changes leave product code untouched.
    #[arg(long)]
    bootstrap_base: Option<String>,
    /// Optional endpoint string to audit for loopback and ownership; it is never contacted.
    #[arg(long)]
    endpoint: Vec<String>,
    /// Required with `--endpoint`; every endpoint must contain this run-owned marker.
    #[arg(long)]
    ownership_marker: Option<String>,
}

/// Runs all service-free harness, provenance, and isolation checks.
pub fn execute(args: CheckArgs) -> Result<u8> {
    let root = workspace_root()?;
    let registry = load_registry(&root)?;
    validate_constants(&registry.constants)?;
    validate_environment_contract(&root)?;
    validate_external_tools(&root)?;
    for id in PROFILES {
        let profile = load_profile(&root, id)?;
        validate_profile_registry(&profile, &registry)?;
        for workload in &profile.workloads {
            if super::config::is_reliability_timing(&workload.timing) {
                continue;
            }
            schedule::validate(
                profile
                    .workloads
                    .iter()
                    .find(|item| item.id == workload.id && item.tuple == workload.tuple)
                    .expect("profile workload exists")
                    .blocks,
                profile.schedule_seed,
                &workload.id,
                &workload.tuple,
            )?;
        }
    }
    validate_profile_implementation_contract(&registry)?;
    validate_dependency_boundaries(&root)?;
    validate_package_contents(&root)?;
    validate_no_roadmap_markers(&root)?;
    validate_fixture_tree(&root)?;
    validate_service_configuration(&root)?;
    validate_ci_contract(&root)?;
    validate_suppressions(&root)?;
    calibration::validate_repository_contract(&root)?;
    let deployments = projection::validate_repository_contract(&root)?;
    if let Some(base) = args.bootstrap_base.as_deref() {
        validate_bootstrap_delta(&root, base)?;
    }
    validate_endpoints(&args.endpoint, args.ownership_marker.as_deref())?;
    let _ = require_artifact_path(&root, Path::new("target/perf/runs/check"))?;
    process_self_test()?;

    println!("Performance harness check passed");
    println!(
        "  registry:    workload-registry/v1 ({} workloads)",
        registry.workloads.len()
    );
    println!("  profiles:    {}", PROFILES.join(", "));
    println!("  environment: canonical AWS template matches environment/v1");
    println!("  tools:       cargo, rustc, and git available");
    println!("  production:  no reverse dependency or package leakage");
    println!("  process:     timeout, truncation, exit, and cleanup self-test passed");
    println!("  services:    Compose and fixture contracts valid; no services provisioned");
    println!("  CI:          hosted check active; self-hosted jobs explicitly gated");
    println!("  deployments: {deployments} named projection input(s) valid");
    Ok(EXIT_PASS)
}

fn validate_ci_contract(root: &Path) -> Result<()> {
    let path = root.join(".github/workflows/performance.yaml");
    let source = fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    validate_ci_contract_source(&source).with_context(|| format!("validate {}", path.display()))
}

fn validate_ci_contract_source(source: &str) -> Result<()> {
    let _: serde_yaml::Value =
        serde_yaml::from_str(source).context("parse performance workflow")?;
    for required in [
        "vars.VIGILO_PERF_RUNNER_ENABLED == 'true'",
        "vars.VIGILO_PERF_SCHEDULES_ENABLED == 'true'",
        "VIGILO_PERF_REFERENCE_PROFILE",
        "VIGILO_PERF_DEPLOYMENT",
        "runs-on: [self-hosted, linux, x64, vigilo-performance]",
        "workflow_dispatch:",
        "schedule:",
        "reference-v1",
        "component-nightly-v1",
        "recovery-v1",
        "soak-v1",
        "GITHUB_STEP_SUMMARY",
        "actions/upload-artifact@v6",
        "env.VIGILO_PERF_DEPLOYMENT != ''",
        "--deployment \"$VIGILO_PERF_DEPLOYMENT\"",
    ] {
        if !source.contains(required) {
            bail!("performance workflow omitted required contract: {required}");
        }
    }
    if source.contains("--deployment performance/deployments/planning-example-v1.toml") {
        bail!(
            "performance workflow must not schedule the intentionally invalid example deployment"
        );
    }
    Ok(())
}

#[derive(Deserialize)]
struct SuppressionPolicy {
    schema_id: String,
    suppressions: Vec<Suppression>,
}

#[derive(Deserialize)]
struct Suppression {
    workload_id: String,
    tuple_id: String,
    metric: String,
    issue: String,
    owner: String,
    expires_at: String,
    reason: String,
}

fn validate_suppressions(root: &Path) -> Result<()> {
    let path = root.join("performance/suppressions-v1.toml");
    let source = fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let policy: SuppressionPolicy =
        toml::from_str(&source).with_context(|| format!("parse {}", path.display()))?;
    validate_suppression_policy(&policy)
}

fn validate_suppression_policy(policy: &SuppressionPolicy) -> Result<()> {
    if policy.schema_id != "performance-suppressions/v1" {
        bail!(
            "unsupported performance suppression schema: {}",
            policy.schema_id
        );
    }
    for suppression in &policy.suppressions {
        if [
            &suppression.workload_id,
            &suppression.tuple_id,
            &suppression.metric,
            &suppression.issue,
            &suppression.owner,
            &suppression.reason,
        ]
        .into_iter()
        .any(|value| value.trim().is_empty() || value == "*")
        {
            bail!("performance suppression must be metric-specific, owned, and issue-linked");
        }
        let expires = chrono::DateTime::parse_from_rfc3339(&suppression.expires_at)
            .context("performance suppression expiry must be RFC 3339")?;
        if expires <= chrono::Utc::now() {
            bail!(
                "performance suppression for {}:{} has expired",
                suppression.workload_id,
                suppression.metric
            );
        }
    }
    Ok(())
}

/// Validates the canonical host descriptor used to label comparable results.
fn validate_environment_contract(root: &Path) -> Result<()> {
    let path = root.join("performance/environments/aws-m6i-2xlarge-al2023-v1.toml");
    let environment_source =
        fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let template_path = root.join("infra/performance/aws/template.yaml");
    let template_source = fs::read_to_string(&template_path)
        .with_context(|| format!("read {}", template_path.display()))?;
    validate_environment_contract_sources(&environment_source, &template_source)
}

/// Requires the host descriptor and its CloudFormation implementation to agree exactly.
fn validate_environment_contract_sources(
    environment_source: &str,
    template_source: &str,
) -> Result<()> {
    let environment: CanonicalEnvironmentContract = toml::from_str(environment_source)?;
    if environment.schema_id != super::model::ENVIRONMENT_SCHEMA
        || environment.id != "aws-m6i-2xlarge-al2023-v1"
        || !environment.canonical
        || environment.provider != "aws"
        || environment.region != "us-west-2"
        || environment.market != "on-demand"
        || environment.os != "Amazon Linux 2023"
        || environment.architecture != "x86_64"
        || environment.vcpus != 8
        || environment.memory_mib != 32_768
        || environment.storage.trim().is_empty()
        || environment.validity
            != [
                "exclusive_host",
                "fixed_cpu_policy",
                "no_swap",
                "thermal_and_load_check",
            ]
    {
        bail!("canonical performance environment contract is invalid");
    }

    let template: YamlValue = serde_yaml::from_str(template_source)?;
    let metadata = yaml_path(&template, &["Metadata", "Vigilo", "Environment"])?;
    require_yaml_string(metadata, "Id", &environment.id)?;
    require_yaml_string(metadata, "Region", &environment.region)?;
    require_yaml_string(
        metadata,
        "AvailabilityZoneId",
        &environment.availability_zone_id,
    )?;
    require_yaml_string(metadata, "AmiId", &environment.ami_id)?;
    require_yaml_string(metadata, "AmiName", &environment.ami_name)?;
    require_yaml_string(metadata, "InstanceType", &environment.instance_type)?;
    require_yaml_string(metadata, "Market", &environment.market)?;
    require_yaml_string(metadata, "Architecture", &environment.architecture)?;
    require_yaml_u64(metadata, "Vcpus", environment.vcpus)?;
    require_yaml_u64(metadata, "MemoryMiB", environment.memory_mib)?;
    require_yaml_u64(metadata, "RootVolumeGiB", environment.root_volume_gib)?;
    require_yaml_string(metadata, "RootVolumeType", &environment.root_volume_type)?;
    require_yaml_u64(metadata, "RootVolumeIops", environment.root_volume_iops)?;
    require_yaml_u64(
        metadata,
        "RootVolumeThroughputMiBps",
        environment.root_volume_throughput_mibps,
    )?;

    let subnet = yaml_path(&template, &["Resources", "PerformanceSubnet", "Properties"])?;
    require_yaml_string(
        subnet,
        "AvailabilityZoneId",
        &environment.availability_zone_id,
    )?;

    let instance = yaml_path(
        &template,
        &["Resources", "PerformanceInstance", "Properties"],
    )?;
    require_yaml_string(instance, "ImageId", &environment.ami_id)?;
    require_yaml_string(instance, "InstanceType", &environment.instance_type)?;
    require_yaml_string(instance, "Tenancy", "default")?;
    require_yaml_string(instance, "InstanceInitiatedShutdownBehavior", "terminate")?;
    require_yaml_bool(instance, "EbsOptimized", true)?;
    require_yaml_bool(instance, "Monitoring", false)?;
    if instance.get("InstanceMarketOptions").is_some() {
        bail!("canonical performance instance must use on-demand capacity");
    }
    require_yaml_string(
        yaml_path(instance, &["MetadataOptions"])?,
        "HttpTokens",
        "required",
    )?;

    let mappings = yaml_path(instance, &["BlockDeviceMappings"])?
        .as_sequence()
        .context("performance instance BlockDeviceMappings must be a sequence")?;
    if mappings.len() != 1 {
        bail!("performance instance must have exactly one root block-device mapping");
    }
    require_yaml_string(&mappings[0], "DeviceName", "/dev/xvda")?;
    let ebs = yaml_path(&mappings[0], &["Ebs"])?;
    require_yaml_bool(ebs, "DeleteOnTermination", true)?;
    require_yaml_bool(ebs, "Encrypted", true)?;
    require_yaml_u64(ebs, "Iops", environment.root_volume_iops)?;
    require_yaml_u64(ebs, "Throughput", environment.root_volume_throughput_mibps)?;
    require_yaml_u64(ebs, "VolumeSize", environment.root_volume_gib)?;
    require_yaml_string(ebs, "VolumeType", &environment.root_volume_type)?;

    let security_group = yaml_path(
        &template,
        &["Resources", "PerformanceSecurityGroup", "Properties"],
    )?;
    if security_group.get("SecurityGroupIngress").is_some() {
        bail!("canonical performance security group must not allow inbound traffic");
    }
    yaml_path(&template, &["Rules", "CanonicalRegionOnly"])?;
    yaml_path(&template, &["Resources", "BootstrapWaitHandle"])?;
    let wait = yaml_path(
        &template,
        &["Resources", "BootstrapWaitCondition", "Properties"],
    )?;
    require_yaml_u64(wait, "Count", 1)?;
    require_yaml_string(wait, "Timeout", "1800")?;
    if template_source.contains("VIGILO_PERF_CANONICAL_VALIDATED") {
        bail!("AWS bootstrap must not self-certify the canonical environment");
    }
    Ok(())
}

fn yaml_path<'a>(value: &'a YamlValue, path: &[&str]) -> Result<&'a YamlValue> {
    let mut current = value;
    for key in path {
        current = current
            .get(*key)
            .with_context(|| format!("CloudFormation contract is missing {}", path.join(".")))?;
    }
    Ok(current)
}

fn require_yaml_string(value: &YamlValue, key: &str, expected: &str) -> Result<()> {
    let actual = yaml_path(value, &[key])?.as_str();
    if actual != Some(expected) {
        bail!("CloudFormation {key} must be {expected:?}, found {actual:?}");
    }
    Ok(())
}

fn require_yaml_u64(value: &YamlValue, key: &str, expected: u64) -> Result<()> {
    let actual = yaml_path(value, &[key])?.as_u64();
    if actual != Some(expected) {
        bail!("CloudFormation {key} must be {expected}, found {actual:?}");
    }
    Ok(())
}

fn require_yaml_bool(value: &YamlValue, key: &str, expected: bool) -> Result<()> {
    let actual = yaml_path(value, &[key])?.as_bool();
    if actual != Some(expected) {
        bail!("CloudFormation {key} must be {expected}, found {actual:?}");
    }
    Ok(())
}

/// Proves the service-free toolchain commands required by the harness are callable.
fn validate_external_tools(root: &Path) -> Result<()> {
    for (program, args) in [
        ("cargo", &["-V"][..]),
        ("rustc", &["-V"][..]),
        ("git", &["--version"][..]),
    ] {
        command_output(root, program, args)?;
    }
    Ok(())
}

/// Rejects zero production limits that would make fixture planning meaningless.
fn validate_constants(constants: &super::model::RegistryConstants) -> Result<()> {
    let values = [
        constants.database_connections_per_target as u64,
        constants.database_acquire_timeout_ms,
        constants.database_operation_deadline_ms,
        constants.run_chunk_size as u64,
        constants.creation_case_page_size as u64,
        constants.creation_page_budget as u64,
        constants.case_blob_group_size as u64,
        constants.membership_group_size as u64,
        constants.chunk_insert_group_size as u64,
        constants.coordinator_tick_ms,
        constants.coordinator_create_recovery_budget as u64,
        constants.coordinator_dispatch_budget as u64,
        constants.coordinator_finalization_budget as u64,
        constants.dispatch_window_size as u64,
        constants.lease_recovery_batch_size as u64,
        constants.outbox_batch_size as u64,
        constants.outbox_publish_parallelism as u64,
        constants.worker_default_inflight_chunks as u64,
        constants.worker_heartbeat_ms,
        constants.case_concurrency as u64,
        constants.evaluator_concurrency as u64,
        constants.wasm_concurrency as u64,
        constants.wasm_max_memory_mib as u64,
        constants.result_insert_group_size as u64,
    ];
    if values.contains(&0) {
        bail!("registered production constants must be positive");
    }
    Ok(())
}

/// Requires the ordered MVP anchors while permitting additive future workloads.
fn validate_profile_implementation_contract(
    registry: &super::model::WorkloadRegistry,
) -> Result<()> {
    let implemented: Vec<_> = registry
        .workloads
        .iter()
        .filter(|workload| workload.status == ImplementationStatus::Implemented)
        .collect();
    let ids = implemented
        .iter()
        .map(|workload| workload.id.as_str())
        .collect::<Vec<_>>();
    let expected = [
        "startup.cli-help.v1",
        "run.create.v1",
        "coordinator.dispatch.v1",
        "worker.execute-wasm.v1",
        "system.lifecycle.v1",
    ];
    let mut implemented = ids.iter();
    if !expected
        .iter()
        .all(|expected| implemented.any(|actual| actual == expected))
    {
        bail!("the complete ordered MVP workload set must be implemented");
    }
    let startup = registry
        .workloads
        .iter()
        .find(|workload| workload.id == "startup.cli-help.v1")
        .context("implemented workload set is empty")?;
    if startup.command != ["--help"] || startup.help_signatures.is_empty() {
        bail!("startup workload must use the supported --help boundary");
    }
    Ok(())
}

/// Checks fixture cardinalities and Compose isolation markers without starting services.
fn validate_service_configuration(root: &Path) -> Result<()> {
    let fixture = fixture::load(root, "mvp-v1")?;
    if fixture.coordinator.chunks != 512
        || fixture.run_create.cases != 1001
        || fixture.lifecycle.cases == 0
    {
        bail!("MVP fixture cardinalities do not match the frozen workload contract");
    }
    let compose = root.join("infra/performance/compose.yml");
    let content = fs::read_to_string(&compose)?;
    for required in [
        "127.0.0.1::5432",
        "127.0.0.1::5672",
        "io.vigilo.performance",
        "io.vigilo.run-id",
        "VIGILO_PERF_PROJECT",
    ] {
        if !content.contains(required) {
            bail!("performance Compose contract is missing {required:?}");
        }
    }
    Ok(())
}

/// Proves production dependencies do not include the performance harness or benchmark crates.
fn validate_dependency_boundaries(root: &Path) -> Result<()> {
    let output = cargo_output(root, &["metadata", "--locked", "--format-version", "1"])?;
    let metadata: Value = serde_json::from_slice(&output)?;
    let packages = metadata["packages"]
        .as_array()
        .context("cargo metadata packages missing")?;
    let vigilo = packages
        .iter()
        .find(|package| package["name"] == "vigilo")
        .context("vigilo package missing from metadata")?;
    for dependency in vigilo["dependencies"]
        .as_array()
        .context("vigilo dependencies missing")?
    {
        let kind = dependency["kind"].as_str().unwrap_or("normal");
        if kind == "dev" {
            continue;
        }
        let name = dependency["name"].as_str().unwrap_or_default();
        let path = dependency["path"].as_str().unwrap_or_default();
        if name == "xtask" || name.starts_with("criterion") || path.contains("performance") {
            bail!("production dependency graph contains performance dependency {name}");
        }
    }
    let xtask = packages
        .iter()
        .find(|package| package["name"] == "xtask")
        .context("xtask package missing from metadata")?;
    if xtask["dependencies"]
        .as_array()
        .context("xtask dependencies missing")?
        .iter()
        .any(|dependency| dependency["name"] == "vigilo")
    {
        bail!("xtask must not depend on vigilo");
    }
    let tree = cargo_output(
        root,
        &[
            "tree",
            "--locked",
            "-p",
            "vigilo",
            "--edges",
            "normal,build",
        ],
    )?;
    let tree = String::from_utf8_lossy(&tree).to_ascii_lowercase();
    if tree.contains("criterion") || tree.lines().any(|line| line.contains("xtask v")) {
        bail!("release dependency tree contains performance tooling");
    }
    Ok(())
}

/// Proves the publishable Vigilo package does not contain harness files or artifacts.
fn validate_package_contents(root: &Path) -> Result<()> {
    let output = cargo_output(
        root,
        &[
            "package",
            "-p",
            "vigilo",
            "--list",
            "--locked",
            "--allow-dirty",
        ],
    )?;
    for line in String::from_utf8_lossy(&output).lines() {
        let normalized = line.replace('\\', "/").to_ascii_lowercase();
        if normalized.contains("performance/")
            || normalized.contains("xtask/")
            || normalized.contains("target/perf")
            || normalized.contains("criterion")
        {
            bail!("published Vigilo package leaks performance content: {line}");
        }
    }
    Ok(())
}

/// Rejects numbered implementation-roadmap labels in repository source files.
fn validate_no_roadmap_markers(root: &Path) -> Result<()> {
    let output = command_output(
        root,
        "git",
        &[
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
        ],
    )?;
    for relative in output
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
    {
        let relative = String::from_utf8_lossy(relative).replace('\\', "/");
        if !is_reviewed_text_path(Path::new(&relative)) {
            continue;
        }
        let path = root.join(&relative);
        if !path.is_file() {
            continue;
        }
        let content = fs::read_to_string(&path)
            .with_context(|| format!("read roadmap policy input {}", path.display()))?;
        if contains_numbered_roadmap_marker(&content) {
            bail!("numbered implementation roadmap marker in repository source: {relative}");
        }
    }
    Ok(())
}

fn is_reviewed_text_path(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            matches!(
                extension,
                "css"
                    | "html"
                    | "js"
                    | "json"
                    | "md"
                    | "mdx"
                    | "mjs"
                    | "mmd"
                    | "ps1"
                    | "rs"
                    | "sh"
                    | "toml"
                    | "ts"
                    | "tsx"
                    | "wit"
                    | "yaml"
                    | "yml"
            )
        })
}

fn contains_numbered_roadmap_marker(content: &str) -> bool {
    let content = content.to_ascii_lowercase();
    let numbers = (0..=9).map(|number| number.to_string());
    let words = ["zero", "one", "two", "three", "four", "five", "six"]
        .into_iter()
        .map(str::to_owned);
    numbers.chain(words).any(|value| {
        [
            format!("phase {value}"),
            format!("phase-{value}"),
            format!("phase_{value}"),
        ]
        .iter()
        .any(|marker| content.contains(marker))
    })
}

/// Rejects oversized, generated, or private-source-importing checked-in fixtures.
fn validate_fixture_tree(root: &Path) -> Result<()> {
    fn visit(path: &Path) -> Result<()> {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                visit(&entry.path())?;
                continue;
            }
            let metadata = entry.metadata()?;
            if metadata.len() > 1024 * 1024 {
                bail!(
                    "checked-in performance fixture is too large: {}",
                    entry.path().display()
                );
            }
            let content = fs::read_to_string(entry.path())?;
            if content.contains("#[path") && content.contains("vigilo/src") {
                bail!(
                    "performance code imports private production source: {}",
                    entry.path().display()
                );
            }
            if matches!(
                entry
                    .path()
                    .extension()
                    .and_then(|extension| extension.to_str()),
                Some("jsonl" | "log")
            ) {
                bail!(
                    "generated performance artifact is checked in: {}",
                    entry.path().display()
                );
            }
        }
        Ok(())
    }
    visit(&root.join("performance"))
}

/// Ensures a harness-only bootstrap diff does not modify production runtime paths.
fn validate_bootstrap_delta(root: &Path, base: &str) -> Result<()> {
    let mut changed = Vec::new();
    for args in [
        vec!["diff", "--name-only", &format!("{base}...HEAD")],
        vec!["diff", "--name-only"],
        vec!["diff", "--cached", "--name-only"],
    ] {
        let owned: Vec<String> = args.into_iter().map(str::to_owned).collect();
        let borrowed: Vec<&str> = owned.iter().map(String::as_str).collect();
        changed.extend(
            String::from_utf8_lossy(&command_output(root, "git", &borrowed)?)
                .lines()
                .map(str::to_owned),
        );
    }
    changed.sort();
    changed.dedup();
    let status_output = command_output(root, "git", &["status", "--porcelain"])?;
    let status = String::from_utf8_lossy(&status_output);
    changed.extend(
        status
            .lines()
            .filter_map(|line| line.get(3..))
            .map(|path| path.rsplit(" -> ").next().unwrap_or(path).to_owned()),
    );
    changed.sort();
    changed.dedup();
    for path in changed {
        let path = path.replace('\\', "/");
        if path.starts_with("vigilo/src/")
            || path.starts_with("vigilo/migrations/")
            || path == "vigilo/Cargo.toml"
        {
            bail!("bootstrap harness delta changes production path: {path}");
        }
    }
    Ok(())
}

/// Validates optional destructive endpoints against one explicit ownership marker.
fn validate_endpoints(endpoints: &[String], marker: Option<&str>) -> Result<()> {
    if endpoints.is_empty() {
        return Ok(());
    }
    let marker = marker.context("--ownership-marker is required with --endpoint")?;
    if !marker.starts_with("vigilo_perf_")
        || !marker
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        bail!("ownership marker must use the vigilo_perf_<id> form");
    }
    for endpoint in endpoints {
        validate_endpoint(endpoint, marker)?;
    }
    Ok(())
}

/// Requires a destructive endpoint to be loopback-only and run-owned.
fn validate_endpoint(endpoint: &str, marker: &str) -> Result<()> {
    let lower = endpoint.to_ascii_lowercase();
    let authority = lower
        .split_once("://")
        .map(|(_, rest)| rest)
        .context("endpoint must include a URL scheme")?
        .split('/')
        .next()
        .unwrap_or_default();
    let host_port = authority.rsplit('@').next().unwrap_or_default();
    let host = if let Some(bracketed) = host_port.strip_prefix('[') {
        bracketed.split(']').next().unwrap_or_default()
    } else {
        host_port.split(':').next().unwrap_or_default()
    };
    if !matches!(host, "localhost" | "127.0.0.1" | "::1") {
        bail!("destructive endpoint is not loopback-isolated: {endpoint}");
    }
    if !endpoint.contains(marker) {
        bail!("endpoint does not contain its ownership marker: {endpoint}");
    }
    Ok(())
}

/// Exercises success, crash, timeout, truncation, and process-tree cleanup behavior.
fn process_self_test() -> Result<()> {
    let executable = std::env::current_exe()?;
    let valid = vec!["__perf-fixture".into(), "--delay-ms".into(), "1".into()];
    let outcome = execute_process(&ProcessSpec {
        program: &executable,
        args: &valid,
        current_dir: None,
        env: &[],
        timeout: Duration::from_secs(2),
        stdout_limit: 1024,
        stderr_limit: 1024,
    })?;
    if outcome.exit_code != Some(0) || outcome.timed_out {
        bail!("process self-test valid fixture failed");
    }

    let crash = vec!["__perf-fixture".into(), "--exit-code".into(), "7".into()];
    let outcome = execute_process(&ProcessSpec {
        program: &executable,
        args: &crash,
        current_dir: None,
        env: &[],
        timeout: Duration::from_secs(2),
        stdout_limit: 1024,
        stderr_limit: 1024,
    })?;
    if outcome.exit_code != Some(7) || outcome.timed_out {
        bail!("process self-test crash classification failed");
    }

    let timeout = vec!["__perf-fixture".into(), "--delay-ms".into(), "1000".into()];
    let outcome = execute_process(&ProcessSpec {
        program: &executable,
        args: &timeout,
        current_dir: None,
        env: &[],
        timeout: Duration::from_millis(20),
        stdout_limit: 1024,
        stderr_limit: 1024,
    })?;
    if !outcome.timed_out {
        bail!("process self-test timeout classification failed");
    }

    let truncation = vec![
        "__perf-fixture".into(),
        "--output-bytes".into(),
        "4096".into(),
    ];
    let outcome = execute_process(&ProcessSpec {
        program: &executable,
        args: &truncation,
        current_dir: None,
        env: &[],
        timeout: Duration::from_secs(2),
        stdout_limit: 64,
        stderr_limit: 64,
    })?;
    if !outcome.stdout.truncated || outcome.stdout.data.len() != 64 {
        bail!("process self-test truncation classification failed");
    }
    Ok(())
}

/// Runs Cargo as a checked byte-producing command in the workspace.
fn cargo_output(root: &Path, args: &[&str]) -> Result<Vec<u8>> {
    command_output(root, "cargo", args)
}

/// Returns standard output only when an external validation command succeeds.
fn command_output(root: &Path, program: &str, args: &[&str]) -> Result<Vec<u8>> {
    let output = Command::new(program)
        .args(args)
        .current_dir(root)
        .output()
        .with_context(|| format!("run {program} {}", args.join(" ")))?;
    if !output.status.success() {
        bail!(
            "{program} {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output.stdout)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_policy_accepts_only_owned_loopback_resources() {
        let marker = "vigilo_perf_123";
        assert!(validate_endpoint("postgres://localhost/vigilo_perf_123", marker).is_ok());
        assert!(validate_endpoint("amqp://127.0.0.1/vigilo_perf_123", marker).is_ok());
        assert!(validate_endpoint("postgres://db.example.com/vigilo_perf_123", marker).is_err());
        assert!(validate_endpoint("postgres://localhost/development", marker).is_err());
    }

    #[test]
    fn repository_static_contracts_pass_without_starting_services() {
        let root = workspace_root().unwrap();
        let registry = load_registry(&root).unwrap();
        validate_constants(&registry.constants).unwrap();
        validate_environment_contract(&root).unwrap();
        validate_external_tools(&root).unwrap();
        validate_profile_implementation_contract(&registry).unwrap();
        validate_dependency_boundaries(&root).unwrap();
        validate_package_contents(&root).unwrap();
        validate_no_roadmap_markers(&root).unwrap();
        validate_fixture_tree(&root).unwrap();
        validate_service_configuration(&root).unwrap();
        validate_ci_contract(&root).unwrap();
        validate_suppressions(&root).unwrap();
        calibration::validate_repository_contract(&root).unwrap();
    }

    #[test]
    fn canonical_environment_rejects_cloudformation_drift_and_self_certification() {
        let root = workspace_root().unwrap();
        let environment = fs::read_to_string(
            root.join("performance/environments/aws-m6i-2xlarge-al2023-v1.toml"),
        )
        .unwrap();
        let template =
            fs::read_to_string(root.join("infra/performance/aws/template.yaml")).unwrap();
        validate_environment_contract_sources(&environment, &template).unwrap();

        let wrong_instance = template.replace(
            "      InstanceType: m6i.2xlarge",
            "      InstanceType: m6i.4xlarge",
        );
        assert!(validate_environment_contract_sources(&environment, &wrong_instance).is_err());

        let self_certifying = format!("{template}\n# VIGILO_PERF_CANONICAL_VALIDATED=1\n");
        assert!(validate_environment_contract_sources(&environment, &self_certifying).is_err());
    }

    #[test]
    fn roadmap_policy_distinguishes_numbered_milestones_from_runtime_language() {
        let numbered = format!("{} {} calibration", "Phase", 3);
        assert!(contains_numbered_roadmap_marker(&numbered));
        let underscored = format!("{}_{}_calibration", "phase", "three");
        assert!(contains_numbered_roadmap_marker(&underscored));
        assert!(!contains_numbered_roadmap_marker("evaluation phase"));
        assert!(!contains_numbered_roadmap_marker("three-phase power"));
    }

    #[test]
    fn ci_projection_requires_an_explicit_nonexample_deployment() {
        let root = workspace_root().unwrap();
        let source = fs::read_to_string(root.join(".github/workflows/performance.yaml")).unwrap();
        validate_ci_contract_source(&source).unwrap();

        let unguarded = source.replace(
            "inputs.suite != 'recovery' && env.VIGILO_PERF_DEPLOYMENT != ''",
            "inputs.suite != 'recovery'",
        );
        assert!(validate_ci_contract_source(&unguarded).is_err());

        let example =
            format!("{source}\n# --deployment performance/deployments/planning-example-v1.toml\n");
        assert!(validate_ci_contract_source(&example).is_err());
    }

    #[test]
    fn static_validators_reject_zero_constants_and_unsafe_fixture_files() {
        let root = workspace_root().unwrap();
        let registry = load_registry(&root).unwrap();
        let mut constants = registry.constants;
        constants.dispatch_window_size = 0;
        assert!(validate_constants(&constants).is_err());

        let directory = tempfile::tempdir().unwrap();
        let performance = directory.path().join("performance");
        fs::create_dir_all(&performance).unwrap();
        fs::write(performance.join("generated.jsonl"), "{}\n").unwrap();
        assert!(validate_fixture_tree(directory.path()).is_err());
        fs::remove_file(performance.join("generated.jsonl")).unwrap();
        fs::write(
            performance.join("private.rs"),
            "#[path = \"../../vigilo/src/private.rs\"] mod private;",
        )
        .unwrap();
        assert!(validate_fixture_tree(directory.path()).is_err());
    }

    #[test]
    fn registry_contract_requires_mvp_anchors_and_allows_future_workloads() {
        let root = workspace_root().unwrap();
        let registry = load_registry(&root).unwrap();

        let mut extended = registry.clone();
        let mut future = extended.workloads[0].clone();
        future.id = "future.calibrated-gate.v1".into();
        future.capability = future.id.clone();
        extended.workloads.push(future);
        assert!(validate_profile_implementation_contract(&extended).is_ok());

        let mut incomplete = registry.clone();
        incomplete
            .workloads
            .retain(|workload| workload.id != "worker.execute-wasm.v1");
        assert!(validate_profile_implementation_contract(&incomplete).is_err());

        let mut reordered = registry;
        reordered.workloads.swap(1, 2);
        assert!(validate_profile_implementation_contract(&reordered).is_err());
    }

    #[test]
    fn suppression_policy_requires_specific_owned_unexpired_entries() {
        let mut policy = SuppressionPolicy {
            schema_id: "performance-suppressions/v1".into(),
            suppressions: vec![Suppression {
                workload_id: "system.lifecycle.v1".into(),
                tuple_id: "workers-1".into(),
                metric: "wall_time".into(),
                issue: "https://example.test/issues/1".into(),
                owner: "distributed-runtime".into(),
                expires_at: "2999-01-01T00:00:00Z".into(),
                reason: "temporary hardware investigation".into(),
            }],
        };
        assert!(validate_suppression_policy(&policy).is_ok());
        policy.suppressions[0].metric = "*".into();
        assert!(validate_suppression_policy(&policy).is_err());
        policy.suppressions[0].metric = "wall_time".into();
        policy.suppressions[0].expires_at = "2020-01-01T00:00:00Z".into();
        assert!(validate_suppression_policy(&policy).is_err());
        policy.schema_id = "performance-suppressions/v2".into();
        assert!(validate_suppression_policy(&policy).is_err());
    }
}
