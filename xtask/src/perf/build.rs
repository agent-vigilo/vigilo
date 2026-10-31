//! Immutable release build snapshots and provenance manifests.
//!
//! Builds run outside measured campaigns. Each snapshot records the executable,
//! toolchain and dependency provenance, migration and evaluator ABI digests, and
//! the semantic workload capabilities that the resulting binary can execute.

use std::{
    collections::BTreeMap,
    fs,
    path::{
        Path,
        PathBuf,
    },
    process::{
        Command,
        Stdio,
    },
    time::Duration,
};

use anyhow::{
    Context,
    Result,
    bail,
};
use chrono::Utc;
use clap::Args;

use super::{
    EXIT_PASS,
    artifact::{
        CampaignLease,
        atomic_json,
        copy_tree,
        digest_file,
        digest_tree,
        digest_tree_without,
        require_artifact_subpath,
        workspace_root,
    },
    model::{
        BUILD_SCHEMA,
        BuildManifest,
        SetupAsset,
    },
    process::{
        ProcessSpec,
        execute as execute_process,
    },
};

const EVALUATOR_METADATA_CACHE: &str = "target/.rustc_info.json";

/// CLI arguments for creating a release build snapshot.
#[derive(Debug, Args)]
pub struct BuildArgs {
    /// Vigilo worktree to compile; use `.` for the current checkout.
    #[arg(long, default_value = ".")]
    source: PathBuf,
    /// New snapshot directory under `target/perf/builds` for the executable and manifest.
    #[arg(long)]
    output: PathBuf,
    /// Replace an existing harness-owned local snapshot at `--output`.
    #[arg(long)]
    force: bool,
}

/// Builds Vigilo and writes its immutable build manifest and setup assets.
pub fn execute(args: BuildArgs) -> Result<u8> {
    let harness_root = workspace_root()?;
    let source = fs::canonicalize(&args.source)
        .with_context(|| format!("resolve source {}", args.source.display()))?;
    if !source.join("Cargo.toml").is_file() || !source.join("vigilo/Cargo.toml").is_file() {
        bail!("source is not a Vigilo workspace: {}", source.display());
    }
    let output = require_artifact_subpath(&harness_root, &args.output, "builds")?;
    let _lease = CampaignLease::acquire(&harness_root)?;
    if output.exists() {
        if !args.force {
            bail!(
                "build directory already exists; choose a unique --output or pass --force: {}",
                output.display()
            );
        }
        fs::remove_dir_all(&output)
            .with_context(|| format!("remove harness build {}", output.display()))?;
    }
    fs::create_dir_all(&output)?;

    println!("Building release binary from {}", source.display());
    let status = Command::new("cargo")
        .args([
            "build",
            "--locked",
            "--release",
            "-p",
            "vigilo",
            "--target-dir",
        ])
        .arg(&output)
        .current_dir(&source)
        .status()
        .context("run nested cargo build --locked")?;
    if !status.success() {
        bail!("release build failed with {status}");
    }

    let executable_name = if cfg!(windows) {
        "vigilo.exe"
    } else {
        "vigilo"
    };
    let executable = output.join("release").join(executable_name);
    if !executable.is_file() {
        bail!("release build did not produce {}", executable.display());
    }
    let capabilities = verify_capabilities(&executable)?;

    let assets = output.join("setup-assets");
    let migrations_source = source.join("vigilo/migrations");
    let wit_source = source.join("vigilo/wit");
    copy_tree(&migrations_source, &assets.join("migrations"))?;
    copy_tree(&wit_source, &assets.join("vigilo/wit"))?;
    let evaluator_source = source.join("evaluators/sentiment-basic-en");
    let evaluator_target = output.join("evaluator-target");
    let evaluator_status = Command::new("cargo")
        .args(["build", "--locked", "--release", "--manifest-path"])
        .arg(evaluator_source.join("Cargo.toml"))
        .args(["--target", "wasm32-wasip2", "--target-dir"])
        .arg(&evaluator_target)
        .current_dir(&source)
        .status()
        .context("build frozen evaluator fixture")?;
    if !evaluator_status.success() {
        bail!("frozen evaluator fixture build failed with {evaluator_status}");
    }
    let evaluator_asset = assets.join("evaluators/sentiment-basic-en");
    copy_tree(&evaluator_source, &evaluator_asset)?;
    isolate_copied_crate(&evaluator_asset)?;
    let evaluator_wasm = evaluator_target.join("wasm32-wasip2/release/sentiment_basic_en.wasm");
    let evaluator_asset_wasm =
        evaluator_asset.join("target/wasm32-wasip2/release/sentiment_basic_en.wasm");
    fs::create_dir_all(
        evaluator_asset_wasm
            .parent()
            .context("evaluator asset parent")?,
    )?;
    fs::write(&evaluator_asset_wasm, fs::read(&evaluator_wasm)?).with_context(|| {
        format!(
            "snapshot evaluator fixture {} to {}",
            evaluator_wasm.display(),
            evaluator_asset_wasm.display()
        )
    })?;
    materialize_standalone_metadata(&evaluator_asset)?;
    let migrations_digest = digest_tree(&migrations_source)?;
    let evaluator_abi_digest = digest_tree(&wit_source)?;
    let evaluator_fixture_digest = setup_asset_digest("evaluator-fixture", &evaluator_asset)?;
    if migrations_digest != digest_tree(&assets.join("migrations"))?
        || evaluator_abi_digest != digest_tree(&assets.join("vigilo/wit"))?
    {
        bail!("setup asset snapshot verification failed");
    }

    let cargo_lock = source.join("Cargo.lock");
    let dependency_tree = command_output(
        &source,
        "cargo",
        &[
            "tree",
            "--locked",
            "-p",
            "vigilo",
            "--edges",
            "normal,build",
        ],
    )?;
    let rustc = command_output(&source, "rustc", &["-vV"])?;
    let cargo = command_output(&source, "cargo", &["-V"])?;
    let target = rustc
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .unwrap_or("unknown")
        .to_owned();
    let source_commit = optional_command_output(&source, "git", &["rev-parse", "HEAD"]);
    let source_dirty = optional_command_output(&source, "git", &["status", "--porcelain"])
        .is_some_and(|output| !output.trim().is_empty());

    let manifest = BuildManifest {
        schema_id: BUILD_SCHEMA.into(),
        created_at: Utc::now().to_rfc3339(),
        executable_name: executable_name.into(),
        executable_digest: digest_file(&executable)?,
        executable_bytes: executable.metadata()?.len(),
        source_commit,
        source_dirty,
        source_label: source.display().to_string(),
        cargo_lock_digest: digest_file(&cargo_lock)?,
        dependency_tree_digest: blake3::hash(dependency_tree.as_bytes())
            .to_hex()
            .to_string(),
        migrations_digest: migrations_digest.clone(),
        evaluator_abi_digest: evaluator_abi_digest.clone(),
        rustc,
        cargo,
        target,
        profile: "release".into(),
        capabilities,
        setup_assets: vec![
            SetupAsset {
                name: "migrations".into(),
                relative_path: "setup-assets/migrations".into(),
                digest: migrations_digest,
            },
            SetupAsset {
                name: "evaluator-wit".into(),
                relative_path: "setup-assets/vigilo/wit".into(),
                digest: evaluator_abi_digest,
            },
            SetupAsset {
                name: "evaluator-fixture".into(),
                relative_path: "setup-assets/evaluators/sentiment-basic-en".into(),
                digest: evaluator_fixture_digest,
            },
        ],
        extra: BTreeMap::new(),
    };
    let manifest_path = output.join("build-manifest.json");
    atomic_json(&manifest_path, &manifest)?;
    println!("Build manifest: {}", manifest_path.display());
    println!("Executable:     {}", executable.display());
    println!("Digest:         {}", manifest.executable_digest);
    Ok(EXIT_PASS)
}

/// Digests immutable setup content while excluding Cargo's mutable rustc cache.
///
/// Evaluator publication runs `cargo metadata`, which may rewrite
/// `target/.rustc_info.json` for the current environment. That cache does not
/// affect the frozen source, lockfile, configuration, or Wasm artifact.
pub(crate) fn setup_asset_digest(name: &str, path: &Path) -> Result<String> {
    if name == "evaluator-fixture" {
        digest_tree_without(path, &[Path::new(EVALUATOR_METADATA_CACHE)])
    } else {
        digest_tree(path)
    }
}

/// Prevents a copied evaluator beneath the repository target tree from
/// inheriting the source workspace during later `evaluator publish` commands.
fn isolate_copied_crate(crate_root: &Path) -> Result<()> {
    let manifest = crate_root.join("Cargo.toml");
    let mut content = fs::read_to_string(&manifest)
        .with_context(|| format!("read copied evaluator manifest {}", manifest.display()))?;
    let parsed: toml::Value = toml::from_str(&content)?;
    if parsed.get("workspace").is_none() {
        content.push_str("\n[workspace]\n");
        fs::write(&manifest, content)
            .with_context(|| format!("isolate copied evaluator {}", manifest.display()))?;
    }
    Ok(())
}

/// Resolves the standalone crate after its frozen Wasm has been placed.
///
/// This intentionally matches the full Cargo metadata request used by evaluator
/// publication. It creates the lockfile and target rustc cache before the asset
/// digest is recorded, so publication cannot mutate an otherwise valid snapshot.
fn materialize_standalone_metadata(crate_root: &Path) -> Result<()> {
    let metadata_status = Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--manifest-path"])
        .arg(crate_root.join("Cargo.toml"))
        .stdout(Stdio::null())
        .status()
        .context("materialize standalone evaluator metadata")?;
    if !metadata_status.success() {
        bail!("standalone evaluator metadata failed with {metadata_status}");
    }
    Ok(())
}

/// Probes supported CLI boundaries and returns the capabilities recorded in the snapshot.
fn verify_capabilities(executable: &Path) -> Result<Vec<String>> {
    verify_help(executable, &["--help"], &["Usage:", "Commands:"])?;
    verify_help(
        executable,
        &["run", "create", "--help"],
        &["profile-file", "dataset-file"],
    )?;
    verify_help(executable, &["coordinator", "once", "--help"], &["Usage:"])?;
    verify_help(executable, &["worker", "once", "--help"], &["Usage:"])?;
    verify_help(executable, &["coordinator", "start", "--help"], &["Usage:"])?;
    verify_help(
        executable,
        &["worker", "start", "--help"],
        &["max-inflight-chunks"],
    )?;
    verify_help(
        executable,
        &["run", "export", "--help"],
        &["batch-size", "format"],
    )?;
    verify_help(
        executable,
        &["shard", "move", "--help"],
        &["--alias", "RUN_SHARD"],
    )?;
    verify_help(
        executable,
        &["rebalance", "plan", "--help"],
        &["--max-items", "--to"],
    )?;
    Ok(vec![
        "startup.cli-help.v1".into(),
        "run.create.v1".into(),
        "coordinator.dispatch.v1".into(),
        "worker.execute-wasm.v1".into(),
        "system.lifecycle.v1".into(),
        "system.reliability.v1".into(),
        "run.admin.v1".into(),
        "shard.admin.v1".into(),
    ])
}

/// Requires a help command to exit successfully and contain every frozen signature.
fn verify_help(executable: &Path, arguments: &[&str], signatures: &[&str]) -> Result<()> {
    let args = arguments
        .iter()
        .map(|value| (*value).to_owned())
        .collect::<Vec<_>>();
    let outcome = execute_process(&ProcessSpec {
        program: executable,
        args: &args,
        current_dir: None,
        env: &[],
        timeout: Duration::from_secs(10),
        stdout_limit: 256 * 1024,
        stderr_limit: 256 * 1024,
    })?;
    let stdout = outcome.stdout.text();
    if outcome.timed_out
        || outcome.exit_code != Some(0)
        || signatures
            .iter()
            .any(|signature| !stdout.contains(signature))
    {
        bail!("release binary does not satisfy `{}`", arguments.join(" "));
    }
    Ok(())
}

/// Runs a provenance command in the source tree and returns trimmed standard output.
fn command_output(source: &Path, program: &str, args: &[&str]) -> Result<String> {
    let output = Command::new(program)
        .args(args)
        .current_dir(source)
        .output()
        .with_context(|| format!("run {program} {}", args.join(" ")))?;
    if !output.status.success() {
        bail!("{program} {} failed with {}", args.join(" "), output.status);
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Runs a best-effort provenance command whose absence must not invalidate a build.
fn optional_command_output(source: &Path, program: &str, args: &[&str]) -> Option<String> {
    command_output(source, program, args).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn help_fixture() -> (PathBuf, Vec<&'static str>) {
        (
            std::env::current_exe().unwrap(),
            vec![
                "--exact",
                "perf::process::tests::subprocess_fixture",
                "--nocapture",
            ],
        )
    }

    #[test]
    fn help_probe_requires_success_and_every_signature() {
        let (program, arguments) = help_fixture();
        assert!(verify_help(&program, &arguments, &["Usage:", "Commands:"]).is_ok());
        assert!(verify_help(&program, &arguments, &["missing"]).is_err());
    }

    #[test]
    fn command_probes_capture_required_and_optional_tools() {
        let root = crate::perf::artifact::workspace_root().unwrap();
        assert!(
            command_output(&root, "rustc", &["-V"])
                .unwrap()
                .starts_with("rustc ")
        );
        assert!(optional_command_output(&root, "missing-vigilo-test-tool", &[]).is_none());
    }

    #[test]
    fn copied_evaluator_is_a_standalone_workspace() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(
            directory.path().join("Cargo.toml"),
            "[package]\nname = \"fixture\"\nversion = \"0.1.0\"\n",
        )
        .unwrap();
        isolate_copied_crate(directory.path()).unwrap();
        let content = fs::read_to_string(directory.path().join("Cargo.toml")).unwrap();
        assert!(content.ends_with("[workspace]\n"));

        isolate_copied_crate(directory.path()).unwrap();
        assert_eq!(
            fs::read_to_string(directory.path().join("Cargo.toml"))
                .unwrap()
                .matches("[workspace]")
                .count(),
            1
        );
    }

    #[test]
    fn standalone_metadata_is_materialized_before_snapshot_digest() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(
            directory.path().join("Cargo.toml"),
            "[package]\nname = \"fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\n[lib]\ncrate-type = [\"cdylib\"]\n\n[workspace]\n",
        )
        .unwrap();
        fs::create_dir(directory.path().join("src")).unwrap();
        fs::write(directory.path().join("src/lib.rs"), "").unwrap();
        let wasm = directory
            .path()
            .join("target/wasm32-wasip2/release/fixture.wasm");
        fs::create_dir_all(wasm.parent().unwrap()).unwrap();
        fs::write(&wasm, b"frozen-wasm").unwrap();

        materialize_standalone_metadata(directory.path()).unwrap();
        assert!(directory.path().join("Cargo.lock").is_file());
        assert!(directory.path().join("target/.rustc_info.json").is_file());
        let first = setup_asset_digest("evaluator-fixture", directory.path()).unwrap();

        materialize_standalone_metadata(directory.path()).unwrap();
        fs::write(
            directory.path().join(EVALUATOR_METADATA_CACHE),
            "different rustc cache",
        )
        .unwrap();
        assert_eq!(
            setup_asset_digest("evaluator-fixture", directory.path()).unwrap(),
            first
        );

        fs::write(&wasm, b"changed-wasm").unwrap();
        assert_ne!(
            setup_asset_digest("evaluator-fixture", directory.path()).unwrap(),
            first
        );
    }
}
