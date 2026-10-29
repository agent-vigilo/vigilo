//! Records the resolved host runtime version at compile time. Dependency metadata
//! is read offline and locked; the build neither updates resolution nor embeds
//! the workspace lockfile in the executable.

mod build_support;

use anyhow::Context;
use cargo_metadata::MetadataCommand;

fn main() -> anyhow::Result<()> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let target = std::env::var("TARGET")?;
    let metadata = MetadataCommand::new()
        .manifest_path(format!("{manifest_dir}/Cargo.toml"))
        .other_options(vec![
            "--locked".into(),
            "--offline".into(),
            "--filter-platform".into(),
            target,
        ])
        .exec()
        .context("resolve Wasmtime build identity from Cargo metadata")?;
    let version = build_support::wasmtime_version(&metadata)?;

    // Directory tracking also rebuilds the embedded set when migrations are added.
    println!("cargo:rerun-if-changed=migrations");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=build_support.rs");
    // Workspace manifests and patches can change which dependency is selected.
    for package in metadata.workspace_packages() {
        println!("cargo:rerun-if-changed={}", package.manifest_path);
    }
    for name in ["Cargo.toml", "Cargo.lock"] {
        println!(
            "cargo:rerun-if-changed={}",
            metadata.workspace_root.join(name)
        );
    }
    println!("cargo:rustc-env=VIGILO_WASMTIME_VERSION={version}");
    Ok(())
}
