//! Records the resolved host runtime version at compile time. Dependency metadata
//! and the lockfile are read offline and locked. The build neither resolves
//! dependency sources nor updates resolution or embeds
//! the workspace lockfile in the executable.

mod build_support;

use anyhow::Context;

fn main() -> anyhow::Result<()> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let manifest = std::path::Path::new(&manifest_dir).join("Cargo.toml");
    let metadata = build_support::metadata_command(&manifest)
        .exec()
        .context("resolve Wasmtime build identity from Cargo metadata")?;
    let lockfile = std::fs::read_to_string(metadata.workspace_root.join("Cargo.lock"))
        .context("read workspace Cargo.lock for Wasmtime build identity")?;
    let version = build_support::wasmtime_version(&metadata, &manifest, &lockfile)?;

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
