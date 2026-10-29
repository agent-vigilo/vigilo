//! Evaluator source-package manifest loading.
//!
//! Evaluator packages carry a `Vigilo.toml` manifest next to their crate. This
//! module parses that manifest into the fields needed by CLI publishing and
//! evaluator build flows: package metadata, the WIT contract reference, and
//! named build profiles. Portable release manifests are owned by evaluator_package.

use std::{
    collections::HashMap,
    fs,
    path::Path,
};

use anyhow::anyhow;
use serde::Deserialize;

/// Package metadata from `Vigilo.toml`.
///
/// `manifest` is the evaluator manifest file path relative to the crate root.
/// Optional fields are carried through for registry/search metadata.
#[derive(Deserialize)]
pub(crate) struct Package {
    pub manifest: String,
    pub description: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub metadata: Option<toml::Value>,
}

/// Build artifact configuration for a named profile.
#[derive(Deserialize)]
pub(crate) struct Profile {
    /// Path to the compiled WebAssembly component for this profile.
    pub wasm: String,
}

fn default_false() -> bool {
    false
}

/// WIT contract reference declared by an evaluator crate.
///
/// These fields identify which immutable interface/world the evaluator
/// implements. Supported contracts are always verified during publication.
/// `strict` remains accepted for manifest compatibility but cannot weaken ABI
/// validation.
#[derive(Deserialize)]
pub(crate) struct Wit {
    pub path: String,
    pub world: String,
    pub package: String,
    pub version: String,
    pub interface: String,
    #[serde(default = "default_false")]
    pub strict: bool,
}

/// Parsed `Vigilo.toml` file.
#[derive(Deserialize)]
pub(crate) struct Manifest {
    pub package: Package,
    pub wit: Option<Wit>,
    profile: HashMap<String, Profile>,
}

impl Manifest {
    /// Returns a named profile or reports a manifest-scoped error.
    pub fn get_profile(&self, profile_name: &str) -> anyhow::Result<&Profile> {
        self.profile
            .get(profile_name)
            .ok_or(anyhow!("manifest profile {} not supported", profile_name))
    }
}

/// Reads and parses `Vigilo.toml` from an evaluator crate directory.
pub(crate) fn read_manifest(crate_path: &Path) -> anyhow::Result<Manifest> {
    let content = fs::read_to_string(crate_path.join("Vigilo.toml"))?;
    let manifest: Manifest = toml::from_str(&content)?;
    Ok(manifest)
}
