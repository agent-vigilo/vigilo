//! Resolves the host's direct Wasmtime identity from workspace metadata and its
//! lockfile. Dependency-free metadata locates the package without fetching source
//! archives. Lockfile edges supply exact versions and sources; ambiguous edges
//! fail closed rather than reporting another package's runtime.

use std::path::Path;

use anyhow::{
    Context,
    ensure,
};
use cargo_metadata::{
    DependencyKind,
    Metadata,
};
use serde::Deserialize;

#[derive(Deserialize)]
struct Lockfile {
    version: u32,
    package: Vec<LockedPackage>,
}

#[derive(Deserialize)]
struct LockedPackage {
    name: String,
    version: String,
    source: Option<String>,
    #[serde(default)]
    dependencies: Vec<String>,
}

/// Discover workspace packages without resolving or downloading dependency sources.
pub fn metadata_command(manifest: &Path) -> cargo_metadata::MetadataCommand {
    let mut command = cargo_metadata::MetadataCommand::new();
    command
        .manifest_path(manifest)
        .no_deps()
        .other_options(vec!["--locked".into(), "--offline".into()]);
    command
}

/// Selects the exact locked runtime for an explicit workspace/package manifest.
/// The host must declare one unconditional normal dependency named `wasmtime`.
/// Missing lock entries, unsupported formats and ambiguous resolutions are errors.
pub fn wasmtime_version(
    metadata: &Metadata,
    manifest: &Path,
    lockfile: &str,
) -> anyhow::Result<String> {
    let root = metadata
        .packages
        .iter()
        .find(|p| p.manifest_path.as_std_path() == manifest)
        .context("missing requested root package")?;
    let mut declarations = root.dependencies.iter().filter(|d| {
        d.kind == DependencyKind::Normal
            && d.target.is_none()
            && d.rename.as_deref().unwrap_or(&d.name) == "wasmtime"
    });
    let declaration = declarations
        .next()
        .context("missing unconditional normal Wasmtime dependency")?;
    ensure!(
        declarations.next().is_none(),
        "ambiguous normal Wasmtime dependency"
    );
    ensure!(
        declaration.name == "wasmtime",
        "runtime dependency does not resolve to Wasmtime"
    );

    ensure!(
        !declaration.optional,
        "optional Wasmtime dependency cannot identify the host runtime"
    );

    let lock: Lockfile = toml::from_str(lockfile).context("parse workspace Cargo.lock")?;
    ensure!(
        matches!(lock.version, 3 | 4),
        "unsupported Cargo.lock format"
    );
    let mut roots = lock.package.iter().filter(|p| {
        p.name == root.name.as_ref() && p.version == root.version.to_string() && p.source.is_none()
    });
    let locked_root = roots.next().context("missing locked root package")?;
    ensure!(roots.next().is_none(), "ambiguous locked root package");
    let mut candidates = Vec::new();
    for edge in &locked_root.dependencies {
        let mut parts = edge.split_whitespace();
        if parts.next() != Some("wasmtime") {
            continue;
        }
        let version = parts.next();
        let source = parts.next();
        ensure!(parts.next().is_none(), "invalid Wasmtime lockfile edge");
        let source = source
            .map(|s| {
                s.strip_prefix('(')
                    .and_then(|s| s.strip_suffix(')'))
                    .context("invalid Wasmtime lockfile source")
            })
            .transpose()?;
        let matches = lock
            .package
            .iter()
            .filter(|p| {
                p.name == "wasmtime"
                    && version.is_none_or(|v| p.version == v)
                    && source.is_none_or(|s| p.source.as_deref() == Some(s))
            })
            .collect::<Vec<_>>();
        ensure!(
            matches.len() == 1,
            "missing or ambiguous locked Wasmtime package"
        );
        let package = matches[0];
        if declaration.req.matches(
            &package
                .version
                .parse()
                .context("invalid locked Wasmtime version")?,
        ) {
            candidates.push(package);
        }
    }
    ensure!(
        candidates.len() == 1,
        "missing or ambiguous direct locked Wasmtime dependency"
    );
    Ok(candidates[0].version.clone())
}

#[cfg(test)]
mod tests {
    use serde_json::{
        Value,
        json,
    };

    use super::*;

    const MANIFEST: &str = "/fixture/vigilo/Cargo.toml";
    const LOCK: &str = r#"
version = 4
[[package]]
name = "vigilo"
version = "0.1.0"
dependencies = ["wasmtime 44.0.2"]
[[package]]
name = "wasmtime"
version = "43.0.0"
source = "registry+example"
[[package]]
name = "wasmtime"
version = "44.0.2"
source = "registry+example"
"#;

    fn metadata() -> Value {
        json!({"packages": [{"id":"vigilo", "name":"vigilo", "version":"0.1.0",
            "manifest_path":MANIFEST,"targets":[],"features":{},
            "dependencies":[{"name":"wasmtime", "req":"^44.0.0", "kind":null,
                "optional":false,"uses_default_features":true,"features":[],"target":null}]}],
            "workspace_members":["vigilo"], "workspace_root":"/fixture",
            "target_directory":"/fixture/target", "version":1, "resolve":null})
    }
    fn version(value: Value, lock: &str) -> anyhow::Result<String> {
        wasmtime_version(&serde_json::from_value(value)?, Path::new(MANIFEST), lock)
    }

    #[test]
    fn metadata_discovery_works_with_an_empty_cargo_cache() {
        let directory = tempfile::tempdir().unwrap();
        let manifest = directory.path().join("Cargo.toml");
        std::fs::write(&manifest, "[package]\nname = \"vigilo\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[lib]\npath = \"lib.rs\"\n[dependencies]\nwasmtime = \"44.0.0\"\n").unwrap();
        std::fs::write(directory.path().join("lib.rs"), "").unwrap();
        std::fs::write(directory.path().join("Cargo.lock"), LOCK).unwrap();
        let cache = directory.path().join("empty-cargo-home");
        std::fs::create_dir(&cache).unwrap();
        let metadata = metadata_command(&manifest)
            .env("CARGO_HOME", &cache)
            .exec()
            .unwrap();
        assert!(metadata.resolve.is_none());
        let actual_manifest = metadata.packages[0].manifest_path.as_std_path();
        assert_eq!(
            wasmtime_version(&metadata, actual_manifest, LOCK).unwrap(),
            "44.0.2"
        );
        assert!(
            !cache.join("registry/cache").exists(),
            "metadata fetched crate archives"
        );
        assert!(
            !cache.join("registry/src").exists(),
            "metadata unpacked dependency sources"
        );
        assert_eq!(
            std::fs::read_to_string(directory.path().join("Cargo.lock")).unwrap(),
            LOCK
        );
    }

    #[test]
    fn dependency_free_metadata_uses_exact_direct_locked_runtime() {
        assert_eq!(version(metadata(), LOCK).unwrap(), "44.0.2");
        assert_eq!(
            version(metadata(), &LOCK.replace("44.0.2", "44.0.3")).unwrap(),
            "44.0.3"
        );
        assert_eq!(
            version(metadata(), &LOCK.replace("version = 4", "version = 3")).unwrap(),
            "44.0.2"
        );
    }

    #[test]
    fn missing_malformed_or_ambiguous_lock_identity_fails_closed() {
        for lock in [
            String::new(),
            LOCK.replace("version = 4", "version = 99"),
            LOCK.replace("name = \"vigilo\"", "name = \"other\""),
            LOCK.replace("dependencies = [\"wasmtime 44.0.2\"]", "dependencies = []"),
            LOCK.replace("version = \"44.0.2\"", "version = \"45.0.0\""),
            LOCK.replace("wasmtime 44.0.2", "wasmtime"),
            format!(
                "{LOCK}\n[[package]]\nname = \"wasmtime\"\nversion = \"44.0.2\"\nsource = \"git+other\"\n"
            ),
        ] {
            assert!(version(metadata(), &lock).is_err(), "accepted {lock}");
        }
        let mut value = metadata();
        value["packages"] = json!([]);
        assert!(version(value, LOCK).is_err());
    }

    #[test]
    fn build_dev_conditional_and_wrong_named_dependencies_cannot_supply_identity() {
        for kind in ["build", "dev"] {
            let mut value = metadata();
            value["packages"][0]["dependencies"][0]["kind"] = json!(kind);
            assert!(version(value, LOCK).is_err());
        }
        let mut value = metadata();
        value["packages"][0]["dependencies"][0]["target"] = json!("cfg(windows)");
        assert!(version(value, LOCK).is_err());
        let mut value = metadata();
        value["packages"][0]["dependencies"][0]["name"] = json!("other");
        value["packages"][0]["dependencies"][0]["rename"] = json!("wasmtime");
        assert!(version(value, LOCK).is_err());
    }

    #[test]
    fn source_qualified_edges_and_standalone_packages_are_supported() {
        let lock = LOCK.replace("wasmtime 44.0.2", "wasmtime 44.0.2 (registry+example)")
            + "\n[[package]]\nname = \"wasmtime\"\nversion = \"44.0.2\"\nsource = \"git+other\"\n";
        let mut value = metadata();
        value["workspace_root"] = json!("/fixture/vigilo");
        assert_eq!(version(value, &lock).unwrap(), "44.0.2");
    }

    #[test]
    fn overlapping_build_and_normal_versions_are_not_guessed() {
        let lock = LOCK.replace(
            "dependencies = [\"wasmtime 44.0.2\"]",
            "dependencies = [\"wasmtime 44.0.2\", \"wasmtime 44.0.3\"]",
        ) + "\n[[package]]\nname = \"wasmtime\"\nversion = \"44.0.3\"\nsource = \"registry+example\"\n";
        assert!(version(metadata(), &lock).is_err());
    }
}
