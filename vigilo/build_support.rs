//! Selects Vigilo's direct runtime from Cargo's resolved graph. Other packages
//! may use different Wasmtime versions; their identities must not be reported
//! as the host's runtime. Missing or ambiguous resolution fails the build.

use anyhow::{
    Context,
    ensure,
};
use cargo_metadata::{
    DependencyKind,
    Metadata,
};

/// Returns the exact normal Wasmtime dependency of the requested root package.
pub fn wasmtime_version(metadata: &Metadata) -> anyhow::Result<String> {
    let root = metadata.root_package().context("missing root package")?;
    let resolve = metadata
        .resolve
        .as_ref()
        .context("missing resolved graph")?;
    let node = resolve
        .nodes
        .iter()
        .find(|node| node.id == root.id)
        .context("missing root dependency node")?;
    let mut dependencies = node.deps.iter().filter(|dependency| {
        dependency.name == "wasmtime"
            && dependency
                .dep_kinds
                .iter()
                .any(|kind| kind.kind == DependencyKind::Normal)
    });
    let dependency = dependencies
        .next()
        .context("missing normal Wasmtime dependency")?;
    ensure!(
        dependencies.next().is_none(),
        "ambiguous normal Wasmtime dependency"
    );
    let package = metadata
        .packages
        .iter()
        .find(|package| package.id == dependency.pkg)
        .context("missing resolved Wasmtime package")?;
    ensure!(
        package.name.as_ref() == "wasmtime",
        "runtime dependency does not resolve to Wasmtime"
    );
    Ok(package.version.to_string())
}

#[cfg(test)]
mod tests {
    use serde_json::{
        Value,
        json,
    };

    use super::*;

    fn metadata() -> Value {
        let package = |id: &str, name: &str, version: &str| {
            json!({
                "id": id, "name": name, "version": version,
                "dependencies": [], "targets": [], "features": {},
                "manifest_path": format!("/fixture/{id}/Cargo.toml")
            })
        };
        json!({
            "packages": [
                package("other-runtime", "wasmtime", "43.0.0"),
                package("vigilo", "vigilo", "0.1.0"),
                package("host-runtime", "wasmtime", "44.0.2")
            ],
            "workspace_members": ["vigilo"],
            "workspace_root": "/fixture", "target_directory": "/fixture/target",
            "version": 1,
            "resolve": {"root": "vigilo", "nodes": [{
                "id": "vigilo", "dependencies": ["host-runtime"],
                "deps": [{"name": "wasmtime", "pkg": "host-runtime",
                    "dep_kinds": [{"kind": null, "target": null}]}]
            }]}
        })
    }

    fn version(value: Value) -> anyhow::Result<String> {
        wasmtime_version(&serde_json::from_value(value)?)
    }

    #[test]
    fn reports_direct_resolved_runtime_instead_of_another_packages_version() {
        assert_eq!(version(metadata()).unwrap(), "44.0.2");
        let mut changed = metadata();
        changed["packages"][2]["version"] = json!("44.0.3");
        assert_eq!(version(changed).unwrap(), "44.0.3");
    }

    #[test]
    fn missing_resolution_fails_closed() {
        for field in ["resolve", "root", "nodes", "deps", "packages"] {
            let mut value = metadata();
            match field {
                "resolve" => value["resolve"] = Value::Null,
                "root" => value["resolve"]["root"] = Value::Null,
                "nodes" => value["resolve"]["nodes"] = json!([]),
                "deps" => value["resolve"]["nodes"][0]["deps"] = json!([]),
                _ => {
                    value["packages"].as_array_mut().unwrap().pop();
                }
            }
            assert!(version(value).is_err(), "accepted missing {field}");
        }
    }

    #[test]
    fn build_and_dev_dependencies_cannot_supply_the_host_identity() {
        for kind in ["build", "dev"] {
            let mut value = metadata();
            value["resolve"]["nodes"][0]["deps"][0]["dep_kinds"][0]["kind"] = json!(kind);
            assert!(
                version(value)
                    .unwrap_err()
                    .to_string()
                    .contains("missing normal")
            );
        }
    }

    #[test]
    fn ambiguous_or_wrong_runtime_fails_closed() {
        let mut value = metadata();
        let mut other = value["resolve"]["nodes"][0]["deps"][0].clone();
        other["pkg"] = json!("other-runtime");
        value["resolve"]["nodes"][0]["deps"]
            .as_array_mut()
            .unwrap()
            .push(other);
        assert!(
            version(value)
                .unwrap_err()
                .to_string()
                .contains("ambiguous")
        );

        let mut value = metadata();
        value["packages"][2]["name"] = json!("another-engine");
        assert!(
            version(value)
                .unwrap_err()
                .to_string()
                .contains("does not resolve")
        );
    }
}
