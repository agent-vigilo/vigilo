//! Portable evaluator release packages.
//!
//! A package contains only a strict, versioned Vigilo.toml and evaluator.wasm.
//! Readers enforce byte limits, reject archive links and unexpected paths, and
//! verify artifact and ABI identities before the host compiles anything. Archives
//! are read without extraction; writers publish deterministic archives atomically
//! without replacing existing releases. Source-package preparation stays in wasm.

use std::{
    fs,
    io::{
        Read,
        Write,
    },
    path::Path,
};

use anyhow::{
    Context,
    ensure,
};
use flate2::{
    Compression,
    GzBuilder,
    read::MultiGzDecoder,
};
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::Value;

use crate::{
    contracts::evaluator_abi::EvaluatorAbiIdentity,
    evaluator_abi::resolve_declaration,
};

const FORMAT: &str = "vigilo/evaluator-package/v1";
const MANIFEST: &str = "Vigilo.toml";
const WASM: &str = "evaluator.wasm";
const MAX_MANIFEST: u64 = 1024 * 1024;
const MAX_WASM: u64 = 64 * 1024 * 1024;
const MAX_ARCHIVE: u64 = MAX_MANIFEST + MAX_WASM + 1024 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format: String,
    package: Package,
    artifact: Artifact,
    wit: Wit,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Package {
    name: String,
    version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    metadata: toml::Table,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Artifact {
    path: String,
    blake3: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Wit {
    pub(crate) package: String,
    pub(crate) world: String,
    pub(crate) interface: String,
    pub(crate) version: String,
    pub(crate) contract_hash: String,
}

/// Verified package bytes and metadata, ready for host ABI compilation.
pub(crate) struct PackageInput {
    pub name: String,
    pub version: String,
    pub description: Option<String>,
    pub tags: Value,
    pub metadata: Value,
    pub abi: EvaluatorAbiIdentity,
    pub wasm: Vec<u8>,
}

/// Returns None only for a source directory, never for an invalid portable package.
pub(crate) fn load(path: &Path) -> anyhow::Result<Option<PackageInput>> {
    let (manifest, wasm) = if path.is_dir() {
        let manifest = read_file(&path.join(MANIFEST), MAX_MANIFEST)?;
        let value: toml::Value = toml::from_str(std::str::from_utf8(&manifest)?)?;
        if value.get("format").is_none() && value.get("artifact").is_none() {
            return Ok(None);
        }
        (manifest, read_file(&path.join(WASM), MAX_WASM)?)
    } else {
        read_archive(path)?
    };
    verify(&manifest, wasm).map(Some)
}

fn read_file(path: &Path, limit: u64) -> anyhow::Result<Vec<u8>> {
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("inspect {}", path.display()))?;
    ensure!(
        metadata.is_file(),
        "package member must be a regular file: {}",
        path.display()
    );
    ensure!(
        metadata.len() <= limit,
        "package member exceeds byte limit: {}",
        path.display()
    );
    read_bounded(fs::File::open(path)?, limit)
}

fn read_bounded(reader: impl Read, limit: u64) -> anyhow::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader.take(limit + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= limit,
        "package member exceeds byte limit"
    );
    Ok(bytes)
}

fn read_archive(path: &Path) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
    let file = fs::File::open(path)?;
    ensure!(
        file.metadata()?.is_file() && file.metadata()?.len() <= MAX_ARCHIVE,
        "archive exceeds byte limit or is not a regular file"
    );
    let decoder = MultiGzDecoder::new(file.take(MAX_ARCHIVE + 1));
    let mut archive = tar::Archive::new(decoder.take(MAX_ARCHIVE + 1));
    let mut manifest = None;
    let mut wasm = None;
    for entry in archive.entries()?.raw(true) {
        let mut entry = entry?;
        ensure!(
            entry.header().entry_type().is_file(),
            "archive members must be regular files"
        );
        let name = entry.path_bytes().into_owned();
        let (slot, limit) = match name.as_slice() {
            b"Vigilo.toml" => (&mut manifest, MAX_MANIFEST),
            b"evaluator.wasm" => (&mut wasm, MAX_WASM),
            _ => anyhow::bail!(
                "unexpected archive member; expected Vigilo.toml and evaluator.wasm at archive root"
            ),
        };
        ensure!(slot.is_none(), "duplicate archive member");
        ensure!(entry.size() <= limit, "archive member exceeds byte limit");
        *slot = Some(read_bounded(&mut entry, limit)?);
    }
    // Drain gzip to validate its checksum and reject trailing payloads or bombs.
    let mut reader = archive.into_inner();
    let mut buffer = [0; 8192];
    loop {
        let size = reader.read(&mut buffer)?;
        if size == 0 {
            break;
        }
        ensure!(
            buffer[..size].iter().all(|byte| *byte == 0),
            "unexpected trailing archive data"
        );
    }
    ensure!(reader.limit() > 0, "expanded archive exceeds byte limit");
    Ok((
        manifest.context("missing Vigilo.toml")?,
        wasm.context("missing evaluator.wasm")?,
    ))
}

fn verify(manifest: &[u8], wasm: Vec<u8>) -> anyhow::Result<PackageInput> {
    let manifest: Manifest = toml::from_str(std::str::from_utf8(manifest)?)?;
    ensure!(
        manifest.format == FORMAT,
        "unsupported evaluator package format"
    );
    ensure!(
        !manifest.package.name.is_empty()
            && manifest
                .package
                .name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
        "invalid evaluator package name"
    );
    cargo_metadata::semver::Version::parse(&manifest.package.version)
        .context("invalid evaluator package version")?;
    ensure!(
        manifest.artifact.path == WASM,
        "artifact path must be evaluator.wasm"
    );
    ensure!(
        blake3::hash(&wasm).to_hex().as_str() == manifest.artifact.blake3,
        "evaluator artifact hash mismatch"
    );
    let wit = manifest.wit;
    let abi = resolve_declaration(&wit.package, &wit.world, &wit.interface, &wit.version)?
        .identity()
        .clone();
    ensure!(
        abi.contract_hash == wit.contract_hash,
        "evaluator WIT contract hash mismatch"
    );
    Ok(PackageInput {
        name: manifest.package.name,
        version: manifest.package.version,
        description: manifest.package.description,
        tags: serde_json::to_value(manifest.package.tags)?,
        metadata: serde_json::to_value(manifest.package.metadata)?,
        abi,
        wasm,
    })
}

/// Writes the already-prepared, canonical artifact without mutating its bytes.
/// No existing output is replaced, including when validation or writing fails.
pub(crate) fn write(path: &Path, input: PackageInput) -> anyhow::Result<()> {
    ensure!(
        input.wasm.len() as u64 <= MAX_WASM,
        "evaluator artifact exceeds byte limit"
    );
    let manifest = Manifest {
        format: FORMAT.into(),
        package: Package {
            name: input.name,
            version: input.version,
            description: input.description,
            tags: serde_json::from_value(input.tags)?,
            metadata: serde_json::from_value(input.metadata)?,
        },
        artifact: Artifact {
            path: WASM.into(),
            blake3: blake3::hash(&input.wasm).to_hex().to_string(),
        },
        wit: Wit {
            package: input.abi.package,
            world: input.abi.world,
            interface: input.abi.interface,
            version: input.abi.version,
            contract_hash: input.abi.contract_hash,
        },
    };
    let manifest = toml::to_string_pretty(&manifest)?;
    ensure!(
        manifest.len() as u64 <= MAX_MANIFEST,
        "package manifest exceeds byte limit"
    );
    // Validate before creating an output file, including names and supported ABI.
    let wasm = verify(manifest.as_bytes(), input.wasm)?.wasm;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut output = tempfile::NamedTempFile::new_in(parent)?;
    {
        let encoder = GzBuilder::new()
            .mtime(0)
            .write(output.as_file_mut(), Compression::default());
        let mut archive = tar::Builder::new(encoder);
        for (name, bytes) in [(MANIFEST, manifest.as_bytes()), (WASM, wasm.as_slice())] {
            let mut header = tar::Header::new_ustar();
            header.set_size(bytes.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            archive.append_data(&mut header, name, bytes)?;
        }
        archive.into_inner()?.finish()?.flush()?;
    }
    output.as_file().sync_all()?;
    output.persist_noclobber(path).map_err(|e| e.error)?;
    Ok(())
}

#[cfg(test)]
mod tests;
