//! Official evaluator collection discovery and installation.
//!
//! GitHub Releases supplies a bounded catalog and immutable portable archives.
//! Catalogs select versions; the existing host and registry enforce artifact/ABI
//! validation and immutable publication. Installation is incremental: a failure
//! leaves completed publications intact and rerunning safely resumes. Downloads
//! are sequential and temporary files are removed on success or failure.

use std::{
    collections::HashSet,
    time::Duration,
};

use anyhow::{
    Context as _,
    ensure,
};
use serde::Deserialize;
use sha2::{
    Digest,
    Sha256,
};
use tokio::io::AsyncWriteExt;
use tracing::info;

use crate::{
    context::Context,
    evaluator_abi::resolve_declaration,
    evaluator_package::Wit,
};

const API: &str = "https://api.github.com/repos/agent-vigilo/vigilo";
const MAX_ARCHIVE: u64 = 66 * 1024 * 1024;
const MAX_CATALOG: u64 = 4 * 1024 * 1024;
const MAX_RELEASES: u32 = 10;

/// Validates a canonical UTC-date collection identifier for CLI and release tags.
pub(crate) fn parse_collection(value: &str) -> Result<String, String> {
    collection_key(value)
        .map(|_| value.to_owned())
        .map_err(|e| e.to_string())
}

fn collection_key(value: &str) -> anyhow::Result<(chrono::NaiveDate, u32)> {
    let (date, sequence) = value.rsplit_once('.').context("expected YYYY.MM.DD.N")?;
    ensure!(
        date.len() == 10
            && date
                .get(..4)
                .is_some_and(|year| year.bytes().all(|b| b.is_ascii_digit()) && year != "0000"),
        "expected positive four-digit year"
    );
    let date = chrono::NaiveDate::parse_from_str(date, "%Y.%m.%d")?;
    let sequence: u32 = sequence.parse()?;
    ensure!(
        sequence > 0 && value == format!("{}.{sequence}", date.format("%Y.%m.%d")),
        "expected canonical YYYY.MM.DD.N with N >= 1"
    );
    Ok((date, sequence))
}

#[derive(Deserialize)]
struct Release {
    tag_name: String,
    draft: bool,
    prerelease: bool,
    assets: Vec<Asset>,
}

#[derive(Deserialize)]
struct Asset {
    name: String,
    size: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(test, derive(serde::Serialize))]
struct Catalog {
    format: String,
    collection: String,
    evaluators: Vec<Entry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(test, derive(serde::Serialize))]
struct Entry {
    name: String,
    version: String,
    asset: String,
    size_bytes: u64,
    sha256: String,
    artifact_blake3: String,
    wit: Wit,
}

fn valid_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

impl Catalog {
    /// Rejects malformed catalogs before deciding whether their ABI is supported.
    fn validate(&self, collection: &str) -> anyhow::Result<()> {
        ensure!(
            self.format == "vigilo/core-evaluators/v1" && self.collection == collection,
            "catalog format or collection mismatch"
        );
        ensure!(
            !self.evaluators.is_empty() && self.evaluators.len() <= 1000,
            "catalog must contain 1..=1000 evaluators"
        );
        let mut names = HashSet::new();
        for entry in &self.evaluators {
            ensure!(
                !entry.name.is_empty()
                    && entry
                        .name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
                "invalid evaluator name"
            );
            cargo_metadata::semver::Version::parse(&entry.version)?;
            ensure!(names.insert(&entry.name), "duplicate evaluator name");
            ensure!(
                entry.asset == format!("{}-{}.tar.gz", entry.name, entry.version),
                "unexpected evaluator asset name"
            );
            ensure!(
                entry.size_bytes > 0 && entry.size_bytes <= MAX_ARCHIVE,
                "invalid evaluator archive size"
            );
            ensure!(
                valid_hash(&entry.sha256)
                    && valid_hash(&entry.artifact_blake3)
                    && valid_hash(&entry.wit.contract_hash),
                "invalid catalog checksum"
            );
        }
        Ok(())
    }

    fn compatible(&self) -> anyhow::Result<bool> {
        let mut supported = true;
        for entry in &self.evaluators {
            let wit = &entry.wit;
            match resolve_declaration(&wit.package, &wit.world, &wit.interface, &wit.version) {
                Ok(adapter) => ensure!(
                    adapter.identity().contract_hash == wit.contract_hash,
                    "catalog changes a supported WIT contract"
                ),
                Err(_) => supported = false,
            }
        }
        Ok(supported)
    }
}

fn asset_url(collection: &str, asset: &str) -> anyhow::Result<reqwest::Url> {
    let mut url = reqwest::Url::parse("https://github.com/agent-vigilo/vigilo/releases/download/")?;
    url.path_segments_mut()
        .map_err(|_| anyhow::anyhow!("invalid release URL"))?
        .pop_if_empty()
        .push(&format!("evaluators/{collection}"))
        .push(asset);
    Ok(url)
}

async fn response(
    client: &reqwest::Client,
    url: reqwest::Url,
    api: bool,
) -> anyhow::Result<reqwest::Response> {
    let mut request = client.get(url);
    if api && let Ok(token) = std::env::var("VIGILO_GITHUB_TOKEN") {
        request = request.bearer_auth(token);
    }
    Ok(request
        .send()
        .await
        .context("request official evaluator release")?
        .error_for_status()?)
}

async fn bounded_json<T: serde::de::DeserializeOwned>(
    mut response: reqwest::Response,
    limit: u64,
) -> anyhow::Result<T> {
    ensure!(
        response.content_length().is_none_or(|size| size <= limit),
        "release metadata exceeds byte limit"
    );
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            (bytes.len() as u64).saturating_add(chunk.len() as u64) <= limit,
            "release metadata exceeds byte limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn collection_releases(releases: Vec<Release>) -> anyhow::Result<Vec<Release>> {
    let mut candidates = Vec::new();
    for release in releases {
        if release.draft || release.prerelease {
            continue;
        }
        if let Some(collection) = release.tag_name.strip_prefix("evaluators/") {
            candidates.push((collection_key(collection)?, release));
        }
    }
    candidates.sort_by_key(|a| std::cmp::Reverse(a.0));
    Ok(candidates.into_iter().map(|(_, release)| release).collect())
}

/// Resolves release metadata independently of transport so selection stays testable offline.
async fn discover<F, Fut>(pin: Option<&str>, mut fetch: F) -> anyhow::Result<Catalog>
where
    F: FnMut(reqwest::Url, bool) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<reqwest::Response>>,
{
    let releases = if let Some(pin) = pin {
        parse_collection(pin).map_err(anyhow::Error::msg)?;
        let mut url = reqwest::Url::parse(&format!("{API}/releases/tags/"))?;
        url.path_segments_mut()
            .unwrap()
            .pop_if_empty()
            .push(&format!("evaluators/{pin}"));
        vec![bounded_json(fetch(url, true).await?, MAX_CATALOG).await?]
    } else {
        let mut releases = Vec::new();
        for page in 1..=MAX_RELEASES {
            let url = format!("{API}/releases?per_page=100&page={page}").parse()?;
            let batch: Vec<Release> =
                bounded_json(fetch(url, true).await?, 16 * 1024 * 1024).await?;
            let finished = batch.len() < 100;
            releases.extend(batch);
            if finished {
                break;
            }
            ensure!(
                page < MAX_RELEASES,
                "release discovery limit reached; pin --evaluators"
            );
        }
        releases
    };
    for release in collection_releases(releases)? {
        let collection = release.tag_name.strip_prefix("evaluators/").unwrap();
        ensure!(
            pin.is_none_or(|pin| pin == collection),
            "release tag does not match pin"
        );
        let assets: Vec<_> = release
            .assets
            .iter()
            .filter(|a| a.name == "catalog.json")
            .collect();
        ensure!(
            assets.len() == 1 && assets[0].size <= MAX_CATALOG,
            "release must contain one bounded catalog.json"
        );
        let catalog: Catalog = bounded_json(
            fetch(asset_url(collection, "catalog.json")?, false).await?,
            MAX_CATALOG,
        )
        .await?;
        catalog.validate(collection)?;
        for entry in &catalog.evaluators {
            ensure!(
                release
                    .assets
                    .iter()
                    .filter(|a| a.name == entry.asset && a.size == entry.size_bytes)
                    .count()
                    == 1,
                "catalog archive is missing or has a different size"
            );
        }
        if catalog.compatible()? {
            return Ok(catalog);
        }
        ensure!(
            pin.is_none(),
            "pinned collection requires an unsupported evaluator ABI"
        );
    }
    anyhow::bail!(
        "no compatible official evaluator collection found; use --skip-evaluators for database-only setup"
    )
}

/// Installs the selected collection through the same validation/publication path as the CLI.
pub(crate) async fn install(context: &Context, pin: Option<&str>) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .https_only(true)
        .user_agent(concat!("vigilo/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(120))
        .build()?;
    let catalog = discover(pin, |url, api| response(&client, url, api)).await?;
    info!(collection = %catalog.collection, "installing official core evaluators");
    for entry in catalog.evaluators {
        install_entry(context, &client, &catalog.collection, &entry)
            .await
            .with_context(|| {
                format!(
                    "install vigilo/{}:{} from {}",
                    entry.name, entry.version, catalog.collection
                )
            })?;
    }
    info!(collection = %catalog.collection, "core evaluator collection installed");
    Ok(())
}

async fn install_entry(
    context: &Context,
    client: &reqwest::Client,
    collection: &str,
    entry: &Entry,
) -> anyhow::Result<()> {
    let db = context.dbr().await?.control().await?;
    if let Some(existing) =
        crate::db::tables::evaluators::select_evaluator(db, "vigilo", &entry.name, &entry.version)
            .await?
    {
        ensure!(
            existing.content_hash == entry.artifact_blake3
                && existing.abi_contract_hash == entry.wit.contract_hash
                && existing.interface_name
                    == format!("{}/{}", entry.wit.package, entry.wit.interface)
                && existing.interface_version == entry.wit.version
                && existing.wit_world == entry.wit.world
                && existing.abi_adapter
                    == resolve_declaration(
                        &entry.wit.package,
                        &entry.wit.world,
                        &entry.wit.interface,
                        &entry.wit.version
                    )?
                    .identity()
                    .adapter,
            "installed evaluator conflicts with catalog"
        );
        info!(name = %entry.name, version = %entry.version, "core evaluator already installed");
        return Ok(());
    }
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("package.tar.gz");
    let response = response(client, asset_url(collection, &entry.asset)?, false).await?;
    download(response, &path, entry).await?;
    publish_archive(context, path, entry).await
}

/// Validates downloaded package identity against its catalog before registry writes.
async fn publish_archive(
    context: &Context,
    path: std::path::PathBuf,
    entry: &Entry,
) -> anyhow::Result<()> {
    let component = context
        .wasm()
        .await?
        .prepare_evaluator(path, "release".into())?;
    verify_component(&component, entry)?;
    crate::evaluators::publish_prepared(context, component).await?;
    Ok(())
}

/// Streams a bounded archive to an owned temporary path and verifies its checksum.
async fn download(
    mut response: reqwest::Response,
    path: &std::path::Path,
    entry: &Entry,
) -> anyhow::Result<()> {
    let mut file = tokio::fs::File::create(path).await?;
    ensure!(
        response
            .content_length()
            .is_none_or(|size| size == entry.size_bytes),
        "archive content length mismatch"
    );
    let mut digest = Sha256::new();
    let mut size = 0u64;
    while let Some(chunk) = response.chunk().await? {
        size = size
            .checked_add(chunk.len() as u64)
            .context("archive size overflow")?;
        ensure!(size <= entry.size_bytes, "archive exceeds declared size");
        digest.update(&chunk);
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    drop(file);
    ensure!(
        size == entry.size_bytes && format!("{:x}", digest.finalize()) == entry.sha256,
        "archive checksum or size mismatch"
    );
    Ok(())
}

fn verify_component(
    component: &crate::context::wasm::Component,
    entry: &Entry,
) -> anyhow::Result<()> {
    ensure!(
        component.name == entry.name
            && component.version == entry.version
            && component.wasm_hash == entry.artifact_blake3
            && component.abi.package == entry.wit.package
            && component.abi.world == entry.wit.world
            && component.abi.interface == entry.wit.interface
            && component.abi.version == entry.wit.version
            && component.abi.contract_hash == entry.wit.contract_hash,
        "package does not match catalog"
    );
    Ok(())
}

#[cfg(test)]
mod tests;
