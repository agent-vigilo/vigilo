//! Wasm evaluator runtime context.
//!
//! This module prepares evaluator artifacts for publishing, compiles registry
//! components, and executes evaluator tests inside a Wasmtime component-model
//! sandbox. Keep each adapter aligned with its versioned WIT contract; resource
//! limits, fuel, timeout, and log caps must be enforced for every invocation.

use std::{
    env::consts::ARCH,
    fs,
    hash::{
        DefaultHasher,
        Hash,
        Hasher,
    },
    io::ErrorKind,
    path::{
        Path,
        PathBuf,
    },
    sync::Arc,
    thread,
    time::{
        Duration,
        SystemTime,
    },
};

use cargo_metadata::MetadataCommand;
use cargo_toml::Manifest;
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::Value;
use tokio::sync::{
    OnceCell,
    OwnedSemaphorePermit,
    Semaphore,
};
use tracing::{
    debug,
    warn,
};
use wasmparser::{
    Parser,
    Payload,
};
use wasmtime::{
    Config as EngineConfig,
    Engine,
    Store,
    StoreLimits,
    StoreLimitsBuilder,
    component::ResourceTable,
};
use wasmtime_wasi::{
    WasiCtx,
    WasiCtxBuilder,
    WasiCtxView,
    WasiView,
};

use super::super::manifest::{
    Wit,
    read_manifest,
};
use crate::{
    contracts::{
        evaluator::{
            EvaluatorInput,
            EvaluatorOutput,
        },
        evaluator_abi::EvaluatorAbiIdentity,
    },
    evaluator_abi::{
        PreparedEvaluator,
        resolve_declaration,
    },
};

pub(crate) struct EvaluatorHost {
    table: ResourceTable,
    ctx: WasiCtx,
    limits: StoreLimits,
    max_log_message_bytes: usize,
    max_log_messages: u32,
    log_messages: u32,
}

impl WasiView for EvaluatorHost {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.ctx,
            table: &mut self.table,
        }
    }
}

impl EvaluatorHost {
    pub(crate) fn capped_log_message(&mut self, msg: String) -> Option<String> {
        if self.max_log_messages == 0 || self.log_messages >= self.max_log_messages {
            return None;
        }

        self.log_messages += 1;
        Some(truncate_utf8_bytes(msg, self.max_log_message_bytes))
    }
}

fn truncate_utf8_bytes(mut value: String, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value;
    }

    let mut end = max_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    value
}

struct PackageMetadata {
    name: String,
    version: String,
    target_dir: PathBuf,
    modified: SystemTime,
    description: Option<String>,
    tags: Vec<String>,
    metadata: Option<Value>,
}

struct EvaluatorMetadata {
    description: Option<String>,
    tags: Value,
    metadata: Value,
}

struct WitWorld {
    name: String,
    exports: Vec<String>,
}

struct WitDocument {
    package: String,
    version: Option<String>,
    worlds: Vec<WitWorld>,
}

#[derive(Debug)]
struct WitMetadata {
    abi: Option<EvaluatorAbiIdentity>,
}

const PACKAGE_METADATA_SECTION: &str = "vigilo.package";
const WASM_RUNTIME_NAME: &str = "wasmtime";

#[derive(Debug, Serialize, Deserialize)]
struct EmbeddedPackageMetadata {
    name: String,
    version: String,
}

/// Read custom section in wasm bytes.
///
/// Retrieves evaluator metadata from wasm bytes (e.g., name and version).
fn read_embedded_package_metadata(
    wasm_bytes: &[u8],
) -> anyhow::Result<Option<EmbeddedPackageMetadata>> {
    let mut metadata = None;
    let mut depth = 0;
    for payload in Parser::new(0).parse_all(wasm_bytes) {
        match payload? {
            Payload::Version { .. } => depth += 1,
            Payload::End(_) => depth -= 1,
            Payload::CustomSection(section)
                if depth == 1 && section.name() == PACKAGE_METADATA_SECTION =>
            {
                anyhow::ensure!(metadata.is_none(), "duplicate vigilo.package metadata");
                metadata = Some(serde_json::from_slice(section.data()).map_err(|err| {
                    anyhow::anyhow!(
                        "failed to decode {} metadata: {}",
                        PACKAGE_METADATA_SECTION,
                        err
                    )
                })?);
            }
            _ => {}
        }
    }

    Ok(metadata)
}

/// Push LEB128 (Little Endian Base 128) compressed value into vector buffer.
fn push_u32_leb128(buf: &mut Vec<u8>, mut value: u32) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;

        if value != 0 {
            byte |= 0x80;
        }

        buf.push(byte);

        if value == 0 {
            break;
        }
    }
}

/// Appends custom section of metadata to wasm bytes.
///
/// Used to attach evaluator metadata (e.g., name and version).
fn append_custom_section(
    wasm_bytes: &[u8],
    section_name: &str,
    section_data: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let mut payload = Vec::new();
    let section_name_len = u32::try_from(section_name.len())
        .map_err(|_| anyhow::anyhow!("custom section name too long"))?;
    push_u32_leb128(&mut payload, section_name_len);
    payload.extend_from_slice(section_name.as_bytes());
    payload.extend_from_slice(section_data);

    let payload_len = u32::try_from(payload.len())
        .map_err(|_| anyhow::anyhow!("custom section payload too long"))?;

    let mut out = Vec::with_capacity(wasm_bytes.len() + payload.len() + 8);
    out.extend_from_slice(wasm_bytes);
    out.push(0);
    push_u32_leb128(&mut out, payload_len);
    out.extend_from_slice(&payload);

    Ok(out)
}

/// Ensure that custom section evaluator metadata has been appended to wasm bytes.
fn ensure_embedded_package_metadata(
    wasm_bytes: Vec<u8>,
    package_name: &str,
    package_version: &str,
) -> anyhow::Result<Vec<u8>> {
    match read_embedded_package_metadata(&wasm_bytes)? {
        Some(existing) => {
            if existing.name != package_name || existing.version != package_version {
                anyhow::bail!(
                    "embedded {} mismatch (found {}@{}, expected {}@{})",
                    PACKAGE_METADATA_SECTION,
                    existing.name,
                    existing.version,
                    package_name,
                    package_version,
                );
            }

            Ok(wasm_bytes)
        }
        None => {
            let metadata = EmbeddedPackageMetadata {
                name: package_name.to_string(),
                version: package_version.to_string(),
            };
            let encoded = serde_json::to_vec(&metadata)?;
            let out = append_custom_section(&wasm_bytes, PACKAGE_METADATA_SECTION, &encoded)?;

            // verify append/read-back so we fail fast on malformed custom section writes.
            let embedded = read_embedded_package_metadata(&out)?.ok_or_else(|| {
                anyhow::anyhow!(
                    "failed to read back embedded {} metadata",
                    PACKAGE_METADATA_SECTION
                )
            })?;

            if embedded.name != package_name || embedded.version != package_version {
                anyhow::bail!(
                    "embedded {} mismatch after write (found {}@{}, expected {}@{})",
                    PACKAGE_METADATA_SECTION,
                    embedded.name,
                    embedded.version,
                    package_name,
                    package_version,
                );
            }

            Ok(out)
        }
    }
}

/// Parse wit file and return struct.
fn parse_wit_file(path: &PathBuf) -> anyhow::Result<WitDocument> {
    let content = fs::read_to_string(path)?;

    let mut package: Option<String> = None;
    let mut version: Option<String> = None;
    let mut worlds: Vec<WitWorld> = Vec::new();

    let mut current_world: Option<WitWorld> = None;
    let mut world_depth: i32 = 0;

    for raw in content.lines() {
        let line = raw.split("//").next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }

        if package.is_none() && line.starts_with("package ") && line.ends_with(';') {
            let body = line
                .trim_start_matches("package ")
                .trim_end_matches(';')
                .trim();

            if let Some((pkg, ver)) = body.split_once('@') {
                package = Some(pkg.trim().to_string());
                version = Some(ver.trim().to_string());
            } else {
                package = Some(body.to_string());
            }
            continue;
        }

        if current_world.is_none() && line.starts_with("world ") && line.contains('{') {
            let name_part = line
                .trim_start_matches("world ")
                .split('{')
                .next()
                .unwrap_or("")
                .trim();

            if !name_part.is_empty() {
                current_world = Some(WitWorld {
                    name: name_part.to_string(),
                    exports: Vec::new(),
                });
                world_depth = 1;
                continue;
            }
        }

        if let Some(world) = current_world.as_mut() {
            if line.starts_with("export ") && line.ends_with(';') {
                let export_name = line
                    .trim_start_matches("export ")
                    .trim_end_matches(';')
                    .trim();
                if !export_name.is_empty() {
                    world.exports.push(export_name.to_string());
                }
            }

            let opens = line.chars().filter(|c| *c == '{').count() as i32;
            let closes = line.chars().filter(|c| *c == '}').count() as i32;
            world_depth += opens - closes;

            if world_depth <= 0 {
                let finished = current_world.take().expect("world exists");
                worlds.push(finished);
                world_depth = 0;
            }
        }
    }

    let package = package.ok_or_else(|| {
        anyhow::anyhow!("missing package declaration in WIT file {}", path.display())
    })?;

    Ok(WitDocument {
        package,
        version,
        worlds,
    })
}

/// Resolve wit file metadata and return as struct.
fn resolve_wit_metadata(
    package_path: &Path,
    manifest_wit: Option<&Wit>,
) -> anyhow::Result<WitMetadata> {
    let Some(wit) = manifest_wit else {
        return Ok(WitMetadata { abi: None });
    };

    let wit_path = package_path.join(&wit.path);
    let parsed = parse_wit_file(&wit_path)?;

    let world = parsed.worlds.iter().find(|w| w.name == wit.world);

    let has_interface_export = world
        .map(|w| w.exports.iter().any(|e| e == &wit.interface))
        .unwrap_or(false);

    let abi_identity = resolve_declaration(&wit.package, &wit.world, &wit.interface, &wit.version)?
        .identity()
        .clone();
    let package_matches = parsed.package == wit.package;
    let version_matches = parsed.version.as_deref() == Some(wit.version.as_str());
    let world_matches = world.is_some();
    let interface_matches = has_interface_export;

    if !wit.strict {
        warn!("[wit].strict=false is deprecated; supported evaluator ABIs are always verified");
    }
    if !package_matches {
        anyhow::bail!(
            "WIT package mismatch (config={}, file={})",
            wit.package,
            parsed.package,
        );
    }
    if !version_matches {
        anyhow::bail!(
            "WIT version mismatch (config={}, file={})",
            wit.version,
            parsed.version.unwrap_or_else(|| "<missing>".to_string()),
        );
    }
    if !world_matches {
        anyhow::bail!(
            "WIT world '{}' not found in {}",
            wit.world,
            wit_path.display(),
        );
    }
    if !interface_matches {
        anyhow::bail!(
            "WIT interface '{}' is not exported by world '{}'",
            wit.interface,
            wit.world,
        );
    }

    Ok(WitMetadata {
        abi: Some(abi_identity),
    })
}

/// Convert toml value to serde value.
fn value_from_toml(value: &toml::Value) -> anyhow::Result<Value> {
    serde_json::to_value(value)
        .map_err(|err| anyhow::anyhow!("failed to encode TOML value to JSON: {}", err))
}

/// Resolve evaluator metadata and return as struct.
fn resolve_evaluator_metadata(
    package: &super::super::manifest::Package,
    cargo: &PackageMetadata,
) -> anyhow::Result<EvaluatorMetadata> {
    let description = package
        .description
        .clone()
        .or_else(|| cargo.description.clone());

    let tags = if package.tags.is_empty() {
        cargo.tags.clone()
    } else {
        package.tags.clone()
    };

    let metadata = match &package.metadata {
        Some(value) => value_from_toml(value)?,
        None => cargo
            .metadata
            .clone()
            .unwrap_or_else(|| Value::Object(Default::default())),
    };

    Ok(EvaluatorMetadata {
        description,
        tags: Value::Array(tags.into_iter().map(Value::String).collect()),
        metadata,
    })
}

/// Get metadata from package manifest defined in Vigilo.toml.
fn get_package_metadata(
    package_path: &Path,
    manifest_file: &str,
) -> anyhow::Result<PackageMetadata> {
    match manifest_file {
        "Cargo.toml" => {
            let manifest_path = package_path.join(manifest_file);
            let manifest_content = fs::read_to_string(&manifest_path)?;
            let manifest_value: toml::Value = toml::from_str(&manifest_content)?;

            let fs_metadata = fs::metadata(&manifest_path)?;

            let metadata = MetadataCommand::new()
                .manifest_path(&manifest_path)
                .exec()?;

            let target_dir = metadata.target_directory;

            let manifest = Manifest::from_path(&manifest_path)?;

            let package = manifest
                .package
                .ok_or_else(|| anyhow::anyhow!("no [package] section found in Cargo.toml"))?;

            let package_table = manifest_value
                .get("package")
                .and_then(|value| value.as_table());

            let description = package_table
                .and_then(|pkg| pkg.get("description"))
                .and_then(|value| value.as_str())
                .map(|value| value.to_string());

            let tags = package_table
                .and_then(|pkg| pkg.get("keywords"))
                .and_then(|value| value.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|item| item.as_str())
                        .map(|item| item.to_string())
                        .collect::<Vec<String>>()
                })
                .unwrap_or_default();

            let metadata = package_table
                .and_then(|pkg| pkg.get("metadata"))
                .and_then(|value| value.as_table())
                .and_then(|metadata| metadata.get("vigilo"))
                .map(value_from_toml)
                .transpose()?;

            Ok(PackageMetadata {
                name: package.name,
                version: package.version.get()?.to_string(),
                target_dir: target_dir.into_std_path_buf(),
                modified: fs_metadata.modified()?,
                description,
                tags,
                metadata,
            })
        }
        _ => Err(anyhow::anyhow!(
            "Vigilo.toml [package] manifest {} is unsupported",
            manifest_file
        )),
    }
}

/// Get fingerprint for wasmtime engine.
fn get_engine_fingerprint(engine: &Engine) -> String {
    let mut hasher = DefaultHasher::new();
    engine.precompile_compatibility_hash().hash(&mut hasher);
    format!("{:x}-{}", hasher.finish(), ARCH)
}

/// Exact Wasmtime dependency version supplied by the build.
const WASM_RUNTIME_VERSION: &str = env!("VIGILO_WASMTIME_VERSION");

/// Wasm component wrapper.
pub struct Component {
    pub name: String,
    pub version: String,
    pub description: Option<String>,
    pub tags: Value,
    pub metadata: Value,
    pub interface_name: String,
    pub interface_version: String,
    pub wit_world: String,
    pub abi: EvaluatorAbiIdentity,
    pub runtime: String,
    pub runtime_version: String,
    pub runtime_fingerprint: String,
    #[allow(dead_code)]
    pub component: PreparedEvaluator,
    pub wasm_hash: String,
    pub wasm_bytes: Vec<u8>,
    #[allow(dead_code)]
    pub serialized: Vec<u8>,
}

pub(crate) const DEFAULT_MAX_MEMORY_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const DEFAULT_MAX_TABLE_ELEMENTS: u64 = 10_000;
pub(crate) const DEFAULT_MAX_INSTANCES: u64 = 3;
pub(crate) const DEFAULT_MAX_MEMORIES: u64 = 1;
pub(crate) const DEFAULT_MAX_TABLES: u64 = 2;
pub(crate) const DEFAULT_FUEL_PER_EVALUATION: u64 = 50_000_000;
pub(crate) const DEFAULT_TIMEOUT_MS: u64 = 5_000;
pub(crate) const DEFAULT_EPOCH_TICK_INTERVAL_MS: u64 = 10;
pub(crate) const DEFAULT_MAX_CONCURRENT_EVALUATIONS: u64 = 8;
pub(crate) const DEFAULT_MAX_LOG_MESSAGE_BYTES: u64 = 4 * 1024;
pub(crate) const DEFAULT_MAX_LOG_MESSAGES: u32 = 128;

#[derive(Debug, Clone)]
pub struct Config {
    pub max_memory_bytes: u64,
    pub max_table_elements: u64,
    pub max_instances: u64,
    pub max_memories: u64,
    pub max_tables: u64,
    pub fuel_per_evaluation: u64,
    pub timeout_ms: u64,
    pub epoch_tick_interval_ms: u64,
    pub max_concurrent_evaluations: u64,
    pub max_log_message_bytes: u64,
    pub max_log_messages: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_memory_bytes: DEFAULT_MAX_MEMORY_BYTES,
            max_table_elements: DEFAULT_MAX_TABLE_ELEMENTS,
            max_instances: DEFAULT_MAX_INSTANCES,
            max_memories: DEFAULT_MAX_MEMORIES,
            max_tables: DEFAULT_MAX_TABLES,
            fuel_per_evaluation: DEFAULT_FUEL_PER_EVALUATION,
            timeout_ms: DEFAULT_TIMEOUT_MS,
            epoch_tick_interval_ms: DEFAULT_EPOCH_TICK_INTERVAL_MS,
            max_concurrent_evaluations: DEFAULT_MAX_CONCURRENT_EVALUATIONS,
            max_log_message_bytes: DEFAULT_MAX_LOG_MESSAGE_BYTES,
            max_log_messages: DEFAULT_MAX_LOG_MESSAGES,
        }
    }
}

impl Config {
    fn store_limits(&self) -> anyhow::Result<StoreLimits> {
        Ok(StoreLimitsBuilder::new()
            .memory_size(to_usize("wasm max memory bytes", self.max_memory_bytes)?)
            .table_elements(to_usize(
                "wasm max table elements",
                self.max_table_elements,
            )?)
            .instances(to_usize("wasm max instances", self.max_instances)?)
            .memories(to_usize("wasm max memories", self.max_memories)?)
            .tables(to_usize("wasm max tables", self.max_tables)?)
            .trap_on_grow_failure(true)
            .build())
    }

    fn epoch_deadline_ticks(&self) -> u64 {
        if self.timeout_ms == 0 {
            return 0;
        }

        self.timeout_ms
            .div_ceil(self.epoch_tick_interval_ms.max(1))
            .max(1)
    }
}

fn to_usize(name: &str, value: u64) -> anyhow::Result<usize> {
    usize::try_from(value).map_err(|_| anyhow::anyhow!("{} is too large: {}", name, value))
}

fn start_epoch_ticker(engine: Engine, tick_interval_ms: u64) {
    if tick_interval_ms == 0 {
        return;
    }

    let interval = Duration::from_millis(tick_interval_ms);
    let _ = thread::Builder::new()
        .name("vigilo-wasm-epoch-ticker".to_string())
        .spawn(move || {
            loop {
                thread::sleep(interval);
                #[cfg(target_has_atomic = "64")]
                engine.increment_epoch();
            }
        });
}

/// Wasm engine wrapper.
#[derive(Clone)]
pub struct Wasm {
    engine: Engine,
    fingerprint: String,
    config: Config,
    evaluation_semaphore: Arc<Semaphore>,
}

impl Wasm {
    pub fn new(config: Config) -> anyhow::Result<Self> {
        let mut engine_config = EngineConfig::new();
        engine_config.wasm_component_model(true);
        engine_config.consume_fuel(true);
        if config.timeout_ms > 0 {
            engine_config.epoch_interruption(true);
        }

        let engine = Engine::new(&engine_config)?;
        let fingerprint = get_engine_fingerprint(&engine);
        if config.timeout_ms > 0 {
            start_epoch_ticker(engine.clone(), config.epoch_tick_interval_ms);
        }
        let max_concurrent_evaluations = to_usize(
            "wasm max concurrent evaluations",
            config.max_concurrent_evaluations,
        )?;

        Ok(Self {
            engine,
            fingerprint,
            config,
            evaluation_semaphore: Arc::new(Semaphore::new(max_concurrent_evaluations)),
        })
    }

    pub(crate) async fn acquire_evaluation_permit(&self) -> anyhow::Result<OwnedSemaphorePermit> {
        self.evaluation_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|err| anyhow::anyhow!("wasm evaluation semaphore closed: {}", err))
    }

    /// Prepare evaluator for execution.
    pub fn prepare_evaluator(
        &self,
        package_path: PathBuf,
        profile: String,
    ) -> anyhow::Result<Component> {
        if let Some(package) = crate::evaluator_package::load(&package_path)? {
            let embedded = read_embedded_package_metadata(&package.wasm)?.ok_or_else(|| {
                anyhow::anyhow!("portable artifact must contain vigilo.package metadata")
            })?;
            anyhow::ensure!(
                embedded.name == package.name && embedded.version == package.version,
                "embedded vigilo.package identity mismatch"
            );
            let prepared = self.compile_evaluator(&package.wasm, &package.abi)?;
            let serialized = prepared.serialize()?;
            return Ok(Component {
                name: package.name,
                version: package.version,
                description: package.description,
                tags: package.tags,
                metadata: package.metadata,
                interface_name: format!("{}/{}", package.abi.package, package.abi.interface),
                interface_version: package.abi.version.clone(),
                wit_world: package.abi.world.clone(),
                abi: package.abi,
                runtime: WASM_RUNTIME_NAME.into(),
                runtime_version: WASM_RUNTIME_VERSION.into(),
                runtime_fingerprint: self.fingerprint.clone(),
                wasm_hash: blake3::hash(&package.wasm).to_hex().to_string(),
                wasm_bytes: package.wasm,
                component: prepared,
                serialized,
            });
        }
        let manifest = read_manifest(&package_path)?;
        let manifest_profile = manifest.get_profile(&profile)?;
        let wit_metadata = resolve_wit_metadata(&package_path, manifest.wit.as_ref())?;
        let abi = wit_metadata.abi.clone().ok_or_else(|| {
            anyhow::anyhow!("Vigilo.toml must declare a supported [wit] evaluator contract")
        })?;

        let package_metadata = get_package_metadata(&package_path, &manifest.package.manifest)?;
        let evaluator_metadata = resolve_evaluator_metadata(&manifest.package, &package_metadata)?;

        let wasm_path = package_metadata.target_dir.join(&manifest_profile.wasm);

        let fs_wasm_metadata = match fs::metadata(&wasm_path) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                let release_flag = if profile == "release" {
                    " --release"
                } else {
                    ""
                };

                anyhow::bail!(
                    "configured wasm artifact was not found at {} (profile '{}'); build it first with: cargo build --manifest-path {} --target wasm32-wasip2{}",
                    wasm_path.display(),
                    profile,
                    package_path.join(&manifest.package.manifest).display(),
                    release_flag,
                );
            }
            Err(err) => {
                return Err(anyhow::anyhow!(
                    "failed to read wasm metadata at {}: {}",
                    wasm_path.display(),
                    err
                ));
            }
        };
        let wasm_modified = fs_wasm_metadata.modified()?;
        if package_metadata.modified > wasm_modified {
            return Err(anyhow::anyhow!(
                "evaluation manifest was modified after wasm build"
            ));
        }
        let wasm_bytes = fs::read(&wasm_path).map_err(|err| {
            anyhow::anyhow!(
                "failed to read wasm bytes at {}: {}",
                wasm_path.display(),
                err
            )
        })?;
        let wasm_bytes = ensure_embedded_package_metadata(
            wasm_bytes,
            &package_metadata.name,
            &package_metadata.version,
        )?;
        let wasm_hash = blake3::hash(&wasm_bytes).to_hex().to_string();

        let prepared = self.compile_evaluator(&wasm_bytes, &abi)?;
        let serialized = prepared.serialize()?;

        let runtime_version = WASM_RUNTIME_VERSION.to_owned();

        Ok(Component {
            name: package_metadata.name,
            version: package_metadata.version,
            description: evaluator_metadata.description,
            tags: evaluator_metadata.tags,
            metadata: evaluator_metadata.metadata,
            interface_name: format!("{}/{}", abi.package, abi.interface),
            interface_version: abi.version.clone(),
            wit_world: abi.world.clone(),
            abi,
            runtime: WASM_RUNTIME_NAME.to_string(),
            runtime_version,
            runtime_fingerprint: self.fingerprint.clone(),
            component: prepared,
            wasm_hash,
            wasm_bytes,
            serialized,
        })
    }

    /// Compile evaluator wasm bytes into a component for registry caching.
    pub fn compile_evaluator(
        &self,
        wasm_bytes: &[u8],
        identity: &EvaluatorAbiIdentity,
    ) -> anyhow::Result<PreparedEvaluator> {
        PreparedEvaluator::compile(self, wasm_bytes, identity)
    }

    /// Run evaluator in test mode.
    pub fn test_evaluator(
        &self,
        wasm_bytes: &[u8],
        abi: &EvaluatorAbiIdentity,
        input: EvaluatorInput,
    ) -> anyhow::Result<EvaluatorOutput> {
        let evaluator = self.compile_evaluator(wasm_bytes, abi)?;
        self.test_evaluator_component(&evaluator, input)
    }

    /// Run evaluator in test mode using a prepared component and adapter.
    pub fn test_evaluator_component(
        &self,
        evaluator: &PreparedEvaluator,
        input: EvaluatorInput,
    ) -> anyhow::Result<EvaluatorOutput> {
        evaluator.execute(self, input)
    }

    pub(crate) fn engine(&self) -> &Engine {
        &self.engine
    }

    pub(crate) fn evaluator_store(&self) -> anyhow::Result<Store<EvaluatorHost>> {
        let mut store = Store::new(
            &self.engine,
            EvaluatorHost {
                table: ResourceTable::new(),
                ctx: WasiCtxBuilder::new().build(),
                limits: self.config.store_limits()?,
                max_log_message_bytes: to_usize(
                    "wasm max log message bytes",
                    self.config.max_log_message_bytes,
                )?,
                max_log_messages: self.config.max_log_messages,
                log_messages: 0,
            },
        );
        store.limiter(|host| &mut host.limits);
        store.set_fuel(self.config.fuel_per_evaluation)?;
        if self.config.timeout_ms > 0 {
            #[cfg(target_has_atomic = "64")]
            {
                store.set_epoch_deadline(self.config.epoch_deadline_ticks());
                store.epoch_deadline_trap();
            }
        }
        Ok(store)
    }
}

pub(crate) struct Context {
    pub(crate) cell: OnceCell<Wasm>,
    pub(crate) config: Config,
}

impl Context {
    pub async fn get(&self) -> anyhow::Result<&Wasm> {
        self.cell
            .get_or_try_init(|| async {
                debug!("initializing wasm engine");
                Wasm::new(self.config.clone())
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::SystemTime,
    };

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;
    use crate::manifest::Package;

    const TEST_WIT: &str = r#"
        package vigilo:evaluator@1.0.0;

        interface evaluator {}

        world evaluator-world {
            export evaluator;
        }
    "#;

    fn minimal_wasm_module() -> Vec<u8> {
        b"\0asm\x01\0\0\0".to_vec()
    }

    fn test_host(max_log_message_bytes: usize, max_log_messages: u32) -> EvaluatorHost {
        EvaluatorHost {
            table: ResourceTable::new(),
            ctx: WasiCtxBuilder::new().build(),
            limits: Config::default().store_limits().unwrap(),
            max_log_message_bytes,
            max_log_messages,
            log_messages: 0,
        }
    }

    fn manifest_wit(
        package: &str,
        version: &str,
        world: &str,
        interface: &str,
        strict: bool,
    ) -> Wit {
        Wit {
            path: "evaluator.wit".to_string(),
            world: world.to_string(),
            package: package.to_string(),
            version: version.to_string(),
            interface: interface.to_string(),
            strict,
        }
    }

    fn cargo_package_metadata() -> PackageMetadata {
        PackageMetadata {
            name: "example".to_string(),
            version: "1.0.0".to_string(),
            target_dir: PathBuf::from("target"),
            modified: SystemTime::UNIX_EPOCH,
            description: Some("Cargo description".to_string()),
            tags: vec!["cargo".to_string()],
            metadata: Some(json!({"owner": "cargo"})),
        }
    }

    fn error_message<T>(result: anyhow::Result<T>) -> String {
        match result {
            Ok(_) => panic!("expected operation to fail"),
            Err(err) => err.to_string(),
        }
    }

    #[test]
    fn utf8_truncation_respects_character_boundaries() {
        let cases = [
            ("hello", 5, "hello"),
            ("hello", 3, "hel"),
            ("aéb", 2, "a"),
            ("aéb", 3, "aé"),
            ("hello", 0, ""),
        ];

        for (value, max_bytes, expected) in cases {
            assert_eq!(truncate_utf8_bytes(value.to_string(), max_bytes), expected);
        }
    }

    #[test]
    fn evaluator_logs_enforce_message_and_byte_limits() {
        let mut host = test_host(3, 2);

        assert_eq!(
            host.capped_log_message("hello".to_string()).as_deref(),
            Some("hel")
        );
        assert_eq!(
            host.capped_log_message("éé".to_string()).as_deref(),
            Some("é")
        );
        assert_eq!(host.capped_log_message("ignored".to_string()), None);

        let mut disabled = test_host(10, 0);
        assert_eq!(disabled.capped_log_message("ignored".to_string()), None);
    }

    #[test]
    fn package_metadata_embedding_is_idempotent_and_rejects_conflicts() {
        let original = minimal_wasm_module();
        let embedded =
            ensure_embedded_package_metadata(original.clone(), "example", "1.0.0").unwrap();
        let metadata = read_embedded_package_metadata(&embedded).unwrap().unwrap();

        assert_eq!(metadata.name, "example");
        assert_eq!(metadata.version, "1.0.0");
        assert!(embedded.len() > original.len());
        assert_eq!(
            ensure_embedded_package_metadata(embedded.clone(), "example", "1.0.0").unwrap(),
            embedded
        );

        let err = ensure_embedded_package_metadata(embedded, "other", "1.0.0").unwrap_err();
        assert!(err.to_string().contains("embedded vigilo.package mismatch"));
    }

    #[test]
    fn duplicate_embedded_package_metadata_is_rejected() {
        let original = b"\0asm\x0d\0\x01\0";
        let embedded =
            ensure_embedded_package_metadata(original.to_vec(), "example", "1.0.0").unwrap();
        let duplicate = append_custom_section(
            &embedded,
            PACKAGE_METADATA_SECTION,
            br#"{"name":"example","version":"1.0.0"}"#,
        )
        .unwrap();
        assert!(
            read_embedded_package_metadata(&duplicate)
                .unwrap_err()
                .to_string()
                .contains("duplicate")
        );
    }

    #[cfg(feature = "evaluator-abi-fixtures")]
    #[test]
    fn portable_artifacts_preserve_exact_bytes_and_verify_embedded_identity_and_host_abi() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let raw =
            fs::read(root.join("../target/wasm32-wasip2/release/sentiment_basic_en.wasm")).unwrap();
        let bytes =
            ensure_embedded_package_metadata(raw.clone(), "sentiment-basic-en", "0.1.0").unwrap();
        let abi = resolve_declaration("vigilo:evaluator", "evaluator-world", "evaluator", "1.0.0")
            .unwrap()
            .identity()
            .clone();
        let package = |wasm, name: &str| crate::evaluator_package::PackageInput {
            name: name.into(),
            version: "0.1.0".into(),
            description: None,
            tags: serde_json::json!([]),
            metadata: serde_json::json!({}),
            abi: abi.clone(),
            wasm,
        };
        let dir = tempfile::tempdir().unwrap();
        let runtime = Wasm::new(Config::default()).unwrap();
        let good = dir.path().join("good.tar.gz");
        crate::evaluator_package::write(&good, package(bytes.clone(), "sentiment-basic-en"))
            .unwrap();
        let component = runtime.prepare_evaluator(good, "dev".into()).unwrap();
        assert_eq!(component.wasm_bytes, bytes);
        assert_eq!(component.wasm_hash, blake3::hash(&bytes).to_hex().as_str());
        assert_eq!(component.abi, abi);
        for (label, wasm, name) in [
            ("missing", raw, "sentiment-basic-en"),
            ("identity", bytes, "different-name"),
            (
                "abi",
                ensure_embedded_package_metadata(
                    b"\0asm\x0d\0\x01\0".to_vec(),
                    "sentiment-basic-en",
                    "0.1.0",
                )
                .unwrap(),
                "sentiment-basic-en",
            ),
        ] {
            let path = dir.path().join(format!("{label}.tar.gz"));
            crate::evaluator_package::write(&path, package(wasm, name)).unwrap();
            assert!(
                runtime.prepare_evaluator(path, "dev".into()).is_err(),
                "accepted {label}"
            );
        }
    }

    #[test]
    fn malformed_embedded_package_metadata_is_rejected() {
        let wasm = append_custom_section(
            &minimal_wasm_module(),
            PACKAGE_METADATA_SECTION,
            b"not-json",
        )
        .unwrap();

        let err = read_embedded_package_metadata(&wasm).unwrap_err();

        assert!(
            err.to_string()
                .contains("failed to decode vigilo.package metadata")
        );
    }

    #[test]
    fn strict_wit_metadata_requires_the_declared_contract() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("evaluator.wit"), TEST_WIT).unwrap();
        let valid = manifest_wit(
            "vigilo:evaluator",
            "1.0.0",
            "evaluator-world",
            "evaluator",
            true,
        );

        let metadata = resolve_wit_metadata(dir.path(), Some(&valid)).unwrap();
        assert_eq!(
            metadata.abi.unwrap(),
            crate::evaluator_abi::current_identity()
        );

        let mismatches = [
            (
                "other:package",
                "1.0.0",
                "evaluator-world",
                "evaluator",
                "unsupported evaluator ABI",
            ),
            (
                "vigilo:evaluator",
                "9.0.0",
                "evaluator-world",
                "evaluator",
                "unsupported evaluator ABI",
            ),
            (
                "vigilo:evaluator",
                "1.0.0",
                "missing-world",
                "evaluator",
                "unsupported evaluator ABI",
            ),
            (
                "vigilo:evaluator",
                "1.0.0",
                "evaluator-world",
                "missing-interface",
                "unsupported evaluator ABI",
            ),
        ];
        for (package, version, world, interface, expected) in mismatches {
            let wit = manifest_wit(package, version, world, interface, true);
            let err = error_message(resolve_wit_metadata(dir.path(), Some(&wit)));
            assert!(err.contains(expected), "expected '{expected}' in '{err}'");
        }
    }

    #[test]
    fn evaluator_abi_metadata_requires_a_supported_declaration() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("evaluator.wit"), TEST_WIT).unwrap();

        let absent = resolve_wit_metadata(dir.path(), None).unwrap();
        assert!(absent.abi.is_none());

        let configured = manifest_wit("other:package", "9.0.0", "missing", "missing", false);
        let error = resolve_wit_metadata(dir.path(), Some(&configured)).unwrap_err();
        assert!(error.to_string().contains("unsupported evaluator ABI"));

        fs::write(
            dir.path().join("evaluator.wit"),
            TEST_WIT.replace("@1.0.0", "@0.1.0"),
        )
        .unwrap();
        let configured = manifest_wit(
            "vigilo:evaluator",
            "1.0.0",
            "evaluator-world",
            "evaluator",
            false,
        );
        let error = resolve_wit_metadata(dir.path(), Some(&configured)).unwrap_err();
        assert!(error.to_string().contains("WIT version mismatch"));
    }

    #[test]
    fn wit_parser_rejects_files_without_a_package_declaration() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("evaluator.wit");
        fs::write(&path, "world evaluator-world {}\n").unwrap();

        let err = error_message(parse_wit_file(&path));

        assert!(err.contains("missing package declaration"));
    }

    #[test]
    fn evaluator_manifest_metadata_overrides_cargo_fallbacks() {
        let fallback = Package {
            manifest: "Cargo.toml".to_string(),
            description: None,
            tags: Vec::new(),
            metadata: None,
        };
        let resolved = resolve_evaluator_metadata(&fallback, &cargo_package_metadata()).unwrap();
        assert_eq!(resolved.description.as_deref(), Some("Cargo description"));
        assert_eq!(resolved.tags, json!(["cargo"]));
        assert_eq!(resolved.metadata, json!({"owner": "cargo"}));

        let overrides = Package {
            manifest: "Cargo.toml".to_string(),
            description: Some("Manifest description".to_string()),
            tags: vec!["manifest".to_string()],
            metadata: Some(toml::from_str("owner = 'manifest'").unwrap()),
        };
        let resolved = resolve_evaluator_metadata(&overrides, &cargo_package_metadata()).unwrap();
        assert_eq!(
            resolved.description.as_deref(),
            Some("Manifest description")
        );
        assert_eq!(resolved.tags, json!(["manifest"]));
        assert_eq!(resolved.metadata, json!({"owner": "manifest"}));
    }

    #[test]
    fn wasm_configuration_uses_bounded_defaults_and_deadlines() {
        let defaults = Config::default();
        defaults.store_limits().unwrap();
        assert_eq!(defaults.max_memory_bytes, DEFAULT_MAX_MEMORY_BYTES);
        assert_eq!(
            defaults.max_concurrent_evaluations,
            DEFAULT_MAX_CONCURRENT_EVALUATIONS
        );

        let mut config = defaults.clone();
        config.timeout_ms = 11;
        config.epoch_tick_interval_ms = 5;
        assert_eq!(config.epoch_deadline_ticks(), 3);

        config.timeout_ms = 0;
        assert_eq!(config.epoch_deadline_ticks(), 0);
    }

    #[test]
    fn runtime_identity_comes_from_the_engine_and_build_metadata() {
        let engine = Engine::default();
        let fingerprint = get_engine_fingerprint(&engine);
        let version = WASM_RUNTIME_VERSION;

        assert!(fingerprint.ends_with(&format!("-{ARCH}")));
        assert!(!version.is_empty());
        assert!(version.chars().next().unwrap().is_ascii_digit());
    }

    #[tokio::test]
    async fn wasm_context_initializes_once_and_bounds_concurrency() {
        let config = Config {
            timeout_ms: 0,
            max_concurrent_evaluations: 1,
            ..Config::default()
        };
        let context = Context {
            cell: OnceCell::new(),
            config,
        };

        let first = context.get().await.unwrap();
        let second = context.get().await.unwrap();
        assert!(std::ptr::eq(first, second));

        let permit = first.acquire_evaluation_permit().await.unwrap();
        assert!(
            first
                .evaluation_semaphore
                .clone()
                .try_acquire_owned()
                .is_err()
        );
        drop(permit);
        let _permit = first.acquire_evaluation_permit().await.unwrap();
    }
}
