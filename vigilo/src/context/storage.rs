//! Process-local storage configuration and bounded adapter reuse.
//!
//! Durable references pin an owner's physical binding; credentials and local
//! paths stay here. Runtime readers share transport and admission resources and
//! never create capability probes or mutate control storage.
use std::sync::Arc;

use anyhow::{
    Context as _,
    Result,
    ensure,
};
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;

use crate::{
    blob_storage::{
        BlobStorage,
        Budget,
        config::OwnerConfig,
    },
    contracts::case_storage::FormatLimits,
};

type StoreCell = Arc<OnceCell<Arc<dyn BlobStorage>>>;
type StoreCache = moka::future::Cache<String, StoreCell>;

#[derive(Default)]
pub(super) struct Context {
    owner: OnceCell<OwnerConfig>,
    stores: OnceCell<StoreCache>,
}
impl Context {
    pub async fn get(&self, binding: &str, limits: &FormatLimits) -> Result<Arc<dyn BlobStorage>> {
        let owner = self
            .owner
            .get_or_try_init(|| async {
                let path = std::env::var_os("VIGILO_STORAGE_CONFIG")
                    .context("VIGILO_STORAGE_CONFIG is required for immutable run inputs")?;
                tokio::task::spawn_blocking(move || OwnerConfig::load(std::path::Path::new(&path)))
                    .await?
            })
            .await?;
        ensure!(
            owner.binding_hash()? == binding,
            "runtime storage owner binding changed"
        );
        let cache = self
            .stores
            .get_or_init(|| async { moka::future::Cache::new(16) })
            .await;
        let key = serde_json::to_string(limits)?;
        let cell = cache
            .get_with(key, async { Arc::new(OnceCell::new()) })
            .await;
        Ok(cell
            .get_or_try_init(|| async {
                Ok::<Arc<dyn BlobStorage>, anyhow::Error>(Arc::from(
                    owner
                        .reader(
                            limits.clone(),
                            Budget::standard(),
                            &CancellationToken::new(),
                        )
                        .await?,
                ))
            })
            .await?
            .clone())
    }
}
