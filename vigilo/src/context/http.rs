//! Lazy shared HTTP client context.
//!
//! Runtime paths use this client for outbound agent calls. Keep transport-wide
//! configuration here; request shape, timeout semantics, and response
//! normalization belong in `agent_client`.

use tokio::sync::OnceCell;
use tracing::debug;

pub struct Context {
    pub(crate) cell: OnceCell<reqwest::Client>,
}

impl Context {
    pub async fn get(&self) -> anyhow::Result<&reqwest::Client> {
        self.cell
            .get_or_try_init(|| async {
                debug!("initializing shared http client");

                reqwest::Client::builder()
                    .no_gzip()
                    .no_brotli()
                    .no_deflate()
                    .no_zstd()
                    .build()
                    .map_err(|e| anyhow::anyhow!("http client initialization failed: {}", e))
            })
            .await
    }
}
