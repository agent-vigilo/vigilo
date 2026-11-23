//! Tenant and caller scoped run pages from the owning control store.

use base64::{
    Engine as _,
    engine::general_purpose::URL_SAFE_NO_PAD,
};
use chrono::{
    DateTime,
    Utc,
};
use serde::{
    Deserialize,
    Serialize,
};
use uuid::Uuid;

use super::{
    Error,
    UserClient,
    UserContext,
    scope,
};

const MAX_PAGE_SIZE: u16 = 100;
const MAX_CURSOR_BYTES: usize = 1024;
const CURSOR_VERSION: u8 = 1;

const FIRST_PAGE: &str =
    "SELECT id AS run_id, status::text AS status, gate_status::text AS gate_status, created_at
    FROM runs WHERE tenant_id = $1 ORDER BY created_at DESC, id DESC LIMIT $2";
const NEXT_PAGE: &str =
    "SELECT id AS run_id, status::text AS status, gate_status::text AS gate_status, created_at
    FROM runs WHERE tenant_id = $1 AND (created_at, id) < ($2, $3)
    ORDER BY created_at DESC, id DESC LIMIT $4";

/// One page of the caller's visible runs. The default page size is 50.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ListRunsRequest {
    /// An explicit page size from 1 through 100.
    pub page_size: u16,
    /// The previous page's opaque continuation, or `None` for the first page.
    pub cursor: Option<String>,
}

impl Default for ListRunsRequest {
    fn default() -> Self {
        Self {
            page_size: 50,
            cursor: None,
        }
    }
}

/// Visible runs in newest-first order and an optional continuation.
#[derive(Debug, Serialize)]
pub struct RunPage {
    pub items: Vec<RunListItem>,
    pub next: Option<String>,
}

/// Run metadata available on the control store.
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct RunListItem {
    pub run_id: Uuid,
    pub status: String,
    pub gate_status: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Position {
    version: u8,
    scope: String,
    created_at: DateTime<Utc>,
    run_id: Uuid,
}

impl Position {
    fn parse(token: &str, alias: &str, context: UserContext) -> Result<Self, Error> {
        let invalid = || Error::InvalidInput("invalid run-list cursor");
        if token.len() > MAX_CURSOR_BYTES {
            return Err(invalid());
        }
        let bytes = URL_SAFE_NO_PAD.decode(token).map_err(|_| invalid())?;
        let position: Self = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if position.version != CURSOR_VERSION
            || position.scope != cursor_scope(alias, context).to_hex().as_str()
        {
            return Err(invalid());
        }
        Ok(position)
    }
}

// Fixed-width caller IDs precede the alias. Domain separation binds the
// position to this query version without putting caller IDs on the wire.
fn cursor_scope(alias: &str, context: UserContext) -> blake3::Hash {
    blake3::Hasher::new_derive_key("vigilo run listing cursor v1")
        .update(context.tenant_id().as_bytes())
        .update(context.actor_id().as_bytes())
        .update(alias.as_bytes())
        .finalize()
}

impl UserClient {
    /// Lists visible runs on this client's bound control store using current
    /// caller context. Each page uses one data query in a short read-only
    /// transaction. Continuations seek strictly below the previous page's last
    /// key. Rows inserted ahead of that key appear on a fresh traversal.
    ///
    /// The page limit bounds returned rows. Row policies that reject candidates
    /// after the index seek can increase database work for sparse visibility.
    pub async fn list_runs(
        &self,
        context: UserContext,
        request: ListRunsRequest,
    ) -> Result<RunPage, Error> {
        if !(1..=MAX_PAGE_SIZE).contains(&request.page_size) {
            return Err(Error::InvalidInput("run-list page size must be 1–100"));
        }
        let position = request
            .cursor
            .as_deref()
            .map(|token| Position::parse(token, &self.control_alias, context))
            .transpose()?;
        self.read_run_page(context, usize::from(request.page_size), position)
            .await
            .map_err(Error::Dependency)
    }

    async fn read_run_page(
        &self,
        context: UserContext,
        page_size: usize,
        position: Option<Position>,
    ) -> anyhow::Result<RunPage> {
        let mut tx = scope::begin_list(&self.control, context).await?;
        let limit = page_size as i64 + 1;
        let mut items: Vec<RunListItem> = if let Some(position) = position {
            sqlx::query_as(NEXT_PAGE)
                .bind(context.tenant_id())
                .bind(position.created_at)
                .bind(position.run_id)
                .bind(limit)
                .fetch_all(&mut *tx)
                .await?
        } else {
            sqlx::query_as(FIRST_PAGE)
                .bind(context.tenant_id())
                .bind(limit)
                .fetch_all(&mut *tx)
                .await?
        };
        tx.commit().await?;
        let next = if items.len() > page_size {
            items.truncate(page_size);
            let last = &items[page_size - 1];
            let bytes = serde_json::to_vec(&Position {
                version: CURSOR_VERSION,
                scope: cursor_scope(&self.control_alias, context)
                    .to_hex()
                    .to_string(),
                created_at: last.created_at,
                run_id: last.run_id,
            })?;
            Some(URL_SAFE_NO_PAD.encode(bytes))
        } else {
            None
        };
        Ok(RunPage { items, next })
    }
}

#[cfg(test)]
mod tests;
