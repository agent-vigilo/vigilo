//! Short registry transactions and metadata-only bounded reads.
//!
//! Row policies belong to the deployment. Caller identity is transaction-local;
//! no connection survives Wasm compilation, HTTP or input receipt. Operator calls
//! retain autocommit behavior. Pages charge metadata before returning payloads.
use std::ops::{
    Deref,
    DerefMut,
};

use sqlx::{
    PgConnection,
    PgPool,
    Postgres,
    Transaction,
    pool::PoolConnection,
    types::Json,
};

use super::*;

pub(super) enum Connection {
    Operator(PoolConnection<Postgres>),
    User(Transaction<'static, Postgres>),
}
impl Deref for Connection {
    type Target = PgConnection;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Operator(c) => c,
            Self::User(c) => c,
        }
    }
}
impl DerefMut for Connection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Operator(c) => c,
            Self::User(c) => c,
        }
    }
}
impl Connection {
    pub async fn open(pool: &PgPool, caller: Option<UserContext>) -> anyhow::Result<Self> {
        if let Some(caller) = caller {
            let mut tx = pool.begin().await?;
            sqlx::query("SELECT set_config('vigilo.tenant_id',$1,true),set_config('vigilo.actor_id',$2,true),set_config('vigilo.run_id','',true),set_config('vigilo.run_batch','',true)")
    .bind(caller.tenant_id().to_string()).bind(caller.actor_id().to_string()).execute(&mut *tx).await?;
            Ok(Self::User(tx))
        } else {
            Ok(Self::Operator(pool.acquire().await?))
        }
    }
    pub async fn finish(self) -> anyhow::Result<()> {
        if let Self::User(tx) = self {
            tx.commit().await?;
        }
        Ok(())
    }
}
/// Validate the shared registry's independent protection once at trusted factory
/// construction. Namespace/visibility policy remains deployment-owned.
pub(super) async fn validate(pool: &PgPool) -> anyhow::Result<()> {
    let valid: bool = sqlx::query_scalar(
        r#"SELECT current_setting('row_security')='on' AND c.relrowsecurity
 AND EXISTS(SELECT 1 FROM pg_policy WHERE polrelid=c.oid)
 AND NOT EXISTS(SELECT 1 FROM pg_roles r WHERE pg_has_role(current_user,r.oid,'MEMBER')
  AND (r.rolsuper OR r.rolbypassrls OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication))
 AND NOT pg_has_role(current_user,c.relowner,'MEMBER')
 AND NOT has_schema_privilege(c.relnamespace,'CREATE')
 AND NOT has_table_privilege(c.oid,'TRUNCATE')
 AND NOT has_table_privilege(c.oid,'TRIGGER')
 FROM pg_class c WHERE c.oid='evaluators'::regclass"#,
    )
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        valid,
        "registry role or row protection does not satisfy operation boundary"
    );
    Ok(())
}

pub(super) async fn show(c: &mut PgConnection, id: &EvaluatorId) -> anyhow::Result<Descriptor> {
    let row: Option<(Option<Json<Descriptor>>,)> = sqlx::query_as(SHOW.as_str())
        .bind(&id.namespace)
        .bind(&id.name)
        .bind(&id.version)
        .fetch_optional(c)
        .await?;
    let row = row.ok_or_else(|| Error::EvaluatorNotFound(id.to_string()))?;
    Ok(row
        .0
        .ok_or(Error::InvalidInput("evaluator metadata exceeds 8 MiB"))?
        .0)
}
type ArtifactColumns = (Option<Json<Descriptor>>, Option<Vec<u8>>);

pub(super) async fn artifact(
    c: &mut PgConnection,
    id: &EvaluatorId,
) -> anyhow::Result<(Descriptor, Vec<u8>)> {
    let row: Option<ArtifactColumns> = sqlx::query_as(ARTIFACT.as_str())
        .bind(&id.namespace)
        .bind(&id.name)
        .bind(&id.version)
        .fetch_optional(c)
        .await?;
    let row = row.ok_or_else(|| Error::EvaluatorNotFound(id.to_string()))?;
    Ok((
        row.0
            .ok_or(Error::InvalidInput("evaluator metadata exceeds 8 MiB"))?
            .0,
        row.1
            .ok_or(Error::InvalidInput("evaluator artifact exceeds 64 MiB"))?,
    ))
}
pub(super) async fn page(
    c: &mut PgConnection,
    namespace: &str,
    query: Option<&str>,
    limits: PageLimits,
    cursor: Option<&Cursor>,
) -> anyhow::Result<Page> {
    let query = query.map(str::trim).filter(|s| !s.is_empty());
    let filter = types::filter_hash(namespace, query);
    let after = cursor.map(|c| c.position(namespace, query)).transpose()?;
    let rows: Vec<(String, String, Option<Json<Descriptor>>)> = sqlx::query_as(PAGE.as_str())
        .bind(namespace)
        .bind(query.map(|s| format!("%{s}%")))
        .bind(after.as_ref().map(|v| v.0.as_str()))
        .bind(after.as_ref().map(|v| v.1.as_str()))
        .bind(i64::from(limits.rows) + 1)
        .bind(i64::from(limits.bytes))
        .fetch_all(c)
        .await?;
    let mut items = Vec::new();
    let mut last = None;
    let mut more = false;
    for (name, version, payload) in rows {
        if items.len() == limits.rows as usize || payload.is_none() {
            more = true;
            break;
        }
        items.push(payload.unwrap().0);
        last = Some((name, version));
    }
    anyhow::ensure!(
        !more || !items.is_empty(),
        "evaluator descriptor exceeds page byte budget"
    );
    let next = if more {
        let (n, v) = last.expect("nonempty page");
        Some(Cursor(serde_json::to_string(&(filter, n, v))?))
    } else {
        None
    };
    Ok(Page { items, next })
}

const DESCRIPTOR: &str = r#"jsonb_build_object('id',e.id,'namespace',e.namespace,'name',e.name,'version',e.version,'content_hash',e.content_hash,'wasm_size_bytes',e.wasm_size_bytes,'interface_name',e.interface_name,'interface_version',e.interface_version,'wit_world',e.wit_world,'abi_contract_hash',e.abi_contract_hash,'abi_adapter',e.abi_adapter,'runtime',e.runtime,'runtime_version',e.runtime_version,'runtime_fingerprint',e.runtime_fingerprint,'description',e.description,'tags',e.tags,'metadata',e.metadata,'state',e.state,'state_reason',e.state_reason,'created_at',e.created_at,'updated_at',e.updated_at)"#;
static SHOW: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    format!(
        r#"WITH d AS MATERIALIZED (SELECT {DESCRIPTOR} AS payload FROM evaluators e WHERE namespace=$1 AND name=$2 AND version=$3) SELECT CASE WHEN octet_length(payload::text)<=8388608 THEN payload END FROM d"#
    )
});
static ARTIFACT: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    format!(
        r#"WITH d AS MATERIALIZED (SELECT {DESCRIPTOR} AS payload,CASE WHEN octet_length(wasm_bytes)<=67108864 THEN wasm_bytes END AS wasm FROM evaluators e WHERE namespace=$1 AND name=$2 AND version=$3) SELECT CASE WHEN octet_length(payload::text)<=8388608 THEN payload END,wasm FROM d"#
    )
});
static PAGE: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    format!(
        r#"WITH candidates AS MATERIALIZED (SELECT name,version,{DESCRIPTOR} AS payload FROM evaluators e WHERE namespace=$1
AND ($2::text IS NULL OR name ILIKE $2 OR COALESCE(description,'') ILIKE $2 OR COALESCE(state_reason,'') ILIKE $2 OR tags::text ILIKE $2 OR metadata::text ILIKE $2)
AND ($3::text IS NULL OR name>$3 OR (name=$3 AND version<$4)) ORDER BY name,version DESC LIMIT $5),
charged AS (SELECT *,sum(octet_length(payload::text)) OVER (ORDER BY name,version DESC) AS bytes FROM candidates)
SELECT name,version,CASE WHEN bytes<=$6 THEN payload END FROM charged ORDER BY name,version DESC"#
    )
});
