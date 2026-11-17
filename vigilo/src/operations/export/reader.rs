//! Fixed export credential domains and bounded live reads.
//!
//! Export pools are distinct from status/summary and service pools. Connection
//! guards reject effective access outside the explicit projection allowlist,
//! including direct partition and inherited grants. Every page rebinds caller
//! identity and discovers routes afresh. No connection or transaction is in a cursor.

use std::collections::BTreeMap;

use sqlx::{
    PgConnection,
    PgPool,
};
use uuid::Uuid;

use super::{
    ExportCollection,
    ExportCursor,
    ExportLimits,
    ExportPage,
    Position,
    sql,
};
use crate::{
    db::scope,
    operations::{
        Error,
        UserClient,
        UserConfig,
        UserContext,
    },
};

/// Ordinary export reads on dedicated, fixed, read-only credentials. The trusted
/// host authenticates callers; deployment policy owns row-level authorization.
#[derive(Clone)]
pub struct ExportClient {
    client: UserClient,
}
/// Content and diagnostic reads on a separate fixed credential domain. The host
/// authorizes each operation; constructing this handle is not end-user input.
#[derive(Clone)]
pub struct SensitiveExportClient {
    client: UserClient,
}
impl ExportClient {
    /// Builds lazy shared export-only pools. Effective grants are checked when
    /// connections open; ordinary credentials must not read sensitive columns.
    pub fn new(config: UserConfig) -> Result<Self, Error> {
        Self::with_trusted_policy_functions(config, BTreeMap::new())
    }
    /// Explicitly trusts host-audited policy function signatures by database alias.
    /// Functions must be schema-qualified, scalar boolean SECURITY DEFINER helpers
    /// declared STABLE with `search_path=pg_catalog, pg_temp`. The host audits and
    /// owns their definitions; the reader must not own them or their schemas. At most
    /// 64 signatures of 1,024 bytes each are accepted per bound alias. Invalid
    /// bindings fail locally; missing or unsuitable functions fail when connecting.
    /// This configures trust, never grants privileges or authenticates a caller.
    pub fn with_trusted_policy_functions(
        config: UserConfig,
        functions: BTreeMap<String, Vec<String>>,
    ) -> Result<Self, Error> {
        Ok(Self {
            client: UserClient::new_export(config, false, functions)?,
        })
    }
    /// Reads ordinary records under current caller authority. A cursor must match
    /// the caller, run and collection. Sensitive collections and oversized records
    /// fail explicitly; a dependency failure returns no successful partial page.
    pub async fn page(
        &self,
        context: UserContext,
        run_id: Uuid,
        collection: ExportCollection,
        limits: ExportLimits,
        cursor: Option<&ExportCursor>,
    ) -> Result<ExportPage, Error> {
        if collection.sensitive() {
            return Err(Error::InvalidInput(
                "sensitive collection requires sensitive export reader",
            ));
        }
        hosted(&self.client, context, run_id, collection, limits, cursor).await
    }

    /// Closes these shared export pools, including clones. In-flight reads finish
    /// or are cancelled by their owner before their connections can be released.
    pub async fn close(&self) {
        close(&self.client).await;
    }
}
impl SensitiveExportClient {
    /// Builds lazy shared content/diagnostic pools. No ordinary or system pool is
    /// used as fallback. The host must authorize content and diagnostics separately.
    pub fn new(config: UserConfig) -> Result<Self, Error> {
        Self::with_trusted_policy_functions(config, BTreeMap::new())
    }
    /// Uses the same explicit host-owned policy-function trust boundary as the
    /// ordinary reader. Content and diagnostics still require host authorization.
    pub fn with_trusted_policy_functions(
        config: UserConfig,
        functions: BTreeMap<String, Vec<String>>,
    ) -> Result<Self, Error> {
        Ok(Self {
            client: UserClient::new_export(config, true, functions)?,
        })
    }
    /// Reads content after host authorization. Current row policies apply on every
    /// participating database; diagnostics require their own operation.
    /// No credentials or routes are accepted from a continuation cursor.
    pub async fn content_page(
        &self,
        context: UserContext,
        run_id: Uuid,
        collection: ExportCollection,
        limits: ExportLimits,
        cursor: Option<&ExportCursor>,
    ) -> Result<ExportPage, Error> {
        if !collection.sensitive() || collection == ExportCollection::Diagnostics {
            return Err(Error::InvalidInput(
                "content operation requires a content collection",
            ));
        }
        hosted(&self.client, context, run_id, collection, limits, cursor).await
    }

    /// Reads diagnostics after a distinct host authorization decision. The
    /// collection is fixed so a content request cannot switch into diagnostic reads.
    pub async fn diagnostics_page(
        &self,
        context: UserContext,
        run_id: Uuid,
        limits: ExportLimits,
        cursor: Option<&ExportCursor>,
    ) -> Result<ExportPage, Error> {
        hosted(
            &self.client,
            context,
            run_id,
            ExportCollection::Diagnostics,
            limits,
            cursor,
        )
        .await
    }

    /// Closes these shared sensitive-reader pools, including clones.
    pub async fn close(&self) {
        close(&self.client).await;
    }
}

async fn close(client: &UserClient) {
    client.control.close().await;
    for pool in client.execution.values() {
        pool.close().await;
    }
}

/// Checks grants and explicit helper trust in one cold connection metadata query.
/// Definition shape and ownership checks cannot certify the host's policy code;
/// no request or cursor can add a trusted function to this fixed credential domain.
pub(super) async fn permissions(
    connection: &mut PgConnection,
    sensitive: bool,
    functions: &[String],
) -> Result<(), sqlx::Error> {
    let allowed = sql::readable_columns(sensitive);
    let valid:bool=sqlx::query_scalar(r#"
WITH reachable AS MATERIALIZED (
 SELECT oid FROM pg_roles WHERE pg_has_role(current_user,oid,'MEMBER')
), trusted AS MATERIALIZED (
 SELECT f.signature,p.oid,p.proowner,p.prosecdef,p.proretset,p.prorettype,p.provolatile,p.prokind,p.proconfig,n.oid AS namespace,n.nspowner AS namespace_owner,
        format('%I.%I(%s)',n.nspname,p.proname,oidvectortypes(p.proargtypes)) AS identity
 FROM unnest($2::text[]) f(signature)
 LEFT JOIN pg_proc p ON p.oid=to_regprocedure(f.signature)
 LEFT JOIN pg_namespace n ON n.oid=p.pronamespace
)
SELECT NOT EXISTS (
 SELECT 1 FROM trusted t WHERE t.oid IS NULL OR NOT COALESCE(
  t.signature=t.identity AND t.prosecdef AND NOT t.proretset AND t.prorettype='boolean'::regtype
  AND t.provolatile='s' AND t.prokind='f'
  AND t.proconfig @> ARRAY['search_path=pg_catalog, pg_temp']::text[]
  AND NOT pg_has_role(current_user,t.proowner,'MEMBER') AND has_function_privilege(t.oid,'EXECUTE')
  AND NOT pg_has_role(current_user,t.namespace_owner,'MEMBER')
  AND NOT EXISTS(SELECT 1 FROM reachable r WHERE has_schema_privilege(r.oid,t.namespace,'CREATE')),false)
) AND NOT EXISTS (
 SELECT 1 FROM pg_roles r WHERE pg_has_role(current_user,r.oid,'MEMBER')
 AND (r.rolsuper OR r.rolbypassrls OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication OR left(r.rolname::text,3)='pg_')
) AND NOT EXISTS (
 SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN reachable r
 WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname <> 'information_schema'
 AND c.relkind IN ('r','p','v','m','f') AND (
  pg_has_role(current_user,c.relowner,'MEMBER') OR
  (c.relname<>'database_placements' AND has_any_column_privilege(r.oid,c.oid,'SELECT') AND
   (NOT c.relrowsecurity OR NOT EXISTS(SELECT 1 FROM pg_policy policy WHERE policy.polrelid=c.oid))) OR
  has_any_column_privilege(r.oid,c.oid,'INSERT') OR has_any_column_privilege(r.oid,c.oid,'UPDATE') OR
  has_table_privilege(r.oid,c.oid,'DELETE,TRUNCATE,TRIGGER,REFERENCES') OR
  EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
   AND has_column_privilege(r.oid,c.oid,a.attnum,'SELECT') AND (
    c.relkind NOT IN ('r','p') OR
    NOT COALESCE(($1::jsonb -> regexp_replace(c.relname,'_p[0-9]{3}$','')) ? a.attname,false)))
 )) AND NOT EXISTS (
 SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace CROSS JOIN reachable r
 WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname <> 'information_schema'
 AND p.prosecdef AND has_function_privilege(r.oid,p.oid,'EXECUTE')
 AND NOT EXISTS(SELECT 1 FROM trusted t WHERE t.oid=p.oid)
) AND NOT EXISTS (SELECT 1 FROM reachable r WHERE has_schema_privilege(r.oid,'public','CREATE'))
"#).bind(allowed).bind(functions).fetch_one(connection).await?;
    if !valid {
        return Err(sqlx::Error::Protocol(
            "export credential grants exceed the projection boundary".into(),
        ));
    }
    Ok(())
}

fn dependency(error: anyhow::Error) -> Error {
    match error.downcast::<Error>() {
        Ok(error) => error,
        Err(error) => Error::Dependency(error),
    }
}

async fn hosted(
    client: &UserClient,
    context: UserContext,
    run_id: Uuid,
    collection: ExportCollection,
    limits: ExportLimits,
    cursor: Option<&ExportCursor>,
) -> Result<ExportPage, Error> {
    let start = Position::start(run_id, collection, Some(context), cursor)?;
    let routes = async {
        let mut tx = scope::begin(&client.control, context, run_id).await?;
        type CatalogRow=(Option<i16>,Option<String>,Option<String>,Option<String>);
        let routes:Vec<CatalogRow>=sqlx::query_as("SELECT s.run_shard,s.database_alias,d.role,d.status FROM runs r LEFT JOIN shard_placements s ON s.run_id=r.id LEFT JOIN database_placements d ON d.alias=s.database_alias WHERE r.id=$1 ORDER BY s.run_shard")
            .bind(run_id).fetch_all(&mut *tx).await?;
        if routes.is_empty() { return Err(anyhow::Error::from(Error::NotFound(run_id))); }
        tx.commit().await?;
        let mut resolved = Vec::with_capacity(routes.len());
        for (shard, alias, role, status) in routes {
            let (Some(shard),Some(alias))=(shard,alias) else { continue; };
            anyhow::ensure!(
                matches!(role.as_deref(), Some("shard" | "control_and_shard"))
                    && matches!(status.as_deref(), Some("active" | "draining")),
                "execution placement unavailable"
            );
            let pool = if alias == client.control_alias {
                client.control.clone()
            } else {
                client
                    .execution
                    .get(&alias)
                    .ok_or(Error::InvalidConfiguration(
                        "execution placement has no export credential binding",
                    ))?
                    .clone()
            };
            resolved.push((shard, alias, pool));
        }
        Ok::<_, anyhow::Error>(resolved)
    }
    .await
    .map_err(dependency)?;
    read(routes, start, limits, Some(context))
        .await
        .map_err(dependency)
}

pub(super) async fn standalone(
    database: &crate::context::database::DatabaseRouter,
    run_id: Uuid,
    collection: ExportCollection,
    limits: ExportLimits,
    cursor: Option<&ExportCursor>,
) -> Result<ExportPage, Error> {
    let start = Position::start(run_id, collection, None, cursor)?;
    let db = database.control().await.map_err(Error::Dependency)?;
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runs WHERE id=$1)")
        .bind(run_id)
        .fetch_one(db)
        .await
        .map_err(|e| Error::Dependency(e.into()))?;
    if !exists {
        return Err(Error::NotFound(run_id));
    }
    let routes = database
        .execution_read_routes_for_run(run_id)
        .await
        .map_err(Error::Dependency)?;
    read(routes, start, limits, None).await.map_err(dependency)
}

async fn read(
    routes: Vec<(i16, String, PgPool)>,
    mut position: Position,
    limits: ExportLimits,
    context: Option<UserContext>,
) -> anyhow::Result<ExportPage> {
    let mut page = ExportPage {
        records: Vec::new(),
        next: None,
        encoded_bytes: 2,
    };
    let first_shard = position.shard;
    let mut routes = routes
        .into_iter()
        .filter(|(shard, _, _)| *shard >= first_shard)
        .peekable();
    let projection = sql::projection(position.collection);
    while let Some((shard, alias, pool)) = routes.next() {
        let mut shards = vec![shard];
        while routes.peek().is_some_and(|(_, next, _)| next == &alias) {
            shards.push(routes.next().expect("peeked route").0);
        }
        let remaining = limits.rows as usize - page.records.len();
        let from = format!(
            "FROM {table} t {joins} CROSS JOIN LATERAL (SELECT {body} AS body OFFSET 0) p WHERE t.run_id=$1 AND t.run_shard=ANY($2::smallint[]) AND (t.run_shard>$3 OR (t.run_shard=$3 AND ($4::uuid IS NULL OR t.{key}>$4)))",
            table = projection.table,
            joins = projection.joins,
            body = projection.body,
            key = projection.key
        );
        let admission = "SELECT count(a.run_shard)=cardinality($2::smallint[]) AND COALESCE(bool_and(a.database_alias=$6 AND a.state IN ('open','draining')),false) AS valid FROM unnest($2::smallint[]) requested(shard) LEFT JOIN local_shard_admissions a ON a.run_id=$1 AND a.run_shard=requested.shard";
        let candidates = format!(
            "SELECT 0 AS kind,admission.valid,c.key,c.bytes,c.ordinal FROM admission LEFT JOIN LATERAL (SELECT jsonb_build_array(t.run_shard,t.{key}) AS key,octet_length(p.body)::bigint AS bytes,row_number() OVER(ORDER BY t.run_shard,t.{key}) AS ordinal {from} AND admission.valid ORDER BY t.run_shard,t.{key} LIMIT $5) c ON true",
            key = projection.key
        );
        let data = format!(
            "SELECT 0 AS kind,b.key,{body} AS body FROM selected b JOIN {table} t ON t.run_id=$1 AND t.run_shard=ANY($2::smallint[]) AND t.run_shard=(b.key->>0)::smallint AND t.{key}=(b.key->>1)::uuid {joins}",
            body = projection.body,
            table = projection.table,
            key = projection.key,
            joins = projection.joins
        );
        let statement =
            crate::db::bounded_read::statement(admission, &candidates, &data, shards.len() == 1);
        let allowance =
            limits.bytes as usize - page.encoded_bytes + usize::from(page.records.is_empty());
        let query = sqlx::query_as::<_, crate::db::bounded_read::Row>(&statement)
            .bind(position.run_id)
            .bind(&shards)
            .bind(position.shard)
            .bind(position.after)
            .bind((remaining + 1) as i64)
            .bind(&alias)
            .bind(allowance as i64);
        let records = if let Some(context) = context {
            let mut tx = scope::begin_read(&pool, context, position.run_id).await?;
            let records = query.fetch_all(&mut *tx).await?;
            tx.commit().await?;
            records
        } else {
            query.fetch_all(&pool).await?
        };
        let mut full = false;
        for (_, valid, key, bytes, body) in records {
            anyhow::ensure!(valid, "execution read route is no longer locally admitted");
            let Some(bytes) = bytes else {
                continue;
            };
            let key = key
                .ok_or_else(|| anyhow::anyhow!("export snapshot omitted its ordering key"))?
                .0;
            let shard = key
                .get(0)
                .and_then(serde_json::Value::as_i64)
                .and_then(|n| i16::try_from(n).ok())
                .ok_or_else(|| anyhow::anyhow!("invalid admitted export shard"))?;
            let id = key
                .get(1)
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("invalid admitted export identity"))?;
            let id = Uuid::parse_str(id)?;
            let cost = bytes as usize + usize::from(!page.records.is_empty());
            if page.records.len() == limits.rows as usize
                || page.encoded_bytes + cost > limits.bytes as usize
            {
                if page.records.is_empty() {
                    return Err(
                        Error::InvalidInput("export record exceeds page byte budget").into(),
                    );
                }
                full = true;
                break;
            }
            let body =
                body.ok_or_else(|| anyhow::anyhow!("export snapshot omitted an admitted key"))?;
            let record: super::ExportRecord = crate::contracts::run_export_json::decode(&body, 2)?;
            let size = body.len().max(super::encoded_size(&record)?)
                + usize::from(!page.records.is_empty());
            if page.encoded_bytes + size > limits.bytes as usize {
                if page.records.is_empty() {
                    return Err(Error::InvalidInput(
                        "export record encoding exceeds page byte budget",
                    )
                    .into());
                }
                full = true;
                break;
            }
            page.records.push(record);
            page.encoded_bytes += size;
            position.shard = shard;
            position.after = Some(id);
        }
        if full {
            page.next = Some(ExportCursor(serde_json::to_string(&position)?));
            return Ok(page);
        }
    }
    Ok(page)
}
