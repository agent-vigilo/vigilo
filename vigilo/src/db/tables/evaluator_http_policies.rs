//! Batched operator-policy reads. The host validates every loaded rule before
//! caching; absent rules deny access. SQL never runs inside a Wasm HTTP call.
use std::collections::BTreeMap;

use sqlx::PgPool;
use uuid::Uuid;
use vigilo::evaluator_http::{
    Action,
    Rule,
};

/// Refresh current policy under ownership already verified by worker admission.
/// The short transaction clears actor context and never spans external I/O.
/// Cache hits bypass this function; a miss reads one bounded batch, not one row
/// or query per evaluator, case, or outbound HTTP request.
pub(crate) async fn load_for_run(
    pool: &PgPool,
    tenant_id: Uuid,
    run_id: Uuid,
    ids: &[Uuid],
) -> anyhow::Result<BTreeMap<Uuid, Vec<Rule>>> {
    anyhow::ensure!(ids.len() <= 256, "HTTP policy batch exceeds 256 evaluators");
    if ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut tx = pool.begin().await?;
    crate::db::scope::set_system(&mut tx, tenant_id, run_id).await?;
    let policies = load(&mut *tx, ids).await?;
    tx.commit().await?;
    Ok(policies)
}

/// Read a bounded policy batch from one committed database statement snapshot.
pub(crate) async fn load<'e, E: sqlx::Executor<'e, Database = sqlx::Postgres>>(
    db: E,
    ids: &[Uuid],
) -> anyhow::Result<BTreeMap<Uuid, Vec<Rule>>> {
    anyhow::ensure!(ids.len() <= 256, "HTTP policy batch exceeds 256 evaluators");
    if ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let rows = sqlx::query_as::<_, (Uuid, String, String, String, Vec<String>)>(
        "SELECT evaluator_id, uri, method, action::text, networks FROM evaluator_http_policies WHERE tenant_id = NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND evaluator_id = ANY($1) ORDER BY evaluator_id, id LIMIT 4097"
    ).bind(ids).fetch_all(db).await?;
    anyhow::ensure!(rows.len() <= 4096, "HTTP policy batch exceeds 4096 rules");
    let mut result = BTreeMap::<_, Vec<_>>::new();
    for (id, uri, method, action, networks) in rows {
        let action = match action.as_str() {
            "allow" => Action::Allow,
            "deny" => Action::Deny,
            _ => anyhow::bail!("unknown HTTP policy action"),
        };
        result.entry(id).or_default().push(Rule {
            uri,
            method,
            action,
            networks,
        });
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires PostgreSQL"]
    async fn policy_refresh_binds_verified_owner_and_clears_actor_without_pool_leakage(
        pool: PgPool,
    ) {
        use anyhow::ensure;
        use sqlx::postgres::{
            PgConnectOptions,
            PgPoolOptions,
        };
        let owner = Uuid::now_v7();
        let other = Uuid::now_v7();
        let run = Uuid::now_v7();
        let role = format!("policy_refresh_{}", Uuid::now_v7().simple());
        let id: Uuid = sqlx::query_scalar("INSERT INTO evaluators(namespace,name,version,content_hash,wasm_bytes,wasm_size_bytes,interface_name,interface_version,wit_world,abi_contract_hash,abi_adapter,runtime,runtime_version,runtime_fingerprint) VALUES ('test','scoped-http','1','hash','',0,'evaluator','1','world','hash','adapter','wasmtime','test','test') RETURNING id").fetch_one(&pool).await.unwrap();
        for (tenant, action) in [(owner, "allow"), (other, "deny"), (Uuid::nil(), "allow")] {
            sqlx::query("INSERT INTO evaluator_http_policies(tenant_id,evaluator_id,uri,method,action) VALUES($1,$2,'https://judge.example/v1','POST',$3::evaluator_http_policy_action)")
                .bind(tenant).bind(id).bind(action).execute(&pool).await.unwrap();
        }
        sqlx::raw_sql(&format!("CREATE ROLE {role} LOGIN PASSWORD 'fixture'; GRANT USAGE ON SCHEMA public TO {role}; GRANT SELECT ON evaluator_http_policies TO {role}; ALTER TABLE evaluator_http_policies ENABLE ROW LEVEL SECURITY; CREATE POLICY refresh_scope ON evaluator_http_policies TO {role} USING(tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND current_setting('vigilo.run_id',true)='{run}' AND current_setting('vigilo.actor_id',true)='');"))
            .execute(&pool).await.unwrap();
        let options = std::env::var("DATABASE_URL")
            .unwrap()
            .parse::<PgConnectOptions>()
            .unwrap()
            .database(pool.connect_options().get_database().unwrap())
            .username(&role)
            .password("fixture");
        let restricted = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .unwrap();
        let result: anyhow::Result<()> = async {
            // A stale session identity must neither authorize nor poison a refresh.
            sqlx::raw_sql(&format!("SET vigilo.tenant_id='{other}'; SET vigilo.actor_id='{other}'; SET vigilo.run_id='{other}'; SET vigilo.run_batch='stale';"))
                .execute(&restricted).await?;
            let cache = vigilo::evaluator_http::cache::Cache::default();
            let calls = std::sync::atomic::AtomicUsize::new(0);
            for (index,tenant) in [owner,other,owner,other].into_iter().enumerate() {
                let (rows,queries) = crate::db::workflows::run_shard_summary::test_support::record_queries(cache.snapshots(tenant, &[id], |ids| {
                    let calls = &calls;
                    let restricted = &restricted;
                    async move {
                        calls.fetch_add(1,std::sync::atomic::Ordering::SeqCst);
                        load_for_run(restricted,tenant,run,&ids).await
                    }
                })).await;
                ensure!(queries.len() == if index < 2 { 2 } else { 0 }, "one scope submission plus one batch read on miss; zero on hit: {queries:?}");
                let rows=rows?;
                ensure!(rows[&id].authorize(id,"POST","https://judge.example/v1").is_ok() == (tenant==owner));
            }
            ensure!(calls.load(std::sync::atomic::Ordering::SeqCst)==2,"warm hits must not refresh policy");
            let state: (String,String,String,String) = sqlx::query_as("SELECT current_setting('vigilo.tenant_id'),current_setting('vigilo.actor_id'),current_setting('vigilo.run_id'),current_setting('vigilo.run_batch')").fetch_one(&restricted).await?;
            ensure!(state == (other.to_string(),other.to_string(),other.to_string(),"stale".into()),"transaction scope leaked into pooled session");
            // No scope fallback on a role/query failure, including after a warm grant.
            sqlx::raw_sql(&format!("REVOKE SELECT ON evaluator_http_policies FROM {role}"))
                .execute(&pool).await?;
            ensure!(load_for_run(&restricted,owner,run,&[id]).await.is_err());
            ensure!(load_for_run(&restricted,owner,run,&[]).await?.is_empty());
            ensure!(load_for_run(&restricted,owner,run,&[id;257]).await.is_err());
            let actor: String = sqlx::query_scalar("SELECT current_setting('vigilo.actor_id')").fetch_one(&restricted).await?;
            ensure!(actor==other.to_string(),"failed refresh did not roll back scope");
            Ok(())
        }.await;
        restricted.close().await;
        sqlx::raw_sql(&format!("DROP POLICY refresh_scope ON evaluator_http_policies; DROP OWNED BY {role}; DROP ROLE {role};"))
            .execute(&pool).await.unwrap();
        result.unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires PostgreSQL"]
    async fn policy_load_is_scoped_and_revisions_change_with_rules(pool: PgPool) {
        let id: Uuid = sqlx::query_scalar("INSERT INTO evaluators(namespace,name,version,content_hash,wasm_bytes,wasm_size_bytes,interface_name,interface_version,wit_world,abi_contract_hash,abi_adapter,runtime,runtime_version,runtime_fingerprint) VALUES ('test','http','1','hash','',0,'evaluator','1','world','hash','adapter','wasmtime','test','test') RETURNING id").fetch_one(&pool).await.unwrap();
        let unknown = Uuid::from_u128(1);
        assert!(load(&pool, &[id, unknown]).await.unwrap().is_empty());
        sqlx::query("INSERT INTO evaluator_http_policies(evaluator_id,uri,method,action) VALUES ($1,'https://judge.example/v1','POST','allow')").bind(id).execute(&pool).await.unwrap();
        let mut rows = load(&pool, &[id, unknown]).await.unwrap();
        assert_eq!(rows.len(), 1);
        let first = vigilo::evaluator_http::Snapshot::new(id, rows.remove(&id).unwrap()).unwrap();
        assert!(
            first
                .authorize(id, "POST", "https://judge.example/v1")
                .is_ok()
        );
        sqlx::query("INSERT INTO evaluator_http_policies(evaluator_id,uri,method,action) VALUES ($1,'https://judge.example/v1','POST','deny')").bind(id).execute(&pool).await.unwrap();
        let next = vigilo::evaluator_http::Snapshot::new(
            id,
            load(&pool, &[id]).await.unwrap().remove(&id).unwrap(),
        )
        .unwrap();
        assert_ne!(first.revision(), next.revision());
        assert!(
            next.authorize(id, "POST", "https://judge.example/v1")
                .is_err()
        );
        assert!(
            first
                .authorize(id, "POST", "https://judge.example/v1")
                .is_ok()
        );
    }
}
