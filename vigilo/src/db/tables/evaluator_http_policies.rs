//! Batched operator-policy reads. The host validates every loaded rule before
//! caching; absent rules deny access. SQL never runs inside a Wasm HTTP call.
use std::collections::BTreeMap;

use sqlx::PgPool;
use uuid::Uuid;
use vigilo::evaluator_http::{
    Action,
    Rule,
};

/// Read a bounded policy batch from one committed database statement snapshot.
pub(crate) async fn load(db: &PgPool, ids: &[Uuid]) -> anyhow::Result<BTreeMap<Uuid, Vec<Rule>>> {
    anyhow::ensure!(ids.len() <= 256, "HTTP policy batch exceeds 256 evaluators");
    if ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let rows = sqlx::query_as::<_, (Uuid, String, String, String, Vec<String>)>(
        "SELECT evaluator_id, uri, method, action::text, networks FROM evaluator_http_policies WHERE evaluator_id = ANY($1) ORDER BY evaluator_id, id LIMIT 4097"
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
