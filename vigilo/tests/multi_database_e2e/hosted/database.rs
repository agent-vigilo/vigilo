//! Run-owned databases and synthetic fixed-role policies; not deployment policy.
//! Test logins cannot bypass protections. Function owners can discover candidates;
//! ordinary work requires exact transaction context, including recovery batch pairs.
use std::collections::BTreeMap;

use sqlx::{
    ConnectOptions,
    PgPool,
    Row,
};
use vigilo::operations::{
    DatabaseBinding,
    DatabaseBindings,
    DatabaseCredential,
    DatabasePools,
    DatabasePurpose,
};

use super::*;

pub(super) struct Databases {
    pub admin: Vec<PgPool>,
    pub bases: Vec<PgPool>,
    pub urls: Vec<String>,
    pub names: Vec<String>,
    pub roles: [String; 4],
    pub operator: String,
    pub submitter: String,
}
impl Databases {
    pub async fn new() -> anyhow::Result<Self> {
        let id = Uuid::now_v7().simple().to_string();
        let mut bases = Vec::new();
        for name in [PRIMARY_DATABASE_URL_ENV, SHARD_DATABASE_URL_ENV] {
            bases.push(connect(&std::env::var(name)?).await?);
        }
        Ok(Self {
            admin: vec![],
            bases,
            urls: vec![],
            names: vec![
                format!("hosted_{id}_control"),
                format!("hosted_{id}_execution"),
            ],
            operator: format!("op_{id}"),
            submitter: format!("submit_{id}"),
            roles: [
                format!("h_{id}_s"),
                format!("h_{id}_e"),
                format!("h_{id}_u"),
                format!("h_{id}_o"),
            ],
        })
    }
    pub async fn setup(&mut self, restricted: bool) -> anyhow::Result<()> {
        for i in 0..2 {
            sqlx::raw_sql(&format!("CREATE DATABASE {}", self.names[i]))
                .execute(&self.bases[i])
                .await?;
            let mut url = self.bases[i].connect_options().to_url_lossy();
            url.set_path(&self.names[i]);
            url.set_query(None);
            self.urls.push(url.to_string());
            let pool = connect(url.as_str()).await?;
            self.admin.push(pool);
            migrate(&self.admin[i]).await?;
        }
        seed_sentiment_evaluator(&self.admin[0]).await?;
        sqlx::query("UPDATE database_placements SET database_url_env='DATABASE_URL',role='control' WHERE alias='primary'").execute(&self.admin[0]).await?;
        sqlx::query("INSERT INTO database_placements(alias,database_url_env,role,status) VALUES('shard_001','VIGILO_TEST_SHARD_001_DATABASE_URL','shard','active')").execute(&self.admin[0]).await?;
        // Each installation knows its own physical alias.
        sqlx::query("UPDATE database_placements SET alias='shard_001',database_url_env='VIGILO_TEST_SHARD_001_DATABASE_URL',role='shard' WHERE alias='primary'").execute(&self.admin[1]).await?;
        for (i, pool) in self.admin.iter().enumerate() {
            if restricted {
                self.install(pool, i == 0).await?;
            } else {
                for role in [&self.operator, &self.submitter] {
                    let exists: bool = sqlx::query_scalar(
                        "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)",
                    )
                    .bind(role)
                    .fetch_one(pool)
                    .await?;
                    if !exists {
                        sqlx::raw_sql(&format!(
                            "CREATE ROLE {role} LOGIN SUPERUSER PASSWORD 'fixture'"
                        ))
                        .execute(pool)
                        .await?;
                    }
                }
            }
        }
        Ok(())
    }
    async fn install(&self, pool: &PgPool, control: bool) -> anyhow::Result<()> {
        for (i, role) in self.roles.iter().enumerate() {
            let exists: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)")
                    .bind(role)
                    .fetch_one(pool)
                    .await?;
            if !exists {
                sqlx::raw_sql(&format!(
                    "CREATE ROLE {role} {} PASSWORD 'fixture'",
                    if i == 3 { "NOLOGIN" } else { "LOGIN" }
                ))
                .execute(pool)
                .await?;
            }
            sqlx::raw_sql(&format!("GRANT USAGE ON SCHEMA public TO {role}"))
                .execute(pool)
                .await?;
        }
        let [scheduler, execution, user, owner] = &self.roles;
        let tables = sqlx::query("SELECT c.relname, EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attname='run_id' AND NOT a.attisdropped) AS has_run FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('r','p') AND NOT c.relispartition AND EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attname='tenant_id' AND NOT a.attisdropped)").fetch_all(pool).await?;
        for row in tables {
            let table: String = row.try_get("relname")?;
            let key = if row.try_get::<bool, _>("has_run")? {
                Some("run_id")
            } else if table == "runs" {
                Some("id")
            } else if table == "outbox_events" {
                Some("aggregate_id")
            } else {
                None
            };
            let scope = "tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid";
            let actor = "current_setting('vigilo.actor_id',true)";
            let exact = match key {
                Some(key) => format!(
                    "(({scope} AND {key}=NULLIF(current_setting('vigilo.run_id',true),'')::uuid) OR ({key},tenant_id) IN (SELECT key::uuid,value::uuid FROM jsonb_each_text(NULLIF(current_setting('vigilo.run_batch',true),'')::jsonb)))"
                ),
                None if table == "outbox_delivery_queue" => "EXISTS(SELECT 1 FROM outbox_events e WHERE e.id=outbox_delivery_queue.event_id AND e.tenant_id=outbox_delivery_queue.tenant_id)".into(),
                None => scope.into(),
            };
            sqlx::raw_sql(&format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY; CREATE POLICY user_context ON {table} TO {user} USING ({scope} AND {actor}=tenant_id::text); CREATE POLICY system_context ON {table} TO {scheduler},{execution} USING ({actor}='' AND {exact}); CREATE POLICY discovery ON {table} TO {owner} USING(true); GRANT SELECT,INSERT,UPDATE,DELETE ON {table} TO {scheduler},{execution},{user}; GRANT SELECT,UPDATE ON {table} TO {owner};")).execute(pool).await?;
        }
        sqlx::raw_sql(&format!("GRANT SELECT ON database_placements TO {scheduler},{execution},{user}; GRANT SELECT,UPDATE ON database_placements TO {owner}; GRANT SELECT ON evaluators TO {execution},{user},{owner}")).execute(pool).await?;
        let functions: Vec<(String,String)> = sqlx::query_as("SELECT p.proname,p.oid::regprocedure::text FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.prosecdef AND p.prorettype<>'trigger'::regtype ORDER BY p.proname").fetch_all(pool).await?;
        anyhow::ensure!(functions.len() == 17, "unexpected function installation");
        for (name, signature) in functions {
            sqlx::raw_sql(&format!("ALTER FUNCTION {signature} OWNER TO {owner}"))
                .execute(pool)
                .await?;
            let shared = matches!(
                name.as_str(),
                "vigilo_claim_outbox" | "vigilo_outbox_backlog"
            );
            let local = matches!(
                name.as_str(),
                "vigilo_claim_chunk_recovery" | "vigilo_pending_aggregation"
            );
            let worker = name == "vigilo_verify_worker_identity";
            if shared || local && !control || !local && !worker && control || worker && !control {
                let role = if worker { execution } else { scheduler };
                sqlx::raw_sql(&format!("GRANT EXECUTE ON FUNCTION {signature} TO {role}"))
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }
    pub fn pools(&self) -> anyhow::Result<DatabasePools> {
        let mut bindings = BTreeMap::new();
        for (i, alias) in [PRIMARY_ALIAS, SHARD_ALIAS].iter().enumerate() {
            let credential = |index: usize| -> anyhow::Result<DatabaseCredential> {
                let mut url = url::Url::parse(&self.urls[i])?;
                url.set_username(&self.roles[index])
                    .map_err(|_| anyhow::anyhow!("fixture username"))?;
                url.set_password(Some("fixture"))
                    .map_err(|_| anyhow::anyhow!("fixture password"))?;
                Ok(DatabaseCredential {
                    database_url: url.to_string(),
                    database_role: self.roles[index].clone(),
                })
            };
            bindings.insert(
                alias.to_string(),
                DatabaseBinding {
                    purpose: if i == 0 {
                        DatabasePurpose::Control
                    } else {
                        DatabasePurpose::Execution
                    },
                    schema: "public".into(),
                    function_owner: self.roles[3].clone(),
                    scheduler: credential(0)?,
                    execution: credential(1)?,
                    user: credential(2)?,
                },
            );
        }
        let mut config = DatabaseBindings::new(PRIMARY_ALIAS.into(), bindings);
        config.max_connections = 4;
        Ok(DatabasePools::new(config)?)
    }
    pub async fn cleanup(&self) -> anyhow::Result<()> {
        for pool in &self.admin {
            pool.close().await;
        }
        for (i, base) in self.bases.iter().enumerate() {
            sqlx::raw_sql(&format!(
                "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                self.names[i]
            ))
            .execute(base)
            .await?;
        }
        for base in &self.bases {
            for role in self.roles.iter().chain([&self.operator, &self.submitter]) {
                sqlx::raw_sql(&format!("DROP ROLE IF EXISTS {role}"))
                    .execute(base)
                    .await?;
            }
            base.close().await;
        }
        Ok(())
    }
}
