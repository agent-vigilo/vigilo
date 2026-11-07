//! Connection-time validation of fixed roles and embedded scheduler installation.
//!
//! Catalog reads are batched per connection, not performed per work item. The
//! deployment owns policy semantics; these guards reject bypass roles, unsafe
//! function definitions/grants and accessible work relations without row policies.
//! Scheduler follow-up work may use protected tables on its claimed transaction;
//! discovery still uses the restricted functions. Policy semantics belong to the host.

use std::time::Duration;

use sqlx::PgConnection;

use super::{
    Authority,
    DatabaseBinding,
    DatabaseCredential,
    DatabasePurpose,
    contracts::{
        CONTRACTS,
        normalize,
    },
};

pub(super) struct Guard {
    role: String,
    schema: String,
    owner: String,
    peers: Vec<String>,
    purpose: DatabasePurpose,
    authority: Authority,
    statement_ms: u128,
    lock_ms: u128,
}

#[derive(sqlx::FromRow)]
struct Installed {
    signature: String,
    body: Option<String>,
    result: Option<String>,
    arguments: Option<Vec<String>>,
    language: Option<String>,
    volatility: Option<String>,
    config: Option<Vec<String>>,
    owner: Option<String>,
    security_definer: Option<bool>,
    public_execute: Option<bool>,
    callable: Option<bool>,
    defaults: Option<i16>,
    ordinary: Option<bool>,
}

fn invalid() -> sqlx::Error {
    sqlx::Error::Protocol("fixed database authority or installation validation failed".into())
}

impl Guard {
    pub fn new(
        binding: &DatabaseBinding,
        credential: &DatabaseCredential,
        authority: Authority,
        timeout: Duration,
    ) -> Self {
        let policy = crate::context::database::DatabaseOperationTimeoutConfig::new(timeout)
            .expect("validated timeout");
        Self {
            role: credential.database_role.clone(),
            schema: binding.schema.clone(),
            owner: binding.function_owner.clone(),
            peers: [&binding.scheduler, &binding.execution, &binding.user]
                .into_iter()
                .map(|c| c.database_role.clone())
                .filter(|r| r != &credential.database_role)
                .collect(),
            purpose: binding.purpose,
            authority,
            statement_ms: policy.statement_timeout().as_millis().max(1),
            lock_ms: policy.lock_timeout().as_millis().max(1),
        }
    }

    pub async fn initialize(&self, connection: &mut PgConnection) -> Result<(), sqlx::Error> {
        sqlx::query("SELECT set_config('search_path',format('pg_catalog,%I,pg_temp',$1::text),false),set_config('row_security','on',false),set_config('vigilo.tenant_id','',false),set_config('vigilo.actor_id','',false),set_config('vigilo.run_id','',false),set_config('vigilo.run_batch','',false),set_config('statement_timeout',$2,false),set_config('lock_timeout',$3,false)")
            .bind(&self.schema).bind(format!("{}ms",self.statement_ms)).bind(format!("{}ms",self.lock_ms))
            .execute(&mut *connection).await?;
        self.validate(connection).await
    }

    pub async fn validate(&self, connection: &mut PgConnection) -> Result<(), sqlx::Error> {
        let (valid,quoted_schema):(bool,String)=sqlx::query_as(r#"
SELECT
    current_user=$1 AND session_user=$1 AND current_setting('row_security')='on'
    AND current_setting('search_path')=format('pg_catalog,%I,pg_temp',$2::text)
    AND EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=$2)
    AND has_schema_privilege($2,'USAGE') AND NOT has_schema_privilege($2,'CREATE')
    AND EXISTS(SELECT 1 FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
       JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$2 AND c.relname='runs'
       AND a.attname='tenant_id' AND NOT a.attisdropped)
    AND NOT EXISTS(SELECT 1 FROM pg_roles r WHERE pg_has_role(current_user,r.oid,'MEMBER')
       AND (r.rolsuper OR r.rolbypassrls OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication
            OR r.rolname=$3 OR r.rolname=ANY($4::text[])))
    AND EXISTS(SELECT 1 FROM pg_roles o WHERE o.rolname=$3 AND NOT o.rolcanlogin
       AND NOT o.rolsuper AND NOT o.rolbypassrls AND NOT o.rolcreaterole AND NOT o.rolcreatedb
       AND NOT o.rolreplication AND NOT has_schema_privilege(o.oid,$2,'CREATE')
       AND NOT EXISTS(SELECT 1 FROM pg_roles parent WHERE pg_has_role(o.oid,parent.oid,'MEMBER')
          AND (parent.rolsuper OR parent.rolbypassrls OR parent.rolcreaterole OR parent.rolcreatedb OR parent.rolreplication)))
    AND NOT EXISTS(SELECT 1 FROM unnest($7::text[]) required(name)
        WHERE NOT has_table_privilege($3,format('%I.%I',$2,required.name),'SELECT'))
    AND NOT EXISTS(SELECT 1 FROM unnest($8::text[]) required(name)
        WHERE NOT has_table_privilege($3,format('%I.%I',$2,required.name),'UPDATE'))
    AND NOT EXISTS(
       SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
       WHERE n.nspname=$2 AND c.relkind IN ('r','p','v','m','f','S')
       AND (pg_has_role(current_user,c.relowner,'MEMBER') OR pg_has_role($3,c.relowner,'MEMBER')))
    AND NOT EXISTS(
       SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
       WHERE n.nspname=$2 AND c.relkind IN ('r','p')
       AND (has_table_privilege(c.oid,'TRUNCATE') OR has_table_privilege(c.oid,'TRIGGER')
         OR has_table_privilege(c.oid,'REFERENCES')
         OR (c.relname='database_placements' AND (has_any_column_privilege(c.oid,'INSERT')
             OR has_any_column_privilege(c.oid,'UPDATE') OR has_table_privilege(c.oid,'DELETE')))
         OR (($5 AND c.relname<>'database_placements' OR EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attname='tenant_id' AND NOT a.attisdropped))
             AND (has_any_column_privilege(c.oid,'SELECT') OR has_any_column_privilege(c.oid,'INSERT')
                  OR has_any_column_privilege(c.oid,'UPDATE') OR has_table_privilege(c.oid,'DELETE'))
             AND (($5 AND NOT EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attname='tenant_id' AND NOT a.attisdropped))
                  OR NOT c.relrowsecurity OR NOT EXISTS(SELECT 1 FROM pg_policy p WHERE p.polrelid=c.oid)))))
    AND NOT EXISTS(
       SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
       WHERE n.nspname=$2 AND c.relkind IN ('v','m','f')
       AND (has_any_column_privilege(c.oid,'SELECT') OR has_any_column_privilege(c.oid,'INSERT')
           OR has_any_column_privilege(c.oid,'UPDATE') OR has_table_privilege(c.oid,'DELETE')))
    AND NOT EXISTS(
       SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
       WHERE n.nspname=$2 AND p.prosecdef AND p.prorettype<>'trigger'::regtype
       AND has_function_privilege(p.oid,'EXECUTE') AND NOT p.oid=ANY(
           SELECT to_regprocedure(format('%I.%s',$2,signature)) FROM unnest($6::text[]) signature)),
    quote_ident($2::text)
"#).bind(&self.role).bind(&self.schema).bind(&self.owner).bind(&self.peers)
            .bind(self.authority==Authority::Scheduler).bind(self.allowed_signatures())
            .bind(self.owner_reads()).bind(self.owner_updates())
            .fetch_one(&mut *connection).await?;
        if !valid {
            return Err(invalid());
        }
        let signatures: Vec<_> = CONTRACTS.iter().map(|c| c.signature).collect();
        let rows:Vec<Installed>=sqlx::query_as(r#"
SELECT expected.signature,p.prosrc AS body,pg_get_function_result(p.oid) AS result,
       p.proargnames[1:p.pronargs] AS arguments,l.lanname AS language,p.provolatile::text AS volatility,
       NOT p.proisstrict AND NOT p.proleakproof AND p.prokind='f' AND p.proparallel='u' AND p.provariadic=0 AS ordinary,
       p.proconfig AS config,o.rolname AS owner,p.prosecdef AS security_definer,p.pronargdefaults AS defaults,
       EXISTS(SELECT 1 FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
              WHERE a.grantee=0 AND a.privilege_type='EXECUTE') AS public_execute,
       has_function_privilege(p.oid,'EXECUTE') AS callable
FROM unnest($2::text[]) expected(signature)
LEFT JOIN pg_proc p ON p.oid=to_regprocedure(format('%I.%s',$1,expected.signature))
LEFT JOIN pg_roles o ON o.oid=p.proowner
LEFT JOIN pg_language l ON l.oid=p.prolang
"#).bind(&self.schema).bind(signatures).fetch_all(connection).await?;
        let allowed = self.allowed_signatures();
        for row in rows {
            let contract = CONTRACTS
                .iter()
                .find(|c| c.signature == row.signature)
                .ok_or_else(invalid)?;
            let expected = contract.definition(&quoted_schema);
            let args = row.arguments.unwrap_or_default();

            if row.body.as_deref().map(str::trim) != Some(expected.body.as_str())
                || row.result.as_deref().map(normalize) != Some(expected.result)
                || args != expected.arguments
                || row.language.as_deref() != Some(expected.language.as_str())
                || row.volatility.as_deref() != Some(expected.volatility)
                || row.config.as_deref()
                    != Some(&["search_path=pg_catalog, pg_temp".to_string()][..])
                || row.owner.as_deref() != Some(&self.owner)
                || row.security_definer != Some(true)
                || row.ordinary != Some(true)
                || row.defaults != Some(0)
                || row.public_execute != Some(false)
                || row.callable != Some(allowed.contains(&contract.signature))
            {
                return Err(sqlx::Error::Protocol(format!(
                    "scheduler installation mismatch: {}",
                    contract.signature
                )));
            }
        }
        Ok(())
    }

    // Match the tables used by the installed functions, including UPDATE privilege
    // required for cursor row locking. Deployment policies still govern visibility.
    fn owner_reads(&self) -> Vec<&'static str> {
        let mut tables = vec!["outbox_delivery_queue", "outbox_events"];
        if self.purpose != DatabasePurpose::Execution {
            tables.extend([
                "runs",
                "run_plans",
                "run_control_deliveries",
                "run_shard_dispatch_cursors",
                "shard_placements",
                "database_placements",
            ]);
        }
        if self.purpose != DatabasePurpose::Control {
            tables.extend([
                "run_chunks",
                "run_shard_summaries",
                "local_shard_admissions",
                "run_chunk_settlement_consumptions",
            ]);
        }
        tables
    }

    fn owner_updates(&self) -> Vec<&'static str> {
        let mut tables = vec!["outbox_delivery_queue"];
        if self.purpose != DatabasePurpose::Execution {
            tables.extend([
                "database_placements",
                "runs",
                "run_plans",
                "run_control_deliveries",
                "run_shard_dispatch_cursors",
            ]);
        }
        tables
    }

    pub fn allowed_signatures(&self) -> Vec<&'static str> {
        if self.authority != Authority::Scheduler {
            return Vec::new();
        }
        CONTRACTS
            .iter()
            .filter(|c| match self.purpose {
                DatabasePurpose::Control => c.control,
                DatabasePurpose::Execution => c.execution,
                DatabasePurpose::ControlAndExecution => true,
            })
            .map(|c| c.signature)
            .collect()
    }
}
