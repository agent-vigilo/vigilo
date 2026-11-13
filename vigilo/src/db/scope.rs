//! Transaction-local caller identity on fixed-credential connection pools.
//!
//! Identity is supplied by the trusted host, never decoded from workload data.
//! Every protected statement uses the returned transaction. Setting all values
//! together prevents inherited optional fields; SQLx rollback-on-drop also covers
//! setup errors and cancelled futures. This module never switches database roles.

use sqlx::{
    PgPool,
    Postgres,
    Row,
    Transaction,
};
use uuid::Uuid;

/// Identity supplied by an authenticated embedding host.
#[derive(Clone, Copy, Debug)]
pub struct UserContext {
    tenant_id: Uuid,
    actor_id: Uuid,
}
impl UserContext {
    /// Requires explicit, nonnil identifiers. Construct after authenticating the
    /// caller; this validates shape, not the caller's authority to claim an identity.
    pub fn new(tenant_id: Uuid, actor_id: Uuid) -> Result<Self, &'static str> {
        if tenant_id.is_nil() || actor_id.is_nil() {
            return Err("caller identifiers must be nonnil");
        }
        Ok(Self {
            tenant_id,
            actor_id,
        })
    }
    /// Fixed request settings for one short transaction. Only typed UUIDs enter SQL.
    pub(crate) fn settings(self, run_id: Uuid) -> String {
        format!(
            "SET LOCAL vigilo.tenant_id = '{}'; SET LOCAL vigilo.actor_id = '{}'; SET LOCAL vigilo.run_id = '{run_id}'; SET LOCAL vigilo.run_batch = ''; ",
            self.tenant_id, self.actor_id
        )
    }
    pub(crate) fn tenant_id(self) -> Uuid {
        self.tenant_id
    }
}

/// Starts one transaction and installs all request settings in one SQL statement.
/// No context-setting query is performed per row or per query in the transaction.
pub(crate) async fn begin(
    pool: &PgPool,
    context: UserContext,
    run_id: Uuid,
) -> Result<Transaction<'static, Postgres>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT set_config('vigilo.tenant_id',$1,true), set_config('vigilo.actor_id',$2,true), set_config('vigilo.run_id',$3,true), set_config('vigilo.run_batch','',true)")
        .bind(context.tenant_id.to_string()).bind(context.actor_id.to_string()).bind(run_id.to_string())
        .execute(&mut *tx).await?;
    Ok(tx)
}

/// Fixed transaction-local system identity. Only canonical typed UUIDs enter
/// this SQL; never append caller-supplied SQL or interpolate workload values.
/// Call inside an already-open SQLx transaction so cancellation rolls it back.
/// These commands can share a submission with a following fixed statement,
/// whose row policies must see the settings before that statement starts.
pub(crate) fn system_settings(tenant_id: Uuid, run_id: Uuid) -> String {
    format!(
        "SET LOCAL vigilo.tenant_id = '{tenant_id}'; SET LOCAL vigilo.run_id = '{run_id}'; SET LOCAL vigilo.actor_id = ''; SET LOCAL vigilo.run_batch = ''; "
    )
}

/// Opaque identity verified against accepted execution-local records. This is
/// never deserialized from a queue message; it can survive a route-validation
/// error so exceptional control discovery uses the already verified owner.
#[derive(Debug, Clone, Copy)]
pub(crate) struct VerifiedWorkerIdentity {
    tenant_id: Uuid,
    run_id: Uuid,
    run_shard: i16,
}
impl VerifiedWorkerIdentity {
    pub(crate) fn tenant_id(self) -> Uuid {
        self.tenant_id
    }
    pub(crate) fn run_id(self) -> Uuid {
        self.run_id
    }
    pub(crate) fn run_shard(self) -> i16 {
        self.run_shard
    }
}
impl std::fmt::Display for VerifiedWorkerIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("verified worker route admission")
    }
}

/// Verify untrusted queue identity against local accepted records and bind only
/// the returned owner. The restricted function holds the movement fence through
/// this transaction. No match returns None; the caller must roll back without protected work.
pub(crate) async fn verify_worker(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    run_shard: i16,
    chunk_id: Uuid,
    tenant_id: Uuid,
    plan_digest: &str,
) -> Result<Option<VerifiedWorkerIdentity>, sqlx::Error> {
    let row = sqlx::query(
        r#"
        WITH verified AS MATERIALIZED (
            SELECT * FROM vigilo_verify_worker_identity($1,$2,$3,$4,$5)
        )
        SELECT v.tenant_id AS verified_tenant_id,v.run_id AS verified_run_id,
               set_config('vigilo.tenant_id',v.tenant_id::text,true),
               set_config('vigilo.run_id',v.run_id::text,true),
               set_config('vigilo.actor_id','',true),set_config('vigilo.run_batch','',true)
        FROM verified v
    "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .bind(chunk_id)
    .bind(tenant_id)
    .bind(plan_digest)
    .fetch_optional(&mut **tx)
    .await?;
    row.map(|row| {
        Ok(VerifiedWorkerIdentity {
            tenant_id: row.try_get("verified_tenant_id")?,
            run_id: row.try_get("verified_run_id")?,
            run_shard,
        })
    })
    .transpose()
}

/// Bind verified ownership and retain the shared movement fence in one submission.
/// Only typed UUID/shard values are interpolated into these fixed commands.
pub(crate) async fn lock_system_shard(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    run_id: Uuid,
    shard: i16,
) -> Result<(), sqlx::Error> {
    let sql = format!(
        "{} SELECT pg_advisory_xact_lock_shared(hashtextextended('{run_id}'::uuid::text,{shard}::bigint))",
        system_settings(tenant_id, run_id)
    );
    sqlx::Executor::execute(&mut **tx, sql.as_str()).await?;
    Ok(())
}

/// Connection-time guard for a dedicated restricted credential domain. Policy
/// contents belong to the deployment; this rejects obvious bypasses and any
/// directly accessible unprotected table carrying ownership metadata.
pub(crate) async fn validate_connection(
    connection: &mut sqlx::PgConnection,
    expected_role: &str,
) -> Result<(), sqlx::Error> {
    let valid: bool = sqlx::query_scalar(r#"
        SELECT current_user = $1 AND NOT r.rolsuper AND NOT r.rolbypassrls
          AND NOT r.rolcreaterole AND NOT r.rolcreatedb AND NOT r.rolreplication
          AND EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid='runs'::regclass AND a.attname='tenant_id' AND NOT a.attisdropped)
          AND NOT EXISTS (
            SELECT 1 FROM pg_class c JOIN pg_attribute a ON a.attrelid=c.oid
            WHERE c.relkind IN ('r','p') AND a.attname='tenant_id' AND NOT a.attisdropped
              AND (pg_has_role(current_user,c.relowner,'MEMBER') OR
                   ((has_table_privilege(c.oid,'SELECT') OR has_table_privilege(c.oid,'INSERT') OR has_table_privilege(c.oid,'UPDATE') OR has_table_privilege(c.oid,'DELETE'))
                    AND (NOT c.relrowsecurity OR NOT EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid=c.oid)))))
        FROM pg_roles r WHERE r.rolname=current_user
    "#).bind(expected_role).fetch_one(&mut *connection).await?;
    if !valid {
        return Err(sqlx::Error::Protocol(
            "database role or row protection does not satisfy the operation boundary".into(),
        ));
    }
    sqlx::query("SELECT set_config('vigilo.tenant_id','',false), set_config('vigilo.actor_id','',false), set_config('vigilo.run_id','',false), set_config('vigilo.run_batch','',false)")
        .execute(connection).await?;
    Ok(())
}

/// Install trusted system identity inside an already-owned transaction. Call once
/// before protected work; rollback-on-drop covers setup failure and cancellation.
pub(crate) async fn set_system(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    run_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::Executor::execute(&mut **tx, system_settings(tenant_id, run_id).as_str()).await?;
    Ok(())
}

/// Bind an exact map of claimed run IDs to their owners for one recovery batch.
/// The claim bounds the map to at most twice its batch size. Single-run and user
/// authority is cleared; deployment policies must use exact map lookup, not sets
/// of independent tenants and runs. The caller retains the claim transaction.
pub(crate) async fn set_system_batch(
    tx: &mut Transaction<'_, Postgres>,
    runs: &std::collections::BTreeMap<Uuid, Uuid>,
) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT set_config('vigilo.run_batch',$1::jsonb::text,true), set_config('vigilo.tenant_id','',true), set_config('vigilo.run_id','',true), set_config('vigilo.actor_id','',true)")
        .bind(sqlx::types::Json(runs)).execute(&mut **tx).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn absent_identity_is_not_standalone_or_a_wildcard() {
        assert!(UserContext::new(Uuid::nil(), Uuid::now_v7()).is_err());
        assert!(UserContext::new(Uuid::now_v7(), Uuid::nil()).is_err());
        assert!(UserContext::new(Uuid::now_v7(), Uuid::now_v7()).is_ok());
    }
}
