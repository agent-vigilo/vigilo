//! Bounded discovery and claims for deferred HTTP work on an execution placement.
//!
//! Discovery shares the coordinator's existing recovery submission. Each remote
//! request is fenced by a committed attempt claim and has its own bounded lifetime.

use anyhow::Result;
use chrono::{
    DateTime,
    Utc,
};
use serde::Deserialize;
use sqlx::PgPool;
use uuid::Uuid;

use super::deferred::{
    Attempt,
    Claim,
    lock_chunk,
    mark_ready,
};
use crate::agent_client::deferred::Route;

pub(crate) const CLAIM_SECS: i32 = 15;
pub(crate) const DISCOVERY_LIMIT: i64 = 32;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Kind {
    Expire,
    Submit,
    Poll,
    Cancel,
}

#[derive(Deserialize)]
pub(crate) struct Job {
    pub key: Attempt,
    pub kind: Kind,
    pub due_at: DateTime<Utc>,
    pub route: Route,
}

// SQL starts with SELECT to keep the existing recovery observation category.
// The three index windows have independent budgets so polls cannot crowd out
// deadline handling or cancellation. Durable fields remain on execution shards.
pub(crate) const DISCOVER: &str = "
SELECT run_id, run_shard, tenant_id, NULL::jsonb AS job
FROM vigilo_claim_chunk_recovery($1,$2)
UNION ALL
SELECT a.run_id,a.run_shard,a.tenant_id,
       jsonb_build_object('key',jsonb_build_object('tenant_id',a.tenant_id,'run_id',a.run_id,
            'run_shard',a.run_shard,'chunk_id',e.chunk_id,'attempt_id',a.id,'write_epoch',ad.write_epoch),
            'kind',a.kind,'due_at',a.due_at,'route',s.config_snapshot->'invocation_transport')
FROM (
  (SELECT run_id,run_shard,tenant_id,id,execution_id,invocation_deadline AS due_at,'expire'::text AS kind
   FROM execution_attempts
   WHERE invocation_status IN ('submitting','waiting','submission_unknown')
     AND invocation_deadline <= statement_timestamp()
   ORDER BY invocation_deadline,run_id,id LIMIT $3)
  UNION ALL
  (SELECT run_id,run_shard,tenant_id,id,execution_id,invocation_next_at,
          CASE WHEN invocation_status='submitting' THEN 'submit' ELSE 'poll' END
   FROM execution_attempts
   WHERE invocation_next_at <= statement_timestamp() AND invocation_deadline > statement_timestamp()
     AND (invocation_status='waiting' OR (invocation_status='submitting' AND EXISTS (
        SELECT 1 FROM executions e JOIN run_chunks c ON c.run_id=e.run_id AND c.run_shard=e.run_shard AND c.id=e.chunk_id
        WHERE e.run_id=execution_attempts.run_id AND e.run_shard=execution_attempts.run_shard
          AND e.id=execution_attempts.execution_id AND c.execution_control='enabled')))
   ORDER BY invocation_next_at,run_id,id LIMIT $3)
  UNION ALL
  (SELECT run_id,run_shard,tenant_id,id,execution_id,invocation_cancel_at,'cancel'
   FROM execution_attempts WHERE invocation_cancel_at <= statement_timestamp()
   ORDER BY invocation_cancel_at,run_id,id LIMIT $3)
) a
JOIN executions e ON e.run_id=a.run_id AND e.run_shard=a.run_shard AND e.id=a.execution_id
JOIN local_shard_admissions ad ON ad.run_id=a.run_id AND ad.run_shard=a.run_shard AND ad.tenant_id=a.tenant_id
JOIN run_snapshots s ON s.run_id=a.run_id AND s.run_shard=a.run_shard AND s.tenant_id=a.tenant_id
WHERE ad.state IN ('open','draining') AND s.config_snapshot ? 'invocation_transport'
  AND (a.kind='cancel' OR e.current_attempt_id=a.id)
";

/// Moves a due submission past a short endpoint cooldown without claiming a send.
pub(crate) async fn postpone_submission(db: &PgPool, key: Attempt) -> Result<()> {
    let mut tx = db.begin().await?;
    if lock_chunk(&mut tx, key)
        .await?
        .is_some_and(|c| c.may_receive_outcome())
    {
        sqlx::query("UPDATE execution_attempts SET invocation_next_at=LEAST(invocation_deadline,statement_timestamp()+interval '10 seconds')
            WHERE run_id=$1 AND run_shard=$2 AND id=$3 AND invocation_status='submitting'
              AND invocation_next_at <= statement_timestamp()
              AND (invocation_claim_until IS NULL OR invocation_claim_until <= statement_timestamp())")
            .bind(key.run_id).bind(key.run_shard).bind(key.attempt_id).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

#[derive(sqlx::FromRow)]
pub(crate) struct PollClaim {
    #[sqlx(flatten)]
    pub invocation: Claim,
    pub reference: String,
    pub require_agent: bool,
    pub poll_count: i32,
    pub evidence_limit: i32,
}

pub(crate) async fn claim_poll(db: &PgPool, key: Attempt) -> Result<Option<PollClaim>> {
    let mut tx = db.begin().await?;
    if !lock_chunk(&mut tx, key)
        .await?
        .is_some_and(|c| c.may_receive_outcome())
    {
        return Ok(None);
    }
    let claim = sqlx::query_as(
        "UPDATE execution_attempts a
         SET invocation_claim=gen_random_uuid(),
             invocation_claim_until=LEAST(invocation_deadline,statement_timestamp()+make_interval(secs=>$1)),
             invocation_next_at=LEAST(invocation_deadline,statement_timestamp()+make_interval(secs=>$1))
         FROM executions e
         WHERE a.run_id=$2 AND a.run_shard=$3 AND a.id=$4 AND e.run_id=a.run_id AND e.run_shard=a.run_shard
           AND e.id=a.execution_id AND e.current_attempt_id=a.id AND e.chunk_id=$5
           AND e.status='awaiting_agent' AND a.invocation_status='waiting' AND a.invocation_reference IS NOT NULL
           AND a.invocation_deadline > statement_timestamp() AND a.invocation_next_at <= statement_timestamp()
           AND (a.invocation_claim_until IS NULL OR a.invocation_claim_until <= statement_timestamp())
         RETURNING a.invocation_id,a.invocation_claim AS token,a.invocation_request AS request,
             a.invocation_requires_reference AS requires_reference,a.invocation_deadline AS deadline,a.execution_id,
             a.invocation_reference AS reference,a.invocation_requires_agent AS require_agent,a.invocation_poll_count AS poll_count,a.invocation_evidence_limit AS evidence_limit",
    ).bind(f64::from(CLAIM_SECS)).bind(key.run_id).bind(key.run_shard).bind(key.attempt_id).bind(key.chunk_id)
    .fetch_optional(&mut *tx).await?;
    tx.commit().await?;
    Ok(claim)
}

/// Reschedules from the end of the exchange; a displaced claimant cannot delay
/// or reopen an invocation.
pub(crate) async fn reschedule_poll(
    db: &PgPool,
    key: Attempt,
    token: Uuid,
    delay_secs: f64,
) -> Result<()> {
    let mut tx = db.begin().await?;
    if lock_chunk(&mut tx, key)
        .await?
        .is_some_and(|c| c.may_receive_outcome())
    {
        sqlx::query("UPDATE execution_attempts SET invocation_claim=NULL,invocation_claim_until=NULL,
            invocation_next_at=LEAST(invocation_deadline,statement_timestamp()+make_interval(secs=>$5)),
            invocation_poll_count=LEAST(invocation_poll_count,30)+1
            WHERE run_id=$1 AND run_shard=$2 AND id=$3 AND invocation_claim=$4
              AND invocation_claim_until > statement_timestamp() AND invocation_status='waiting'
              AND invocation_deadline > statement_timestamp()")
            .bind(key.run_id).bind(key.run_shard).bind(key.attempt_id).bind(token).bind(delay_secs)
            .execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Resolves the local deadline independently of remote capacity or availability.
pub(crate) async fn expire(db: &PgPool, key: Attempt, cancel: bool) -> Result<bool> {
    let mut tx = db.begin().await?;
    if !lock_chunk(&mut tx, key)
        .await?
        .is_some_and(|c| c.may_receive_outcome())
    {
        return Ok(false);
    }
    let updated = sqlx::query(
        "WITH expired AS (
            UPDATE execution_attempts a
            SET invocation_status='timed_out',status='ready_to_score',
                invocation_claim=NULL,invocation_claim_until=NULL,invocation_request=NULL,invocation_next_at=NULL,
                invocation_cancel_at=CASE WHEN $5 AND (a.invocation_status<>'submitting' OR a.invocation_claim IS NOT NULL) THEN statement_timestamp() END,
                invocation_cancel_until=CASE WHEN $5 THEN statement_timestamp()+interval '1 minute' END,
                invocation_cancel_uncertain=a.invocation_status<>'waiting',updated_at=statement_timestamp()
            FROM executions e
            WHERE a.run_id=$1 AND a.run_shard=$2 AND a.id=$3 AND e.run_id=a.run_id AND e.run_shard=a.run_shard
              AND e.id=a.execution_id AND e.current_attempt_id=a.id AND e.chunk_id=$4 AND e.status='awaiting_agent'
              AND a.invocation_deadline <= statement_timestamp() AND a.invocation_status IN ('submitting','waiting','submission_unknown')
            RETURNING a.execution_id
        )
        UPDATE executions e SET status='awaiting_evaluators',updated_at=statement_timestamp()
        FROM expired WHERE e.run_id=$1 AND e.run_shard=$2 AND e.id=expired.execution_id",
    ).bind(key.run_id).bind(key.run_shard).bind(key.attempt_id).bind(key.chunk_id).bind(cancel)
    .execute(&mut *tx).await?.rows_affected() != 0;
    if updated {
        mark_ready(&mut tx, key).await?;
    }
    tx.commit().await?;
    Ok(updated)
}

#[derive(sqlx::FromRow)]
pub(crate) struct CancellationClaim {
    pub invocation_id: Uuid,
    pub execution_id: Uuid,
    pub token: Uuid,
    pub reference: Option<String>,
    pub deadline: DateTime<Utc>,
    pub attempts: i32,
    pub uncertain: bool,
}

/// Claims cancellation independently of chunk/process lease and clears exhausted
/// requests after a sender disappears.
pub(crate) async fn claim_cancellation(
    db: &PgPool,
    key: Attempt,
) -> Result<Option<CancellationClaim>> {
    let mut tx = db.begin().await?;
    if lock_chunk(&mut tx, key).await?.is_none() {
        return Ok(None);
    }
    let claim = sqlx::query_as(
        "WITH available AS (
            SELECT id,invocation_cancel_attempts>=3 OR invocation_cancel_until<=statement_timestamp() AS exhausted
            FROM execution_attempts
            WHERE run_id=$2 AND run_shard=$3 AND id=$4 AND invocation_cancel_at <= statement_timestamp()
              AND invocation_status IN ('cancelled','timed_out')
              AND (invocation_claim_until IS NULL OR invocation_claim_until <= statement_timestamp())
        ), claimed AS (
            UPDATE execution_attempts a
            SET invocation_cancel_at=CASE WHEN NOT work.exhausted THEN LEAST(a.invocation_cancel_until,statement_timestamp()+make_interval(secs=>$1)) END,
                invocation_claim_until=CASE WHEN NOT work.exhausted THEN LEAST(a.invocation_cancel_until,statement_timestamp()+make_interval(secs=>$1)) END,
                invocation_claim=CASE WHEN NOT work.exhausted THEN gen_random_uuid() END,
                invocation_cancel_attempts=a.invocation_cancel_attempts+CASE WHEN work.exhausted THEN 0 ELSE 1 END,
                invocation_cancel_result=CASE WHEN work.exhausted THEN 'unconfirmed' END
            FROM available work WHERE a.run_id=$2 AND a.run_shard=$3 AND a.id=work.id
            RETURNING a.invocation_id,a.execution_id,a.invocation_claim AS token,a.invocation_reference AS reference,
                        a.invocation_cancel_until AS deadline,a.invocation_cancel_attempts AS attempts,a.invocation_cancel_uncertain AS uncertain
        ) SELECT * FROM claimed WHERE token IS NOT NULL",
    ).bind(f64::from(CLAIM_SECS)).bind(key.run_id).bind(key.run_shard).bind(key.attempt_id)
    .fetch_optional(&mut *tx).await?;
    tx.commit().await?;
    Ok(claim)
}

pub(crate) async fn finish_cancellation(
    db: &PgPool,
    key: Attempt,
    token: Uuid,
    result: Option<&str>,
    delay_secs: f64,
) -> Result<()> {
    let mut tx = db.begin().await?;
    if lock_chunk(&mut tx, key).await?.is_some() {
        sqlx::query("UPDATE execution_attempts SET invocation_claim=NULL,invocation_claim_until=NULL,
            invocation_cancel_result=$5,invocation_cancel_at=CASE WHEN $5 IS NULL THEN
                LEAST(invocation_cancel_until,statement_timestamp()+make_interval(secs=>$6)) END
            WHERE run_id=$1 AND run_shard=$2 AND id=$3 AND invocation_claim=$4
               AND invocation_claim_until>statement_timestamp() AND invocation_cancel_at IS NOT NULL")
            .bind(key.run_id).bind(key.run_shard).bind(key.attempt_id).bind(token).bind(result).bind(delay_secs)
            .execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}
