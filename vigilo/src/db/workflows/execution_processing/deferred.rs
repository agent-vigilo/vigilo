//! Execution-local ownership for deferred harness invocations.
//!
//! A current attempt owns a frozen submission, a deadline, and a short remote
//! request claim. Outcome acceptance and scoring readiness share a transaction
//! on the shard. Chunk locks serialize readiness with release and revocation.
//! The outbox carries chunk wakeups through the existing placement publisher.

use anyhow::{
    Result,
    ensure,
};
use chrono::{
    DateTime,
    Utc,
};
use sqlx::{
    PgPool,
    Postgres,
    Transaction,
};
use uuid::Uuid;
use vigilo::invocation::{
    profile::AgentIdentity,
    wire::{
        self,
        CaseInput,
        Outcome,
        Receipt,
        Submission,
    },
};

use crate::{
    db::scope,
    models::run_chunk::RunChunk,
};

/// Routing and ownership established by the caller before using a shard pool.
#[derive(Clone, Copy, serde::Deserialize)]
pub(crate) struct Attempt {
    pub tenant_id: Uuid,
    pub run_id: Uuid,
    pub run_shard: i16,
    pub chunk_id: Uuid,
    pub attempt_id: Uuid,
    pub write_epoch: i64,
}

impl Attempt {
    pub(crate) fn from_chunk(chunk: &RunChunk, attempt_id: Uuid) -> Self {
        Self {
            tenant_id: chunk.tenant_id,
            run_id: chunk.run_id,
            run_shard: chunk.run_shard,
            chunk_id: chunk.id,
            attempt_id,
            write_epoch: chunk.write_epoch,
        }
    }
}

/// Frozen submission inputs. Callback authorization is supplied by the host at delivery.
pub(crate) struct Intent {
    pub agent: AgentIdentity,
    pub case: CaseInput,
    pub deadline: DateTime<Utc>,
    pub max_request_bytes: usize,
    pub require_agent: bool,
    pub evidence_limit: usize,
    pub deduplicates: bool,
    pub requires_reference: bool,
    pub delay_before_send_secs: Option<f64>,
}

/// One remote request may be issued after its claim commits.
#[derive(sqlx::FromRow)]
pub(crate) struct Claim {
    pub deadline: DateTime<Utc>,
    pub execution_id: Uuid,
    pub invocation_id: Uuid,
    pub token: Uuid,
    pub request: Vec<u8>,
    pub requires_reference: bool,
}

/// This value is constructed by the host only after verifying callback authority.
#[derive(Clone, Copy)]
pub(crate) enum CompletionProof {
    RequestClaim(Uuid),
    #[cfg(test)]
    VerifiedCallback {
        invocation_id: Uuid,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Acceptance {
    Accepted,
    Duplicate,
    Ineligible,
}

#[derive(sqlx::FromRow)]
pub(super) struct ChunkState {
    status: String,
    control: String,
    lease_token: Option<Uuid>,
    live_lease: bool,
}

impl ChunkState {
    pub(super) fn may_receive_outcome(&self) -> bool {
        self.control != "revoked" && matches!(self.status.as_str(), "pending" | "leased")
    }
}

pub(super) async fn lock_chunk(
    tx: &mut Transaction<'_, Postgres>,
    key: Attempt,
) -> Result<Option<ChunkState>> {
    scope::lock_system_shard(tx, key.tenant_id, key.run_id, key.run_shard).await?;
    Ok(sqlx::query_as(
        "SELECT c.status, c.execution_control::text AS control, c.lease_token,
             COALESCE(c.leased_until > clock_timestamp(), false) AS live_lease
         FROM run_chunks c
         JOIN local_shard_admissions s USING (run_id, run_shard)
         WHERE c.tenant_id=$1 AND c.run_id=$2 AND c.run_shard=$3 AND c.id=$4
           AND s.tenant_id=$1 AND s.write_epoch=$5 AND s.state IN ('open','draining')
         FOR UPDATE OF c",
    )
    .bind(key.tenant_id)
    .bind(key.run_id)
    .bind(key.run_shard)
    .bind(key.chunk_id)
    .bind(key.write_epoch)
    .fetch_optional(&mut **tx)
    .await?)
}

/// Records immutable submission bytes and claims the first send under the worker lease.
pub(crate) async fn start(
    db: &PgPool,
    key: Attempt,
    lease_token: Uuid,
    intent: Intent,
    lease_seconds: i32,
) -> Result<Option<Claim>> {
    ensure!(
        lease_seconds > 0,
        "invocation claim must have a positive lease"
    );
    ensure!(
        (1..=wire::MAX_EVIDENCE_BYTES).contains(&intent.evidence_limit),
        "invalid deferred evidence limit"
    );
    let invocation_id = uuid::Builder::from_random_bytes(rand::random()).into_uuid();
    let submission = Submission {
        schema_version: wire::SCHEMA_VERSION.into(),
        invocation_id,
        requested_agent: intent.agent,
        deadline_at: intent.deadline.fixed_offset(),
        case: intent.case,
        callback: None,
    };
    submission.validate(false)?;
    let body = crate::agent_client::limits::serialize(
        &submission,
        u64::try_from(intent.max_request_bytes)?,
    )
    .map_err(|error| {
        crate::agent_client::failure::Failure::terminal(
            crate::agent_client::failure::Kind::BodyLimit,
            error,
        )
    })?;

    let mut tx = db.begin().await?;
    ensure!(
        lock_chunk(&mut tx, key)
            .await?
            .is_some_and(|chunk| chunk.control == "enabled"
                && chunk.live_lease
                && chunk.lease_token == Some(lease_token)),
        "deferred submission lost its chunk lease"
    );
    let parked: Option<(Option<Uuid>,Uuid)> = sqlx::query_as(
        "WITH started AS (
            UPDATE execution_attempts a
            SET invocation_id=$1, invocation_status='submitting', invocation_deadline=$2,
                invocation_request=$3, invocation_deduplicates=$4, invocation_requires_reference=$5,
                invocation_requires_agent=$12, invocation_evidence_limit=$13,
                invocation_claim=CASE WHEN $14::double precision IS NULL THEN gen_random_uuid() END,
                invocation_claim_until=CASE WHEN $14 IS NULL THEN LEAST($2,statement_timestamp()+make_interval(secs=>$6)) END,
                invocation_next_at=LEAST($2,statement_timestamp()+make_interval(secs=>COALESCE($14,$6))),
                status='awaiting_agent', leased_until=NULL, updated_at=clock_timestamp()
            FROM executions e
            WHERE a.tenant_id=$7 AND a.run_id=$8 AND a.run_shard=$9 AND a.id=$10
              AND e.run_id=a.run_id AND e.run_shard=a.run_shard AND e.id=a.execution_id
              AND e.chunk_id=$11 AND e.current_attempt_id=a.id AND e.status='running'
              AND a.status='running' AND a.leased_until > clock_timestamp()
              AND a.invocation_id IS NULL AND $2 > clock_timestamp()
            RETURNING a.invocation_id,a.invocation_claim,a.execution_id
        ), parked AS (
            UPDATE executions e SET status='awaiting_agent',updated_at=clock_timestamp()
             FROM started s WHERE e.run_id=$8 AND e.run_shard=$9 AND e.id=s.execution_id
        )
        SELECT invocation_claim,execution_id FROM started",
    )
    .bind(invocation_id).bind(intent.deadline).bind(&body).bind(intent.deduplicates)
    .bind(intent.requires_reference).bind(f64::from(lease_seconds)).bind(key.tenant_id)
    .bind(key.run_id).bind(key.run_shard).bind(key.attempt_id).bind(key.chunk_id).bind(intent.require_agent).bind(i32::try_from(intent.evidence_limit)?).bind(intent.delay_before_send_secs)
    .fetch_optional(&mut *tx).await?;
    let (token, execution_id) = parked
        .ok_or_else(|| anyhow::anyhow!("deferred submission could not park its current attempt"))?;
    tx.commit().await?;
    Ok(token.map(|token| Claim {
        deadline: intent.deadline,
        execution_id,
        invocation_id,
        token,
        request: body,
        requires_reference: intent.requires_reference,
    }))
}

/// Reclaims a lost submission or records ambiguous acceptance when resending is unsafe.
pub(crate) async fn reclaim_submission(
    db: &PgPool,
    key: Attempt,
    lease_seconds: i32,
) -> Result<Option<Claim>> {
    ensure!(
        lease_seconds > 0,
        "invocation claim must have a positive lease"
    );
    let mut tx = db.begin().await?;
    if !lock_chunk(&mut tx, key).await?.is_some_and(|c| {
        c.control == "enabled" && matches!(c.status.as_str(), "pending" | "leased")
    }) {
        return Ok(None);
    }
    let claim = sqlx::query_as(
        "WITH claimed AS (
           UPDATE execution_attempts a
           SET invocation_status=CASE WHEN invocation_claim IS NOT NULL AND NOT invocation_deduplicates
                 THEN 'submission_unknown'::invocation_status ELSE invocation_status END,
               invocation_claim=CASE WHEN invocation_claim IS NOT NULL AND NOT invocation_deduplicates
                 THEN NULL ELSE gen_random_uuid() END,
               invocation_claim_until=CASE WHEN invocation_claim IS NOT NULL AND NOT invocation_deduplicates
                 THEN NULL ELSE LEAST(invocation_deadline,statement_timestamp()+make_interval(secs=>$1)) END,
               invocation_next_at=CASE WHEN invocation_deduplicates OR invocation_claim IS NULL
                 THEN LEAST(invocation_deadline,statement_timestamp()+make_interval(secs=>$1)) END,
               updated_at=clock_timestamp()
           FROM executions e
           WHERE a.run_id=$2 AND a.run_shard=$3 AND a.id=$4 AND e.run_id=a.run_id
             AND e.run_shard=a.run_shard AND e.id=a.execution_id AND e.chunk_id=$5
             AND e.current_attempt_id=a.id AND e.status='awaiting_agent' AND a.status='awaiting_agent'
             AND a.invocation_status='submitting' AND a.invocation_deadline > clock_timestamp()
             AND a.invocation_next_at <= clock_timestamp()
             AND (a.invocation_claim_until IS NULL OR a.invocation_claim_until <= clock_timestamp())
           RETURNING invocation_id,invocation_claim,invocation_request,invocation_requires_reference,invocation_deadline,a.execution_id
        )
        SELECT invocation_id,invocation_claim AS token,invocation_request AS request,
            invocation_requires_reference AS requires_reference,invocation_deadline AS deadline,execution_id
        FROM claimed WHERE invocation_claim IS NOT NULL",
    ).bind(f64::from(lease_seconds)).bind(key.run_id).bind(key.run_shard)
    .bind(key.attempt_id).bind(key.chunk_id).fetch_optional(&mut *tx).await?;
    tx.commit().await?;
    Ok(claim)
}

/// Records a valid receipt only while the original submission claim owns the invocation.
pub(crate) async fn record_receipt(
    db: &PgPool,
    key: Attempt,
    claim: &Claim,
    body: &[u8],
) -> Result<bool> {
    let receipt = Receipt::decode(body, claim.invocation_id, claim.requires_reference)?;
    let mut tx = db.begin().await?;
    if !lock_chunk(&mut tx, key).await?.is_some_and(|c| {
        c.control != "revoked" && matches!(c.status.as_str(), "pending" | "leased")
    }) {
        return Ok(false);
    }
    let changed = sqlx::query(
        "UPDATE execution_attempts a
         SET invocation_status='waiting',invocation_reference=$1,invocation_claim=NULL,invocation_claim_until=NULL,
             invocation_next_at=CASE WHEN invocation_requires_reference THEN LEAST(invocation_deadline,statement_timestamp()+interval '5 seconds') END,
             updated_at=clock_timestamp()
         FROM executions e
         WHERE a.run_id=$2 AND a.run_shard=$3 AND a.id=$4 AND e.run_id=a.run_id AND e.run_shard=a.run_shard
           AND e.id=a.execution_id AND e.chunk_id=$5 AND e.current_attempt_id=a.id AND e.status='awaiting_agent'
           AND a.invocation_id=$6 AND a.invocation_claim=$7 AND a.invocation_status='submitting'
           AND a.invocation_claim_until > clock_timestamp() AND a.invocation_deadline > clock_timestamp()",
    ).bind(receipt.provider_reference).bind(key.run_id).bind(key.run_shard).bind(key.attempt_id)
    .bind(key.chunk_id).bind(claim.invocation_id).bind(claim.token).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(changed.rows_affected() == 1)
}

#[derive(sqlx::FromRow)]
struct OutcomeTarget {
    request: Option<Vec<u8>>,
    digest: Option<Vec<u8>>,
    eligible: bool,
    claim: Option<Uuid>,
    require_agent: bool,
    evidence_limit: i32,
}

/// Accepts bounded evidence only for the currently authorized invocation.
/// The original bytes are available to a replacement scoring worker after commit.
pub(crate) async fn accept(
    db: &PgPool,
    key: Attempt,
    invocation_id: Uuid,
    proof: CompletionProof,
    body: &[u8],
) -> Result<Acceptance> {
    ensure!(
        body.len() <= wire::MAX_OUTCOME_BYTES,
        "invocation outcome exceeds byte limit"
    );
    let mut tx = db.begin().await?;
    let Some(chunk) = lock_chunk(&mut tx, key).await? else {
        return Ok(Acceptance::Ineligible);
    };
    let target: Option<OutcomeTarget> = sqlx::query_as(
        "SELECT CASE WHEN invocation_outcome_digest IS NULL THEN invocation_request END AS request,
            invocation_outcome_digest AS digest,
            (e.status='awaiting_agent' AND a.status='awaiting_agent'
             AND a.invocation_status IN ('submitting','waiting','submission_unknown')
             AND a.invocation_deadline > clock_timestamp()) AS eligible,
            CASE WHEN a.invocation_claim_until > clock_timestamp() THEN a.invocation_claim END AS claim,
            a.invocation_requires_agent AS require_agent,a.invocation_evidence_limit AS evidence_limit
         FROM execution_attempts a JOIN executions e
           ON e.run_id=a.run_id AND e.run_shard=a.run_shard AND e.id=a.execution_id AND e.current_attempt_id=a.id
         WHERE a.run_id=$1 AND a.run_shard=$2 AND a.id=$3 AND e.chunk_id=$4 AND a.invocation_id=$5",
    ).bind(key.run_id).bind(key.run_shard).bind(key.attempt_id).bind(key.chunk_id).bind(invocation_id)
    .fetch_optional(&mut *tx).await?;
    let Some(target) = target else {
        return Ok(Acceptance::Ineligible);
    };
    let request_claim = match proof {
        CompletionProof::RequestClaim(token) => Some(token),
        #[cfg(test)]
        CompletionProof::VerifiedCallback { .. } => None,
    };
    let authorized = match proof {
        CompletionProof::RequestClaim(token) => target.claim == Some(token),
        #[cfg(test)]
        CompletionProof::VerifiedCallback {
            invocation_id: authorized_id,
        } => authorized_id == invocation_id,
    };
    if !authorized || chunk.control == "revoked" {
        return Ok(Acceptance::Ineligible);
    }
    let digest = blake3::hash(body);
    if let Some(prior) = target.digest {
        ensure!(prior == digest.as_bytes(), "conflicting invocation outcome");
        return Ok(Acceptance::Duplicate);
    }
    if !target.eligible || !matches!(chunk.status.as_str(), "pending" | "leased") {
        return Ok(Acceptance::Ineligible);
    }
    let request = target
        .request
        .ok_or_else(|| anyhow::anyhow!("missing frozen submission"))?;
    let submission = Submission::decode(&request, request.len(), false)?;
    ensure!(
        submission.invocation_id == invocation_id,
        "frozen submission identity mismatch"
    );
    let (evidence, content_type) = match Outcome::decode(
        body,
        invocation_id,
        &submission.requested_agent,
        target.require_agent,
    )? {
        Outcome::Completed { evidence, .. } => {
            ensure!(
                evidence.bytes.len() <= usize::try_from(target.evidence_limit)?,
                "invocation evidence exceeds profile limit"
            );
            (Some(evidence.bytes), Some(evidence.content_type))
        }
        Outcome::Failed { .. } => (None, None),
    };
    let accepted = sqlx::query(
        "WITH accepted AS (
             UPDATE execution_attempts
             SET invocation_status=CASE WHEN $1::bytea IS NOT NULL THEN 'completed'::invocation_status ELSE 'failed'::invocation_status END,
                 invocation_request=NULL,invocation_claim=NULL,invocation_claim_until=NULL,invocation_next_at=NULL,
                 invocation_outcome_digest=$3,invocation_evidence=$1,invocation_content_type=$2,
                 status='ready_to_score',updated_at=clock_timestamp()
             WHERE run_id=$4 AND run_shard=$5 AND id=$6 AND invocation_deadline > clock_timestamp()
               AND ($7::uuid IS NULL OR (invocation_claim=$7 AND invocation_claim_until > clock_timestamp()))
             RETURNING execution_id
         )
         UPDATE executions e
         SET status='awaiting_evaluators',updated_at=clock_timestamp()
         FROM accepted a WHERE e.run_id=$4 AND e.run_shard=$5 AND e.id=a.execution_id",
    ).bind(evidence).bind(content_type).bind(digest.as_bytes().as_slice())
    .bind(key.run_id).bind(key.run_shard).bind(key.attempt_id).bind(request_claim).execute(&mut *tx).await?;
    if accepted.rows_affected() == 0 {
        return Ok(Acceptance::Ineligible);
    }
    mark_ready(&mut tx, key).await?;
    tx.commit().await?;
    Ok(Acceptance::Accepted)
}

/// Marks a wakeup while the chunk is locked and publishes only after release.
pub(super) async fn mark_ready(tx: &mut Transaction<'_, Postgres>, key: Attempt) -> Result<()> {
    let changed = sqlx::query(
        "UPDATE run_chunks SET deferred_wakeup_id=gen_random_uuid()
         WHERE run_id=$1 AND run_shard=$2 AND id=$3 AND deferred_wakeup_id IS NULL",
    )
    .bind(key.run_id)
    .bind(key.run_shard)
    .bind(key.chunk_id)
    .execute(&mut **tx)
    .await?;
    if changed.rows_affected() != 0 {
        enqueue_ready(tx, key).await?;
    }
    Ok(())
}

async fn enqueue_ready(tx: &mut Transaction<'_, Postgres>, key: Attempt) -> Result<()> {
    sqlx::query(
        "INSERT INTO outbox_events(tenant_id,event_type,aggregate_type,aggregate_id,dedupe_key,payload)
         SELECT c.tenant_id,'run.chunk.ready','run',c.run_id,
             format('run:%s:chunk:%s:ready:invocation:%s',c.run_id,c.id,c.deferred_wakeup_id),
             jsonb_build_object('tenant_id',c.tenant_id,'run_id',c.run_id,'run_shard',c.run_shard,
                 'chunk_id',c.id,'database_alias',a.database_alias,'write_epoch',a.write_epoch,
                 'recovery_count',c.recovery_count,'plan_digest',s.config_snapshot #>> '{run_plan,digest}')
         FROM run_chunks c JOIN local_shard_admissions a USING(run_id,run_shard)
         JOIN run_snapshots s USING(run_id,run_shard)
         WHERE c.run_id=$1 AND c.run_shard=$2 AND c.id=$3 AND c.status='pending'
           AND c.deferred_wakeup_id IS NOT NULL AND c.execution_control <> 'revoked'
         ON CONFLICT (tenant_id,dedupe_key) DO NOTHING",
    ).bind(key.run_id).bind(key.run_shard).bind(key.chunk_id).execute(&mut **tx).await?;
    Ok(())
}

/// Releases a parked chunk without broker retry traffic. A previously accepted
/// completion gets its wakeup in the same transaction as the release.
pub(crate) async fn release(db: &PgPool, chunk: &RunChunk) -> Result<bool> {
    let key = Attempt::from_chunk(chunk, Uuid::nil());
    let mut tx = db.begin().await?;
    if !lock_chunk(&mut tx, key).await?.is_some_and(|c| {
        c.status == "leased"
            && c.live_lease
            && c.lease_token == chunk.lease_token
            && c.control != "revoked"
    }) {
        return Ok(false);
    }
    sqlx::query("UPDATE run_chunks SET status='pending',lease_token=NULL,leased_until=NULL,updated_at=clock_timestamp() WHERE run_id=$1 AND run_shard=$2 AND id=$3")
        .bind(key.run_id).bind(key.run_shard).bind(key.chunk_id).execute(&mut *tx).await?;
    enqueue_ready(&mut tx, key).await?;
    tx.commit().await?;
    Ok(true)
}

/// The accepted evidence is separate from the evaluator retention packet.
#[derive(Debug)]
pub(super) enum ScoringInput {
    Ready,
    Completed(vigilo::response::CapturedBody),
    Failed,
    TimedOut,
}

impl ScoringInput {
    pub(super) fn output(&self) -> Result<crate::contracts::evaluator::AgentOutput> {
        match self {
            Self::Ready => anyhow::bail!("scoring evidence was not loaded"),
            Self::Completed(body) => Ok(crate::agent_client::captured_output(body.clone())),
            Self::TimedOut => Err(crate::agent_client::failure::Failure::terminal(
                crate::agent_client::failure::Kind::Timeout,
                "deferred invocation deadline elapsed",
            )
            .into()),
            Self::Failed => Err(crate::agent_client::failure::Failure::terminal(
                crate::agent_client::failure::Kind::RemoteFailure,
                "remote invocation reported failure",
            )
            .into()),
        }
    }
}

#[derive(sqlx::FromRow)]
struct ExistingAttempt {
    case_id: Uuid,
    execution_id: Uuid,
    attempt_id: Uuid,
    attempt_no: i32,
    state: String,
}

/// Resolves parked and scoring-ready cases with one shard submission inside
/// the allocation transaction. A replacement worker reacquires the original
/// attempt, including when chunk recovery retired its old scoring lease.
pub(super) async fn resume(
    tx: &mut Transaction<'_, Postgres>,
    chunk: &RunChunk,
    lease: &super::AttemptLeaseContext,
    cases: &[Uuid],
) -> Result<Vec<super::AttemptAllocation>> {
    let rows: Vec<ExistingAttempt> = sqlx::query_as(
        "WITH current_attempts AS MATERIALIZED (
             SELECT e.case_id,e.id AS execution_id,a.id AS attempt_id,a.attempt_no,
                 a.invocation_status::text AS state,e.status='awaiting_evaluators' AS ready
             FROM executions e JOIN execution_attempts a
               ON a.run_id=e.run_id AND a.run_shard=e.run_shard AND a.id=e.current_attempt_id
             WHERE e.run_id=$1 AND e.run_shard=$2 AND e.chunk_id=$3 AND e.case_id=ANY($4)
               AND a.invocation_id IS NOT NULL AND e.status IN ('awaiting_agent','awaiting_evaluators')
         ), leased AS (
             UPDATE execution_attempts a
             SET status='running', worker_id=$5,worker_host=$6,queue_message_id=$7,broker_message_id=$8,
                 leased_until=clock_timestamp()+make_interval(secs=>$9),heartbeat_at=clock_timestamp(),
                 completed_at=NULL,error_message=NULL,updated_at=clock_timestamp()
             FROM current_attempts c WHERE a.run_id=$1 AND a.run_shard=$2 AND a.id=c.attempt_id AND c.ready
             RETURNING a.id
         )
         SELECT c.* FROM current_attempts c WHERE NOT c.ready OR c.attempt_id IN (SELECT id FROM leased)",
    ).bind(chunk.run_id).bind(chunk.run_shard).bind(chunk.id).bind(cases)
    .bind(lease.worker_id).bind(&lease.worker_host).bind(lease.queue_message_id)
    .bind(&lease.broker_message_id).bind(f64::from(lease.lease_seconds)).fetch_all(&mut **tx).await?;
    rows.into_iter()
        .map(|row| {
            let deferred_output = match row.state.as_str() {
                "completed" => Some(ScoringInput::Ready),
                "failed" => Some(ScoringInput::Failed),
                "timed_out" => Some(ScoringInput::TimedOut),
                "submitting" | "waiting" | "submission_unknown" => None,
                _ => anyhow::bail!("invalid parked invocation state"),
            };
            Ok(super::AttemptAllocation {
                case_id: row.case_id,
                execution_id: row.execution_id,
                attempt_id: Some(row.attempt_id),
                attempt_no: row.attempt_no,
                should_process: deferred_output.is_some(),
                already_terminal: false,
                retry_not_due: false,
                max_attempts_exhausted: false,
                deferred_output,
            })
        })
        .collect()
}

#[derive(sqlx::FromRow)]
struct EvidenceRow {
    id: Uuid,
    body: Vec<u8>,
    content_type: String,
}

/// Loads only one bounded window of already claimed evidence. Chunk and attempt
/// fences are checked before materializing bytes for the local evaluator.
pub(super) async fn load_scoring_inputs(
    db: &PgPool,
    chunk: &RunChunk,
    worker: Uuid,
    allocations: &mut [super::AttemptAllocation],
) -> Result<()> {
    ensure!(
        allocations.len() <= super::CASE_EXECUTION_PARALLELISM,
        "deferred evidence window exceeds worker concurrency"
    );
    let attempts: Vec<_> = allocations
        .iter()
        .filter_map(|allocation| {
            matches!(allocation.deferred_output, Some(ScoringInput::Ready))
                .then_some(allocation.attempt_id)
                .flatten()
        })
        .collect();
    if attempts.is_empty() {
        return Ok(());
    }
    let mut tx = db.begin().await?;
    scope::set_system(&mut tx, chunk.tenant_id, chunk.run_id).await?;
    let rows: Vec<EvidenceRow> = sqlx::query_as(
        "SELECT a.id,a.invocation_evidence AS body,a.invocation_content_type AS content_type
         FROM execution_attempts a
         JOIN executions e ON e.run_id=a.run_id AND e.run_shard=a.run_shard AND e.id=a.execution_id AND e.current_attempt_id=a.id
         JOIN run_chunks c ON c.run_id=e.run_id AND c.run_shard=e.run_shard AND c.id=e.chunk_id
         JOIN local_shard_admissions s ON s.run_id=c.run_id AND s.run_shard=c.run_shard
         WHERE a.run_id=$1 AND a.run_shard=$2 AND c.id=$3 AND a.id=ANY($4) AND a.worker_id=$5
           AND c.lease_token=$6 AND c.leased_until > clock_timestamp() AND c.execution_control<>'revoked'
           AND s.write_epoch=$7 AND s.state IN ('open','draining')
           AND a.status='running' AND a.leased_until > clock_timestamp() AND a.invocation_status='completed'",
    ).bind(chunk.run_id).bind(chunk.run_shard).bind(chunk.id).bind(&attempts).bind(worker)
    .bind(chunk.lease_token).bind(chunk.write_epoch).fetch_all(&mut *tx).await?;
    ensure!(
        rows.len() == attempts.len(),
        "deferred scoring lost its evidence or lease"
    );
    tx.commit().await?;
    let mut rows: std::collections::BTreeMap<_, _> =
        rows.into_iter().map(|row| (row.id, row)).collect();
    for allocation in allocations {
        if let Some(row) = allocation.attempt_id.and_then(|id| rows.remove(&id)) {
            allocation.deferred_output = Some(ScoringInput::Completed(
                vigilo::response::CapturedBody::new(row.body, Some(row.content_type)),
            ));
        }
    }
    Ok(())
}
