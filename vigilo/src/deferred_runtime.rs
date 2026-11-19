//! Standalone deferred invocation adapter.
//!
//! Workers commit submission intent before HTTP. The coordinator advances due
//! work already discovered on execution placements and runs HTTP outside SQL
//! transactions. Expiry, polls, and remote cancellation have separate budgets.

use std::{
    collections::BTreeMap,
    sync::Mutex,
    time::{
        Duration,
        Instant,
    },
};

use anyhow::{
    Context as _,
    Result,
};
use chrono::{
    DateTime,
    TimeDelta,
    Utc,
};
use futures_util::{
    StreamExt,
    stream,
};
use reqwest::{
    Method,
    StatusCode,
};
use sqlx::PgPool;
use uuid::Uuid;
use vigilo::invocation::{
    profile::poll_url,
    wire::{
        self,
        Cancellation,
        PollResponse,
        Submission,
    },
};

use crate::{
    agent_client::{
        credentials,
        deferred::{
            self as http,
            Message,
            Route,
        },
        failure::{
            Failure,
            Kind as FailureKind,
        },
    },
    context::Context,
    db::workflows::execution_processing::{
        deferred,
        deferred_jobs::{
            self as jobs,
            Job,
            Kind,
        },
    },
};

const HTTP_TIMEOUT: u64 = 5;
const POLL_PARALLELISM: usize = 8;
const CANCEL_PARALLELISM: usize = 4;

#[derive(Default)]
pub(crate) struct EndpointHealth(Mutex<BTreeMap<String, Health>>);

struct Health {
    failures: u32,
    next: Instant,
}

impl EndpointHealth {
    // Once a circuit opens, reserve one probe window before releasing the lock.
    fn allows(&self, endpoint: &str) -> bool {
        let mut entries = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(entry) = entries.get_mut(endpoint) else {
            return true;
        };
        if entry.failures < 2 {
            return true;
        }
        let now = Instant::now();
        if entry.next > now {
            return false;
        }
        entry.next = now + Duration::from_secs(HTTP_TIMEOUT);
        true
    }

    fn observe(&self, endpoint: &str, connection_failure: bool) {
        let mut entries = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !connection_failure {
            entries.remove(endpoint);
            return;
        }
        if entries.len() >= 512
            && !entries.contains_key(endpoint)
            && let Some(oldest) = entries
                .iter()
                .min_by_key(|(_, item)| item.next)
                .map(|(key, _)| key.clone())
        {
            entries.remove(&oldest);
        }
        let now = Instant::now();
        let entry = entries.entry(endpoint.to_owned()).or_insert(Health {
            failures: 0,
            next: now,
        });
        entry.failures = (entry.failures + 1).min(5);
        entry.next = now + Duration::from_secs((5_u64 << entry.failures).min(60));
    }
}

fn connection_failure(result: &Result<Vec<u8>, Failure>) -> bool {
    result
        .as_ref()
        .is_err_and(|error| matches!(error.kind, FailureKind::Timeout | FailureKind::Transport))
}

fn endpoint_key(url: &str) -> Result<String> {
    Ok(reqwest::Url::parse(url)?.origin().ascii_serialization())
}

fn timeout(deadline: DateTime<Utc>, seconds: u64) -> Duration {
    (deadline - Utc::now())
        .to_std()
        .unwrap_or_default()
        .min(Duration::from_secs(seconds))
}

fn scope(context: &Context, key: deferred::Attempt, execution_id: Uuid) -> credentials::Execution {
    credentials::Execution {
        run_id: key.run_id,
        execution_id,
        attempt_id: key.attempt_id,
        resources: context.accepted_resources(),
    }
}

fn failure_outcome(id: Uuid) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"schema_version":wire::SCHEMA_VERSION,
        "invocation_id":id,"status":"failed","error":{"code":"deferred_transport_error"}}))
    .expect("fixed outcome serializes")
}

pub(crate) async fn submit(
    context: &Context,
    db: &PgPool,
    key: deferred::Attempt,
    route: &Route,
    mut claim: deferred::Claim,
    recovery: bool,
) -> Result<()> {
    let seconds = if recovery {
        HTTP_TIMEOUT.min(u64::from(route.submit_timeout_secs))
    } else {
        u64::from(route.submit_timeout_secs)
    };
    let budget = timeout(claim.deadline, seconds);
    if budget.is_zero() {
        return Ok(());
    }
    let endpoint = endpoint_key(&route.http.url)?;
    let response = http::send(
        context.agent_credentials(),
        scope(context, key, claim.execution_id),
        Message {
            method: http::submit_method(route)?,
            url: &route.http.url,
            headers: &route.http.headers,
            credential: route.http.credential.as_deref(),
            body: Some(std::mem::take(&mut claim.request)),
            timeout: budget,
            max_bytes: wire::MAX_RECEIPT_BYTES as u64,
            expected_status: StatusCode::ACCEPTED,
        },
    )
    .await;
    context
        .deferred_health()
        .observe(&endpoint, connection_failure(&response));
    match response {
        Ok(body) => {
            if wire::Receipt::decode(&body, claim.invocation_id, claim.requires_reference).is_ok() {
                deferred::record_receipt(db, key, &claim, &body).await?;
            }
            // An invalid receipt leaves the send ambiguous, just like a lost receipt.
        }
        Err(error)
            if error
                .http_status
                .is_some_and(|status| (400..500).contains(&status))
                && !error.retryable =>
        {
            deferred::accept(
                db,
                key,
                claim.invocation_id,
                deferred::CompletionProof::RequestClaim(claim.token),
                &failure_outcome(claim.invocation_id),
            )
            .await?;
        }
        Err(_) => {}
    }
    Ok(())
}

fn jitter(seconds: f64, cap: f64) -> f64 {
    (seconds * rand::random_range(0.85..=1.15)).min(cap)
}

async fn poll(context: &Context, db: &PgPool, job: Job) -> Result<()> {
    let Some(poll) = jobs::claim_poll(db, job.key).await? else {
        return Ok(());
    };
    let claim = poll.invocation;
    let budget = timeout(claim.deadline, HTTP_TIMEOUT);
    if budget.is_zero() {
        return Ok(());
    }
    let url = match poll_url(&job.route.status_url, &poll.reference) {
        Ok(url) => url,
        Err(_) => {
            deferred::accept(
                db,
                job.key,
                claim.invocation_id,
                deferred::CompletionProof::RequestClaim(claim.token),
                &failure_outcome(claim.invocation_id),
            )
            .await?;
            return Ok(());
        }
    };
    let endpoint = url.origin().ascii_serialization();
    let delay = jitter(f64::from(10 << poll.poll_count.min(3)).min(60.0), 60.0);
    if !context.deferred_health().allows(&endpoint) {
        return jobs::reschedule_poll(db, job.key, claim.token, delay).await;
    }
    let response = http::send(
        context.agent_credentials(),
        scope(context, job.key, claim.execution_id),
        Message {
            method: Method::GET,
            url: url.as_str(),
            headers: &job.route.http.headers,
            credential: job.route.http.credential.as_deref(),
            body: None,
            timeout: budget,
            max_bytes: wire::MAX_OUTCOME_BYTES as u64,
            expected_status: StatusCode::OK,
        },
    )
    .await;
    context
        .deferred_health()
        .observe(&endpoint, connection_failure(&response));
    let proof = deferred::CompletionProof::RequestClaim(claim.token);
    match response {
        Ok(body) => {
            let submitted = Submission::decode(&claim.request, claim.request.len(), false)?;
            match PollResponse::decode(
                &body,
                claim.invocation_id,
                &submitted.requested_agent,
                poll.require_agent,
            ) {
                Ok(PollResponse::Pending) => {
                    jobs::reschedule_poll(db, job.key, claim.token, delay).await?
                }
                Ok(PollResponse::Final(outcome)) => {
                    let body = if matches!(&outcome, wire::Outcome::Completed { evidence, .. }
                        if evidence.bytes.len() > usize::try_from(poll.evidence_limit)?)
                    {
                        failure_outcome(claim.invocation_id)
                    } else {
                        body
                    };
                    deferred::accept(db, job.key, claim.invocation_id, proof, &body).await?;
                }
                Err(_) => {
                    deferred::accept(
                        db,
                        job.key,
                        claim.invocation_id,
                        proof,
                        &failure_outcome(claim.invocation_id),
                    )
                    .await?;
                }
            }
        }
        Err(error) if error.retryable => {
            let advice = error
                .retry_not_before
                .map(|at| (at - Utc::now()).num_milliseconds().max(0) as f64 / 1000.0)
                .unwrap_or_default()
                .max(f64::from(error.retry_delay_secs.unwrap_or_default()));
            jobs::reschedule_poll(db, job.key, claim.token, delay.max(advice)).await?;
        }
        Err(_) => {
            deferred::accept(
                db,
                job.key,
                claim.invocation_id,
                proof,
                &failure_outcome(claim.invocation_id),
            )
            .await?;
        }
    }
    Ok(())
}

async fn cancel(context: &Context, db: &PgPool, job: Job) -> Result<()> {
    let Some(route) = job.route.cancellation else {
        return Ok(());
    };
    let Some(claim) = jobs::claim_cancellation(db, job.key).await? else {
        return Ok(());
    };
    let budget = timeout(claim.deadline, HTTP_TIMEOUT);
    if budget.is_zero() {
        return Ok(());
    }
    let body = serde_json::to_vec(&Cancellation {
        schema_version: wire::SCHEMA_VERSION.into(),
        invocation_id: claim.invocation_id,
        provider_reference: claim.reference,
    })?;
    let response = http::send(
        context.agent_credentials(),
        scope(context, job.key, claim.execution_id),
        Message {
            method: Method::POST,
            url: &route.url,
            headers: &BTreeMap::new(),
            credential: route.credential.as_deref(),
            body: Some(body),
            timeout: budget,
            max_bytes: 0,
            expected_status: StatusCode::ACCEPTED,
        },
    )
    .await;
    let result = match response {
        Ok(_) => Some("accepted"),
        Err(error) if error.retryable || (claim.uncertain && error.http_status == Some(404)) => {
            (claim.attempts >= 3).then_some("unconfirmed")
        }
        Err(error) if error.http_status == Some(404) => Some("unknown"),
        Err(_) => Some("failed"),
    };
    jobs::finish_cancellation(
        db,
        job.key,
        claim.token,
        result,
        jitter(f64::from(5 << (claim.attempts - 1)), 30.0),
    )
    .await
}

/// Runtime request deadline and immutable input are committed under the current chunk lease.
pub(crate) async fn start_case(
    context: &Context,
    db: &PgPool,
    chunk: &crate::models::run_chunk::RunChunk,
    profile: &crate::contracts::run::RunProfile,
    attempt_id: Uuid,
    case: crate::contracts::evaluator::TestCase,
) -> Result<()> {
    let route = Route::from_profile(profile)?.context("polling profile required")?;
    let limits = context
        .agent_http_limits()
        .resolve(&route.http)
        .map_err(|error| Failure::terminal(FailureKind::Configuration, error))?;
    let key = deferred::Attempt::from_chunk(chunk, attempt_id);
    let deadline = Utc::now() + TimeDelta::seconds(i64::from(route.deadline_secs));
    let intent = deferred::Intent {
        agent: crate::agent_client::requested_agent(profile)?,
        case: wire::CaseInput {
            input: case.input,
            context: case.context.filter(|_| route.forward_context),
        },
        deadline,
        max_request_bytes: usize::try_from(limits.max_request_bytes)?,
        evidence_limit: usize::try_from(limits.max_response_bytes)?.min(wire::MAX_EVIDENCE_BYTES),
        require_agent: route.require_agent,
        deduplicates: route.deduplicates,
        requires_reference: true,
        delay_before_send_secs: (!context
            .deferred_health()
            .allows(&endpoint_key(&route.http.url)?))
        .then_some(10.0),
    };
    let claim = deferred::start(
        db,
        key,
        chunk.lease_token.context("missing chunk claim")?,
        intent,
        i32::try_from(route.submit_timeout_secs.saturating_add(10).min(86_400))?,
    )
    .await?;
    if let Some(claim) = claim {
        submit(context, db, key, &route, claim, false).await?;
    }
    Ok(())
}

/// Runs separately capped work lanes without holding the placement recovery
/// transaction or occupying a database-operation timeout during HTTP.
pub(crate) type Work = Vec<(PgPool, Job)>;

pub(crate) async fn run(context: &Context, mut work: Work) -> Work {
    work.sort_by_key(|(_, job)| job.due_at);
    let mut expired = Vec::new();
    let mut cancels = Vec::new();
    let mut polls = Vec::new();
    for item in work {
        let target = match item.1.kind {
            Kind::Expire => &mut expired,
            Kind::Cancel => &mut cancels,
            _ => &mut polls,
        };
        if target.len() < jobs::DISCOVERY_LIMIT as usize {
            target.push(item);
        }
    }
    tokio::join!(
        lane(context, expired, POLL_PARALLELISM),
        lane(context, polls, POLL_PARALLELISM)
    );
    cancels
}

/// Sends stop requests only after local coordinator work and outbox publication.
pub(crate) async fn deliver_cancellations(context: &Context, mut cancellations: Work) {
    cancellations.truncate(CANCEL_PARALLELISM);
    lane(context, cancellations, CANCEL_PARALLELISM).await;
}

async fn lane(context: &Context, work: Vec<(PgPool, Job)>, concurrency: usize) {
    stream::iter(work).for_each_concurrent(concurrency, |(db,job)| async move {
            let key = job.key;
            let lag_ms = (Utc::now()-job.due_at).num_milliseconds().max(0);
            let kind = job.kind;
            let result = match kind {
                Kind::Expire => jobs::expire(&db,key,job.route.cancellation.is_some()).await.map(|_|()),
                Kind::Cancel => cancel(context,&db,job).await,
                Kind::Poll => poll(context,&db,job).await,
                Kind::Submit => async {
                    if !context.deferred_health().allows(&endpoint_key(&job.route.http.url)?) {
                    return jobs::postpone_submission(&db,key).await;
                }
                if let Some(claim) = deferred::reclaim_submission(&db,key,jobs::CLAIM_SECS).await? {
                        submit(context,&db,key,&job.route,claim,true).await?;
                    }
                    Ok(())
                }.await,
            };
            match result {
                Ok(()) => tracing::debug!(kind=?kind, lag_ms,"processed deferred invocation work"),
                Err(error) => tracing::warn!(run_id=%key.run_id,kind=?kind,%error,"deferred invocation work failed"),
            }
        }).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unhealthy_endpoint_admits_one_recovery_probe_and_recovers_on_answer() {
        let health = EndpointHealth::default();
        let endpoint = "https://harness.example";
        health.observe(endpoint, true);
        assert!(health.allows(endpoint));
        health.observe(endpoint, true);
        assert!(!health.allows(endpoint));
        assert!(health.allows("https://unrelated.example"));
        health.0.lock().unwrap().get_mut(endpoint).unwrap().next =
            Instant::now() - Duration::from_secs(1);
        assert!(health.allows(endpoint));
        assert!(!health.allows(endpoint));
        health.observe(endpoint, false);
        assert!(health.allows(endpoint));
    }
}
