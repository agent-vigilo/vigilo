//! PostgreSQL contracts for parked attempts, late responses, and chunk wakeups.

use chrono::{
    Duration,
    Utc,
};
use vigilo::invocation::{
    profile::{
        AgentIdentity,
        Capabilities,
        Completion,
        Harness,
    },
    wire::{
        self,
        CaseInput,
    },
};

use super::*;
use crate::db::workflows::execution_processing::deferred::{
    self,
    Acceptance,
    Claim,
    CompletionProof,
    Intent,
};

fn agent() -> AgentIdentity {
    AgentIdentity {
        provider: "example".into(),
        name: "agent".into(),
        version: "1.0.0".into(),
    }
}

fn intent(deduplicates: bool) -> Intent {
    Intent {
        agent: agent(),
        case: CaseInput {
            input: json!({"prompt":"frozen"}),
            context: None,
        },
        deadline: Utc::now() + Duration::hours(1),
        max_request_bytes: 1024,
        require_agent: true,
        evidence_limit: 1024,
        deduplicates,
        requires_reference: true,
    }
}

fn receipt(id: Uuid) -> Vec<u8> {
    serde_json::to_vec(&json!({"schema_version":"1.0.0","invocation_id":id,
        "status":"accepted","provider_reference":"remote-reference"}))
    .unwrap()
}

fn completion(id: Uuid, body: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({"schema_version":"1.0.0","invocation_id":id,
        "status":"completed","executed_agent":agent(),
        "evidence":{"content_type":"text/plain","encoding":"utf8","body":body}}))
    .unwrap()
}

fn proof(claim: &Claim) -> CompletionProof {
    CompletionProof::VerifiedCallback {
        invocation_id: claim.invocation_id,
    }
}

async fn start(pool: &PgPool, seed: &SeededAttempt, deduplicates: bool) -> Claim {
    deferred::start(
        pool,
        deferred::Attempt::from_chunk(&seed.chunk, seed.attempt_id),
        seed.chunk.lease_token.unwrap(),
        intent(deduplicates),
        60,
    )
    .await
    .unwrap()
    .unwrap()
}

async fn expire_claim(pool: &PgPool, attempt: Uuid) {
    sqlx::query("UPDATE execution_attempts SET invocation_claim_until=now()-interval '1 second' WHERE id=$1")
        .bind(attempt).execute(pool).await.unwrap();
}

async fn wakeups(pool: &PgPool, run: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM outbox_events WHERE aggregate_id=$1 AND dedupe_key LIKE '%:ready:invocation:%'")
        .bind(run).fetch_one(pool).await.unwrap()
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn submission_claim_recovery_preserves_identity_and_requires_deduplication(pool: PgPool) {
    for deduplicates in [false, true] {
        let seed = seed_running_attempt(&pool, 60, 120).await;
        let key = deferred::Attempt::from_chunk(&seed.chunk, seed.attempt_id);
        let first = start(&pool, &seed, deduplicates).await;
        let request = wire::Submission::decode(&first.request, 1024, false).unwrap();
        assert_eq!(request.invocation_id, first.invocation_id);
        assert_eq!(request.case.input, json!({"prompt":"frozen"}));
        assert!(
            deferred::reclaim_submission(&pool, key, 60)
                .await
                .unwrap()
                .is_none()
        );
        assert!(deferred::release(&pool, &seed.chunk).await.unwrap());
        assert_eq!(wakeups(&pool, seed.run_id).await, 0);
        expire_claim(&pool, seed.attempt_id).await;
        let resumed = deferred::reclaim_submission(&pool, key, 60).await.unwrap();
        assert!(
            !deferred::record_receipt(&pool, key, &first, &receipt(first.invocation_id))
                .await
                .unwrap()
        );
        if deduplicates {
            let current = resumed.unwrap();
            assert_eq!(current.invocation_id, first.invocation_id);
            assert_eq!(current.request, first.request);
            assert_ne!(current.token, first.token);
            assert!(
                deferred::record_receipt(&pool, key, &current, &receipt(current.invocation_id))
                    .await
                    .unwrap()
            );
            assert!(
                deferred::reclaim_submission(&pool, key, 60)
                    .await
                    .unwrap()
                    .is_none()
            );
        } else {
            assert!(resumed.is_none());
            let state: (String, bool) = sqlx::query_as("SELECT invocation_status::text,leased_until IS NULL FROM execution_attempts WHERE id=$1")
                .bind(seed.attempt_id).fetch_one(&pool).await.unwrap();
            assert_eq!(state, ("submission_unknown".into(), true));
            assert_eq!(
                deferred::accept(
                    &pool,
                    key,
                    first.invocation_id,
                    proof(&first),
                    &completion(first.invocation_id, "answer")
                )
                .await
                .unwrap(),
                Acceptance::Accepted
            );
            assert_eq!(wakeups(&pool, seed.run_id).await, 1);
        }
    }
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn completion_and_release_are_durable_and_scoring_reuses_the_attempt(pool: PgPool) {
    for release_first in [false, true] {
        let seed = seed_running_attempt(&pool, 60, 120).await;
        let key = deferred::Attempt::from_chunk(&seed.chunk, seed.attempt_id);
        let claim = start(&pool, &seed, false).await;
        let result = completion(claim.invocation_id, "original evidence");
        if release_first {
            assert!(deferred::release(&pool, &seed.chunk).await.unwrap());
        }
        let (first, second) = tokio::join!(
            deferred::accept(&pool, key, claim.invocation_id, proof(&claim), &result),
            deferred::accept(&pool, key, claim.invocation_id, proof(&claim), &result),
        );
        let pair = (first.unwrap(), second.unwrap());
        assert!(matches!(
            pair,
            (Acceptance::Accepted, Acceptance::Duplicate)
                | (Acceptance::Duplicate, Acceptance::Accepted)
        ));
        assert!(
            !deferred::record_receipt(&pool, key, &claim, &receipt(claim.invocation_id))
                .await
                .unwrap()
        );
        assert!(
            deferred::accept(
                &pool,
                key,
                claim.invocation_id,
                proof(&claim),
                &completion(claim.invocation_id, "conflicting evidence")
            )
            .await
            .is_err()
        );
        if !release_first {
            assert_eq!(wakeups(&pool, seed.run_id).await, 0);
            assert!(deferred::release(&pool, &seed.chunk).await.unwrap());
        }
        assert_eq!(wakeups(&pool, seed.run_id).await, 1);

        let mut chunk = seed.chunk.clone();
        let mut profile = run_profile(vec![case_group(
            "default",
            "classification",
            vec!["sentiment"],
            "test/evaluator:1.0.0",
            AggregationMethod::WeightedMean,
        )]);
        let Harness::DirectProbe {
            completion,
            capabilities,
            ..
        } = &mut profile.harness
        else {
            panic!("fixture harness");
        };
        *completion = Completion::Polling {
            status_url: "https://harness.example/status/{provider_reference}".into(),
        };
        *capabilities = Some(Capabilities {
            idempotent_submission: false,
            cancellation: None,
        });
        profile.defaults.max_attempts = 1;
        let mut case = worker_case(None);
        case.case_id = seed.case_id;
        let plan = evaluation_plan_for_case(&profile, &case).unwrap();
        let replacement = AttemptLeaseContext {
            worker_id: Uuid::now_v7(),
            worker_host: None,
            queue_message_id: Uuid::now_v7(),
            broker_message_id: None,
            lease_seconds: 60,
        };

        for pass in 0..2 {
            chunk.lease_token = sqlx::query_scalar("UPDATE run_chunks SET status='leased', lease_token=gen_random_uuid(),leased_until=now()+interval '1 minute' WHERE id=$1 RETURNING lease_token")
                .bind(chunk.id).fetch_one(&pool).await.unwrap();
            let mut allocations = allocate_execution_attempts_for_cases(
                &pool,
                &chunk,
                &replacement,
                &profile,
                std::slice::from_ref(&case),
                std::slice::from_ref(&plan),
            )
            .await
            .unwrap();
            deferred::load_scoring_inputs(&pool, &chunk, replacement.worker_id, &mut allocations)
                .await
                .unwrap();
            let allocation = allocations.pop().unwrap();
            assert!(allocation.should_process);
            assert_eq!(allocation.attempt_id, Some(seed.attempt_id));
            assert_eq!(allocation.attempt_no, 1);
            let output = allocation.deferred_output.unwrap().output().unwrap();
            assert_eq!(
                output.captured_body.as_ref().unwrap().bytes(),
                b"original evidence"
            );
            if pass == 1 {
                let template =
                    evidence_for_seed(&pool, &seed, crate::contracts::run::PersistenceMode::Full)
                        .await;
                let metadata = template.metadata_json().unwrap();
                let provenance = serde_json::from_value(metadata["provenance"].clone()).unwrap();
                let (_, retained) = super::super::response::prepare(output, &profile, provenance)
                    .await
                    .unwrap();
                let actual = retained.metadata_json().unwrap();
                sqlx::query("UPDATE run_snapshots SET config_snapshot=jsonb_set(config_snapshot, '{response_policy,configuration_hash}', $3) WHERE run_id=$1 AND run_shard=$2")
                    .bind(seed.run_id).bind(seed.run_shard).bind(&actual["configuration_hash"])
                    .execute(&pool).await.unwrap();
                let mut transition = terminal(&seed);
                transition.response_evidence = Some(retained);
                finalize_execution_terminal_transitions(
                    &pool,
                    chunk.tenant_id,
                    seed.run_id,
                    seed.run_shard,
                    replacement.worker_id,
                    1,
                    &[transition],
                )
                .await
                .unwrap();
                let attempt: (String, bool) = sqlx::query_as(
                    "SELECT status::text, invocation_evidence IS NULL FROM execution_attempts WHERE id=$1",
                ).bind(seed.attempt_id).fetch_one(&pool).await.unwrap();
                assert_eq!(attempt, ("failed_evaluation".into(), true));
                let retained = crate::db::tables::execution_attempts::select_response_evidence(
                    &pool,
                    seed.run_id,
                    seed.run_shard,
                    seed.attempt_id,
                )
                .await
                .unwrap()
                .unwrap();
                assert_eq!(
                    retained.reconstruct().unwrap().original.bytes(),
                    b"original evidence"
                );
                assert_eq!(
                    crate::db::workflows::chunk_processing::mark_chunk_completed_and_settle(
                        &pool, &chunk
                    )
                    .await
                    .unwrap(),
                    1
                );
                continue;
            }
            sqlx::query("UPDATE run_chunks SET leased_until=now()-interval '1 second' WHERE id=$1")
                .bind(chunk.id)
                .execute(&pool)
                .await
                .unwrap();
            assert_eq!(
                run_dispatch::recover_expired_chunk_leases(&pool, 3, 10)
                    .await
                    .unwrap()
                    .recovered_chunks,
                1
            );
        }
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM execution_attempts WHERE execution_id=$1")
                .bind(seed.execution_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 1);
    }
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn completion_fences_expired_claims_deadlines_and_wrong_authority(pool: PgPool) {
    let seed = seed_running_attempt(&pool, 60, 120).await;
    let key = deferred::Attempt::from_chunk(&seed.chunk, seed.attempt_id);
    let claim = start(&pool, &seed, false).await;
    let body = completion(claim.invocation_id, "answer");
    assert_eq!(
        deferred::accept(
            &pool,
            key,
            claim.invocation_id,
            CompletionProof::VerifiedCallback {
                invocation_id: Uuid::now_v7()
            },
            &body
        )
        .await
        .unwrap(),
        Acceptance::Ineligible
    );
    assert!(
        deferred::accept(
            &pool,
            key,
            claim.invocation_id,
            proof(&claim),
            &completion(claim.invocation_id, &"x".repeat(1025)),
        )
        .await
        .is_err()
    );
    let mut wrong_agent: serde_json::Value = serde_json::from_slice(&body).unwrap();
    wrong_agent["executed_agent"]["version"] = json!("2.0.0");
    assert!(
        deferred::accept(
            &pool,
            key,
            claim.invocation_id,
            proof(&claim),
            &serde_json::to_vec(&wrong_agent).unwrap(),
        )
        .await
        .is_err()
    );
    expire_claim(&pool, seed.attempt_id).await;
    assert_eq!(
        deferred::accept(
            &pool,
            key,
            claim.invocation_id,
            CompletionProof::RequestClaim(claim.token),
            &body
        )
        .await
        .unwrap(),
        Acceptance::Ineligible
    );
    sqlx::query("UPDATE execution_attempts SET invocation_claim_until=NULL,invocation_claim=NULL,invocation_deadline=now()-interval '1 second' WHERE id=$1")
        .bind(seed.attempt_id).execute(&pool).await.unwrap();
    assert_eq!(
        deferred::accept(&pool, key, claim.invocation_id, proof(&claim), &body)
            .await
            .unwrap(),
        Acceptance::Ineligible
    );
    let preserved: (String, bool) = sqlx::query_as(
        "SELECT status::text,invocation_evidence IS NULL FROM execution_attempts WHERE id=$1",
    )
    .bind(seed.attempt_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(preserved, ("awaiting_agent".into(), true));
    assert_eq!(wakeups(&pool, seed.run_id).await, 0);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn two_invocations_share_one_ready_chunk_event(pool: PgPool) {
    let seed = seed_running_attempt(&pool, 60, 120).await;
    let first_key = deferred::Attempt::from_chunk(&seed.chunk, seed.attempt_id);
    let first = start(&pool, &seed, false).await;
    // Allocate a second case using the same chunk lease.
    let profile = run_profile(vec![case_group(
        "default",
        "classification",
        vec!["sentiment"],
        "test/evaluator:1.0.0",
        AggregationMethod::WeightedMean,
    )]);
    let case = worker_case(None);
    let plan = evaluation_plan_for_case(&profile, &case).unwrap();
    let lease = AttemptLeaseContext {
        worker_id: seed.worker_id,
        worker_host: None,
        queue_message_id: Uuid::now_v7(),
        broker_message_id: None,
        lease_seconds: 60,
    };
    let other = allocate_execution_attempts_for_cases(
        &pool,
        &seed.chunk,
        &lease,
        &profile,
        &[case],
        &[plan],
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    let second_key = deferred::Attempt::from_chunk(&seed.chunk, other.attempt_id.unwrap());
    let second = deferred::start(
        &pool,
        second_key,
        seed.chunk.lease_token.unwrap(),
        intent(false),
        60,
    )
    .await
    .unwrap()
    .unwrap();
    assert_ne!(first.invocation_id, second.invocation_id);
    let probe = query_probe::Probe::new(&pool).await;
    probe.start();
    assert!(deferred::release(&probe.pool, &seed.chunk).await.unwrap());
    let release_exchanges = probe.stop().0;
    let mut exchanges = Vec::new();
    for (key, claim) in [(first_key, first), (second_key, second)] {
        let body = completion(claim.invocation_id, "answer");
        probe.start();
        assert_eq!(
            deferred::accept(
                &probe.pool,
                key,
                claim.invocation_id,
                CompletionProof::RequestClaim(claim.token),
                &body
            )
            .await
            .unwrap(),
            Acceptance::Accepted
        );
        exchanges.push(probe.stop().0);
    }
    probe.close().await;
    assert!(exchanges[1] < exchanges[0]);
    println!(
        "deferred execution shard protocol exchanges: release={release_exchanges}, first_completion={}, coalesced_completion={}",
        exchanges[0], exchanges[1]
    );
    assert_eq!(wakeups(&pool, seed.run_id).await, 1);
    let ready: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM executions WHERE run_id=$1 AND status='awaiting_evaluators'",
    )
    .bind(seed.run_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(ready, 2);
}
