//! Restricted runtime callback acceptance and recovery over real placement pools.

use std::sync::Mutex;

use chrono::{
    DateTime,
    Utc,
};
use vigilo::operations::{
    Error,
    invocations::{
        Callback,
        CallbackIssuer,
        Completion,
        Target,
    },
    resources,
};

use super::*;

#[derive(Default)]
pub(super) struct Issuer {
    issued: Mutex<Vec<(Target, DateTime<Utc>)>>,
}

impl CallbackIssuer for Issuer {
    fn issue(
        &self,
        target: Target,
        deadline: DateTime<Utc>,
    ) -> resources::Resolution<'_, Callback> {
        Box::pin(async move {
            // Deterministic fixture value. A real host uses its secret issuer.
            let token = format!(
                "fixture-{}-{}",
                target.invocation_id,
                deadline.timestamp_micros()
            );
            self.issued.lock().unwrap().push((target, deadline));
            Ok(Callback {
                url: "https://callback.example.test/results".into(),
                token,
            })
        })
    }
}

fn outcome(id: Uuid, text: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "schema_version":"1.0.0",
        "invocation_id":id,
        "status":"completed",
        "executed_agent":{"provider":"integration","name":"mock-agent","version":"1.0.0"},
        "evidence":{
            "content_type":"application/json","encoding":"utf8",
            "body":json!({"text":text}).to_string()
        },
    }))
    .unwrap()
}

async fn exercise(fixture: &mut Fixture) -> anyhow::Result<()> {
    let issuer = Arc::new(Issuer::default());
    fixture.callback_issuer = Some(issuer.clone());
    fixture.setup().await?;
    fixture.runtime.take().unwrap().close().await;
    for url in &mut fixture.db.urls {
        let probe = postgres_probe::Probe::start(url)?;
        *url = probe.url.clone();
        fixture.probes.push(probe);
    }
    fixture.construct_runtime().await?;
    let runtime = fixture.runtime.clone().unwrap();
    let service = runtime.invocations().context("callback service")?;
    let coordinator = runtime.coordinator();
    let owner = *fixture.owners.keys().next().unwrap();
    let other = *fixture.owners.keys().nth(1).unwrap();
    let upload = fixture.upload(owner, 1).await?;
    let db = fixture.db.admin[1].clone();
    let observed =
        |fixture: &Fixture, index: usize| fixture.probes[index].snapshot().query_sync_exchanges;
    for (kind, recover, expire, revoke) in [
        ("direct_probe", false, false, false),
        ("delegated_trial", true, false, false),
        ("delegated_trial", false, true, false),
        ("delegated_trial", false, false, true),
    ] {
        let run = Uuid::now_v7();
        fixture.object_owners.insert(run, owner);
        let mut input = fixture.input(run, upload)?;
        input.profile["harness"]["kind"] = json!(kind);
        input.profile["harness"]["completion"] = json!({"kind":"callback","deadline_secs":120});
        input.profile["harness"]["capabilities"] = json!({"idempotent_submission":recover});
        if revoke {
            input.profile["harness"]["capabilities"]["cancellation"] = json!({
                "url":fixture.agent.url.replace("/invoke","/cancel"),
                "credential":"agent-api",
            });
        }
        let user = fixture.user(owner);
        let prepared = user.runs().prepare_run(input, &fixture.stop).await?;
        user.runs().accept_run(prepared, &fixture.stop).await?;
        coordinator.once().await?;

        fixture
            .agent
            .hold_receipt
            .store(!recover && !expire && !revoke, Ordering::SeqCst);
        fixture.agent.drop_receipt.store(recover, Ordering::SeqCst);
        let worker = runtime.worker();
        let submitted = tokio::spawn(async move { worker.once().await });
        tokio::time::timeout(Duration::from_secs(10), fixture.agent.entered.notified()).await?;
        let (target, deadline) = issuer.issued.lock().unwrap().last().unwrap().clone();
        assert_eq!(target.run_id, run);
        assert_eq!(target.tenant_id, owner);
        let stored: (bool, bool) = sqlx::query_as(
            "SELECT invocation_request IS NOT NULL,
                    NOT (convert_from(invocation_request,'UTF8')::jsonb ? 'callback')
             FROM execution_attempts WHERE id=$1",
        )
        .bind(target.attempt_id)
        .fetch_one(&db)
        .await?;
        assert_eq!(stored, (true, true));

        let body = outcome(target.invocation_id, "good safe");
        if revoke {
            assert_eq!(submitted.await??, 1);
            user.control_run(run, vigilo::operations::ExecutionControl::Revoked)
                .await?;
            coordinator.once().await?;
            verify_delivery(fixture, &service, &target, &body, other, true).await?;
            let before = observed(fixture, 0);
            service.advance(&target).await?;
            service.advance(&target).await?;
            assert_eq!(
                observed(fixture, 0),
                before,
                "cancellation used the control DB"
            );
            assert_eq!(fixture.agent.cancellations.load(Ordering::SeqCst), 1);
            let state: (String, String) = sqlx::query_as(
                "SELECT invocation_status::text,invocation_cancel_result FROM execution_attempts WHERE id=$1",
            ).bind(target.attempt_id).fetch_one(&db).await?;
            assert_eq!(state, ("cancelled".into(), "accepted".into()));
            continue;
        }
        if recover || expire {
            assert_eq!(submitted.await??, 1);
            if recover {
                sqlx::query("UPDATE execution_attempts SET invocation_claim_until=now()-interval '1 second',invocation_next_at=now()-interval '1 second' WHERE id=$1")
                    .bind(target.attempt_id).execute(&db).await?;
            } else {
                sqlx::query("UPDATE execution_attempts SET invocation_deadline=now()-interval '1 second' WHERE id=$1")
                    .bind(target.attempt_id).execute(&db).await?;
            }
            let before = observed(fixture, 0);
            service.advance(&target).await?;
            assert_eq!(observed(fixture, 0), before, "advance used the control DB");
            if recover {
                tokio::time::timeout(Duration::from_secs(5), fixture.agent.entered.notified())
                    .await?;
                {
                    let issued = issuer.issued.lock().unwrap();
                    assert_eq!(issued[issued.len() - 2], (target.clone(), deadline));
                    assert_eq!(issued.last().unwrap(), &(target.clone(), deadline));
                }
                let state: String = sqlx::query_scalar(
                    "SELECT invocation_status::text FROM execution_attempts WHERE id=$1",
                )
                .bind(target.attempt_id)
                .fetch_one(&db)
                .await?;
                assert_eq!(state, "waiting");
            }
            verify_delivery(fixture, &service, &target, &body, other, expire).await?;
        } else {
            assert!(
                !submitted.is_finished(),
                "completion should precede the receipt"
            );
            verify_delivery(fixture, &service, &target, &body, other, false).await?;
            fixture.agent.receipt_gate.add_permits(1);
            assert_eq!(submitted.await??, 1);
        }

        // The worker that submitted the invocation has exited. A bounded,
        // placement-local operation republishes readiness to a fresh worker.
        let before = observed(fixture, 0);
        assert!(coordinator.publish_execution_outbox(SHARD_ALIAS).await? > 0);
        assert_eq!(
            observed(fixture, 0),
            before,
            "outbox relay used the control DB"
        );
        assert_eq!(runtime.worker().once().await?, 1);
        coordinator.once().await?;
        let status = fixture.await_terminal(owner, run).await?;
        assert_eq!(status.gate_passed, !expire);
        assert_eq!(status.details.terminal_execution_count, 1);
        let actual: Vec<(Uuid, String)> =
            sqlx::query_as("SELECT id,status::text FROM execution_attempts WHERE run_id=$1")
                .bind(run)
                .fetch_all(&db)
                .await?;
        let want = if expire { "timed_out" } else { "completed" };
        assert_eq!(actual, vec![(target.attempt_id, want.to_string())]);
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM evaluator_results WHERE run_id=$1")
                .bind(run)
                .fetch_one(&db)
                .await?;
        assert_eq!(count, i64::from(!expire));
    }
    Ok(())
}

async fn verify_delivery(
    fixture: &Fixture,
    service: &vigilo::operations::invocations::Service,
    target: &Target,
    body: &[u8],
    other: Uuid,
    expire: bool,
) -> anyhow::Result<()> {
    let before = fixture.probes[0].snapshot();
    let shard_before = fixture.probes[1].snapshot();
    let mut wrong = target.clone();
    wrong.tenant_id = other;
    assert_eq!(
        service.complete(&wrong, body).await?,
        Completion::Ineligible
    );
    wrong = target.clone();
    wrong.write_epoch += 1;
    assert_eq!(
        service.complete(&wrong, body).await?,
        Completion::Ineligible
    );
    wrong = target.clone();
    wrong.invocation_id = Uuid::new_v4();
    assert_eq!(
        service.complete(&wrong, body).await?,
        Completion::Ineligible
    );
    if expire {
        assert_eq!(
            service.complete(target, body).await?,
            Completion::Ineligible
        );
    } else {
        assert!(matches!(
            service
                .complete(target, &outcome(Uuid::new_v4(), "wrong invocation"))
                .await,
            Err(Error::InvalidInput(_))
        ));
        let (first, second) = tokio::join!(
            service.complete(target, body),
            service.complete(target, body),
        );
        assert!(matches!(
            (first?, second?),
            (Completion::Accepted, Completion::Duplicate)
                | (Completion::Duplicate, Completion::Accepted)
        ));
        assert!(matches!(
            service
                .complete(target, &outcome(target.invocation_id, "conflict"))
                .await,
            Err(Error::Conflict(_))
        ));
    }
    let after = fixture.probes[0].snapshot();
    assert_eq!(after.query_sync_exchanges, before.query_sync_exchanges);
    assert_eq!(after.frontend_bytes, before.frontend_bytes);
    let shard_delta =
        fixture.probes[1].snapshot().query_sync_exchanges - shard_before.query_sync_exchanges;
    assert!(shard_delta > 0);
    println!(
        "callback delivery verification: expired={expire} control_exchanges=0 owning_shard_exchanges={shard_delta}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn callbacks_are_fenced_replay_safe_and_resume_the_original_attempt() -> anyhow::Result<()> {
    if std::env::var(E2E_ENABLED_ENV).as_deref() != Ok("1") {
        return Ok(());
    }
    let mut fixture = Fixture::new().await?;
    let result = std::panic::AssertUnwindSafe(Box::pin(exercise(&mut fixture)))
        .catch_unwind()
        .await;
    let cleanup = fixture.cleanup().await;
    match result {
        Ok(result) => result.and(cleanup),
        Err(panic) => {
            cleanup?;
            std::panic::resume_unwind(panic)
        }
    }
}
