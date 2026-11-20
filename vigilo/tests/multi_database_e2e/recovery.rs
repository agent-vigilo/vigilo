//! Live recovery ownership through the public CLI.
//! Covers admission after a delayed storage failure and resumption after an
//! expired worker lease on a draining shard moves to another database.
use super::*;

#[path = "../support/storage_proxy.rs"]
pub(super) mod storage_proxy;

#[tokio::test]
async fn aggregation_recovers_from_interrupted_page_and_committed_seal() -> anyhow::Result<()> {
    let Some(fixture) = E2eFixture::setup().await? else {
        return Ok(());
    };
    let run = create_run(
        &fixture.config,
        RunCreateInput {
            default_execution_alias: SHARD_ALIAS,
            shard_assignment_policy: "single-default",
            agent_url: &fixture.agent.url,
            profile_id: "e2e_aggregation_recovery",
            case_count: 1,
            case_mode: CaseMode::Passing,
            evaluator_ref: SENTIMENT_EVALUATOR_REF,
        },
    )?;
    let once = || {
        run_vigilo_ok(
            &fixture.config,
            SHARD_ALIAS,
            "single-default",
            ["coordinator", "once"],
        )
    };
    once()?;
    run_vigilo_ok(
        &fixture.config,
        SHARD_ALIAS,
        "single-default",
        ["worker", "once"],
    )?;

    let checkpoint = || {
        sqlx::query_as::<_, (bool, i32, i32, Option<Value>)>(
            "SELECT sealed,settled_chunk_count,passed_execution_count,aggregation_state
             FROM run_shard_summaries WHERE run_id=$1 AND run_shard=0",
        )
        .bind(run)
        .fetch_one(&fixture.shard)
    };
    let receipt = || {
        sqlx::query_as::<_, (Uuid, bool, Value)>(
            "SELECT s.chunk_id,c.consumed,s.summary FROM run_chunk_settlements s
             JOIN run_chunk_settlement_consumptions c USING(run_id,run_shard,chunk_id)
             WHERE s.run_id=$1 AND s.run_shard=0",
        )
        .bind(run)
        .fetch_one(&fixture.shard)
    };
    let original = checkpoint().await?;
    assert_eq!(original, (false, 0, 0, None));
    let (chunk_id, consumed, evidence) = receipt().await?;
    assert!(!consumed);
    assert_eq!(evidence["passed_execution_count"], 1);

    interrupt_coordinator_commit(
        &fixture,
        &fixture.shard,
        "run_chunk_settlement_consumptions",
        "NEW.consumed AND NOT OLD.consumed",
    )
    .await?;
    assert_eq!(checkpoint().await?, original);
    assert_eq!(receipt().await?, (chunk_id, false, evidence.clone()));
    assert_eq!(
        outbox_event_count(&fixture.primary, run, "run.completed").await?,
        0
    );

    // Commit the receipt page, leaving the canonical seal for another process.
    run_vigilo_ok(
        &fixture.config,
        SHARD_ALIAS,
        "single-default",
        [
            "coordinator",
            "--max-aggregation-pages-per-cycle",
            "1",
            "once",
        ],
    )?;
    let (sealed, settled, passed, state) = checkpoint().await?;
    assert_eq!((sealed, settled, passed), (false, 1, 1));
    assert!(state.is_some());
    assert_eq!(receipt().await?, (chunk_id, true, evidence.clone()));

    // The remote shard seal commits before the coordinator updates the run.
    interrupt_coordinator_commit(
        &fixture,
        &fixture.primary,
        "runs",
        "NEW.status='completed' AND OLD.status='finalizing'",
    )
    .await?;
    let seal = checkpoint().await?;
    assert_eq!((seal.0, seal.1, seal.2), (true, 1, 1));
    let scorecards = || {
        sqlx::query_scalar::<_, Value>("SELECT scorecard FROM run_scorecards WHERE run_id=$1")
            .bind(run)
            .fetch_all(&fixture.primary)
    };
    assert!(scorecards().await?.is_empty());
    assert_eq!(
        outbox_event_count(&fixture.primary, run, "run.completed").await?,
        0
    );

    let expired = sqlx::query(
        "UPDATE runs SET coordinator_leased_until=clock_timestamp()-interval '1 second'
         WHERE id=$1 AND status='finalizing' AND coordinator_id IS NOT NULL",
    )
    .bind(run)
    .execute(&fixture.primary)
    .await?;
    assert_eq!(expired.rows_affected(), 1);
    once()?;
    let persisted = scorecards().await?;
    assert_eq!(persisted.len(), 1);
    once()?;
    assert_eq!(scorecards().await?, persisted);
    assert_eq!(checkpoint().await?, seal);
    assert_eq!(receipt().await?, (chunk_id, true, evidence));
    assert_eq!(execution_count(&fixture.shard, run).await?, 1);
    assert_eq!(
        outbox_event_count(&fixture.primary, run, "run.completed").await?,
        1
    );
    let status = run_vigilo_json(
        &fixture.config,
        SHARD_ALIAS,
        "single-default",
        ["run", "status", &run.to_string()],
    )?;
    assert_eq!(status["data"]["status"], "completed");
    assert_eq!(status["data"]["gate_status"], "pass");
    assert_eq!(status["data"]["terminal_execution_count"], 1);
    Ok(())
}

/// Abort a real coordinator process at a test-owned transaction barrier.
async fn interrupt_coordinator_commit(
    fixture: &E2eFixture,
    db: &PgPool,
    table: &str,
    condition: &str,
) -> anyhow::Result<()> {
    sqlx::raw_sql(&format!(
        "CREATE FUNCTION hold_coordinator_commit() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN PERFORM pg_advisory_xact_lock(42188); RETURN NEW; END $$;
         CREATE TRIGGER hold_coordinator_commit AFTER UPDATE ON {table}
         FOR EACH ROW WHEN ({condition}) EXECUTE FUNCTION hold_coordinator_commit();",
    ))
    .execute(db)
    .await?;
    let mut barrier = db.begin().await?;
    let blocker: i32 =
        sqlx::query_scalar("SELECT pg_backend_pid() FROM pg_advisory_xact_lock(42188)")
            .fetch_one(&mut *barrier)
            .await?;
    let mut command = tokio::process::Command::from(vigilo_command(
        &fixture.config,
        SHARD_ALIAS,
        "single-default",
    ));
    let mut child = command
        .kill_on_drop(true)
        .args(["coordinator", "once"])
        .spawn()?;
    let pid = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(pid) = sqlx::query_scalar::<_, i32>(
                "SELECT pid FROM pg_stat_activity
                 WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid))",
            )
            .bind(blocker)
            .fetch_optional(db)
            .await?
            {
                return Ok::<_, anyhow::Error>(pid);
            }
            anyhow::ensure!(
                child.try_wait()?.is_none(),
                "coordinator exited before updating {table}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;

    child.kill().await?;
    // Keep the lock held until PostgreSQL aborts the disconnected transaction.
    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1, 5000)")
        .bind(pid)
        .fetch_one(db)
        .await?;
    if !terminated {
        let running: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)")
                .bind(pid)
                .fetch_one(db)
                .await?;
        assert!(
            !running,
            "checkpoint backend must exit before releasing the barrier"
        );
    }
    barrier.rollback().await?;
    sqlx::raw_sql(&format!(
        "DROP TRIGGER hold_coordinator_commit ON {table}; DROP FUNCTION hold_coordinator_commit();",
    ))
    .execute(db)
    .await?;
    Ok(())
}

#[tokio::test]
async fn split_dispatch_replays_after_execution_commit_and_control_rollback() -> anyhow::Result<()>
{
    let Some(fixture) = E2eFixture::setup().await? else {
        return Ok(());
    };
    let once = ["coordinator", "--max-dispatch-per-cycle", "1", "once"];
    for (cases, dispatched_before, expected_status) in [(1, 0, "pending"), (101, 1, "running")] {
        let run = create_run(
            &fixture.config,
            RunCreateInput {
                default_execution_alias: SHARD_ALIAS,
                shard_assignment_policy: "single-default",
                agent_url: &fixture.agent.url,
                profile_id: "dispatch_commit_handoff",
                case_count: cases,
                case_mode: CaseMode::Passing,
                evaluator_ref: SENTIMENT_EVALUATOR_REF,
            },
        )?;
        if dispatched_before != 0 {
            run_vigilo_ok(&fixture.config, SHARD_ALIAS, "single-default", once)?;
        }
        let events = || {
            sqlx::query_as::<_, (Uuid, String)>(
                "SELECT id, dedupe_key FROM outbox_events WHERE aggregate_id=$1 AND event_type='run.chunk.ready' ORDER BY id",
            )
            .bind(run)
            .fetch_all(&fixture.shard)
        };
        assert_eq!(events().await?.len(), dispatched_before);
        sqlx::raw_sql(
            "CREATE FUNCTION reject_dispatch_cursor() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN RAISE EXCEPTION 'test interruption before control cursor commit'; END $$;
             CREATE TRIGGER reject_dispatch_cursor BEFORE UPDATE ON run_shard_dispatch_cursors
             FOR EACH ROW EXECUTE FUNCTION reject_dispatch_cursor();",
        )
        .execute(&fixture.primary)
        .await?;
        let interrupted = run_vigilo_output(&fixture.config, SHARD_ALIAS, "single-default", once);
        sqlx::raw_sql(
            "DROP TRIGGER reject_dispatch_cursor ON run_shard_dispatch_cursors;
             DROP FUNCTION reject_dispatch_cursor();",
        )
        .execute(&fixture.primary)
        .await?;
        let interrupted = interrupted?;
        assert!(!interrupted.status.success());
        assert!(
            [&interrupted.stdout, &interrupted.stderr]
                .iter()
                .any(|output| String::from_utf8_lossy(output)
                    .contains("test interruption before control cursor commit")),
            "{interrupted:?}"
        );
        let persisted = events().await?;
        assert_eq!(persisted.len(), dispatched_before + 1);
        let (status, open): (String, i64) = sqlx::query_as(
            "SELECT status::text, (SELECT count(*) FROM run_shard_dispatch_cursors WHERE run_id=$1 AND status='open') FROM runs WHERE id=$1",
        )
        .bind(run)
        .fetch_one(&fixture.primary)
        .await?;
        assert_eq!((status.as_str(), open), (expected_status, 1));

        // A fresh coordinator repairs control without creating new readiness.
        run_vigilo_ok(&fixture.config, SHARD_ALIAS, "single-default", once)?;
        assert_eq!(events().await?, persisted);
        let (open, starts): (i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM run_shard_dispatch_cursors WHERE run_id=$1 AND status='open'),
                    (SELECT count(*) FROM outbox_events WHERE aggregate_id=$1 AND event_type='run.started')",
        )
        .bind(run)
        .fetch_one(&fixture.primary)
        .await?;
        assert_eq!((open, starts), (0, 1));
        let terminal = drive_run_to_status(
            &fixture,
            "replay split dispatch",
            SHARD_ALIAS,
            "single-default",
            run,
            "completed",
        )
        .await?;
        assert_eq!(terminal["data"]["gate_status"], "pass");
        assert_eq!(execution_count(&fixture.shard, run).await?, cases as i64);
        assert_eq!(events().await?, persisted);
    }
    Ok(())
}

#[tokio::test]
async fn routed_cancellation_drains_cursor_after_dispatch_releases_claim() -> anyhow::Result<()> {
    let Some(fixture) = E2eFixture::setup().await? else {
        return Ok(());
    };
    let run = create_run(
        &fixture.config,
        RunCreateInput {
            default_execution_alias: SHARD_ALIAS,
            shard_assignment_policy: "single-default",
            agent_url: &fixture.agent.url,
            profile_id: "cancel_held_dispatch",
            case_count: 1,
            case_mode: CaseMode::Passing,
            evaluator_ref: SENTIMENT_EVALUATOR_REF,
        },
    )?;
    let mut claim = fixture.primary.begin().await?;
    let claimed: i16 = sqlx::query_scalar(
        "SELECT run_shard FROM run_shard_dispatch_cursors
         WHERE run_id=$1 AND status='open' FOR UPDATE SKIP LOCKED",
    )
    .bind(run)
    .fetch_one(&mut *claim)
    .await?;
    assert_eq!(claimed, 0);
    let holder: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *claim)
        .await?;
    let mut cancel = tokio::process::Command::from(vigilo_command(
        &fixture.config,
        SHARD_ALIAS,
        "single-default",
    ));
    cancel
        .kill_on_drop(true)
        .args(["run", "cancel", &run.to_string()]);

    let output = tokio::time::timeout(Duration::from_secs(30), async {
        let release = async {
            loop {
                let waiting: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity
                     WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)))",
                )
                .bind(holder)
                .fetch_one(&fixture.primary)
                .await?;
                if waiting {
                    let revoked: bool = sqlx::query_scalar(
                        "SELECT execution_control='revoked' FROM runs WHERE id=$1",
                    )
                    .bind(run)
                    .fetch_one(&fixture.primary)
                    .await?;
                    assert!(
                        revoked,
                        "cancellation must commit revocation before waiting on the cursor"
                    );
                    return claim.rollback().await;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        let (output, ()) = tokio::try_join!(
            async { Ok::<_, anyhow::Error>(cancel.output().await?) },
            async {
                release.await?;
                Ok::<_, anyhow::Error>(())
            }
        )?;
        Ok::<_, anyhow::Error>(output)
    })
    .await??;
    assert!(output.status.success(), "{output:?}");
    run_vigilo_ok(
        &fixture.config,
        SHARD_ALIAS,
        "single-default",
        ["coordinator", "once"],
    )?;
    let (status, open, cancelled_events): (String, i64, i64) = sqlx::query_as(
        "SELECT status::text,
           (SELECT count(*) FROM run_shard_dispatch_cursors WHERE run_id=$1 AND status='open'),
           (SELECT count(*) FROM outbox_events WHERE aggregate_id=$1 AND event_type='run.cancelled')
         FROM runs WHERE id=$1",
    )
    .bind(run)
    .fetch_one(&fixture.primary)
    .await?;
    assert_eq!(
        (status.as_str(), open, cancelled_events),
        ("cancelled", 0, 1)
    );
    assert_eq!(
        run_chunk_status_count(&fixture.shard, run, "cancelled").await?,
        1
    );
    assert_eq!(execution_count(&fixture.shard, run).await?, 0);
    Ok(())
}

#[tokio::test]
async fn draining_shard_recovers_expired_lease_and_resumes_on_target() -> anyhow::Result<()> {
    let Some(fixture) = E2eFixture::setup().await? else {
        return Ok(());
    };
    let run = create_run(
        &fixture.config,
        RunCreateInput {
            default_execution_alias: PRIMARY_ALIAS,
            shard_assignment_policy: "single-default",
            agent_url: &fixture.agent.url,
            profile_id: "recovery_during_drain",
            case_count: 1,
            case_mode: CaseMode::Passing,
            evaluator_ref: SENTIMENT_EVALUATOR_REF,
        },
    )?;
    run_vigilo_ok(
        &fixture.config,
        PRIMARY_ALIAS,
        "single-default",
        ["coordinator", "once"],
    )?;
    let token = Uuid::now_v7();
    sqlx::query("UPDATE run_chunks SET status='leased', lease_token=$2, leased_until=clock_timestamp()+interval '10 minutes' WHERE run_id=$1")
        .bind(run)
        .bind(token)
        .execute(&fixture.primary)
        .await?;
    // A live lease refuses the original delivery. The resumed worker must
    // receive the new recovery event after the source lease expires.
    run_vigilo_ok(
        &fixture.config,
        PRIMARY_ALIAS,
        "single-default",
        ["worker", "once"],
    )?;
    let run_id = run.to_string();
    let move_args = ["shard", "move", &run_id, "0", "--alias", SHARD_ALIAS];
    let pending = run_vigilo_output(&fixture.config, PRIMARY_ALIAS, "single-default", move_args)?;
    assert!(!pending.status.success());
    let output = format!(
        "{}\n{}",
        String::from_utf8_lossy(&pending.stdout),
        String::from_utf8_lossy(&pending.stderr)
    );
    assert!(output.contains("route remains draining"), "{output}");
    run_vigilo_ok(
        &fixture.config,
        PRIMARY_ALIAS,
        "single-default",
        ["coordinator", "once"],
    )?;
    let (state, live_token): (String, Option<Uuid>) = sqlx::query_as(
        "SELECT a.state,c.lease_token FROM run_chunks c JOIN local_shard_admissions a USING (run_id, run_shard) WHERE c.run_id=$1",
    )
    .bind(run)
    .fetch_one(&fixture.primary)
    .await?;
    assert_eq!((state.as_str(), live_token), ("draining", Some(token)));

    sqlx::query("UPDATE run_chunks SET leased_until=clock_timestamp()-interval '1 second' WHERE run_id=$1 AND lease_token=$2")
        .bind(run)
        .bind(token)
        .execute(&fixture.primary)
        .await?;
    run_vigilo_ok(
        &fixture.config,
        PRIMARY_ALIAS,
        "single-default",
        ["coordinator", "once"],
    )?;
    let recovered: (String, i32, Option<Uuid>) =
        sqlx::query_as("SELECT status,recovery_count,lease_token FROM run_chunks WHERE run_id=$1")
            .bind(run)
            .fetch_one(&fixture.primary)
            .await?;
    assert_eq!(
        recovered,
        ("pending".to_owned(), 1, None),
        "recover the expired lease under draining admission"
    );
    let published: Vec<String> = sqlx::query_scalar(
        "SELECT dedupe_key FROM outbox_events WHERE aggregate_id=$1 AND event_type='run.chunk.ready' AND dedupe_key LIKE '%:ready:recovery:1' AND status='published'",
    )
    .bind(run)
    .fetch_all(&fixture.primary)
    .await?;
    assert_eq!(published.len(), 1);

    let moved = run_vigilo_json(&fixture.config, PRIMARY_ALIAS, "single-default", move_args)?;
    assert_eq!(moved["meta"]["moved"], true);
    assert_eq!(
        shard_placement_alias(&fixture.primary, run, 0).await?,
        SHARD_ALIAS
    );
    let status = drive_run_to_status(
        &fixture,
        "retry expired lease after shard move",
        PRIMARY_ALIAS,
        "single-default",
        run,
        "completed",
    )
    .await?;
    assert_eq!(status["data"]["gate_status"], "pass");
    assert_eq!(execution_count(&fixture.primary, run).await?, 0);
    assert_eq!(execution_count(&fixture.shard, run).await?, 1);
    let completed_by: Vec<String> = sqlx::query_scalar(
        "SELECT broker_message_id FROM execution_attempts WHERE run_id=$1 AND status='completed'",
    )
    .bind(run)
    .fetch_all(&fixture.shard)
    .await?;
    assert_eq!(completed_by, published);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_root_failure_releases_owner_and_next_run_recovers() -> anyhow::Result<()> {
    let Some(fixture) = E2eFixture::setup().await? else {
        return Ok(());
    };
    install_seed_status_failure(&fixture.primary).await?;
    let mut runs = Vec::new();
    for profile_id in ["recovery_delayed", "recovery_healthy"] {
        let output = create_run_output(
            &fixture.config,
            RunCreateInput {
                default_execution_alias: SHARD_ALIAS,
                shard_assignment_policy: "single-default",
                agent_url: &fixture.agent.url,
                profile_id,
                case_count: 1,
                case_mode: CaseMode::Passing,
                evaluator_ref: SENTIMENT_EVALUATOR_REF,
            },
        )?;
        anyhow::ensure!(
            output.status.success(),
            "creation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let payload: Value = serde_json::from_slice(&output.stdout)?;
        assert_eq!(payload["data"]["status"], "creating");
        assert_eq!(payload["meta"]["creation_placements_pending"], 1);
        runs.push(Uuid::parse_str(
            payload["data"]["run_id"].as_str().unwrap(),
        )?);
    }
    remove_seed_status_failure(&fixture.primary).await?;
    // Make candidate order deterministic without changing either immutable plan.
    sqlx::query("UPDATE runs SET updated_at=clock_timestamp()-interval '1 hour' WHERE id=$1")
        .bind(runs[0])
        .execute(&fixture.primary)
        .await?;

    let owner_path = std::env::var("VIGILO_STORAGE_CONFIG")?;
    let mut owner: toml::Value = toml::from_str(&fs::read_to_string(owner_path)?)?;
    let proxy = storage_proxy::Proxy::start(owner["endpoint"].as_str().unwrap().into()).await?;
    proxy.set(5);
    owner.as_table_mut().unwrap().insert(
        "read_endpoint_variable".into(),
        "VIGILO_E2E_RECOVERY_ENDPOINT".into(),
    );
    let directory = tempfile::tempdir()?;
    let owner_path = directory.path().join("storage.toml");
    fs::write(&owner_path, toml::to_string(&owner)?)?;
    let started = std::time::Instant::now();
    let output = Command::new(env!("CARGO_BIN_EXE_vigilo"))
        .env(PRIMARY_DATABASE_URL_ENV, &fixture.config.primary_url)
        .env(SHARD_DATABASE_URL_ENV, &fixture.config.shard_url)
        .env("MESSAGING_URL", &fixture.config.messaging_url)
        .env("VIGILO_MQ_NAMESPACE", &fixture.config.mq_namespace)
        .env("VIGILO_DEFAULT_SHARD_DATABASE_ALIAS", SHARD_ALIAS)
        .env("VIGILO_SHARD_ASSIGNMENT_POLICY", "single-default")
        .env("VIGILO_STORAGE_CONFIG", &owner_path)
        .env("VIGILO_E2E_RECOVERY_ENDPOINT", &proxy.endpoint)
        .args(["-q", "-f", "json", "coordinator", "once"])
        .output()?;
    let elapsed = started.elapsed();
    anyhow::ensure!(
        output.status.success(),
        "recovery failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    tokio::time::timeout(Duration::from_secs(1), proxy.entered.notified()).await?;
    assert!(proxy.count() > 1, "healthy plan must read storage too");
    let (status, owner, expiry): (String, Option<Uuid>, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as(
            "SELECT status::text,coordinator_id,coordinator_leased_until FROM runs WHERE id=$1",
        )
        .bind(runs[0])
        .fetch_one(&fixture.primary)
        .await?;
    assert_eq!(status, "creating");
    assert!(owner.is_none() && expiry.is_none());
    assert_eq!(dispatch_cursor_count(&fixture.primary, runs[0]).await?, 0);
    assert_eq!(dispatch_cursor_count(&fixture.primary, runs[1]).await?, 1);
    println!(
        "recovery_delay_observation={}",
        json!({
            "injected_root_delay_ms": 2000,
            "cycle_elapsed_ms": elapsed.as_millis(),
            "healthy_run_admitted": true,
            "failed_owner_released": true,
            "timing": "local diagnostic including process startup and coordinator work; serial recovery retains head-of-line delay"
        })
    );
    run_vigilo_ok(
        &fixture.config,
        SHARD_ALIAS,
        "single-default",
        ["coordinator", "once"],
    )?;
    assert_eq!(dispatch_cursor_count(&fixture.primary, runs[0]).await?, 1);
    assert_eq!(run_chunk_count(&fixture.shard, runs[0]).await?, 1);
    Ok(())
}
