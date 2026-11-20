//! Live recovery ownership through the public CLI.
//! Exercises commit boundaries across databases and RabbitMQ, including
//! recovery of expired worker leases on draining shards.
use super::*;

#[path = "../support/storage_proxy.rs"]
pub(super) mod storage_proxy;

#[tokio::test]
async fn outbox_replays_confirmed_message_after_publisher_crash() -> anyhow::Result<()> {
    use lapin::{
        Connection,
        ConnectionProperties,
        options::{
            BasicGetOptions,
            BasicNackOptions,
        },
    };

    let Some(fixture) = E2eFixture::setup().await? else {
        return Ok(());
    };
    let run = create_run(
        &fixture.config,
        RunCreateInput {
            default_execution_alias: SHARD_ALIAS,
            shard_assignment_policy: "single-default",
            agent_url: &fixture.agent.url,
            profile_id: "e2e_outbox_recovery",
            case_count: 1,
            case_mode: CaseMode::Passing,
            evaluator_ref: SENTIMENT_EVALUATOR_REF,
        },
    )?;
    // The publisher only reaches this update after RabbitMQ confirms its send.
    interrupt_cli_commit(
        &fixture,
        &fixture.shard,
        "outbox_events",
        (
            "UPDATE",
            "NEW.event_type='run.chunk.ready' AND NEW.status='published'",
        ),
        &["coordinator", "once"],
    )
    .await?;
    let pending = || {
        sqlx::query_as::<_, (Uuid, String, Value, Uuid, i32, bool)>(
            "SELECT e.id,e.dedupe_key,e.payload,q.claim_token,q.publish_attempt_count,
                    q.claimed_until > clock_timestamp()
             FROM outbox_events e JOIN outbox_delivery_queue q ON q.event_id=e.id
             WHERE e.aggregate_id=$1 AND e.event_type='run.chunk.ready'
               AND e.status='pending' AND e.published_at IS NULL",
        )
        .bind(run)
        .fetch_one(&fixture.shard)
    };
    let original = pending().await?;
    assert_eq!((original.4, original.5), (1, true));

    let broker = Connection::connect(
        &fixture.config.messaging_url,
        ConnectionProperties::default(),
    )
    .await?;
    let channel = broker.create_channel().await?;
    let queue = format!("vigilo.{}.worker", fixture.config.mq_namespace);
    let first = channel
        .basic_get(&queue, BasicGetOptions::default())
        .await?
        .ok_or_else(|| anyhow::anyhow!("confirmed message must survive the publisher crash"))?;
    assert_eq!(
        first.properties.message_id().as_ref().map(|id| id.as_str()),
        Some(original.1.as_str())
    );
    assert_eq!(serde_json::from_slice::<Value>(&first.data)?, original.2);
    assert_eq!(first.routing_key.as_str(), "run.chunk.ready");

    let once = || {
        run_vigilo_ok(
            &fixture.config,
            SHARD_ALIAS,
            "single-default",
            ["coordinator", "once"],
        )
    };
    once()?;
    assert_eq!(pending().await?, original);
    assert!(
        channel
            .basic_get(&queue, BasicGetOptions::default())
            .await?
            .is_none()
    );

    let expired = sqlx::query(
        "UPDATE outbox_delivery_queue
         SET available_at=clock_timestamp()-interval '1 second',
             claimed_until=clock_timestamp()-interval '1 second'
         WHERE event_id=$1 AND claim_token=$2",
    )
    .bind(original.0)
    .bind(original.3)
    .execute(&fixture.shard)
    .await?;
    assert_eq!(expired.rows_affected(), 1);
    once()?;
    let second = channel
        .basic_get(&queue, BasicGetOptions::default())
        .await?
        .ok_or_else(|| anyhow::anyhow!("replacement publisher must replay the event"))?;
    assert_eq!(second.routing_key, first.routing_key);
    assert_eq!(
        second.properties.message_id(),
        first.properties.message_id()
    );
    assert_eq!(second.data, first.data);
    let published: Vec<(Uuid, String, Value, bool)> = sqlx::query_as(
        "SELECT e.id,e.dedupe_key,e.payload,e.status='published' AND e.published_at IS NOT NULL
             AND NOT EXISTS (SELECT 1 FROM outbox_delivery_queue q WHERE q.event_id=e.id)
         FROM outbox_events e WHERE e.aggregate_id=$1 AND e.event_type='run.chunk.ready'",
    )
    .bind(run)
    .fetch_all(&fixture.shard)
    .await?;
    assert_eq!(
        published,
        vec![(original.0, original.1.clone(), original.2, true)]
    );
    assert!(
        channel
            .basic_get(&queue, BasicGetOptions::default())
            .await?
            .is_none()
    );

    // Give the normal worker both real copies, with their original properties.
    for delivery in [first, second] {
        delivery
            .nack(BasicNackOptions {
                multiple: false,
                requeue: true,
            })
            .await?;
    }
    for _ in 0..2 {
        run_vigilo_ok(
            &fixture.config,
            SHARD_ALIAS,
            "single-default",
            ["worker", "once"],
        )?;
    }
    assert!(
        channel
            .basic_get(&queue, BasicGetOptions::default())
            .await?
            .is_none()
    );
    channel.close(200, "replay verified").await?;
    broker.close(200, "replay verified").await?;

    let attempts: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT status::text,broker_message_id FROM execution_attempts WHERE run_id=$1",
    )
    .bind(run)
    .fetch_all(&fixture.shard)
    .await?;
    assert_eq!(attempts, vec![("completed".into(), Some(original.1))]);
    assert_eq!(execution_count(&fixture.shard, run).await?, 1);
    let receipts: i64 =
        sqlx::query_scalar("SELECT count(*) FROM run_chunk_settlements WHERE run_id=$1")
            .bind(run)
            .fetch_one(&fixture.shard)
            .await?;
    assert_eq!(receipts, 1);
    once()?;
    assert_eq!(
        outbox_event_count(&fixture.primary, run, "run.completed").await?,
        1
    );
    Ok(())
}

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

    interrupt_cli_commit(
        &fixture,
        &fixture.shard,
        "run_chunk_settlement_consumptions",
        ("UPDATE", "NEW.consumed AND NOT OLD.consumed"),
        &["coordinator", "once"],
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
    interrupt_cli_commit(
        &fixture,
        &fixture.primary,
        "runs",
        (
            "UPDATE",
            "NEW.status='completed' AND OLD.status='finalizing'",
        ),
        &["coordinator", "once"],
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

/// Abort a real CLI process at a test-owned transaction barrier.
async fn interrupt_cli_commit(
    fixture: &E2eFixture,
    db: &PgPool,
    table: &str,
    (event, condition): (&str, &str),
    args: &[&str],
) -> anyhow::Result<()> {
    let mut barrier = db.begin().await?;
    let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *barrier)
        .await?;
    // Tests isolate schemas but share a database. The live backend PID keeps
    // each advisory barrier separate even when multiple fault tests run.
    sqlx::query("SELECT pg_advisory_xact_lock(42188,$1)")
        .bind(blocker)
        .execute(&mut *barrier)
        .await?;
    sqlx::raw_sql(&format!(
        "CREATE FUNCTION hold_cli_commit() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN PERFORM pg_advisory_xact_lock(42188,{blocker}); RETURN NEW; END $$;
         CREATE TRIGGER hold_cli_commit AFTER {event} ON {table}
         FOR EACH ROW WHEN ({condition}) EXECUTE FUNCTION hold_cli_commit();",
    ))
    .execute(db)
    .await?;
    let mut command = tokio::process::Command::from(vigilo_command(
        &fixture.config,
        SHARD_ALIAS,
        "single-default",
    ));
    let mut child = command.kill_on_drop(true).args(args).spawn()?;
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
                "CLI exited before reaching the {table} barrier"
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
        "DROP TRIGGER hold_cli_commit ON {table}; DROP FUNCTION hold_cli_commit();",
    ))
    .execute(db)
    .await?;
    Ok(())
}

#[tokio::test]
async fn shard_move_replays_committed_target_page_and_fences_old_route() -> anyhow::Result<()> {
    let Some(fixture) = E2eFixture::setup().await? else {
        return Ok(());
    };
    for (profile, table, already_scored) in [
        ("move_scored_page", "execution_aggregates", true),
        ("move_dispatched_page", "run_chunks", false),
    ] {
        let run = create_run(
            &fixture.config,
            RunCreateInput {
                default_execution_alias: PRIMARY_ALIAS,
                shard_assignment_policy: "single-default",
                agent_url: &fixture.agent.url,
                profile_id: profile,
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
        let original: (String, Value) = sqlx::query_as(
            "SELECT dedupe_key,payload FROM outbox_events
             WHERE aggregate_id=$1 AND event_type='run.chunk.ready' AND status='published'",
        )
        .bind(run)
        .fetch_one(&fixture.primary)
        .await?;
        assert_eq!(original.1["database_alias"], PRIMARY_ALIAS);
        assert_eq!(original.1["write_epoch"], 1);
        if already_scored {
            run_vigilo_ok(
                &fixture.config,
                PRIMARY_ALIAS,
                "single-default",
                ["worker", "once"],
            )?;
        }
        let copied_rows = |db: &PgPool| {
            let db = db.clone();
            let query =
                format!("SELECT to_jsonb(r) FROM {table} r WHERE run_id=$1 AND run_shard=0");
            async move {
                sqlx::query_scalar::<_, Value>(&query)
                    .bind(run)
                    .fetch_all(&db)
                    .await
            }
        };
        let source = copied_rows(&fixture.primary).await?;
        assert_eq!(source.len(), 1);
        let run_arg = run.to_string();
        let move_args = ["shard", "move", &run_arg, "0", "--alias", SHARD_ALIAS];
        interrupt_cli_commit(
            &fixture,
            &fixture.primary,
            "shard_move_table_progress",
            ("INSERT", &format!("NEW.table_name='{table}'")),
            &move_args,
        )
        .await?;
        assert_eq!(copied_rows(&fixture.shard).await?, source);
        let (move_id, token, generation): (Uuid, Uuid, i64) = sqlx::query_as(
            "SELECT id,claim_token,claim_generation FROM shard_move_operations
             WHERE run_id=$1 AND status='active' AND claimed_until > now()",
        )
        .bind(run)
        .fetch_one(&fixture.primary)
        .await?;
        let checkpoint = || {
            sqlx::query_as::<_, (i64, i64)>(
                "SELECT completed_page_count,copied_row_count FROM shard_move_table_progress
                 WHERE move_id=$1 AND table_name=$2",
            )
            .bind(move_id)
            .bind(table)
            .fetch_optional(&fixture.primary)
        };
        assert_eq!(checkpoint().await?, None);
        let live = run_vigilo_output(&fixture.config, PRIMARY_ALIAS, "single-default", move_args)?;
        assert!(!live.status.success());
        assert!(
            [&live.stdout, &live.stderr]
                .iter()
                .any(|stream| String::from_utf8_lossy(stream).contains("currently claimed")),
            "{live:?}"
        );

        let expired = sqlx::query(
            "UPDATE shard_move_operations SET claimed_until=clock_timestamp()-interval '1 second'
             WHERE id=$1 AND claim_token=$2",
        )
        .bind(move_id)
        .bind(token)
        .execute(&fixture.primary)
        .await?;
        assert_eq!(expired.rows_affected(), 1);
        let moved = run_vigilo_json(&fixture.config, PRIMARY_ALIAS, "single-default", move_args)?;
        assert_eq!(moved["meta"]["moved"], true);
        assert_eq!(checkpoint().await?, Some((1, 1)));
        assert_eq!(copied_rows(&fixture.shard).await?, source);
        let route = || {
            sqlx::query_as::<_, (String, String, i64, i64)>(
                "SELECT database_alias,status,route_version,write_epoch FROM shard_placements
                 WHERE run_id=$1 AND run_shard=0",
            )
            .bind(run)
            .fetch_one(&fixture.primary)
        };
        let cutover = route().await?;
        assert_eq!(
            (cutover.0.as_str(), cutover.1.as_str(), cutover.3),
            (SHARD_ALIAS, "active", 2)
        );
        assert!(cutover.2 > 1);
        let retry = run_vigilo_json(&fixture.config, PRIMARY_ALIAS, "single-default", move_args)?;
        assert_eq!(retry["meta"]["moved"], false);
        assert_eq!(route().await?, cutover);
        let completed: Vec<(Uuid, String, i64)> = sqlx::query_as(
            "SELECT id,status,claim_generation FROM shard_move_operations WHERE run_id=$1",
        )
        .bind(run)
        .fetch_all(&fixture.primary)
        .await?;
        assert_eq!(
            completed,
            vec![(move_id, "completed".into(), generation + 1)]
        );
        for (db, alias, state) in [
            (&fixture.primary, PRIMARY_ALIAS, "closed"),
            (&fixture.shard, SHARD_ALIAS, "open"),
        ] {
            let local: (String, String, i64) = sqlx::query_as(
                "SELECT database_alias,state,write_epoch FROM local_shard_admissions
                 WHERE run_id=$1 AND run_shard=0",
            )
            .bind(run)
            .fetch_one(db)
            .await?;
            assert_eq!(local, (alias.into(), state.into(), 2));
        }

        if !already_scored {
            // The only queued readiness message still names the old route.
            // It must claim and write on the target, with no source execution.
            run_vigilo_ok(
                &fixture.config,
                PRIMARY_ALIAS,
                "single-default",
                ["worker", "once"],
            )?;
            assert_eq!(execution_count(&fixture.primary, run).await?, 0);
            let consumed: Vec<String> = sqlx::query_scalar(
                "SELECT broker_message_id FROM execution_attempts WHERE run_id=$1 AND status='completed'",
            )
            .bind(run)
            .fetch_all(&fixture.shard)
            .await?;
            assert_eq!(consumed, vec![original.0]);
        }
        assert_eq!(execution_count(&fixture.shard, run).await?, 1);
        let terminal = drive_run_to_status(
            &fixture,
            profile,
            PRIMARY_ALIAS,
            "single-default",
            run,
            "completed",
        )
        .await?;
        assert_eq!(terminal["data"]["gate_status"], "pass");
        assert_eq!(terminal["data"]["terminal_execution_count"], 1);
        assert_eq!(
            outbox_event_count(&fixture.primary, run, "run.completed").await?,
            1
        );
    }
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
