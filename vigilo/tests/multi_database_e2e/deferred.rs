//! Standalone polling acceptance against real shard state and a bounded loopback harness.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        Mutex,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
    time::Instant,
};

use vigilo::invocation::wire::{
    Cancellation,
    Submission,
};

use super::*;

#[derive(Default)]
struct Calls {
    submissions: BTreeMap<Uuid, Vec<String>>,
    polls: BTreeMap<Uuid, usize>,
    cancellations: Vec<Uuid>,
}

struct HarnessServer {
    base: String,
    calls: Arc<Mutex<Calls>>,
    shutdown: Arc<AtomicBool>,
    stall_poll: Arc<AtomicBool>,
    task: Option<thread::JoinHandle<()>>,
}

impl HarnessServer {
    fn start(
        lost_receipt: bool,
        pending_first: bool,
        cancel_unreachable: bool,
    ) -> anyhow::Result<Self> {
        let server = TcpListener::bind("127.0.0.1:0")?;
        let base = format!("http://{}", server.local_addr()?);
        server.set_nonblocking(true)?;
        let calls = Arc::new(Mutex::new(Calls::default()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let stall_poll = Arc::new(AtomicBool::new(false));
        let stall = stall_poll.clone();
        let observer = calls.clone();
        let stopped = shutdown.clone();
        let task = thread::spawn(move || {
            while !stopped.load(Ordering::Acquire) {
                match server.accept() {
                    Ok((stream, _)) => {
                        if let Err(error) = reply(
                            stream,
                            &observer,
                            lost_receipt,
                            pending_first,
                            cancel_unreachable,
                            &stall,
                        ) {
                            eprintln!("deferred HTTP fixture: {error}");
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2))
                    }
                    Err(_) => break,
                }
            }
        });
        Ok(Self {
            base,
            calls,
            shutdown,
            stall_poll,
            task: Some(task),
        })
    }
}

impl Drop for HarnessServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(task) = self.task.take() {
            let _ = task.join();
        }
    }
}

fn reply(
    mut stream: TcpStream,
    calls: &Mutex<Calls>,
    lost: bool,
    pending_first: bool,
    unreachable: bool,
    stall: &AtomicBool,
) -> anyhow::Result<()> {
    let request = read_agent_request(&mut stream)?;
    let (head, body) = request.split_once("\r\n\r\n").unwrap();
    let path = head
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap();
    let mut calls = calls.lock().unwrap();
    let (status, body) = if path == "/submit" {
        let submission = Submission::decode(body.as_bytes(), 16 * 1024, false)?;
        anyhow::ensure!(
            submission.case.input["user_message"].is_string() && submission.case.context.is_none()
        );
        let sends = calls
            .submissions
            .entry(submission.invocation_id)
            .or_default();
        sends.push(body.to_owned());
        if lost && sends.len() == 1 {
            return Ok(());
        }
        (
            "202 Accepted",
            json!({"schema_version":"1.0.0","invocation_id":submission.invocation_id,
            "status":"accepted","provider_reference":submission.invocation_id.to_string()})
            .to_string(),
        )
    } else if path == "/cancel" {
        let cancellation = Cancellation::decode(body.as_bytes(), 1024)?;
        calls.cancellations.push(cancellation.invocation_id);
        if unreachable {
            return Ok(());
        }
        ("202 Accepted", String::new())
    } else {
        let id: Uuid = path.strip_prefix("/status/").unwrap().parse()?;
        let submission =
            Submission::decode(calls.submissions[&id][0].as_bytes(), 16 * 1024, false)?;
        let delayed = submission.case.input["user_message"]
            .as_str()
            .unwrap()
            .starts_with("case 0:");
        let polls = calls.polls.entry(id).or_default();
        *polls += 1;
        if pending_first && delayed && *polls == 1 {
            (
                "200 OK",
                json!({"schema_version":"1.0.0","invocation_id":id,"status":"pending"}).to_string(),
            )
        } else {
            ("200 OK",json!({"schema_version":"1.0.0","invocation_id":id,"status":"completed",
                "executed_agent":submission.requested_agent,"evidence":{"content_type":"application/json",
                "encoding":"utf8","body":r#"{"text":"good reliable response"}"#}}).to_string())
        }
    };
    let hold = path.starts_with("/status/") && stall.load(Ordering::Acquire);
    drop(calls);
    if hold {
        let end = Instant::now() + Duration::from_secs(6);
        while stall.load(Ordering::Acquire) && Instant::now() < end {
            thread::sleep(Duration::from_millis(2));
        }
        return Ok(());
    }
    stream.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes())?;
    Ok(())
}

fn create(
    fixture: &E2eFixture,
    server: &HarnessServer,
    kind: &str,
    count: usize,
    dedup: bool,
) -> anyhow::Result<Uuid> {
    let dir = tempfile::tempdir()?;
    let file = dir.path().join("profile.json");
    let dataset = dir.path().join("dataset.yaml");
    let mut profile: Value = serde_yaml::from_str(&profile_yaml(
        "polling_harness",
        &format!("{}/submit", server.base),
        SENTIMENT_EVALUATOR_REF,
    ))?;
    profile["harness"]["kind"] = json!(kind);
    profile["harness"]["completion"] = json!({"kind":"polling","status_url":format!("{}/status/{{provider_reference}}",server.base),"deadline_secs":180});
    profile["harness"]["capabilities"] = json!({"idempotent_submission":dedup,"cancellation":{"url":format!("{}/cancel",server.base)}});
    fs::write(&file, serde_json::to_vec(&profile)?)?;
    fs::write(&dataset, dataset_yaml(count, CaseMode::Passing))?;
    Ok(run_vigilo_json(
        &fixture.config,
        SHARD_ALIAS,
        "single-default",
        [
            "run",
            "create",
            "--profile-file",
            path_arg(&file)?,
            "--dataset-file",
            path_arg(&dataset)?,
        ],
    )?["data"]["run_id"]
        .as_str()
        .unwrap()
        .parse()?)
}

fn cycle(fixture: &E2eFixture, what: &str) -> anyhow::Result<()> {
    run_vigilo_ok(
        &fixture.config,
        SHARD_ALIAS,
        "single-default",
        [what, "once"],
    )?;
    Ok(())
}

async fn due(db: &PgPool, run: Uuid) -> anyhow::Result<()> {
    sqlx::query("UPDATE execution_attempts SET invocation_next_at=now()-interval '1 second' WHERE run_id=$1 AND invocation_status='waiting'")
        .bind(run).execute(db).await?;
    Ok(())
}

#[tokio::test]
async fn polling_harnesses_release_mixed_chunks_and_score_the_same_attempts() -> anyhow::Result<()>
{
    let Some(fixture) = E2eFixture::setup().await? else {
        return Ok(());
    };
    for kind in ["direct_probe", "delegated_trial"] {
        let server = HarnessServer::start(false, true, false)?;
        let run = create(&fixture, &server, kind, 2, true)?;
        cycle(&fixture, "coordinator")?;
        cycle(&fixture, "worker")?;
        let parked: (String, bool) =
            sqlx::query_as("SELECT status,leased_until IS NULL FROM run_chunks WHERE run_id=$1")
                .bind(run)
                .fetch_one(&fixture.shard)
                .await?;
        assert_eq!(parked, ("pending".into(), true));
        let original:Vec<Uuid>=sqlx::query_scalar("SELECT id FROM execution_attempts WHERE run_id=$1 AND invocation_status='waiting' ORDER BY id")
            .bind(run).fetch_all(&fixture.shard).await?;
        assert_eq!(original.len(), 2);
        if kind == "direct_probe" {
            sqlx::query("UPDATE execution_attempts SET invocation_next_at=now()+interval '1 minute' WHERE run_id=$1")
                .bind(run).execute(&fixture.shard).await?;
            let unrelated = create_run(
                &fixture.config,
                RunCreateInput {
                    default_execution_alias: SHARD_ALIAS,
                    shard_assignment_policy: "single-default",
                    agent_url: &fixture.agent.url,
                    profile_id: "inline_while_trial_waits",
                    case_count: 1,
                    case_mode: CaseMode::Passing,
                    evaluator_ref: SENTIMENT_EVALUATOR_REF,
                },
            )?;
            let status = drive_run_to_status(
                &fixture,
                "other_chunk",
                SHARD_ALIAS,
                "single-default",
                unrelated,
                "completed",
            )
            .await?;
            assert_eq!(status["data"]["gate_status"], "pass");
            assert!(server.calls.lock().unwrap().polls.is_empty());
        }
        due(&fixture.shard, run).await?;
        cycle(&fixture, "coordinator")?;
        cycle(&fixture, "worker")?;
        let first: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM execution_attempts WHERE run_id=$1 AND status='completed'",
        )
        .bind(run)
        .fetch_one(&fixture.shard)
        .await?;
        assert_eq!(first, 1);
        due(&fixture.shard, run).await?;
        cycle(&fixture, "coordinator")?;
        cycle(&fixture, "worker")?;
        let result = drive_run_to_status(
            &fixture,
            kind,
            SHARD_ALIAS,
            "single-default",
            run,
            "completed",
        )
        .await?;
        assert_eq!(result["data"]["gate_status"], "pass");
        let persisted:Vec<(Uuid,Vec<u8>,f64)>=sqlx::query_as("SELECT a.id,a.response_evidence_bytes,g.aggregate_score FROM execution_attempts a JOIN execution_aggregates g ON g.attempt_id=a.id AND g.run_id=a.run_id AND g.run_shard=a.run_shard WHERE a.run_id=$1 ORDER BY a.id")
            .bind(run).fetch_all(&fixture.shard).await?;
        assert_eq!(persisted.len(), 2);
        for ((id, bytes, score), expected) in persisted.into_iter().zip(original) {
            assert_eq!(id, expected);
            assert_eq!(
                bytes,
                br#"{"text":"good reliable response"}good reliable response"#
            );
            assert_eq!(score, 1.0);
        }
        let calls = server.calls.lock().unwrap();
        assert!(calls.submissions.values().all(|sends| sends.len() == 1));
        assert_eq!(calls.polls.values().sum::<usize>(), 3);
    }
    Ok(())
}

#[tokio::test]
async fn polling_recovers_lost_receipts_and_settles_without_remote_cancellation()
-> anyhow::Result<()> {
    let Some(fixture) = E2eFixture::setup().await? else {
        return Ok(());
    };
    for dedup in [true, false] {
        let server = HarnessServer::start(true, false, true)?;
        let run = create(&fixture, &server, "delegated_trial", 1, dedup)?;
        cycle(&fixture, "coordinator")?;
        cycle(&fixture, "worker")?;
        sqlx::query("UPDATE execution_attempts SET invocation_claim_until=now()-interval '1 second',invocation_next_at=now()-interval '1 second' WHERE run_id=$1")
            .bind(run).execute(&fixture.shard).await?;
        cycle(&fixture, "coordinator")?;
        let state: String = sqlx::query_scalar(
            "SELECT invocation_status::text FROM execution_attempts WHERE run_id=$1",
        )
        .bind(run)
        .fetch_one(&fixture.shard)
        .await?;
        assert_eq!(
            state,
            if dedup {
                "waiting"
            } else {
                "submission_unknown"
            }
        );
        {
            let calls = server.calls.lock().unwrap();
            assert_eq!(calls.submissions.len(), 1);
            let sends = calls.submissions.values().next().unwrap();
            assert_eq!(sends.len(), if dedup { 2 } else { 1 });
            assert!(sends.iter().all(|send| send == &sends[0]));
        }
        if dedup {
            due(&fixture.shard, run).await?;
            server.stall_poll.store(true, Ordering::Release);
            let mut command = tokio::process::Command::from(vigilo_command(
                &fixture.config,
                SHARD_ALIAS,
                "single-default",
            ));
            let mut process = command
                .args(["coordinator", "once"])
                .kill_on_drop(true)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()?;
            tokio::time::timeout(Duration::from_secs(15), async {
                while server.calls.lock().unwrap().polls.is_empty() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;
            process.kill().await?;
            server.stall_poll.store(false, Ordering::Release);
            let claimed:bool=sqlx::query_scalar("SELECT invocation_claim IS NOT NULL FROM execution_attempts WHERE run_id=$1 AND invocation_status='waiting'")
                .bind(run).fetch_one(&fixture.shard).await?;
            assert!(claimed);
            sqlx::query("UPDATE execution_attempts SET invocation_claim_until=now()-interval '1 second' WHERE run_id=$1")
                .bind(run).execute(&fixture.shard).await?;
            due(&fixture.shard, run).await?;
            cycle(&fixture, "coordinator")?;
            cycle(&fixture, "worker")?;
            let result = drive_run_to_status(
                &fixture,
                "receipt_recovery",
                SHARD_ALIAS,
                "single-default",
                run,
                "completed",
            )
            .await?;
            assert_eq!(result["data"]["gate_status"], "pass");
        } else {
            sqlx::query("UPDATE execution_attempts SET invocation_deadline=now()-interval '1 second' WHERE run_id=$1")
                .bind(run).execute(&fixture.shard).await?;
            cycle(&fixture, "coordinator")?;
            cycle(&fixture, "worker")?;
            let result = drive_run_to_status(
                &fixture,
                "polling_deadline",
                SHARD_ALIAS,
                "single-default",
                run,
                "completed",
            )
            .await?;
            assert_eq!(result["data"]["gate_status"], "fail");
            let state:(String,String)=sqlx::query_as("SELECT status::text,invocation_status::text FROM execution_attempts WHERE run_id=$1")
                .bind(run).fetch_one(&fixture.shard).await?;
            assert_eq!(state, ("timed_out".into(), "timed_out".into()));
        }
    }
    // An accepted invocation that is cancelled locally also leaves durable remote work.
    let server = HarnessServer::start(false, false, true)?;
    let run = create(&fixture, &server, "direct_probe", 1, false)?;
    cycle(&fixture, "coordinator")?;
    cycle(&fixture, "worker")?;
    run_vigilo_ok(
        &fixture.config,
        SHARD_ALIAS,
        "single-default",
        ["run", "cancel", &run.to_string()],
    )?;
    assert_eq!(
        run_chunk_status_count(&fixture.shard, run, "cancelled").await?,
        1
    );
    for _ in 0..3 {
        sqlx::query("UPDATE execution_attempts SET invocation_cancel_at=now()-interval '1 second' WHERE run_id=$1 AND invocation_cancel_at IS NOT NULL")
            .bind(run).execute(&fixture.shard).await?;
        cycle(&fixture, "coordinator")?;
    }
    let state:(String,String,i32)=sqlx::query_as("SELECT invocation_status::text,invocation_cancel_result,invocation_cancel_attempts FROM execution_attempts WHERE run_id=$1")
        .bind(run).fetch_one(&fixture.shard).await?;
    assert_eq!(state, ("cancelled".into(), "unconfirmed".into(), 3));
    assert_eq!(server.calls.lock().unwrap().cancellations.len(), 3);
    Ok(())
}

async fn mutations(db: &PgPool) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT COALESCE(sum(n_tup_ins+n_tup_upd+n_tup_del),0)::bigint FROM pg_stat_user_tables",
    )
    .fetch_one(db)
    .await?)
}

fn observed_cycle(fixture: &E2eFixture) -> anyhow::Result<(postgres_probe::Observation, u64)> {
    let control = postgres_probe::Probe::start(&fixture.config.primary_url)?;
    let execution = postgres_probe::Probe::start(&fixture.config.shard_url)?;
    let config = IntegrationConfig {
        primary_url: control.url.clone(),
        shard_url: execution.url.clone(),
        ..fixture.config.clone()
    };
    run_vigilo_ok(
        &config,
        SHARD_ALIAS,
        "single-default",
        ["coordinator", "once"],
    )?;
    Ok((control.finish()?, execution.finish()?.query_sync_exchanges))
}

#[tokio::test]
async fn deferred_backlog_limits_polling_and_expires_work_independently() -> anyhow::Result<()> {
    let Some(fixture) = E2eFixture::setup().await? else {
        return Ok(());
    };
    let server = HarnessServer::start(false, false, false)?;
    let run = create(&fixture, &server, "direct_probe", 34, true)?;
    cycle(&fixture, "coordinator")?;
    cycle(&fixture, "worker")?;
    sqlx::query("UPDATE execution_attempts SET invocation_next_at=now()+interval '1 minute' WHERE run_id=$1")
        .bind(run).execute(&fixture.shard).await?;
    // Server totals are database-wide. Only an isolated benchmark process may
    // compare them; the normal distributed suite isolates tests by schema.
    let tracks_sql = std::env::var("VIGILO_DEFERRED_QUERY_CAPTURE").as_deref() == Ok("1");
    let sql_before = if tracks_sql {
        Some(response_queries::counters(&fixture.primary).await?)
    } else {
        None
    };
    let (idle_control, _) = observed_cycle(&fixture)?;
    assert!(server.calls.lock().unwrap().polls.is_empty());
    let expired: Uuid =
        sqlx::query_scalar("SELECT min(id::text)::uuid FROM execution_attempts WHERE run_id=$1")
            .bind(run)
            .fetch_one(&fixture.shard)
            .await?;
    sqlx::query(
        "UPDATE execution_attempts SET invocation_deadline=now()-interval '1 second' WHERE id=$1",
    )
    .bind(expired)
    .execute(&fixture.shard)
    .await?;
    let due_at: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT now()-interval '1 second'")
            .fetch_one(&fixture.shard)
            .await?;
    sqlx::query("UPDATE execution_attempts SET invocation_next_at=$2 WHERE run_id=$1")
        .bind(run)
        .bind(due_at)
        .execute(&fixture.shard)
        .await?;
    let before_writes = mutations(&fixture.shard).await?;
    let sql_idle_end = if tracks_sql {
        Some(response_queries::counters(&fixture.primary).await?)
    } else {
        None
    };
    let started = Instant::now();
    let (control, execution) = observed_cycle(&fixture)?;
    // The idle observation is an upper bound on control traffic while polling.
    assert!(
        control.query_sync_exchanges <= idle_control.query_sync_exchanges,
        "polling added control database protocol exchanges: idle={idle_control:?}, polling={control:?}"
    );
    if let (Some(before), Some(idle_end)) = (sql_before, sql_idle_end) {
        let after = response_queries::counters(&fixture.primary).await?;
        let idle = response_queries::delta(&before, &idle_end);
        let busy = response_queries::delta(&idle_end, &after);
        assert_eq!(idle, busy, "polling changed control stream SQL categories");
        println!("deferred backlog control SQL statement categories: {busy:?}");
    }
    let writes = mutations(&fixture.shard).await? - before_writes;
    let max_lag_ms:i64=sqlx::query_scalar("SELECT max(EXTRACT(EPOCH FROM updated_at-$2)*1000)::bigint FROM execution_attempts WHERE run_id=$1 AND invocation_status='completed'")
        .bind(run).bind(due_at).fetch_one(&fixture.shard).await?;
    let calls = server.calls.lock().unwrap().polls.values().sum::<usize>();
    assert_eq!(calls, 32);
    let states:Vec<(String,i64)>=sqlx::query_as("SELECT invocation_status::text,count(*)::bigint FROM execution_attempts WHERE run_id=$1 GROUP BY invocation_status ORDER BY 1")
        .bind(run).fetch_all(&fixture.shard).await?;
    assert_eq!(
        states,
        vec![
            ("completed".into(), 32),
            ("timed_out".into(), 1),
            ("waiting".into(), 1)
        ]
    );
    println!(
        "deferred backlog: HTTP polls={calls} accepted=32 expired=1 waiting=1 control_exchanges={} execution_exchanges={execution} execution_row_mutations={writes} max_completion_lag_ms={max_lag_ms} cycle_ms={}",
        control.query_sync_exchanges,
        started.elapsed().as_millis()
    );
    cycle(&fixture, "worker")?;
    let settled: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM executions WHERE run_id=$1 AND status IN ('completed','timed_out')",
    )
    .bind(run)
    .fetch_one(&fixture.shard)
    .await?;
    assert_eq!(settled, 33);
    Ok(())
}
