//! End-to-end multi-database runtime integration coverage.
//!
//! This harness is opt-in because it requires two PostgreSQL databases,
//! RabbitMQ, and a built evaluator WASM artifact. It drives the public `vigilo`
//! binary through recoverable creation, dispatch, worker processing,
//! finalization, results/export, cancellation, and validation failure paths.

mod support;

use std::{
    fs,
    io::{
        Read,
        Write,
    },
    net::{
        TcpListener,
        TcpStream,
    },
    path::{
        Path,
        PathBuf,
    },
    process::{
        Command,
        Output,
    },
    thread,
    time::Duration,
};

use serde_json::{
    Value,
    json,
};
use sqlx::{
    PgPool,
    postgres::PgPoolOptions,
};
use uuid::Uuid;

const PRIMARY_DATABASE_URL_ENV: &str = "DATABASE_URL";
const SHARD_DATABASE_URL_ENV: &str = "VIGILO_TEST_SHARD_001_DATABASE_URL";
const E2E_ENABLED_ENV: &str = "VIGILO_E2E_MULTI_DATABASE";
const PRIMARY_ALIAS: &str = "primary";
const SHARD_ALIAS: &str = "shard_001";
const SHARD_DATABASE_URL_ENV_VALUE: &str = "VIGILO_TEST_SHARD_001_DATABASE_URL";
const SENTIMENT_EVALUATOR_REF: &str = "vigilo/sentiment-basic-en:0.1.0";
const SENTIMENT_WASM_PATH_ENV: &str = "VIGILO_E2E_SENTIMENT_WASM_PATH";
const SENTIMENT_WASM_TARGET_PATH: &str = "wasm32-wasip2/release/sentiment_basic_en.wasm";
const E2E_MAX_RUNTIME_CYCLES: usize = 8;
const E2E_WORKER_PASSES_PER_CYCLE: usize = 4;

#[tokio::test]
async fn run_creation_recovers_after_cross_database_seed_status_failure() -> anyhow::Result<()> {
    let Some(fixture) = E2eFixture::setup().await? else {
        return Ok(());
    };

    install_seed_status_failure(&fixture.primary).await?;
    let recoverable_output = create_run_output(
        &fixture.config,
        RunCreateInput {
            default_execution_alias: SHARD_ALIAS,
            shard_assignment_policy: "single-default",
            agent_url: &fixture.agent.url,
            profile_id: "e2e_creation_recovery",
            case_count: 1,
            case_mode: CaseMode::Passing,
            evaluator_ref: SENTIMENT_EVALUATOR_REF,
        },
    )?;
    remove_seed_status_failure(&fixture.primary).await?;
    if !recoverable_output.status.success() {
        anyhow::bail!(
            "recoverable run create failed with status {}\nstdout:\n{}\nstderr:\n{}",
            recoverable_output.status,
            String::from_utf8_lossy(&recoverable_output.stdout),
            String::from_utf8_lossy(&recoverable_output.stderr)
        );
    }
    let recoverable_payload: Value = serde_json::from_slice(&recoverable_output.stdout)?;
    let recoverable_run_id = Uuid::parse_str(
        recoverable_payload["data"]["run_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("recoverable create response omitted data.run_id"))?,
    )?;
    assert_eq!(recoverable_payload["data"]["status"], "creating");
    assert_eq!(
        recoverable_payload["meta"]["creation_placements_pending"],
        1
    );
    assert_eq!(
        run_chunk_count(&fixture.shard, recoverable_run_id).await?,
        1
    );
    assert_eq!(
        dispatch_cursor_count(&fixture.primary, recoverable_run_id).await?,
        0
    );

    let creating_status = run_vigilo_json(
        &fixture.config,
        SHARD_ALIAS,
        "single-default",
        ["run", "status", &recoverable_run_id.to_string()],
    )?;
    assert_eq!(creating_status["data"]["status"], "creating");
    assert_eq!(
        creating_status["data"]["creation_progress"]["pending_placement_count"],
        1
    );

    expire_run_creation_lease(&fixture.primary, recoverable_run_id).await?;
    run_vigilo_ok(
        &fixture.config,
        SHARD_ALIAS,
        "single-default",
        ["coordinator", "once"],
    )?;
    assert_eq!(
        run_chunk_count(&fixture.shard, recoverable_run_id).await?,
        1
    );
    assert_eq!(
        dispatch_cursor_count(&fixture.primary, recoverable_run_id).await?,
        1
    );
    assert_eq!(
        run_creation_placement_state(&fixture.primary, recoverable_run_id, SHARD_ALIAS).await?,
        ("seeded".to_string(), 2)
    );
    assert_eq!(
        run_creation_chunk_plan_count(&fixture.primary, recoverable_run_id).await?,
        0
    );

    Ok(())
}

#[tokio::test]
async fn cross_shard_run_completes_and_exports_results() -> anyhow::Result<()> {
    let Some(fixture) = E2eFixture::setup().await? else {
        return Ok(());
    };

    let completed_run_id = create_run(
        &fixture.config,
        RunCreateInput {
            default_execution_alias: PRIMARY_ALIAS,
            shard_assignment_policy: "spread-active",
            agent_url: &fixture.agent.url,
            profile_id: "e2e_completed",
            case_count: 101,
            case_mode: CaseMode::Passing,
            evaluator_ref: SENTIMENT_EVALUATOR_REF,
        },
    )?;

    assert_eq!(
        shard_placement_alias(&fixture.primary, completed_run_id, 0).await?,
        PRIMARY_ALIAS
    );
    assert_eq!(
        shard_placement_alias(&fixture.primary, completed_run_id, 1).await?,
        SHARD_ALIAS
    );

    let status = drive_run_to_status(
        &fixture,
        "cross-shard completion",
        PRIMARY_ALIAS,
        "spread-active",
        completed_run_id,
        "completed",
    )
    .await?;
    assert_eq!(status["data"]["status"].as_str(), Some("completed"));
    assert_eq!(status["data"]["gate_status"].as_str(), Some("pass"));
    assert_eq!(status["data"]["expected_execution_count"], 101);
    assert_eq!(status["data"]["terminal_execution_count"], 101);

    let results = run_vigilo_json(
        &fixture.config,
        PRIMARY_ALIAS,
        "spread-active",
        ["run", "results", &completed_run_id.to_string()],
    )?;
    assert_eq!(results["data"]["results"]["execution_count"], 101);
    assert_eq!(
        results["data"]["results"]["status_counts"]["passed"],
        json!(101)
    );

    let export = run_vigilo_ok(
        &fixture.config,
        PRIMARY_ALIAS,
        "spread-active",
        [
            "run",
            "export",
            &completed_run_id.to_string(),
            "--format",
            "jsonl",
            "--batch-size",
            "25",
        ],
    )?;
    let export_lines = parse_jsonl(&export.stdout)?;
    assert_eq!(
        export_lines
            .iter()
            .filter(|line| line["type"] == "execution")
            .count(),
        101
    );
    assert!(export_lines.iter().any(|line| {
        line["type"] == "execution_route" && line["database_alias"] == PRIMARY_ALIAS
    }));
    assert!(export_lines.iter().any(|line| {
        line["type"] == "execution_route" && line["database_alias"] == SHARD_ALIAS
    }));

    assert_eq!(
        execution_count(&fixture.primary, completed_run_id).await?,
        100
    );
    assert_eq!(execution_count(&fixture.shard, completed_run_id).await?, 1);
    assert_eq!(
        outbox_event_count(&fixture.primary, completed_run_id, "run.completed").await?,
        1
    );
    assert_eq!(
        outbox_event_count(&fixture.shard, completed_run_id, "run.completed").await?,
        0
    );
    assert_eq!(
        outbox_event_count(&fixture.shard, completed_run_id, "run.chunk.ready").await?,
        1
    );
    assert_eq!(
        dispatch_cursor_status_count(&fixture.primary, completed_run_id, "drained").await?,
        2
    );

    Ok(())
}

#[tokio::test]
async fn unavailable_shard_does_not_block_healthy_placement_work() -> anyhow::Result<()> {
    let Some(fixture) = E2eFixture::setup().await? else {
        return Ok(());
    };

    let isolation_config = IntegrationConfig {
        mq_namespace: format!("{}-placement-isolation", fixture.config.mq_namespace),
        ..fixture.config.clone()
    };
    let unavailable_run_id = create_run(
        &isolation_config,
        RunCreateInput {
            default_execution_alias: SHARD_ALIAS,
            shard_assignment_policy: "single-default",
            agent_url: &fixture.agent.url,
            profile_id: "e2e_unavailable_shard",
            case_count: 1,
            case_mode: CaseMode::Passing,
            evaluator_ref: SENTIMENT_EVALUATOR_REF,
        },
    )?;
    let healthy_run_id = create_run(
        &isolation_config,
        RunCreateInput {
            default_execution_alias: PRIMARY_ALIAS,
            shard_assignment_policy: "single-default",
            agent_url: &fixture.agent.url,
            profile_id: "e2e_healthy_placement",
            case_count: 1,
            case_mode: CaseMode::Passing,
            evaluator_ref: SENTIMENT_EVALUATOR_REF,
        },
    )?;
    age_dispatch_cursor(&fixture.primary, unavailable_run_id).await?;
    let unavailable_config = IntegrationConfig {
        shard_url: unavailable_postgres_url()?,
        ..isolation_config.clone()
    };

    run_vigilo_ok(
        &unavailable_config,
        PRIMARY_ALIAS,
        "single-default",
        ["coordinator", "--max-dispatch-per-cycle", "1", "once"],
    )?;
    assert_eq!(
        dispatch_cursor_status_count(&fixture.primary, unavailable_run_id, "open").await?,
        1,
        "a failed execution write must leave its control cursor retryable"
    );
    assert_eq!(
        dispatch_cursor_status_count(&fixture.primary, healthy_run_id, "drained").await?,
        1,
        "dispatch must continue on healthy placements after excluding the failed alias"
    );
    assert_eq!(
        published_outbox_event_count(&fixture.primary, healthy_run_id, "run.chunk.ready").await?,
        1,
        "the healthy placement outbox must still publish"
    );

    expire_chunk_lease(&fixture.primary, healthy_run_id).await?;
    run_vigilo_ok(
        &unavailable_config,
        PRIMARY_ALIAS,
        "single-default",
        ["coordinator", "--max-dispatch-per-cycle", "1", "once"],
    )?;
    assert_eq!(
        run_chunk_recovery_state(&fixture.primary, healthy_run_id).await?,
        ("pending".to_string(), 1),
        "lease recovery must continue on the healthy placement"
    );
    assert_eq!(
        published_outbox_event_count(&fixture.primary, healthy_run_id, "run.chunk.ready").await?,
        2,
        "the recovery event must publish despite the unavailable shard"
    );

    Ok(())
}

#[tokio::test]
async fn older_nonterminal_run_does_not_block_newer_terminal_run() -> anyhow::Result<()> {
    let Some(fixture) = E2eFixture::setup().await? else {
        return Ok(());
    };

    let blocked_config = IntegrationConfig {
        mq_namespace: format!("{}-blocked-run", fixture.config.mq_namespace),
        ..fixture.config.clone()
    };
    let blocked_run_id = create_run(
        &blocked_config,
        RunCreateInput {
            default_execution_alias: PRIMARY_ALIAS,
            shard_assignment_policy: "single-default",
            agent_url: &fixture.agent.url,
            profile_id: "e2e_nonterminal_blocker",
            case_count: 1,
            case_mode: CaseMode::Passing,
            evaluator_ref: SENTIMENT_EVALUATOR_REF,
        },
    )?;
    run_vigilo_ok(
        &blocked_config,
        PRIMARY_ALIAS,
        "single-default",
        ["coordinator", "once"],
    )?;
    assert_eq!(
        dispatch_cursor_status_count(&fixture.primary, blocked_run_id, "drained").await?,
        1
    );

    let terminal_run_id = create_run(
        &fixture.config,
        RunCreateInput {
            default_execution_alias: PRIMARY_ALIAS,
            shard_assignment_policy: "single-default",
            agent_url: &fixture.agent.url,
            profile_id: "e2e_newer_terminal",
            case_count: 1,
            case_mode: CaseMode::Passing,
            evaluator_ref: SENTIMENT_EVALUATOR_REF,
        },
    )?;
    let terminal_status = drive_run_to_status(
        &fixture,
        "finalization fairness",
        PRIMARY_ALIAS,
        "single-default",
        terminal_run_id,
        "completed",
    )
    .await?;
    assert_eq!(terminal_status["data"]["gate_status"], "pass");

    let blocked_status = run_vigilo_json(
        &fixture.config,
        PRIMARY_ALIAS,
        "single-default",
        ["run", "status", &blocked_run_id.to_string()],
    )?;
    assert_eq!(blocked_status["data"]["status"], "running");

    Ok(())
}

#[tokio::test]
async fn agent_failure_completes_run_with_failed_gate() -> anyhow::Result<()> {
    let Some(fixture) = E2eFixture::setup().await? else {
        return Ok(());
    };

    let failing_run_id = create_run(
        &fixture.config,
        RunCreateInput {
            default_execution_alias: PRIMARY_ALIAS,
            shard_assignment_policy: "single-default",
            agent_url: &fixture.agent.url,
            profile_id: "e2e_agent_failure",
            case_count: 1,
            case_mode: CaseMode::AgentFailure,
            evaluator_ref: SENTIMENT_EVALUATOR_REF,
        },
    )?;
    let failed_status = drive_run_to_status(
        &fixture,
        "agent failure finalization",
        PRIMARY_ALIAS,
        "single-default",
        failing_run_id,
        "completed",
    )
    .await?;
    assert_eq!(failed_status["data"]["status"].as_str(), Some("completed"));
    assert_eq!(failed_status["data"]["gate_status"].as_str(), Some("fail"));
    assert_eq!(failed_status["data"]["errored_execution_count"], 1);
    assert_eq!(
        execution_status_count(&fixture.primary, failing_run_id, "failed").await?,
        1
    );

    Ok(())
}

#[tokio::test]
async fn run_cancellation_updates_routed_storage() -> anyhow::Result<()> {
    let Some(fixture) = E2eFixture::setup().await? else {
        return Ok(());
    };

    let cancel_run_id = create_run(
        &fixture.config,
        RunCreateInput {
            default_execution_alias: SHARD_ALIAS,
            shard_assignment_policy: "single-default",
            agent_url: &fixture.agent.url,
            profile_id: "e2e_cancelled",
            case_count: 1,
            case_mode: CaseMode::Passing,
            evaluator_ref: SENTIMENT_EVALUATOR_REF,
        },
    )?;
    let cancel_payload = run_vigilo_json(
        &fixture.config,
        SHARD_ALIAS,
        "single-default",
        ["run", "cancel", &cancel_run_id.to_string()],
    )?;
    assert_eq!(cancel_payload["meta"]["cancelled"], true);
    assert_eq!(
        run_chunk_status_count(&fixture.shard, cancel_run_id, "cancelled").await?,
        1
    );
    assert_eq!(
        outbox_event_count(&fixture.primary, cancel_run_id, "run.cancelled").await?,
        1
    );
    assert_eq!(
        outbox_event_count(&fixture.shard, cancel_run_id, "run.cancelled").await?,
        0
    );

    Ok(())
}

#[tokio::test]
async fn exhausted_shard_preserves_healthy_results_and_finalizes_partial_report()
-> anyhow::Result<()> {
    let Some(fixture) = E2eFixture::setup().await? else {
        return Ok(());
    };
    let run = create_run(
        &fixture.config,
        RunCreateInput {
            default_execution_alias: PRIMARY_ALIAS,
            shard_assignment_policy: "spread-active",
            agent_url: &fixture.agent.url,
            profile_id: "e2e_partial_settlement",
            case_count: 101,
            case_mode: CaseMode::Passing,
            evaluator_ref: SENTIMENT_EVALUATOR_REF,
        },
    )?;
    run_vigilo_ok(
        &fixture.config,
        PRIMARY_ALIAS,
        "spread-active",
        ["coordinator", "once"],
    )?;
    // Exhaust the remote one-case chunk before it creates an execution. The
    // primary chunk still runs through the real broker, agent, Wasm, and worker.
    sqlx::query("UPDATE run_chunks SET status='leased',lease_token=gen_random_uuid(),leased_until=now()-interval '1 second',recovery_count=3 WHERE run_id=$1")
        .bind(run).execute(&fixture.shard).await?;
    let status = drive_run_to_status(
        &fixture,
        "settled partial report",
        PRIMARY_ALIAS,
        "spread-active",
        run,
        "completed",
    )
    .await?;
    assert_eq!(status["data"]["gate_status"], "fail");
    assert_eq!(
        status["data"]["live_progress"]["missing_execution_count"],
        1
    );
    assert_eq!(status["data"]["live_progress"]["sealed_shard_count"], 2);
    assert_eq!(execution_count(&fixture.primary, run).await?, 100);
    assert_eq!(execution_count(&fixture.shard, run).await?, 0);
    assert_eq!(
        outbox_event_count(&fixture.primary, run, "run.completed").await?,
        1
    );
    let results = run_vigilo_json(
        &fixture.config,
        PRIMARY_ALIAS,
        "spread-active",
        ["run", "results", &run.to_string()],
    )?;
    assert_eq!(results["data"]["results"]["aggregate_count"], 100);
    assert_eq!(results["data"]["results"]["missing_execution_count"], 1);
    assert_eq!(results["data"]["results"]["score"]["average"], 1.0);
    let scorecard: Value =
        sqlx::query_scalar("SELECT scorecard FROM run_scorecards WHERE run_id=$1")
            .bind(run)
            .fetch_one(&fixture.primary)
            .await?;
    assert_eq!(scorecard["gates"][0]["expected_count"], 101);
    assert_eq!(scorecard["gates"][0]["scored_count"], 100);
    let export = run_vigilo_ok(
        &fixture.config,
        PRIMARY_ALIAS,
        "spread-active",
        [
            "run",
            "export",
            &run.to_string(),
            "--format",
            "jsonl",
            "--batch-size",
            "17",
        ],
    )?;
    let lines = String::from_utf8(export.stdout)?;
    let records = lines
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let aggregates = records
        .iter()
        .filter(|r| r["type"] == "execution_aggregate")
        .collect::<Vec<_>>();
    assert_eq!(aggregates.len(), 100);
    assert!(aggregates.iter().all(|r| r["authoritative"] == true));
    assert!(
        records
            .iter()
            .filter(|r| r["type"] == "execution_attempt")
            .all(|r| r["evidence_state"] == "authoritative")
    );
    let source_receipt: Value = sqlx::query_scalar(
        "SELECT to_jsonb(s) FROM run_chunk_settlements s WHERE run_id=$1 AND run_shard=1",
    )
    .bind(run)
    .fetch_one(&fixture.shard)
    .await?;
    run_vigilo_ok(
        &fixture.config,
        PRIMARY_ALIAS,
        "spread-active",
        [
            "shard",
            "move",
            &run.to_string(),
            "1",
            "--alias",
            PRIMARY_ALIAS,
        ],
    )?;
    let target_receipt: Value = sqlx::query_scalar(
        "SELECT to_jsonb(s) FROM run_chunk_settlements s WHERE run_id=$1 AND run_shard=1",
    )
    .bind(run)
    .fetch_one(&fixture.primary)
    .await?;
    assert_eq!(target_receipt, source_receipt);
    let (consumed,sealed):(bool,bool)=sqlx::query_as("SELECT c.consumed,s.sealed FROM run_chunk_settlement_consumptions c JOIN run_shard_summaries s USING (run_id,run_shard) WHERE c.run_id=$1 AND c.run_shard=1")
        .bind(run).fetch_one(&fixture.primary).await?;
    assert!(consumed && sealed);
    run_vigilo_ok(
        &fixture.config,
        PRIMARY_ALIAS,
        "spread-active",
        ["coordinator", "once"],
    )?;
    assert_eq!(
        outbox_event_count(&fixture.primary, run, "run.completed").await?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn unpublished_evaluator_is_rejected() -> anyhow::Result<()> {
    let Some(fixture) = E2eFixture::setup().await? else {
        return Ok(());
    };

    let invalid = create_run_output(
        &fixture.config,
        RunCreateInput {
            default_execution_alias: PRIMARY_ALIAS,
            shard_assignment_policy: "single-default",
            agent_url: &fixture.agent.url,
            profile_id: "e2e_invalid_evaluator",
            case_count: 1,
            case_mode: CaseMode::Passing,
            evaluator_ref: "vigilo/missing:0.0.0",
        },
    )?;
    assert!(
        !invalid.status.success(),
        "run create with an unpublished evaluator should fail"
    );
    assert!(
        String::from_utf8_lossy(&invalid.stderr).contains("unpublished evaluator"),
        "unexpected stderr: {}",
        String::from_utf8_lossy(&invalid.stderr)
    );

    Ok(())
}

struct E2eFixture {
    config: IntegrationConfig,
    agent: MockAgent,
    primary: PgPool,
    shard: PgPool,
}

impl E2eFixture {
    async fn setup() -> anyhow::Result<Option<Self>> {
        let Some(config) = IntegrationConfig::from_env()? else {
            return Ok(None);
        };
        let config = config.isolated().await?;
        let agent = MockAgent::start()?;
        let primary = connect(&config.primary_url).await?;
        let shard = connect(&config.shard_url).await?;
        migrate(&primary).await?;
        migrate(&shard).await?;
        seed_sentiment_evaluator(&primary).await?;
        configure_shard_placement(&primary).await?;

        Ok(Some(Self {
            config,
            agent,
            primary,
            shard,
        }))
    }
}

#[derive(Clone)]
struct IntegrationConfig {
    primary_url: String,
    shard_url: String,
    messaging_url: String,
    mq_namespace: String,
}

impl IntegrationConfig {
    fn from_env() -> anyhow::Result<Option<Self>> {
        if std::env::var(E2E_ENABLED_ENV).ok().as_deref() != Some("1") {
            return Ok(None);
        }

        Ok(Some(Self {
            primary_url: std::env::var(PRIMARY_DATABASE_URL_ENV)?,
            shard_url: std::env::var(SHARD_DATABASE_URL_ENV)?,
            messaging_url: std::env::var("MESSAGING_URL")?,
            mq_namespace: format!("e2e-{}", Uuid::now_v7()),
        }))
    }

    async fn isolated(mut self) -> anyhow::Result<Self> {
        (self.primary_url, self.shard_url) =
            support::isolated_postgres_urls(&self.primary_url, &self.shard_url).await?;
        Ok(self)
    }
}

struct MockAgent {
    url: String,
}

impl MockAgent {
    fn start() -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let _ = handle_agent_request(stream);
            }
        });

        Ok(Self {
            url: format!("http://{addr}/v1/agent/invoke"),
        })
    }
}

fn unavailable_postgres_url() -> anyhow::Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    drop(listener);
    Ok(format!("postgres://vigilo@{address}/vigilo"))
}

fn handle_agent_request(mut stream: TcpStream) -> anyhow::Result<()> {
    let mut buffer = [0u8; 16 * 1024];
    let read = stream.read(&mut buffer)?;
    let request = String::from_utf8_lossy(&buffer[..read]);

    let (status, body) = if request.contains("force-agent-error") {
        (
            "HTTP/1.1 500 Internal Server Error",
            json!({"error": "forced integration agent failure"}).to_string(),
        )
    } else {
        (
            "HTTP/1.1 200 OK",
            json!({"text": "good reliable response"}).to_string(),
        )
    };

    let response = format!(
        "{status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes())?;
    Ok(())
}

async fn connect(url: &str) -> anyhow::Result<PgPool> {
    Ok(PgPoolOptions::new().max_connections(5).connect(url).await?)
}

async fn migrate(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::migrate!("../migrations").run(pool).await?;
    Ok(())
}

async fn seed_sentiment_evaluator(primary: &PgPool) -> anyhow::Result<()> {
    let wasm_path = sentiment_wasm_path()?;
    let wasm_bytes = fs::read(&wasm_path).map_err(|err| {
        anyhow::anyhow!(
            "failed to read {}; build it first with `cargo build -p sentiment-basic-en --target wasm32-wasip2 --release --locked`: {}",
            wasm_path.display(),
            err
        )
    })?;
    let wasm_size_bytes = wasm_bytes.len() as i64;
    let content_hash = blake3::hash(&wasm_bytes).to_hex().to_string();
    let abi_contract_hash =
        blake3::hash(include_bytes!("../../wit/evaluator/v1.0.0/evaluator.wit"))
            .to_hex()
            .to_string();

    sqlx::query(
        r#"
        INSERT INTO evaluators (
            namespace,
            name,
            version,
            content_hash,
            wasm_bytes,
            wasm_size_bytes,
            interface_name,
            interface_version,
            wit_world,
            abi_contract_hash,
            abi_adapter,
            runtime,
            runtime_version,
            runtime_fingerprint,
            description,
            tags,
            metadata,
            state
        )
        VALUES (
            'vigilo',
            'sentiment-basic-en',
            '0.1.0',
            $1,
            $2,
            $3,
            'vigilo:evaluator/evaluator',
            '1.0.0',
            'evaluator-world',
            $4,
            'vigilo-evaluator-v1@1',
            'wasmtime',
            'integration',
            'integration',
            'Multi-database integration evaluator',
            '["integration", "sentiment"]'::jsonb,
            '{}'::jsonb,
            'active'::evaluator_state
        )
        ON CONFLICT (namespace, name, version) DO UPDATE
        SET content_hash = EXCLUDED.content_hash,
            wasm_bytes = EXCLUDED.wasm_bytes,
            wasm_size_bytes = EXCLUDED.wasm_size_bytes,
            interface_name = EXCLUDED.interface_name,
            interface_version = EXCLUDED.interface_version,
            wit_world = EXCLUDED.wit_world,
            abi_contract_hash = EXCLUDED.abi_contract_hash,
            abi_adapter = EXCLUDED.abi_adapter,
            runtime = EXCLUDED.runtime,
            runtime_version = EXCLUDED.runtime_version,
            runtime_fingerprint = EXCLUDED.runtime_fingerprint,
            description = EXCLUDED.description,
            tags = EXCLUDED.tags,
            metadata = EXCLUDED.metadata,
            state = EXCLUDED.state,
            updated_at = now()
        "#,
    )
    .bind(content_hash)
    .bind(wasm_bytes)
    .bind(wasm_size_bytes)
    .bind(abi_contract_hash)
    .execute(primary)
    .await?;

    Ok(())
}

fn sentiment_wasm_path() -> anyhow::Result<PathBuf> {
    let mut candidates = Vec::new();

    if let Ok(path) = std::env::var(SENTIMENT_WASM_PATH_ENV) {
        candidates.push(PathBuf::from(path));
    }

    if let Ok(target_dir) = std::env::var("CARGO_TARGET_DIR") {
        candidates.push(Path::new(&target_dir).join(SENTIMENT_WASM_TARGET_PATH));
    }

    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    if let Some(workspace_root) = manifest_dir.parent() {
        candidates.push(
            workspace_root
                .join("target")
                .join(SENTIMENT_WASM_TARGET_PATH),
        );
    }

    candidates.push(manifest_dir.join("target").join(SENTIMENT_WASM_TARGET_PATH));
    candidates.push(Path::new("target").join(SENTIMENT_WASM_TARGET_PATH));

    let mut searched = Vec::new();
    for candidate in candidates {
        if searched.iter().any(|path: &PathBuf| path == &candidate) {
            continue;
        }
        if candidate.is_file() {
            return Ok(candidate);
        }
        searched.push(candidate);
    }

    anyhow::bail!(
        "failed to find sentiment evaluator WASM; build it first with `cargo build -p sentiment-basic-en --target wasm32-wasip2 --release --locked` or set {}. Searched: {}",
        SENTIMENT_WASM_PATH_ENV,
        searched
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    )
}

async fn configure_shard_placement(primary: &PgPool) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO database_placements (alias, database_url_env, role, status)
        VALUES ($1, $2, 'shard', 'active')
        ON CONFLICT (alias) DO UPDATE
        SET database_url_env = EXCLUDED.database_url_env,
            role = EXCLUDED.role,
            status = EXCLUDED.status,
            updated_at = now()
        "#,
    )
    .bind(SHARD_ALIAS)
    .bind(SHARD_DATABASE_URL_ENV_VALUE)
    .execute(primary)
    .await?;

    Ok(())
}

async fn install_seed_status_failure(primary: &PgPool) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        CREATE OR REPLACE FUNCTION fail_run_creation_seed_status()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF OLD.status = 'pending'
               AND NEW.status = 'seeded'
               AND NEW.database_alias = 'shard_001'
            THEN
                RAISE EXCEPTION 'injected failure after execution seed commit';
            END IF;
            RETURN NEW;
        END;
        $$
        "#,
    )
    .execute(primary)
    .await?;
    sqlx::query(
        r#"
        CREATE TRIGGER fail_run_creation_seed_status
        BEFORE UPDATE ON run_creation_placements
        FOR EACH ROW
        EXECUTE FUNCTION fail_run_creation_seed_status()
        "#,
    )
    .execute(primary)
    .await?;
    Ok(())
}

async fn remove_seed_status_failure(primary: &PgPool) -> anyhow::Result<()> {
    sqlx::query("DROP TRIGGER IF EXISTS fail_run_creation_seed_status ON run_creation_placements")
        .execute(primary)
        .await?;
    sqlx::query("DROP FUNCTION IF EXISTS fail_run_creation_seed_status()")
        .execute(primary)
        .await?;
    Ok(())
}

async fn expire_run_creation_lease(primary: &PgPool, run_id: Uuid) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE runs
        SET coordinator_leased_until = now() - interval '1 second'
        WHERE id = $1::uuid
          AND status = 'creating'::run_status
        "#,
    )
    .bind(run_id)
    .execute(primary)
    .await?;
    Ok(())
}

async fn age_dispatch_cursor(primary: &PgPool, run_id: Uuid) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE run_shard_dispatch_cursors
        SET updated_at = now() - interval '1 hour'
        WHERE run_id = $1::uuid
        "#,
    )
    .bind(run_id)
    .execute(primary)
    .await?;
    Ok(())
}

async fn expire_chunk_lease(primary: &PgPool, run_id: Uuid) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE run_chunks
        SET status = 'leased',
            lease_token = gen_random_uuid(),
            leased_until = now() - interval '1 second',
            updated_at = now()
        WHERE run_id = $1::uuid
        "#,
    )
    .bind(run_id)
    .execute(primary)
    .await?;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum CaseMode {
    Passing,
    AgentFailure,
}

fn create_run(config: &IntegrationConfig, input: RunCreateInput<'_>) -> anyhow::Result<Uuid> {
    let output = create_run_output(config, input)?;
    if !output.status.success() {
        anyhow::bail!(
            "run create failed with status {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let payload: Value = serde_json::from_slice(&output.stdout)?;
    let run_id = payload["data"]["run_id"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("run create response did not include data.run_id"))?;

    Ok(Uuid::parse_str(run_id)?)
}

#[derive(Clone, Copy)]
struct RunCreateInput<'a> {
    default_execution_alias: &'a str,
    shard_assignment_policy: &'a str,
    agent_url: &'a str,
    profile_id: &'a str,
    case_count: usize,
    case_mode: CaseMode,
    evaluator_ref: &'a str,
}

fn create_run_output(
    config: &IntegrationConfig,
    input: RunCreateInput<'_>,
) -> anyhow::Result<Output> {
    let test_files = write_run_inputs(
        input.profile_id,
        input.agent_url,
        input.evaluator_ref,
        input.case_count,
        input.case_mode,
    )?;
    run_vigilo_output(
        config,
        input.default_execution_alias,
        input.shard_assignment_policy,
        [
            "run",
            "create",
            "--profile-file",
            path_arg(&test_files.profile)?,
            "--dataset-file",
            path_arg(&test_files.dataset)?,
        ],
    )
}

async fn drive_run_to_status(
    fixture: &E2eFixture,
    phase: &str,
    default_execution_alias: &str,
    shard_assignment_policy: &str,
    run_id: Uuid,
    expected_status: &str,
) -> anyhow::Result<Value> {
    let run_id_arg = run_id.to_string();
    let mut last_status = None;

    for cycle in 1..=E2E_MAX_RUNTIME_CYCLES {
        run_vigilo_ok(
            &fixture.config,
            default_execution_alias,
            shard_assignment_policy,
            [
                "coordinator",
                "--run-chunk-dispatch-window-size",
                "512",
                "once",
            ],
        )?;

        let status = run_vigilo_json(
            &fixture.config,
            default_execution_alias,
            shard_assignment_policy,
            ["run", "status", run_id_arg.as_str()],
        )?;
        if status["data"]["status"].as_str() == Some(expected_status) {
            return Ok(status);
        }

        if !run_has_terminal_live_progress(&status) {
            for _ in 0..E2E_WORKER_PASSES_PER_CYCLE {
                run_vigilo_ok(
                    &fixture.config,
                    default_execution_alias,
                    shard_assignment_policy,
                    ["worker", "once"],
                )?;
            }
        }

        run_vigilo_ok(
            &fixture.config,
            default_execution_alias,
            shard_assignment_policy,
            ["coordinator", "once"],
        )?;

        let status = run_vigilo_json(
            &fixture.config,
            default_execution_alias,
            shard_assignment_policy,
            ["run", "status", run_id_arg.as_str()],
        )?;
        if status["data"]["status"].as_str() == Some(expected_status) {
            return Ok(status);
        }

        last_status = Some(status);
        if cycle < E2E_MAX_RUNTIME_CYCLES {
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    }

    let last_status = match last_status {
        Some(status) => serde_json::to_string_pretty(&status)?,
        None => "<none>".to_owned(),
    };
    let diagnostics = match collect_runtime_diagnostics(fixture, run_id).await {
        Ok(diagnostics) => serde_json::to_string_pretty(&diagnostics)?,
        Err(error) => format!("<failed to collect diagnostics: {error:#}>"),
    };
    anyhow::bail!(
        "phase {phase:?}: run {run_id} did not reach status {expected_status} after {E2E_MAX_RUNTIME_CYCLES} cycles; last status:\n{last_status}\nruntime diagnostics:\n{diagnostics}"
    );
}

fn run_has_terminal_live_progress(status: &Value) -> bool {
    let sealed = status["data"]["live_progress"]["sealed_shard_count"]
        .as_u64()
        .unwrap_or(0);
    let routes = status["meta"]["execution_route_count"]
        .as_u64()
        .unwrap_or(0);
    routes > 0 && sealed == routes
}

#[test]
fn terminal_live_progress_requires_every_shard_seal() {
    let mut status = json!({"data":{"live_progress":{"failed_chunk_count":1,
        "expected_execution_count":2,"terminal_execution_count":2,"sealed_shard_count":1}},
        "meta":{"execution_route_count":2}});
    assert!(!run_has_terminal_live_progress(&status));
    status["data"]["live_progress"]["sealed_shard_count"] = json!(2);
    assert!(run_has_terminal_live_progress(&status));
    status["meta"]["execution_route_count"] = json!(0);
    assert!(!run_has_terminal_live_progress(&status));
}

async fn collect_runtime_diagnostics(fixture: &E2eFixture, run_id: Uuid) -> anyhow::Result<Value> {
    let target_control = sqlx::query_scalar::<_, Value>(
        r#"
        SELECT COALESCE(
            (
                SELECT jsonb_build_object(
                    'run_id', r.id,
                    'run_key', r.run_key,
                    'status', r.status::text,
                    'gate_status', r.gate_status::text,
                    'expected_execution_count', r.expected_execution_count,
                    'terminal_execution_count', r.terminal_execution_count,
                    'passed_execution_count', r.passed_execution_count,
                    'failed_execution_count', r.failed_execution_count,
                    'errored_execution_count', r.errored_execution_count,
                    'coordinator_leased_until', r.coordinator_leased_until,
                    'coordinator_heartbeat_at', r.coordinator_heartbeat_at,
                    'updated_at', r.updated_at,
                    'dispatch_cursors', COALESCE(
                        (
                            SELECT jsonb_agg(
                                jsonb_build_object(
                                    'run_shard', c.run_shard,
                                    'status', c.status,
                                    'updated_at', c.updated_at
                                )
                                ORDER BY c.run_shard
                            )
                            FROM run_shard_dispatch_cursors c
                            WHERE c.run_id = r.id
                        ),
                        '[]'::jsonb
                    ),
                    'placements', COALESCE(
                        (
                            SELECT jsonb_agg(
                                jsonb_build_object(
                                    'run_shard', sp.run_shard,
                                    'database_alias', sp.database_alias,
                                    'status', sp.status,
                                    'route_version', sp.route_version
                                )
                                ORDER BY sp.run_shard
                            )
                            FROM shard_placements sp
                            WHERE sp.run_id = r.id
                        ),
                        '[]'::jsonb
                    )
                )
                FROM runs r
                WHERE r.id = $1::uuid
            ),
            'null'::jsonb
        )
        "#,
    )
    .bind(run_id)
    .fetch_one(&fixture.primary)
    .await?;

    let finalization_candidates = sqlx::query_scalar::<_, Value>(
        r#"
        SELECT COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'run_id', r.id,
                    'run_key', r.run_key,
                    'status', r.status::text,
                    'expected_execution_count', r.expected_execution_count,
                    'terminal_execution_count', r.terminal_execution_count,
                    'coordinator_heartbeat_at', r.coordinator_heartbeat_at,
                    'updated_at', r.updated_at,
                    'cursor_count', (
                        SELECT COUNT(*)
                        FROM run_shard_dispatch_cursors c
                        WHERE c.run_id = r.id
                    ),
                    'open_cursor_count', (
                        SELECT COUNT(*)
                        FROM run_shard_dispatch_cursors c
                        WHERE c.run_id = r.id
                          AND c.status = 'open'
                    )
                )
                ORDER BY COALESCE(r.coordinator_heartbeat_at, r.updated_at), r.id
            ),
            '[]'::jsonb
        )
        FROM runs r
        WHERE r.status IN ('running'::run_status, 'finalizing'::run_status)
          AND (
              r.status <> 'finalizing'::run_status
              OR r.coordinator_leased_until IS NULL
              OR r.coordinator_leased_until < now()
          )
          AND EXISTS (
              SELECT 1
              FROM run_shard_dispatch_cursors c
              WHERE c.run_id = r.id
          )
          AND NOT EXISTS (
              SELECT 1
              FROM run_shard_dispatch_cursors c
              WHERE c.run_id = r.id
                AND c.status = 'open'
          )
        "#,
    )
    .fetch_one(&fixture.primary)
    .await?;

    let mut execution_storage = serde_json::Map::new();
    execution_storage.insert(
        PRIMARY_ALIAS.to_owned(),
        execution_storage_diagnostics(&fixture.primary, run_id).await?,
    );
    execution_storage.insert(
        SHARD_ALIAS.to_owned(),
        execution_storage_diagnostics(&fixture.shard, run_id).await?,
    );

    Ok(json!({
        "target_control": target_control,
        "eligible_finalization_candidates": finalization_candidates,
        "execution_storage": execution_storage,
    }))
}

async fn execution_storage_diagnostics(db: &PgPool, run_id: Uuid) -> anyhow::Result<Value> {
    Ok(sqlx::query_scalar::<_, Value>(
        r#"
        SELECT jsonb_build_object(
            'chunks', COALESCE(
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'chunk_id', rc.id,
                            'run_shard', rc.run_shard,
                            'status', rc.status,
                            'dispatched_at', rc.dispatched_at,
                            'leased_until', rc.leased_until,
                            'recovery_count', rc.recovery_count,
                            'updated_at', rc.updated_at
                        )
                        ORDER BY rc.run_shard, rc.ordinal_start
                    )
                    FROM run_chunks rc
                    WHERE rc.run_id = $1::uuid
                ),
                '[]'::jsonb
            ),
            'execution_status_counts', COALESCE(
                (
                    SELECT jsonb_object_agg(counts.status, counts.execution_count)
                    FROM (
                        SELECT e.status::text AS status, COUNT(*) AS execution_count
                        FROM executions e
                        WHERE e.run_id = $1::uuid
                        GROUP BY e.status
                    ) counts
                ),
                '{}'::jsonb
            ),
            'problem_executions', COALESCE(
                (
                    SELECT jsonb_agg(problem ORDER BY run_shard, execution_id)
                    FROM (
                        SELECT
                            jsonb_build_object(
                                'execution_id', e.id,
                                'run_shard', e.run_shard,
                                'status', e.status::text,
                                'current_attempt_no', e.current_attempt_no,
                                'current_attempt_id', e.current_attempt_id,
                                'last_error_message', e.last_error_message,
                                'retry_count', e.retry_count,
                                'retry_after', e.retry_after,
                                'updated_at', e.updated_at
                            ) AS problem,
                            e.run_shard,
                            e.id AS execution_id
                        FROM executions e
                        WHERE e.run_id = $1::uuid
                          AND (
                              e.status <> 'completed'::execution_status
                              OR e.last_error_message IS NOT NULL
                          )
                        ORDER BY e.run_shard, e.id
                        LIMIT 50
                    ) problems
                ),
                '[]'::jsonb
            ),
            'attempts', COALESCE(
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'attempt_id', ea.id,
                            'execution_id', ea.execution_id,
                            'run_shard', ea.run_shard,
                            'attempt_no', ea.attempt_no,
                            'status', ea.status::text,
                            'error_message', ea.error_message,
                            'leased_until', ea.leased_until,
                            'updated_at', ea.updated_at
                        )
                        ORDER BY ea.run_shard, ea.execution_id, ea.attempt_no
                    )
                    FROM execution_attempts ea
                    WHERE ea.run_id = $1::uuid
                ),
                '[]'::jsonb
            ),
            'aggregates', COALESCE(
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'execution_id', agg.execution_id,
                            'run_shard', agg.run_shard,
                            'attempt_id', agg.attempt_id,
                            'overall_status', agg.overall_status::text,
                            'aggregate_score', agg.aggregate_score,
                            'evaluator_result_count', agg.evaluator_result_count,
                            'blocking_failures', agg.blocking_failures,
                            'updated_at', agg.updated_at
                        )
                        ORDER BY agg.run_shard, agg.execution_id
                    )
                    FROM execution_aggregates agg
                    WHERE agg.run_id = $1::uuid
                ),
                '[]'::jsonb
            ),
            'shard_summaries', COALESCE(
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'run_shard', summary.run_shard,
                            'status', summary.status,
                            'expected_execution_count', summary.expected_execution_count,
                            'execution_count', summary.execution_count,
                            'terminal_execution_count', summary.terminal_execution_count,
                            'aggregate_count', summary.aggregate_count,
                            'passed_execution_count', summary.passed_execution_count,
                            'failed_execution_count', summary.failed_execution_count,
                            'errored_execution_count', summary.errored_execution_count,
                            'missing_aggregate_count', summary.missing_aggregate_count,
                            'failed_chunk_count', summary.failed_chunk_count,
                            'cancelled_chunk_count', summary.cancelled_chunk_count,
                            'updated_at', summary.updated_at
                        )
                        ORDER BY summary.run_shard
                    )
                    FROM run_shard_summaries summary
                    WHERE summary.run_id = $1::uuid
                ),
                '[]'::jsonb
            )
        )
        "#,
    )
    .bind(run_id)
    .fetch_one(db)
    .await?)
}

fn run_vigilo_json<'a>(
    config: &IntegrationConfig,
    default_execution_alias: &str,
    shard_assignment_policy: &str,
    args: impl IntoIterator<Item = &'a str>,
) -> anyhow::Result<Value> {
    let output = run_vigilo_ok(
        config,
        default_execution_alias,
        shard_assignment_policy,
        args,
    )?;
    Ok(serde_json::from_slice(&output.stdout)?)
}

fn run_vigilo_ok<'a>(
    config: &IntegrationConfig,
    default_execution_alias: &str,
    shard_assignment_policy: &str,
    args: impl IntoIterator<Item = &'a str>,
) -> anyhow::Result<Output> {
    let args = args.into_iter().collect::<Vec<_>>();
    let command = format!("vigilo {}", args.join(" "));
    let output = run_vigilo_output(
        config,
        default_execution_alias,
        shard_assignment_policy,
        args,
    )?;
    if !output.status.success() {
        anyhow::bail!(
            "command {command:?} failed with status {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    Ok(output)
}

fn run_vigilo_output<'a>(
    config: &IntegrationConfig,
    default_execution_alias: &str,
    shard_assignment_policy: &str,
    args: impl IntoIterator<Item = &'a str>,
) -> anyhow::Result<Output> {
    let args = args.into_iter().collect::<Vec<_>>();
    Ok(Command::new(env!("CARGO_BIN_EXE_vigilo"))
        .env(PRIMARY_DATABASE_URL_ENV, &config.primary_url)
        .env(SHARD_DATABASE_URL_ENV, &config.shard_url)
        .env("MESSAGING_URL", &config.messaging_url)
        .env("VIGILO_MQ_NAMESPACE", &config.mq_namespace)
        .env(
            "VIGILO_DEFAULT_SHARD_DATABASE_ALIAS",
            default_execution_alias,
        )
        .env("VIGILO_SHARD_ASSIGNMENT_POLICY", shard_assignment_policy)
        .args(["-q", "-f", "json"])
        .args(&args)
        .output()?)
}

struct RunInputFiles {
    profile: PathBuf,
    dataset: PathBuf,
}

fn write_run_inputs(
    profile_id: &str,
    agent_url: &str,
    evaluator_ref: &str,
    case_count: usize,
    case_mode: CaseMode,
) -> anyhow::Result<RunInputFiles> {
    let dir = std::env::temp_dir()
        .join("vigilo-multi-database-e2e")
        .join(Uuid::now_v7().to_string());
    fs::create_dir_all(&dir)?;

    let profile = dir.join("profile.yaml");
    let dataset = dir.join("dataset.yaml");
    fs::write(&profile, profile_yaml(profile_id, agent_url, evaluator_ref))?;
    fs::write(&dataset, dataset_yaml(case_count, case_mode))?;

    Ok(RunInputFiles { profile, dataset })
}

fn path_arg(path: &Path) -> anyhow::Result<&str> {
    path.to_str()
        .ok_or_else(|| anyhow::anyhow!("test path is not valid UTF-8: {}", path.display()))
}

fn profile_yaml(profile_id: &str, agent_url: &str, evaluator_ref: &str) -> String {
    format!(
        r#"
profile_id: {profile_id}
profile_version: 1.0.0
description: End-to-end multi-database runtime profile.
defaults:
  max_attempts: 1
  request_timeout_secs: 10
  fail_on_any_blocking_failure: true
  min_execution_score: 0.5
scorecard:
  gates:
    - id: quality_release
      dimension: quality
      min_mean_score: 0.5
      min_coverage: 1.0
persistence:
  mode: full
  persist_raw_outputs: all
  persist_evaluator_evidence: true
agent:
  provider: integration
  name: mock-agent
  http:
    url: {agent_url}
case_groups:
  - id: sentiment
    description: Integration sentiment cases.
    applies_to:
      task_type: classification
    evaluators:
      - id: sentiment
        ref: {evaluator_ref}
        required: true
        dimension: quality
        blocking: true
        weight: 1.0
        normalization:
          method: ordinal
          values:
            positive: 1.0
            neutral: 0.5
            negative: 0.0
        pass_threshold: 0.5
    aggregation:
      dimensions:
        quality:
          method: min_score
          blocking: true
          weight: 1.0
"#
    )
}

fn dataset_yaml(case_count: usize, case_mode: CaseMode) -> String {
    let dataset_id = Uuid::now_v7();
    let mut raw = format!("dataset_id: {dataset_id}\ndataset_version: 1.0.0\ncases:\n");

    for ordinal in 0..case_count {
        let case_id = Uuid::now_v7();
        let prompt = match case_mode {
            CaseMode::Passing => format!("case {ordinal}: this is good and reliable"),
            CaseMode::AgentFailure => "force-agent-error".to_string(),
        };
        raw.push_str(&format!(
            r#"  - id: {case_id}
    task_type: classification
    input:
      user_message: "{prompt}"
    expected:
      label: positive
    tags: [integration]
    metadata:
      ordinal: {ordinal}
"#
        ));
    }

    raw
}

fn parse_jsonl(bytes: &[u8]) -> anyhow::Result<Vec<Value>> {
    String::from_utf8_lossy(bytes)
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).map_err(Into::into))
        .collect()
}

async fn shard_placement_alias(
    db: &PgPool,
    run_id: Uuid,
    run_shard: i16,
) -> anyhow::Result<String> {
    Ok(sqlx::query_scalar::<_, String>(
        r#"
        SELECT database_alias
        FROM shard_placements
        WHERE run_id = $1::uuid
          AND run_shard = $2
        "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .fetch_one(db)
    .await?)
}

async fn execution_count(db: &PgPool, run_id: Uuid) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM executions
        WHERE run_id = $1::uuid
        "#,
    )
    .bind(run_id)
    .fetch_one(db)
    .await?)
}

async fn execution_status_count(db: &PgPool, run_id: Uuid, status: &str) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM executions
        WHERE run_id = $1::uuid
          AND status = $2::execution_status
        "#,
    )
    .bind(run_id)
    .bind(status)
    .fetch_one(db)
    .await?)
}

async fn run_chunk_count(db: &PgPool, run_id: Uuid) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::bigint FROM run_chunks WHERE run_id = $1::uuid",
    )
    .bind(run_id)
    .fetch_one(db)
    .await?)
}

async fn dispatch_cursor_count(db: &PgPool, run_id: Uuid) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::bigint FROM run_shard_dispatch_cursors WHERE run_id = $1::uuid",
    )
    .bind(run_id)
    .fetch_one(db)
    .await?)
}

async fn run_creation_placement_state(
    db: &PgPool,
    run_id: Uuid,
    database_alias: &str,
) -> anyhow::Result<(String, i32)> {
    Ok(sqlx::query_as::<_, (String, i32)>(
        r#"
        SELECT status, attempt_count
        FROM run_creation_placements
        WHERE run_id = $1::uuid
          AND database_alias = $2
        "#,
    )
    .bind(run_id)
    .bind(database_alias)
    .fetch_one(db)
    .await?)
}

async fn run_creation_chunk_plan_count(db: &PgPool, run_id: Uuid) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::bigint FROM run_creation_chunks WHERE run_id = $1::uuid",
    )
    .bind(run_id)
    .fetch_one(db)
    .await?)
}

async fn run_chunk_status_count(db: &PgPool, run_id: Uuid, status: &str) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM run_chunks
        WHERE run_id = $1::uuid
          AND status = $2
        "#,
    )
    .bind(run_id)
    .bind(status)
    .fetch_one(db)
    .await?)
}

async fn run_chunk_recovery_state(db: &PgPool, run_id: Uuid) -> anyhow::Result<(String, i32)> {
    Ok(sqlx::query_as::<_, (String, i32)>(
        r#"
        SELECT status, recovery_count
        FROM run_chunks
        WHERE run_id = $1::uuid
        "#,
    )
    .bind(run_id)
    .fetch_one(db)
    .await?)
}

async fn dispatch_cursor_status_count(
    db: &PgPool,
    run_id: Uuid,
    status: &str,
) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM run_shard_dispatch_cursors
        WHERE run_id = $1::uuid
          AND status = $2
        "#,
    )
    .bind(run_id)
    .bind(status)
    .fetch_one(db)
    .await?)
}

async fn outbox_event_count(db: &PgPool, run_id: Uuid, event_type: &str) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM outbox_events
        WHERE aggregate_id = $1::uuid
          AND event_type = $2
        "#,
    )
    .bind(run_id)
    .bind(event_type)
    .fetch_one(db)
    .await?)
}

async fn published_outbox_event_count(
    db: &PgPool,
    run_id: Uuid,
    event_type: &str,
) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM outbox_events
        WHERE aggregate_id = $1::uuid
          AND event_type = $2
          AND status = 'published'::outbox_status
        "#,
    )
    .bind(run_id)
    .bind(event_type)
    .fetch_one(db)
    .await?)
}
