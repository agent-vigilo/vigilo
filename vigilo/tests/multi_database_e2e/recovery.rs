//! Live recovery ownership and serial-delay observations through the public CLI.
//! Two requested plans share a recovery cycle; a delayed missing-root response
//! must release its owner and leave the healthy plan eligible for admission.
use super::*;

#[path = "../support/storage_proxy.rs"]
pub(super) mod storage_proxy;

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
