//! Complete resident-worker storage traffic with exact persisted case/results
//! oracles. Serial chunks isolate reuse across shard contexts. An optional
//! frozen worker executable supports matched before/after diagnostics; creators
//! and correctness checks always use this checkout. No timing/throughput gate.
use super::{
    recovery::storage_proxy,
    *,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resident_worker_storage_traffic_preserves_exact_results() -> anyhow::Result<()> {
    if IntegrationConfig::from_env()?.is_none() {
        return Ok(());
    }
    for (name, batch_cases, batch_bytes, retry) in [
        ("aligned", 100, 8 * 1024 * 1024, false),
        ("shared_file", 128, 8 * 1024 * 1024, false),
        ("reread", 8, 2048, false),
        ("retry", 128, 8 * 1024 * 1024, true),
    ] {
        observe(name, batch_cases, batch_bytes, retry).await?;
    }
    Ok(())
}

async fn observe(
    name: &str,
    batch_cases: usize,
    batch_bytes: usize,
    retry: bool,
) -> anyhow::Result<()> {
    let fixture = E2eFixture::setup().await?.unwrap();
    let dir = tempfile::tempdir()?;
    let dataset = dir.path().join("dataset.yaml");
    let profile = dir.path().join("profile.yaml");
    let source: Value = serde_yaml::from_str(&dataset_yaml(201, CaseMode::Passing))?;
    fs::write(&dataset, serde_yaml::to_string(&source)?)?;
    fs::write(
        &profile,
        profile_yaml(
            "worker_storage",
            &fixture.agent.url,
            SENTIMENT_EVALUATOR_REF,
        ),
    )?;
    let owner_original = std::env::var("VIGILO_STORAGE_CONFIG")?;
    let upload = Uuid::now_v7().to_string();
    run_vigilo_ok(
        &fixture.config,
        SHARD_ALIAS,
        "single-default",
        [
            "dataset",
            "upload",
            "--upload-id",
            &upload,
            "--input-file",
            path_arg(&dataset)?,
            "--input-format",
            "yaml",
            "--storage-config",
            &owner_original,
            "--batch-cases",
            &batch_cases.to_string(),
            "--batch-bytes",
            &batch_bytes.to_string(),
        ],
    )?;
    let created = run_vigilo_json(
        &fixture.config,
        SHARD_ALIAS,
        "single-default",
        [
            "run",
            "create",
            "--profile-file",
            path_arg(&profile)?,
            "--dataset-upload-id",
            &upload,
        ],
    )?;
    let run = Uuid::parse_str(created["data"]["run_id"].as_str().unwrap())?;
    run_vigilo_ok(
        &fixture.config,
        SHARD_ALIAS,
        "single-default",
        ["coordinator", "once"],
    )?;
    let mut owner: toml::Value = toml::from_str(&fs::read_to_string(owner_original)?)?;
    let proxy = storage_proxy::Proxy::start(owner["endpoint"].as_str().unwrap().into()).await?;
    proxy.set(if retry { 5 } else { 0 });
    owner.as_table_mut().unwrap().insert(
        "read_endpoint_variable".into(),
        "VIGILO_E2E_WORKER_READ_ENDPOINT".into(),
    );
    let owner_path = dir.path().join("storage.toml");
    fs::write(&owner_path, toml::to_string(&owner)?)?;
    let binary = std::env::var("VIGILO_WORKER_TRAFFIC_BIN")
        .unwrap_or_else(|_| env!("CARGO_BIN_EXE_vigilo").into());
    let stderr = dir.path().join("worker.log");
    let mut worker = tokio::process::Command::new(&binary)
        .kill_on_drop(true)
        .env(PRIMARY_DATABASE_URL_ENV, &fixture.config.primary_url)
        .env(SHARD_DATABASE_URL_ENV, &fixture.config.shard_url)
        .env("MESSAGING_URL", &fixture.config.messaging_url)
        .env("VIGILO_MQ_NAMESPACE", &fixture.config.mq_namespace)
        .env("VIGILO_DEFAULT_SHARD_DATABASE_ALIAS", SHARD_ALIAS)
        .env("VIGILO_SHARD_ASSIGNMENT_POLICY", "single-default")
        .env("VIGILO_STORAGE_CONFIG", &owner_path)
        .env("VIGILO_E2E_WORKER_READ_ENDPOINT", &proxy.endpoint)
        .args(["-q", "worker", "start", "--max-inflight-chunks", "1"])
        .stdout(std::process::Stdio::null())
        .stderr(fs::File::create(&stderr)?)
        .spawn()?;
    let pid = worker.id().unwrap();
    let mut peak_rss_kib = None;
    let completion = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            if let Some(status) = worker.try_wait()? {
                anyhow::bail!("worker exited: {status}");
            }
            if let Some(rss) = rss_high_water_kib(pid) {
                peak_rss_kib = Some(peak_rss_kib.unwrap_or(0).max(rss));
            }
            if run_chunk_status_count(&fixture.shard, run, "completed").await? == 3 {
                break;
            }
            if retry {
                run_vigilo_ok(
                    &fixture.config,
                    SHARD_ALIAS,
                    "single-default",
                    ["coordinator", "once"],
                )?;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await;
    // Always reap the measured child, including timeout and database failures.
    let stopped = worker.kill().await;
    if let Err(error) = completion
        .as_ref()
        .map_err(|e| e.to_string())
        .and_then(|r| r.as_ref().map_err(|e| e.to_string()))
    {
        anyhow::bail!(
            "{name}: {error}; worker log: {}",
            fs::read_to_string(&stderr)?
        );
    }
    stopped?;
    completion??;
    if retry {
        tokio::time::timeout(Duration::from_secs(1), proxy.entered.notified()).await?;
    }
    let traffic = proxy.traffic.lock().unwrap().clone();
    assert_eq!(traffic.gets, proxy.count()); // This endpoint receives only GETs.
    assert!(traffic.gets > 0 && traffic.bytes > 0);
    // Frozen comparison executables only report observations. The checkout's
    // normal integration tier enforces one download per fitting object plus
    // the deliberately failed read. These fixtures fit within retention bounds.
    if std::env::var_os("VIGILO_WORKER_TRAFFIC_BIN").is_none() {
        assert_eq!(traffic.gets, traffic.objects.len() + usize::from(retry));
    }
    // Exact persisted IDs, payloads and terminal results prove no missing,
    // duplicated, reordered or substituted case input before reporting traffic.
    let actual: Vec<(Uuid, Value, i32, String)> = sqlx::query_as(
        "SELECT case_id,input_payload,(case_metadata->>'ordinal')::int,status::text FROM executions WHERE run_id=$1 ORDER BY (case_metadata->>'ordinal')::int")
        .bind(run).fetch_all(&fixture.shard).await?;
    assert_eq!(actual.len(), 201);
    for (ordinal, ((id, input, stored_ordinal, status), expected)) in actual
        .iter()
        .zip(source["cases"].as_array().unwrap())
        .enumerate()
    {
        assert_eq!(id.to_string(), expected["id"].as_str().unwrap());
        assert_eq!(input, &expected["input"]);
        assert_eq!(*stored_ordinal, ordinal as i32);
        assert_eq!(status, "completed");
    }
    assert_eq!(execution_count(&fixture.primary, run).await?, 0);
    let status = drive_run_to_status(
        &fixture,
        "worker traffic finalization",
        SHARD_ALIAS,
        "single-default",
        run,
        "completed",
    )
    .await?;
    assert_eq!(status["data"]["gate_status"], "pass");
    let scorecard: Value =
        sqlx::query_scalar("SELECT scorecard FROM run_scorecards WHERE run_id=$1")
            .bind(run)
            .fetch_one(&fixture.primary)
            .await?;
    assert_eq!(scorecard["gates"][0]["expected_count"], 201);
    assert_eq!(scorecard["gates"][0]["scored_count"], 201);
    println!(
        "worker_storage_traffic={}",
        json!({"scenario":name,"cases":201,"chunks":3,
        "batch_cases":batch_cases,"batch_bytes":batch_bytes,"injected_missing_read":retry,
        "worker_binary":binary,"peak_rss_kib":peak_rss_kib,"traffic":traffic,
        "correctness":"exact persisted case IDs, ordinals, inputs, completed executions and passing gate coverage",
        "measurement":"local complete worker GET diagnostic; RSS includes runtime/Wasm; not a throughput estimate"})
    );
    Ok(())
}

/// Linux process high-water observation, optional on other service-test hosts.
fn rss_high_water_kib(pid: u32) -> Option<u64> {
    fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:")?
                .split_whitespace()
                .next()?
                .parse()
                .ok()
        })
}
