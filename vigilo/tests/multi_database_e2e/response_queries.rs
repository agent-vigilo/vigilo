//! Routed whole-worker observations with exact attempt/result oracles. Only an
//! explicitly enabled service run observes SQL; normal tests never start services.
//! A supplied immutable binary and Wasm allow comparison with preserved releases.
use std::collections::BTreeMap;

use super::{
    postgres_probe,
    *,
};

/// Owns only the unique temporary directory created by this scenario.
struct InputCleanup(PathBuf);
impl Drop for InputCleanup {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn invoke(
    config: &IntegrationConfig,
    binary: &Path,
    args: &[&str],
    fuel: bool,
) -> anyhow::Result<Value> {
    let mut command = Command::new(binary);
    command
        .env(PRIMARY_DATABASE_URL_ENV, &config.primary_url)
        .env(SHARD_DATABASE_URL_ENV, &config.shard_url)
        .env("MESSAGING_URL", &config.messaging_url)
        .env("VIGILO_MQ_NAMESPACE", &config.mq_namespace)
        .env("VIGILO_DEFAULT_SHARD_DATABASE_ALIAS", SHARD_ALIAS)
        .env("VIGILO_SHARD_ASSIGNMENT_POLICY", "single-default")
        .args(["-q", "-f", "json"]);
    if fuel {
        command.env("VIGILO_WASM_FUEL_PER_EVALUATION", "1");
    }
    let output = command.args(args).output()?;
    anyhow::ensure!(
        output.status.success(),
        "comparison command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    if output.stdout.is_empty() {
        Ok(Value::Null)
    } else {
        Ok(serde_json::from_slice(&output.stdout)?)
    }
}

async fn counters(pool: &PgPool) -> anyhow::Result<BTreeMap<String, i64>> {
    Ok(sqlx::query_as::<_, (String, i64)>(
        r#"
        /* response-query-observer */
        SELECT CASE WHEN ltrim(query) ~* '^select' THEN 'read'
                    WHEN ltrim(query) ~* '^(insert|update|delete)' THEN 'dml'
                    WHEN ltrim(query) ~* '^with' THEN 'mixed'
                    WHEN ltrim(query) ~* '^(begin|commit|rollback)' THEN 'transaction'
                    ELSE 'other' END, sum(calls)::bigint
        FROM public.pg_stat_statements
        WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())
          AND query NOT LIKE '%response-query-observer%'
        GROUP BY 1
    "#,
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect())
}
fn delta(before: &BTreeMap<String, i64>, after: &BTreeMap<String, i64>) -> BTreeMap<String, i64> {
    after
        .iter()
        .map(|(key, value)| {
            let difference = value - before.get(key).copied().unwrap_or(0);
            assert!(difference >= 0, "server counters reset");
            (key.clone(), difference)
        })
        .filter(|(_, value)| *value != 0)
        .collect()
}

#[tokio::test]
async fn routed_response_query_matrix() -> anyhow::Result<()> {
    // Separate opt-in prevents unrelated concurrent tests contaminating counters.
    if std::env::var("VIGILO_RESPONSE_QUERY_MATRIX")
        .ok()
        .as_deref()
        != Some("1")
    {
        return Ok(());
    }
    let fixture = E2eFixture::setup()
        .await?
        .ok_or_else(|| anyhow::anyhow!("enable the multi-database tier"))?;
    let binary = std::env::var_os("VIGILO_RESPONSE_QUERY_BINARY")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_vigilo")));
    let mapped = match std::env::var("VIGILO_RESPONSE_QUERY_PROFILE").as_deref() {
        Ok("raw") => false,
        Ok("mapped") | Err(_) => true,
        Ok(_) => anyhow::bail!("response query profile must be raw or mapped"),
    };
    let digest = |path: &Path| -> anyhow::Result<String> {
        let mut hasher = blake3::Hasher::new();
        hasher.update_reader(fs::File::open(path)?)?;
        Ok(hasher.finalize().to_hex().to_string())
    };
    println!(
        "response-worker-build {}",
        json!({"binary":digest(&binary)?,"evaluator":digest(&sentiment_wasm_path()?)?,"mapped_profile":mapped,"schema":"current greenfield migrations"})
    );
    let epoch_before:(String,i64)=sqlx::query_as("/* response-query-observer */ SELECT stats_reset::text,dealloc::bigint FROM public.pg_stat_statements_info").fetch_one(&fixture.primary).await?;
    for (name, mode, max_attempts, fuel) in [
        ("success", CaseMode::Passing, 1, false),
        ("resources", CaseMode::Passing, 1, false),
        ("evaluator-failure", CaseMode::Passing, 1, true),
        ("agent-failure", CaseMode::AgentFailure, 1, false),
        ("retry", CaseMode::AgentFailure, 2, false),
        ("mapping-failure", CaseMode::MappingFailure, 1, false),
    ] {
        if name == "mapping-failure" && !mapped {
            continue;
        }
        let files = write_run_inputs(
            &format!("query_{name}"),
            &fixture.agent.url,
            SENTIMENT_EVALUATOR_REF,
            1,
            mode,
        )?;
        let _cleanup = InputCleanup(files.profile.parent().unwrap().to_path_buf());
        let mut profile: Value = serde_yaml::from_str(&fs::read_to_string(&files.profile)?)?;
        profile["defaults"]["max_attempts"] = json!(max_attempts);
        if name == "resources" {
            fs::write(
                &files.dataset,
                fs::read_to_string(&files.dataset)?
                    .replace("this is good and reliable", "force-resource-response"),
            )?;
            let evaluator = profile["case_groups"][0]["evaluators"][0].clone();
            profile["case_groups"][0]["evaluators"] = json!(
                (0..8)
                    .map(|index| {
                        let mut binding = evaluator.clone();
                        binding["id"] = json!(format!("sentiment-{index}"));
                        binding
                    })
                    .collect::<Vec<_>>()
            );
            let parts=profile["agent"]["response"]["interpretation"]["mapping"]["items"][0]["item"]["content"].as_array_mut().unwrap();
            for _ in 0..2 {
                parts.push(json!({"media_type":"image/png","source":{"representation":"base64","selection":{"pointer":"/data"}}}));
            }
        }
        if !mapped {
            profile["agent"].as_object_mut().unwrap().remove("response");
        }
        fs::write(&files.profile, serde_yaml::to_string(&profile)?)?;
        let created = invoke(
            &fixture.config,
            &binary,
            &[
                "run",
                "create",
                "--profile-file",
                path_arg(&files.profile)?,
                "--dataset-file",
                path_arg(&files.dataset)?,
            ],
            false,
        )?;
        let run = Uuid::parse_str(created["data"]["run_id"].as_str().unwrap())?;
        let mut previous_attempts = 0i64;
        for pass in 0..8 {
            // Dispatch, retry scheduling and finalization are outside worker observation.
            invoke(&fixture.config, &binary, &["coordinator", "once"], false)?;
            invoke(&fixture.config, &binary, &["coordinator", "once"], false)?;
            let terminal:i64=sqlx::query_scalar("SELECT count(*) FROM executions WHERE run_id=$1 AND status::text IN ('completed','failed')").bind(run).fetch_one(&fixture.shard).await?;
            if terminal == 1 {
                break;
            }
            let control_probe = postgres_probe::Probe::start(&fixture.config.primary_url)?;
            let execution_probe = postgres_probe::Probe::start(&fixture.config.shard_url)?;
            let measured = IntegrationConfig {
                primary_url: control_probe.url.clone(),
                shard_url: execution_probe.url.clone(),
                ..fixture.config.clone()
            };
            let before_control = counters(&fixture.primary).await?;
            let before_execution = counters(&fixture.shard).await?;
            invoke(&measured, &binary, &["worker", "once"], fuel)?;
            let control_protocol = control_probe.finish()?;
            let execution_protocol = execution_probe.finish()?;
            let after_control = counters(&fixture.primary).await?;
            let after_execution = counters(&fixture.shard).await?;
            let attempts: i64 =
                sqlx::query_scalar("SELECT count(*) FROM execution_attempts WHERE run_id=$1")
                    .bind(run)
                    .fetch_one(&fixture.shard)
                    .await?;
            let control = delta(&before_control, &after_control);
            let execution = delta(&before_execution, &after_execution);
            if attempts > previous_attempts {
                // Each runtime connection sets statement_timeout and lock_timeout.
                // Keep those two reads/five protocol exchanges explicit; pool growth
                // is scheduling-dependent and must not hide added workflow queries.
                let control_connections = control_protocol.connections.unwrap() as i64;
                let execution_connections = execution_protocol.connections.unwrap() as i64;
                assert_eq!(
                    control,
                    BTreeMap::from([("read".into(), 4 + 2 * control_connections)])
                );
                let first_retry = name == "retry" && previous_attempts == 0;
                let failure = matches!(mode, CaseMode::AgentFailure);
                let (reads, dml, mixed, transactions, other, exchanges) = if first_retry {
                    (7, 5, 4, 6, 0, 51)
                } else if failure {
                    (10, 5, 6, 8, 1, 64)
                } else if fuel || name == "mapping-failure" {
                    (11, 7, 7, 10, 1, 77)
                } else {
                    (11, 7, 8, 10, 1, 79)
                };
                let expected: BTreeMap<String, i64> = [
                    ("read", reads + 2 * execution_connections),
                    ("dml", dml),
                    ("mixed", mixed),
                    ("transaction", transactions),
                    ("other", other),
                ]
                .into_iter()
                .filter(|(_, count)| *count > 0)
                .map(|(name, count)| (name.into(), count))
                .collect();
                assert_eq!(execution, expected, "{name}: execution statement budget");
                assert_eq!(
                    control_protocol.query_sync_exchanges,
                    11 + 5 * control_connections as u64
                );
                assert_eq!(
                    execution_protocol.query_sync_exchanges,
                    exchanges + 5 * execution_connections as u64,
                    "{name}: execution exchange budget"
                );
            } else {
                assert!(
                    control.is_empty() && execution.is_empty(),
                    "empty queue pass submitted database work"
                );
                assert_eq!(
                    control_protocol.query_sync_exchanges + execution_protocol.query_sync_exchanges,
                    0
                );
            }
            println!(
                "response-worker-observation {}",
                json!({"scenario":name,"pass":pass,"attempt_delta":attempts-previous_attempts,"control":delta(&before_control,&after_control),"execution":delta(&before_execution,&after_execution),"control_protocol":control_protocol,"execution_protocol":execution_protocol})
            );
            previous_attempts = attempts;
            // Fixed retry schedule, not wall-clock polling; this fixture owns the run.
            sqlx::query("UPDATE executions SET retry_after=now()-interval '1 second' WHERE run_id=$1 AND status='retry_scheduled'").bind(run).execute(&fixture.shard).await?;
        }
        let (attempts,terminal,results):(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM execution_attempts WHERE run_id=$1),(SELECT count(*) FROM executions WHERE run_id=$1 AND status::text IN ('completed','failed')),(SELECT count(*) FROM evaluator_results WHERE run_id=$1)").bind(run).fetch_one(&fixture.shard).await?;
        assert_eq!(attempts, max_attempts, "{name}");
        assert_eq!(terminal, 1, "{name}");
        assert_eq!(
            results,
            if matches!(mode, CaseMode::AgentFailure) {
                0
            } else if name == "resources" {
                8
            } else {
                1
            },
            "{name}"
        );
        let (errors, retained, retained_bytes): (i64,i64,i64) = sqlx::query_as("SELECT (SELECT count(*) FROM evaluator_results WHERE run_id=$1 AND error_code IS NOT NULL),(SELECT count(*) FROM execution_attempts WHERE run_id=$1 AND response_evidence IS NOT NULL),(SELECT COALESCE(sum(octet_length(response_evidence_bytes)),0)::bigint FROM execution_attempts WHERE run_id=$1)").bind(run).fetch_one(&fixture.shard).await?;
        assert_eq!(
            errors,
            if fuel || name == "mapping-failure" {
                1
            } else {
                0
            },
            "{name}"
        );
        assert_eq!(retained, if mapped { attempts } else { 0 }, "{name}");
        println!(
            "response-worker-oracle {}",
            json!({"scenario":name,"attempts":attempts,"terminal":terminal,"results":results,"errors":errors,"retained_packets":retained,"retained_payload_bytes":retained_bytes})
        );
        // No process receives these files after run creation; snapshots own the inputs.
        fs::remove_dir_all(files.profile.parent().unwrap())?;
    }
    let epoch_after:(String,i64)=sqlx::query_as("/* response-query-observer */ SELECT stats_reset::text,dealloc::bigint FROM public.pg_stat_statements_info").fetch_one(&fixture.primary).await?;
    assert_eq!(
        epoch_before, epoch_after,
        "server statistics reset or evicted during comparison"
    );
    Ok(())
}
