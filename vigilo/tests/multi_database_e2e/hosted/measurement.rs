//! Optional exact-oracle hosting-mode diagnostic. Persistent services and their
//! warmup precede measurement; setup/observer traffic bypasses protocol probes.
//! Output separates communications, top-level server SQL, physical writes and time.
//! This local diagnostic is not a calibrated capacity or historical-revision gate.
use std::{
    process::Stdio,
    time::Instant,
};

use serde::{
    Deserialize,
    Serialize,
};

use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Sample {
    mode: Mode,
    cases: usize,
    jobs: usize,
}
#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    Hosted,
    Standalone,
}

impl Fixture {
    fn operator_url(&self, index: usize, submit: bool) -> anyhow::Result<String> {
        let mut url = url::Url::parse(&self.db.urls[index])?;
        url.set_username(if submit {
            &self.db.submitter
        } else {
            &self.db.operator
        })
        .map_err(|_| anyhow::anyhow!("fixture role"))?;
        url.set_password(Some("fixture"))
            .map_err(|_| anyhow::anyhow!("fixture password"))?;
        Ok(url.into())
    }
    fn cli(&self, owner: Uuid, submit: bool) -> anyhow::Result<tokio::process::Command> {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_vigilo"));
        command
            .kill_on_drop(true)
            .args(["-q", "-f", "json"])
            .env("DATABASE_MAX_CONNECTIONS", "4")
            .env("DATABASE_URL", self.operator_url(0, submit)?)
            .env(SHARD_DATABASE_URL_ENV, self.operator_url(1, submit)?)
            .env("MESSAGING_URL", std::env::var("MESSAGING_URL")?)
            .env("VIGILO_MQ_NAMESPACE", &self.namespace)
            .env("VIGILO_DEFAULT_SHARD_DATABASE_ALIAS", SHARD_ALIAS)
            .env("VIGILO_SHARD_ASSIGNMENT_POLICY", "single-default")
            .env(
                "VIGILO_STORAGE_CONFIG",
                self.root.path().join(format!("{owner}.toml")),
            )
            .env("VIGILO_HOSTED_SAMPLE_SECRET", owner.to_string());
        Ok(command)
    }
    async fn start_services(&mut self, owner: Uuid) -> anyhow::Result<()> {
        if self.standalone {
            let credentials = self.root.path().join("credentials.json");
            tokio::fs::write(&credentials,serde_json::to_vec(&json!({"version":1,"allow_http_loopback":true,"provider":{"type":"local","bindings":{"agent-api":{"source":{"type":"env","name":"VIGILO_HOSTED_SAMPLE_SECRET"},"allowed_origin":self.host.origin,"header":"authorization","prefix":"Bearer "}}}}))?).await?;
            let mut command = self.cli(owner, false)?;
            self.children.push(
                command
                    .args(["coordinator", "--tick-seconds", "1", "start"])
                    .stdout(Stdio::null())
                    .stderr(Stdio::inherit())
                    .spawn()?,
            );
            for _ in 0..2 {
                let mut command = self.cli(owner, false)?;
                command.env("DATABASE_MAX_CONNECTIONS", "2");
                self.children.push(
                    command
                        .args([
                            "worker",
                            "--wasm-max-concurrent-evaluations",
                            "4",
                            "--agent-credentials",
                            credentials.to_str().context("credential path")?,
                            "start",
                            "--max-inflight-chunks",
                            "2",
                        ])
                        .stdout(Stdio::null())
                        .stderr(Stdio::inherit())
                        .spawn()?,
                );
            }
        } else {
            let coordinator = self.runtime.as_ref().unwrap().coordinator();
            self.coordinator_count = 1;
            self.coordinator_handles.push(coordinator.clone());
            let stopped = self.stop.clone();
            self.tasks
                .push(tokio::spawn(async move { coordinator.run(stopped).await }));
            self.start_worker();
            self.start_worker();
        }
        Ok(())
    }
    pub(super) async fn stop_services(&mut self) -> anyhow::Result<()> {
        self.stop.cancel();
        let mut error = None;
        for mut task in self.tasks.drain(..) {
            match tokio::time::timeout(Duration::from_secs(20), &mut task).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(e))) => {
                    error.get_or_insert(anyhow::anyhow!(e));
                }
                Ok(Err(e)) => {
                    error.get_or_insert(e.into());
                }
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    error.get_or_insert(anyhow::anyhow!("service shutdown deadline"));
                }
            }
        }
        for mut child in self.children.drain(..) {
            if let Some(id) = child.id() {
                // Opt-in process diagnostics use the same Unix signal as the
                // service harness. Ordinary hosted correctness does not need it.
                if cfg!(unix) {
                    let status = tokio::process::Command::new("kill")
                        .args(["-TERM", &id.to_string()])
                        .status()
                        .await?;
                    if !status.success() {
                        child.start_kill()?;
                    }
                } else {
                    child.start_kill()?;
                }
            }
            if tokio::time::timeout(Duration::from_secs(20), child.wait())
                .await
                .is_err()
            {
                child.start_kill()?;
                child.wait().await?;
                error.get_or_insert(anyhow::anyhow!("CLI shutdown deadline"));
            }
        }
        self.worker_handles.clear();
        self.coordinator_handles.clear();
        if let Some(e) = error { Err(e) } else { Ok(()) }
    }
    async fn sample_submit(&mut self, owner: Uuid, upload: Uuid) -> anyhow::Result<Uuid> {
        let run = Uuid::now_v7();
        self.object_owners.insert(run, owner);
        let input = self.input(run, upload)?;
        if self.standalone {
            let profile = self.root.path().join(format!("{run}.json"));
            tokio::fs::write(&profile, serde_json::to_vec(&input.profile)?).await?;
            let output = tokio::time::timeout(
                Duration::from_secs(90),
                self.cli(owner, true)?
                    .args([
                        "run",
                        "create",
                        "--run-id",
                        &run.to_string(),
                        "--profile-file",
                        profile.to_str().context("profile path")?,
                        "--dataset-upload-id",
                        &upload.to_string(),
                        "--storage-config",
                        self.root
                            .path()
                            .join(format!("{owner}.toml"))
                            .to_str()
                            .context("storage path")?,
                    ])
                    .output(),
            )
            .await??;
            anyhow::ensure!(
                output.status.success(),
                "standalone submit failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        } else {
            let user = self.user(owner);
            let prepared = user.runs().prepare_run(input, &self.stop).await?;
            user.runs().accept_run(prepared, &self.stop).await?;
        }
        Ok(run)
    }
    async fn sample_oracle(&mut self, run: Uuid, cases: usize) -> anyhow::Result<()> {
        tokio::time::timeout(Duration::from_secs(180),async {
            loop {
                for child in &mut self.children {anyhow::ensure!(child.try_wait()?.is_none(),"resident child exited before completing work");}
                let (status,gate):(String,String)=sqlx::query_as("SELECT status::text,gate_status::text FROM runs WHERE id=$1").bind(run).fetch_one(&self.db.admin[0]).await?;
                if status=="completed" {anyhow::ensure!(gate=="pass","measured run failed its gate");break;}
                anyhow::ensure!(!matches!(status.as_str(),"failed"|"cancelled"),"measured run was {status}");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            let actual:Vec<Uuid>=sqlx::query_scalar("SELECT case_id FROM executions WHERE run_id=$1 AND status='completed' ORDER BY case_id LIMIT 5002").bind(run).fetch_all(&self.db.admin[1]).await?;
            let expected:Vec<_>=(1..=cases).map(|i|Uuid::from_u128(i as u128)).collect();
            anyhow::ensure!(actual==expected,"sample lost or duplicated case IDs");
            let exact:(i64,i64)=sqlx::query_as("SELECT count(*),count(*) FILTER(WHERE total=1 AND passed=1) FROM (SELECT e.id,count(r.execution_id) total,count(r.execution_id) FILTER(WHERE r.outcome='completed' AND r.judgment='passed') passed FROM executions e LEFT JOIN evaluator_results r ON r.run_id=e.run_id AND r.run_shard=e.run_shard AND r.execution_id=e.id WHERE e.run_id=$1 GROUP BY e.run_id,e.run_shard,e.id) results").bind(run).fetch_one(&self.db.admin[1]).await?;
            anyhow::ensure!(exact==(cases as i64,cases as i64),"sample evaluator oracle: {exact:?}");
            anyhow::Ok(())
        }).await.context("sample completion deadline")?
    }
    async fn metrics(&self) -> anyhow::Result<Vec<Value>> {
        let mut result = Vec::new();
        let role_names = if self.standalone {
            vec![self.db.operator.clone(), self.db.submitter.clone()]
        } else {
            self.db.roles[..3].to_vec()
        };
        for db in &self.db.admin {
            let sql:Value=sqlx::query_scalar("SELECT jsonb_build_object('calls',coalesce(sum(calls),0),'exec_ms',coalesce(sum(total_exec_time),0),'plan_ms',coalesce(sum(total_plan_time),0),'rows',coalesce(sum(rows),0),'wal_bytes',coalesce(sum(wal_bytes),0)) FROM diagnostics.pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) AND userid IN (SELECT oid FROM pg_roles WHERE rolname=ANY($1)) AND toplevel").bind(&role_names).fetch_one(db).await?;
            sqlx::query("SELECT pg_stat_clear_snapshot()")
                .execute(db)
                .await?;
            let writes:Value=sqlx::query_scalar("SELECT jsonb_build_object('inserted',coalesce(sum(n_tup_ins),0),'updated',coalesce(sum(n_tup_upd),0),'deleted',coalesce(sum(n_tup_del),0)) FROM pg_stat_user_tables WHERE schemaname='public'").fetch_one(db).await?;
            let collector: Value =
                sqlx::query_scalar("SELECT to_jsonb(i) FROM diagnostics.pg_stat_statements_info i")
                    .fetch_one(db)
                    .await?;
            result.push(json!({"sql":sql,"writes":writes,"collector":collector}));
        }
        Ok(result)
    }
    pub(super) async fn measurement(&mut self, sample: Sample) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1..=5001).contains(&sample.cases) && (1..=8).contains(&sample.jobs),
            "invalid diagnostic workload"
        );
        self.standalone = matches!(sample.mode, Mode::Standalone);
        self.setup().await?;
        for db in &self.db.admin {
            sqlx::raw_sql("CREATE SCHEMA diagnostics; REVOKE ALL ON SCHEMA diagnostics FROM PUBLIC; CREATE EXTENSION pg_stat_statements WITH SCHEMA diagnostics").execute(db).await?;
        }
        // Dedicated proxies exclude metadata/bootstrap/oracle connections. Warm
        // counters are sampled after initialization and representative full work.
        if let Some(runtime) = self.runtime.take() {
            runtime.close().await;
        }
        for url in &mut self.db.urls {
            let probe = postgres_probe::Probe::start(url)?;
            *url = probe.url.clone();
            self.probes.push(probe);
        }
        let startup = Instant::now();
        if !self.standalone {
            self.construct_runtime().await?;
        }
        let startup_ms = startup.elapsed().as_secs_f64() * 1000.0;
        let owner = *self.owners.keys().next().unwrap();
        let upload = self.upload(owner, sample.cases).await?;
        // Remove only this sample's schema/bootstrap entries before warmup. Do
        // not reset other databases or include bootstrap DDL in collector demand.
        for db in &self.db.admin {
            sqlx::query("SELECT diagnostics.pg_stat_statements_reset(0::oid,(SELECT oid FROM pg_database WHERE datname=current_database()),0::bigint)").execute(db).await?;
        }
        self.start_services(owner).await?;
        let mut warmups = Vec::new();
        for _ in 0..2 {
            warmups.push(self.sample_submit(owner, upload).await?);
        }
        for run in warmups {
            self.sample_oracle(run, sample.cases).await?;
        }
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let before = self.metrics().await?;
        let protocol_before: Vec<_> = self.probes.iter().map(|p| p.snapshot()).collect();
        let start = Instant::now();
        let mut runs = Vec::new();
        for _ in 0..sample.jobs {
            runs.push(self.sample_submit(owner, upload).await?);
        }
        let submitted_ms = start.elapsed().as_secs_f64() * 1000.0;
        for &run in &runs {
            self.sample_oracle(run, sample.cases).await?;
        }
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        self.stop_services().await?;
        if let Some(runtime) = self.runtime.take() {
            runtime.close().await;
        }
        let mut protocol = Vec::new();
        for (probe, before) in self.probes.drain(..).zip(protocol_before) {
            let after = probe.finish()?;
            protocol.push(json!({"warm_exchanges":after.query_sync_exchanges.checked_sub(before.query_sync_exchanges).context("protocol counter reset")?,"warm_frontend_bytes":after.frontend_bytes.checked_sub(before.frontend_bytes).context("byte counter reset")?,"total_connections_including_warmup":after.connections}));
        }
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let after = self.metrics().await?;
        for (before, after) in before.iter().zip(&after) {
            anyhow::ensure!(
                before["collector"] == after["collector"],
                "SQL collector reset or evicted entries during measurement"
            );
        }
        eprintln!(
            "hosting_sample={}",
            json!({"mode":sample.mode,"cases":sample.cases,"jobs":sample.jobs,"workers":2,"inflight_per_worker":2,"startup_ms":startup_ms,"submit_ms":submitted_ms,"elapsed_ms":elapsed_ms,"before":before,"after":after,"protocol":protocol,"oracle":"exact completed case IDs and one completed/passing evaluator result per case","timing":"local debug-mode comparison; CLI caller process startup is included; persistent service warmup/startup excluded; SQL includes final service drain"})
        );
        Ok(())
    }
}
