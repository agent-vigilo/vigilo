//! Real control transitions and abandoned-future recovery preserve durable work.
//! The fixture advances only leases belonging to its interrupted run, after
//! joining the aborted workers. Live execution policies remain enabled throughout.
use vigilo::operations::ExecutionControl;

use super::*;

impl Fixture {
    pub(super) fn start_worker(&mut self) {
        let worker = self.runtime.as_ref().unwrap().worker();
        self.worker_handles.push(worker.clone());
        let stopped = self.stop.clone();
        self.tasks
            .push(tokio::spawn(async move { worker.run(stopped).await }));
    }
    async fn submitted(&mut self, owner: Uuid, upload: Uuid, retry: bool) -> anyhow::Result<Uuid> {
        let run = Uuid::now_v7();
        self.object_owners.insert(run, owner);
        let user = self.user(owner);
        let mut input = self.input(run, upload)?;
        if retry {
            input.profile["defaults"]["max_attempts"] = json!(2);
        }
        let prepared = user.runs().prepare_run(input, &self.stop).await?;
        user.runs().accept_run(prepared, &self.stop).await?;
        Ok(run)
    }
    async fn calls_after(&self, before: usize, extra: usize) -> anyhow::Result<()> {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let signal = self.agent.entered.notified();
                if self.agent.calls.load(Ordering::SeqCst) >= before + extra {
                    break;
                }
                signal.await;
            }
        })
        .await
        .context("agent did not receive admitted work")
    }
    pub(super) async fn failure_scenarios(
        &mut self,
        owner: Uuid,
        upload: Uuid,
    ) -> anyhow::Result<()> {
        self.agent.delay.store(250, Ordering::SeqCst);
        let before = self.agent.calls.load(Ordering::SeqCst);
        let run = self.submitted(owner, upload, false).await?;
        self.calls_after(before, 1).await?;
        let paused = self
            .user(owner)
            .control_run(run, ExecutionControl::Paused)
            .await?;
        tokio::time::timeout(Duration::from_secs(25), async {
            loop {
                let progress = self.user(owner).run_control_status(run).await?;
                let leased: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM run_chunks WHERE run_id=$1 AND status='leased'",
                )
                .bind(run)
                .fetch_one(&self.db.admin[1])
                .await?;
                if progress.applied_placement_count == progress.placement_count && leased == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            anyhow::Ok(())
        })
        .await
        .context("pause did not quiesce admitted work")??;
        let calls = self.agent.calls.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            self.agent.calls.load(Ordering::SeqCst),
            calls,
            "paused run admitted work"
        );
        assert!(!self.user(owner).run_status(run).await?.terminal);
        let resumed = self
            .user(owner)
            .control_run(run, ExecutionControl::Enabled)
            .await?;
        assert_eq!(resumed.revision, paused.revision + 1);
        self.agent.delay.store(0, Ordering::SeqCst);
        assert!(self.await_terminal(owner, run).await?.gate_passed);

        self.agent.delay.store(400, Ordering::SeqCst);
        let before = self.agent.calls.load(Ordering::SeqCst);
        let run = self.submitted(owner, upload, false).await?;
        self.calls_after(before, 3).await?;
        let retained=tokio::time::timeout(Duration::from_secs(10),async {
            loop {
                let retained:Vec<(Uuid,String)>=sqlx::query_as("SELECT execution_id,md5(response_evidence_bytes) FROM execution_attempts WHERE run_id=$1 AND response_evidence_bytes IS NOT NULL ORDER BY execution_id LIMIT 128").bind(run).fetch_all(&self.db.admin[1]).await?;
                if !retained.is_empty() {return anyhow::Ok(retained);}
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.context("revocation oracle needs completed evidence")??;
        self.user(owner)
            .control_run(run, ExecutionControl::Revoked)
            .await?;
        let terminal = self.await_terminal(owner, run).await?;
        assert_eq!(terminal.details.status, "cancelled");
        tokio::time::timeout(Duration::from_secs(25), async {
            loop {
                if self.user(owner).run_control_status(run).await?.settled {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            anyhow::Ok(())
        })
        .await
        .context("revocation did not settle across placements")??;
        for (execution, digest) in retained {
            let found:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM execution_attempts WHERE run_id=$1 AND execution_id=$2 AND md5(response_evidence_bytes)=$3)").bind(run).bind(execution).bind(digest).fetch_one(&self.db.admin[1]).await?;
            assert!(found, "revocation lost persisted evidence");
        }
        let running: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM execution_attempts WHERE run_id=$1 AND status='running'",
        )
        .bind(run)
        .fetch_one(&self.db.admin[1])
        .await?;
        assert_eq!(running, 0, "settled revocation retained live attempts");

        self.agent.delay.store(2000, Ordering::SeqCst);
        let before = self.agent.calls.load(Ordering::SeqCst);
        let run = self.submitted(owner, upload, true).await?;
        self.calls_after(before, 1).await?;
        // Keep the coordinator, but drop every execution service while it has work.
        for task in self.tasks.drain(self.coordinator_count..) {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        }
        let leases:Vec<(Uuid,chrono::DateTime<chrono::Utc>)>=sqlx::query_as("SELECT id,leased_until FROM run_chunks WHERE run_id=$1 AND status='leased' ORDER BY id").bind(run).fetch_all(&self.db.admin[1]).await?;
        assert!(!leases.is_empty());
        tokio::time::sleep(Duration::from_secs(16)).await;
        let current:Vec<(Uuid,chrono::DateTime<chrono::Utc>)>=sqlx::query_as("SELECT id,leased_until FROM run_chunks WHERE run_id=$1 AND status='leased' ORDER BY id").bind(run).fetch_all(&self.db.admin[1]).await?;
        assert_eq!(
            current, leases,
            "abandoned work continued renewing its lease"
        );
        sqlx::query("UPDATE run_chunks SET leased_until=clock_timestamp()-interval '1 second' WHERE run_id=$1 AND status='leased'").bind(run).execute(&self.db.admin[1]).await?;
        sqlx::query("UPDATE execution_attempts SET leased_until=clock_timestamp()-interval '1 second' WHERE run_id=$1 AND status='running'").bind(run).execute(&self.db.admin[1]).await?;
        self.agent.delay.store(0, Ordering::SeqCst);
        self.start_worker();
        self.start_worker();
        assert!(
            self.await_terminal(owner, run).await?.gate_passed,
            "dropped work did not recover"
        );
        let exact:(i64,i64)=sqlx::query_as("SELECT count(*),count(DISTINCT case_id) FROM executions WHERE run_id=$1 AND status='completed'").bind(run).fetch_one(&self.db.admin[1]).await?;
        assert_eq!(exact, (101, 101), "recovery duplicated or lost cases");
        Ok(())
    }
}
