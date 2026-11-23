//! A durable RabbitMQ subscription observes completion through the library's
//! wire contract and reads it with independently authorized caller context.

use std::process::Stdio;

use lapin::{
    Connection,
    ConnectionProperties,
    ExchangeKind,
    message::BasicGetMessage,
    options::*,
    types::FieldTable,
};
use tokio::io::AsyncWriteExt as _;
use vigilo::operations::{
    RunCompletedEventV1,
    UserClient,
};

use super::*;

async fn next_delivery(channel: &lapin::Channel, queue: &str) -> anyhow::Result<BasicGetMessage> {
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            if let Some(message) = channel.basic_get(queue, BasicGetOptions::default()).await? {
                return Ok(message);
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    })
    .await
    .context("completion delivery deadline")?
}

impl Fixture {
    async fn verify_independent_read(
        &self,
        owner: Uuid,
        body: &[u8],
        expected: &vigilo::operations::RunResults,
    ) -> anyhow::Result<()> {
        let Some(binary) = std::env::var_os("VIGILO_TEST_COMPLETION_CONSUMER") else {
            return Ok(());
        };
        let role = &self.db.roles[2];
        let url = |index: usize| -> anyhow::Result<url::Url> {
            let mut url = url::Url::parse(&self.db.urls[index])?;
            url.set_username(role)
                .map_err(|()| anyhow::anyhow!("fixture username"))?;
            url.set_password(Some("fixture"))
                .map_err(|()| anyhow::anyhow!("fixture password"))?;
            Ok(url)
        };
        let mut child = tokio::process::Command::new(binary)
            .arg("completion")
            .env("VIGILO_CONSUMER_TENANT", owner.to_string())
            .env("VIGILO_CONSUMER_ACTOR", owner.to_string())
            .env("VIGILO_CONSUMER_DATABASE_ROLE", role)
            .env("VIGILO_CONSUMER_DATABASE_URL", url(0)?.as_str())
            .env("VIGILO_CONSUMER_EXECUTION_ALIAS", SHARD_ALIAS)
            .env("VIGILO_CONSUMER_EXECUTION_URL", url(1)?.as_str())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let mut input = child.stdin.take().context("consumer stdin")?;
        input.write_all(body).await?;
        drop(input);
        let output = tokio::time::timeout(Duration::from_secs(20), child.wait_with_output())
            .await
            .context("independent completion consumer deadline")??;
        anyhow::ensure!(
            output.status.success(),
            "independent completion consumer: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&output.stdout)?,
            serde_json::to_value(expected)?
        );
        Ok(())
    }

    async fn exercise_completion_subscription(&mut self) -> anyhow::Result<()> {
        self.setup().await?;
        let runtime = self.runtime.as_ref().unwrap().clone();
        let coordinator = runtime.coordinator();
        let broker = std::env::var("MESSAGING_URL")?;
        let queue = format!("vigilo.{}.completion-test", self.namespace);
        let exchange = format!("vigilo.{}.events", self.namespace);
        let first_connection =
            Connection::connect(&broker, ConnectionProperties::default()).await?;
        let channel = first_connection.create_channel().await?;
        channel
            .exchange_declare(
                &exchange,
                ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;
        channel
            .queue_declare(
                &queue,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;
        channel
            .queue_bind(
                &queue,
                &exchange,
                RunCompletedEventV1::ROUTING_KEY,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;

        let mut owners = self.owners.keys().copied();
        let owner = owners.next().context("run owner")?;
        let other = owners.next().context("other owner")?;
        let upload = self.upload(owner, 101).await?;
        let run = Uuid::now_v7();
        self.object_owners.insert(run, owner);
        let user = self.user(owner);
        let prepared = user
            .runs()
            .prepare_run(self.input(run, upload)?, &self.stop)
            .await?;
        user.runs().accept_run(prepared, &self.stop).await?;
        let stop = self.stop.clone();
        self.tasks
            .push(tokio::spawn(async move { coordinator.run(stop).await }));
        self.start_worker();
        self.start_worker();

        let first = next_delivery(&channel, &queue).await?;
        assert_eq!(first.routing_key.as_str(), RunCompletedEventV1::ROUTING_KEY);
        assert!(!first.redelivered);
        let event: RunCompletedEventV1 = serde_json::from_slice(&first.data)?;
        assert_eq!((event.tenant_id, event.run_id), (owner, run));
        assert_eq!(event.gate_status, "pass");
        assert_eq!(event.terminal_execution_count, 101);
        assert_eq!(event.passed_execution_count, 101);
        assert_eq!(event.failed_execution_count, 0);
        assert_eq!(event.errored_execution_count, 0);
        let message_id = format!("run:{run}:completed");
        assert_eq!(
            first.properties.message_id().as_ref().unwrap().as_str(),
            message_id
        );
        assert_eq!(first.properties.delivery_mode(), &Some(2));
        assert_eq!(
            first.properties.content_type().as_ref().unwrap().as_str(),
            "application/json"
        );

        let pools = self.db.pools()?;
        let authorized = UserContext::new(owner, owner).map_err(anyhow::Error::msg)?;
        let client: UserClient = pools.user_client();
        let committed = client.run_results(authorized, event.run_id).await?;
        assert_eq!(committed.details.run_id, run);
        assert_eq!(committed.details.run_key, event.run_key);
        assert_eq!(committed.details.status, "completed");
        assert_eq!(committed.details.gate_status, event.gate_status);
        assert_eq!(committed.summary.passed_execution_count, 101);
        assert!(committed.summary_complete);
        assert!(committed.scorecard.is_some());
        for denied in [
            UserContext::new(other, other).map_err(anyhow::Error::msg)?,
            UserContext::new(owner, Uuid::now_v7()).map_err(anyhow::Error::msg)?,
        ] {
            assert!(matches!(
                client.run_results(denied, event.run_id).await,
                Err(vigilo::operations::Error::NotFound(_))
            ));
        }

        self.verify_independent_read(owner, &first.data, &committed)
            .await?;

        // Processing finished, but the consumer disconnected before its ack.
        first_connection.close(200, "restart before ack").await?;
        let restarted = Connection::connect(&broker, ConnectionProperties::default()).await?;
        let channel = restarted.create_channel().await?;
        let repeated = next_delivery(&channel, &queue).await?;
        assert!(repeated.redelivered);
        assert_eq!(repeated.data, first.data);
        assert_eq!(
            repeated.properties.message_id(),
            first.properties.message_id()
        );
        let event: RunCompletedEventV1 = serde_json::from_slice(&repeated.data)?;
        assert_eq!(
            pools
                .user_client()
                .run_results(authorized, event.run_id)
                .await?,
            committed
        );
        self.verify_independent_read(owner, &repeated.data, &committed)
            .await?;
        repeated.ack(BasicAckOptions::default()).await?;
        assert!(
            channel
                .basic_get(&queue, BasicGetOptions::default())
                .await?
                .is_none()
        );
        restarted.close(200, "completion acknowledged").await?;
        pools.close().await;
        self.sample_oracle(run, 101).await?;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn completion_subscription_recovers_and_reads_exact_results() -> anyhow::Result<()> {
    if std::env::var(E2E_ENABLED_ENV).as_deref() != Ok("1") {
        return Ok(());
    }
    let mut fixture = Fixture::new().await?;
    let result = std::panic::AssertUnwindSafe(Box::pin(fixture.exercise_completion_subscription()))
        .catch_unwind()
        .await;
    let cleanup = fixture.cleanup().await;
    match result {
        Ok(result) => result.and(cleanup),
        Err(panic) => {
            cleanup?;
            std::panic::resume_unwind(panic);
        }
    }
}
