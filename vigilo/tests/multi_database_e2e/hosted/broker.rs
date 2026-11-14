//! Adversarial queue hints and exact namespace cleanup use fixture authority.
//! Only known queues/exchanges belonging to this invocation are removed; normal
//! workers must verify message hints before invoking any host resource provider.
use lapin::{
    BasicProperties,
    Connection,
    ConnectionProperties,
    options::*,
};

use super::*;

impl Fixture {
    pub(super) async fn forged_messages(&self) -> anyhow::Result<()> {
        self.runtime.as_ref().unwrap().coordinator().once().await?;
        let connection = Connection::connect(
            &std::env::var("MESSAGING_URL")?,
            ConnectionProperties::default(),
        )
        .await?;
        let channel = connection.create_channel().await?;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await?;
        let queue = format!("vigilo.{}.worker", self.namespace);
        let mut original = Vec::new();
        for _ in 0..16 {
            let Some(message) = channel
                .basic_get(&queue, BasicGetOptions::default())
                .await?
            else {
                break;
            };
            original.push(message);
        }
        anyhow::ensure!(
            !original.is_empty(),
            "forgery fixture needs real accepted messages"
        );
        let valid: Value = serde_json::from_slice(&original[0].data)?;
        let owner = Uuid::parse_str(
            valid["tenant_id"]
                .as_str()
                .context("canonical message owner")?,
        )?;
        let other = *self
            .owners
            .keys()
            .find(|&&id| id != owner)
            .context("different fixture owner")?;
        let before = self.host.calls.lock().unwrap().len();
        let requests = self.agent.calls.load(Ordering::SeqCst);
        let attempts: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_attempts")
            .fetch_one(&self.db.admin[1])
            .await?;
        for field in ["tenant_id", "plan_digest"] {
            let mut forged = valid.clone();
            forged[field] = if field == "tenant_id" {
                json!(other)
            } else {
                json!("0".repeat(64))
            };
            channel
                .basic_publish(
                    "",
                    &queue,
                    BasicPublishOptions::default(),
                    &serde_json::to_vec(&forged)?,
                    BasicProperties::default(),
                )
                .await?
                .await?;
            assert_eq!(self.runtime.as_ref().unwrap().worker().once().await?, 1);
        }
        assert_eq!(
            self.host.calls.lock().unwrap().len(),
            before,
            "unverified hints reached host bindings"
        );
        assert_eq!(self.agent.calls.load(Ordering::SeqCst), requests);
        let after: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_attempts")
            .fetch_one(&self.db.admin[1])
            .await?;
        assert_eq!(after, attempts, "forged authority allocated work");
        for message in original {
            channel
                .basic_publish(
                    "",
                    &queue,
                    BasicPublishOptions::default(),
                    &message.data,
                    message.properties.clone(),
                )
                .await?
                .await?;
            message.ack(BasicAckOptions::default()).await?;
        }
        channel.close(200, "fixture injection complete").await?;
        connection.close(200, "fixture injection complete").await?;
        Ok(())
    }
    pub(super) async fn cleanup_broker(&self) -> anyhow::Result<()> {
        let connection = Connection::connect(
            &std::env::var("MESSAGING_URL")?,
            ConnectionProperties::default(),
        )
        .await?;
        let prefix = format!("vigilo.{}", self.namespace);
        let mut queues = vec![
            format!("{prefix}.worker"),
            format!("{prefix}.events.domain"),
            format!("{prefix}.worker.quarantine"),
        ];
        for delay in ["5s", "10s", "20s", "40s", "80s", "160s", "320s", "640s"] {
            queues.push(format!("{prefix}.worker.retry.{delay}"));
        }
        for queue in queues {
            let channel = connection.create_channel().await?;
            match channel
                .queue_delete(&queue, QueueDeleteOptions::default())
                .await
            {
                Ok(_) => {
                    channel.close(200, "fixture resource removed").await?;
                }
                Err(lapin::Error::ProtocolError(e)) if e.get_id() == 404 => {}
                Err(e) => return Err(e.into()),
            }
        }
        for name in ["events", "worker.retry", "worker.quarantine"] {
            let channel = connection.create_channel().await?;
            match channel
                .exchange_delete(
                    &format!("{prefix}.{name}"),
                    ExchangeDeleteOptions::default(),
                )
                .await
            {
                Ok(_) => {
                    channel.close(200, "fixture resource removed").await?;
                }
                Err(lapin::Error::ProtocolError(e)) if e.get_id() == 404 => {}
                Err(e) => return Err(e.into()),
            }
        }
        connection.close(200, "fixture cleanup complete").await?;
        Ok(())
    }
}
