//! Durable outbox publication.
//!
//! Database workflows insert event records into the local `outbox_events` table
//! in the same transaction as state changes. Control-database workflows write
//! to the control placement; execution workflows write to the execution
//! placement that owns the changed rows.
//!
//! The coordinator calls this module once per active database placement after
//! each orchestration pass to claim a bounded batch of delivery rows, publish
//! each joined event payload to the message broker, and either mark it
//! published or reschedule the delivery row for retry.
//! This keeps database state and external message delivery loosely coupled
//! without losing events when a process exits between commit and publish.

use async_trait::async_trait;
use futures_util::{
    StreamExt,
    stream::FuturesUnordered,
};
use sqlx::PgPool;
use tracing::{
    error,
    warn,
};

use crate::{
    db::tables::outbox_events,
    models::outbox_event::OutboxEvent,
    mq,
};

/// Runtime knobs for one outbox publishing pass.
///
/// `batch_size` bounds coordinator work per cycle, `publish_parallelism` bounds
/// concurrent broker publishes, `lease_seconds` prevents other coordinators
/// from claiming the same events immediately, and `retry_delay_seconds`
/// controls when a failed publish becomes eligible again.
#[derive(Debug, Clone)]
pub(crate) struct OutboxPublisherConfig {
    pub(crate) batch_size: i64,
    pub(crate) publish_parallelism: usize,
    pub(crate) lease_seconds: i32,
    pub(crate) retry_delay_seconds: i32,
}

impl Default for OutboxPublisherConfig {
    fn default() -> Self {
        Self {
            batch_size: 1_000,
            publish_parallelism: 64,
            lease_seconds: 60,
            retry_delay_seconds: 10,
        }
    }
}

/// Counts produced by a single publish pass.
///
/// `claimed_events` is the number of events leased from the database. The
/// remaining fields describe the outcomes for those claimed events.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct OutboxPublishStats {
    pub(crate) claimed_events: usize,
    pub(crate) published_events: usize,
    pub(crate) failed_events: usize,
    pub(crate) stale_event_claims: usize,
}

#[derive(Debug, Clone, Copy)]
enum OutboxPublishOutcome {
    Published,
    Failed,
    StaleClaim,
}

/// Transport boundary for publishing outbox event records as broker messages.
///
/// Keeping this as a trait lets the coordinator publish to RabbitMQ in
/// production while tests or future transports can supply a different
/// implementation without changing claim/reschedule behavior.
#[async_trait]
pub(crate) trait EventPublisher: Send + Sync {
    #[must_use = "the event is published only when this future is awaited"]
    async fn publish(&self, event: &OutboxEvent) -> anyhow::Result<()>;
}

/// RabbitMQ-backed event publisher.
pub(crate) struct MqEventPublisher<'a> {
    client: &'a mq::Client,
}

impl<'a> MqEventPublisher<'a> {
    /// Wraps the shared message-queue client without taking ownership.
    pub(crate) fn new(client: &'a mq::Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl EventPublisher for MqEventPublisher<'_> {
    async fn publish(&self, event: &OutboxEvent) -> anyhow::Result<()> {
        self.client
            .publish_json(&event.event_type, &event.payload, &event.dedupe_key)
            .await
    }
}

async fn publish_claimed_event(
    db: &PgPool,
    publisher: &dyn EventPublisher,
    retry_delay_seconds: i32,
    event: OutboxEvent,
) -> anyhow::Result<OutboxPublishOutcome> {
    let Some(claim_token) = event.claim_token else {
        warn!(
            event_id = %event.id,
            event_type = %event.event_type,
            "claimed outbox event did not include a claim token"
        );
        return Ok(OutboxPublishOutcome::StaleClaim);
    };

    match publisher.publish(&event).await {
        Ok(()) => {
            let marked =
                outbox_events::mark_outbox_event_published(db, event.id, claim_token).await?;
            if marked == 0 {
                warn!(
                    event_id = %event.id,
                    event_type = %event.event_type,
                    "outbox publish succeeded but claim was no longer current"
                );
                return Ok(OutboxPublishOutcome::StaleClaim);
            }
            Ok(OutboxPublishOutcome::Published)
        }
        Err(err) => {
            let message = err.to_string();
            error!(
                event_id = %event.id,
                event_type = %event.event_type,
                error = %message,
                "outbox publish failed; scheduling retry"
            );
            let rescheduled = outbox_events::reschedule_outbox_event(
                db,
                event.id,
                claim_token,
                retry_delay_seconds,
                &message,
            )
            .await?;
            if rescheduled == 0 {
                warn!(
                    event_id = %event.id,
                    event_type = %event.event_type,
                    "outbox publish failed but claim was no longer current"
                );
                return Ok(OutboxPublishOutcome::StaleClaim);
            }
            Ok(OutboxPublishOutcome::Failed)
        }
    }
}

/// Claims and publishes a bounded batch of pending outbox delivery rows.
///
/// Each claimed event record is published independently. Successful publishes are
/// marked `published` in the ledger and removed from the delivery queue;
/// failures are logged and rescheduled so a later coordinator pass can retry
/// them.
pub(crate) async fn publish_pending_events(
    db: &PgPool,
    publisher: &dyn EventPublisher,
    config: &OutboxPublisherConfig,
) -> anyhow::Result<OutboxPublishStats> {
    let claimed_events =
        outbox_events::claim_publishable_outbox_events(db, config.batch_size, config.lease_seconds)
            .await?;

    let mut stats = OutboxPublishStats {
        claimed_events: claimed_events.len(),
        ..OutboxPublishStats::default()
    };

    let parallelism = config.publish_parallelism.max(1);
    let mut events = claimed_events.into_iter();
    let mut pending = FuturesUnordered::new();

    loop {
        while pending.len() < parallelism {
            let Some(event) = events.next() else {
                break;
            };

            pending.push(publish_claimed_event(
                db,
                publisher,
                config.retry_delay_seconds,
                event,
            ));
        }

        let Some(result) = pending.next().await else {
            break;
        };

        match result? {
            OutboxPublishOutcome::Published => {
                stats.published_events += 1;
            }
            OutboxPublishOutcome::Failed => {
                stats.failed_events += 1;
            }
            OutboxPublishOutcome::StaleClaim => {
                stats.stale_event_claims += 1;
            }
        }
    }

    Ok(stats)
}
