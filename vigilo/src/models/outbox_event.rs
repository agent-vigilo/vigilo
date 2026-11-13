//! Outbox event persistence models.
//!
//! Outbox events are durable records written inside database transactions and
//! later published as broker messages.
//! Active delivery state is stored in a separate hot queue table so publication
//! can be retried without scanning the historical event ledger.

use chrono::{
    DateTime,
    Utc,
};
use serde::{
    Deserialize,
    Serialize,
};
use uuid::Uuid;

/// Insert payload for a durable outbox event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OutboxEventDraft {
    /// Routing key or semantic event type to publish.
    pub(crate) event_type: String,
    /// Aggregate category associated with the event, such as `run`.
    pub(crate) aggregate_type: String,
    /// Aggregate instance id associated with the event.
    pub(crate) aggregate_id: Uuid,
    /// Idempotency key used to avoid duplicate event rows.
    pub(crate) dedupe_key: String,
}

/// Mutable outbox delivery status fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OutboxEventPatch {
    /// New outbox lifecycle status.
    pub(crate) status: String,
    /// Latest delivery error, if any.
    pub(crate) error_message: Option<String>,
}

/// Persisted outbox event row.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub(crate) struct OutboxEvent {
    /// Authoritative ledger owner returned by restricted discovery.
    pub(crate) tenant_id: Uuid,
    /// Event row id.
    pub(crate) id: Uuid,
    /// Routing key or semantic event type to publish.
    pub(crate) event_type: String,
    /// Aggregate category associated with the event, such as `run`.
    pub(crate) aggregate_type: String,
    /// Aggregate instance id associated with the event.
    pub(crate) aggregate_id: Uuid,
    /// Idempotency key used to avoid duplicate event rows.
    pub(crate) dedupe_key: String,
    /// JSON event body to publish.
    pub(crate) payload: serde_json::Value,
    /// Current ledger lifecycle status.
    pub(crate) status: String,
    /// Delivery queue shard, if the event still has pending delivery work.
    pub(crate) claim_shard: Option<i16>,
    /// Earliest time the event may be claimed for publication, if pending.
    pub(crate) available_at: Option<DateTime<Utc>>,
    /// Current publisher claim token, if the delivery row is leased.
    pub(crate) claim_token: Option<Uuid>,
    /// Deadline for the current publisher claim, if leased.
    pub(crate) claimed_until: Option<DateTime<Utc>>,
    /// Number of publication claims issued for the delivery row.
    pub(crate) publish_attempt_count: Option<i32>,
    /// Time the event was successfully published.
    pub(crate) published_at: Option<DateTime<Utc>>,
    /// Latest delivery error, if any.
    pub(crate) error_message: Option<String>,
    /// Time this event row was inserted.
    pub(crate) created_at: DateTime<Utc>,
    /// Time this event row was last updated.
    pub(crate) updated_at: DateTime<Utc>,
}
