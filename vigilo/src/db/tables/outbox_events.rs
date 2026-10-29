//! Outbox event table access.
//!
//! `outbox_events` is the durable event ledger. `outbox_delivery_queue` is the
//! hot claim/retry table scanned by publishers. Successful publication updates
//! the ledger and deletes the delivery row so the hot queue only contains
//! unfinished work.

use sqlx::PgPool;
use uuid::Uuid;

use crate::models::outbox_event::{
    OutboxEvent,
    OutboxEventDraft,
    OutboxEventPatch,
};

const OUTBOX_EVENT_SELECT: &str = r#"
    SELECT
        e.id,
        e.event_type,
        e.aggregate_type,
        e.aggregate_id,
        e.dedupe_key,
        e.payload,
        e.status::text as status,
        q.claim_shard,
        q.available_at,
        q.claim_token,
        q.claimed_until,
        q.publish_attempt_count,
        e.published_at,
        COALESCE(q.error_message, e.error_message) AS error_message,
        e.created_at,
        e.updated_at
    FROM outbox_events e
    LEFT JOIN outbox_delivery_queue q
      ON q.event_id = e.id
"#;

/// Counts events that still require broker publication or retry.
pub(crate) async fn count_pending_outbox_deliveries(db: &PgPool) -> anyhow::Result<i64> {
    let count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::bigint FROM outbox_delivery_queue")
        .fetch_one(db)
        .await?;

    Ok(count)
}

/// Inserts one durable outbox event.
///
/// The database trigger creates the matching `outbox_delivery_queue` row inside
/// the same transaction.
pub(crate) async fn insert_outbox_event(
    db: &PgPool,
    draft: &OutboxEventDraft,
) -> anyhow::Result<OutboxEvent> {
    let id = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO outbox_events (
            event_type, aggregate_type, aggregate_id, dedupe_key
        )
        VALUES ($1, $2, $3::uuid, $4)
        RETURNING id
        "#,
    )
    .bind(&draft.event_type)
    .bind(&draft.aggregate_type)
    .bind(draft.aggregate_id)
    .bind(&draft.dedupe_key)
    .fetch_one(db)
    .await?;

    select_outbox_event_by_id(db, id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("outbox event '{}' disappeared after insert", id))
}

/// Finds an outbox event by primary key.
pub(crate) async fn select_outbox_event_by_id(
    db: &PgPool,
    id: Uuid,
) -> anyhow::Result<Option<OutboxEvent>> {
    let event = sqlx::query_as::<_, OutboxEvent>(&format!(
        r#"
        {}
        WHERE e.id = $1::uuid
        "#,
        OUTBOX_EVENT_SELECT
    ))
    .bind(id)
    .fetch_optional(db)
    .await?;

    Ok(event)
}

/// Lists outbox event records with a given ledger status.
pub(crate) async fn list_outbox_events_by_status(
    db: &PgPool,
    status: &str,
    limit: i64,
) -> anyhow::Result<Vec<OutboxEvent>> {
    let events = sqlx::query_as::<_, OutboxEvent>(&format!(
        r#"
        {}
        WHERE e.status = $1::outbox_status
        ORDER BY e.created_at ASC
        LIMIT $2
        "#,
        OUTBOX_EVENT_SELECT
    ))
    .bind(status)
    .bind(limit)
    .fetch_all(db)
    .await?;

    Ok(events)
}

/// Claims available delivery rows and returns their joined event payloads.
///
/// The returned rows are the caller's publish lease. If publishing fails, use
/// `reschedule_outbox_event` to make the delivery row available for retry.
pub(crate) async fn claim_publishable_outbox_events(
    db: &PgPool,
    limit: i64,
    lease_seconds: i32,
) -> anyhow::Result<Vec<OutboxEvent>> {
    let events = sqlx::query_as::<_, OutboxEvent>(
        r#"
        WITH claim AS (
            SELECT claim_shard, event_id
            FROM outbox_delivery_queue
            WHERE available_at <= now()
            ORDER BY available_at ASC, event_id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT $1
        ),
        leased AS (
            UPDATE outbox_delivery_queue q
            SET claim_token = gen_random_uuid(),
                claimed_until = now() + ($2::int * interval '1 second'),
                available_at = now() + ($2::int * interval '1 second'),
                publish_attempt_count = q.publish_attempt_count + 1,
                updated_at = now()
            FROM claim
            WHERE q.claim_shard = claim.claim_shard
              AND q.event_id = claim.event_id
            RETURNING
                q.claim_shard,
                q.event_id,
                q.available_at,
                q.claim_token,
                q.claimed_until,
                q.publish_attempt_count,
                q.error_message
        )
        SELECT
            e.id,
            e.event_type,
            e.aggregate_type,
            e.aggregate_id,
            e.dedupe_key,
            e.payload,
            e.status::text as status,
            leased.claim_shard,
            leased.available_at,
            leased.claim_token,
            leased.claimed_until,
            leased.publish_attempt_count,
            e.published_at,
            COALESCE(leased.error_message, e.error_message) AS error_message,
            e.created_at,
            e.updated_at
        FROM leased
        JOIN outbox_events e
          ON e.id = leased.event_id
        ORDER BY leased.available_at ASC, e.id ASC
        "#,
    )
    .bind(limit)
    .bind(lease_seconds)
    .fetch_all(db)
    .await?;

    Ok(events)
}

/// Counts delivery rows currently available for publication.
///
/// Coordinators log this as a per-placement outbox backlog gauge before
/// claiming a bounded publish batch.
pub(crate) async fn count_publishable_outbox_backlog(db: &PgPool) -> anyhow::Result<i64> {
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM outbox_delivery_queue
        WHERE available_at <= now()
        "#,
    )
    .fetch_one(db)
    .await?;

    Ok(count)
}

/// Marks an outbox event as successfully published and removes delivery work.
pub(crate) async fn mark_outbox_event_published(
    db: &PgPool,
    id: Uuid,
    claim_token: Uuid,
) -> anyhow::Result<u64> {
    let result = sqlx::query(
        r#"
        WITH deleted AS (
            DELETE FROM outbox_delivery_queue
            WHERE event_id = $1::uuid
              AND claim_token = $2::uuid
            RETURNING event_id
        )
        UPDATE outbox_events e
        SET status = 'published'::outbox_status,
            published_at = now(),
            error_message = NULL,
            updated_at = now()
        FROM deleted
        WHERE e.id = deleted.event_id
        "#,
    )
    .bind(id)
    .bind(claim_token)
    .execute(db)
    .await?;

    Ok(result.rows_affected())
}

/// Reschedules a failed publish attempt for later retry.
pub(crate) async fn reschedule_outbox_event(
    db: &PgPool,
    id: Uuid,
    claim_token: Uuid,
    retry_after_seconds: i32,
    error_message: &str,
) -> anyhow::Result<u64> {
    let result = sqlx::query(
        r#"
        WITH rescheduled AS (
            UPDATE outbox_delivery_queue q
            SET available_at = now() + ($3::int * interval '1 second'),
                claim_token = NULL,
                claimed_until = NULL,
                error_message = $4,
                updated_at = now()
            WHERE q.event_id = $1::uuid
              AND q.claim_token = $2::uuid
            RETURNING q.event_id
        )
        UPDATE outbox_events e
        SET error_message = $4,
            updated_at = now()
        FROM rescheduled
        WHERE e.id = rescheduled.event_id
        "#,
    )
    .bind(id)
    .bind(claim_token)
    .bind(retry_after_seconds)
    .bind(error_message)
    .execute(db)
    .await?;

    Ok(result.rows_affected())
}

/// Updates outbox ledger status fields directly.
pub(crate) async fn update_outbox_event_status(
    db: &PgPool,
    id: Uuid,
    patch: &OutboxEventPatch,
) -> anyhow::Result<Option<OutboxEvent>> {
    let mut tx = db.begin().await?;

    let event_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        UPDATE outbox_events
        SET status = $2::outbox_status,
            published_at = CASE WHEN $2 = 'published' THEN now() ELSE published_at END,
            error_message = $3,
            updated_at = now()
        WHERE id = $1::uuid
        RETURNING id
        "#,
    )
    .bind(id)
    .bind(&patch.status)
    .bind(&patch.error_message)
    .fetch_optional(&mut *tx)
    .await?;

    if let Some(event_id) = event_id
        && (patch.status == "published" || patch.status == "failed")
    {
        sqlx::query(
            r#"
            DELETE FROM outbox_delivery_queue
            WHERE event_id = $1::uuid
            "#,
        )
        .bind(event_id)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    select_outbox_event_by_id(db, id).await
}

/// Deletes an outbox event by primary key.
pub(crate) async fn delete_outbox_event_by_id(db: &PgPool, id: Uuid) -> anyhow::Result<u64> {
    let result = sqlx::query(
        r#"
        DELETE FROM outbox_events
        WHERE id = $1::uuid
        "#,
    )
    .bind(id)
    .execute(db)
    .await?;

    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::*;
    use crate::models::outbox_event::OutboxEventDraft;

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx outbox tests"]
    async fn count_publishable_outbox_backlog_tracks_available_delivery_rows(pool: PgPool) {
        insert_outbox_event(&pool, &draft("first")).await.unwrap();
        insert_outbox_event(&pool, &draft("second")).await.unwrap();

        assert_eq!(count_publishable_outbox_backlog(&pool).await.unwrap(), 2);

        let claimed = claim_publishable_outbox_events(&pool, 1, 60).await.unwrap();

        assert_eq!(claimed.len(), 1);
        assert_eq!(count_publishable_outbox_backlog(&pool).await.unwrap(), 1);
    }

    fn draft(label: &str) -> OutboxEventDraft {
        OutboxEventDraft {
            event_type: "test.event".to_string(),
            aggregate_type: "test".to_string(),
            aggregate_id: Uuid::now_v7(),
            dedupe_key: format!("test:{label}"),
        }
    }
}
