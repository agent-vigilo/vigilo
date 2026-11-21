//! Outbox event table access.
//!
//! `outbox_events` is the durable event ledger. `outbox_delivery_queue` is the
//! hot claim/retry table scanned by publishers. Successful publication updates
//! the ledger and deletes the delivery row so the hot queue only contains
//! unfinished work. Restricted claims carry ledger ownership; completion/retry
//! transactions bind that identity, clear other caller context, and atomically
//! change the ledger and delivery row. No transaction spans broker I/O.

use sqlx::PgPool;
use uuid::Uuid;

use crate::models::outbox_event::{
    OutboxEvent,
    OutboxEventDraft,
    OutboxEventPatch,
};

const OUTBOX_EVENT_SELECT: &str = r#"
    SELECT
        e.tenant_id,
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
    let events = sqlx::query_as::<_, OutboxEvent>("SELECT * FROM vigilo_claim_outbox($1,$2)")
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
    let count = sqlx::query_scalar::<_, i64>("SELECT vigilo_outbox_backlog()")
        .fetch_one(db)
        .await?;

    Ok(count)
}

/// Marks an outbox event as successfully published and removes delivery work.
pub(crate) async fn mark_outbox_event_published(
    db: &PgPool,
    event: &OutboxEvent,
    claim_token: Uuid,
) -> anyhow::Result<u64> {
    let mut tx = db.begin().await?;
    crate::db::scope::set_system(&mut tx, event.tenant_id, event.aggregate_id).await?;
    let result = sqlx::query(
        r#"
        WITH deleted AS (
            DELETE FROM outbox_delivery_queue
            WHERE event_id = $1::uuid
              AND claim_token = $2::uuid
              AND tenant_id = $3::uuid
              AND EXISTS (SELECT 1 FROM outbox_events e WHERE e.id=$1::uuid AND e.tenant_id=$3::uuid AND e.aggregate_id=$4::uuid)
            RETURNING event_id
        )
        UPDATE outbox_events e
        SET status = 'published'::outbox_status,
            published_at = statement_timestamp(),
            error_message = NULL,
            updated_at = statement_timestamp()
        FROM deleted
        WHERE e.id = deleted.event_id
        "#,
    )
    .bind(event.id)
    .bind(claim_token)
    .bind(event.tenant_id)
    .bind(event.aggregate_id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(result.rows_affected())
}

/// Reschedules a failed publish attempt for later retry.
pub(crate) async fn reschedule_outbox_event(
    db: &PgPool,
    event: &OutboxEvent,
    claim_token: Uuid,
    retry_after_seconds: i32,
    error_message: &str,
) -> anyhow::Result<u64> {
    let mut tx = db.begin().await?;
    crate::db::scope::set_system(&mut tx, event.tenant_id, event.aggregate_id).await?;
    let result = sqlx::query(
        r#"
        WITH rescheduled AS (
            UPDATE outbox_delivery_queue q
            SET available_at = statement_timestamp() + ($3::int * interval '1 second'),
                claim_token = NULL,
                claimed_until = NULL,
                error_message = $4,
                updated_at = statement_timestamp()
            WHERE q.event_id = $1::uuid
              AND q.claim_token = $2::uuid
              AND q.tenant_id = $5::uuid
              AND EXISTS (SELECT 1 FROM outbox_events e WHERE e.id=$1::uuid AND e.tenant_id=$5::uuid AND e.aggregate_id=$6::uuid)
            RETURNING q.event_id
        )
        UPDATE outbox_events e
        SET error_message = $4,
            updated_at = statement_timestamp()
        FROM rescheduled
        WHERE e.id = rescheduled.event_id
        "#,
    )
    .bind(event.id)
    .bind(claim_token)
    .bind(retry_after_seconds)
    .bind(error_message)
    .bind(event.tenant_id)
    .bind(event.aggregate_id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
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

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires PostgreSQL"]
    async fn restricted_outbox_claim_preserves_locks_payloads_and_rollback(pool: PgPool) {
        let event = insert_outbox_event(&pool, &draft("restricted"))
            .await
            .unwrap();
        let payload = serde_json::json!({"run_id":"opaque","nested":{"value":42}});
        sqlx::query("UPDATE outbox_events SET payload=$2 WHERE id=$1")
            .bind(event.id)
            .bind(&payload)
            .execute(&pool)
            .await
            .unwrap();
        let role = format!("outbox_caller_{}", Uuid::new_v4().simple());
        let owner = format!("outbox_owner_{}", Uuid::new_v4().simple());
        let mut tx = pool.begin().await.unwrap();
        sqlx::raw_sql(&format!("CREATE ROLE {role} NOLOGIN; GRANT USAGE ON SCHEMA public TO {role}; SET LOCAL ROLE {role};"))
            .execute(&mut *tx).await.unwrap();
        sqlx::raw_sql("DO $$ BEGIN BEGIN PERFORM * FROM public.vigilo_claim_outbox(1,60); RAISE EXCEPTION 'unexpected access'; EXCEPTION WHEN insufficient_privilege THEN NULL; END; END $$;")
            .execute(&mut *tx).await.unwrap();
        sqlx::raw_sql(&format!(
            "RESET ROLE; CREATE ROLE {owner} NOLOGIN;
            GRANT USAGE ON SCHEMA public TO {owner};
            GRANT SELECT ON public.outbox_events, public.outbox_delivery_queue TO {owner};
            GRANT UPDATE ON public.outbox_delivery_queue TO {owner};
            ALTER FUNCTION public.vigilo_claim_outbox(bigint,integer) OWNER TO {owner};
            GRANT EXECUTE ON FUNCTION public.vigilo_claim_outbox(bigint,integer) TO {role};
            SET LOCAL ROLE {role};"
        ))
        .execute(&mut *tx)
        .await
        .unwrap();
        let access: bool = sqlx::query_scalar("SELECT has_table_privilege(current_user,'public.outbox_events','SELECT') OR has_table_privilege(current_user,'public.outbox_delivery_queue','SELECT') OR has_table_privilege(current_user,'public.outbox_delivery_queue','UPDATE')")
            .fetch_one(&mut *tx).await.unwrap();
        assert!(!access);
        let (rows, calls) = crate::db::workflows::run_shard_summary::test_support::record_queries(
            sqlx::query_as::<_, OutboxEvent>("SELECT * FROM public.vigilo_claim_outbox(1,60)")
                .fetch_all(&mut *tx),
        )
        .await;
        let rows = rows.unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, event.id);
        assert_eq!(rows[0].payload, payload);
        assert_eq!(rows[0].event_type, event.event_type);
        assert_eq!(rows[0].dedupe_key, event.dedupe_key);
        assert_eq!(rows[0].publish_attempt_count, Some(1));
        assert_eq!(rows[0].claimed_until, rows[0].available_at);
        assert!(rows[0].claim_token.is_some());
        // A competing publisher skips the row lock instead of waiting.
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_secs(10),
                claim_publishable_outbox_events(&pool, 1, 60)
            )
            .await
            .unwrap()
            .unwrap()
            .is_empty()
        );
        tx.rollback().await.unwrap();
        let retry = claim_publishable_outbox_events(&pool, 1, 60).await.unwrap();
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].publish_attempt_count, Some(1));
        assert_ne!(retry[0].claim_token, rows[0].claim_token);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires PostgreSQL"]
    async fn outbox_claim_bounds_order_retries_and_stale_tokens(pool: PgPool) {
        let first = insert_outbox_event(&pool, &draft("oldest")).await.unwrap();
        let second = insert_outbox_event(&pool, &draft("next")).await.unwrap();
        let delayed = insert_outbox_event(&pool, &draft("delayed")).await.unwrap();
        sqlx::query("UPDATE outbox_delivery_queue SET available_at=CASE event_id WHEN $1 THEN now()-interval '2 minutes' WHEN $2 THEN now()-interval '1 minute' ELSE now()+interval '1 hour' END")
            .bind(first.id).bind(second.id).execute(&pool).await.unwrap();
        let claimed = claim_publishable_outbox_events(&pool, 1, 60).await.unwrap();
        assert_eq!(claimed[0].id, first.id);
        let old_token = claimed[0].claim_token.unwrap();
        let next = claim_publishable_outbox_events(&pool, 10, 60)
            .await
            .unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].id, second.id);
        assert!(
            claim_publishable_outbox_events(&pool, 10, 60)
                .await
                .unwrap()
                .is_empty()
        );
        sqlx::query("UPDATE outbox_delivery_queue SET available_at=now()-interval '1 second', claimed_until=now()-interval '1 second' WHERE event_id=$1")
            .bind(first.id).execute(&pool).await.unwrap();
        let reclaimed = claim_publishable_outbox_events(&pool, 10, 60)
            .await
            .unwrap();
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].id, first.id);
        assert_eq!(reclaimed[0].publish_attempt_count, Some(2));
        let current = reclaimed[0].claim_token.unwrap();
        assert_ne!(old_token, current);
        assert_eq!(
            mark_outbox_event_published(&pool, &first, old_token)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            reschedule_outbox_event(&pool, &first, old_token, 60, "stale")
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            reschedule_outbox_event(&pool, &first, current, 60, "retry")
                .await
                .unwrap(),
            1
        );
        assert!(
            claim_publishable_outbox_events(&pool, 10, 60)
                .await
                .unwrap()
                .is_empty()
        );
        sqlx::query("UPDATE outbox_delivery_queue SET available_at=now()-interval '1 second' WHERE event_id=$1")
            .bind(first.id).execute(&pool).await.unwrap();
        let retry = claim_publishable_outbox_events(&pool, 10, 60)
            .await
            .unwrap();
        assert_eq!(retry[0].error_message.as_deref(), Some("retry"));
        assert_eq!(retry[0].publish_attempt_count, Some(3));
        assert_eq!(
            mark_outbox_event_published(&pool, &first, retry[0].claim_token.unwrap())
                .await
                .unwrap(),
            1
        );
        assert!(
            claim_publishable_outbox_events(&pool, 10, 60)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            select_outbox_event_by_id(&pool, first.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            "published"
        );
        assert_eq!(
            select_outbox_event_by_id(&pool, delayed.id)
                .await
                .unwrap()
                .unwrap()
                .publish_attempt_count,
            Some(0)
        );
        for (limit, lease) in [
            (None, Some(60)),
            (Some(0_i64), Some(60)),
            (Some(1000001), Some(60)),
            (Some(1), None),
            (Some(1), Some(0)),
            (Some(1), Some(86401)),
        ] {
            let error = sqlx::query("SELECT * FROM vigilo_claim_outbox($1::bigint,$2::integer)")
                .bind(limit)
                .bind(lease)
                .execute(&pool)
                .await
                .unwrap_err();
            assert_eq!(
                error.as_database_error().unwrap().code().as_deref(),
                Some("22023")
            );
        }
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
