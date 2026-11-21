//! Restricted publisher integration: mixed ownership, delivery fencing and cleanup.

use std::sync::Mutex;

use futures_util::FutureExt;
use sqlx::PgPool;
use uuid::Uuid;

use super::*;

struct Publisher {
    restricted: PgPool,
    owner: PgPool,
    seen: Mutex<Vec<Uuid>>,
}

#[async_trait]
impl EventPublisher for Publisher {
    async fn publish(&self, event: &OutboxEvent) -> anyhow::Result<()> {
        // A single-connection pool proves that no claim/bookkeeping transaction
        // spans broker I/O. Neither prior user nor prior work scope may survive.
        let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM outbox_events")
            .fetch_one(&self.restricted)
            .await?;
        anyhow::ensure!(visible == 0, "publication leaked pooled authority");
        self.seen.lock().unwrap().push(event.id);
        if event.event_type == "fail" {
            anyhow::bail!("broker failure with 'quotes' and Unicode λ");
        }
        if event.event_type == "stale" {
            sqlx::query(
                "UPDATE outbox_delivery_queue SET claim_token=gen_random_uuid() WHERE event_id=$1",
            )
            .bind(event.id)
            .execute(&self.owner)
            .await?;
        }
        Ok(())
    }
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn publication_preserves_claimed_ownership_and_retry_fencing(pool: PgPool) {
    let tenant = Uuid::now_v7();
    let mut ids = Vec::new();
    for (kind, owner) in [
        ("ok", tenant),
        ("ok", tenant),
        ("fail", Uuid::now_v7()),
        ("stale", Uuid::now_v7()),
    ] {
        let id: Uuid = sqlx::query_scalar("INSERT INTO outbox_events(tenant_id,event_type,aggregate_type,aggregate_id,dedupe_key,payload) VALUES($1,$2,'run',$3,$3::text,$4) RETURNING id")
            .bind(owner).bind(kind).bind(Uuid::now_v7())
            .bind(serde_json::json!({"tenant_id":"untrusted", "run_id":"untrusted"}))
            .fetch_one(&pool).await.unwrap();
        ids.push(id);
    }
    let role = format!("publication_scope_{}", Uuid::now_v7().simple());
    sqlx::raw_sql(&format!("CREATE ROLE {role} LOGIN PASSWORD 'fixture'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION vigilo_claim_outbox(bigint,integer),vigilo_outbox_backlog() TO {role}; GRANT SELECT,UPDATE ON outbox_events TO {role}; GRANT SELECT,UPDATE,DELETE ON outbox_delivery_queue TO {role}; ALTER TABLE outbox_events ENABLE ROW LEVEL SECURITY; ALTER TABLE outbox_delivery_queue ENABLE ROW LEVEL SECURITY;
    CREATE POLICY scope ON outbox_events TO {role} USING(tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND aggregate_id=NULLIF(current_setting('vigilo.run_id',true),'')::uuid AND current_setting('vigilo.actor_id',true)='' AND COALESCE(current_setting('vigilo.run_batch',true),'')='');
    CREATE POLICY scope ON outbox_delivery_queue TO {role} USING(EXISTS(SELECT 1 FROM outbox_events e WHERE e.id=event_id AND e.tenant_id=outbox_delivery_queue.tenant_id));"))
        .execute(&pool).await.unwrap();
    let restricted = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|connection, _| Box::pin(async move {
            sqlx::query("SELECT set_config('vigilo.tenant_id','',false),set_config('vigilo.run_id','',false),set_config('vigilo.actor_id','',false),set_config('vigilo.run_batch','',false)")
                .execute(connection).await?;
            Ok(())
        }))
        .connect_with(
            pool.connect_options()
                .as_ref()
                .clone()
                .username(&role)
                .password("fixture"),
        )
        .await
        .unwrap();
    let result = std::panic::AssertUnwindSafe(async {
        let publisher = Publisher {
            restricted: restricted.clone(),
            owner: pool.clone(),
            seen: Mutex::new(Vec::new()),
        };
        let config = OutboxPublisherConfig {
            batch_size: 4,
            publish_parallelism: 4,
            retry_delay_seconds: 60,
            ..Default::default()
        };
        let (stats, calls) = crate::db::workflows::run_shard_summary::test_support::record_queries(tokio::time::timeout(
            std::time::Duration::from_secs(15),
            publish_pending_events(&restricted, &publisher, &config),
        )).await;
        let stats = stats.unwrap().unwrap();
        assert_eq!(calls.iter().filter(|s| s.contains("vigilo_claim_outbox(")).count(),1);
        assert_eq!(calls.iter().filter(|s| s.contains("SET LOCAL vigilo.tenant_id")).count(),4);
        assert_eq!(calls.iter().filter(|s| s.contains("WITH deleted AS") || s.contains("WITH rescheduled AS")).count(),4);
        assert_eq!(stats.claimed_events, 4);
        assert_eq!(stats.published_events, 2);
        assert_eq!(stats.failed_events, 1);
        assert_eq!(stats.stale_event_claims, 1);
        assert_eq!(publisher.seen.lock().unwrap().len(), 4);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM outbox_delivery_queue")
                .fetch_one(&pool)
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM outbox_events WHERE status='published'"
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            2
        );
        let failed = crate::db::tables::outbox_events::select_outbox_event_by_id(&pool, ids[2])
            .await
            .unwrap()
            .unwrap();
        assert!(failed.claim_token.is_none());
        assert_eq!(
            failed.error_message.as_deref(),
            Some("broker failure with 'quotes' and Unicode λ")
        );
        assert_eq!(failed.publish_attempt_count, Some(1));
        assert_eq!(
            crate::db::tables::outbox_events::count_publishable_outbox_backlog(&restricted)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM outbox_events")
                .fetch_one(&restricted)
                .await
                .unwrap(),
            0
        );

        // Reclaim delayed work, preserving exact ledger ownership rather than
        // trusting the forged identity fields in the payload.
        sqlx::query("UPDATE outbox_delivery_queue SET available_at=now()-interval '1 second'")
            .execute(&pool).await.unwrap();
        let (claimed, calls) = crate::db::workflows::run_shard_summary::test_support::record_queries(
            outbox_events::claim_publishable_outbox_events(&restricted, 4, 60)).await;
        let claimed=claimed.unwrap();
        assert_eq!(calls.len(),1);
        assert_eq!(claimed.len(),2);
        let event=claimed.iter().find(|e| e.id==ids[2]).unwrap();
        let token=event.claim_token.unwrap();
        { let mut wrong=event.clone(); wrong.tenant_id=Uuid::now_v7();
            assert_eq!(outbox_events::mark_outbox_event_published(&restricted,&wrong,token).await.unwrap(),0);
        }
        let mut wrong=event.clone(); wrong.aggregate_id=Uuid::now_v7();
        assert_eq!(outbox_events::mark_outbox_event_published(&restricted,&wrong,token).await.unwrap(),0);
        assert_eq!(outbox_events::reschedule_outbox_event(&restricted,&wrong,token,1,"wrong aggregate").await.unwrap(),0);
        assert_eq!(outbox_events::reschedule_outbox_event(&restricted,event,Uuid::now_v7(),1,"stale").await.unwrap(),0);
        // Force the ledger write to fail after the delivery deletion. The whole
        // statement and transaction must roll back before the pool is reused.
        sqlx::raw_sql("CREATE FUNCTION publication_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected publication bookkeeping failure'; END $$; CREATE TRIGGER publication_failure BEFORE UPDATE ON outbox_events FOR EACH ROW EXECUTE FUNCTION publication_failure();")
            .execute(&pool).await.unwrap();
        let error=outbox_events::mark_outbox_event_published(&restricted,event,token).await.unwrap_err();
        assert!(error.to_string().contains("injected publication bookkeeping failure"));
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM outbox_delivery_queue WHERE event_id=$1 AND claim_token=$2").bind(event.id).bind(token).fetch_one(&pool).await.unwrap(),1);
        sqlx::raw_sql("DROP TRIGGER publication_failure ON outbox_events; DROP FUNCTION publication_failure();").execute(&pool).await.unwrap();

        // Cancelling a blocked bookkeeping future rolls back its installed scope.
        let mut lock=pool.begin().await.unwrap();
        sqlx::query("SELECT event_id FROM outbox_delivery_queue WHERE event_id=$1 FOR UPDATE")
            .bind(event.id).execute(&mut *lock).await.unwrap();
        assert!(tokio::time::timeout(std::time::Duration::from_millis(150),
            outbox_events::mark_outbox_event_published(&restricted,event,token)).await.is_err());
        lock.rollback().await.unwrap();
        let (marked,calls)=crate::db::workflows::run_shard_summary::test_support::record_queries(
            outbox_events::mark_outbox_event_published(&restricted,event,token)).await;
        assert_eq!(marked.unwrap(),1); assert_eq!(calls.len(),2);
        assert_eq!(outbox_events::mark_outbox_event_published(&restricted,event,token).await.unwrap(),0);
        let other=claimed.iter().find(|e| e.id!=event.id).unwrap();
        let (retried,calls)=crate::db::workflows::run_shard_summary::test_support::record_queries(
            outbox_events::reschedule_outbox_event(&restricted,other,other.claim_token.unwrap(),0,"retry")).await;
        assert_eq!(retried.unwrap(),1); assert_eq!(calls.len(),2);
        sqlx::query("UPDATE outbox_events SET event_type='ok' WHERE id=$1").bind(other.id).execute(&pool).await.unwrap();
        let retried=publish_pending_events(&restricted,&publisher,&config).await.unwrap();
        assert_eq!(retried.claimed_events,1); assert_eq!(retried.published_events,1);
        assert_eq!(publish_pending_events(&restricted,&publisher,&config).await.unwrap().claimed_events,0);
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM outbox_delivery_queue").fetch_one(&pool).await.unwrap(),0);
        let settings: (String,String,String,String)=sqlx::query_as("SELECT COALESCE(current_setting('vigilo.tenant_id',true),''),COALESCE(current_setting('vigilo.run_id',true),''),COALESCE(current_setting('vigilo.actor_id',true),''),COALESCE(current_setting('vigilo.run_batch',true),'')")
            .fetch_one(&restricted).await.unwrap();
        assert_eq!(settings,(String::new(),String::new(),String::new(),String::new()));
    })
    .catch_unwind()
    .await;
    restricted.close().await;
    sqlx::raw_sql(&format!("DROP OWNED BY {role}; DROP ROLE {role};"))
        .execute(&pool)
        .await
        .unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
