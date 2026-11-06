//! Restricted delivery scope, revision fencing, and connection reuse contracts.
use futures_util::FutureExt;

use super::*;
use crate::db::workflows::run_shard_summary::test_support::record_queries;

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn delivery_followups_use_claimed_identity(pool: PgPool) {
    let (run, _) = seed(&pool).await;
    let (other, _) = seed(&pool).await;
    let tenant = Uuid::now_v7();
    let tenant_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(move |c, _| {
            Box::pin(async move {
                sqlx::query("SELECT set_config('vigilo.tenant_id',$1,false)")
                    .bind(tenant.to_string())
                    .execute(c)
                    .await?;
                Ok(())
            })
        })
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    let (third, _) = seed(&tenant_pool).await;
    request(&pool, run, ExecutionControl::Paused).await.unwrap();
    request(&tenant_pool, third, ExecutionControl::Paused)
        .await
        .unwrap();
    tenant_pool.close().await;
    sqlx::query("UPDATE run_chunks SET status='leased',lease_token=gen_random_uuid(),leased_until=clock_timestamp()+interval '1 hour' WHERE run_id=$1").bind(run).execute(&pool).await.unwrap();
    let role = format!("control_scope_{}", Uuid::now_v7().simple());
    sqlx::raw_sql(&format!("CREATE ROLE {role} LOGIN PASSWORD 'fixture'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION vigilo_claim_control_deliveries(uuid) TO {role};")).execute(&pool).await.unwrap();
    for (table, key) in [
        ("runs", "id"),
        ("run_chunks", "run_id"),
        ("run_control_deliveries", "run_id"),
        ("shard_move_captures", "run_id"),
    ] {
        sqlx::raw_sql(&format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY; CREATE POLICY work_scope ON {table} TO {role} USING(tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND {key}=NULLIF(current_setting('vigilo.run_id',true),'')::uuid AND current_setting('vigilo.actor_id',true)=''); GRANT SELECT,UPDATE ON {table} TO {role};")).execute(&pool).await.unwrap();
    }
    sqlx::raw_sql(&format!("ALTER TABLE shard_move_dirty_keys ENABLE ROW LEVEL SECURITY; CREATE POLICY work_scope ON shard_move_dirty_keys TO {role} USING(tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND EXISTS(SELECT 1 FROM shard_move_captures c WHERE c.move_id=shard_move_dirty_keys.move_id)); GRANT SELECT,INSERT,UPDATE ON shard_move_dirty_keys TO {role};")).execute(&pool).await.unwrap();
    let restricted = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
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
        for expected in [run, third] {
            let claimed: Delivery =
                sqlx::query_as("SELECT * FROM vigilo_claim_control_deliveries($1)")
                    .bind(expected)
                    .fetch_one(&restricted)
                    .await
                    .unwrap();
            assert_eq!(
                claimed.tenant_id,
                if expected == run { Uuid::nil() } else { tenant }
            );
            let (quiet, queries) = record_queries(observe_quiescence(&restricted, &claimed)).await;
            assert_eq!(
                quiet.unwrap(),
                expected != run,
                "hidden leased chunks must not appear quiescent"
            );
            assert_eq!(queries.len(), 1);
            let (quiet, queries) = record_queries(apply(&restricted, &claimed)).await;
            assert_eq!(quiet.unwrap(), expected != run);
            assert_eq!(queries.len(), 4);
            let state: ExecutionControl =
                sqlx::query_scalar("SELECT execution_control FROM run_chunks WHERE run_id=$1")
                    .bind(expected)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(state, ExecutionControl::Paused);
            let (ack, queries) =
                record_queries(acknowledge(&restricted, &claimed, expected != run)).await;
            assert!(ack.unwrap());
            assert_eq!(queries.len(), 2);
            assert!(
                !acknowledge(&restricted, &claimed, true).await.unwrap(),
                "old token cannot reacknowledge"
            );
            let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM runs")
                .fetch_one(&restricted)
                .await
                .unwrap();
            assert_eq!(
                visible, 0,
                "transaction context cannot leak into pooled reads"
            );
        }
        request(&pool, run, ExecutionControl::Enabled)
            .await
            .unwrap();
        let first: Delivery = sqlx::query_as("SELECT * FROM vigilo_claim_control_deliveries($1)")
            .bind(run)
            .fetch_one(&restricted)
            .await
            .unwrap();
        let (deferred, queries) = record_queries(defer(&restricted, &first)).await;
        assert!(deferred.unwrap());
        assert_eq!(queries.len(), 2);
        let stored: (bool, String) = sqlx::query_as(
            "SELECT claim_token IS NULL,last_error FROM run_control_deliveries WHERE run_id=$1",
        )
        .bind(run)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            stored,
            (true, "placement control delivery unavailable".into())
        );
        sqlx::query(
            "UPDATE run_control_deliveries SET available_at=clock_timestamp() WHERE run_id=$1",
        )
        .bind(run)
        .execute(&pool)
        .await
        .unwrap();
        let retry: Delivery = sqlx::query_as("SELECT * FROM vigilo_claim_control_deliveries($1)")
            .bind(run)
            .fetch_one(&restricted)
            .await
            .unwrap();
        assert_ne!(retry.claim_token, first.claim_token);
        assert!(!defer(&restricted, &first).await.unwrap());
        assert!(!acknowledge(&restricted, &first, true).await.unwrap());
        // Same revision, conflicting state fails without changing work or leaking context.
        assert!(
            apply(&restricted, &delivery(run, 2, ExecutionControl::Paused))
                .await
                .is_err()
        );
        // A failure after the root update must roll back that update too.
        sqlx::raw_sql(&format!("REVOKE UPDATE ON run_chunks FROM {role}"))
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            apply(&restricted, &delivery(run, 3, ExecutionControl::Paused))
                .await
                .is_err()
        );
        let revision: i64 = sqlx::query_scalar("SELECT control_revision FROM runs WHERE id=$1")
            .bind(run)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(revision, 2);
        sqlx::raw_sql(&format!("GRANT UPDATE ON run_chunks TO {role}"))
            .execute(&pool)
            .await
            .unwrap();
        let mut tx = retry.begin(&restricted).await.unwrap();
        let visible: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM runs")
            .fetch_all(&mut *tx)
            .await
            .unwrap();
        assert_eq!(visible, vec![run]);
        let changed = sqlx::query("UPDATE runs SET error_message='forbidden' WHERE id<>$1")
            .bind(run)
            .execute(&mut *tx)
            .await
            .unwrap()
            .rows_affected();
        assert_eq!(changed, 0);
        sqlx::query("UPDATE run_control_deliveries SET last_error='rollback' WHERE run_id=$1")
            .bind(run)
            .execute(&mut *tx)
            .await
            .unwrap();
        let timeout = tokio::time::timeout(std::time::Duration::from_millis(10), async move {
            let _held = tx;
            std::future::pending::<()>().await;
        })
        .await;
        assert!(timeout.is_err());
        // Reacquisition waits for SQLx rollback and proves context cleanup.
        let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM runs")
            .fetch_one(&restricted)
            .await
            .unwrap();
        assert_eq!(visible, 0);
        let error: Option<String> =
            sqlx::query_scalar("SELECT last_error FROM run_control_deliveries WHERE run_id=$1")
                .bind(run)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_ne!(error.as_deref(), Some("rollback"));
        assert!(!apply(&restricted, &retry).await.unwrap());
        assert!(acknowledge(&restricted, &retry, false).await.unwrap());
        assert!(
            apply(
                &restricted,
                &delivery(Uuid::now_v7(), 1, ExecutionControl::Paused)
            )
            .await
            .unwrap(),
            "absent local work remains quiescent"
        );
        let state: ExecutionControl =
            sqlx::query_scalar("SELECT execution_control FROM run_chunks WHERE run_id=$1")
                .bind(other)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(state, ExecutionControl::Enabled);
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
