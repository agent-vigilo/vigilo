//! Fixed-role admission scope, lease fencing and pooled cleanup regressions.
use futures_util::FutureExt;

use super::*;

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn admission_followups_use_claimed_scope_and_preserve_fences(db: PgPool) {
    let f = Fixture::new(&db).await;
    let preparation = claim(&db, f.plan.run_id).await.unwrap();
    f.prepare(&db, preparation).await.unwrap();
    let plan = load(&db, f.plan.run_id).await.unwrap().unwrap();
    admission::accept(&db, plan.run_id, plan.manifest.as_ref().unwrap())
        .await
        .unwrap();
    let other = Fixture::new(&db).await;
    let other_tenant = Uuid::now_v7();
    let tenant_db = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(move |c, _| {
            Box::pin(async move {
                sqlx::query("SELECT set_config('vigilo.tenant_id',$1,false)")
                    .bind(other_tenant.to_string())
                    .execute(c)
                    .await?;
                Ok(())
            })
        })
        .connect_with(db.connect_options().as_ref().clone())
        .await
        .unwrap();
    let second = Fixture::new(&tenant_db).await;
    let preparation = claim(&tenant_db, second.plan.run_id).await.unwrap();
    assert_eq!(preparation.tenant_id, other_tenant);
    second.prepare(&tenant_db, preparation).await.unwrap();
    let second_plan = load(&tenant_db, second.plan.run_id).await.unwrap().unwrap();
    admission::accept(
        &tenant_db,
        second_plan.run_id,
        second_plan.manifest.as_ref().unwrap(),
    )
    .await
    .unwrap();
    tenant_db.close().await;
    let role = format!("admission_scope_{}", Uuid::now_v7().simple());
    let owner = format!("admission_scope_owner_{}", Uuid::now_v7().simple());
    sqlx::raw_sql(&format!("CREATE ROLE {role} LOGIN PASSWORD 'fixture'; CREATE ROLE {owner} NOLOGIN; GRANT USAGE ON SCHEMA public TO {role},{owner}; GRANT SELECT ON database_placements TO {role}; GRANT SELECT,UPDATE ON runs,run_plans,database_placements TO {owner}; ALTER FUNCTION vigilo_lock_database_placement(text) OWNER TO {owner}; ALTER FUNCTION vigilo_claim_admission(uuid,uuid) OWNER TO {owner}; GRANT EXECUTE ON FUNCTION vigilo_claim_admission(uuid,uuid) TO {role};")).execute(&db).await.unwrap();
    for (table, key) in [
        ("runs", "id"),
        ("run_plans", "run_id"),
        ("run_plan_shards", "run_id"),
        ("shard_placements", "run_id"),
        ("run_shard_dispatch_cursors", "run_id"),
        ("run_chunks", "run_id"),
        ("run_snapshots", "run_id"),
        ("local_shard_admissions", "run_id"),
        ("shard_move_captures", "run_id"),
    ] {
        sqlx::raw_sql(&format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY; CREATE POLICY work_scope ON {table} TO {role} USING(tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND {key}=NULLIF(current_setting('vigilo.run_id',true),'')::uuid AND current_setting('vigilo.actor_id',true)=''); CREATE POLICY owner_scope ON {table} TO {owner} USING(true); GRANT SELECT,INSERT,UPDATE ON {table} TO {role};")).execute(&db).await.unwrap();
    }
    sqlx::raw_sql(&format!("ALTER TABLE dataset_versions ENABLE ROW LEVEL SECURITY; CREATE POLICY work_scope ON dataset_versions TO {role} USING(tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid); GRANT SELECT,INSERT,UPDATE ON dataset_versions TO {role};")).execute(&db).await.unwrap();
    sqlx::raw_sql(&format!("ALTER TABLE shard_move_dirty_keys ENABLE ROW LEVEL SECURITY; CREATE POLICY work_scope ON shard_move_dirty_keys TO {role} USING(tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND EXISTS(SELECT 1 FROM shard_move_captures c WHERE c.move_id=shard_move_dirty_keys.move_id)); GRANT SELECT,INSERT,UPDATE ON shard_move_dirty_keys TO {role};")).execute(&db).await.unwrap();
    let restricted=sqlx::postgres::PgPoolOptions::new().max_connections(1).after_connect(|c,_|Box::pin(async move {
        sqlx::query("SELECT set_config('vigilo.tenant_id','',false),set_config('vigilo.run_id','',false),set_config('vigilo.actor_id','',false)").execute(c).await?; Ok(())
    })).connect_with(db.connect_options().as_ref().clone().username(&role).password("fixture")).await.unwrap();
    let result = std::panic::AssertUnwindSafe(async {
        let lease = admission::claim(&restricted, plan.run_id).await.unwrap();
        let (renewed, queries) =
            crate::db::workflows::run_shard_summary::test_support::record_queries(
                admission::renew(&restricted, lease),
            )
            .await;
        renewed.expect("renew must bind authoritative claim identity");
        assert_eq!(lease.tenant_id, Uuid::nil());
        assert_eq!(
            admission::load(&restricted, lease)
                .await
                .unwrap()
                .unwrap()
                .run_id,
            plan.run_id
        );
        assert_eq!(
            admission::draft(&restricted, lease)
                .await
                .unwrap()
                .profile_hash,
            f.draft.profile_hash
        );
        let entered = std::sync::atomic::AtomicBool::new(false);
        let timeout = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            let mut tx = restricted.begin().await.unwrap();
            lease.lock(&mut tx).await.unwrap();
            let visible: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM runs")
                .fetch_all(&mut *tx)
                .await
                .unwrap();
            assert_eq!(visible, vec![plan.run_id]);
            let changed = sqlx::query("UPDATE runs SET error_message='forbidden' WHERE id=ANY($1)")
                .bind(vec![other.plan.run_id, second_plan.run_id])
                .execute(&mut *tx)
                .await
                .unwrap()
                .rows_affected();
            assert_eq!(changed, 0);
            sqlx::query("UPDATE runs SET error_message='rollback' WHERE id=$1")
                .bind(plan.run_id)
                .execute(&mut *tx)
                .await
                .unwrap();
            entered.store(true, std::sync::atomic::Ordering::Relaxed);
            std::future::pending::<()>().await;
        })
        .await;
        assert!(timeout.is_err());
        assert!(entered.load(std::sync::atomic::Ordering::Relaxed));
        let error: Option<String> =
            sqlx::query_scalar("SELECT error_message FROM runs WHERE id=$1")
                .bind(plan.run_id)
                .fetch_one(&db)
                .await
                .unwrap();
        assert!(
            error.is_none(),
            "dropping the transaction rolls back pending writes"
        );
        assert_eq!(
            queries.len(),
            2,
            "renew keeps its lock plus fenced update: {queries:?}"
        );
        assert!(
            admission::activate(&restricted, lease, &std::collections::BTreeMap::new())
                .await
                .is_err()
        );
        release(&restricted, lease, None).await.unwrap();
        let next = admission::claim(&restricted, plan.run_id).await.unwrap();
        assert!(next.generation > lease.generation);
        assert!(admission::renew(&restricted, lease).await.is_err());
        assert!(admission::fail(&restricted, lease, "stale").await.is_err());
        release(&restricted, lease, Some("stale release"))
            .await
            .unwrap();
        admission::renew(&restricted, next).await.unwrap();
        admission::fail(&restricted, next, "expected failure")
            .await
            .unwrap();
        let status: (String, Option<String>) =
            sqlx::query_as("SELECT status::text,error_message FROM runs WHERE id=$1")
                .bind(plan.run_id)
                .fetch_one(&db)
                .await
                .unwrap();
        assert_eq!(status, ("failed".into(), Some("expected failure".into())));
        // Reuse the same physical restricted connection for a different owner.
        let lease = admission::claim(&restricted, second_plan.run_id)
            .await
            .unwrap();
        assert_eq!(lease.tenant_id, other_tenant);
        let (loaded, statements) =
            crate::db::workflows::run_shard_summary::test_support::record_queries(admission::load(
                &restricted,
                lease,
            ))
            .await;
        assert_eq!(loaded.unwrap().unwrap().run_id, second_plan.run_id);
        assert_eq!(statements.len(), 1, "scoped load uses one data submission");
        let chunks = (0..3)
            .map(|index| crate::models::run_chunk::RunChunkDraft {
                chunk_id: identity::uuid_from_json(
                    &json!({"run_id":lease.run_id,"chunk_index":index}),
                )
                .unwrap(),
                run_shard: index as i16,
                ordinal_start: index * 100,
                ordinal_end: ((index + 1) * 100).min(205),
                profile_group_id: "default".into(),
            })
            .collect::<Vec<_>>();
        let mut expected = std::collections::BTreeMap::<
            i16,
            crate::db::workflows::run_shard_summary::plan::Manifest,
        >::new();
        for c in &chunks {
            expected
                .entry(c.run_shard)
                .or_default()
                .push(
                    &crate::db::workflows::run_shard_summary::plan::ChunkIdentity {
                        chunk_id: c.chunk_id,
                        run_shard: c.run_shard,
                        ordinal_start: c.ordinal_start,
                        ordinal_end: c.ordinal_end,
                    },
                )
                .unwrap();
        }
        let gates = expected.keys().map(|s| (*s, json!({}))).collect();
        let draft = admission::draft(&restricted, lease).await.unwrap();
        let seed = admission::placement::Seed {
            run_id: lease.run_id,
            draft: &draft,
            snapshot: &draft.config_snapshot,
            expected: &expected,
            gate_counts: &gates,
        };
        for _ in 0..2 {
            let mut tx = admission::placement::begin(&restricted, lease)
                .await
                .unwrap();
            seed.write(&mut tx, "primary", "creating", &chunks, 100)
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }
        let (page, statements) =
            crate::db::workflows::run_shard_summary::test_support::record_queries(
                admission::placement::read_chunks(
                    &restricted,
                    lease,
                    second.draft.dataset_version_id,
                    "primary",
                    &[0, 1, 2],
                    (-1, -1, Uuid::nil()),
                ),
            )
            .await;
        let page = page.unwrap();
        assert_eq!(page.len(), 3);
        assert!(page.iter().all(|r| r.4));
        assert_eq!(
            statements.len(),
            2,
            "placement verification adds one bounded scope submission"
        );
        let (checkpoint, statements) =
            crate::db::workflows::run_shard_summary::test_support::record_queries(
                admission::acknowledge(&restricted, lease, 0, 3, &expected),
            )
            .await;
        checkpoint.unwrap();
        assert_eq!(
            statements.len(),
            3,
            "checkpoint retains lock, renew, update"
        );
        assert!(
            admission::acknowledge(&restricted, lease, 0, 3, &expected)
                .await
                .is_err()
        );
        assert!(
            admission::activate(&restricted, lease, &expected)
                .await
                .is_err(),
            "catalog lock requires an explicit function grant"
        );
        assert!(
            sqlx::query("UPDATE database_placements SET status='disabled' WHERE alias='primary'")
                .execute(&restricted)
                .await
                .is_err(),
            "runtime must not edit the catalog"
        );
        sqlx::raw_sql(&format!(
            "GRANT EXECUTE ON FUNCTION vigilo_lock_database_placement(text) TO {role}"
        ))
        .execute(&db)
        .await
        .unwrap();
        // The restricted read retains the catalog lock on the caller's transaction.
        let mut placement_tx = restricted.begin().await.unwrap();
        let placement: (String, String) =
            sqlx::query_as("SELECT * FROM vigilo_lock_database_placement('primary')")
                .fetch_one(&mut *placement_tx)
                .await
                .unwrap();
        assert_eq!(placement, ("control_and_shard".into(), "active".into()));
        let mut admin_tx = db.begin().await.unwrap();
        sqlx::query("SET LOCAL lock_timeout='50ms'")
            .execute(&mut *admin_tx)
            .await
            .unwrap();
        let blocked =
            sqlx::query("UPDATE database_placements SET status='draining' WHERE alias='primary'")
                .execute(&mut *admin_tx)
                .await
                .unwrap_err();
        assert_eq!(
            blocked.as_database_error().unwrap().code().as_deref(),
            Some("55P03")
        );
        admin_tx.rollback().await.unwrap();
        placement_tx.rollback().await.unwrap();
        admission::activate(&restricted, lease, &expected)
            .await
            .unwrap();
        assert!(
            admission::renew(&restricted, lease).await.is_err(),
            "activation releases authority"
        );
        let routes: Vec<(Uuid, Uuid)> =
            sqlx::query_as("SELECT tenant_id,run_id FROM shard_placements WHERE run_id=$1")
                .bind(lease.run_id)
                .fetch_all(&db)
                .await
                .unwrap();
        assert_eq!(routes.len(), 3);
        assert!(routes.iter().all(|r| *r == (other_tenant, lease.run_id)));
        let status: String = sqlx::query_scalar("SELECT status::text FROM runs WHERE id=$1")
            .bind(lease.run_id)
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(status, "pending");
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM runs")
            .fetch_one(&restricted)
            .await
            .unwrap();
        assert_eq!(count, 0, "pooled work identity must be transaction local");
    })
    .catch_unwind()
    .await;
    restricted.close().await;
    sqlx::raw_sql(&format!("ALTER FUNCTION vigilo_claim_admission(uuid,uuid) OWNER TO CURRENT_USER; ALTER FUNCTION vigilo_lock_database_placement(text) OWNER TO CURRENT_USER; DROP OWNED BY {role}; DROP ROLE {role}; DROP OWNED BY {owner}; DROP ROLE {owner};")).execute(&db).await.unwrap();
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}
