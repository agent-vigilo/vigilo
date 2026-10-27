//! Real database coverage for grouped placement writes, exact replay and fences.
//! Fixtures keep blob reads local; distributed service tests prove remote commits.
use super::*;

async fn ready(db: &PgPool) -> (Fixture, Plan) {
    let f = Fixture::new(db).await;
    let lease = claim(db, f.plan.run_id).await.unwrap();
    f.prepare(db, lease).await.unwrap();
    let plan = load(db, f.plan.run_id).await.unwrap().unwrap();
    (f, plan)
}
async fn stop_ack(db: &PgPool) {
    sqlx::raw_sql("CREATE FUNCTION stop_ack() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.admission_cursor>OLD.admission_cursor THEN RAISE EXCEPTION 'lost control acknowledgement'; END IF; RETURN NEW; END $$; CREATE TRIGGER stop_ack BEFORE UPDATE ON run_plans FOR EACH ROW EXECUTE FUNCTION stop_ack();").execute(db).await.unwrap();
}
async fn allow_ack(db: &PgPool) {
    sqlx::raw_sql("DROP TRIGGER stop_ack ON run_plans; DROP FUNCTION stop_ack();")
        .execute(db)
        .await
        .unwrap();
}
async fn progress(db: &PgPool, id: Uuid) -> (i32, i64, i64) {
    sqlx::query_as("SELECT admission_cursor,(SELECT count(*) FROM run_chunks WHERE run_id=$1),(SELECT count(*) FROM run_shard_dispatch_cursors WHERE run_id=$1) FROM run_plans WHERE run_id=$1").bind(id).fetch_one(db).await.unwrap()
}
#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn grouped_local_commit_survives_lost_control_ack_and_replays_exactly(db: PgPool) {
    let (f, plan) = ready(&db).await;
    let context = runtime_context(&db);
    let store: Arc<dyn crate::blob_storage::BlobStorage> = Arc::new(f.store);
    stop_ack(&db).await;
    assert!(
        crate::run_admission::resume(
            &context,
            &plan,
            store.clone(),
            Default::default(),
            &f.cancel
        )
        .await
        .is_err()
    );
    assert_eq!(progress(&db, plan.run_id).await, (0, 2, 0));
    allow_ack(&db).await;
    assert!(
        crate::run_admission::resume(&context, &plan, store, Default::default(), &f.cancel)
            .await
            .unwrap()
    );
    assert_eq!(progress(&db, plan.run_id).await, (3, 3, 3));
}
#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn grouped_replay_rejects_changed_snapshots_and_closed_or_moved_admissions(db: PgPool) {
    for mutation in [
        "UPDATE run_snapshots SET expected_execution_count=99 WHERE run_id=$1 AND run_shard=1",
        "UPDATE run_snapshots SET config_snapshot=config_snapshot || '{\"changed\":true}'::jsonb WHERE run_id=$1 AND run_shard=1",
        "UPDATE local_shard_admissions SET state='closed' WHERE run_id=$1 AND run_shard=1",
        "UPDATE local_shard_admissions SET write_epoch=2 WHERE run_id=$1 AND run_shard=1",
        "UPDATE local_shard_admissions SET database_alias='other' WHERE run_id=$1 AND run_shard=1",
    ] {
        let (f, plan) = ready(&db).await;
        let context = runtime_context(&db);
        let store: Arc<dyn crate::blob_storage::BlobStorage> = Arc::new(f.store);
        stop_ack(&db).await;
        assert!(
            crate::run_admission::resume(
                &context,
                &plan,
                store.clone(),
                Default::default(),
                &f.cancel
            )
            .await
            .is_err()
        );
        allow_ack(&db).await;
        sqlx::query(mutation)
            .bind(plan.run_id)
            .execute(&db)
            .await
            .unwrap();
        assert!(
            crate::run_admission::resume(&context, &plan, store, Default::default(), &f.cancel)
                .await
                .is_err()
        );
        assert_eq!(progress(&db, plan.run_id).await, (0, 2, 0));
        let status: String = sqlx::query_scalar("SELECT status::text FROM runs WHERE id=$1")
            .bind(plan.run_id)
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(status, "failed");
    }
}
#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn disabled_or_draining_placement_never_activates_until_active(db: PgPool) {
    let (f, plan) = ready(&db).await;
    let context = runtime_context(&db);
    let store: Arc<dyn crate::blob_storage::BlobStorage> = Arc::new(f.store);
    let config = crate::db::workflows::run_creation::Config {
        case_page_budget: 1,
        ..Default::default()
    };
    assert!(
        !crate::run_admission::resume(&context, &plan, store.clone(), config, &f.cancel)
            .await
            .unwrap()
    );
    sqlx::query("UPDATE database_placements SET status='disabled' WHERE alias='primary'")
        .execute(&db)
        .await
        .unwrap();
    assert!(
        crate::run_admission::resume(&context, &plan, store.clone(), config, &f.cancel)
            .await
            .is_err()
    );
    assert_eq!(progress(&db, plan.run_id).await, (2, 2, 0));
    sqlx::query("UPDATE database_placements SET status='draining' WHERE alias='primary'")
        .execute(&db)
        .await
        .unwrap();
    assert!(
        crate::run_admission::resume(&context, &plan, store.clone(), config, &f.cancel)
            .await
            .is_err()
    );
    assert_eq!(progress(&db, plan.run_id).await, (3, 3, 0));
    sqlx::query("UPDATE database_placements SET status='active' WHERE alias='primary'")
        .execute(&db)
        .await
        .unwrap();
    assert!(
        crate::run_admission::resume(&context, &plan, store, config, &f.cancel)
            .await
            .unwrap()
    );
    assert_eq!(progress(&db, plan.run_id).await, (3, 3, 3));
}
#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn snapshot_groups_cross_64_shards_without_per_shard_renewal(db: PgPool) {
    let f = Fixture::with_size(&db, 6500, 100, 256).await;
    let lease = claim(&db, f.plan.run_id).await.unwrap();
    f.prepare(&db, lease).await.unwrap();
    let plan = load(&db, f.plan.run_id).await.unwrap().unwrap();
    sqlx::raw_sql("CREATE TABLE snapshot_sizes(n bigint); CREATE FUNCTION observe_snapshots() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO snapshot_sizes SELECT count(*) FROM inserted; RETURN NULL; END $$; CREATE TRIGGER observe_snapshots AFTER INSERT ON run_snapshots REFERENCING NEW TABLE AS inserted FOR EACH STATEMENT EXECUTE FUNCTION observe_snapshots(); CREATE TABLE renewals(n integer); CREATE FUNCTION observe_renewal() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO renewals VALUES(1); RETURN NEW; END $$; CREATE TRIGGER observe_renewal AFTER UPDATE ON runs FOR EACH ROW WHEN (NEW.coordinator_heartbeat_at IS DISTINCT FROM OLD.coordinator_heartbeat_at) EXECUTE FUNCTION observe_renewal();").execute(&db).await.unwrap();
    assert!(
        crate::run_admission::resume(
            &runtime_context(&db),
            &plan,
            Arc::new(f.store),
            Default::default(),
            &f.cancel
        )
        .await
        .unwrap()
    );
    let sizes: Vec<i64> = sqlx::query_scalar("SELECT n FROM snapshot_sizes ORDER BY n")
        .fetch_all(&db)
        .await
        .unwrap();
    assert_eq!(sizes, vec![1, 64]);
    let renewals: i64 = sqlx::query_scalar("SELECT count(*) FROM renewals")
        .fetch_one(&db)
        .await
        .unwrap();
    assert!(
        renewals <= 6,
        "unexpected per-shard lease updates: {renewals}"
    );
    assert_eq!(progress(&db, plan.run_id).await, (65, 65, 65));
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn placement_verification_folds_shards_across_256_row_boundaries(db: PgPool) {
    let f = Fixture::with_size(&db, 25701, 100, 256).await;
    let lease = claim(&db, f.plan.run_id).await.unwrap();
    f.prepare(&db, lease).await.unwrap();
    let plan = load(&db, f.plan.run_id).await.unwrap().unwrap();
    assert!(
        crate::run_admission::resume(
            &runtime_context(&db),
            &plan,
            Arc::new(f.store),
            Default::default(),
            &f.cancel
        )
        .await
        .unwrap()
    );
    assert_eq!(progress(&db, plan.run_id).await, (258, 258, 128));
}

async fn wait_for_fence(db: &PgPool) {
    tokio::time::timeout(std::time::Duration::from_secs(5),async {
        loop {
            let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory' AND query LIKE '%pg_advisory_xact_lock_shared%')").fetch_one(db).await.unwrap();
            if waiting {break;}
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await.expect("seed did not wait on shard authority");
}
#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn grouped_seed_checks_cancellation_tombstone_after_waiting_for_shard_fence(db: PgPool) {
    let (f, plan) = ready(&db).await;
    let mut lock = db.begin().await.unwrap();
    crate::db::shard_write_fence::lock_exclusive(&mut lock, plan.run_id, 1)
        .await
        .unwrap();
    let pool = db.clone();
    let copy = plan.clone();
    let task = tokio::spawn(async move {
        crate::run_admission::resume(
            &runtime_context(&pool),
            &copy,
            Arc::new(f.store),
            Default::default(),
            &f.cancel,
        )
        .await
    });
    wait_for_fence(&db).await;
    let lower_available: bool =
        sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtextextended($1::uuid::text,0))")
            .bind(plan.run_id)
            .fetch_one(&mut *lock)
            .await
            .unwrap();
    assert!(
        !lower_available,
        "lower shard fence must precede the blocked higher shard"
    );
    sqlx::query("INSERT INTO local_shard_admissions(run_id,run_shard,database_alias,write_epoch,state) VALUES($1,1,'primary',1,'closed')").bind(plan.run_id).execute(&mut *lock).await.unwrap();
    lock.commit().await.unwrap();
    assert!(task.await.unwrap().is_err());
    assert_eq!(progress(&db, plan.run_id).await, (0, 0, 0));
    let states: Vec<(i16, String)> = sqlx::query_as(
        "SELECT run_shard,state FROM local_shard_admissions WHERE run_id=$1 ORDER BY run_shard",
    )
    .bind(plan.run_id)
    .fetch_all(&db)
    .await
    .unwrap();
    assert_eq!(states, vec![(1, "closed".into())]);
}
#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn owner_replaced_during_local_lock_wait_cannot_acknowledge_or_activate(db: PgPool) {
    let (f, plan) = ready(&db).await;
    let store: Arc<dyn crate::blob_storage::BlobStorage> = Arc::new(f.store);
    let mut lock = db.begin().await.unwrap();
    crate::db::shard_write_fence::lock_exclusive(&mut lock, plan.run_id, 1)
        .await
        .unwrap();
    let pool = db.clone();
    let copy = plan.clone();
    let storage = store.clone();
    let task = tokio::spawn(async move {
        crate::run_admission::resume(
            &runtime_context(&pool),
            &copy,
            storage,
            Default::default(),
            &f.cancel,
        )
        .await
    });
    wait_for_fence(&db).await;
    tokio::time::timeout(std::time::Duration::from_secs(2),sqlx::query("UPDATE runs SET coordinator_leased_until=clock_timestamp()-interval '1 second' WHERE id=$1").bind(plan.run_id).execute(&db)).await.unwrap().unwrap();
    let replacement = admission::claim(&db, plan.run_id).await.unwrap();
    release(&db, replacement, None).await.unwrap();
    lock.rollback().await.unwrap();
    assert!(task.await.unwrap().is_err());
    assert_eq!(progress(&db, plan.run_id).await, (0, 2, 0));
    assert!(
        crate::run_admission::resume(
            &runtime_context(&db),
            &plan,
            store,
            Default::default(),
            &CancellationToken::new()
        )
        .await
        .unwrap()
    );
    assert_eq!(progress(&db, plan.run_id).await, (3, 3, 3));
}
#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn cancelled_or_timed_out_placement_operations_rollback_and_remain_resumable(db: PgPool) {
    for cancel_early in [true, false] {
        let (f, plan) = ready(&db).await;
        let store: Arc<dyn crate::blob_storage::BlobStorage> = Arc::new(f.store);
        let cancel = f.cancel.clone();
        let mut lock = db.begin().await.unwrap();
        crate::db::shard_write_fence::lock_exclusive(&mut lock, plan.run_id, 1)
            .await
            .unwrap();
        let pool = db.clone();
        let copy = plan.clone();
        let storage = store.clone();
        let task = tokio::spawn(async move {
            crate::run_admission::resume(
                &runtime_context(&pool),
                &copy,
                storage,
                Default::default(),
                &f.cancel,
            )
            .await
        });
        wait_for_fence(&db).await;
        if cancel_early {
            cancel.cancel();
        }
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(if cancel_early { 3 } else { 35 }),
            task,
        )
        .await
        .unwrap()
        .unwrap();
        assert!(result.is_err());
        lock.rollback().await.unwrap();
        assert_eq!(progress(&db, plan.run_id).await, (0, 0, 0));
        assert!(
            crate::run_admission::resume(
                &runtime_context(&db),
                &plan,
                store,
                Default::default(),
                &CancellationToken::new()
            )
            .await
            .unwrap()
        );
    }
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn snapshot_byte_bounds_split_large_templates_and_preserve_one_admitted_snapshot(db: PgPool) {
    use crate::{
        db::workflows::run_shard_summary::plan::{
            ChunkIdentity,
            Manifest,
        },
        models::run_chunk::RunChunkDraft,
    };
    for padding in [5 * 1024 * 1024, 9 * 1024 * 1024] {
        let (f, plan) = ready(&db).await;
        let draft: RunDraft = sqlx::query_as("SELECT * FROM runs WHERE id=$1")
            .bind(plan.run_id)
            .fetch_one(&db)
            .await
            .unwrap();
        let chunks = (0..2_i16)
            .map(|shard| RunChunkDraft {
                chunk_id: Uuid::now_v7(),
                run_shard: shard,
                ordinal_start: i32::from(shard) * 100,
                ordinal_end: (i32::from(shard) + 1) * 100,
                profile_group_id: "default".into(),
            })
            .collect::<Vec<_>>();
        let mut expected = std::collections::BTreeMap::new();
        for c in &chunks {
            let mut manifest = Manifest::default();
            manifest
                .push(&ChunkIdentity {
                    chunk_id: c.chunk_id,
                    run_shard: c.run_shard,
                    ordinal_start: c.ordinal_start,
                    ordinal_end: c.ordinal_end,
                })
                .unwrap();
            expected.insert(c.run_shard, manifest);
        }
        let gates = std::collections::BTreeMap::from([(0, json!({})), (1, json!({}))]);
        let snapshot = json!({"padding":"x".repeat(padding)});
        // Admission can carry source configurations up to the existing format
        // limit. A group target must not reject a valid larger single snapshot.
        let seed = admission::placement::Seed {
            run_id: plan.run_id,
            draft: &draft,
            snapshot: &snapshot,
            expected: &expected,
            gate_counts: &gates,
        };
        sqlx::raw_sql("CREATE TABLE sizes(n bigint); CREATE FUNCTION observe_sizes() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO sizes SELECT count(*) FROM inserted; RETURN NULL; END $$; CREATE TRIGGER observe_sizes AFTER INSERT ON run_snapshots REFERENCING NEW TABLE AS inserted FOR EACH STATEMENT EXECUTE FUNCTION observe_sizes();").execute(&db).await.unwrap();
        let mut tx = db.begin().await.unwrap();
        seed.write(&mut tx, "primary", "creating", &chunks, 256)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let sizes: Vec<i64> = sqlx::query_scalar("SELECT n FROM sizes ORDER BY n")
            .fetch_all(&db)
            .await
            .unwrap();
        assert_eq!(sizes, vec![1, 1]);
        let preserved:bool=sqlx::query_scalar("SELECT bool_and(config_snapshot->'padding'=$2::jsonb) FROM run_snapshots WHERE run_id=$1").bind(plan.run_id).bind(&snapshot["padding"]).fetch_one(&db).await.unwrap();
        assert!(preserved);
        sqlx::raw_sql("DROP TRIGGER observe_sizes ON run_snapshots; DROP FUNCTION observe_sizes(); DROP TABLE sizes;").execute(&db).await.unwrap();
        drop(f);
    }
}
