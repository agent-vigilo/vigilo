//! PostgreSQL regressions for bounded plan-file publication and recovery authority.
use super::*;

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn gate_files_share_a_durable_registration_statement(db: PgPool) {
    let f = Fixture::new(&db).await;
    sqlx::raw_sql("CREATE TABLE gate_groups(n bigint); CREATE FUNCTION count_gate_group() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF EXISTS(SELECT 1 FROM inserted WHERE kind='gates') THEN INSERT INTO gate_groups SELECT count(*) FROM inserted WHERE kind='gates'; END IF; RETURN NULL; END $$; CREATE TRIGGER count_gate_group AFTER INSERT ON run_plan_objects REFERENCING NEW TABLE AS inserted FOR EACH STATEMENT EXECUTE FUNCTION count_gate_group();")
        .execute(&db).await.unwrap();
    let lease = claim(&db, f.plan.run_id).await.unwrap();
    f.prepare(&db, lease).await.unwrap();
    let groups: Vec<i64> = sqlx::query_scalar("SELECT n FROM gate_groups ORDER BY n")
        .fetch_all(&db)
        .await
        .unwrap();
    assert_eq!(
        groups,
        vec![9],
        "bounded gate files should register together"
    );
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn competing_admission_does_not_read_blobs_before_claim(db: PgPool) {
    let f = Fixture::new(&db).await;
    let lease = claim(&db, f.plan.run_id).await.unwrap();
    f.prepare(&db, lease).await.unwrap();
    admission::request(&db, f.plan.run_id).await.unwrap();
    let owner = admission::claim(&db, f.plan.run_id).await.unwrap();
    let plan = load(&db, f.plan.run_id).await.unwrap().unwrap();
    let before = f.memory.0.lock().unwrap().reads;
    assert!(
        crate::run_admission::resume(
            &runtime_context(&db),
            &plan,
            Arc::new(f.store),
            Default::default(),
            &f.cancel
        )
        .await
        .is_err()
    );
    assert_eq!(
        f.memory.0.lock().unwrap().reads,
        before,
        "losing admission must not fetch metadata"
    );
    admission::renew(&db, owner).await.unwrap();
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn interrupted_group_owns_every_file_before_put_and_replays_without_duplicates(db: PgPool) {
    let f = Fixture::new(&db).await;
    let lease = claim(&db, f.plan.run_id).await.unwrap();
    f.memory.0.lock().unwrap().stop = Some("/gates/".into());
    assert!(f.prepare(&db, lease).await.is_err());
    let (owned, acknowledged): (i64,i64) = sqlx::query_as("SELECT count(*),count(*) FILTER(WHERE acknowledged) FROM run_plan_objects WHERE run_id=$1 AND kind='gates'")
        .bind(f.plan.run_id).fetch_one(&db).await.unwrap();
    assert_eq!((owned, acknowledged), (9, 0));
    assert_eq!(
        f.memory
            .0
            .lock()
            .unwrap()
            .creates
            .keys()
            .filter(|k| k.contains("/gates/"))
            .count(),
        1
    );
    release(&db, lease, None).await.unwrap();
    let next = claim(&db, f.plan.run_id).await.unwrap();
    f.prepare(&db, next).await.unwrap();
    assert!(f.memory.0.lock().unwrap().creates.values().all(|n| *n == 1));
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn lost_group_ack_and_corrupt_acknowledged_file_cannot_publish(db: PgPool) {
    let f = Fixture::new(&db).await;
    let lease = claim(&db, f.plan.run_id).await.unwrap();
    sqlx::raw_sql("CREATE FUNCTION reject_gate_ack() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.kind='gates' AND NEW.acknowledged THEN RAISE EXCEPTION 'lost group acknowledgement'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_gate_ack BEFORE UPDATE ON run_plan_objects FOR EACH ROW EXECUTE FUNCTION reject_gate_ack();")
        .execute(&db).await.unwrap();
    assert!(f.prepare(&db, lease).await.is_err());
    let n: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM run_plan_objects WHERE run_id=$1 AND kind='gates' AND acknowledged",
    )
    .bind(f.plan.run_id)
    .fetch_one(&db)
    .await
    .unwrap();
    assert_eq!(n, 0);
    assert_eq!(
        f.memory
            .0
            .lock()
            .unwrap()
            .creates
            .keys()
            .filter(|k| k.contains("/gates/"))
            .count(),
        9
    );
    sqlx::raw_sql("DROP TRIGGER reject_gate_ack ON run_plan_objects; CREATE FUNCTION reject_publication() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.status='ready' THEN RAISE EXCEPTION 'publication interrupted'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_publication BEFORE UPDATE ON run_plans FOR EACH ROW EXECUTE FUNCTION reject_publication();").execute(&db).await.unwrap();
    assert!(f.prepare(&db, lease).await.is_err());
    sqlx::query("DROP TRIGGER reject_publication ON run_plans")
        .execute(&db)
        .await
        .unwrap();
    let key = f
        .memory
        .0
        .lock()
        .unwrap()
        .objects
        .keys()
        .find(|k| k.contains("/gates/"))
        .unwrap()
        .clone();
    let original = f
        .memory
        .0
        .lock()
        .unwrap()
        .objects
        .insert(key.clone(), b"corrupt".to_vec())
        .unwrap();
    assert!(f.prepare(&db, lease).await.is_err());
    assert!(
        load(&db, f.plan.run_id)
            .await
            .unwrap()
            .unwrap()
            .manifest
            .is_none()
    );
    f.memory.0.lock().unwrap().objects.insert(key, original);
    f.prepare(&db, lease).await.unwrap();
    assert!(f.memory.0.lock().unwrap().creates.values().all(|n| *n == 1));
}

fn chunk(f: &Fixture, start: i32, end: i32) -> Object {
    let reference = ObjectRef::for_bytes(
        "cases".into(),
        format!("runs/{}/chunks/{start}-{end}", f.plan.run_id),
        StorageFormat::RunChunks,
        b"chunk",
        &f.plan.limits,
    )
    .unwrap();
    Object {
        object_key: reference.key.clone(),
        kind: "chunks".into(),
        level: 0,
        range_start: Some(start),
        range_end: Some(end),
        reference: Json(reference),
        acknowledged: false,
    }
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn group_registration_and_ack_are_exact_atomic_and_bounded(db: PgPool) {
    let f = Fixture::new(&db).await;
    let lease = claim(&db, f.plan.run_id).await.unwrap();
    let objects = vec![chunk(&f, 0, 1), chunk(&f, 1, 2), chunk(&f, 2, 3)];
    assert!(batches::plan(&db, lease, &[]).await.is_err());
    assert!(
        batches::plan(&db, lease, &vec![objects[0].clone(); 65])
            .await
            .is_err()
    );
    assert!(
        batches::plan(&db, lease, &[objects[0].clone(), objects[0].clone()])
            .await
            .is_err()
    );
    let mut oversized = objects[..2].to_vec();
    for o in &mut oversized {
        o.reference.byte_count = batches::GROUP_BYTES;
    }
    assert!(batches::plan(&db, lease, &oversized).await.is_err());
    assert_eq!(
        batches::plan(&db, lease, &objects[..1]).await.unwrap(),
        vec![false]
    );
    let mut changed = objects[0].clone();
    changed.object_key.push_str("-changed");
    changed.reference.key = changed.object_key.clone();
    assert!(
        batches::plan(&db, lease, &[objects[1].clone(), changed])
            .await
            .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM run_plan_objects WHERE run_id=$1")
        .bind(f.plan.run_id)
        .fetch_one(&db)
        .await
        .unwrap();
    assert_eq!(count, 1, "conflict must roll back new sibling registration");
    batches::plan(&db, lease, &objects).await.unwrap();
    assert!(
        batches::acknowledge(&db, lease, &objects[1..])
            .await
            .is_err()
    );
    batches::acknowledge(&db, lease, &objects[..1])
        .await
        .unwrap();
    let mut changed = objects.clone();
    changed[2].reference.digest = "f".repeat(64);
    assert!(batches::acknowledge(&db, lease, &changed).await.is_err());
    assert_eq!(
        load(&db, f.plan.run_id)
            .await
            .unwrap()
            .unwrap()
            .chunk_cursor,
        1
    );
    assert_eq!(
        batches::plan(&db, lease, &objects).await.unwrap(),
        vec![true, false, false]
    );
    batches::acknowledge(&db, lease, &objects).await.unwrap();
    batches::acknowledge(&db, lease, &objects).await.unwrap();
    assert_eq!(
        load(&db, f.plan.run_id)
            .await
            .unwrap()
            .unwrap()
            .chunk_cursor,
        3
    );
    sqlx::query("UPDATE runs SET status='cancelled' WHERE id=$1")
        .bind(f.plan.run_id)
        .execute(&db)
        .await
        .unwrap();
    assert!(batches::plan(&db, lease, &objects).await.is_err());
    assert!(batches::acknowledge(&db, lease, &objects).await.is_err());
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn grouped_gate_publication_crosses_file_limit_with_complete_chains(db: PgPool) {
    let f = Fixture::with_size(&db, 6500, 100, 256).await;
    sqlx::raw_sql("CREATE TABLE gate_groups(n bigint); CREATE FUNCTION count_gate_group() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF EXISTS(SELECT 1 FROM inserted WHERE kind='gates') THEN INSERT INTO gate_groups SELECT count(*) FROM inserted WHERE kind='gates'; END IF; RETURN NULL; END $$; CREATE TRIGGER count_gate_group AFTER INSERT ON run_plan_objects REFERENCING NEW TABLE AS inserted FOR EACH STATEMENT EXECUTE FUNCTION count_gate_group();").execute(&db).await.unwrap();
    let lease = claim(&db, f.plan.run_id).await.unwrap();
    let root = f.prepare(&db, lease).await.unwrap();
    let groups: Vec<i64> = sqlx::query_scalar("SELECT n FROM gate_groups ORDER BY n")
        .fetch_all(&db)
        .await
        .unwrap();
    assert_eq!(groups, vec![1, 64]);
    let reader = crate::case_reader::RunReader::open(
        Arc::new(f.store),
        &root,
        f.plan.limits.0.clone(),
        f.plan.run_id,
        &f.cancel,
    )
    .await
    .unwrap();
    assert_eq!(reader.manifest.shards.len(), 65);
    for shard in &reader.manifest.shards {
        assert_eq!(shard.gate_counts.entry_count, 5);
        assert_eq!(shard.gate_counts.page_count, 1);
    }
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn missing_root_and_cancelled_metadata_reads_release_recovery_owner(db: PgPool) {
    let f = Fixture::new(&db).await;
    let lease = claim(&db, f.plan.run_id).await.unwrap();
    let root = f.prepare(&db, lease).await.unwrap();
    let plan = load(&db, f.plan.run_id).await.unwrap().unwrap();
    let original = f
        .memory
        .0
        .lock()
        .unwrap()
        .objects
        .remove(&root.key)
        .unwrap();
    let store: Arc<dyn crate::blob_storage::BlobStorage> = Arc::new(f.store);
    let context = runtime_context(&db);
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
    let unleased: bool = sqlx::query_scalar("SELECT coordinator_id IS NULL FROM runs WHERE id=$1")
        .bind(plan.run_id)
        .fetch_one(&db)
        .await
        .unwrap();
    assert!(unleased);
    f.memory
        .0
        .lock()
        .unwrap()
        .objects
        .insert(root.key.clone(), original);
    let entered = Arc::new(tokio::sync::Notify::new());
    let unblock = Arc::new(tokio::sync::Notify::new());
    f.memory.0.lock().unwrap().blocked_read = Some((root.key, entered.clone(), unblock));
    let c = context.clone();
    let p = plan.clone();
    let s = store.clone();
    let cancel = f.cancel.clone();
    let task = tokio::spawn(async move {
        crate::run_admission::resume(&c, &p, s, Default::default(), &cancel).await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    let leased: bool =
        sqlx::query_scalar("SELECT coordinator_id IS NOT NULL FROM runs WHERE id=$1")
            .bind(plan.run_id)
            .fetch_one(&db)
            .await
            .unwrap();
    assert!(leased, "metadata reads must hold recovery authority");
    assert!(
        crate::run_admission::resume(
            &context,
            &plan,
            store.clone(),
            Default::default(),
            &CancellationToken::new()
        )
        .await
        .is_err()
    );
    f.cancel.cancel();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    let unleased: bool = sqlx::query_scalar("SELECT coordinator_id IS NULL FROM runs WHERE id=$1")
        .bind(plan.run_id)
        .fetch_one(&db)
        .await
        .unwrap();
    assert!(unleased);
    f.memory.0.lock().unwrap().blocked_read = None;
    assert!(
        crate::run_admission::resume(
            &context,
            &plan,
            store,
            Default::default(),
            &CancellationToken::new()
        )
        .await
        .unwrap()
    );
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn slow_plan_group_checkpoints_verified_prefix_then_renews_remaining_work(db: PgPool) {
    let f = Fixture::new(&db).await;
    sqlx::raw_sql("CREATE TABLE gate_acks(n bigint); CREATE FUNCTION count_gate_ack() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF EXISTS(SELECT 1 FROM updated WHERE kind='gates') THEN INSERT INTO gate_acks SELECT count(*) FROM updated WHERE kind='gates'; END IF; RETURN NULL; END $$; CREATE TRIGGER count_gate_ack AFTER UPDATE ON run_plan_objects REFERENCING NEW TABLE AS updated FOR EACH STATEMENT EXECUTE FUNCTION count_gate_ack();").execute(&db).await.unwrap();
    f.memory.0.lock().unwrap().delayed_put =
        Some(("/gates/".into(), std::time::Duration::from_secs(31)));
    let lease = claim(&db, f.plan.run_id).await.unwrap();
    f.prepare(&db, lease).await.unwrap();
    let groups: Vec<i64> = sqlx::query_scalar("SELECT n FROM gate_acks ORDER BY n")
        .fetch_all(&db)
        .await
        .unwrap();
    assert_eq!(
        groups,
        vec![1, 8],
        "slow group must checkpoint only its verified prefix"
    );
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn plan_group_rechecks_expiry_after_registration_and_ack_lock_waits(db: PgPool) {
    for acknowledgement in [false, true] {
        let f = Fixture::new(&db).await;
        let lease = claim(&db, f.plan.run_id).await.unwrap();
        let objects = vec![chunk(&f, 0, 1), chunk(&f, 1, 2)];
        if acknowledgement {
            batches::plan(&db, lease, &objects).await.unwrap();
        }
        let mut lock = db.begin().await.unwrap();
        sqlx::query("UPDATE runs SET coordinator_leased_until=clock_timestamp()+interval '1 second' WHERE id=$1").bind(f.plan.run_id).execute(&mut *lock).await.unwrap();
        let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *lock)
            .await
            .unwrap();
        let pool = db.clone();
        let task = tokio::spawn(async move {
            if acknowledgement {
                batches::acknowledge(&pool, lease, &objects).await
            } else {
                batches::plan(&pool, lease, &objects).await.map(|_| ())
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5),async {loop {
            let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))").bind(blocker).fetch_one(&db).await.unwrap();
            if waiting {break;} tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }}).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        lock.commit().await.unwrap();
        assert!(task.await.unwrap().is_err());
        assert_eq!(
            load(&db, f.plan.run_id)
                .await
                .unwrap()
                .unwrap()
                .chunk_cursor,
            0
        );
    }
}
