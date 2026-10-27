//! Group-checkpoint atomicity, replay, timing and cleanup against PostgreSQL.
//! The observed in-memory transport checks durable state at real I/O boundaries;
//! named service tests separately exercise signed uploads and process death.
use super::*;

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn group_limits_reject_empty_oversized_and_noncontiguous_checkpoints(db: PgPool) {
    let (mut row, _) = fixture(&db).await;
    let mut identity = row.identity.0.clone();
    identity.case_count = 65;
    row = begin(
        &db,
        Uuid::now_v7(),
        "cases",
        &row.binding_hash,
        &identity,
        &row.limits,
    )
    .await
    .unwrap();
    let lease = claim(&db, row.id, false).await.unwrap();
    let objects: Vec<_> = (0..65).map(|n| batch(&row, n, n + 1)).collect();
    assert!(batches::plan(&db, lease, &[]).await.is_err());
    assert!(batches::acknowledge(&db, lease, &[]).await.is_err());
    assert!(batches::plan(&db, lease, &objects).await.is_err());
    assert!(
        batches::plan(&db, lease, &[objects[0].clone(), objects[2].clone()])
            .await
            .is_err()
    );
    let mut oversized = objects[0].clone();
    oversized.reference.byte_count = batches::MAX_PAYLOAD_BYTES + 1;
    assert!(batches::plan(&db, lease, &[oversized]).await.is_err());
    let mut oversized = objects[0].clone();
    oversized.object_key = "x".repeat(batches::MAX_METADATA_BYTES);
    assert!(batches::plan(&db, lease, &[oversized]).await.is_err());
    let mut wrong = objects[0].clone();
    wrong.reference.storage_alias = "other".into();
    assert!(batches::plan(&db, lease, &[wrong]).await.is_err());
    assert_eq!(
        batches::plan(&db, lease, &objects[..64])
            .await
            .unwrap()
            .len(),
        64
    );
    batches::acknowledge(&db, lease, &objects[..64])
        .await
        .unwrap();
    batches::plan(&db, lease, &objects[64..]).await.unwrap();
    batches::acknowledge(&db, lease, &objects[64..])
        .await
        .unwrap();
    let progress = load(&db, row.id).await.unwrap();
    assert_eq!((progress.case_cursor, progress.batch_count), (65, 65));
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn checkpoint_replay_is_exact_and_acknowledgement_is_atomic(db: PgPool) {
    let (row, lease) = fixture(&db).await;
    let objects = vec![batch(&row, 0, 1), batch(&row, 1, 2), batch(&row, 2, 3)];
    plan(&db, lease, &objects[1], false).await.unwrap();
    let mut changed = objects.clone();
    changed[1].reference.digest = "f".repeat(64);
    assert!(batches::plan(&db, lease, &changed).await.is_err());
    let registered: i64 =
        sqlx::query_scalar("SELECT count(*) FROM dataset_upload_objects WHERE upload_id=$1")
            .bind(row.id)
            .fetch_one(&db)
            .await
            .unwrap();
    assert_eq!(
        registered, 1,
        "conflicting slot rolls back all new registrations"
    );
    assert_eq!(
        batches::plan(&db, lease, &objects).await.unwrap(),
        vec![false; 3]
    );
    acknowledge(&db, lease, &objects[0], false).await.unwrap();
    release(&db, lease).await.unwrap();
    let lease = claim(&db, row.id, false).await.unwrap();
    assert_eq!(
        batches::plan(&db, lease, &objects).await.unwrap(),
        vec![true, false, false]
    );
    assert!(batches::acknowledge(&db, lease, &changed).await.is_err());
    assert!(
        batches::acknowledge(&db, lease, &objects[2..])
            .await
            .is_err()
    );
    let mut invalid = objects.clone();
    invalid[2].kind = "source".into();
    assert!(batches::acknowledge(&db, lease, &invalid).await.is_err());
    assert_eq!(load(&db, row.id).await.unwrap().case_cursor, 1);
    assert_eq!(
        batches::plan(&db, lease, &objects).await.unwrap(),
        vec![true, false, false]
    );
    batches::acknowledge(&db, lease, &objects).await.unwrap();
    // Lost commit response/restart: replay does not double-count progress.
    release(&db, lease).await.unwrap();
    let replacement = claim(&db, row.id, false).await.unwrap();
    assert!(batches::acknowledge(&db, lease, &objects).await.is_err());
    batches::acknowledge(&db, replacement, &objects)
        .await
        .unwrap();
    assert_eq!(
        batches::plan(&db, replacement, &objects).await.unwrap(),
        vec![true; 3]
    );
    let progress = load(&db, row.id).await.unwrap();
    assert_eq!((progress.case_cursor, progress.batch_count), (3, 3));
    abort(&db, row.id).await.unwrap();
    assert!(batches::plan(&db, replacement, &objects).await.is_err());
    assert!(
        batches::acknowledge(&db, replacement, &objects)
            .await
            .is_err()
    );
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn fences_recheck_expiry_after_registration_and_ack_lock_waits(db: PgPool) {
    for acknowledging in [false, true] {
        let (row, lease) = fixture(&db).await;
        let objects = vec![batch(&row, 0, 1), batch(&row, 1, 2)];
        batches::plan(&db, lease, &objects).await.unwrap();
        sqlx::query("UPDATE dataset_uploads SET leased_until=clock_timestamp()+interval '2 seconds' WHERE id=$1").bind(row.id).execute(&db).await.unwrap();
        let mut lock = db.begin().await.unwrap();
        sqlx::query("SELECT id FROM dataset_uploads WHERE id=$1 FOR UPDATE")
            .bind(row.id)
            .fetch_one(&mut *lock)
            .await
            .unwrap();
        let pool = db.clone();
        let task = tokio::spawn(async move {
            if acknowledging {
                batches::acknowledge(&pool, lease, &objects).await
            } else {
                batches::plan(&pool, lease, &objects).await.map(|_| ())
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(1),async {
            loop {
                let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%dataset_uploads%' AND query LIKE '%FOR UPDATE%')").fetch_one(&db).await.unwrap();
                if waiting {break;} tokio::task::yield_now().await;
            }
        }).await.unwrap();
        sqlx::query("SELECT pg_sleep(2.1)")
            .execute(&mut *lock)
            .await
            .unwrap();
        lock.commit().await.unwrap();
        assert!(task.await.unwrap().is_err());
        let progress = load(&db, row.id).await.unwrap();
        assert_eq!((progress.case_cursor, progress.batch_count), (0, 0));
    }
}

async fn prepared() -> ingestion::input::Prepared {
    let input = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        input.path(),
        include_bytes!("../../../../cli/commands/run/fixtures/dataset-identity.json"),
    )
    .unwrap();
    let mut limits = FormatLimits::default();
    limits.batch.max_cases = 1;
    prepare(
        input.path().to_owned(),
        InputFormat::Json,
        limits,
        &CancellationToken::new(),
    )
    .await
    .unwrap()
}
async fn store(memory: Memory, limits: FormatLimits) -> Store<Memory> {
    Store::connect(
        memory,
        "cases".into(),
        limits,
        Resources::standard(),
        Budget::standard(),
        &CancellationToken::new(),
        "capability-probes/fixture".into(),
    )
    .await
    .unwrap()
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn slow_group_checkpoints_a_verified_prefix_before_resuming_remaining_files(db: PgPool) {
    let prepared = prepared().await;
    let row = begin(
        &db,
        Uuid::now_v7(),
        "cases",
        &"2".repeat(64),
        &prepared.identity,
        &prepared.limits,
    )
    .await
    .unwrap();
    let lease = claim(&db, row.id, false).await.unwrap();
    let memory = Memory::default();
    let store = store(memory.clone(), prepared.limits.clone()).await;
    {
        let mut state = memory.0.lock().unwrap();
        state.observe_upload = Some((db.clone(), row.id));
        state.delay_batch_once = Some(std::time::Duration::from_secs(31));
        state.stop_at_batch = Some(2);
    }
    assert!(
        ingestion::ingest(
            &db,
            &store,
            &row,
            lease,
            &prepared,
            &CancellationToken::new()
        )
        .await
        .is_err()
    );
    let progress = load(&db, row.id).await.unwrap();
    assert_eq!((progress.case_cursor, progress.batch_count), (1, 1));
    assert_eq!(
        memory.0.lock().unwrap().registrations_at_batch_put,
        vec![3, 3]
    );
    release(&db, lease).await.unwrap();
    let lease = claim(&db, row.id, false).await.unwrap();
    ingestion::ingest(
        &db,
        &store,
        &row,
        lease,
        &prepared,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(load(&db, row.id).await.unwrap().batch_count, 3);
    assert!(memory.0.lock().unwrap().creates.values().all(|&n| n == 1));
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn abort_during_group_io_fences_ack_and_cleanup_reconciles_every_registered_key(db: PgPool) {
    let prepared = prepared().await;
    let row = begin(
        &db,
        Uuid::now_v7(),
        "cases",
        &"2".repeat(64),
        &prepared.identity,
        &prepared.limits,
    )
    .await
    .unwrap();
    let lease = claim(&db, row.id, false).await.unwrap();
    let memory = Memory::default();
    let store = store(memory.clone(), prepared.limits.clone()).await;
    {
        let mut state = memory.0.lock().unwrap();
        state.observe_upload = Some((db.clone(), row.id));
        state.abort_after_batch = true;
        state
            .objects
            .insert("unrelated/published".into(), b"keep".to_vec());
    }
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            ingestion::ingest(
                &db,
                &store,
                &row,
                lease,
                &prepared,
                &CancellationToken::new()
            )
        )
        .await
        .unwrap()
        .is_err(),
        "remote I/O must not hold the upload lock"
    );
    assert_eq!(load(&db, row.id).await.unwrap().case_cursor, 0);
    assert!(catalog(&db, row.id).await.unwrap().is_none());
    assert!(claim(&db, row.id, true).await.is_err());
    sqlx::query("UPDATE dataset_uploads SET cleanup_after=clock_timestamp()-interval '1 second' WHERE id=$1").bind(row.id).execute(&db).await.unwrap();
    let cleaner = claim(&db, row.id, true).await.unwrap();
    let mut done = false;
    for _ in 0..8 {
        done = ingestion::cleanup(&db, &store, cleaner, 1, &CancellationToken::new())
            .await
            .unwrap();
        if done {
            break;
        }
    }
    assert!(done);
    let state = memory.0.lock().unwrap();
    assert_eq!(
        state.objects,
        HashMap::from([("unrelated/published".into(), b"keep".to_vec())])
    );
}
