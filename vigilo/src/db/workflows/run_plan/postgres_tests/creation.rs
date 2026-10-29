//! Foreground input reuse keeps immutable identity separate from live authority.
//! State reporting observes committed status and progress together; stale input
//! snapshots cannot authorize publication or overwrite a competing creation.
use super::*;
use crate::db::workflows::run_creation::{
    RunCreationProgress,
    select_creation_progress,
    select_creation_state,
};

async fn replay_loaded_inputs(db: &PgPool, f: &Fixture, loaded: &Plan) -> Result<Plan> {
    begin(
        db,
        loaded.run_id,
        loaded.dataset_upload_id,
        &loaded.storage_binding_hash,
        &loaded.limits,
        &f.draft,
        &loaded.execution_plan,
        &loaded.assignments,
        false,
    )
    .await
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn reused_inputs_cannot_restore_cancelled_or_replaced_authority(db: PgPool) {
    let f = Fixture::with_size(&db, 2, 2, 2).await;
    let loaded = f.plan.clone();
    let old_owner = claim(&db, loaded.run_id).await.unwrap();
    sqlx::query("UPDATE runs SET coordinator_leased_until=clock_timestamp()-interval '1 second' WHERE id=$1")
        .bind(loaded.run_id).execute(&db).await.unwrap();
    let replacement = claim(&db, loaded.run_id).await.unwrap();
    // Identical immutable inputs may replay, but convey no ownership rights.
    replay_loaded_inputs(&db, &f, &loaded).await.unwrap();
    assert!(claim(&db, loaded.run_id).await.is_err());
    assert!(f.prepare(&db, old_owner).await.is_err());
    // Cleanup after an uncertain old-owner outcome cannot clear its successor.
    release(&db, old_owner, Some("old outcome uncertain"))
        .await
        .unwrap();
    renew(&db, replacement).await.unwrap();
    sqlx::query("UPDATE runs SET status='cancelled',error_message='cancelled after loading inputs' WHERE id=$1")
        .bind(loaded.run_id).execute(&db).await.unwrap();
    assert!(replay_loaded_inputs(&db, &f, &loaded).await.is_err());
    assert!(f.prepare(&db, replacement).await.is_err());
    let state = select_creation_state(&db, loaded.run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.status, "cancelled");
    assert_eq!(
        state.error.as_deref(),
        Some("cancelled after loading inputs")
    );
    assert_eq!(state.progress.last_error, state.error);
    assert_eq!(state.progress.plan_status.as_deref(), Some("preparing"));
    assert!(
        load(&db, loaded.run_id)
            .await
            .unwrap()
            .unwrap()
            .manifest
            .is_none()
    );
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn reused_finalized_dataset_and_plan_inputs_preserve_publication_and_replay(db: PgPool) {
    let f = Fixture::with_size(&db, 2, 2, 2).await;
    let upload = dataset_upload::load(&db, f.upload.id).await.unwrap();
    assert_eq!(upload.status, "finalized");
    assert!(dataset_upload::abort(&db, upload.id).await.is_err());
    sqlx::query(
        "UPDATE dataset_uploads SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1",
    )
    .bind(upload.id)
    .execute(&db)
    .await
    .unwrap();
    assert!(
        !dataset_upload::expire(&db)
            .await
            .unwrap()
            .contains(&upload.id)
    );
    let loaded = f.plan.clone();
    replay_loaded_inputs(&db, &f, &loaded).await.unwrap();
    let lease = claim(&db, loaded.run_id).await.unwrap();
    let root = f.prepare(&db, lease).await.unwrap();
    // The caller's old snapshot still says preparing; begin returns live state.
    assert_eq!(loaded.status, "preparing");
    let replay = replay_loaded_inputs(&db, &f, &loaded).await.unwrap();
    assert_eq!(replay.status, "ready");
    assert_eq!(replay.manifest.unwrap().0, root);
    assert!(claim(&db, loaded.run_id).await.is_err());
    let mut changed = loaded.clone();
    changed.assignments.0[0].database_alias = "different".into();
    assert!(replay_loaded_inputs(&db, &f, &changed).await.is_err());
}

#[sqlx::test(migrations = "../migrations")]
#[ignore = "requires PostgreSQL"]
async fn creation_state_reports_current_progress_and_missing_plan_defaults(db: PgPool) {
    let missing = Uuid::now_v7();
    assert!(select_creation_state(&db, missing).await.unwrap().is_none());
    assert_eq!(
        select_creation_progress(&db, missing).await.unwrap(),
        RunCreationProgress::default()
    );
    let f = Fixture::with_size(&db, 2, 2, 2).await;
    let id = f.plan.run_id;
    let lease = claim(&db, id).await.unwrap();
    f.prepare(&db, lease).await.unwrap();
    let before = select_creation_state(&db, id).await.unwrap().unwrap();
    assert_eq!(before.status, "creating");
    assert_eq!(before.progress.placement_count, 1);
    assert_eq!(before.progress.pending_placement_count, 1);
    assert_eq!(before.progress.seeded_placement_count, 0);
    let plan = load(&db, id).await.unwrap().unwrap();
    let context = runtime_context(&db);
    let store: Arc<dyn crate::blob_storage::BlobStorage> = Arc::new(f.store);
    assert!(
        crate::run_admission::resume(&context, &plan, store, Default::default(), &f.cancel)
            .await
            .unwrap()
    );
    let after = select_creation_state(&db, id).await.unwrap().unwrap();
    assert_eq!(after.status, "pending");
    assert_eq!(after.error, None);
    assert_eq!(after.progress.pending_placement_count, 0);
    assert_eq!(after.progress.seeded_placement_count, 1);
    assert_eq!(
        after.progress,
        select_creation_progress(&db, id).await.unwrap()
    );
    // Runs without blob plans retain the existing empty-progress contract.
    sqlx::query("DELETE FROM run_plans WHERE run_id=$1")
        .bind(id)
        .execute(&db)
        .await
        .unwrap();
    let state = select_creation_state(&db, id).await.unwrap().unwrap();
    assert_eq!(state.status, "pending");
    assert_eq!(state.progress, RunCreationProgress::default());
}
