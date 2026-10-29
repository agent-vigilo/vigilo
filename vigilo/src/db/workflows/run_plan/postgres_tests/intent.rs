//! Execution intent commits with validated creation identity and remains
//! monotonic on replay. Run locking serializes intent against cancellation.
use super::*;

async fn replay(db: &PgPool, f: &Fixture, execute: bool) -> Result<Plan> {
    begin(
        db,
        f.plan.run_id,
        f.upload.id,
        &f.upload.binding_hash,
        &f.plan.limits,
        &f.draft,
        &f.plan.execution_plan,
        &f.plan.assignments,
        execute,
    )
    .await
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn fresh_creation_and_execution_intent_commit_together(db: PgPool) {
    let f = Fixture::with_size(&db, 2, 2, 2).await;
    let id = Uuid::now_v7();
    let mut draft = f.draft.clone();
    draft.run_key = id.to_string();
    sqlx::raw_sql("CREATE FUNCTION reject_requested_plan() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.execution_requested THEN RAISE EXCEPTION 'creation commit interrupted'; END IF; RETURN NULL; END $$; CREATE CONSTRAINT TRIGGER reject_requested_plan AFTER INSERT ON run_plans DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_requested_plan();")
        .execute(&db).await.unwrap();
    let error = begin(
        &db,
        id,
        f.upload.id,
        &f.upload.binding_hash,
        &f.plan.limits,
        &draft,
        &f.plan.execution_plan,
        &f.plan.assignments,
        true,
    )
    .await
    .unwrap_err();
    assert!(format!("{error:#}").contains("creation commit interrupted"));
    let absent: bool = sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM runs WHERE id=$1) AND NOT EXISTS(SELECT 1 FROM run_plans WHERE run_id=$1)")
        .bind(id).fetch_one(&db).await.unwrap();
    assert!(
        absent,
        "a failed commit must leave neither identity nor intent"
    );
    sqlx::query("DROP TRIGGER reject_requested_plan ON run_plans")
        .execute(&db)
        .await
        .unwrap();
    let plan = begin(
        &db,
        id,
        f.upload.id,
        &f.upload.binding_hash,
        &f.plan.limits,
        &draft,
        &f.plan.execution_plan,
        &f.plan.assignments,
        true,
    )
    .await
    .unwrap();
    assert!(plan.execution_requested);
    assert!(load(&db, id).await.unwrap().unwrap().execution_requested);
    assert!(
        begin(
            &db,
            id,
            f.upload.id,
            &f.upload.binding_hash,
            &f.plan.limits,
            &draft,
            &f.plan.execution_plan,
            &f.plan.assignments,
            false
        )
        .await
        .unwrap()
        .execution_requested
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn held_plan_requests_execution_only_after_replay_validation(db: PgPool) {
    let f = Fixture::with_size(&db, 2, 2, 2).await;
    let lease = claim(&db, f.plan.run_id).await.unwrap();
    f.prepare(&db, lease).await.unwrap();
    assert!(!replay(&db, &f, false).await.unwrap().execution_requested);
    assert!(admission::claim(&db, f.plan.run_id).await.is_err());
    let mut limits = f.plan.limits.0.clone();
    limits.page_entries += 1;
    let error = begin(
        &db,
        f.plan.run_id,
        f.upload.id,
        &f.upload.binding_hash,
        &limits,
        &f.draft,
        &f.plan.execution_plan,
        &f.plan.assignments,
        true,
    )
    .await
    .unwrap_err();
    assert!(format!("{error:#}").contains("run plan replay changed immutable inputs"));
    assert!(
        !load(&db, f.plan.run_id)
            .await
            .unwrap()
            .unwrap()
            .execution_requested
    );
    assert!(replay(&db, &f, true).await.unwrap().execution_requested);
    assert!(replay(&db, &f, false).await.unwrap().execution_requested);
    let admission = admission::claim(&db, f.plan.run_id).await.unwrap();
    release(&db, admission, None).await.unwrap();
    // Terminal runs cannot acquire new intent through a replay.
    let stopped = Fixture::with_size(&db, 2, 2, 2).await;
    sqlx::query("UPDATE runs SET status='failed' WHERE id=$1")
        .bind(stopped.plan.run_id)
        .execute(&db)
        .await
        .unwrap();
    assert!(
        !replay(&db, &stopped, true)
            .await
            .unwrap()
            .execution_requested
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn cancellation_while_creation_waits_cannot_enable_execution(db: PgPool) {
    let f = Fixture::with_size(&db, 2, 2, 2).await;
    let mut lock = db.begin().await.unwrap();
    sqlx::query("SELECT id FROM runs WHERE id=$1 FOR UPDATE")
        .bind(f.plan.run_id)
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    let pool = db.clone();
    let plan = f.plan.clone();
    let upload = f.upload.clone();
    let draft = f.draft.clone();
    let task = tokio::spawn(async move {
        begin(
            &pool,
            plan.run_id,
            upload.id,
            &upload.binding_hash,
            &plan.limits,
            &draft,
            &plan.execution_plan,
            &plan.assignments,
            true,
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))",
            )
            .bind(blocker)
            .fetch_one(&db)
            .await
            .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    sqlx::query("UPDATE runs SET status='cancelled' WHERE id=$1")
        .bind(f.plan.run_id)
        .execute(&mut *lock)
        .await
        .unwrap();
    lock.commit().await.unwrap();
    let error = task.await.unwrap().unwrap_err();
    assert!(format!("{error:#}").contains("run was cancelled"));
    assert!(
        !load(&db, f.plan.run_id)
            .await
            .unwrap()
            .unwrap()
            .execution_requested
    );
}
