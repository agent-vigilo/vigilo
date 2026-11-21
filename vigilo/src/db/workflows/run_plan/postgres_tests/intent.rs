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
async fn execution_intent_does_not_authorize_a_published_plan(db: PgPool) {
    let f = Fixture::with_size(&db, 2, 2, 2).await;
    let lease = claim(&db, f.plan.run_id).await.unwrap();
    f.prepare(&db, lease).await.unwrap();
    assert!(replay(&db, &f, true).await.unwrap().execution_requested);
    assert!(
        admission::claim(&db, f.plan.run_id).await.is_err(),
        "requesting execution must not approve an immutable plan"
    );
    let stats = crate::run_admission::recover(&runtime_context(&db), 1, Default::default())
        .await
        .unwrap();
    assert_eq!(
        stats.claimed_runs, 0,
        "recovery must not manufacture approval"
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn acceptance_binds_exact_root_and_survives_replay_without_replacement(db: PgPool) {
    let f = Fixture::with_size(&db, 2, 2, 2).await;
    let lease = claim(&db, f.plan.run_id).await.unwrap();
    let root = f.prepare(&db, lease).await.unwrap();
    let other = Uuid::now_v7();
    let other_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(move |connection, _| {
            Box::pin(async move {
                sqlx::query("SELECT set_config('vigilo.tenant_id',$1,false)")
                    .bind(other.to_string())
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect_with(db.connect_options().as_ref().clone())
        .await
        .unwrap();
    assert!(
        admission::accept(&other_pool, f.plan.run_id, &root)
            .await
            .is_err()
    );
    other_pool.close().await;
    let mut changed = root.clone();
    changed.key.push_str("-different");
    assert!(
        admission::accept(&db, f.plan.run_id, &changed)
            .await
            .is_err()
    );
    assert!(admission::claim(&db, f.plan.run_id).await.is_err());
    sqlx::raw_sql("CREATE FUNCTION fail_acceptance() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.accepted_manifest IS NOT NULL THEN RAISE EXCEPTION 'acceptance commit interrupted'; END IF; RETURN NULL; END $$; CREATE CONSTRAINT TRIGGER fail_acceptance AFTER UPDATE ON run_plans DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fail_acceptance();")
        .execute(&db).await.unwrap();
    assert!(
        format!(
            "{:#}",
            admission::accept(&db, f.plan.run_id, &root)
                .await
                .unwrap_err()
        )
        .contains("acceptance commit interrupted")
    );
    assert!(admission::claim(&db, f.plan.run_id).await.is_err());
    sqlx::query("DROP TRIGGER fail_acceptance ON run_plans")
        .execute(&db)
        .await
        .unwrap();
    admission::accept(&db, f.plan.run_id, &root).await.unwrap();
    admission::accept(&db, f.plan.run_id, &root).await.unwrap();
    assert!(
        admission::accept(&db, f.plan.run_id, &changed)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE run_plans SET manifest=$2 WHERE run_id=$1")
            .bind(f.plan.run_id)
            .bind(Json(&changed))
            .execute(&db)
            .await
            .is_err()
    );
    let lease = admission::claim(&db, f.plan.run_id).await.unwrap();
    release(&db, lease, None).await.unwrap();
    sqlx::query("UPDATE runs SET status='cancelled' WHERE id=$1")
        .bind(f.plan.run_id)
        .execute(&db)
        .await
        .unwrap();
    assert!(admission::accept(&db, f.plan.run_id, &root).await.is_err());
    assert!(admission::claim(&db, f.plan.run_id).await.is_err());
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn acceptance_rechecks_cancellation_after_waiting_for_run_lock(db: PgPool) {
    let f = Fixture::with_size(&db, 2, 2, 2).await;
    let owner = claim(&db, f.plan.run_id).await.unwrap();
    let root = f.prepare(&db, owner).await.unwrap();
    let mut tx = db.begin().await.unwrap();
    sqlx::query("SELECT id FROM runs WHERE id=$1 FOR UPDATE")
        .bind(f.plan.run_id)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let pool = db.clone();
    let id = f.plan.run_id;
    let task = tokio::spawn(async move { admission::accept(&pool, id, &root).await });
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
        .bind(id)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(task.await.unwrap().is_err());
    let absent: bool =
        sqlx::query_scalar("SELECT accepted_manifest IS NULL FROM run_plans WHERE run_id=$1")
            .bind(id)
            .fetch_one(&db)
            .await
            .unwrap();
    assert!(
        absent,
        "a waiting acceptance must not approve cancelled work"
    );
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
    assert!(admission::claim(&db, f.plan.run_id).await.is_err());
    admission::accept(
        &db,
        f.plan.run_id,
        load(&db, f.plan.run_id)
            .await
            .unwrap()
            .unwrap()
            .manifest
            .as_ref()
            .unwrap(),
    )
    .await
    .unwrap();
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
