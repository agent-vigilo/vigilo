//! PostgreSQL tests of durable control transitions and revision fencing.
use super::*;

async fn seed(pool: &PgPool) -> (Uuid, Uuid) {
    let run = Uuid::now_v7();
    let dataset = Uuid::now_v7();
    sqlx::query("INSERT INTO dataset_versions(dataset_version_id,dataset_id,dataset_version) VALUES($1,$1,'1')").bind(dataset).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO runs(id,run_key,dataset_id,dataset_version_id,dataset_version,evaluation_profile_id,evaluation_profile_version,profile_version_id,profile_hash,aggregation_policy_id,aggregation_policy_version,aggregation_policy_hash,agent_provider,agent_name,prompt_config_id,prompt_config_version,status) VALUES($1,$1::text,$2,$2,'1','profile','1','profile:1','hash','policy','1','hash','test','agent','prompt','1','running')")
        .bind(run).bind(dataset).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO database_placements(alias,role,status,database_url_env) VALUES('primary','control_and_shard','active','DATABASE_URL') ON CONFLICT DO NOTHING").execute(pool).await.unwrap();
    sqlx::query("INSERT INTO shard_placements(run_id,run_shard,database_alias,status) VALUES($1,0,'primary','active')").bind(run).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO run_chunks(id,run_id,run_shard,dataset_version_id,profile_group_id,ordinal_start,ordinal_end) VALUES($1,$1,0,$2,'default',0,10)").bind(run).bind(dataset).execute(pool).await.unwrap();
    (run, dataset)
}
fn delivery(run: Uuid, revision: i64, state: ExecutionControl) -> Delivery {
    Delivery {
        tenant_id: Uuid::nil(),
        run_id: run,
        database_alias: "primary".into(),
        revision,
        execution_control: state,
        claim_token: Uuid::now_v7(),
        applied: false,
    }
}
#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn transitions_are_durable_idempotent_and_stale_delivery_cannot_restore_execution(
    pool: PgPool,
) {
    let (run, _) = seed(&pool).await;
    let paused = request(&pool, run, ExecutionControl::Paused).await.unwrap();
    assert_eq!(paused.revision, 1);
    assert_eq!(paused.placement_count, 1);
    assert_eq!(paused.applied_placement_count, 0);
    assert_eq!(
        request(&pool, run, ExecutionControl::Paused)
            .await
            .unwrap()
            .revision,
        1
    );
    assert!(
        apply(&pool, &delivery(run, 1, ExecutionControl::Paused))
            .await
            .unwrap()
    );
    let resumed = request(&pool, run, ExecutionControl::Enabled)
        .await
        .unwrap();
    assert_eq!(resumed.revision, 2);
    apply(&pool, &delivery(run, 2, ExecutionControl::Enabled))
        .await
        .unwrap();
    apply(&pool, &delivery(run, 1, ExecutionControl::Paused))
        .await
        .unwrap();
    let state: ExecutionControl =
        sqlx::query_scalar("SELECT execution_control FROM run_chunks WHERE run_id=$1")
            .bind(run)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(state, ExecutionControl::Enabled);
    request(&pool, run, ExecutionControl::Revoked)
        .await
        .unwrap();
    assert!(
        request(&pool, run, ExecutionControl::Enabled)
            .await
            .is_err()
    );
    assert!(request(&pool, run, ExecutionControl::Paused).await.is_err());
    assert!(
        apply(&pool, &delivery(run, 3, ExecutionControl::Enabled))
            .await
            .is_err()
    );
}
#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn application_does_not_claim_worker_quiescence_or_clear_a_live_lease(pool: PgPool) {
    let (run, _) = seed(&pool).await;
    sqlx::query("UPDATE run_chunks SET status='leased',lease_token=gen_random_uuid(),leased_until=clock_timestamp()+interval '60 seconds' WHERE run_id=$1").bind(run).execute(&pool).await.unwrap();
    request(&pool, run, ExecutionControl::Revoked)
        .await
        .unwrap();
    assert!(
        !apply(&pool, &delivery(run, 1, ExecutionControl::Revoked))
            .await
            .unwrap()
    );
    let state: (String, bool) =
        sqlx::query_as("SELECT status,lease_token IS NOT NULL FROM run_chunks WHERE run_id=$1")
            .bind(run)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(state, ("leased".into(), true));
    sqlx::query(
        "UPDATE run_chunks SET leased_until=clock_timestamp()-interval '1 second' WHERE run_id=$1",
    )
    .bind(run)
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        apply(&pool, &delivery(run, 1, ExecutionControl::Revoked))
            .await
            .unwrap()
    );
}
#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn failed_delivery_record_rolls_back_canonical_control(pool: PgPool) {
    let (run, _) = seed(&pool).await;
    sqlx::raw_sql("CREATE FUNCTION reject_control() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'delivery rejected'; END $$; CREATE TRIGGER reject_control BEFORE INSERT ON run_control_deliveries FOR EACH ROW EXECUTE FUNCTION reject_control();").execute(&pool).await.unwrap();
    assert!(request(&pool, run, ExecutionControl::Paused).await.is_err());
    let state: (ExecutionControl, i64) =
        sqlx::query_as("SELECT execution_control,control_revision FROM runs WHERE id=$1")
            .bind(run)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(state, (ExecutionControl::Enabled, 0));
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn seeded_chunks_inherit_control_and_copy_repair_keeps_the_newer_revision(pool: PgPool) {
    let (run, dataset) = seed(&pool).await;
    request(&pool, run, ExecutionControl::Paused).await.unwrap();
    apply(&pool, &delivery(run, 1, ExecutionControl::Paused))
        .await
        .unwrap();
    let id = Uuid::now_v7();
    let mut tx = pool.begin().await.unwrap();
    super::super::run_create::bulk_insert_run_chunks(
        &mut tx,
        run,
        dataset,
        &[crate::models::run_chunk::RunChunkDraft {
            chunk_id: id,
            run_shard: 0,
            profile_group_id: "default".into(),
            ordinal_start: 10,
            ordinal_end: 20,
        }],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let state: (ExecutionControl, i64) = sqlx::query_as(
        "SELECT execution_control,control_revision FROM run_chunks WHERE run_id=$1 AND id=$2",
    )
    .bind(run)
    .bind(id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(state, (ExecutionControl::Paused, 1));
    request(&pool, run, ExecutionControl::Revoked)
        .await
        .unwrap();
    apply(&pool, &delivery(run, 2, ExecutionControl::Revoked))
        .await
        .unwrap();
    // Simulate copied stale chunk metadata arriving after the newer delivery.
    sqlx::query("UPDATE run_chunks SET execution_control='enabled',control_revision=0 WHERE run_id=$1 AND id=$2").bind(run).bind(id).execute(&pool).await.unwrap();
    apply(&pool, &delivery(run, 1, ExecutionControl::Paused))
        .await
        .unwrap();
    let state: (ExecutionControl, i64) = sqlx::query_as(
        "SELECT execution_control,control_revision FROM run_chunks WHERE run_id=$1 AND id=$2",
    )
    .bind(run)
    .bind(id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(state, (ExecutionControl::Revoked, 2));
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn control_query_volume_is_bounded_by_placements_not_case_count(pool: PgPool) {
    use crate::db::workflows::run_shard_summary::test_support::record_queries;
    // Isolate connection warmup so cold and steady-state exchanges are exact.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    let (run, _) = seed(&pool).await;
    let start = std::time::Instant::now();
    let (result, queries) = record_queries(request(&pool, run, ExecutionControl::Paused)).await;
    let result = result.unwrap();
    assert_eq!(result.placement_count, 1);
    // SQLx resolves the new PostgreSQL enum on the first use of a connection.
    // Include these catalog exchanges rather than hiding cold-start costs.
    assert_eq!(queries.len(), 6, "{queries:#?}");
    eprintln!(
        "cold control request: {} SQL submissions (including 2 enum catalog reads) + BEGIN/COMMIT; elapsed {:?}",
        queries.len(),
        start.elapsed()
    );
    let start = std::time::Instant::now();
    let (result, queries) =
        record_queries(apply(&pool, &delivery(run, 1, ExecutionControl::Paused))).await;
    assert!(result.unwrap());
    assert_eq!(queries.len(), 4, "{queries:#?}");
    eprintln!(
        "control placement apply: {} SQL submissions + BEGIN/COMMIT; elapsed {:?}",
        queries.len(),
        start.elapsed()
    );
    let (result, queries) = record_queries(request(&pool, run, ExecutionControl::Enabled)).await;
    assert_eq!(result.unwrap().revision, 2);
    assert_eq!(queries.len(), 4, "{queries:#?}");
    let (result, queries) = record_queries(observe_quiescence(
        &pool,
        &delivery(run, 2, ExecutionControl::Enabled),
    ))
    .await;
    assert!(result.unwrap());
    assert_eq!(queries.len(), 1, "{queries:#?}");
    pool.close().await;
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn lost_acknowledgment_replays_and_stale_claim_cannot_ack_a_new_revision(pool: PgPool) {
    let (run, _) = seed(&pool).await;
    request(&pool, run, ExecutionControl::Paused).await.unwrap();
    let old = delivery(run, 1, ExecutionControl::Paused);
    sqlx::query("UPDATE run_control_deliveries SET claim_token=$2,claimed_until=clock_timestamp()+interval '60 seconds' WHERE run_id=$1")
        .bind(run).bind(old.claim_token).execute(&pool).await.unwrap();
    assert!(apply(&pool, &old).await.unwrap());
    let replacement = delivery(run, 1, ExecutionControl::Paused);
    sqlx::query("UPDATE run_control_deliveries SET claim_token=$2 WHERE run_id=$1")
        .bind(run)
        .bind(replacement.claim_token)
        .execute(&pool)
        .await
        .unwrap();
    assert!(!acknowledge(&pool, &old, true).await.unwrap());
    assert!(apply(&pool, &replacement).await.unwrap());
    assert!(acknowledge(&pool, &replacement, true).await.unwrap());
    let progress = progress(&pool, run).await.unwrap();
    assert_eq!(progress.applied_placement_count, 1);
    assert_eq!(progress.quiescent_placement_count, 1);
    assert!(!progress.settled);
    request(&pool, run, ExecutionControl::Enabled)
        .await
        .unwrap();
    assert!(!acknowledge(&pool, &replacement, true).await.unwrap());
    let progress = super::progress(&pool, run).await.unwrap();
    assert_eq!(progress.applied_placement_count, 0);
    assert_eq!(progress.revision, 2);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn restricted_delivery_claim_requires_grant_and_preserves_rollback(pool: PgPool) {
    let (run, _) = seed(&pool).await;
    request(&pool, run, ExecutionControl::Paused).await.unwrap();
    let caller = format!("delivery_caller_{}", Uuid::new_v4().simple());
    let owner = format!("delivery_owner_{}", Uuid::new_v4().simple());
    let mut tx = pool.begin().await.unwrap();
    sqlx::raw_sql(&format!("CREATE ROLE {caller} NOLOGIN; GRANT USAGE ON SCHEMA public TO {caller}; SET LOCAL ROLE {caller};"))
        .execute(&mut *tx).await.unwrap();
    sqlx::raw_sql("DO $$ BEGIN BEGIN PERFORM * FROM public.vigilo_claim_control_deliveries(NULL); RAISE EXCEPTION 'unexpected permission'; EXCEPTION WHEN insufficient_privilege THEN NULL; END; END $$;")
        .execute(&mut *tx).await.unwrap();
    sqlx::raw_sql(&format!("RESET ROLE; CREATE ROLE {owner} NOLOGIN; GRANT USAGE ON SCHEMA public TO {owner}; GRANT SELECT,UPDATE ON public.run_control_deliveries TO {owner}; ALTER FUNCTION public.vigilo_claim_control_deliveries(uuid) OWNER TO {owner}; GRANT EXECUTE ON FUNCTION public.vigilo_claim_control_deliveries(uuid) TO {caller}; SET LOCAL ROLE {caller};"))
        .execute(&mut *tx).await.unwrap();
    let access:bool=sqlx::query_scalar("SELECT has_table_privilege(current_user,'run_control_deliveries','SELECT') OR has_table_privilege(current_user,'run_control_deliveries','UPDATE')")
        .fetch_one(&mut *tx).await.unwrap();
    assert!(!access);
    let first: Delivery = sqlx::query_as("SELECT * FROM vigilo_claim_control_deliveries($1)")
        .bind(run)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(first.run_id, run);
    assert_eq!(first.revision, 1);
    assert_eq!(first.execution_control, ExecutionControl::Paused);
    let competing: i64 =
        sqlx::query_scalar("SELECT count(*) FROM vigilo_claim_control_deliveries($1)")
            .bind(run)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(competing, 0);
    tx.rollback().await.unwrap();
    let replay: Delivery = sqlx::query_as("SELECT * FROM vigilo_claim_control_deliveries($1)")
        .bind(run)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_ne!(first.claim_token, replay.claim_token);
    assert!(!acknowledge(&pool, &first, true).await.unwrap());
    assert!(acknowledge(&pool, &replay, true).await.unwrap());
    let attempts: i32 =
        sqlx::query_scalar("SELECT attempts FROM run_control_deliveries WHERE run_id=$1")
            .bind(run)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(attempts, 1);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn delivery_claim_batches_retry_timing_and_query_count_are_preserved(pool: PgPool) {
    let (run, _) = seed(&pool).await;
    request(&pool, run, ExecutionControl::Paused).await.unwrap();
    sqlx::query("INSERT INTO database_placements(alias,role,status,database_url_env) SELECT 'delivery-'||n,'shard','active','DATABASE_URL' FROM generate_series(1,34) n")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO run_control_deliveries(tenant_id,run_id,database_alias,revision,execution_control) SELECT tenant_id,id,'delivery-'||n,1,'paused' FROM runs CROSS JOIN generate_series(1,34) n WHERE id=$1")
        .bind(run).execute(&pool).await.unwrap();
    let first: Vec<Delivery> = sqlx::query_as("SELECT * FROM vigilo_claim_control_deliveries($1)")
        .bind(run)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(first.len(), 32);
    let (second, queries) = crate::db::workflows::run_shard_summary::test_support::record_queries(
        sqlx::query_scalar::<_, Uuid>(
            "SELECT claim_token FROM vigilo_claim_control_deliveries($1)",
        )
        .bind(run)
        .fetch_all(&pool),
    )
    .await;
    assert_eq!(second.unwrap().len(), 3);
    assert_eq!(queries.len(), 1, "{queries:#?}");
    let remaining: i64 =
        sqlx::query_scalar("SELECT count(*) FROM vigilo_claim_control_deliveries(NULL)")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(remaining, 0);
    let old = &first[0];
    sqlx::query("UPDATE run_control_deliveries SET claimed_until=clock_timestamp()-interval '1 second',available_at=clock_timestamp()+interval '1 hour',applied_at=clock_timestamp() WHERE run_id=$1 AND database_alias=$2")
        .bind(run).bind(&old.database_alias).execute(&pool).await.unwrap();
    let deferred: i64 =
        sqlx::query_scalar("SELECT count(*) FROM vigilo_claim_control_deliveries($1)")
            .bind(run)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(deferred, 0);
    sqlx::query("UPDATE run_control_deliveries SET available_at=clock_timestamp()-interval '1 second' WHERE run_id=$1 AND database_alias=$2")
        .bind(run).bind(&old.database_alias).execute(&pool).await.unwrap();
    let wrong_run: i64 =
        sqlx::query_scalar("SELECT count(*) FROM vigilo_claim_control_deliveries($1)")
            .bind(Uuid::now_v7())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(wrong_run, 0);
    let retry: Delivery = sqlx::query_as("SELECT * FROM vigilo_claim_control_deliveries($1)")
        .bind(run)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(retry.applied);
    assert_ne!(retry.claim_token, old.claim_token);
    assert!(!acknowledge(&pool, old, true).await.unwrap());
    assert!(acknowledge(&pool, &retry, true).await.unwrap());
    let done: i64 = sqlx::query_scalar("SELECT count(*) FROM vigilo_claim_control_deliveries($1)")
        .bind(run)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(done, 0);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn restricted_settlement_claim_requires_grants_quiescence_and_rollback(pool: PgPool) {
    let (run, _) = seed(&pool).await;
    request(&pool, run, ExecutionControl::Revoked)
        .await
        .unwrap();
    sqlx::query("INSERT INTO database_placements(alias,role,status,database_url_env) VALUES('settlement-second','shard','active','DATABASE_URL')")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO run_control_deliveries(tenant_id,run_id,database_alias,revision,execution_control) SELECT tenant_id,run_id,'settlement-second',revision,execution_control FROM run_control_deliveries WHERE run_id=$1")
        .bind(run).execute(&pool).await.unwrap();
    let caller = format!("settlement_caller_{}", Uuid::new_v4().simple());
    let owner = format!("settlement_owner_{}", Uuid::new_v4().simple());
    let mut tx = pool.begin().await.unwrap();
    sqlx::raw_sql(&format!("CREATE ROLE {caller} NOLOGIN; GRANT USAGE ON SCHEMA public TO {caller}; SET LOCAL ROLE {caller};"))
        .execute(&mut *tx).await.unwrap();
    sqlx::raw_sql("DO $$ BEGIN BEGIN PERFORM * FROM public.vigilo_claim_revocation_settlements(NULL); RAISE EXCEPTION 'unexpected permission'; EXCEPTION WHEN insufficient_privilege THEN NULL; END; END $$;")
        .execute(&mut *tx).await.unwrap();
    sqlx::raw_sql(&format!("RESET ROLE; CREATE ROLE {owner} NOLOGIN; GRANT USAGE ON SCHEMA public TO {owner}; GRANT SELECT,UPDATE ON public.runs TO {owner}; GRANT SELECT ON public.run_control_deliveries TO {owner}; ALTER FUNCTION public.vigilo_claim_revocation_settlements(uuid) OWNER TO {owner}; GRANT EXECUTE ON FUNCTION public.vigilo_claim_revocation_settlements(uuid) TO {caller}; SET LOCAL ROLE {caller};"))
        .execute(&mut *tx).await.unwrap();
    let access:bool=sqlx::query_scalar("SELECT has_table_privilege(current_user,'runs','SELECT') OR has_table_privilege(current_user,'runs','UPDATE') OR has_table_privilege(current_user,'run_control_deliveries','SELECT')")
        .fetch_one(&mut *tx).await.unwrap();
    assert!(!access);
    let blocked: i64 =
        sqlx::query_scalar("SELECT count(*) FROM vigilo_claim_revocation_settlements($1)")
            .bind(run)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert_eq!(blocked, 0);
    sqlx::query("UPDATE run_control_deliveries SET quiescent_at=clock_timestamp() WHERE run_id=$1 AND database_alias='primary'")
        .bind(run)
        .execute(&pool)
        .await
        .unwrap();
    let partial: i64 =
        sqlx::query_scalar("SELECT count(*) FROM vigilo_claim_revocation_settlements($1)")
            .bind(run)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert_eq!(partial, 0, "every destination must be quiescent");
    sqlx::query("UPDATE run_control_deliveries SET quiescent_at=clock_timestamp() WHERE run_id=$1")
        .bind(run)
        .execute(&pool)
        .await
        .unwrap();
    let first: (Uuid, i64, Uuid) =
        sqlx::query_as("SELECT * FROM vigilo_claim_revocation_settlements($1)")
            .bind(run)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert_eq!((first.0, first.1), (run, 1));
    let competing: i64 =
        sqlx::query_scalar("SELECT count(*) FROM vigilo_claim_revocation_settlements($1)")
            .bind(run)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(competing, 0);
    tx.rollback().await.unwrap();
    let retry: (Uuid, i64, Uuid) =
        sqlx::query_as("SELECT * FROM vigilo_claim_revocation_settlements($1)")
            .bind(run)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_ne!(first.2, retry.2);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn settlement_claim_preserves_batch_revision_lease_and_query_budgets(pool: PgPool) {
    let mut ids = Vec::new();
    for _ in 0..18 {
        let (run, _) = seed(&pool).await;
        request(&pool, run, ExecutionControl::Revoked)
            .await
            .unwrap();
        ids.push(run);
    }
    let blocked: i64 =
        sqlx::query_scalar("SELECT count(*) FROM vigilo_claim_revocation_settlements(NULL)")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(blocked, 0);
    sqlx::query("UPDATE run_control_deliveries SET quiescent_at=clock_timestamp(),revision=2")
        .execute(&pool)
        .await
        .unwrap();
    let stale: i64 =
        sqlx::query_scalar("SELECT count(*) FROM vigilo_claim_revocation_settlements(NULL)")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(stale, 0);
    sqlx::query("UPDATE run_control_deliveries SET revision=1")
        .execute(&pool)
        .await
        .unwrap();
    let wrong: i64 =
        sqlx::query_scalar("SELECT count(*) FROM vigilo_claim_revocation_settlements($1)")
            .bind(Uuid::now_v7())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(wrong, 0);
    let (first, queries) = crate::db::workflows::run_shard_summary::test_support::record_queries(
        sqlx::query_as::<_, (Uuid, i64, Uuid)>(
            "SELECT * FROM vigilo_claim_revocation_settlements(NULL)",
        )
        .fetch_all(&pool),
    )
    .await;
    let first = first.unwrap();
    assert_eq!(queries.len(), 1, "{queries:#?}");
    assert_eq!(first.len(), 16);
    let second: Vec<(Uuid, i64, Uuid)> =
        sqlx::query_as("SELECT * FROM vigilo_claim_revocation_settlements(NULL)")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(second.len(), 2);
    let actual = first
        .iter()
        .chain(&second)
        .map(|r| r.0)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(actual, ids.into_iter().collect());
    let live: i64 =
        sqlx::query_scalar("SELECT count(*) FROM vigilo_claim_revocation_settlements(NULL)")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(live, 0);
    let old = first[0];
    sqlx::query("UPDATE runs SET control_settlement_until=clock_timestamp()-interval '1 second' WHERE id=$1")
        .bind(old.0).execute(&pool).await.unwrap();
    let retry: (Uuid, i64, Uuid) =
        sqlx::query_as("SELECT * FROM vigilo_claim_revocation_settlements($1)")
            .bind(old.0)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_ne!(old.2, retry.2);
    assert!(
        !acknowledge_settlement(&pool, Uuid::nil(), old.0, old.1, old.2)
            .await
            .unwrap()
    );
    assert!(
        !acknowledge_settlement(&pool, Uuid::nil(), retry.0, retry.1 + 1, retry.2)
            .await
            .unwrap()
    );
    assert!(
        acknowledge_settlement(&pool, Uuid::nil(), retry.0, retry.1, retry.2)
            .await
            .unwrap()
    );
    let settled: i64 =
        sqlx::query_scalar("SELECT count(*) FROM vigilo_claim_revocation_settlements($1)")
            .bind(retry.0)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(settled, 0);
}

#[path = "run_control_scope_tests.rs"]
mod scope_tests;

#[path = "revocation_scope_tests.rs"]
mod revocation_scope_tests;

/// A finishing result must pass its parent FK while control waits on its chunk.
/// Otherwise parent/chunk lock inversion aborts the worker and delays quiescence.
#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn control_delivery_allows_finishing_result_foreign_keys(pool: PgPool) {
    use std::time::Duration;

    for state in [ExecutionControl::Paused, ExecutionControl::Revoked] {
        let (run, _) = seed(&pool).await;
        let execution = Uuid::now_v7();
        let attempt = Uuid::now_v7();
        sqlx::query("INSERT INTO executions(id,run_id,run_shard,chunk_id,case_id,case_hash,profile_group_id,task_type,evaluation_profile_id,evaluation_profile_version) VALUES($1,$2,0,$2,$1,'fixture','default','classification','fixture','1')")
            .bind(execution).bind(run).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO execution_attempts(id,execution_id,run_id,run_shard,attempt_no) VALUES($1,$2,$3,0,1)")
            .bind(attempt).bind(execution).bind(run).execute(&pool).await.unwrap();
        sqlx::query("UPDATE run_chunks SET status='leased',lease_token=gen_random_uuid(),leased_until=clock_timestamp()+interval '60 seconds' WHERE run_id=$1")
            .bind(run).execute(&pool).await.unwrap();
        let mut worker = pool.begin().await.unwrap();
        let worker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *worker)
            .await
            .unwrap();
        sqlx::query("SELECT id FROM run_chunks WHERE run_id=$1 FOR UPDATE")
            .bind(run)
            .execute(&mut *worker)
            .await
            .unwrap();
        let control_pool = pool.clone();
        let mut control =
            tokio::spawn(async move { apply(&control_pool, &delivery(run, 1, state)).await });
        let waiting = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND $1=ANY(pg_blocking_pids(pid)) AND query LIKE 'UPDATE run_chunks SET execution_control%')")
                    .bind(worker_pid).fetch_one(&pool).await.unwrap();
                if blocked { break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await;
        sqlx::query("SET LOCAL lock_timeout='500ms'")
            .execute(&mut *worker)
            .await
            .unwrap();
        let result = sqlx::query("INSERT INTO evaluator_results(run_id,run_shard,execution_id,attempt_id,binding_id,evaluator_id,evaluator_version,evaluator_profile_id,evaluator_profile_version,dimension,outcome,error_code,error_message,normalization_policy_hash,pass_threshold) VALUES($1,0,$2,$3,'fixture',$2,'1','fixture','1','quality','error','fixture','retained','fixture',0.5)")
            .bind(run).bind(execution).bind(attempt).execute(&mut *worker).await;
        if result.is_ok() {
            worker.commit().await.unwrap();
        } else {
            worker.rollback().await.unwrap();
        }
        let applied = tokio::time::timeout(Duration::from_secs(5), &mut control).await;
        if applied.is_err() {
            control.abort();
            let _ = control.await;
        }
        assert!(waiting.is_ok(), "control never reached the owned chunk");
        assert!(
            result.is_ok(),
            "finishing result was blocked by control: {result:?}"
        );
        assert!(
            !applied.unwrap().unwrap().unwrap(),
            "live lease was incorrectly reported quiescent"
        );
        let retained: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM evaluator_results WHERE run_id=$1 AND error_message='retained'",
        )
        .bind(run)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(retained, 1);
        let current: (ExecutionControl,i64,bool) = sqlx::query_as("SELECT execution_control,control_revision,lease_token IS NOT NULL FROM run_chunks WHERE run_id=$1")
            .bind(run).fetch_one(&pool).await.unwrap();
        assert_eq!(current, (state, 1, true));
    }
}
