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
