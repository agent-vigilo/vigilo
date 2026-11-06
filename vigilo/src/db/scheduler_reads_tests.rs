//! PostgreSQL contracts for scheduler-wide aggregate and deployment-routing reads.
//!
//! These fixtures verify restricted function access without granting the caller
//! work-table reads, plus eligibility and unchanged client statement counts.

use sqlx::PgPool;
use uuid::Uuid;

use super::{
    tables::{
        database_placements,
        outbox_events,
    },
    test_support::seed_run_owner,
    workflows::{
        run_dispatch,
        run_finalize,
        run_shard_summary::test_support::record_queries,
    },
};

const FUNCTIONS: &[&str] = &[
    "vigilo_dispatch_backlog",
    "vigilo_finalization_backlog",
    "vigilo_outbox_backlog",
    "vigilo_execution_aliases",
    "vigilo_unserviceable_routes",
];

async fn seed(pool: &PgPool) -> Uuid {
    let run = Uuid::now_v7();
    seed_run_owner(pool, run).await;
    sqlx::query(
        "UPDATE runs SET status='running',updated_at=now()-interval '2 minutes' WHERE id=$1",
    )
    .bind(run)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO shard_placements(run_id,run_shard,database_alias,status) VALUES($1,0,'primary','active'),($1,1,'primary','active')")
        .bind(run).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO run_shard_dispatch_cursors(run_id,run_shard,status) VALUES($1,0,'open'),($1,1,'open')")
        .bind(run).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO outbox_events(event_type,aggregate_type,aggregate_id,dedupe_key) VALUES('test','run',$1,$1::text)")
        .bind(run).execute(pool).await.unwrap();
    run
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn scheduler_reads_require_explicit_grants_without_table_access(pool: PgPool) {
    seed(&pool).await;
    let role = format!("scheduler_reader_{}", Uuid::new_v4().simple());
    let owner = format!("scheduler_read_owner_{}", Uuid::new_v4().simple());
    let mut tx = pool.begin().await.unwrap();
    sqlx::raw_sql(&format!(
        "CREATE ROLE {role} NOLOGIN; GRANT USAGE ON SCHEMA public TO {role}; SET LOCAL ROLE {role};"
    ))
    .execute(&mut *tx)
    .await
    .unwrap();
    for function in FUNCTIONS {
        sqlx::raw_sql(&format!("DO $$ BEGIN BEGIN PERFORM * FROM public.{function}(); RAISE EXCEPTION 'unexpected access'; EXCEPTION WHEN insufficient_privilege THEN NULL; END; END $$;"))
            .execute(&mut *tx).await.unwrap();
    }
    sqlx::raw_sql(&format!("RESET ROLE; CREATE ROLE {owner} NOLOGIN; GRANT USAGE ON SCHEMA public TO {owner}; GRANT SELECT ON public.runs,public.run_shard_dispatch_cursors,public.shard_placements,public.database_placements,public.outbox_delivery_queue TO {owner};"))
        .execute(&mut *tx).await.unwrap();
    for function in FUNCTIONS {
        sqlx::raw_sql(&format!("ALTER FUNCTION public.{function}() OWNER TO {owner}; GRANT EXECUTE ON FUNCTION public.{function}() TO {role};"))
            .execute(&mut *tx).await.unwrap();
    }
    sqlx::raw_sql(&format!("SET LOCAL ROLE {role};"))
        .execute(&mut *tx)
        .await
        .unwrap();
    for table in [
        "runs",
        "run_shard_dispatch_cursors",
        "shard_placements",
        "database_placements",
        "outbox_delivery_queue",
    ] {
        let access: bool =
            sqlx::query_scalar("SELECT has_table_privilege(current_user,$1,'SELECT')")
                .bind(format!("public.{table}"))
                .fetch_one(&mut *tx)
                .await
                .unwrap();
        assert!(!access, "unexpected access to {table}");
    }
    for (function, expected) in [
        ("vigilo_dispatch_backlog", serde_json::json!(2)),
        (
            "vigilo_finalization_backlog",
            serde_json::json!({"candidate_count":0,"oldest_candidate_lag_seconds":null}),
        ),
        ("vigilo_outbox_backlog", serde_json::json!(1)),
        ("vigilo_execution_aliases", serde_json::json!("primary")),
        ("vigilo_unserviceable_routes", serde_json::json!(0)),
    ] {
        let sql = format!("SELECT to_jsonb(v) FROM public.{function}() v");
        let (actual, calls) =
            record_queries(sqlx::query_scalar::<_, serde_json::Value>(&sql).fetch_one(&mut *tx))
                .await;
        assert_eq!(actual.unwrap(), expected);
        assert_eq!(calls.len(), 1);
    }
    tx.rollback().await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn scheduler_reads_preserve_empty_and_filtered_backlogs(pool: PgPool) {
    assert_eq!(
        run_dispatch::count_dispatch_cursor_backlog(&pool)
            .await
            .unwrap(),
        0
    );
    let empty = run_finalize::select_finalization_candidate_backlog(&pool)
        .await
        .unwrap();
    assert_eq!(empty.candidate_count, 0);
    assert_eq!(empty.oldest_candidate_lag_seconds, None);
    assert!(
        database_placements::list_serviceable_shard_database_aliases(&pool)
            .await
            .unwrap()
            .is_empty()
    );
    let run = seed(&pool).await;
    let (dispatch, calls) =
        record_queries(run_dispatch::count_dispatch_cursor_backlog(&pool)).await;
    assert_eq!(dispatch.unwrap(), 2);
    assert_eq!(calls.len(), 1);
    sqlx::query("UPDATE runs SET execution_control='paused' WHERE id=$1")
        .bind(run)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        run_dispatch::count_dispatch_cursor_backlog(&pool)
            .await
            .unwrap(),
        0
    );
    sqlx::query("UPDATE runs SET execution_control='enabled' WHERE id=$1")
        .bind(run)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE run_shard_dispatch_cursors SET status='drained' WHERE run_id=$1")
        .bind(run)
        .execute(&pool)
        .await
        .unwrap();
    let (backlog, calls) =
        record_queries(run_finalize::select_finalization_candidate_backlog(&pool)).await;
    let backlog = backlog.unwrap();
    assert_eq!(backlog.candidate_count, 1);
    assert!(backlog.oldest_candidate_lag_seconds.unwrap() >= 120);
    assert_eq!(calls.len(), 1);
    sqlx::query("UPDATE runs SET status='finalizing',coordinator_leased_until=now()+interval '1 hour' WHERE id=$1").bind(run).execute(&pool).await.unwrap();
    assert_eq!(
        run_finalize::select_finalization_candidate_backlog(&pool)
            .await
            .unwrap()
            .candidate_count,
        0
    );
    sqlx::query("UPDATE runs SET coordinator_leased_until=now()-interval '1 second' WHERE id=$1")
        .bind(run)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        run_finalize::select_finalization_candidate_backlog(&pool)
            .await
            .unwrap()
            .candidate_count,
        1
    );
    let (outbox, calls) =
        record_queries(outbox_events::count_publishable_outbox_backlog(&pool)).await;
    assert_eq!(outbox.unwrap(), 1);
    assert_eq!(calls.len(), 1);
    let claimed = outbox_events::claim_publishable_outbox_events(&pool, 1, 60)
        .await
        .unwrap();
    assert_eq!(
        outbox_events::count_publishable_outbox_backlog(&pool)
            .await
            .unwrap(),
        0
    );
    outbox_events::mark_outbox_event_published(
        &pool,
        claimed[0].id,
        claimed[0].claim_token.unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        outbox_events::count_publishable_outbox_backlog(&pool)
            .await
            .unwrap(),
        0
    );
    sqlx::query("INSERT INTO database_placements(alias,database_url_env,role,status) VALUES('drain','DRAIN_URL','shard','draining'),('off','OFF_URL','shard','disabled'),('empty','EMPTY_URL','shard','active'),('new','NEW_URL','shard','provisioning')").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO shard_placements(run_id,run_shard,database_alias,status,move_target_database_alias) VALUES($1,2,'drain','moving','primary'),($1,3,'off','active',NULL),($1,4,'new','active',NULL)")
        .bind(run).execute(&pool).await.unwrap();
    let (aliases, calls) =
        record_queries(database_placements::list_serviceable_shard_database_aliases(&pool)).await;
    assert_eq!(aliases.unwrap(), vec!["drain", "primary"]);
    assert_eq!(calls.len(), 1);
    let (invalid, calls) = record_queries(
        database_placements::count_shard_placements_on_unserviceable_databases(&pool),
    )
    .await;
    assert_eq!(invalid.unwrap(), 2);
    assert_eq!(calls.len(), 1);
}
