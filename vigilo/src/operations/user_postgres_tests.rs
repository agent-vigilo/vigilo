//! Database-backed ownership and pooled identity contract tests.
use sqlx::{
    ConnectOptions,
    PgPool,
};

use super::*;
use crate::db::workflows::run_shard_summary::test_support::record_queries;

async fn seed(pool: &PgPool, tenant: Uuid, run: Uuid, dataset: Uuid) {
    sqlx::query("INSERT INTO dataset_versions(tenant_id,dataset_version_id,dataset_id,dataset_version) VALUES($1,$2,$2,'1') ON CONFLICT DO NOTHING")
        .bind(tenant).bind(dataset).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO runs(tenant_id,id,run_key,dataset_id,dataset_version_id,dataset_version,evaluation_profile_id,evaluation_profile_version,profile_version_id,profile_hash,aggregation_policy_id,aggregation_policy_version,aggregation_policy_hash,agent_provider,agent_name,prompt_config_id,prompt_config_version,status) VALUES($1,$2,'same-key',$3,$3,'1','profile','1','profile:1','hash','policy','1','hash','test','agent','prompt','1','creating')")
        .bind(tenant).bind(run).bind(dataset).execute(pool).await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn ownership_constraints_preserve_global_runs_and_scoped_natural_keys(pool: PgPool) {
    let a = Uuid::now_v7();
    let b = Uuid::now_v7();
    let run = Uuid::now_v7();
    let dataset = Uuid::now_v7();
    seed(&pool, a, run, dataset).await;
    seed(&pool, b, Uuid::now_v7(), dataset).await;
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM dataset_versions WHERE dataset_version_id=$1")
            .bind(dataset)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 2);
    // An existing global run cannot be assigned to another owner by insertion.
    let error=sqlx::query("INSERT INTO runs SELECT (jsonb_populate_record(NULL::runs,to_jsonb(r)||jsonb_build_object('tenant_id',$2::uuid))).* FROM runs r WHERE id=$1")
        .bind(run).bind(b).execute(&pool).await.unwrap_err();
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("23505")
    );
    // Even with a valid run ID, a child cannot claim a different owner.
    let error = sqlx::query(
        "INSERT INTO run_shard_dispatch_cursors(tenant_id,run_id,run_shard) VALUES($1,$2,0)",
    )
    .bind(b)
    .bind(run)
    .execute(&pool)
    .await
    .unwrap_err();
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("23503")
    );
    // Partition-local writes retain the same ownership constraint.
    let chunk = Uuid::now_v7();
    sqlx::query("INSERT INTO run_chunks(tenant_id,id,run_id,run_shard,dataset_version_id,profile_group_id,ordinal_start,ordinal_end) VALUES($1,$2,$3,0,$4,'default',0,1)")
        .bind(a).bind(chunk).bind(run).bind(dataset).execute(&pool).await.unwrap();
    let error=sqlx::query("INSERT INTO executions(tenant_id,run_id,run_shard,chunk_id,case_id,case_hash,profile_group_id,task_type,evaluation_profile_id,evaluation_profile_version) VALUES($1,$2,0,$3,$4,'hash','default','test','profile','1')")
        .bind(b).bind(run).bind(chunk).bind(Uuid::now_v7()).execute(&pool).await.unwrap_err();
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("23503")
    );
    // Exercise the actual replay workflow, with no competing natural-key row.
    let draft: crate::models::run::RunDraft = sqlx::query_as("SELECT * FROM runs WHERE id=$1")
        .bind(run)
        .fetch_one(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM runs WHERE tenant_id=$1")
        .bind(b)
        .execute(&pool)
        .await
        .unwrap();
    let mut tx = scope::begin(&pool, UserContext::new(a, Uuid::now_v7()).unwrap(), run)
        .await
        .unwrap();
    crate::db::workflows::run_create::insert_run_create(&mut tx, run, &draft, "creating")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let mut tx = scope::begin(&pool, UserContext::new(b, Uuid::now_v7()).unwrap(), run)
        .await
        .unwrap();
    crate::db::workflows::run_create::upsert_dataset_version(&mut tx, dataset, dataset, "1")
        .await
        .unwrap();
    let error =
        crate::db::workflows::run_create::insert_run_create(&mut tx, run, &draft, "creating")
            .await
            .unwrap_err();
    assert!(crate::db::workflows::run_create::is_seed_invariant_error(
        &error
    ));
    tx.rollback().await.unwrap();
    // Missing connection identity does not silently acquire standalone ownership.
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT set_config('vigilo.tenant_id','',true)")
        .execute(&mut *tx)
        .await
        .unwrap();
    let error=sqlx::query("INSERT INTO dataset_versions(dataset_version_id,dataset_id,dataset_version) VALUES($1,$1,'1')")
        .bind(Uuid::now_v7()).execute(&mut *tx).await.unwrap_err();
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("23502")
    );
    tx.rollback().await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn restricted_status_and_pooled_context_fail_closed(pool: PgPool) {
    let a = Uuid::now_v7();
    let b = Uuid::now_v7();
    let actor = Uuid::now_v7();
    let run = Uuid::now_v7();
    seed(&pool, a, run, Uuid::now_v7()).await;
    let role = format!("read_{}", Uuid::now_v7().simple());
    sqlx::raw_sql(&format!("CREATE ROLE {role} LOGIN PASSWORD 'fixture'; GRANT USAGE ON SCHEMA public TO {role}; GRANT SELECT ON database_placements TO {role};"))
        .execute(&pool).await.unwrap();
    for table in [
        "runs",
        "run_plans",
        "run_plan_shards",
        "shard_placements",
        "run_shard_summaries",
    ] {
        // This fixture provides only an identity fence, not an application RBAC model.
        sqlx::raw_sql(&format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY; CREATE POLICY bounded_read ON {table} FOR SELECT TO {role} USING (tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid); GRANT SELECT ON {table} TO {role};"))
            .execute(&pool).await.unwrap();
    }
    let options = pool
        .connect_options()
        .as_ref()
        .clone()
        .username(&role)
        .password("fixture");
    let mut config = Config::new(options.to_url_lossy().to_string());
    config.max_connections = 1;
    let client = UserClient::new(UserConfig {
        control: config,
        database_role: role.clone(),
        execution_databases: BTreeMap::new(),
    })
    .unwrap();
    let a = UserContext::new(a, actor).unwrap();
    let b = UserContext::new(b, actor).unwrap();
    assert!(client.run_status(a, run).await.is_ok());
    let (result, queries) = record_queries(client.run_status(a, run)).await;
    assert!(result.is_ok());
    assert_eq!(
        queries.len(),
        3,
        "creating status: one context statement and two reads: {queries:?}"
    );
    eprintln!(
        "restricted creating status: {} SQL submissions plus BEGIN/COMMIT",
        queries.len()
    );
    let standalone_run = Uuid::now_v7();
    seed(&pool, Uuid::nil(), standalone_run, Uuid::now_v7()).await;
    let standalone = super::super::Client::new(Config::new(
        pool.connect_options().to_url_lossy().to_string(),
    ))
    .unwrap();
    assert!(standalone.run_status(standalone_run).await.is_ok());
    let (result, queries) = record_queries(standalone.run_status(standalone_run)).await;
    assert!(result.is_ok());
    assert_eq!(
        queries.len(),
        2,
        "standalone creating status retains two reads: {queries:?}"
    );
    assert!(matches!(
        standalone.run_status(run).await,
        Err(Error::NotFound(_))
    ));
    standalone.database.control().await.unwrap().close().await;
    assert!(matches!(
        client.run_status(b, run).await,
        Err(Error::NotFound(_))
    ));
    assert!(client.run_status(a, run).await.is_ok());
    // The same physical connection has no residual authority between calls.
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM runs")
        .fetch_one(&client.control)
        .await
        .unwrap();
    assert_eq!(count, 0);
    let mut tx = scope::begin(&client.control, a, run).await.unwrap();
    let identity: (String, String) = sqlx::query_as(
        "SELECT current_setting('vigilo.actor_id'), current_setting('vigilo.run_id')",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(identity, (actor.to_string(), run.to_string()));
    assert!(sqlx::query("SELECT 1/0").execute(&mut *tx).await.is_err());
    drop(tx);
    assert!(client.run_status(a, run).await.is_ok());
    let tx = scope::begin(&client.control, a, run).await.unwrap();
    drop(tx);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM runs")
        .fetch_one(&client.control)
        .await
        .unwrap();
    assert_eq!(count, 0);
    // Concurrent callers sharing one connection cannot inherit each other's identity.
    let (allowed, denied) = tokio::join!(client.run_status(a, run), client.run_status(b, run));
    assert!(allowed.is_ok());
    assert!(matches!(denied, Err(Error::NotFound(_))));
    // A timed-out future drops its transaction and releases its identity.
    let cancelled = tokio::time::timeout(std::time::Duration::from_millis(100), async {
        let mut tx = scope::begin(&client.control, a, run).await.unwrap();
        sqlx::query("SELECT pg_sleep(10)")
            .execute(&mut *tx)
            .await
            .unwrap();
    })
    .await;
    assert!(cancelled.is_err());
    assert!(matches!(
        client.run_status(b, run).await,
        Err(Error::NotFound(_))
    ));
    client.control.close().await;
    let mut owner = pool.acquire().await.unwrap();
    assert!(
        scope::validate_connection(&mut owner, "postgres")
            .await
            .is_err()
    );
    drop(owner);
    sqlx::raw_sql(&format!("DROP OWNED BY {role}; DROP ROLE {role};"))
        .execute(&pool)
        .await
        .unwrap();
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn routed_status_requires_explicit_protected_execution_binding(pool: PgPool) {
    let tenant = Uuid::now_v7();
    let run = Uuid::now_v7();
    let dataset = Uuid::now_v7();
    let role = format!("route_{}", Uuid::now_v7().simple());
    let database = format!("route_{}", Uuid::now_v7().simple());
    sqlx::raw_sql(&format!("CREATE DATABASE {database} TEMPLATE template1"))
        .execute(&pool)
        .await
        .unwrap();
    let execution = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(pool.connect_options().as_ref().clone().database(&database))
        .await
        .unwrap();
    // The test also works without a premigrated template.
    crate::db::migrations::migrate(&execution, None)
        .await
        .unwrap();
    sqlx::raw_sql(&format!("CREATE ROLE {role} LOGIN PASSWORD 'fixture'"))
        .execute(&pool)
        .await
        .unwrap();
    for db in [&pool, &execution] {
        seed(db, tenant, run, dataset).await;
        sqlx::query("UPDATE runs SET status='running' WHERE id=$1")
            .bind(run)
            .execute(db)
            .await
            .unwrap();
        sqlx::raw_sql(&format!(
            "GRANT USAGE ON SCHEMA public TO {role}; GRANT SELECT ON database_placements TO {role}"
        ))
        .execute(db)
        .await
        .unwrap();
        for (table, key) in [
            ("runs", "id"),
            ("shard_placements", "run_id"),
            ("run_shard_summaries", "run_id"),
        ] {
            sqlx::raw_sql(&format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY; CREATE POLICY bounded_read ON {table} FOR SELECT TO {role} USING (tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND {key}=NULLIF(current_setting('vigilo.run_id',true),'')::uuid); GRANT SELECT ON {table} TO {role}"))
                .execute(db).await.unwrap();
        }
    }
    sqlx::query("INSERT INTO database_placements(alias,database_url_env,role,status) VALUES('execution','UNUSED_TEST_BINDING','shard','active')").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO shard_placements(tenant_id,run_id,run_shard,database_alias,status) VALUES($1,$2,0,'execution','active')").bind(tenant).bind(run).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO run_snapshots SELECT (jsonb_populate_record(NULL::run_snapshots,to_jsonb(r)||jsonb_build_object('run_id',r.id,'run_shard',0))).* FROM runs r WHERE id=$1").bind(run).execute(&execution).await.unwrap();
    sqlx::query("INSERT INTO run_shard_summaries(tenant_id,run_id,run_shard,expected_execution_count,execution_count,status) VALUES($1,$2,0,10,4,'running')").bind(tenant).bind(run).execute(&execution).await.unwrap();
    let restricted_url = |db: &PgPool| {
        db.connect_options()
            .as_ref()
            .clone()
            .username(&role)
            .password("fixture")
            .to_url_lossy()
            .to_string()
    };
    let mut config = UserConfig {
        control: Config::new(restricted_url(&pool)),
        database_role: role.clone(),
        execution_databases: BTreeMap::new(),
    };
    config.control.max_connections = 1;
    let context = UserContext::new(tenant, Uuid::now_v7()).unwrap();
    let missing = UserClient::new(config.clone()).unwrap();
    assert!(matches!(
        missing.run_status(context, run).await,
        Err(Error::InvalidConfiguration(_))
    ));
    missing.control.close().await;
    config
        .execution_databases
        .insert("execution".into(), restricted_url(&execution));
    let client = UserClient::new(config).unwrap();
    let status = client.run_status(context, run).await.unwrap();
    let (result, queries) = record_queries(client.run_status(context, run)).await;
    assert!(result.is_ok());
    assert_eq!(
        queries.len(),
        5,
        "routed status: three reads plus one context statement per database: {queries:?}"
    );
    eprintln!(
        "restricted routed status: {} SQL submissions plus two BEGIN/COMMIT pairs",
        queries.len()
    );
    assert_eq!(status.execution_route_count, 1);
    assert_eq!(status.shard_summary_count, 1);
    assert!(status.live_progress_complete);
    // Deliberately different control and execution counts prove the read routed.
    assert_eq!(status.details.live_progress.execution_count, 4);
    assert!(matches!(
        client
            .run_status(
                UserContext::new(Uuid::now_v7(), Uuid::now_v7()).unwrap(),
                run
            )
            .await,
        Err(Error::NotFound(_))
    ));
    let execution_pool = client.execution.get("execution").unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM run_shard_summaries")
        .fetch_one(execution_pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    // Inaccessible routing metadata must not disappear into a control fallback.
    sqlx::raw_sql("ALTER TABLE database_placements ENABLE ROW LEVEL SECURITY")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        client.run_status(context, run).await,
        Err(Error::Dependency(_))
    ));
    sqlx::raw_sql("ALTER TABLE database_placements DISABLE ROW LEVEL SECURITY")
        .execute(&pool)
        .await
        .unwrap();
    // A policy/role error on a new connection must fail before serving any rows.
    let mut connection = execution_pool.acquire().await.unwrap();
    assert!(
        scope::validate_connection(&mut connection, "different_role")
            .await
            .is_err()
    );
    sqlx::query("ALTER TABLE run_shard_summaries DISABLE ROW LEVEL SECURITY")
        .execute(&execution)
        .await
        .unwrap();
    assert!(
        scope::validate_connection(&mut connection, &role)
            .await
            .is_err()
    );
    drop(connection);
    client.control.close().await;
    execution_pool.close().await;
    for db in [&pool, &execution] {
        sqlx::raw_sql(&format!("DROP OWNED BY {role}"))
            .execute(db)
            .await
            .unwrap();
    }
    execution.close().await;
    sqlx::raw_sql(&format!("DROP DATABASE {database}"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::raw_sql(&format!("DROP ROLE {role}"))
        .execute(&pool)
        .await
        .unwrap();
}
