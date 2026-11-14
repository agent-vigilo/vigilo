//! Real PostgreSQL deployment validation with isolated roles and exact cleanup.
use futures_util::FutureExt;
use sqlx::{
    ConnectOptions,
    Connection,
    PgPool,
};

use super::*;

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn fixed_roles_validate_and_reject_unsafe_deployments(pool: PgPool) {
    let prefix = format!("bindings_{}", uuid::Uuid::now_v7().simple());
    let roles: Vec<_> = ["s", "e", "u", "o"]
        .map(|suffix| format!("{prefix}_{suffix}"))
        .into();
    for (i, role) in roles.iter().enumerate() {
        sqlx::raw_sql(&format!(
            "CREATE ROLE {role} {} PASSWORD 'fixture'; GRANT USAGE ON SCHEMA public TO {role};",
            if i == 3 { "NOLOGIN" } else { "LOGIN" }
        ))
        .execute(&pool)
        .await
        .unwrap();
    }
    let mut config = tests::config();
    config.max_connections = 1;
    config.acquire_timeout = Duration::from_secs(1);
    let binding = config.databases.get_mut("primary").unwrap();
    binding.function_owner = roles[3].clone();
    for (credential, role) in [
        &mut binding.scheduler,
        &mut binding.execution,
        &mut binding.user,
    ]
    .into_iter()
    .zip(&roles)
    {
        credential.database_role = role.clone();
        let mut url = pool
            .connect_options()
            .as_ref()
            .clone()
            .username(role)
            .password("fixture")
            .to_url_lossy();
        url.set_query(None);
        credential.database_url = url.to_string();
    }
    for contract in contracts::CONTRACTS {
        sqlx::raw_sql(&format!(
            "ALTER FUNCTION public.{} OWNER TO {}; GRANT EXECUTE ON FUNCTION public.{} TO {};",
            contract.signature,
            roles[3],
            contract.signature,
            if contract.worker {
                &roles[1]
            } else {
                &roles[0]
            }
        ))
        .execute(&pool)
        .await
        .unwrap();
    }
    sqlx::raw_sql(&format!("GRANT SELECT ON run_snapshots,runs,run_plans,run_control_deliveries,run_shard_dispatch_cursors,shard_placements,database_placements,run_chunks,run_shard_summaries,local_shard_admissions,run_chunk_settlement_consumptions,outbox_delivery_queue,outbox_events TO {}; GRANT UPDATE ON database_placements,runs,run_plans,run_control_deliveries,run_shard_dispatch_cursors,outbox_delivery_queue TO {};",roles[3],roles[3])).execute(&pool).await.unwrap();
    let pools = DatabasePools::new(config.clone()).unwrap();
    let result=std::panic::AssertUnwindSafe(async {
        for bound in [&pools.0.placements["primary"].scheduler,&pools.0.placements["primary"].execution,&pools.0.placements["primary"].user] {
            let mut direct=sqlx::PgConnection::connect_with(&bound.pool.connect_options()).await.unwrap();
            bound.guard.initialize(&mut direct).await.unwrap();
            direct.close().await.unwrap();
        }
        pools.validate().await.unwrap();
        for (bound,role) in [&pools.0.placements["primary"].scheduler,&pools.0.placements["primary"].execution,&pools.0.placements["primary"].user].into_iter().zip(&roles) {
            let actual:String=sqlx::query_scalar("SELECT current_user::text").fetch_one(&bound.pool).await.unwrap();
            assert_eq!(&actual,role);
        }
        // Role routing reuses the same guarded pools through actual Context access.
        // Runtime metadata cannot provide alternate credentials or expand bindings.
        for role in &roles[..3] {
            sqlx::query(&format!("GRANT SELECT ON database_placements TO {role}")).execute(&pool).await.unwrap();
        }
        for (authority,role) in [Authority::Scheduler,Authority::Execution,Authority::User].into_iter().zip(&roles) {
            let router=pools.router(authority,"primary".into(),"single-default".into()).unwrap();
            let context=crate::context::Context::for_operations(router.clone());
            let actual:String=sqlx::query_scalar("SELECT current_user::text").fetch_one(context.dbr().await.unwrap().control().await.unwrap()).await.unwrap();
            assert_eq!(&actual,role);
            let (routed,calls)=crate::db::workflows::run_shard_summary::test_support::record_queries(router.execution_database("primary")).await;
            let routed=routed.unwrap();
            assert_eq!(calls.len(),1,"one existing placement lifecycle read, no credential discovery: {calls:#?}");
            let actual:String=sqlx::query_scalar("SELECT current_user::text").fetch_one(&routed).await.unwrap();
            assert_eq!(&actual,role);
            let (hinted,calls)=crate::db::workflows::run_shard_summary::test_support::record_queries(router.execution_write_route(crate::db::workflows::local_shard_admission::LocalShardRouteHint{run_id:uuid::Uuid::now_v7(),run_shard:0,database_alias:"primary".into(),write_epoch:1})).await;
            hinted.unwrap();
            assert!(calls.is_empty(),"known worker route remains free of control queries: {calls:#?}");
            sqlx::query("UPDATE database_placements SET status='draining' WHERE alias='primary'").execute(&pool).await.unwrap();
            assert!(router.execution_database("primary").await.is_ok());
            sqlx::query("UPDATE database_placements SET status='disabled' WHERE alias='primary'").execute(&pool).await.unwrap();
            assert!(router.execution_database("primary").await.is_err());
            sqlx::query("UPDATE database_placements SET status='active',role='control' WHERE alias='primary'").execute(&pool).await.unwrap();
            assert!(router.placement("primary").await.is_err(),"metadata cannot change a supplied purpose");
            sqlx::query("UPDATE database_placements SET role='control_and_shard' WHERE alias='primary'").execute(&pool).await.unwrap();
            sqlx::query("INSERT INTO database_placements(alias,database_url_env,role,status) VALUES('unbound','DATABASE_URL','shard','active')").execute(&pool).await.unwrap();
            assert!(router.execution_database("unbound").await.unwrap_err().to_string().contains("fixed database binding"));
            sqlx::query("DELETE FROM database_placements WHERE alias='unbound'").execute(&pool).await.unwrap();
        }
        // Each unsafe change is observed even on already-established connections.
        let changes=[
            (format!("REVOKE SELECT ON runs FROM {}",roles[3]),format!("GRANT SELECT ON runs TO {}",roles[3])),
            (format!("ALTER ROLE {} BYPASSRLS",roles[2]),format!("ALTER ROLE {} NOBYPASSRLS",roles[2])),
            (format!("GRANT SELECT(id) ON runs TO {}",roles[2]),format!("REVOKE SELECT(id) ON runs FROM {}",roles[2])),
            ("ALTER FUNCTION vigilo_dispatch_backlog() STRICT".into(),"ALTER FUNCTION vigilo_dispatch_backlog() CALLED ON NULL INPUT".into()),
            (format!("GRANT SELECT ON runs TO {}",roles[0]),format!("REVOKE SELECT ON runs FROM {}",roles[0])),
            (format!("GRANT SELECT ON runs TO {}",roles[2]),format!("REVOKE SELECT ON runs FROM {}",roles[2])),
            ("GRANT EXECUTE ON FUNCTION vigilo_dispatch_backlog() TO PUBLIC".into(),"REVOKE EXECUTE ON FUNCTION vigilo_dispatch_backlog() FROM PUBLIC".into()),
            (format!("ALTER ROLE {} LOGIN",roles[3]),format!("ALTER ROLE {} NOLOGIN",roles[3])),
            (format!("GRANT {} TO {}",roles[1],roles[2]),format!("REVOKE {} FROM {}",roles[1],roles[2])),
            (format!("GRANT CREATE ON SCHEMA public TO {}",roles[2]),format!("REVOKE CREATE ON SCHEMA public FROM {}",roles[2])),
            (format!("GRANT EXECUTE ON FUNCTION vigilo_dispatch_backlog() TO {}",roles[2]),format!("REVOKE EXECUTE ON FUNCTION vigilo_dispatch_backlog() FROM {}",roles[2])),
            ("ALTER FUNCTION vigilo_dispatch_backlog() SET search_path=public".into(),"ALTER FUNCTION vigilo_dispatch_backlog() SET search_path=pg_catalog,pg_temp".into()),
        ];
        for (change,restore) in changes {
            sqlx::raw_sql(&change).execute(&pool).await.unwrap();
            assert!(pools.validate().await.is_err(),"accepted {change}");
            sqlx::raw_sql(&restore).execute(&pool).await.unwrap();
            pools.validate().await.unwrap();
        }
        let definition:String=sqlx::query_scalar("SELECT pg_get_functiondef('vigilo_dispatch_backlog()'::regprocedure)").fetch_one(&pool).await.unwrap();
        sqlx::raw_sql("CREATE OR REPLACE FUNCTION vigilo_dispatch_backlog() RETURNS bigint LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS 'SELECT 0::bigint'").execute(&pool).await.unwrap();
        assert!(pools.validate().await.is_err(),"changed body must be rejected");
        let fresh=DatabasePools::new(config.clone()).unwrap();
        assert!(fresh.validate().await.is_err(),"new connections must fail closed too");
        fresh.close().await;
        sqlx::raw_sql(&definition).execute(&pool).await.unwrap();
        pools.validate().await.unwrap();
        // Limited user access is valid only once a deployment supplies a row policy.
        sqlx::raw_sql(&format!("ALTER TABLE runs ENABLE ROW LEVEL SECURITY; CREATE POLICY binding_read ON runs FOR SELECT TO {} USING (tenant_id = NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid); GRANT SELECT ON runs TO {};",roles[2],roles[2])).execute(&pool).await.unwrap();
        pools.validate().await.unwrap();
        let tenant=uuid::Uuid::now_v7();
        let run=uuid::Uuid::now_v7();
        sqlx::query("INSERT INTO dataset_versions(tenant_id,dataset_version_id,dataset_id,dataset_version) VALUES($1,$2,$2,'1')").bind(tenant).bind(run).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO runs(tenant_id,id,run_key,dataset_id,dataset_version_id,dataset_version,evaluation_profile_id,evaluation_profile_version,profile_version_id,profile_hash,aggregation_policy_id,aggregation_policy_version,aggregation_policy_hash,agent_provider,agent_name,prompt_config_id,prompt_config_version,status) VALUES($1,$2,'binding-run',$2,$2,'1','profile','1','profile:1','hash','policy','1','hash','test','agent','prompt','1','creating')").bind(tenant).bind(run).execute(&pool).await.unwrap();
        for table in ["run_plans","run_plan_shards"] {
            sqlx::raw_sql(&format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY; CREATE POLICY binding_read ON {table} FOR SELECT TO {} USING (tenant_id = NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid); GRANT SELECT ON {table} TO {};",roles[2],roles[2])).execute(&pool).await.unwrap();
        }
        let client=pools.user_client();
        let context=super::super::UserContext::new(tenant,uuid::Uuid::now_v7()).unwrap();
        client.run_status(context,run).await.unwrap();
        let (status,queries)=crate::db::workflows::run_shard_summary::test_support::record_queries(client.run_status(context,run)).await;
        status.unwrap();
        assert_eq!(queries.len(),3,"warm status retains context plus two reads: {queries:?}");
        let other=super::super::UserContext::new(uuid::Uuid::now_v7(),uuid::Uuid::now_v7()).unwrap();
        assert!(matches!(client.run_status(other,run).await,Err(Error::NotFound(_))));
        client.run_status(context,run).await.unwrap();
        let count:i64=sqlx::query_scalar("SELECT count(*) FROM runs").fetch_one(&pools.0.placements["primary"].user.pool).await.unwrap();
        assert_eq!(count,0);
        let scheduler=pools.router(Authority::Scheduler,"primary".into(),"single-default".into()).unwrap();
        scoped_dispatch(&pool, &scheduler, &roles, tenant, run).await;
        hosted_operations(&pool, &pools, &roles, tenant, run).await;
        pools.validate().await.unwrap();

    }).catch_unwind().await;
    pools.close().await;
    // Restore function ownership before dropping the exact fixture roles.
    for contract in contracts::CONTRACTS {
        sqlx::raw_sql(&format!(
            "ALTER FUNCTION public.{} OWNER TO CURRENT_USER",
            contract.signature
        ))
        .execute(&pool)
        .await
        .unwrap();
    }
    for role in &roles {
        sqlx::raw_sql(&format!("DROP OWNED BY {role}; DROP ROLE {role};"))
            .execute(&pool)
            .await
            .unwrap();
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

/// Proves the same fixed scheduler connection can claim, bind context and finish
/// protected control writes without losing its lock or exposing other runs.
async fn scoped_dispatch(
    admin: &PgPool,
    router: &crate::context::database::DatabaseRouter,
    roles: &[String],
    tenant: uuid::Uuid,
    run: uuid::Uuid,
) {
    use crate::db::workflows::{
        run_dispatch,
        run_shard_summary::test_support::record_queries,
    };
    let scheduler = router.control().await.unwrap();
    for (table, key) in [
        ("runs", "id"),
        ("shard_placements", "run_id"),
        ("run_shard_dispatch_cursors", "run_id"),
        ("outbox_events", "aggregate_id"),
    ] {
        sqlx::raw_sql(&format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY; CREATE POLICY scheduler_work ON {table} TO {} USING (tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND {key}=NULLIF(current_setting('vigilo.run_id',true),'')::uuid AND current_setting('vigilo.actor_id',true)=''); CREATE POLICY claim_owner ON {table} TO {} USING(true); GRANT SELECT,UPDATE ON {table} TO {};",roles[0],roles[3],roles[0])).execute(admin).await.unwrap();
    }
    sqlx::raw_sql(&format!("GRANT INSERT ON outbox_events,outbox_delivery_queue TO {}; ALTER TABLE outbox_delivery_queue ENABLE ROW LEVEL SECURITY; CREATE POLICY scheduler_delivery ON outbox_delivery_queue TO {} USING (tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND EXISTS(SELECT 1 FROM outbox_events e WHERE e.id=event_id AND e.tenant_id=outbox_delivery_queue.tenant_id)); CREATE POLICY claim_owner ON outbox_delivery_queue TO {} USING(true);",roles[0],roles[0],roles[3])).execute(admin).await.unwrap();
    sqlx::query("UPDATE runs SET status='pending' WHERE id=$1")
        .bind(run)
        .execute(admin)
        .await
        .unwrap();
    sqlx::query("INSERT INTO shard_placements(tenant_id,run_id,run_shard,database_alias,status) VALUES($1,$2,0,'primary','active')").bind(tenant).bind(run).execute(admin).await.unwrap();
    sqlx::query(
        "INSERT INTO run_shard_dispatch_cursors(tenant_id,run_id,run_shard) VALUES($1,$2,0)",
    )
    .bind(tenant)
    .bind(run)
    .execute(admin)
    .await
    .unwrap();
    let mut other_runs = Vec::new();
    for other_tenant in [tenant, uuid::Uuid::now_v7()] {
        let other = uuid::Uuid::now_v7();
        other_runs.push((other_tenant, other));
        sqlx::query("INSERT INTO dataset_versions(tenant_id,dataset_version_id,dataset_id,dataset_version) VALUES($1,$2,$2,'1')").bind(other_tenant).bind(other).execute(admin).await.unwrap();
        sqlx::query("INSERT INTO runs SELECT (jsonb_populate_record(NULL::runs,to_jsonb(r)||jsonb_build_object('tenant_id',$2::uuid,'id',$3::uuid,'dataset_id',$3::uuid,'dataset_version_id',$3::uuid,'run_key',$3::text,'status','creating'))).* FROM runs r WHERE id=$1")
            .bind(run).bind(other_tenant).bind(other).execute(admin).await.unwrap();
    }
    let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM runs")
        .fetch_one(scheduler)
        .await
        .unwrap();
    assert_eq!(visible, 0, "no work scope grants no visibility");
    for finish in 0..3 {
        let (claim, queries) =
            record_queries(run_dispatch::claim_next_dispatch_route(scheduler, &[])).await;
        let mut claim = claim.unwrap().unwrap();
        assert_eq!(
            queries.len(),
            1,
            "claim and context must remain one statement: {queries:?}"
        );
        assert_eq!(claim.route.run_id, run);
        let scope:(String,String,String)=sqlx::query_as("SELECT current_setting('vigilo.tenant_id',true),current_setting('vigilo.run_id',true),current_setting('vigilo.actor_id',true)").fetch_one(&mut *claim.control_tx).await.unwrap();
        assert_eq!(scope, (tenant.to_string(), run.to_string(), String::new()));
        let visible: Vec<uuid::Uuid> = sqlx::query_scalar("SELECT id FROM runs")
            .fetch_all(&mut *claim.control_tx)
            .await
            .unwrap();
        assert_eq!(
            visible,
            vec![run],
            "neither another tenant nor another run is visible"
        );
        assert!(
            run_dispatch::claim_next_dispatch_route(admin, &[])
                .await
                .unwrap()
                .is_none(),
            "claim lock survives scope binding"
        );
        let changed = sqlx::query("UPDATE runs SET error_message='must not escape' WHERE id<>$1")
            .bind(run)
            .execute(&mut *claim.control_tx)
            .await
            .unwrap()
            .rows_affected();
        assert_eq!(changed, 0);
        if finish == 0 {
            sqlx::query("SAVEPOINT rejected_write")
                .execute(&mut *claim.control_tx)
                .await
                .unwrap();
            let error=sqlx::query("INSERT INTO outbox_events(tenant_id,event_type,aggregate_type,aggregate_id,dedupe_key) VALUES($1,'test','run',$2,'forbidden')")
                .bind(tenant).bind(other_runs[0].1).execute(&mut *claim.control_tx).await.unwrap_err();
            assert!(
                error
                    .as_database_error()
                    .unwrap()
                    .message()
                    .contains("row-level security")
            );
            sqlx::query("ROLLBACK TO SAVEPOINT rejected_write")
                .execute(&mut *claim.control_tx)
                .await
                .unwrap();
        }

        let (snapshot, queries) = record_queries(run_dispatch::prepare_dispatch_run_snapshot_with(
            &mut claim.control_tx,
            &claim.route,
            uuid::Uuid::now_v7(),
            60,
        ))
        .await;
        let snapshot = snapshot.unwrap().unwrap();
        assert_eq!(snapshot.tenant_id, tenant);
        let (resolved, routing_queries) = record_queries(tokio::time::timeout(
            Duration::from_secs(1),
            router.execution_route_from_placements(
                &snapshot.shard_placement,
                &snapshot.database_placement,
            ),
        ))
        .await;
        let resolved = resolved
            .expect("routing must not borrow the occupied single-connection pool")
            .unwrap();
        assert_eq!(resolved.placement.run_id, run);
        assert!(
            routing_queries.is_empty(),
            "claimed routing must reuse projected metadata: {routing_queries:?}"
        );
        let mut disabled = snapshot.database_placement.0.clone();
        disabled.status = "disabled".into();
        assert!(
            router
                .execution_route_from_placements(&snapshot.shard_placement, &disabled)
                .await
                .is_err()
        );
        let mut moving = snapshot.shard_placement.0.clone();
        moving.status = "moving".into();
        assert!(
            router
                .execution_route_from_placements(&moving, &snapshot.database_placement)
                .await
                .is_err()
        );
        assert_eq!(
            queries.len(),
            1,
            "starting a pending run remains one statement"
        );
        sqlx::query("UPDATE run_shard_dispatch_cursors SET status='drained' WHERE run_id=$1")
            .bind(run)
            .execute(&mut *claim.control_tx)
            .await
            .unwrap();
        match finish {
            0 => claim.control_tx.rollback().await.unwrap(),
            1 => {
                assert!(
                    tokio::time::timeout(std::time::Duration::from_millis(20), async move {
                        let _held_claim = claim;
                        std::future::pending::<()>().await;
                    })
                    .await
                    .is_err()
                );
            }
            _ => claim.control_tx.commit().await.unwrap(),
        }
        let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM runs")
            .fetch_one(scheduler)
            .await
            .unwrap();
        assert_eq!(
            visible, 0,
            "pooled connection must not retain transaction scope"
        );
        let events: i64 =
            sqlx::query_scalar("SELECT count(*) FROM outbox_events WHERE aggregate_id=$1")
                .bind(run)
                .fetch_one(admin)
                .await
                .unwrap();
        assert_eq!(
            events,
            if finish == 2 { 1 } else { 0 },
            "rollback/drop includes run and outbox writes"
        );
    }
    assert!(
        run_dispatch::claim_next_dispatch_route(scheduler, &[])
            .await
            .unwrap()
            .is_none()
    );
    for (other_tenant, other) in other_runs {
        sqlx::query("UPDATE runs SET status='pending' WHERE id=$1")
            .bind(other)
            .execute(admin)
            .await
            .unwrap();
        sqlx::query("INSERT INTO shard_placements(tenant_id,run_id,run_shard,database_alias,status) VALUES($1,$2,0,'primary','active')").bind(other_tenant).bind(other).execute(admin).await.unwrap();
        sqlx::query(
            "INSERT INTO run_shard_dispatch_cursors(tenant_id,run_id,run_shard) VALUES($1,$2,0)",
        )
        .bind(other_tenant)
        .bind(other)
        .execute(admin)
        .await
        .unwrap();
        let mut claim = run_dispatch::claim_next_dispatch_route(scheduler, &[])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (claim.route.tenant_id, claim.route.run_id),
            (other_tenant, other)
        );
        let visible: Vec<uuid::Uuid> = sqlx::query_scalar("SELECT id FROM runs")
            .fetch_all(&mut *claim.control_tx)
            .await
            .unwrap();
        assert_eq!(
            visible,
            vec![other],
            "pooled claims switch exact scope without retaining prior runs"
        );
        claim.control_tx.rollback().await.unwrap();
        sqlx::query("UPDATE run_shard_dispatch_cursors SET status='drained' WHERE run_id=$1")
            .bind(other)
            .execute(admin)
            .await
            .unwrap();
    }
}

/// Exercise public hosting with real fixed identities and request-local policies.
async fn hosted_operations(
    admin: &PgPool,
    pools: &DatabasePools,
    roles: &[String],
    tenant: uuid::Uuid,
    run: uuid::Uuid,
) {
    use crate::operations::{
        BrokerConfig,
        ExecutionControl,
        HostedConfig,
        HostedRuntime,
        UserContext,
        resources::{
            self,
            Bindings,
            Request,
            Resolution,
            Resolver,
        },
    };
    struct Deny;
    impl Resolver for Deny {
        fn bindings(&self, _: Request) -> Resolution<'_, Bindings> {
            Box::pin(async { Err(resources::Error::Denied) })
        }
    }
    // Keep hosted control transitions independent of the dispatch fixture's rows.
    let source_run = run;
    let run = uuid::Uuid::now_v7();
    sqlx::query("INSERT INTO dataset_versions(tenant_id,dataset_version_id,dataset_id,dataset_version) VALUES($1,$2,$2,'1')")
        .bind(tenant).bind(run).execute(admin).await.unwrap();
    sqlx::query("INSERT INTO runs SELECT (jsonb_populate_record(NULL::runs,to_jsonb(r)||jsonb_build_object('id',$2::uuid,'dataset_id',$2::uuid,'dataset_version_id',$2::uuid,'run_key',$2::text,'status','creating'))).* FROM runs r WHERE id=$1")
        .bind(source_run).bind(run).execute(admin).await.unwrap();
    let actor = uuid::Uuid::now_v7();
    let predicate = format!(
        "tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND current_setting('vigilo.actor_id',true)='{actor}'"
    );
    sqlx::raw_sql(&format!("DROP POLICY binding_read ON runs; CREATE POLICY hosted_access ON runs TO {} USING ({predicate}); GRANT UPDATE ON runs TO {};", roles[2], roles[2])).execute(admin).await.unwrap();
    for table in [
        "run_control_deliveries",
        "shard_placements",
        "shard_move_operations",
    ] {
        sqlx::raw_sql(&format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY; CREATE POLICY hosted_access ON {table} TO {} USING ({predicate}); CREATE POLICY hosted_owner ON {table} TO {} USING(true); GRANT SELECT ON {table} TO {};", roles[2], roles[3], roles[2])).execute(admin).await.unwrap();
    }
    sqlx::raw_sql(&format!(
        "GRANT INSERT,UPDATE ON run_control_deliveries TO {};",
        roles[2]
    ))
    .execute(admin)
    .await
    .unwrap();
    let configuration = || {
        let mut config = HostedConfig::new(
            pools.clone(),
            Arc::new(Deny),
            BrokerConfig::new("amqp://host:fixture@127.0.0.1:1".into()),
            "primary".into(),
            "single-default".into(),
        );
        config.wasm.max_log_messages = 0;
        config
    };
    let runtime = HostedRuntime::new(configuration()).await.unwrap();
    // Explicit startup revalidation must reject changed privileges on warm pools.
    sqlx::raw_sql(&format!("ALTER ROLE {} BYPASSRLS", roles[2]))
        .execute(admin)
        .await
        .unwrap();
    let rejected = HostedRuntime::new(configuration()).await;
    sqlx::raw_sql(&format!("ALTER ROLE {} NOBYPASSRLS", roles[2]))
        .execute(admin)
        .await
        .unwrap();
    assert!(
        rejected.is_err(),
        "hosting must reject an unsafe login before yielding a runtime"
    );
    let user = runtime.user(UserContext::new(tenant, actor).unwrap());
    let wrong_actor = runtime.user(UserContext::new(tenant, uuid::Uuid::now_v7()).unwrap());
    let wrong_owner = runtime.user(UserContext::new(uuid::Uuid::now_v7(), actor).unwrap());
    for denied in [&wrong_actor, &wrong_owner] {
        assert!(matches!(
            denied.run_status(run).await,
            Err(Error::NotFound(_))
        ));
        assert!(
            denied
                .control_run(run, ExecutionControl::Paused)
                .await
                .is_err()
        );
        assert!(denied.run_control_status(run).await.is_err());
    }
    let (status, queries) = crate::db::workflows::run_shard_summary::test_support::record_queries(
        user.clone().run_status(run),
    )
    .await;
    status.unwrap();
    assert_eq!(
        queries.len(),
        3,
        "hosted status has no extra authority lookups: {queries:?}"
    );
    let (control, queries) = crate::db::workflows::run_shard_summary::test_support::record_queries(
        user.control_run(run, ExecutionControl::Paused),
    )
    .await;
    assert_eq!(control.unwrap().execution_control, ExecutionControl::Paused);
    assert_eq!(
        queries.len(),
        5,
        "context and existing control statements only: {queries:?}"
    );
    let (control, queries) = crate::db::workflows::run_shard_summary::test_support::record_queries(
        user.run_control_status(run),
    )
    .await;
    assert_eq!(control.unwrap().revision, 1);
    assert_eq!(
        queries.len(),
        2,
        "context plus existing progress read: {queries:?}"
    );
    let repeated = user
        .control_run(run, ExecutionControl::Paused)
        .await
        .unwrap();
    assert_eq!(repeated.revision, 1);
    user.control_run(run, ExecutionControl::Enabled)
        .await
        .unwrap();
    let cancel = tokio_util::sync::CancellationToken::new();
    cancel.cancel();
    // A cancelled service must neither touch the unavailable broker nor claim work.
    let coordinator = runtime.coordinator();
    let worker = runtime.worker();
    let stopped = cancel.clone();
    let coordinator_task = tokio::spawn(async move {
        crate::db::workflows::run_shard_summary::test_support::record_queries(
            coordinator.run(stopped),
        )
        .await
    });
    let worker_task = tokio::spawn(async move {
        crate::db::workflows::run_shard_summary::test_support::record_queries(worker.run(cancel))
            .await
    });
    for (result, queries) in [coordinator_task.await.unwrap(), worker_task.await.unwrap()] {
        result.unwrap();
        assert!(
            queries.is_empty(),
            "cancelled services did database work: {queries:?}"
        );
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM runs")
        .fetch_one(&pools.0.placements["primary"].user.pool)
        .await
        .unwrap();
    assert_eq!(count, 0, "pooled request settings must not leak");
    // Leave pool shutdown to the fixture's exact cleanup, shared with the host root.
    drop(runtime);
}
