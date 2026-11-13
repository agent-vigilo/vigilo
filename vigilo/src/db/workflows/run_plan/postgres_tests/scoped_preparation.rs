//! Caller-scoped preparation retains actor policy until exact durable acceptance.
//! A memory blob adapter exercises real file checkpoints without external services.
use futures_util::FutureExt;

use super::*;

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn preparation_preserves_caller_through_checkpoints_and_publication(pool: PgPool) {
    let tenant = Uuid::now_v7();
    let actor = Uuid::now_v7();
    let owned = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(move |c, _| {
            Box::pin(async move {
                sqlx::query("SELECT set_config('vigilo.tenant_id',$1,false)")
                    .bind(tenant.to_string())
                    .execute(c)
                    .await?;
                Ok(())
            })
        })
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    let f = Fixture::with_size(&owned, 20, 7, 2).await;
    owned.close().await;
    let role = format!("prepare_{}", Uuid::now_v7().simple());
    sqlx::raw_sql(&format!(
        "CREATE ROLE {role} LOGIN PASSWORD 'fixture'; GRANT USAGE ON SCHEMA public TO {role};"
    ))
    .execute(&pool)
    .await
    .unwrap();
    for table in [
        "runs",
        "dataset_versions",
        "dataset_uploads",
        "dataset_catalog",
        "run_plans",
        "run_plan_objects",
        "run_plan_shards",
        "evaluator_http_policies",
    ] {
        sqlx::raw_sql(&format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY; CREATE POLICY caller ON {table} TO {role} USING (tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND current_setting('vigilo.actor_id',true)='{actor}' AND COALESCE(current_setting('vigilo.run_batch',true),'')=''); GRANT SELECT,INSERT,UPDATE ON {table} TO {role};"))
            .execute(&pool).await.unwrap();
    }
    sqlx::query(&format!("GRANT SELECT ON evaluators TO {role}"))
        .execute(&pool)
        .await
        .unwrap();
    let abi = crate::evaluator_abi::current_identity();
    let evaluator: Uuid = sqlx::query_scalar("INSERT INTO evaluators(namespace,name,version,content_hash,wasm_bytes,wasm_size_bytes,interface_name,interface_version,wit_world,abi_contract_hash,abi_adapter,runtime,runtime_version,runtime_fingerprint,state) VALUES('test','judge','1.0.0','hash','',0,$1,$2,$3,$4,$5,'wasmtime','test','test','active') RETURNING id")
        .bind(format!("{}/{}",abi.package,abi.interface)).bind(&abi.version).bind(&abi.world).bind(&abi.contract_hash).bind(&abi.adapter).fetch_one(&pool).await.unwrap();
    for (owner, uri) in [
        (tenant, "https://judge.example/own"),
        (Uuid::now_v7(), "https://judge.example/other"),
    ] {
        sqlx::query("INSERT INTO evaluator_http_policies(tenant_id,evaluator_id,uri,method,action) VALUES($1,$2,$3,'POST','allow')").bind(owner).bind(evaluator).bind(uri).execute(&pool).await.unwrap();
    }
    let restricted=sqlx::postgres::PgPoolOptions::new().max_connections(1)
        .after_connect(|c,_|Box::pin(async move {
            sqlx::query("SELECT set_config('vigilo.tenant_id','',false),set_config('vigilo.actor_id','',false),set_config('vigilo.run_id','',false),set_config('vigilo.run_batch','',false)").execute(c).await?;
            Ok(())
        })).connect_with(pool.connect_options().as_ref().clone().username(&role).password("fixture")).await.unwrap();
    let result = std::panic::AssertUnwindSafe(async {
        let caller = crate::db::scope::UserContext::new(tenant, actor).unwrap();
        let access = preparation::Access::new(&restricted, Some(caller));
        use crate::db::workflows::run_shard_summary::test_support::record_queries;
        let mut profile = f.profile.clone();
        for group in &mut profile.case_groups {
            for binding in &mut group.evaluators { binding.evaluator_ref = "test/judge:1.0.0".into(); }
        }
        let mut tx = crate::db::scope::begin(&restricted, caller, f.plan.run_id).await.unwrap();
        let initial = crate::db::workflows::run_profile_validation::resolve_evaluators_with(&mut tx, &profile).await.unwrap();
        assert_eq!(initial.len(),1);
        assert_eq!(initial[0].http_rules.len(),1);
        assert_eq!(initial[0].http_rules[0].uri,"https://judge.example/own");
        let (resolved, calls) = record_queries(crate::db::workflows::run_profile_validation::resolve_evaluators_with(&mut tx, &profile)).await;
        assert_eq!(resolved.unwrap(),initial);
        assert_eq!(calls.len(),2,"one metadata batch and one policy batch, with existing caller scope: {calls:#?}");
        tx.commit().await.unwrap();
        for bad in [crate::db::scope::UserContext::new(Uuid::now_v7(), actor).unwrap(), crate::db::scope::UserContext::new(tenant, Uuid::now_v7()).unwrap()] {
            let denied = preparation::Access::new(&restricted, Some(bad));
            assert!(denied.load(f.plan.run_id).await.unwrap().is_none());
            assert!(claim_as(denied, f.plan.run_id).await.is_err());
        }
        let (replay, calls) = record_queries(begin_as(access, f.plan.run_id, f.upload.id, &f.upload.binding_hash, &f.upload.limits, &f.draft, &f.plan.execution_plan, &f.plan.assignments, false)).await;
        assert_eq!(replay.unwrap().run_id, f.plan.run_id);
        assert!(calls[0].contains("set_config"));
        let (lease, calls) = record_queries(claim_as(access, f.plan.run_id)).await;
        let lease = lease.unwrap();
        assert_eq!(calls.len(), 4, "caller scope plus existing three preparation claim statements: {calls:#?}");
        assert!(access.pending_probes(f.plan.run_id).await.unwrap().is_empty());

        // A cancelled lock wait must not retain caller state on pooled reuse.
        let mut held = pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM runs WHERE id=$1 FOR UPDATE").bind(f.plan.run_id).execute(&mut *held).await.unwrap();
        assert!(tokio::time::timeout(std::time::Duration::from_millis(150), renew(&restricted, lease)).await.is_err());
        held.rollback().await.unwrap();
        let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM runs").fetch_one(&restricted).await.unwrap();
        assert_eq!(visible, 0);

        // Interrupt a durable file group, yield, and reconstruct under a new lease.
        f.memory.0.lock().unwrap().stop = Some("/chunks/".into());
        assert!(f.prepare(&restricted, lease).await.is_err());
        release(&restricted, lease, Some("fixture interruption")).await.unwrap();
        let lease = claim_as(access, f.plan.run_id).await.unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        f.memory.0.lock().unwrap().blocked_read = Some((f.dataset.key.clone(), entered.clone(), resume.clone()));
        let prepare = f.prepare(&restricted, lease);
        tokio::pin!(prepare);
        tokio::select! {
            result = &mut prepare => panic!("preparation should wait for fixture storage: {result:?}"),
            _ = entered.notified() => {}
        }
        // The only connection is available while the blob operation is blocked.
        tokio::time::timeout(std::time::Duration::from_secs(1), sqlx::query("SELECT 1").execute(&restricted)).await.unwrap().unwrap();
        resume.notify_one();
        let reference = prepare.await.unwrap();
        f.memory.0.lock().unwrap().blocked_read = None;
        assert_eq!(reference.schema, StorageFormat::RunPlanManifest);
        assert!(access.load(f.plan.run_id).await.unwrap().unwrap().manifest.is_some());
        assert!(!access.objects(f.plan.run_id, "chunks", 0, -1).await.unwrap().is_empty());

        // The same operation methods use only the supplied restricted pool.
        let context = runtime_context(&restricted);
        context.dbr().await.unwrap().control_pool.set(restricted.clone()).unwrap();
        let owner: crate::blob_storage::config::OwnerConfig = toml::from_str("provider='s3'\nalias='cases'\nbucket='cases'\nprefix='tests'\nregion='us-east-1'\n[credential_variables]\naccess_key='UNUSED_TEST_ACCESS'\nsecret_key='UNUSED_TEST_SECRET'").unwrap();
        let client = crate::operations::RunClient::from_context(context, owner, Default::default(), Some(caller));
        let (input_result, input_calls) = record_queries(client.prepare_run(
            crate::operations::PrepareRun { run_id: Uuid::now_v7(), dataset_upload_id: f.upload.id, profile: f.payload.clone() }, &f.cancel
        )).await;
        match input_result {
            Err(crate::operations::Error::Dependency(error)) => assert!(error.to_string().contains("dataset storage owner binding changed")),
            _ => panic!("authorized input reads must reach physical binding validation without opening storage"),
        }
        assert_eq!(input_calls.len(),3,"one caller scope for upload and catalog: {input_calls:#?}");
        let shared = Arc::new(Store::connect(f.memory.clone(), "cases".into(), f.plan.limits.0.clone(), Resources::standard(), Budget::standard(), &f.cancel, "capability-probes/reader-fixture".into()).await.unwrap());
        let prepared = || async {
            let reader = crate::case_reader::RunReader::open(shared.clone(), &reference, f.plan.limits.0.clone(), f.plan.run_id, &f.cancel).await.unwrap();
            client.prepared_for_test(access.load(f.plan.run_id).await.unwrap().unwrap(), reader, &f.draft, f.upload.identity.dataset_hash.clone())
        };
        let proof = prepared().await;
        let (description, calls) = record_queries(client.describe(&proof)).await;
        assert_eq!(description.unwrap().data.status, "creating");
        assert_eq!(calls.len(), 2, "caller scope and one status projection: {calls:#?}");
        // A caller cannot carry an old proof through a permission revocation.
        sqlx::raw_sql("ALTER POLICY caller ON run_plans USING(false)").execute(&pool).await.unwrap();
        assert!(client.accept_run(proof, &f.cancel).await.is_err());
        sqlx::raw_sql(&format!("ALTER POLICY caller ON run_plans USING (tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND current_setting('vigilo.actor_id',true)='{actor}' AND COALESCE(current_setting('vigilo.run_batch',true),'')='')")).execute(&pool).await.unwrap();
        let mut changed = reference.clone();
        changed.digest = "0".repeat(64);
        let mut tx = crate::db::scope::begin(&restricted, caller, f.plan.run_id).await.unwrap();
        assert!(admission::accept(&mut *tx, f.plan.run_id, &changed).await.is_err());
        tx.rollback().await.unwrap();
        let mut tx = crate::db::scope::begin(&restricted, caller, f.plan.run_id).await.unwrap();
        admission::accept(&mut *tx, f.plan.run_id, &reference).await.unwrap();
        tx.rollback().await.unwrap();
        let accepted: bool = sqlx::query_scalar("SELECT accepted_manifest IS NOT NULL FROM run_plans WHERE run_id=$1").bind(f.plan.run_id).fetch_one(&pool).await.unwrap();
        assert!(!accepted, "rollback must not grant execution");
        let proof = prepared().await;
        let (accepted, calls) = record_queries(client.accept_run(proof, &f.cancel)).await;
        assert_eq!(accepted.unwrap().data.status, "creating");
        assert_eq!(calls.len(), 5, "accept scope, status, exact approval, report scope, report: {calls:#?}");
        let retried = client.accept_run(prepared().await, &f.cancel).await.unwrap();
        assert_eq!(retried.data.status, "creating");

        let (accepted, status): (serde_json::Value, String) = sqlx::query_as("SELECT p.accepted_manifest,r.status::text FROM run_plans p JOIN runs r ON r.id=p.run_id WHERE p.run_id=$1").bind(f.plan.run_id).fetch_one(&pool).await.unwrap();
        assert_eq!(accepted, serde_json::to_value(&reference).unwrap());
        assert_eq!(status, "creating", "acceptance must leave system execution to a separate claim");
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM run_plans").fetch_one(&restricted).await.unwrap(),0);
    })
    .catch_unwind()
    .await;
    restricted.close().await;
    sqlx::raw_sql(&format!("DROP OWNED BY {role}; DROP ROLE {role};"))
        .execute(&pool)
        .await
        .unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
