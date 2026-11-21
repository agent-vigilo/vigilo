//! Caller-bound ledger replay under real row policies, including pool reuse.
use super::*;
use crate::db::{
    scope::UserContext,
    workflows::run_shard_summary::test_support::record_queries,
};

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn scoped_replay_publication_and_abort_keep_authority_and_query_budgets(db: PgPool) {
    let role = format!("dataset_{}", Uuid::now_v7().simple());
    let a = UserContext::new(Uuid::now_v7(), Uuid::now_v7()).unwrap();
    let b = UserContext::new(Uuid::now_v7(), Uuid::now_v7()).unwrap();
    sqlx::raw_sql(&format!(
        "CREATE ROLE {role} LOGIN PASSWORD 'fixture'; GRANT USAGE ON SCHEMA public TO {role}"
    ))
    .execute(&db)
    .await
    .unwrap();
    for table in [
        "dataset_uploads",
        "dataset_upload_objects",
        "dataset_catalog",
    ] {
        sqlx::raw_sql(&format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY; GRANT SELECT,INSERT,UPDATE ON {table} TO {role}; CREATE POLICY caller ON {table} TO {role} USING (tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND ((tenant_id='{}' AND current_setting('vigilo.actor_id',true)='{}') OR (tenant_id='{}' AND current_setting('vigilo.actor_id',true)='{}')))", a.tenant_id(),a.actor_id(),b.tenant_id(),b.actor_id()))
            .execute(&db).await.unwrap();
    }
    let options = db
        .connect_options()
        .as_ref()
        .clone()
        .username(&role)
        .password("fixture");
    let user = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(2))
        .connect_with(options)
        .await
        .unwrap();
    // Roles are cluster-wide: clean them even if a contract assertion panics.
    use futures_util::FutureExt;
    let outcome = std::panic::AssertUnwindSafe(async {
        let input = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(input.path(), include_bytes!("../../../../cli/commands/run/fixtures/dataset-identity.json")).unwrap();
        let mut limits = FormatLimits::default();
        limits.batch.max_cases = 1;
        let cancel = CancellationToken::new();
        let prepared = prepare(input.path().into(), InputFormat::Json, limits.clone(), &cancel).await.unwrap();
        let scoped = access::Access::scoped(&user, a);
        let (row, queries) = record_queries(begin(scoped,Uuid::now_v7(),"cases",&"2".repeat(64),&prepared.identity,&limits)).await;
        let row = row.unwrap();
        assert_eq!(queries.len(),3,"context, insert and read; BEGIN/COMMIT excluded: {queries:?}");
        assert_eq!(row.tenant_id,a.tenant_id());
        let operator_json = serde_json::to_value(&row).unwrap();
        assert!(operator_json.get("tenant_id").is_none());
        let wrong = UserContext::new(a.tenant_id(),b.actor_id()).unwrap();
        for caller in [wrong,b] {
            let access = access::Access::scoped(&user,caller);
            assert!(access.load(row.id).await.is_err());
            assert!(claim(access,row.id,false).await.is_err());
            assert!(abort(access,row.id).await.is_err());
            assert!(begin(access,row.id,"cases",&"2".repeat(64),&prepared.identity,&limits).await.is_err());
        }
        let lease = claim(scoped,row.id,false).await.unwrap();
        let memory = Memory::default();
        memory.0.lock().unwrap().idle_pool=Some(user.clone());
        let store = Store::connect(memory.clone(),"cases".into(),limits.clone(),Resources::standard(),Budget::standard(),&cancel,"capability-probes/scoped".into()).await.unwrap();
        memory.0.lock().unwrap().stop_after_batch = true;
        assert!(ingestion::ingest(scoped,&store,&row,lease,&prepared,&cancel).await.is_err());
        assert_eq!(scoped.load(row.id).await.unwrap().case_cursor,0);
        release(scoped,lease).await.unwrap();
        let lease = claim(scoped,row.id,false).await.unwrap();
        let catalog = ingestion::ingest(scoped,&store,&row,lease,&prepared,&cancel).await.unwrap();
        release(scoped,lease).await.unwrap();
        assert_eq!(catalog.id,row.id);
        assert!(memory.0.lock().unwrap().creates.values().all(|&n|n==1));
        let (result,queries)=record_queries(scoped.inspect(row.id)).await;
        let (status,catalog)=result.unwrap();
        assert_eq!(status.status,"finalized");
        assert_eq!(status.case_cursor,3);
        assert!(catalog.is_some());
        assert_eq!(queries.len(),3,"context plus two reads; BEGIN/COMMIT excluded: {queries:?}");
        assert!(abort(scoped,row.id).await.is_err());
        assert!(reusable(access::Access::scoped(&user,b),"cases",&"2".repeat(64),&prepared.identity,&limits).await.unwrap().is_none());
        let other = begin(access::Access::scoped(&user,b),Uuid::now_v7(),"cases",&"2".repeat(64),&prepared.identity,&limits).await.unwrap();
        assert_ne!(other.id,row.id);
        let (result,queries)=record_queries(abort(access::Access::scoped(&user,b),other.id)).await;
        result.unwrap();
        assert_eq!(queries.len(),2,"context plus update; BEGIN/COMMIT excluded: {queries:?}");
        // Rejected setup must not strand an untracked transaction on this pool.
        sqlx::raw_sql("REVOKE EXECUTE ON FUNCTION pg_catalog.set_config(text,text,boolean) FROM PUBLIC").execute(&db).await.unwrap();
        assert!(scoped.connection().await.is_err());
        sqlx::raw_sql("GRANT EXECUTE ON FUNCTION pg_catalog.set_config(text,text,boolean) TO PUBLIC").execute(&db).await.unwrap();
        assert_eq!(scoped.load(row.id).await.unwrap().status,"finalized");
        // Error rollback and alternating callers must not leave identity in the pool.
        let (tenant,actor):(String,String)=sqlx::query_as("SELECT COALESCE(current_setting('vigilo.tenant_id',true),''),COALESCE(current_setting('vigilo.actor_id',true),'')").fetch_one(&user).await.unwrap();
        assert_ne!(tenant,a.tenant_id().to_string());
        assert!(actor.is_empty());
        let (row,lease)=fixture(&db).await;
        let (r,q)=record_queries(release(&db,lease)).await; r.unwrap(); assert_eq!(q.len(),1);
        let (r,q)=record_queries(abort(&db,row.id)).await; r.unwrap(); assert_eq!(q.len(),1);
        let (r,q)=record_queries(access::Access::from(&db).inspect(row.id)).await; r.unwrap(); assert_eq!(q.len(),2);
    }).catch_unwind().await;
    user.close().await;
    sqlx::raw_sql(&format!("DROP OWNED BY {role}; DROP ROLE {role}"))
        .execute(&db)
        .await
        .unwrap();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
