//! Real PostgreSQL registry identity, bounded discovery and caller fencing.
//! Synthetic drafts test persistence only; compiled/imported Wasm is covered by
//! the named public-consumer tier and ABI fixture tests.
use serde_json::json;
use sqlx::PgPool;

use super::*;
use crate::db::workflows::run_shard_summary::test_support::record_queries;
fn draft(namespace: &str, name: &str, version: &str, seed: u8) -> EvaluatorDraft {
    EvaluatorDraft {
        namespace: namespace.into(),
        name: name.into(),
        version: version.into(),
        content_hash: format!("{seed:064x}"),
        wasm_bytes: vec![seed],
        interface_name: "vigilo:evaluator/evaluator".into(),
        interface_version: "1.0.0".into(),
        wit_world: "evaluator-world".into(),
        abi_contract_hash: "a".repeat(64),
        abi_adapter: "vigilo-evaluator-v1@1".into(),
        runtime: "wasmtime".into(),
        runtime_version: "fixture".into(),
        runtime_fingerprint: "fixture".into(),
        description: Some("description".into()),
        tags: json!(["test"]),
        metadata: json!({"fixture":true}),
    }
}
async fn publish(
    pool: &PgPool,
    draft: EvaluatorDraft,
) -> anyhow::Result<crate::evaluators::PublishEvaluatorOutcome> {
    let mut c = pool.acquire().await?;
    crate::evaluators::publish_draft_on(&mut c, draft).await
}
#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn concurrent_identical_publication_reconciles_without_overwriting(pool: PgPool) {
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE evaluators IN SHARE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let one = pool.clone();
    let two = pool.clone();
    let a = tokio::spawn(async move { publish(&one, draft("vigilo", "race", "1.0.0", 1)).await });
    let b = tokio::spawn(async move { publish(&two, draft("vigilo", "race", "1.0.0", 1)).await });
    let waits=tokio::time::timeout(std::time::Duration::from_secs(5),async{loop{let waiting:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event='relation' AND query ILIKE '%INSERT INTO evaluators%'").fetch_one(&pool).await.unwrap();if waiting==2 {break;}tokio::time::sleep(std::time::Duration::from_millis(10)).await;}}).await;
    blocker.commit().await.unwrap();
    let results = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        (a.await.unwrap(), b.await.unwrap())
    })
    .await
    .unwrap();
    assert!(
        waits.is_ok(),
        "both publications must reach insertion before releasing barrier"
    );
    assert!(
        results.0.is_ok() && results.1.is_ok(),
        "exact concurrent replay must succeed: {results:?}"
    );
    assert_ne!(
        matches!(
            results.0.unwrap(),
            crate::evaluators::PublishEvaluatorOutcome::Inserted(_)
        ),
        matches!(
            results.1.unwrap(),
            crate::evaluators::PublishEvaluatorOutcome::Inserted(_)
        )
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM evaluators")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}
#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn metadata_pages_and_immutable_state_preserve_query_budgets(pool: PgPool) {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    // SQLx resolves PostgreSQL enum OIDs/labels once per physical connection.
    // Warm both result and parameter type metadata before counting business SQL.
    crate::db::tables::evaluators::select_evaluator(&pool, "vigilo", "absent", "1.0.0")
        .await
        .unwrap();
    sqlx::query("SELECT $1::evaluator_state")
        .bind(crate::models::evaluator::EvaluatorState::Active)
        .execute(&pool)
        .await
        .unwrap();
    let (invalid, queries) = record_queries(publish(
        &pool,
        draft("vigilo", &"x".repeat(4096), "1.0.0", 9),
    ))
    .await;
    assert!(invalid.is_err());
    assert!(queries.is_empty(), "invalid identity reached data SQL");
    for (name, version, seed) in [("a", "1.0.0", 1), ("a", "2.0.0", 2), ("b", "1.0.0", 3)] {
        let (r, q) = record_queries(publish(&pool, draft("vigilo", name, version, seed))).await;
        r.unwrap();
        assert_eq!(q.len(), 3, "warm publication statements: {q:?}");
    }
    let (r, q) = record_queries(publish(&pool, draft("vigilo", "a", "1.0.0", 1))).await;
    assert!(matches!(
        r.unwrap(),
        crate::evaluators::PublishEvaluatorOutcome::Skipped(_)
    ));
    assert_eq!(q.len(), 1);
    assert!(
        publish(&pool, draft("vigilo", "a", "1.0.0", 4))
            .await
            .is_err()
    );
    assert!(
        publish(&pool, draft("vigilo", "other", "1.0.0", 1))
            .await
            .is_err()
    );
    let user = UserClient::from_fixed_pools(pool.clone(), "primary".into(), Default::default());
    assert!(
        user.evaluators("vigilo", Options::default()).await.is_err(),
        "owner/superuser must not masquerade as hosted registry"
    );
    let mut c = db::Connection::open(&pool, None).await.unwrap();
    let (page, q) = record_queries(db::page(
        &mut c,
        "vigilo",
        None,
        PageLimits::new(2, 8 * 1024 * 1024).unwrap(),
        None,
    ))
    .await;
    let page = page.unwrap();
    assert_eq!(q.len(), 1);
    assert_eq!(
        page.items
            .iter()
            .map(|e| (&*e.name, &*e.version))
            .collect::<Vec<_>>(),
        [("a", "2.0.0"), ("a", "1.0.0")]
    );
    let next = db::page(
        &mut c,
        "vigilo",
        None,
        PageLimits::default(),
        page.next.as_ref(),
    )
    .await
    .unwrap();
    assert_eq!(next.items.len(), 1);
    assert_eq!(next.items[0].name, "b");
    assert!(next.next.is_none());
    assert!(
        db::page(
            &mut c,
            "other",
            None,
            PageLimits::default(),
            page.next.as_ref()
        )
        .await
        .is_err()
    );
    let model = crate::db::tables::evaluators::select_evaluator(&mut *c, "vigilo", "a", "1.0.0")
        .await
        .unwrap()
        .unwrap();
    let expected = serde_json::to_value(Descriptor::from(model)).unwrap();
    let id = EvaluatorId::parse("vigilo/a:1.0.0").unwrap();
    let (actual, q) = record_queries(db::show(&mut c, &id)).await;
    assert_eq!(serde_json::to_value(actual.unwrap()).unwrap(), expected);
    assert_eq!(q.len(), 1);
    assert!(expected.get("wasm_bytes").is_none());
    let (r, q) = record_queries(crate::db::tables::evaluators::update_evaluator_state(
        &mut *c,
        "vigilo",
        "a",
        "1.0.0",
        &crate::models::evaluator::EvaluatorPatch {
            state: State::Removed.into(),
            state_reason: None,
        },
    ))
    .await;
    assert_eq!(r.unwrap(), 1);
    assert_eq!(q.len(), 1);
    assert_eq!(
        crate::db::tables::evaluators::update_evaluator_state(
            &mut *c,
            "vigilo",
            "a",
            "1.0.0",
            &crate::models::evaluator::EvaluatorPatch {
                state: State::Active.into(),
                state_reason: None
            }
        )
        .await
        .unwrap(),
        0
    );
    c.finish().await.unwrap();
    assert!(
        matches!(publish(&pool,draft("vigilo","a","1.0.0",1)).await.unwrap(),crate::evaluators::PublishEvaluatorOutcome::Skipped(e) if e.state==crate::models::evaluator::EvaluatorState::Removed)
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn caller_registry_pages_and_mutations_recheck_policy_without_pool_leakage(pool: PgPool) {
    use futures_util::FutureExt;
    let role = format!("registry_{}", Uuid::now_v7().simple());
    let a = UserContext::new(Uuid::now_v7(), Uuid::now_v7()).unwrap();
    let b = UserContext::new(Uuid::now_v7(), Uuid::now_v7()).unwrap();
    sqlx::raw_sql(&format!("CREATE ROLE {role} LOGIN PASSWORD 'fixture'; GRANT USAGE ON SCHEMA public TO {role}; ALTER TABLE evaluators ENABLE ROW LEVEL SECURITY; GRANT SELECT,INSERT,UPDATE ON evaluators TO {role}; CREATE POLICY caller ON evaluators TO {role} USING ((namespace='alpha' AND current_setting('vigilo.actor_id',true)='{}' AND current_setting('vigilo.tenant_id',true)='{}') OR (namespace='beta' AND current_setting('vigilo.actor_id',true)='{}' AND current_setting('vigilo.tenant_id',true)='{}'))",a.actor_id(),a.tenant_id(),b.actor_id(),b.tenant_id())).execute(&pool).await.unwrap();
    let login = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(2))
        .connect_with(
            pool.connect_options()
                .as_ref()
                .clone()
                .username(&role)
                .password("fixture"),
        )
        .await
        .unwrap();
    let outcome=std::panic::AssertUnwindSafe(async{
  publish(&pool,draft("alpha","score","1.0.0",1)).await.unwrap();publish(&pool,draft("beta","score","1.0.0",1)).await.unwrap();
  let root=UserClient::from_fixed_pools(login.clone(),"primary".into(),Default::default());
  let service=root.evaluators("alpha",Options::default()).await.unwrap();
  let ca=service.user(a);let cb=service.user(b);let id=EvaluatorId::parse("alpha/score:1.0.0").unwrap();
  let (r,q)=record_queries(ca.inspect(&id)).await;r.unwrap();assert_eq!(q.len(),2,"context plus one metadata read: {q:?}");
  assert!(matches!(cb.inspect(&id).await,Err(Error::EvaluatorNotFound(_))));
  let wrong=UserContext::new(a.tenant_id(),b.actor_id()).unwrap();assert!(service.user(wrong).inspect(&id).await.is_err());
  assert!(cb.set_state(&id,State::Removed,None).await.is_err());
  let(r,q)=record_queries(ca.set_state(&id,State::Yanked,Some("withdrawn".into()))).await;r.unwrap();assert_eq!(q.len(),2);
  let mut c=db::Connection::open(&login,Some(b)).await.unwrap();
  assert!(crate::evaluators::publish_draft_on(&mut c,draft("alpha","new","1.0.0",9)).await.is_err());drop(c);
  assert_eq!(ca.inspect(&id).await.unwrap().state,State::Yanked);
  let page=ca.search("alpha",None,PageLimits::new(1,8388608).unwrap(),None).await.unwrap();assert_eq!(page.items.len(),1);
  assert!(cb.search("alpha",None,PageLimits::default(),None).await.unwrap().items.is_empty());
  let page=cb.search("beta",None,PageLimits::default(),None).await.unwrap();assert_eq!(page.items.len(),1);
  sqlx::raw_sql(&format!("DROP POLICY caller ON evaluators; CREATE POLICY caller ON evaluators TO {role} USING(false)")).execute(&pool).await.unwrap();
  assert!(ca.inspect(&id).await.is_err());assert!(ca.set_state(&id,State::Active,None).await.is_err());
  let actor:Option<String>=sqlx::query_scalar("SELECT current_setting('vigilo.actor_id',true)").fetch_one(&login).await.unwrap();assert!(actor.is_none_or(|s|s.is_empty()));
  sqlx::raw_sql("ALTER TABLE evaluators DISABLE ROW LEVEL SECURITY").execute(&pool).await.unwrap();
  assert!(root.evaluators("alpha",Options::default()).await.is_err());
 }).catch_unwind().await;
    login.close().await;
    sqlx::raw_sql(&format!("DROP OWNED BY {role};DROP ROLE {role}"))
        .execute(&pool)
        .await
        .unwrap();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn discovery_charges_bytes_and_rejects_oversized_descriptors(pool: PgPool) {
    for i in 0..4 {
        let mut d = draft("vigilo", &format!("entry{i}"), "1.0.0", i);
        d.description = Some("x".repeat(100_000));
        publish(&pool, d).await.unwrap();
    }
    let mut c = db::Connection::open(&pool, None).await.unwrap();
    let mut cursor = None;
    let mut seen = Vec::new();
    loop {
        let page = db::page(
            &mut c,
            "vigilo",
            None,
            PageLimits::new(20, 200_000).unwrap(),
            cursor.as_ref(),
        )
        .await
        .unwrap();
        assert!(serde_json::to_vec(&page.items).unwrap().len() <= 200_000);
        seen.extend(page.items.into_iter().map(|e| e.name));
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(seen, ["entry0", "entry1", "entry2", "entry3"]);
    sqlx::query("UPDATE evaluators SET metadata=jsonb_build_object('x',repeat('x',8388609)) WHERE name='entry0'").execute(&mut *c).await.unwrap();
    assert!(
        db::show(&mut c, &EvaluatorId::parse("vigilo/entry0:1.0.0").unwrap())
            .await
            .is_err()
    );
    assert!(
        db::page(&mut c, "vigilo", None, PageLimits::default(), None)
            .await
            .is_err()
    );
    c.finish().await.unwrap();
}
