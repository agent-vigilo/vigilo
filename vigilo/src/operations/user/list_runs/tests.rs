//! Run listing contracts under real protected control reads.

use sqlx::{
    ConnectOptions as _,
    PgPool,
    Postgres,
    Transaction,
};
use uuid::Uuid;

use super::*;
use crate::operations::{
    Config,
    UserConfig,
};

const SEED: &str = "INSERT INTO runs (tenant_id,id,run_key,dataset_id,dataset_version_id,dataset_version,
    evaluation_profile_id,evaluation_profile_version,profile_version_id,profile_hash,aggregation_policy_id,
    aggregation_policy_version,aggregation_policy_hash,agent_provider,agent_name,prompt_config_id,
    prompt_config_version,created_at)
    SELECT $1,md5($1::text || n::text)::uuid,n::text,$1,$1,'1','p','1','p:1','hash','a','1','hash','p','a',
        'p','1','2026-01-01'::timestamptz + (n / 10) * interval '1 second'
    FROM generate_series($2::integer,$3::integer) AS n";

async fn seed(pool: &PgPool, tenant: Uuid, first: i32, last: i32) {
    sqlx::query("INSERT INTO dataset_versions(tenant_id,dataset_id,dataset_version_id,dataset_version) VALUES ($1,$1,$1,'1') ON CONFLICT DO NOTHING")
        .bind(tenant).execute(pool).await.unwrap();
    sqlx::query(SEED)
        .bind(tenant)
        .bind(first)
        .bind(last)
        .execute(pool)
        .await
        .unwrap();
}

async fn client(pool: &PgPool, owner: Uuid, sparse: Uuid) -> UserClient {
    let role = format!("listing_{}", Uuid::now_v7().simple());
    sqlx::raw_sql(&format!(
        "CREATE ROLE {role} LOGIN PASSWORD 'fixture';
         GRANT SELECT ON runs TO {role};
         ALTER TABLE runs ENABLE ROW LEVEL SECURITY;
         CREATE POLICY scoped_list ON runs FOR SELECT TO {role} USING (
             current_setting('vigilo.run_batch',true) = '' AND (
                 (current_setting('vigilo.actor_id',true) = '{owner}'
                     AND (current_setting('vigilo.run_id',true) = ''
                         OR tenant_id::text = current_setting('vigilo.tenant_id',true)))
                 OR (current_setting('vigilo.run_id',true) = ''
                     AND current_setting('vigilo.actor_id',true) = '{sparse}'
                     AND length(run_key) <= 2)));"
    ))
    .execute(pool)
    .await
    .unwrap();
    let role_options = pool
        .connect_options()
        .as_ref()
        .clone()
        .username(&role)
        .password("fixture");
    let mut control = Config::new(role_options.to_url_lossy().to_string());
    control.max_connections = 1;
    UserClient::new(UserConfig {
        control,
        database_role: role,
        execution_databases: [(
            "unused".into(),
            "postgresql://127.0.0.1:1/unavailable".into(),
        )]
        .into(),
    })
    .unwrap()
}

fn request(page_size: u16, cursor: Option<String>) -> ListRunsRequest {
    ListRunsRequest { page_size, cursor }
}

async fn exact_ids(pool: &PgPool, tenant: Uuid) -> Vec<Uuid> {
    sqlx::query_scalar("SELECT id FROM runs WHERE tenant_id=$1 ORDER BY created_at DESC,id DESC")
        .bind(tenant)
        .fetch_all(pool)
        .await
        .unwrap()
}

fn ids(page: &RunPage) -> Vec<Uuid> {
    page.items.iter().map(|item| item.run_id).collect()
}

#[tokio::test]
async fn invalid_page_and_cursor_fail_before_database_access() {
    let client = UserClient::new(UserConfig {
        control: Config::new("postgresql://127.0.0.1:1/unavailable".into()),
        database_role: "listing".into(),
        execution_databases: Default::default(),
    })
    .unwrap();
    let context = UserContext::new(Uuid::now_v7(), Uuid::now_v7()).unwrap();
    let unsupported = URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&Position {
            version: CURSOR_VERSION + 1,
            scope: cursor_scope(&client.control_alias, context)
                .to_hex()
                .to_string(),
            created_at: Utc::now(),
            run_id: Uuid::now_v7(),
        })
        .unwrap(),
    );
    for invalid in [
        request(0, None),
        request(101, None),
        request(10, Some("!".into())),
        request(10, Some("x".repeat(MAX_CURSOR_BYTES + 1))),
        request(10, Some(unsupported)),
    ] {
        assert!(matches!(
            client.list_runs(context, invalid).await,
            Err(Error::InvalidInput(_))
        ));
    }
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn lists_exact_visible_runs_with_scoped_keyset_pages(pool: PgPool) {
    let tenant = Uuid::now_v7();
    let foreign = Uuid::now_v7();
    let actor = Uuid::now_v7();
    let caller = UserContext::new(tenant, actor).unwrap();
    let client = client(&pool, actor, Uuid::now_v7()).await;
    seed(&pool, tenant, 1, 22).await;
    seed(&pool, foreign, 100, 120).await;
    let expected = exact_ids(&pool, tenant).await;

    // A pooled connection may have an exact run context in a different operation.
    let mut exact_tx = scope::begin(&client.control, caller, expected[0])
        .await
        .unwrap();
    let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM runs")
        .fetch_one(&mut *exact_tx)
        .await
        .unwrap();
    assert_eq!(visible, 22);
    exact_tx.commit().await.unwrap();

    let (first, queries) = crate::db::workflows::run_shard_summary::test_support::record_queries(
        client.list_runs(caller, request(3, None)),
    )
    .await;
    let first = first.unwrap();
    assert_eq!(ids(&first), expected[..3]);
    assert_eq!(first.items[0].status, "pending");
    assert_eq!(first.items[0].gate_status, "unknown");
    assert_eq!(queries.len(), 2, "{queries:?}");
    assert!(queries[0].contains("set_config("));
    assert!(queries[1].contains("FROM runs"));

    let token = first.next.clone().unwrap();
    for other in [
        UserContext::new(foreign, actor).unwrap(),
        UserContext::new(tenant, Uuid::now_v7()).unwrap(),
    ] {
        assert!(matches!(
            client
                .list_runs(other, request(3, Some(token.clone())))
                .await,
            Err(Error::InvalidInput(_))
        ));
    }
    let mut wrong_control = client.clone();
    wrong_control.control_alias = "other-control".into();
    assert!(matches!(
        wrong_control
            .list_runs(caller, request(3, Some(token.clone())))
            .await,
        Err(Error::InvalidInput(_))
    ));
    let denied = UserContext::new(tenant, Uuid::now_v7()).unwrap();
    assert!(
        client
            .list_runs(denied, request(100, None))
            .await
            .unwrap()
            .items
            .is_empty()
    );

    // Insert on another connection after page one. The new key sorts ahead
    // of the cursor. Tied timestamps already span several pages.
    seed(&pool, tenant, 100, 100).await;
    seed(&pool, tenant, -20, -20).await;
    let with_older = exact_ids(&pool, tenant).await;
    assert_eq!(with_older[1..23], expected);
    let mut seen = ids(&first);
    let mut next = first.next;
    for _ in 0..10 {
        let Some(cursor) = next else { break };
        let page = client
            .list_runs(caller, request(3, Some(cursor)))
            .await
            .unwrap();
        seen.extend(ids(&page));
        next = page.next;
    }
    assert!(next.is_none());
    assert_eq!(seen, with_older[1..]);
    let fresh = client.list_runs(caller, request(100, None)).await.unwrap();
    assert_eq!(ids(&fresh), with_older);
    assert!(fresh.next.is_none());

    sqlx::raw_sql("CREATE POLICY revoke_lists ON runs AS RESTRICTIVE FOR SELECT USING (current_setting('vigilo.run_id',true) <> '')")
        .execute(&pool).await.unwrap();
    let revoked = client
        .list_runs(caller, request(3, Some(token)))
        .await
        .unwrap();
    assert!(revoked.items.is_empty() && revoked.next.is_none());
}

async fn explain(
    tx: &mut Transaction<'_, Postgres>,
    tenant: Uuid,
    position: &RunListItem,
) -> serde_json::Value {
    let statement = format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {NEXT_PAGE}");
    sqlx::query_scalar(&statement)
        .bind(tenant)
        .bind(position.created_at)
        .bind(position.run_id)
        .bind(21_i64)
        .fetch_one(&mut **tx)
        .await
        .unwrap()
}

fn run_scan(plan: &serde_json::Value) -> Option<&serde_json::Value> {
    if plan["Relation Name"] == "runs" {
        return Some(plan);
    }
    plan["Plans"].as_array()?.iter().find_map(run_scan)
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn measures_run_list_index_work_as_history_grows(pool: PgPool) {
    let tenant = Uuid::now_v7();
    let dense = Uuid::now_v7();
    let sparse = Uuid::now_v7();
    let client = client(&pool, dense, sparse).await;
    let dense_context = UserContext::new(tenant, dense).unwrap();
    let sparse_context = UserContext::new(tenant, sparse).unwrap();
    let mut first = 1;
    for last in [1_000, 100_000] {
        seed(&pool, tenant, first, last).await;
        first = last + 1;
        sqlx::query("ANALYZE runs").execute(&pool).await.unwrap();

        let tenant_policy = "tenant_id = NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid";
        sqlx::raw_sql(&format!(
            "ALTER POLICY scoped_list ON runs USING ({tenant_policy})"
        ))
        .execute(&pool)
        .await
        .unwrap();
        let page = client
            .list_runs(dense_context, request(20, None))
            .await
            .unwrap();
        let key = &page.items[19];
        for (name, context, index) in [
            ("dense", dense_context, Some("idx_runs_tenant_created_id")),
            ("sparse", sparse_context, None),
            (
                "indexed_sparse",
                sparse_context,
                Some("idx_fixture_sparse_listing"),
            ),
        ] {
            if name == "sparse" {
                sqlx::raw_sql(&format!(
                    "ALTER POLICY scoped_list ON runs USING ({tenant_policy} AND length(run_key) <= 2)"
                )).execute(&pool).await.unwrap();
            }
            if name == "indexed_sparse" {
                sqlx::query("CREATE INDEX idx_fixture_sparse_listing ON runs (tenant_id, created_at DESC, id DESC) WHERE length(run_key) <= 2")
                    .execute(&pool).await.unwrap();
            }
            let mut tx = scope::begin_list(&client.control, context).await.unwrap();
            let measured = explain(&mut tx, tenant, key).await;
            tx.commit().await.unwrap();
            let scan = run_scan(&measured[0]["Plan"]).expect("plan must read runs");
            assert_eq!(measured[0]["Plan"]["Actual Rows"], 21, "{measured}");
            let visited = scan["Actual Rows"].as_u64().unwrap()
                + scan["Rows Removed by Filter"].as_u64().unwrap_or_default()
                + scan["Rows Removed by Index Recheck"]
                    .as_u64()
                    .unwrap_or_default();
            let buffers = scan["Shared Hit Blocks"].as_u64().unwrap()
                + scan["Shared Read Blocks"].as_u64().unwrap();
            if let Some(index) = index {
                assert_eq!(scan["Node Type"], "Index Scan", "{measured}");
                assert_eq!(scan["Index Name"], index, "{measured}");
                assert_eq!(visited, 21);
                assert!(buffers <= 32, "{measured}");
            }
            eprintln!(
                "run-list {name}: history={last} rows_visited={visited} buffers={buffers} scan={}",
                scan["Node Type"]
            );
        }
        sqlx::query("DROP INDEX idx_fixture_sparse_listing")
            .execute(&pool)
            .await
            .unwrap();
    }
}
