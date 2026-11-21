//! Real PostgreSQL projection, paging, privilege and query-budget contracts.
use sqlx::{
    ConnectOptions,
    PgPool,
};
use uuid::Uuid;

use super::*;
use crate::{
    db::workflows::run_shard_summary::test_support::record_queries,
    operations::{
        Config,
        UserConfig,
        UserContext,
    },
};

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn trusted_policy_helpers_preserve_private_authority_and_page_query_budget(pool: PgPool) {
    let (tenant, run, _) = fixture(&pool).await;
    let actor = Uuid::now_v7();
    let original_owner: String = sqlx::query_scalar("SELECT quote_ident(current_user)")
        .fetch_one(&pool)
        .await
        .unwrap();
    let context = UserContext::new(tenant, actor).unwrap();
    let (mut config, role) = restricted(&pool, false).await;
    config.control.acquire_timeout = std::time::Duration::from_secs(2);
    sqlx::raw_sql(&format!(r#"
CREATE SCHEMA access;
CREATE TABLE access.permissions(actor_id uuid NOT NULL,run_id uuid NOT NULL,PRIMARY KEY(actor_id,run_id));
INSERT INTO access.permissions VALUES('{actor}','{run}');
CREATE FUNCTION access.can_read_run(uuid) RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER
SET search_path=pg_catalog,pg_temp AS 'SELECT EXISTS(SELECT 1 FROM access.permissions WHERE actor_id=NULLIF(current_setting(''vigilo.actor_id'',true),'''')::uuid AND run_id=$1)';
REVOKE ALL ON FUNCTION access.can_read_run(uuid) FROM PUBLIC;
GRANT USAGE ON SCHEMA access TO {role};
GRANT EXECUTE ON FUNCTION access.can_read_run(uuid) TO {role};
ALTER POLICY {role} ON runs USING(tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND access.can_read_run(id));
ALTER POLICY {role} ON executions USING(tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND access.can_read_run(run_id));
"#)).execute(&pool).await.unwrap();
    let direct = PgPool::connect(&config.control.database_url).await.unwrap();
    assert!(
        sqlx::query("SELECT * FROM access.permissions")
            .execute(&direct)
            .await
            .is_err()
    );
    let default_reader = ExportClient::new(config.clone()).unwrap();
    assert!(
        default_reader
            .page(
                context,
                run,
                ExportCollection::Executions,
                ExportLimits::default(),
                None
            )
            .await
            .is_err()
    );
    default_reader.close().await;
    let functions = std::collections::BTreeMap::from([(
        "primary".to_string(),
        vec!["access.can_read_run(uuid)".to_string()],
    )]);
    let reader =
        ExportClient::with_trusted_policy_functions(config.clone(), functions.clone()).unwrap();
    let (first, cold_queries) = record_queries(reader.page(
        context,
        run,
        ExportCollection::Executions,
        ExportLimits::new(1, 4096).unwrap(),
        None,
    ))
    .await;
    let first = first.expect("explicitly trusted policy helper must permit the protected read");
    assert_eq!(
        cold_queries.len(),
        7,
        "helper validation shares the existing cold guard submission"
    );
    assert_eq!(first.records.len(), 1);
    let (next, queries) = record_queries(reader.page(
        context,
        run,
        ExportCollection::Executions,
        ExportLimits::new(1, 4096).unwrap(),
        first.next.as_ref(),
    ))
    .await;
    assert_eq!(next.unwrap().records.len(), 1);
    assert_eq!(
        queries.len(),
        4,
        "trusted policy functions add no warm page submissions"
    );
    let (mut sensitive_config, sensitive_role) = restricted(&pool, true).await;
    sensitive_config.control.acquire_timeout = std::time::Duration::from_secs(2);
    sqlx::raw_sql(&format!("GRANT USAGE ON SCHEMA access TO {sensitive_role}; GRANT EXECUTE ON FUNCTION access.can_read_run(uuid) TO {sensitive_role}; ALTER POLICY {sensitive_role} ON runs USING(tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND access.can_read_run(id)); ALTER POLICY {sensitive_role} ON executions USING(tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND access.can_read_run(run_id))")).execute(&pool).await.unwrap();
    let sensitive =
        SensitiveExportClient::with_trusted_policy_functions(sensitive_config, functions).unwrap();
    let content = sensitive
        .content_page(
            context,
            run,
            ExportCollection::ExecutionContent,
            ExportLimits::default(),
            None,
        )
        .await
        .unwrap();
    assert!(
        serde_json::to_string(&content)
            .unwrap()
            .contains("PRIVATE-CANARY")
    );
    let (combined, queries) = record_queries(async {
        reader
            .page(
                context,
                run,
                ExportCollection::Executions,
                ExportLimits::default(),
                None,
            )
            .await?;
        sensitive
            .content_page(
                context,
                run,
                ExportCollection::ExecutionContent,
                ExportLimits::default(),
                None,
            )
            .await
    })
    .await;
    assert_eq!(combined.unwrap().records.len(), 3);
    assert_eq!(queries.len(), 8);
    let mut connection = direct.acquire().await.unwrap();
    let trusted = vec!["access.can_read_run(uuid)".to_string()];
    for invalid in [
        "can_read_run(uuid)",
        "access.missing(uuid)",
        "access.can_read_run(text)",
    ] {
        let mut list = trusted.clone();
        list.push(invalid.into());
        assert!(
            reader::permissions(&mut connection, false, &list)
                .await
                .is_err(),
            "must reject {invalid}"
        );
    }
    for (return_type, volatility, path, body, security) in [
        (
            "text",
            "STABLE",
            "SET search_path=pg_catalog,pg_temp",
            "'hidden'::text",
            "SECURITY DEFINER",
        ),
        (
            "boolean",
            "VOLATILE",
            "SET search_path=pg_catalog,pg_temp",
            "true",
            "SECURITY DEFINER",
        ),
        (
            "boolean",
            "IMMUTABLE",
            "SET search_path=pg_catalog,pg_temp",
            "true",
            "SECURITY DEFINER",
        ),
        ("boolean", "STABLE", "", "true", "SECURITY DEFINER"),
        (
            "boolean",
            "STABLE",
            "SET search_path=public,pg_catalog,pg_temp",
            "true",
            "SECURITY DEFINER",
        ),
        (
            "SETOF boolean",
            "STABLE",
            "SET search_path=pg_catalog,pg_temp",
            "true",
            "SECURITY DEFINER",
        ),
        (
            "boolean",
            "STABLE",
            "SET search_path=pg_catalog,pg_temp",
            "true",
            "SECURITY INVOKER",
        ),
    ] {
        sqlx::raw_sql(&format!("CREATE FUNCTION access.unsuitable(uuid) RETURNS {return_type} LANGUAGE sql {volatility} {security} {path} AS $$ SELECT {body} $$; REVOKE ALL ON FUNCTION access.unsuitable(uuid) FROM PUBLIC; GRANT EXECUTE ON FUNCTION access.unsuitable(uuid) TO {role}")).execute(&pool).await.unwrap();
        let list = vec![trusted[0].clone(), "access.unsuitable(uuid)".into()];
        assert!(
            reader::permissions(&mut connection, false, &list)
                .await
                .is_err()
        );
        sqlx::query("DROP FUNCTION access.unsuitable(uuid)")
            .execute(&pool)
            .await
            .unwrap();
    }
    sqlx::raw_sql(&format!("CREATE FUNCTION access.can_read_run(text) RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$ SELECT false $$; REVOKE ALL ON FUNCTION access.can_read_run(text) FROM PUBLIC; GRANT EXECUTE ON FUNCTION access.can_read_run(text) TO {role}")).execute(&pool).await.unwrap();
    assert!(
        reader::permissions(&mut connection, false, &trusted)
            .await
            .is_err(),
        "an unlisted overload must remain denied"
    );
    let switch_role = format!("policy_switch_{}", Uuid::now_v7().simple());
    sqlx::raw_sql(&format!("CREATE ROLE {switch_role}; REVOKE EXECUTE ON FUNCTION access.can_read_run(text) FROM {role}; GRANT EXECUTE ON FUNCTION access.can_read_run(text) TO {switch_role}; GRANT {switch_role} TO {role} WITH INHERIT false")).execute(&pool).await.unwrap();
    assert!(
        reader::permissions(&mut connection, false, &trusted)
            .await
            .is_err(),
        "trust must not hide an unlisted function reachable through SET ROLE"
    );
    sqlx::raw_sql(&format!(
        "REVOKE {switch_role} FROM {role}; DROP OWNED BY {switch_role}; DROP ROLE {switch_role}"
    ))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("DROP FUNCTION access.can_read_run(text)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("REVOKE EXECUTE ON FUNCTION access.can_read_run(uuid) FROM PUBLIC")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::raw_sql(&format!(
        "REVOKE EXECUTE ON FUNCTION access.can_read_run(uuid) FROM {role}"
    ))
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        reader::permissions(&mut connection, false, &trusted)
            .await
            .is_err(),
        "trust must not grant execution rights"
    );
    sqlx::raw_sql(&format!("GRANT EXECUTE ON FUNCTION access.can_read_run(uuid) TO {role}; GRANT CREATE ON SCHEMA access TO {role}")).execute(&pool).await.unwrap();
    assert!(
        reader::permissions(&mut connection, false, &trusted)
            .await
            .is_err(),
        "reader must not alter the helper namespace"
    );
    sqlx::raw_sql(&format!("REVOKE CREATE ON SCHEMA access FROM {role}; ALTER SCHEMA access OWNER TO {role}; REVOKE CREATE ON SCHEMA access FROM {role}")).execute(&pool).await.unwrap();
    let can_create: bool = sqlx::query_scalar("SELECT has_schema_privilege('access','CREATE')")
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    assert!(
        !can_create,
        "fixture must separate namespace ownership from its CREATE grant"
    );
    assert!(
        reader::permissions(&mut connection, false, &trusted)
            .await
            .is_err(),
        "schema ownership must not bypass the trusted-code boundary after CREATE is revoked"
    );
    sqlx::raw_sql(&format!("ALTER SCHEMA access OWNER TO {original_owner}; GRANT USAGE ON SCHEMA access TO {role}; ALTER FUNCTION access.can_read_run(uuid) OWNER TO {role}")).execute(&pool).await.unwrap();
    assert!(
        reader::permissions(&mut connection, false, &trusted)
            .await
            .is_err(),
        "reader must not replace trusted code"
    );
    sqlx::raw_sql(&format!("ALTER FUNCTION access.can_read_run(uuid) OWNER TO {original_owner}; GRANT EXECUTE ON FUNCTION access.can_read_run(uuid) TO {role}")).execute(&pool).await.unwrap();
    assert!(
        reader::permissions(&mut connection, false, &trusted)
            .await
            .is_ok()
    );
    drop(connection);
    let other = UserContext::new(tenant, Uuid::now_v7()).unwrap();
    assert!(matches!(
        reader
            .page(
                other,
                run,
                ExportCollection::Executions,
                ExportLimits::default(),
                None
            )
            .await,
        Err(Error::NotFound(_))
    ));
    sqlx::query("DELETE FROM access.permissions WHERE actor_id=$1")
        .bind(actor)
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        reader
            .page(
                context,
                run,
                ExportCollection::Executions,
                ExportLimits::default(),
                first.next.as_ref()
            )
            .await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        sensitive
            .content_page(
                context,
                run,
                ExportCollection::ExecutionContent,
                ExportLimits::default(),
                None
            )
            .await,
        Err(Error::NotFound(_))
    ));
    reader.close().await;
    sensitive.close().await;
    direct.close().await;
    sqlx::raw_sql(&format!("DROP OWNED BY {role}; DROP ROLE {role}; DROP OWNED BY {sensitive_role}; DROP ROLE {sensitive_role}"))
        .execute(&pool)
        .await
        .unwrap();
}

async fn fixture(pool: &PgPool) -> (Uuid, Uuid, Uuid) {
    let tenant = Uuid::now_v7();
    let run = Uuid::now_v7();
    let dataset = Uuid::now_v7();
    let chunk = Uuid::now_v7();
    sqlx::query("INSERT INTO dataset_versions(tenant_id,dataset_version_id,dataset_id,dataset_version) VALUES($1,$2,$2,'1')").bind(tenant).bind(dataset).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO runs(tenant_id,id,run_key,dataset_id,dataset_version_id,dataset_version,evaluation_profile_id,evaluation_profile_version,profile_version_id,profile_hash,aggregation_policy_id,aggregation_policy_version,aggregation_policy_hash,agent_provider,agent_name,prompt_config_id,prompt_config_version,status) VALUES($1,$2,'export',$3,$3,'1','profile','1','profile:1','hash','policy','1','hash','test','agent','prompt','1','running')").bind(tenant).bind(run).bind(dataset).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO database_placements(alias,database_url_env,role,status) VALUES('primary','DATABASE_URL','control_and_shard','active') ON CONFLICT DO NOTHING").execute(pool).await.unwrap();
    sqlx::query("INSERT INTO shard_placements(tenant_id,run_id,run_shard,database_alias,status) SELECT $1,$2,s,'primary','active' FROM generate_series(0,127) s").bind(tenant).bind(run).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO run_chunks(tenant_id,id,run_id,run_shard,dataset_version_id,profile_group_id,ordinal_start,ordinal_end) VALUES($1,$2,$3,0,$4,'default',0,3)").bind(tenant).bind(chunk).bind(run).bind(dataset).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO local_shard_admissions(tenant_id,run_id,run_shard,database_alias,write_epoch,state) SELECT $1,$2,s,'primary',1,'open' FROM generate_series(0,127) s").bind(tenant).bind(run).execute(pool).await.unwrap();
    for _ in 0..3 {
        sqlx::query("INSERT INTO executions(tenant_id,id,run_id,run_shard,chunk_id,case_id,case_hash,profile_group_id,task_type,evaluation_profile_id,evaluation_profile_version,input_payload) VALUES($1,$2,$3,0,$4,$2,$2::text,'default','test','profile','1',$5)").bind(tenant).bind(Uuid::now_v7()).bind(run).bind(chunk).bind(serde_json::json!({"secret":"PRIVATE-CANARY-雪"})).execute(pool).await.unwrap();
    }
    (tenant, run, chunk)
}

fn operator(pool: &PgPool) -> crate::operations::Client {
    crate::operations::Client::new(Config::new(
        pool.connect_options().to_url_lossy().to_string(),
    ))
    .unwrap()
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn bounded_pages_preserve_records_and_reject_oversized_content(pool: PgPool) {
    let (_, run, _) = fixture(&pool).await;
    let client = operator(&pool);
    let limits = ExportLimits::new(2, 4096).unwrap();
    let first = client
        .export_page(run, ExportCollection::Executions, limits, None)
        .await
        .unwrap();
    assert_eq!(first.records.len(), 2);
    assert!(first.encoded_bytes <= 4096);
    assert!(
        !serde_json::to_string(&first)
            .unwrap()
            .contains("PRIVATE-CANARY")
    );
    let next = client
        .export_page(
            run,
            ExportCollection::Executions,
            limits,
            first.next.as_ref(),
        )
        .await
        .unwrap();
    assert_eq!(next.records.len(), 1);
    assert!(next.next.is_none());
    assert!(matches!(
        client
            .export_page(run, ExportCollection::Attempts, limits, first.next.as_ref())
            .await,
        Err(Error::InvalidInput(_))
    ));
    let content = client
        .export_page(run, ExportCollection::ExecutionContent, limits, None)
        .await
        .unwrap();
    assert!(
        serde_json::to_string(&content)
            .unwrap()
            .contains("PRIVATE-CANARY")
    );
    let bytes = content.encoded_bytes as u32;
    let one = client
        .export_page(
            run,
            ExportCollection::ExecutionContent,
            ExportLimits::new(10, bytes - 1).unwrap(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(one.records.len(), 1);
    assert!(one.next.is_some());
    sqlx::query("UPDATE executions SET input_payload=jsonb_build_object('large',repeat('雪',20000)) WHERE run_id=$1").bind(run).execute(&pool).await.unwrap();
    assert!(
        client
            .export_page(run, ExportCollection::ExecutionContent, limits, None)
            .await
            .is_err()
    );
    // Metadata remains readable despite an oversized sensitive payload.
    assert_eq!(
        client
            .export_page(run, ExportCollection::Executions, limits, None)
            .await
            .unwrap()
            .records
            .len(),
        2
    );
    let (tenant, execution): (Uuid, Uuid) =
        sqlx::query_as("SELECT tenant_id,id FROM executions WHERE run_id=$1 ORDER BY id LIMIT 1")
            .bind(run)
            .fetch_one(&pool)
            .await
            .unwrap();
    let attempt = Uuid::now_v7();
    let result = Uuid::now_v7();
    sqlx::query("INSERT INTO execution_attempts(tenant_id,id,run_id,run_shard,execution_id,attempt_no,status,outcome_summary) VALUES($1,$2,$3,0,$4,1,'completed',$5)").bind(tenant).bind(attempt).bind(run).bind(execution).bind(serde_json::json!({"private":"PRIVATE-CANARY"})).execute(&pool).await.unwrap();
    sqlx::query("UPDATE executions SET current_attempt_id=$2,current_attempt_no=1,status='completed' WHERE id=$1 AND run_id=$3").bind(execution).bind(attempt).bind(run).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO execution_aggregates(tenant_id,run_id,run_shard,execution_id,attempt_id,overall_status,aggregate_score,evaluator_result_count) VALUES($1,$2,0,$3,$4,'passed',1,1)").bind(tenant).bind(run).bind(execution).bind(attempt).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO evaluator_results(tenant_id,id,run_id,run_shard,execution_id,attempt_id,binding_id,evaluator_id,evaluator_version,evaluator_profile_id,evaluator_profile_version,dimension,outcome,judgment,measurement_kind,raw_boolean,normalized_score,normalization_policy_hash,pass_threshold,raw_evaluator_output) VALUES($1,$2,$3,0,$4,$5,'test',$6,'1','profile','1','quality','completed','passed','binary',true,1,'hash',0.5,$7)").bind(tenant).bind(result).bind(run).bind(execution).bind(attempt).bind(Uuid::now_v7()).bind(serde_json::json!({"private":"PRIVATE-CANARY"})).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO evaluator_diagnostics(tenant_id,run_id,run_shard,evaluator_result_id,diagnostic_index,severity,category,evidence) VALUES($1,$2,0,$3,0,'low','test',$4)").bind(tenant).bind(run).bind(result).bind(serde_json::json!({"private":"PRIVATE-CANARY"})).execute(&pool).await.unwrap();
    for collection in [
        ExportCollection::Attempts,
        ExportCollection::Aggregates,
        ExportCollection::EvaluatorResults,
        ExportCollection::AttemptContent,
        ExportCollection::AggregateContent,
        ExportCollection::EvaluatorResultContent,
        ExportCollection::Diagnostics,
    ] {
        let page = client
            .export_page(run, collection, limits, None)
            .await
            .unwrap();
        assert_eq!(page.records.len(), 1);
    }
    sqlx::query("UPDATE executions SET status='completed',current_attempt_id=$2,current_attempt_no=1 WHERE id=$1").bind(execution).bind(attempt).execute(&pool).await.unwrap();
    let page = client
        .export_page(run, ExportCollection::Attempts, limits, None)
        .await
        .unwrap();
    assert!(
        matches!(&page.records[0],ExportRecord::AttemptMetadata(row) if row.evidence_state=="authoritative")
    );
    let page = client
        .export_page(run, ExportCollection::Aggregates, limits, None)
        .await
        .unwrap();
    assert!(matches!(&page.records[0],ExportRecord::AggregateMetadata(row) if row.authoritative));
    sqlx::query("UPDATE execution_attempts SET status='stale' WHERE id=$1")
        .bind(attempt)
        .execute(&pool)
        .await
        .unwrap();
    let page = client
        .export_page(run, ExportCollection::Attempts, limits, None)
        .await
        .unwrap();
    assert!(
        matches!(&page.records[0],ExportRecord::AttemptMetadata(row) if row.evidence_state=="stale")
    );
    sqlx::query("UPDATE execution_attempts SET status='completed' WHERE id=$1")
        .bind(attempt)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE executions SET status='running' WHERE id=$1")
        .bind(execution)
        .execute(&pool)
        .await
        .unwrap();
    let page = client
        .export_page(run, ExportCollection::EvaluatorResults, limits, None)
        .await
        .unwrap();
    assert!(
        matches!(&page.records[0],ExportRecord::EvaluatorResultMetadata(row) if row.evidence_state=="partial")
    );
    // The private CLI pager preserves every operator field while bounding child
    // fan-out across the batch. Compare it to the existing materialized oracle.
    use crate::db::workflows::run_export::{
        self,
        ChildPager,
        Kind,
    };
    sqlx::query("INSERT INTO evaluator_diagnostics(tenant_id,run_id,run_shard,evaluator_result_id,diagnostic_index,severity,category,evidence) SELECT $1,$2,0,$3,i,'low','test',jsonb_build_object('i',i) FROM generate_series(1,1000) i").bind(tenant).bind(run).bind(result).execute(&pool).await.unwrap();
    // A long binding label is not an ordering-cursor payload. Diagnostics must
    // retain it through their result reference without imposing a new label limit.
    let second_result = Uuid::from_u128(1);
    sqlx::query("INSERT INTO evaluator_results SELECT (jsonb_populate_record(NULL::evaluator_results,to_jsonb(r)||jsonb_build_object('id',$2::uuid,'binding_id',repeat('z',2048)))).* FROM evaluator_results r WHERE id=$1").bind(result).bind(second_result).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO evaluator_diagnostics(tenant_id,run_id,run_shard,evaluator_result_id,diagnostic_index,severity,category) VALUES($1,$2,0,$3,0,'low','long-binding')").bind(tenant).bind(run).bind(second_result).execute(&pool).await.unwrap();
    let routes = run_export::select_run_export_routes(&client.database, run)
        .await
        .unwrap();
    let route = &routes[0];
    let executions = run_export::select_execution_batch(route, run, None, 100)
        .await
        .unwrap();
    let ids = executions.iter().map(|e| e.id).collect::<Vec<_>>();
    let oracle = run_export::select_materialized_oracle(route, run, executions)
        .await
        .unwrap();
    let root = run_export::select_execution_batch(route, run, None, 100)
        .await
        .unwrap();
    let (buffered, queries) =
        record_queries(run_export::select_batch_for_executions(route, run, root)).await;
    let buffered = buffered.unwrap();
    assert_eq!(
        serde_json::to_value(&buffered.attempts).unwrap(),
        serde_json::to_value(&oracle.attempts).unwrap()
    );
    assert_eq!(
        serde_json::to_value(&buffered.aggregates).unwrap(),
        serde_json::to_value(&oracle.aggregates).unwrap()
    );
    assert_eq!(
        serde_json::to_value(&buffered.evaluator_results).unwrap(),
        serde_json::to_value(&oracle.evaluator_results).unwrap()
    );
    assert_eq!(
        serde_json::to_value(&buffered.evaluator_diagnostics).unwrap(),
        serde_json::to_value(&oracle.evaluator_diagnostics).unwrap()
    );
    assert_eq!(
        queries.len(),
        5,
        "shared initial pages plus four diagnostic continuations"
    );
    let mut pager = ChildPager::<crate::models::evaluator_result::EvaluatorDiagnostic>::new(
        route,
        run,
        &ids,
        Kind::Diagnostic,
        7,
    );
    let (actual, queries) = record_queries(async {
        let mut actual = Vec::new();
        while pager.peek().await.unwrap().is_some() {
            actual.push(pager.pop());
        }
        actual
    })
    .await;
    // Streaming diagnostics follow their enclosing result; the JSON document
    // retains its independent result-ID ordering. Use the existing SQL rows to
    // assemble that parent grouping rather than assuming UUID order matches it.
    let expected_stream = oracle
        .evaluator_results
        .iter()
        .flat_map(|result| {
            oracle
                .evaluator_diagnostics
                .iter()
                .filter(move |diagnostic| {
                    (diagnostic.run_shard, diagnostic.evaluator_result_id)
                        == (result.run_shard, result.id)
                })
        })
        .collect::<Vec<_>>();
    assert_eq!(
        serde_json::to_value(&actual).unwrap(),
        serde_json::to_value(&expected_stream).unwrap()
    );
    assert_eq!(actual.len(), 1002);
    assert_eq!(
        queries.len(),
        144,
        "one bounded statement per seven records, not per parent"
    );
    // UUIDs are unique within their logical shard, not across all shards.
    // Grouped root and child pages must retain both copies and shard order.
    for table in [
        "run_chunks",
        "executions",
        "execution_attempts",
        "execution_aggregates",
        "evaluator_results",
        "evaluator_diagnostics",
    ] {
        sqlx::query(&format!("INSERT INTO {table} SELECT (jsonb_populate_record(NULL::{table},to_jsonb(t)||jsonb_build_object('run_shard',1))).* FROM {table} t WHERE run_id=$1 AND run_shard=0"))
            .bind(run).execute(&pool).await.unwrap();
    }
    let groups = run_export::group_routes(&routes);
    let mut actual = [Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()];
    for group in &groups {
        let mut cursor = None;
        loop {
            let page = run_export::select_group_execution_page(group, run, cursor.as_ref(), 2)
                .await
                .unwrap();
            cursor = page.cursor;
            let batch = run_export::select_batch_for_group(group, run, page.executions)
                .await
                .unwrap();
            for (target, rows) in actual.iter_mut().zip([
                serde_json::to_value(batch.executions).unwrap(),
                serde_json::to_value(batch.attempts).unwrap(),
                serde_json::to_value(batch.aggregates).unwrap(),
                serde_json::to_value(batch.evaluator_results).unwrap(),
                serde_json::to_value(batch.evaluator_diagnostics).unwrap(),
            ]) {
                target.extend(rows.as_array().unwrap().iter().cloned());
            }
            if !page.has_more {
                break;
            }
        }
    }
    let mut expected = [Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()];
    for route in &routes[..2] {
        let root = run_export::select_execution_batch(route, run, None, 100)
            .await
            .unwrap();
        let batch = run_export::select_materialized_oracle(route, run, root)
            .await
            .unwrap();
        for (target, rows) in expected.iter_mut().zip([
            serde_json::to_value(batch.executions).unwrap(),
            serde_json::to_value(batch.attempts).unwrap(),
            serde_json::to_value(batch.aggregates).unwrap(),
            serde_json::to_value(batch.evaluator_results).unwrap(),
            serde_json::to_value(batch.evaluator_diagnostics).unwrap(),
        ]) {
            target.extend(rows.as_array().unwrap().iter().cloned());
        }
    }
    assert_eq!(actual, expected);
    sqlx::query("UPDATE local_shard_admissions SET state='closed' WHERE run_id=$1 AND run_shard=0")
        .bind(run)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        client
            .export_page(run, ExportCollection::Executions, limits, None)
            .await
            .is_err()
    );
    assert!(
        run_export::select_execution_batch(route, run, None, 100)
            .await
            .is_err()
    );
}

async fn restricted(pool: &PgPool, sensitive: bool) -> (UserConfig, String) {
    let role = format!("export_{}", Uuid::now_v7().simple());
    sqlx::raw_sql(&format!(
        "CREATE ROLE {role} LOGIN PASSWORD 'fixture'; GRANT USAGE ON SCHEMA public TO {role}"
    ))
    .execute(pool)
    .await
    .unwrap();
    grant_reader(pool, &role, sensitive).await;
    let options = pool
        .connect_options()
        .as_ref()
        .clone()
        .username(&role)
        .password("fixture");
    let mut control = Config::new(options.to_url_lossy().to_string());
    control.max_connections = 1;
    (
        UserConfig {
            control,
            database_role: role.clone(),
            execution_databases: Default::default(),
        },
        role,
    )
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn restricted_exports_fence_columns_context_and_query_counts(pool: PgPool) {
    let (tenant, run, _) = fixture(&pool).await;
    let (config, role) = restricted(&pool, false).await;
    let ordinary = ExportClient::new(config.clone()).unwrap();
    let context = UserContext::new(tenant, Uuid::now_v7()).unwrap();
    let limits = ExportLimits::new(2, 4096).unwrap();
    let (first, cold_queries) =
        record_queries(ordinary.page(context, run, ExportCollection::Executions, limits, None))
            .await;
    let first = first.unwrap();
    eprintln!(
        "ordinary export: {} cold SQL submissions (including connection guards); transaction pairs recorded separately",
        cold_queries.len()
    );
    let (value, queries) = record_queries(ordinary.page(
        context,
        run,
        ExportCollection::Executions,
        limits,
        first.next.as_ref(),
    ))
    .await;
    assert_eq!(value.unwrap().records.len(), 1);
    assert_eq!(
        queries.len(),
        4,
        "one control context + catalog, one execution context + bounded read: {queries:?}"
    );
    eprintln!(
        "ordinary export: {} warm SQL submissions plus two BEGIN/COMMIT pairs for 128 shards on one alias",
        queries.len()
    );
    // Cancellation while waiting for execution locks releases request identity.
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE executions IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            ordinary.page(context, run, ExportCollection::Executions, limits, None)
        )
        .await
        .is_err()
    );
    blocker.rollback().await.unwrap();
    assert!(
        ordinary
            .page(context, run, ExportCollection::Executions, limits, None)
            .await
            .is_ok()
    );
    for collection in [
        ExportCollection::Attempts,
        ExportCollection::Aggregates,
        ExportCollection::EvaluatorResults,
    ] {
        assert!(
            ordinary
                .page(context, run, collection, limits, None)
                .await
                .unwrap()
                .records
                .is_empty()
        );
    }
    let insufficient = SensitiveExportClient::new(config.clone()).unwrap();
    assert!(
        insufficient
            .content_page(
                context,
                run,
                ExportCollection::ExecutionContent,
                limits,
                None
            )
            .await
            .is_err()
    );
    insufficient.close().await;
    let wrong = UserContext::new(Uuid::now_v7(), Uuid::now_v7()).unwrap();
    assert!(matches!(
        ordinary
            .page(wrong, run, ExportCollection::Executions, limits, None)
            .await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        ordinary
            .page(context, run, ExportCollection::Diagnostics, limits, None)
            .await,
        Err(Error::InvalidInput(_))
    ));
    let direct = PgPool::connect(&config.control.database_url).await.unwrap();
    assert!(
        sqlx::query("SELECT input_payload FROM executions")
            .execute(&direct)
            .await
            .is_err()
    );
    let (sensitive_config, sensitive_role) = restricted(&pool, true).await;
    let sensitive = SensitiveExportClient::new(sensitive_config).unwrap();
    assert!(
        serde_json::to_string(
            &sensitive
                .content_page(
                    context,
                    run,
                    ExportCollection::ExecutionContent,
                    limits,
                    None
                )
                .await
                .unwrap()
        )
        .unwrap()
        .contains("PRIVATE-CANARY")
    );
    let (combined, combined_queries) = record_queries(async {
        ordinary
            .page(context, run, ExportCollection::Executions, limits, None)
            .await?;
        sensitive
            .content_page(
                context,
                run,
                ExportCollection::ExecutionContent,
                limits,
                None,
            )
            .await
    })
    .await;
    assert_eq!(combined.unwrap().records.len(), 2);
    assert_eq!(
        combined_queries.len(),
        8,
        "composed ordinary/content reads use two page operations, not per-record reads"
    );
    eprintln!(
        "ordinary plus content: {} warm submissions plus four BEGIN/COMMIT pairs",
        combined_queries.len()
    );
    for collection in [
        ExportCollection::AttemptContent,
        ExportCollection::AggregateContent,
        ExportCollection::EvaluatorResultContent,
    ] {
        assert!(
            sensitive
                .content_page(context, run, collection, limits, None)
                .await
                .unwrap()
                .records
                .is_empty()
        );
    }
    assert!(
        sensitive
            .diagnostics_page(context, run, limits, None)
            .await
            .unwrap()
            .records
            .is_empty()
    );
    assert!(matches!(
        sensitive
            .content_page(context, run, ExportCollection::Diagnostics, limits, None)
            .await,
        Err(Error::InvalidInput(_))
    ));
    // Server-level predefined roles bypass the table/column boundary. Even
    // membership without inherited grants must not be accepted for export pools.
    for inherit in [true, false] {
        sqlx::raw_sql(&format!(
            "GRANT pg_read_server_files TO {role} WITH INHERIT {inherit}"
        ))
        .execute(&pool)
        .await
        .unwrap();
        let privileged_reader = ExportClient::new(config.clone()).unwrap();
        assert!(
            privileged_reader
                .page(context, run, ExportCollection::Executions, limits, None)
                .await
                .is_err()
        );
        privileged_reader.close().await;
        sqlx::raw_sql(&format!("REVOKE pg_read_server_files FROM {role}"))
            .execute(&pool)
            .await
            .unwrap();
    }
    // SET ROLE can expose grants even when membership does not inherit them.
    let switch_role = format!("export_switch_{}", Uuid::now_v7().simple());
    sqlx::raw_sql(&format!(
        "CREATE ROLE {switch_role}; GRANT SELECT(input_payload) ON executions TO {switch_role}; CREATE POLICY {switch_role} ON executions FOR SELECT TO {switch_role} USING (true); GRANT {switch_role} TO {role} WITH INHERIT false"
    ))
    .execute(&pool)
    .await
    .unwrap();
    let mut switched = direct.begin().await.unwrap();
    sqlx::raw_sql(&format!("SET LOCAL ROLE {switch_role}"))
        .execute(&mut *switched)
        .await
        .unwrap();
    let inputs: Vec<sqlx::types::Json<serde_json::Value>> =
        sqlx::query_scalar("SELECT input_payload FROM executions")
            .fetch_all(&mut *switched)
            .await
            .unwrap();
    assert!(
        serde_json::to_string(&inputs)
            .unwrap()
            .contains("PRIVATE-CANARY")
    );
    switched.rollback().await.unwrap();
    let switch_reader = ExportClient::new(config.clone()).unwrap();
    assert!(
        switch_reader
            .page(context, run, ExportCollection::Executions, limits, None)
            .await
            .is_err(),
        "export credentials must reject sensitive grants reachable through SET ROLE"
    );
    switch_reader.close().await;
    sqlx::raw_sql(&format!(
        "REVOKE {switch_role} FROM {role}; DROP OWNED BY {switch_role}; DROP ROLE {switch_role}"
    ))
    .execute(&pool)
    .await
    .unwrap();
    // New connections reject a broad grant, even on an individual partition.
    sqlx::raw_sql(&format!(
        "GRANT SELECT(input_payload) ON executions_p000 TO {role}"
    ))
    .execute(&pool)
    .await
    .unwrap();
    let unsafe_reader = ExportClient::new(config).unwrap();
    assert!(
        unsafe_reader
            .page(context, run, ExportCollection::Executions, limits, None)
            .await
            .is_err()
    );
    // Revocation takes effect on the next page even on a warm shared connection.
    sqlx::raw_sql(&format!("ALTER POLICY {role} ON runs USING (false)"))
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        ordinary
            .page(
                context,
                run,
                ExportCollection::Executions,
                limits,
                first.next.as_ref()
            )
            .await,
        Err(Error::NotFound(_))
    ));
    ordinary.close().await;
    sensitive.close().await;
    unsafe_reader.close().await;
    direct.close().await;
    for role in [role, sensitive_role] {
        sqlx::raw_sql(&format!("DROP OWNED BY {role}; DROP ROLE {role}"))
            .execute(&pool)
            .await
            .unwrap();
    }
}

async fn grant_reader(pool: &PgPool, role: &str, sensitive: bool) {
    sqlx::raw_sql(&format!("GRANT USAGE ON SCHEMA public TO {role}"))
        .execute(pool)
        .await
        .unwrap();
    let allowed = sql::readable_columns(sensitive);
    for (table, columns) in allowed.as_object().unwrap() {
        if table != "database_placements" {
            let key = if table == "runs" { "id" } else { "run_id" };
            sqlx::raw_sql(&format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY; CREATE POLICY {role} ON {table} FOR SELECT TO {role} USING (tenant_id=NULLIF(current_setting('vigilo.tenant_id',true),'')::uuid AND {key}=NULLIF(current_setting('vigilo.run_id',true),'')::uuid)")).execute(pool).await.unwrap();
        }
        let columns = columns
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c.as_str().unwrap())
            .collect::<Vec<_>>()
            .join(",");
        sqlx::raw_sql(&format!("GRANT SELECT ({columns}) ON {table} TO {role}"))
            .execute(pool)
            .await
            .unwrap();
    }
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn hosted_export_uses_fixed_remote_binding_and_rechecks_placement(pool: PgPool) {
    use futures_util::FutureExt;
    let (tenant, run, _) = fixture(&pool).await;
    let remote_name = format!("export_remote_{}", Uuid::now_v7().simple());
    sqlx::raw_sql(&format!("CREATE DATABASE {remote_name}"))
        .execute(&pool)
        .await
        .unwrap();
    let remote = PgPool::connect_with(
        pool.connect_options()
            .as_ref()
            .clone()
            .database(&remote_name),
    )
    .await
    .unwrap();
    let mut clients = Vec::new();
    let mut role = None;
    let outcome=std::panic::AssertUnwindSafe(async {
        crate::db::migrations::migrate(&remote,None).await.unwrap();
        for table in ["dataset_versions","runs","local_shard_admissions","shard_placements","run_chunks","executions"] {
            let rows:serde_json::Value=sqlx::query_scalar(&format!("SELECT jsonb_agg(t) FROM {table} t")).fetch_one(&pool).await.unwrap();
            sqlx::query(&format!("INSERT INTO {table} SELECT * FROM jsonb_populate_recordset(NULL::{table},$1::jsonb)")).bind(rows).execute(&remote).await.unwrap();
        }
        sqlx::query("UPDATE executions SET input_payload=jsonb_build_object('source','REMOTE')").execute(&remote).await.unwrap();
        // Ordinary records also carry a distinguishing label, without exposing content.
        sqlx::query("UPDATE executions SET task_type='remote'").execute(&remote).await.unwrap();
        sqlx::query("INSERT INTO database_placements(alias,database_url_env,role,status) VALUES('execution','UNUSED_FIXED_EXPORT_BINDING','shard','active')").execute(&pool).await.unwrap();
        sqlx::query("UPDATE shard_placements SET database_alias='execution' WHERE run_id=$1").bind(run).execute(&pool).await.unwrap();
        sqlx::query("UPDATE local_shard_admissions SET database_alias='execution' WHERE run_id=$1").bind(run).execute(&remote).await.unwrap();
        let (mut config,name)=restricted(&pool,false).await;role=Some(name.clone());
        grant_reader(&remote,&name,false).await;
        let missing=ExportClient::new(config.clone()).unwrap();clients.push(missing.clone());
        let context=UserContext::new(tenant,Uuid::now_v7()).unwrap();
        assert!(missing.page(context,run,ExportCollection::Executions,ExportLimits::default(),None).await.is_err());
        let options=remote.connect_options().as_ref().clone().username(&name).password("fixture");
        config.execution_databases.insert("execution".into(),options.to_url_lossy().to_string());
        // Only the control database owns this helper. Its allowance must not
        // leak into validation on the execution database, where it is absent.
        sqlx::raw_sql(&format!("CREATE FUNCTION public.control_policy() RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS 'SELECT true'; REVOKE ALL ON FUNCTION public.control_policy() FROM PUBLIC; GRANT EXECUTE ON FUNCTION public.control_policy() TO {name}")).execute(&pool).await.unwrap();
        let functions=std::collections::BTreeMap::from([("primary".into(),vec!["public.control_policy()".into()])]);
        let client=ExportClient::with_trusted_policy_functions(config,functions).unwrap();clients.push(client.clone());
        let page=client.page(context,run,ExportCollection::Executions,ExportLimits::default(),None).await.unwrap();
        assert_eq!(page.records.len(),3);
        assert!(page.records.iter().all(|r|matches!(r,ExportRecord::ExecutionMetadata(row) if row.task_type=="remote")));
        let (_,queries)=record_queries(client.page(context,run,ExportCollection::Executions,ExportLimits::default(),None)).await;
        assert_eq!(queries.len(),4);
        sqlx::query("UPDATE database_placements SET status='disabled' WHERE alias='execution'").execute(&pool).await.unwrap();
        assert!(client.page(context,run,ExportCollection::Executions,ExportLimits::default(),None).await.is_err());
        sqlx::query("UPDATE database_placements SET status='draining' WHERE alias='execution'").execute(&pool).await.unwrap();
        assert!(client.page(context,run,ExportCollection::Executions,ExportLimits::default(),None).await.is_ok());
    }).catch_unwind().await;
    for client in clients {
        client.close().await;
    }
    remote.close().await;
    sqlx::raw_sql(&format!("DROP DATABASE {remote_name} WITH (FORCE)"))
        .execute(&pool)
        .await
        .unwrap();
    if let Some(role) = role {
        sqlx::raw_sql(&format!("DROP OWNED BY {role}; DROP ROLE {role}"))
            .execute(&pool)
            .await
            .unwrap();
    }
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
