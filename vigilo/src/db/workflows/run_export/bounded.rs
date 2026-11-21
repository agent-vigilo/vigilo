//! Private operator export projections and buffered child pagination.
//!
//! Each collection is paged across the execution batch, rather than once per
//! execution. Bounded buffers let JSONL retain its execution/attempt grouping
//! without materializing every child. One statement snapshot admits ordered
//! identities and encoded sizes, then projects only fitting bodies; excluded
//! bodies never enter a retained CTE or cross the connection. Shard identity stays in every key because UUIDs are
//! not unique across logical shards. Explicit joined shard predicates bound
//! partition planning. Operator data never becomes a hosted DTO.
use std::collections::VecDeque;

use serde::de::DeserializeOwned;
use serde_json::Value;
use uuid::Uuid;

use super::RunExportGroup;
#[cfg(test)]
use super::RunExportRoute;

#[derive(Clone, Copy)]
pub(crate) enum Kind {
    Execution,
    Attempt,
    Aggregate,
    Result,
    Diagnostic,
}
struct Projection {
    table: &'static str,
    joins: &'static str,
    filter: &'static str,
    body: &'static str,
    key: &'static str,
    order: &'static str,
    after: &'static str,
    cursor_guard: &'static str,
}
fn projection(kind: Kind) -> Projection {
    match kind {
        Kind::Execution => Projection {
            table: "executions",
            joins: "",
            filter: "AND $3::jsonb IS NOT NULL",
            body: "json_build_object('id',t.id,'run_id',t.run_id,'run_shard',t.run_shard,'chunk_id',t.chunk_id,'case_id',t.case_id,'task_type',t.task_type,'tags',t.tags,'input_payload',t.input_payload,'expected_output',t.expected_output,'case_metadata',t.case_metadata,'evaluation_profile_id',t.evaluation_profile_id,'evaluation_profile_version',t.evaluation_profile_version,'evaluator_manifest',t.evaluator_manifest,'expected_evaluator_count',t.expected_evaluator_count,'status',t.status,'current_attempt_no',t.current_attempt_no,'current_attempt_id',t.current_attempt_id,'last_error_message',t.last_error_message,'retry_after',t.retry_after,'retry_count',t.retry_count,'last_attempt_completed_at',t.last_attempt_completed_at,'created_at',t.created_at,'started_at',t.started_at,'completed_at',t.completed_at,'updated_at',t.updated_at)::text",
            key: "jsonb_build_array(t.run_shard,t.id)",
            order: "t.run_shard,t.id",
            after: "ROW(t.run_shard,t.id) > ROW(($4::jsonb->>0)::smallint,($4::jsonb->>1)::uuid)",
            cursor_guard: "true",
        },
        Kind::Attempt => Projection {
            table: "execution_attempts",
            joins: "",
            filter: "AND (t.run_shard,t.execution_id) IN (SELECT (v->>0)::smallint,(v->>1)::uuid FROM jsonb_array_elements($3::jsonb) v)",
            body: "json_build_object('id',t.id,'execution_id',t.execution_id,'run_id',t.run_id,'run_shard',t.run_shard,'attempt_no',t.attempt_no,'status',t.status,'worker_id',t.worker_id,'worker_host',t.worker_host,'queue_message_id',t.queue_message_id,'broker_message_id',t.broker_message_id,'leased_until',t.leased_until::text,'heartbeat_at',t.heartbeat_at::text,'request_artifact_uri',t.request_artifact_uri,'response_artifact_uri',t.response_artifact_uri,'agent_latency_ms',t.agent_latency_ms,'evaluator_latency_ms',t.evaluator_latency_ms,'total_latency_ms',t.total_latency_ms,'token_usage',t.token_usage,'outcome_summary',t.outcome_summary,'error_message',t.error_message,'created_at',t.created_at,'started_at',t.started_at,'completed_at',t.completed_at,'updated_at',t.updated_at)::text",
            key: "jsonb_build_array(t.run_shard,t.execution_id,t.attempt_no,t.id)",
            order: "t.run_shard,t.execution_id,t.attempt_no,t.id",
            after: "ROW(t.run_shard,t.execution_id,t.attempt_no,t.id) > ROW(($4::jsonb->>0)::smallint,($4::jsonb->>1)::uuid,($4::jsonb->>2)::integer,($4::jsonb->>3)::uuid)",
            cursor_guard: "true",
        },
        Kind::Aggregate => Projection {
            table: "execution_aggregates",
            joins: "LEFT JOIN execution_attempts a ON (a.run_id,a.run_shard,a.id)=(t.run_id,t.run_shard,t.attempt_id) AND a.run_shard=ANY($2::smallint[])",
            filter: "AND (t.run_shard,t.execution_id) IN (SELECT (v->>0)::smallint,(v->>1)::uuid FROM jsonb_array_elements($3::jsonb) v)",
            body: "json_build_object('aggregate',json_build_object('execution_id',t.execution_id,'run_id',t.run_id,'run_shard',t.run_shard,'attempt_id',t.attempt_id,'overall_status',t.overall_status,'aggregate_score',t.aggregate_score,'evaluator_result_count',t.evaluator_result_count,'dimension_scores',t.dimension_scores,'blocking_failures',t.blocking_failures,'summary',t.summary,'created_at',t.created_at,'updated_at',t.updated_at),'attempt_terminal',COALESCE(a.status::text IN ('completed','failed_agent_call','failed_evaluation','timed_out'),false))::text",
            key: "jsonb_build_array(t.run_shard,t.execution_id)",
            order: "t.run_shard,t.execution_id",
            after: "ROW(t.run_shard,t.execution_id) > ROW(($4::jsonb->>0)::smallint,($4::jsonb->>1)::uuid)",
            cursor_guard: "true",
        },
        Kind::Result => Projection {
            table: "evaluator_results",
            joins: "JOIN execution_attempts a ON (a.run_id,a.run_shard,a.id)=(t.run_id,t.run_shard,t.attempt_id) AND a.run_shard=ANY($2::smallint[])",
            filter: "AND (t.run_shard,t.execution_id) IN (SELECT (v->>0)::smallint,(v->>1)::uuid FROM jsonb_array_elements($3::jsonb) v)",
            body: "json_build_object('id',t.id,'run_id',t.run_id,'run_shard',t.run_shard,'execution_id',t.execution_id,'attempt_id',t.attempt_id,'binding_id',t.binding_id,'evaluator_id',t.evaluator_id,'evaluator_version',t.evaluator_version,'evaluator_profile_id',t.evaluator_profile_id,'evaluator_profile_version',t.evaluator_profile_version,'evaluator_interface_version',t.evaluator_interface_version,'evaluator_runtime_version',t.evaluator_runtime_version,'dimension',t.dimension,'outcome',t.outcome,'judgment',t.judgment,'blocking',t.blocking,'measurement_kind',t.measurement_kind,'raw_boolean',t.raw_boolean,'raw_numeric',t.raw_numeric,'raw_ordinal',t.raw_ordinal,'raw_unit',t.raw_unit,'normalized_score',t.normalized_score,'normalization_policy_hash',t.normalization_policy_hash,'pass_threshold',t.pass_threshold,'weight',t.weight,'error_code',t.error_code,'error_message',t.error_message,'abstention_category',t.abstention_category,'abstention_reason',t.abstention_reason,'raw_evaluator_output',t.raw_evaluator_output,'created_at',t.created_at)::text",
            key: "jsonb_build_array(t.run_shard,t.id)",
            order: "t.run_shard,t.execution_id,a.attempt_no,t.binding_id,t.created_at,t.id",
            after: "ROW(t.run_shard,t.execution_id,a.attempt_no,t.binding_id,t.created_at,t.id) > (SELECT z.run_shard,z.execution_id,za.attempt_no,z.binding_id,z.created_at,z.id FROM evaluator_results z JOIN execution_attempts za ON (za.run_id,za.run_shard,za.id)=(z.run_id,z.run_shard,z.attempt_id) AND za.run_shard=($4::jsonb->>0)::smallint WHERE z.run_id=$1 AND z.run_shard=($4::jsonb->>0)::smallint AND z.id=($4::jsonb->>1)::uuid)",
            cursor_guard: "EXISTS(SELECT 1 FROM evaluator_results z WHERE z.run_id=$1 AND z.run_shard=($4::jsonb->>0)::smallint AND z.id=($4::jsonb->>1)::uuid)",
        },
        Kind::Diagnostic => Projection {
            table: "evaluator_diagnostics",
            joins: "JOIN evaluator_results r ON (r.run_id,r.run_shard,r.id)=(t.run_id,t.run_shard,t.evaluator_result_id) AND r.run_shard=ANY($2::smallint[]) JOIN execution_attempts a ON (a.run_id,a.run_shard,a.id)=(r.run_id,r.run_shard,r.attempt_id) AND a.run_shard=ANY($2::smallint[])",
            filter: "AND (r.run_shard,r.execution_id) IN (SELECT (v->>0)::smallint,(v->>1)::uuid FROM jsonb_array_elements($3::jsonb) v)",
            body: "json_build_object('id',t.id,'run_id',t.run_id,'run_shard',t.run_shard,'evaluator_result_id',t.evaluator_result_id,'diagnostic_index',t.diagnostic_index,'severity',t.severity,'category',t.category,'reason',t.reason,'evidence',t.evidence,'tags',t.tags,'created_at',t.created_at)::text",
            key: "jsonb_build_array(t.run_shard,r.id,t.diagnostic_index,t.id)",
            order: "t.run_shard,r.execution_id,a.attempt_no,r.binding_id,r.created_at,r.id,t.diagnostic_index",
            after: "ROW(t.run_shard,r.execution_id,a.attempt_no,r.binding_id,r.created_at,r.id,t.diagnostic_index) > (SELECT z.run_shard,z.execution_id,za.attempt_no,z.binding_id,z.created_at,z.id,($4::jsonb->>2)::integer FROM evaluator_results z JOIN execution_attempts za ON (za.run_id,za.run_shard,za.id)=(z.run_id,z.run_shard,z.attempt_id) AND za.run_shard=($4::jsonb->>0)::smallint WHERE z.run_id=$1 AND z.run_shard=($4::jsonb->>0)::smallint AND z.id=($4::jsonb->>1)::uuid)",
            cursor_guard: "EXISTS(SELECT 1 FROM evaluator_results z WHERE z.run_id=$1 AND z.run_shard=($4::jsonb->>0)::smallint AND z.id=($4::jsonb->>1)::uuid)",
        },
    }
}

/// One aggregate and the attempt status needed to preserve its evidence label.
#[derive(serde::Deserialize)]
pub(crate) struct AggregateRow {
    pub aggregate: crate::models::execution_aggregate::ExecutionAggregate,
    pub attempt_terminal: bool,
}

/// One bounded operator collection buffer, owned only for the current CLI batch.
pub(crate) struct ChildPager<T> {
    route: RunExportGroup,
    run_id: Uuid,
    ids: Vec<Value>,
    kind: Kind,
    limit: i64,
    cursor: Option<Value>,
    buffer: VecDeque<T>,
    exhausted: bool,
}
impl<T: DeserializeOwned> ChildPager<T> {
    #[cfg(test)]
    pub(crate) fn new(
        route: &RunExportRoute,
        run_id: Uuid,
        ids: &[Uuid],
        kind: Kind,
        limit: i64,
    ) -> Self {
        let keys = ids
            .iter()
            .map(|id| serde_json::json!([route.run_shard, id]))
            .collect::<Vec<_>>();
        Self::grouped(&RunExportGroup::single(route), run_id, &keys, kind, limit)
    }
    fn grouped(
        route: &RunExportGroup,
        run_id: Uuid,
        ids: &[Value],
        kind: Kind,
        limit: i64,
    ) -> Self {
        Self {
            route: route.clone(),
            run_id,
            ids: ids.to_vec(),
            kind,
            limit,
            cursor: None,
            buffer: VecDeque::new(),
            exhausted: false,
        }
    }
    pub(crate) async fn peek(&mut self) -> anyhow::Result<Option<&T>> {
        if self.buffer.is_empty() && !self.exhausted {
            let (records, cursor, complete) = page(
                &self.route,
                self.run_id,
                &self.ids,
                self.kind,
                self.cursor.as_ref(),
                self.limit,
            )
            .await?;
            self.buffer = records.into();
            self.cursor = cursor;
            self.exhausted = complete;
        }
        Ok(self.buffer.front())
    }
    /// Only the explicit JSON document path materializes a complete family.
    pub(crate) async fn collect_all(mut self) -> anyhow::Result<Vec<T>> {
        let mut rows = Vec::new();
        while self.peek().await?.is_some() {
            rows.push(self.pop());
        }
        Ok(rows)
    }
    pub(crate) fn pop(&mut self) -> T {
        self.buffer.pop_front().expect("peeked export row")
    }
}

pub(super) async fn page<T: DeserializeOwned>(
    route: &RunExportGroup,
    run_id: Uuid,
    ids: &[Value],
    kind: Kind,
    cursor: Option<&Value>,
    limit: i64,
) -> anyhow::Result<(Vec<T>, Option<Value>, bool)> {
    anyhow::ensure!(
        (1..=10000).contains(&limit),
        "export row limit must be between 1 and 10000"
    );
    let p = projection(kind);
    let from = format!(
        "FROM {table} t {joins} WHERE t.run_id=$1 AND t.run_shard=ANY($2::smallint[]) {filter} AND ($4::jsonb IS NULL OR {after})",
        table = p.table,
        joins = p.joins,
        filter = p.filter,
        after = p.after
    );
    let order = format!("ORDER BY {} LIMIT $5", p.order);
    let admission = format!(
        "SELECT count(a.run_shard)=cardinality($2::smallint[]) AND COALESCE(bool_and(a.database_alias=$6 AND a.state IN ('open','draining')),false) AND ($4::jsonb IS NULL OR {guard}) AS valid FROM unnest($2::smallint[]) requested(shard) LEFT JOIN local_shard_admissions a ON a.run_id=$1 AND a.run_shard=requested.shard",
        guard = p.cursor_guard
    );
    let candidates = format!(
        "SELECT 0 AS kind,admission.valid,c.key,c.bytes,c.ordinal FROM admission LEFT JOIN LATERAL (SELECT CASE WHEN octet_length(({key})::text)<=1024 THEN {key} END AS key,octet_length({body})::bigint AS bytes,row_number() OVER(ORDER BY {ordering}) AS ordinal {from} AND admission.valid {order}) c ON true",
        key = p.key,
        body = p.body,
        ordering = p.order
    );
    let data = data_branch(kind, 0);
    let statement =
        crate::db::bounded_read::statement(&admission, &candidates, &data, route.shards.len() == 1);
    let records = sqlx::query_as::<_, crate::db::bounded_read::Row>(&statement)
        .bind(run_id)
        .bind(&route.shards)
        .bind(sqlx::types::Json(ids))
        .bind(cursor.map(sqlx::types::Json))
        .bind(limit + 1)
        .bind(&route.database_alias)
        .bind(16i64 * 1024 * 1024 - 1)
        .fetch_all(&route.db)
        .await?;
    let mut rows = Vec::new();
    let mut bytes = 2usize;
    let mut complete = true;
    let mut last = cursor.cloned();
    for (_, valid, key, size, body) in records {
        anyhow::ensure!(valid, "execution read route is no longer locally admitted");
        let Some(size) = size else {
            continue;
        };
        let key = key.ok_or_else(|| anyhow::anyhow!("export ordering key exceeds 1024 bytes"))?;
        let cost = size as usize + usize::from(!rows.is_empty());
        if rows.len() == limit as usize || bytes + cost > 16 * 1024 * 1024 {
            anyhow::ensure!(!rows.is_empty(), "export record exceeds 16 MiB byte budget");
            complete = false;
            break;
        }
        let body =
            body.ok_or_else(|| anyhow::anyhow!("export snapshot omitted an admitted key"))?;
        bytes += cost;
        last = Some(key.0);
        rows.push(crate::contracts::run_export_json::decode(
            &body,
            if matches!(kind, Kind::Aggregate) {
                2
            } else {
                1
            },
        )?);
    }
    Ok((rows, last, complete))
}

/// Initial child pages share admission and one bounded statement snapshot.
/// Additional fan-out resumes only its own collection through `ChildPager`.
pub(crate) struct ChildPages {
    pub attempts: ChildPager<super::ExecutionAttempt>,
    pub aggregates: ChildPager<AggregateRow>,
    pub results: ChildPager<super::EvaluatorResult>,
    pub diagnostics: ChildPager<super::EvaluatorDiagnostic>,
}

pub(crate) async fn load_children(
    group: &RunExportGroup,
    run_id: Uuid,
    parents: &[(i16, Uuid)],
    limit: i64,
) -> anyhow::Result<ChildPages> {
    let mut scoped = group.clone();
    scoped.shards = parents
        .iter()
        .map(|p| p.0)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let route = &scoped;
    let ids = parents
        .iter()
        .map(|(shard, id)| serde_json::json!([shard, id]))
        .collect::<Vec<_>>();
    let ids = ids.as_slice();
    anyhow::ensure!(
        (1..=10000).contains(&limit),
        "export row limit must be between 1 and 10000"
    );
    let kinds = [
        Kind::Attempt,
        Kind::Aggregate,
        Kind::Result,
        Kind::Diagnostic,
    ];
    let mut branches = Vec::new();
    for (i, kind) in kinds.iter().enumerate() {
        let p = projection(*kind);
        branches.push(format!("SELECT {i} AS kind,admission.valid,c.key,c.bytes,c.ordinal FROM admission LEFT JOIN LATERAL (SELECT {key} AS key,octet_length({body})::bigint AS bytes,row_number() OVER(ORDER BY {order}) AS ordinal FROM {table} t {joins} WHERE t.run_id=$1 AND t.run_shard=ANY($2::smallint[]) {filter} AND ($4::jsonb IS NULL OR {after}) AND admission.valid ORDER BY {order} LIMIT $5) c ON true",key=p.key,body=p.body,order=p.order,table=p.table,joins=p.joins,filter=p.filter,after=p.after));
    }
    let admission = "SELECT count(a.run_shard)=cardinality($2::smallint[]) AND COALESCE(bool_and(a.database_alias=$6 AND a.state IN ('open','draining')),false) AS valid FROM unnest($2::smallint[]) requested(shard) LEFT JOIN local_shard_admissions a ON a.run_id=$1 AND a.run_shard=requested.shard";
    let candidates = format!(
        "SELECT kind,valid,CASE WHEN octet_length(key::text)<=1024 THEN key END AS key,bytes,ordinal FROM ({}) c",
        branches.join(" UNION ALL ")
    );
    let data = kinds
        .iter()
        .enumerate()
        .map(|(i, kind)| data_branch(*kind, i))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let statement =
        crate::db::bounded_read::statement(admission, &candidates, &data, route.shards.len() == 1);
    let records = sqlx::query_as::<_, crate::db::bounded_read::Row>(&statement)
        .bind(run_id)
        .bind(&route.shards)
        .bind(sqlx::types::Json(ids))
        .bind(None::<sqlx::types::Json<Value>>)
        .bind(limit + 1)
        .bind(&route.database_alias)
        .bind(16i64 * 1024 * 1024 - 1)
        .fetch_all(&route.db)
        .await?;
    let mut keys: [Vec<Value>; 4] = std::array::from_fn(|_| Vec::new());
    let mut data: [VecDeque<String>; 4] = std::array::from_fn(|_| VecDeque::new());
    let mut bytes = [2usize; 4];
    let mut complete = [true; 4];
    for (kind, valid, key, size, body) in records {
        anyhow::ensure!(valid, "execution read route is no longer locally admitted");
        let i = kind as usize;
        if !complete[i] {
            continue;
        }
        let Some(size) = size else {
            continue;
        };
        let key = key
            .ok_or_else(|| anyhow::anyhow!("export ordering key exceeds 1024 bytes"))?
            .0;
        let cost = size as usize + usize::from(!keys[i].is_empty());
        if keys[i].len() == limit as usize || bytes[i] + cost > 16 * 1024 * 1024 {
            anyhow::ensure!(
                !keys[i].is_empty(),
                "export record exceeds 16 MiB byte budget"
            );
            complete[i] = false;
            continue;
        }
        let body =
            body.ok_or_else(|| anyhow::anyhow!("export snapshot omitted an admitted key"))?;
        bytes[i] += cost;
        keys[i].push(key);
        data[i].push_back(body);
    }
    fn initial<T: DeserializeOwned>(
        route: &RunExportGroup,
        run_id: Uuid,
        ids: &[Value],
        kind: Kind,
        limit: i64,
        state: (&[Value], bool, &mut VecDeque<String>),
    ) -> anyhow::Result<ChildPager<T>> {
        let (keys, complete, rows) = state;
        let mut pager = ChildPager::grouped(route, run_id, ids, kind, limit);
        pager.cursor = keys.last().cloned();
        pager.exhausted = complete;
        for row in rows.drain(..) {
            pager
                .buffer
                .push_back(crate::contracts::run_export_json::decode(
                    &row,
                    if matches!(kind, Kind::Aggregate) {
                        2
                    } else {
                        1
                    },
                )?);
        }
        Ok(pager)
    }
    Ok(ChildPages {
        attempts: initial(
            route,
            run_id,
            ids,
            kinds[0],
            limit,
            (&keys[0], complete[0], &mut data[0]),
        )?,
        aggregates: initial(
            route,
            run_id,
            ids,
            kinds[1],
            limit,
            (&keys[1], complete[1], &mut data[1]),
        )?,
        results: initial(
            route,
            run_id,
            ids,
            kinds[2],
            limit,
            (&keys[2], complete[2], &mut data[2]),
        )?,
        diagnostics: initial(
            route,
            run_id,
            ids,
            kinds[3],
            limit,
            (&keys[3], complete[3], &mut data[3]),
        )?,
    })
}

/// Retrieve bodies through admitted native identities, without ordering joins
/// unless their fields are part of the exported record itself.
fn data_branch(kind: Kind, family: usize) -> String {
    let p = projection(kind);
    let column = if matches!(kind, Kind::Aggregate) {
        "execution_id"
    } else {
        "id"
    };
    let index = if matches!(kind, Kind::Attempt | Kind::Diagnostic) {
        3
    } else {
        1
    };
    let joins = if matches!(kind, Kind::Aggregate) {
        p.joins
    } else {
        ""
    };
    format!(
        "SELECT {family} AS kind,b.key,{body} AS body FROM selected b JOIN {table} t ON t.run_id=$1 AND t.run_shard=ANY($2::smallint[]) AND t.run_shard=(b.key->>0)::smallint AND t.{column}=(b.key->>{index})::uuid {joins} WHERE b.kind={family}",
        body = p.body,
        table = p.table
    )
}
