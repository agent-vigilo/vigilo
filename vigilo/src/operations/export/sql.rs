//! Explicit, collection-specific projections read in key order.
//!
//! Size admission and record retrieval share one short database snapshot. Only
//! admitted bodies cross the connection; candidate metadata has its own row bound.
//! No projection selects a whole persistence record or an unbounded child set.
use super::ExportCollection;

pub(super) struct Projection {
    pub table: &'static str,
    pub key: &'static str,
    pub joins: &'static str,
    pub body: &'static str,
}

pub(super) fn projection(collection: ExportCollection) -> Projection {
    match collection {
        ExportCollection::Executions => Projection {
            table: "executions",
            key: "id",
            joins: "",
            body: "json_build_object('type','execution_metadata','data',json_build_object('id',t.id,'run_id',t.run_id,'run_shard',t.run_shard,'chunk_id',t.chunk_id,'case_id',t.case_id,'task_type',t.task_type,'evaluation_profile_id',t.evaluation_profile_id,'evaluation_profile_version',t.evaluation_profile_version,'expected_evaluator_count',t.expected_evaluator_count,'status',t.status,'current_attempt_no',t.current_attempt_no,'current_attempt_id',t.current_attempt_id,'retry_after',t.retry_after,'retry_count',t.retry_count,'last_attempt_completed_at',t.last_attempt_completed_at,'created_at',t.created_at,'started_at',t.started_at,'completed_at',t.completed_at,'updated_at',t.updated_at))::text",
        },
        ExportCollection::Attempts => Projection {
            table: "execution_attempts",
            key: "id",
            joins: " LEFT JOIN executions e ON (e.run_id,e.run_shard,e.id)=(t.run_id,t.run_shard,t.execution_id) AND e.run_shard=ANY($2::smallint[]) LEFT JOIN execution_attempts a ON (a.run_id,a.run_shard,a.id)=(t.run_id,t.run_shard,t.id) AND a.run_shard=ANY($2::smallint[]) LEFT JOIN execution_aggregates g ON (g.run_id,g.run_shard,g.execution_id)=(t.run_id,t.run_shard,t.execution_id) AND g.run_shard=ANY($2::smallint[])",
            body: "json_build_object('type','attempt_metadata','data',json_build_object('id',t.id,'run_id',t.run_id,'run_shard',t.run_shard,'execution_id',t.execution_id,'attempt_no',t.attempt_no,'status',t.status,'agent_latency_ms',t.agent_latency_ms,'evaluator_latency_ms',t.evaluator_latency_ms,'total_latency_ms',t.total_latency_ms,'created_at',t.created_at,'started_at',t.started_at,'completed_at',t.completed_at,'updated_at',t.updated_at,'evidence_state',CASE WHEN e.current_attempt_id IS DISTINCT FROM a.id OR a.status::text='stale' THEN 'stale' WHEN e.status::text IN ('completed','failed','timed_out','cancelled') AND a.status::text IN ('completed','failed_agent_call','failed_evaluation','timed_out') AND g.attempt_id=a.id THEN 'authoritative' ELSE 'partial' END))::text",
        },
        ExportCollection::Aggregates => Projection {
            table: "execution_aggregates",
            key: "execution_id",
            joins: " LEFT JOIN executions e ON (e.run_id,e.run_shard,e.id)=(t.run_id,t.run_shard,t.execution_id) AND e.run_shard=ANY($2::smallint[]) LEFT JOIN execution_attempts a ON (a.run_id,a.run_shard,a.id)=(t.run_id,t.run_shard,t.attempt_id) AND a.run_shard=ANY($2::smallint[]) LEFT JOIN execution_aggregates g ON (g.run_id,g.run_shard,g.execution_id)=(t.run_id,t.run_shard,t.execution_id) AND g.run_shard=ANY($2::smallint[])",
            body: "json_build_object('type','aggregate_metadata','data',json_build_object('run_id',t.run_id,'run_shard',t.run_shard,'execution_id',t.execution_id,'attempt_id',t.attempt_id,'overall_status',t.overall_status,'aggregate_score',t.aggregate_score,'evaluator_result_count',t.evaluator_result_count,'created_at',t.created_at,'updated_at',t.updated_at,'authoritative',COALESCE((CASE WHEN e.current_attempt_id IS DISTINCT FROM a.id OR a.status::text='stale' THEN 'stale' WHEN e.status::text IN ('completed','failed','timed_out','cancelled') AND a.status::text IN ('completed','failed_agent_call','failed_evaluation','timed_out') AND g.attempt_id=a.id THEN 'authoritative' ELSE 'partial' END)='authoritative',false)))::text",
        },
        ExportCollection::EvaluatorResults => Projection {
            table: "evaluator_results",
            key: "id",
            joins: " LEFT JOIN executions e ON (e.run_id,e.run_shard,e.id)=(t.run_id,t.run_shard,t.execution_id) AND e.run_shard=ANY($2::smallint[]) LEFT JOIN execution_attempts a ON (a.run_id,a.run_shard,a.id)=(t.run_id,t.run_shard,t.attempt_id) AND a.run_shard=ANY($2::smallint[]) LEFT JOIN execution_aggregates g ON (g.run_id,g.run_shard,g.execution_id)=(t.run_id,t.run_shard,t.execution_id) AND g.run_shard=ANY($2::smallint[])",
            body: "json_build_object('type','evaluator_result_metadata','data',json_build_object('id',t.id,'run_id',t.run_id,'run_shard',t.run_shard,'execution_id',t.execution_id,'attempt_id',t.attempt_id,'binding_id',t.binding_id,'evaluator_id',t.evaluator_id,'evaluator_version',t.evaluator_version,'evaluator_profile_id',t.evaluator_profile_id,'evaluator_profile_version',t.evaluator_profile_version,'evaluator_interface_version',t.evaluator_interface_version,'evaluator_runtime_version',t.evaluator_runtime_version,'dimension',t.dimension,'outcome',t.outcome,'judgment',t.judgment,'blocking',t.blocking,'measurement_kind',t.measurement_kind,'raw_boolean',t.raw_boolean,'raw_numeric',t.raw_numeric,'normalized_score',t.normalized_score,'normalization_policy_hash',t.normalization_policy_hash,'pass_threshold',t.pass_threshold,'weight',t.weight,'created_at',t.created_at,'evidence_state',CASE WHEN e.current_attempt_id IS DISTINCT FROM a.id OR a.status::text='stale' THEN 'stale' WHEN e.status::text IN ('completed','failed','timed_out','cancelled') AND a.status::text IN ('completed','failed_agent_call','failed_evaluation','timed_out') AND g.attempt_id=a.id THEN 'authoritative' ELSE 'partial' END))::text",
        },
        ExportCollection::ExecutionContent => Projection {
            table: "executions",
            key: "id",
            joins: "",
            body: "json_build_object('type','execution_content','data',json_build_object('id',t.id,'run_id',t.run_id,'run_shard',t.run_shard,'tags',t.tags,'input_payload',t.input_payload,'expected_output',t.expected_output,'case_metadata',t.case_metadata,'last_error_message',t.last_error_message))::text",
        },
        ExportCollection::AttemptContent => Projection {
            table: "execution_attempts",
            key: "id",
            joins: "",
            body: "json_build_object('type','attempt_content','data',json_build_object('id',t.id,'run_id',t.run_id,'run_shard',t.run_shard,'execution_id',t.execution_id,'token_usage',t.token_usage,'outcome_summary',t.outcome_summary,'error_message',t.error_message))::text",
        },
        ExportCollection::AggregateContent => Projection {
            table: "execution_aggregates",
            key: "execution_id",
            joins: "",
            body: "json_build_object('type','aggregate_content','data',json_build_object('run_id',t.run_id,'run_shard',t.run_shard,'execution_id',t.execution_id,'attempt_id',t.attempt_id,'dimension_scores',t.dimension_scores,'blocking_failures',t.blocking_failures,'summary',t.summary))::text",
        },
        ExportCollection::EvaluatorResultContent => Projection {
            table: "evaluator_results",
            key: "id",
            joins: "",
            body: "json_build_object('type','evaluator_result_content','data',json_build_object('id',t.id,'run_id',t.run_id,'run_shard',t.run_shard,'execution_id',t.execution_id,'attempt_id',t.attempt_id,'raw_ordinal',t.raw_ordinal,'raw_unit',t.raw_unit,'error_code',t.error_code,'error_message',t.error_message,'abstention_category',t.abstention_category,'abstention_reason',t.abstention_reason,'raw_evaluator_output',t.raw_evaluator_output))::text",
        },
        ExportCollection::Diagnostics => Projection {
            table: "evaluator_diagnostics",
            key: "id",
            joins: "",
            body: "json_build_object('type','export_diagnostic','data',json_build_object('id',t.id,'run_id',t.run_id,'run_shard',t.run_shard,'evaluator_result_id',t.evaluator_result_id,'diagnostic_index',t.diagnostic_index,'severity',t.severity,'category',t.category,'reason',t.reason,'evidence',t.evidence,'tags',t.tags,'created_at',t.created_at))::text",
        },
    }
}

/// Exact effective SELECT allowlists, including authority joins and routing.
pub(super) fn readable_columns(sensitive: bool) -> serde_json::Value {
    if sensitive {
        serde_json::json!({"executions": ["case_id", "case_metadata", "chunk_id", "completed_at", "created_at", "current_attempt_id", "current_attempt_no", "evaluation_profile_id", "evaluation_profile_version", "expected_evaluator_count", "expected_output", "id", "input_payload", "last_attempt_completed_at", "last_error_message", "retry_after", "retry_count", "run_id", "run_shard", "started_at", "status", "tags", "task_type", "updated_at"], "execution_attempts": ["agent_latency_ms", "attempt_no", "completed_at", "created_at", "error_message", "evaluator_latency_ms", "execution_id", "id", "outcome_summary", "run_id", "run_shard", "started_at", "status", "token_usage", "total_latency_ms", "updated_at"], "execution_aggregates": ["aggregate_score", "attempt_id", "blocking_failures", "created_at", "dimension_scores", "evaluator_result_count", "execution_id", "overall_status", "run_id", "run_shard", "summary", "updated_at"], "evaluator_results": ["abstention_category", "abstention_reason", "attempt_id", "binding_id", "blocking", "created_at", "dimension", "error_code", "error_message", "evaluator_id", "evaluator_interface_version", "evaluator_profile_id", "evaluator_profile_version", "evaluator_runtime_version", "evaluator_version", "execution_id", "id", "judgment", "measurement_kind", "normalization_policy_hash", "normalized_score", "outcome", "pass_threshold", "raw_boolean", "raw_evaluator_output", "raw_numeric", "raw_ordinal", "raw_unit", "run_id", "run_shard", "weight"], "evaluator_diagnostics": ["category", "created_at", "diagnostic_index", "evaluator_result_id", "evidence", "id", "reason", "run_id", "run_shard", "severity", "tags"], "runs": ["id"], "shard_placements": ["database_alias", "run_id", "run_shard"], "local_shard_admissions":["run_id","run_shard","database_alias","state"],"database_placements": ["alias", "role", "status"]})
    } else {
        serde_json::json!({"executions": ["case_id", "chunk_id", "completed_at", "created_at", "current_attempt_id", "current_attempt_no", "evaluation_profile_id", "evaluation_profile_version", "expected_evaluator_count", "id", "last_attempt_completed_at", "retry_after", "retry_count", "run_id", "run_shard", "started_at", "status", "task_type", "updated_at"], "execution_attempts": ["agent_latency_ms", "attempt_no", "completed_at", "created_at", "evaluator_latency_ms", "execution_id", "id", "run_id", "run_shard", "started_at", "status", "total_latency_ms", "updated_at"], "execution_aggregates": ["aggregate_score", "attempt_id", "created_at", "evaluator_result_count", "execution_id", "overall_status", "run_id", "run_shard", "updated_at"], "evaluator_results": ["attempt_id", "binding_id", "blocking", "created_at", "dimension", "evaluator_id", "evaluator_interface_version", "evaluator_profile_id", "evaluator_profile_version", "evaluator_runtime_version", "evaluator_version", "execution_id", "id", "judgment", "measurement_kind", "normalization_policy_hash", "normalized_score", "outcome", "pass_threshold", "raw_boolean", "raw_numeric", "run_id", "run_shard", "weight"], "runs": ["id"], "shard_placements": ["database_alias", "run_id", "run_shard"], "local_shard_admissions":["run_id","run_shard","database_alias","state"],"database_placements": ["alias", "role", "status"]})
    }
}
