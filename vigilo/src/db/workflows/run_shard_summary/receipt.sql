-- Chunk-local receipt statistics. Planned cases supply gate denominators even when execution rows are absent.
WITH snapshot AS (
    SELECT rs.run_id, rs.run_shard, rc.ordinal_end - rc.ordinal_start AS expected_execution_count,
           rs.aggregation_policy_hash, rs.config_snapshot
    FROM run_snapshots rs
    JOIN run_chunks rc ON rc.run_id = rs.run_id AND rc.run_shard = rs.run_shard AND rc.id = $3
    WHERE rs.run_id = $1::uuid AND rs.run_shard = $2
),
execution_counts AS (
    SELECT
        snapshot.run_id,
        snapshot.run_shard,
        COUNT(e.id)::int AS execution_count,
        COUNT(e.id) FILTER (
            WHERE e.status IN (
                'completed'::execution_status,
                'failed'::execution_status,
                'timed_out'::execution_status,
                'cancelled'::execution_status
            )
        )::int AS terminal_execution_count,
        COUNT(ea.execution_id)::int AS aggregate_count,
        COUNT(e.id) FILTER (
            WHERE e.status IN (
                'completed'::execution_status,
                'failed'::execution_status,
                'timed_out'::execution_status,
                'cancelled'::execution_status
            )
              AND ea.overall_status = 'passed'::evaluation_status
        )::int AS passed_execution_count,
        COUNT(e.id) FILTER (
            WHERE e.status IN (
                'completed'::execution_status,
                'failed'::execution_status,
                'timed_out'::execution_status,
                'cancelled'::execution_status
            )
              AND ea.overall_status = 'failed'::evaluation_status
        )::int AS failed_execution_count,
        COUNT(e.id) FILTER (
            WHERE e.status IN (
                'completed'::execution_status,
                'failed'::execution_status,
                'timed_out'::execution_status,
                'cancelled'::execution_status
            )
              AND ea.overall_status = 'error'::evaluation_status
        )::int AS errored_execution_count,
        COUNT(e.id) FILTER (
            WHERE e.status IN (
                'completed'::execution_status,
                'failed'::execution_status,
                'timed_out'::execution_status,
                'cancelled'::execution_status
            )
              AND ea.overall_status = 'skipped'::evaluation_status
        )::int AS skipped_execution_count,
        COUNT(e.id) FILTER (
            WHERE e.status IN (
                'completed'::execution_status,
                'failed'::execution_status,
                'timed_out'::execution_status,
                'cancelled'::execution_status
            )
              AND ea.execution_id IS NULL
        )::int AS missing_aggregate_count,
        COALESCE(SUM(ea.evaluator_result_count), 0)::bigint AS evaluator_result_count,
        COALESCE(SUM(
            CASE
                WHEN ea.blocking_failures IS NULL THEN 0
                ELSE jsonb_array_length(ea.blocking_failures)
            END
        ), 0)::bigint AS blocking_failure_count,
        COUNT(ea.aggregate_score)::bigint AS score_count,
        COALESCE(SUM(ea.aggregate_score::numeric), 0.0)::double precision AS score_sum,
        MIN(ea.aggregate_score) AS min_score,
        MAX(ea.aggregate_score) AS max_score
    FROM snapshot
    LEFT JOIN executions e
      ON e.run_id = snapshot.run_id
     AND e.run_shard = snapshot.run_shard
     AND e.chunk_id = $3
    LEFT JOIN execution_aggregates ea
      ON ea.run_id = e.run_id
     AND ea.run_shard = e.run_shard
     AND ea.execution_id = e.id
     AND ea.attempt_id = e.current_attempt_id
     AND e.status IN ('completed', 'failed', 'timed_out', 'cancelled')
     AND EXISTS (SELECT 1 FROM execution_attempts a
         WHERE a.run_id = e.run_id AND a.run_shard = e.run_shard
           AND a.execution_id = e.id AND a.id = e.current_attempt_id
           AND a.status IN ('completed', 'failed_agent_call', 'failed_evaluation', 'timed_out'))
    GROUP BY snapshot.run_id, snapshot.run_shard
),
chunk_counts AS (
    SELECT
        snapshot.run_id,
        snapshot.run_shard,
        COUNT(rc.id) FILTER (WHERE rc.status = 'failed')::int AS failed_chunk_count,
        COUNT(rc.id) FILTER (WHERE rc.status = 'cancelled')::int AS cancelled_chunk_count,
        COUNT(rc.id) FILTER (WHERE rc.status IN ('pending', 'leased'))::int AS open_chunk_count
    FROM snapshot
    LEFT JOIN run_chunks rc
      ON rc.run_id = snapshot.run_id
     AND rc.run_shard = snapshot.run_shard
     AND rc.id = $3
    GROUP BY snapshot.run_id, snapshot.run_shard
),
scorecard_gates AS (
    SELECT
        gate->>'id' AS id,
        gate->>'dimension' AS dimension,
        gate->>'binding_id' AS binding_id,
        gate->>'case_group' AS case_group,
        COALESCE(gate->'tags_all', '[]'::jsonb) AS tags_all,
        (gate->>'min_mean_score')::double precision AS min_mean_score,
        (gate->>'score_threshold')::double precision AS score_threshold,
        (gate->>'min_pass_rate')::double precision AS min_pass_rate,
        (gate->>'min_coverage')::double precision AS min_coverage,
        (gate->>'max_error_rate')::double precision AS max_error_rate,
        (gate->>'max_abstention_rate')::double precision AS max_abstention_rate
    FROM snapshot
    JOIN execution_counts
      ON execution_counts.run_id = snapshot.run_id
     AND execution_counts.run_shard = snapshot.run_shard
    JOIN chunk_counts
      ON chunk_counts.run_id = snapshot.run_id
     AND chunk_counts.run_shard = snapshot.run_shard
    CROSS JOIN LATERAL jsonb_array_elements(
        COALESCE(snapshot.config_snapshot #> '{profile,scorecard,gates}', '[]'::jsonb)
    ) AS gate
    WHERE chunk_counts.open_chunk_count = 0

),
scorecard_targets AS (
    SELECT
        gate.*,
        planned.case_id AS execution_id,
        CASE
            WHEN gate.binding_id IS NULL
            THEN (ea.dimension_scores->>gate.dimension)::double precision
            ELSE MAX(er.normalized_score) FILTER (WHERE er.binding_id = gate.binding_id)
        END AS score,
        COALESCE(BOOL_OR(er.outcome = 'error'::evaluator_outcome), false) AS errored,
        COALESCE(BOOL_OR(er.outcome = 'abstained'::evaluator_outcome), false) AS abstained
    FROM scorecard_gates gate
    JOIN jsonb_to_recordset($4::jsonb) AS planned(
        case_id uuid, profile_group_id text, tags jsonb, evaluator_manifest jsonb
    ) ON (gate.case_group IS NULL OR gate.case_group = ANY(string_to_array(planned.profile_group_id, ',')))
      AND NOT EXISTS (
          SELECT 1 FROM jsonb_array_elements_text(gate.tags_all) AS required_tag(tag)
          WHERE NOT planned.tags ? required_tag.tag
      )
    LEFT JOIN executions e ON e.run_id = $1 AND e.run_shard = $2
        AND e.chunk_id = $3 AND e.case_id = planned.case_id
    JOIN LATERAL (
        SELECT ARRAY_AGG(manifest->>'id') AS binding_ids
        FROM jsonb_array_elements(planned.evaluator_manifest) manifest
        WHERE COALESCE((manifest->>'required')::boolean, true)
          AND manifest->>'dimension' = gate.dimension
          AND (gate.binding_id IS NULL OR manifest->>'id' = gate.binding_id)
    ) expected ON CARDINALITY(expected.binding_ids) > 0
    LEFT JOIN execution_aggregates ea
      ON ea.run_id = e.run_id
     AND ea.run_shard = e.run_shard
     AND ea.execution_id = e.id
     AND ea.attempt_id = e.current_attempt_id
     AND e.status IN ('completed', 'failed', 'timed_out', 'cancelled')
     AND EXISTS (SELECT 1 FROM execution_attempts a
         WHERE a.run_id = e.run_id AND a.run_shard = e.run_shard
           AND a.execution_id = e.id AND a.id = e.current_attempt_id
           AND a.status IN ('completed', 'failed_agent_call', 'failed_evaluation', 'timed_out'))
    LEFT JOIN evaluator_results er
      ON er.run_id = e.run_id
     AND er.run_shard = e.run_shard
     AND er.execution_id = e.id
     AND er.attempt_id = e.current_attempt_id
     AND er.binding_id = ANY(expected.binding_ids)
     AND ea.execution_id IS NOT NULL
    GROUP BY
        gate.id, gate.dimension, gate.binding_id, gate.case_group, gate.tags_all,
        gate.min_mean_score, gate.score_threshold, gate.min_pass_rate,
        gate.min_coverage, gate.max_error_rate, gate.max_abstention_rate,
        planned.case_id, ea.dimension_scores
),
scorecard_entries AS (
    SELECT
        gate.*,
        COUNT(target.execution_id)::bigint AS expected_count,
        COUNT(target.score)::bigint AS scored_count,
        COUNT(target.execution_id) FILTER (
            WHERE target.score IS NOT NULL
              AND target.score >= gate.score_threshold
        )::bigint AS passed_count,
        COUNT(target.execution_id) FILTER (WHERE target.errored)::bigint AS error_count,
        COUNT(target.execution_id) FILTER (WHERE target.abstained)::bigint AS abstained_count,
        COALESCE(SUM(target.score::numeric), 0.0)::double precision AS score_sum,
        MIN(target.score) AS min_score,
        MAX(target.score) AS max_score
    FROM scorecard_gates gate
    LEFT JOIN scorecard_targets target ON target.id = gate.id
    GROUP BY
        gate.id, gate.dimension, gate.binding_id, gate.case_group, gate.tags_all,
        gate.min_mean_score, gate.score_threshold, gate.min_pass_rate,
        gate.min_coverage, gate.max_error_rate, gate.max_abstention_rate
),
scorecard AS (
    SELECT jsonb_build_object(
        'version', 1,
        'run_shard', snapshot.run_shard,
        'policy_hash', snapshot.aggregation_policy_hash,
        'entries', COALESCE((
            SELECT jsonb_agg(to_jsonb(entry) ORDER BY entry.id)
            FROM scorecard_entries entry
        ), '[]'::jsonb)
    ) AS payload
    FROM snapshot
),
computed AS (
    SELECT
        snapshot.run_id,
        snapshot.run_shard,
        snapshot.expected_execution_count,
        snapshot.expected_execution_count - execution_counts.execution_count AS missing_execution_count,
        execution_counts.execution_count - execution_counts.aggregate_count AS unresolved_execution_count,
        execution_counts.execution_count,
        execution_counts.terminal_execution_count,
        execution_counts.aggregate_count,
        execution_counts.passed_execution_count,
        execution_counts.failed_execution_count,
        execution_counts.errored_execution_count,
        execution_counts.skipped_execution_count,
        execution_counts.missing_aggregate_count,
        execution_counts.evaluator_result_count,
        execution_counts.blocking_failure_count,
        execution_counts.score_count,
        execution_counts.score_sum,
        execution_counts.min_score,
        execution_counts.max_score,
        scorecard.payload AS scorecard,
        chunk_counts.failed_chunk_count,
        chunk_counts.cancelled_chunk_count,
        CASE
            WHEN chunk_counts.failed_chunk_count > 0
              OR chunk_counts.cancelled_chunk_count > 0
              OR execution_counts.failed_execution_count > 0
              OR execution_counts.errored_execution_count > 0
              OR execution_counts.missing_aggregate_count > 0
            THEN 'failed'
            WHEN chunk_counts.open_chunk_count = 0

            THEN 'completed'
            ELSE 'running'
        END AS status
    FROM snapshot
    JOIN execution_counts
      ON execution_counts.run_id = snapshot.run_id
     AND execution_counts.run_shard = snapshot.run_shard
    JOIN chunk_counts
      ON chunk_counts.run_id = snapshot.run_id
     AND chunk_counts.run_shard = snapshot.run_shard
    CROSS JOIN scorecard
)
SELECT to_jsonb(computed) FROM computed
