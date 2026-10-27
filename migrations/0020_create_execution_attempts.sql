CREATE TABLE execution_attempts (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    execution_id UUID NOT NULL,
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    run_shard SMALLINT NOT NULL CHECK (run_shard >= 0 AND run_shard < 128),

    attempt_no INTEGER NOT NULL CHECK (attempt_no > 0),
    status attempt_status NOT NULL DEFAULT 'pending',

    worker_id UUID,
    worker_host TEXT,
    queue_message_id UUID,
    broker_message_id TEXT,

    -- lease/heartbeat for distributed recovery
    leased_until TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,

    -- raw artifacts should generally live outside the DB;
    -- keep references here
    request_artifact_uri TEXT,
    response_artifact_uri TEXT,

    -- timing
    agent_latency_ms BIGINT CHECK (agent_latency_ms IS NULL OR agent_latency_ms >= 0),
    evaluator_latency_ms BIGINT CHECK (evaluator_latency_ms IS NULL OR evaluator_latency_ms >= 0),
    total_latency_ms BIGINT CHECK (total_latency_ms IS NULL OR total_latency_ms >= 0),

    -- optional usage/cost summary
    token_usage JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- compact attempt-level summary
    outcome_summary JSONB NOT NULL DEFAULT '{}'::jsonb,

    error_message TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT pk_execution_attempts PRIMARY KEY (run_id, run_shard, id),
    CONSTRAINT fk_execution_attempts_execution
        FOREIGN KEY (run_id, run_shard, execution_id)
        REFERENCES executions(run_id, run_shard, id)
        ON DELETE CASCADE,
    CONSTRAINT uq_execution_attempt_no UNIQUE (run_id, run_shard, execution_id, attempt_no)
) PARTITION BY LIST (run_shard);

DO $$
DECLARE
    partition_index INTEGER;
BEGIN
    FOR partition_index IN 0..127 LOOP
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF execution_attempts FOR VALUES IN (%s)',
            'execution_attempts_p' || lpad(partition_index::text, 3, '0'),
            partition_index
        );
    END LOOP;
END $$;

CREATE INDEX idx_execution_attempts_status ON execution_attempts(status);

CREATE INDEX idx_execution_attempts_run_lease ON execution_attempts(run_id, run_shard, leased_until)
    WHERE status = 'running';

CREATE INDEX idx_execution_attempts_run_status ON execution_attempts(run_id, run_shard, status);

COMMENT ON TABLE execution_attempts IS
    'Represents a single worker attempt to process an execution. Multiple attempts may exist due to retries, failures, or lease expiration. Only the most recent non-stale attempt is considered authoritative. Rows are list partitioned by run_shard.';

COMMENT ON COLUMN execution_attempts.id IS
    'Unique identifier for the attempt.';

COMMENT ON COLUMN execution_attempts.execution_id IS
    'Reference to the execution this attempt is processing.';

COMMENT ON COLUMN execution_attempts.run_id IS
    'Reference to the run this attempt belongs to. Used for grouping and query efficiency.';

COMMENT ON COLUMN execution_attempts.run_shard IS
    'Logical shard inherited from the parent execution and source chunk.';

COMMENT ON COLUMN execution_attempts.attempt_no IS
    'Monotonically increasing attempt number for the execution. Used to order retries and identify the latest attempt.';

COMMENT ON COLUMN execution_attempts.status IS
    'State of this specific attempt. Multiple attempts may exist for a single execution; only the most recent non-stale attempt is authoritative and should be used for final results.';

COMMENT ON COLUMN execution_attempts.worker_id IS
    'Identifier of the worker process that claimed and is executing this attempt.';

COMMENT ON COLUMN execution_attempts.worker_host IS
    'Optional host or node identifier where the worker is running. Useful for debugging distributed execution.';

COMMENT ON COLUMN execution_attempts.queue_message_id IS
    'Internal UUID correlation id for the worker delivery that allocated this attempt.';

COMMENT ON COLUMN execution_attempts.broker_message_id IS
    'Broker-provided message id or dedupe key associated with this attempt, if applicable. Used for tracing and debugging message processing.';

COMMENT ON COLUMN execution_attempts.leased_until IS
    'Lease expiration timestamp for this attempt. Used to detect abandoned or stalled work and allow reassignment.';

COMMENT ON COLUMN execution_attempts.heartbeat_at IS
    'Last heartbeat timestamp from the worker processing this attempt. Used to determine liveness and lease renewal.';

COMMENT ON COLUMN execution_attempts.request_artifact_uri IS
    'Optional URI pointing to stored request payload or artifacts associated with this attempt.';

COMMENT ON COLUMN execution_attempts.response_artifact_uri IS
    'Optional URI pointing to stored response payload or artifacts produced by this attempt.';

COMMENT ON COLUMN execution_attempts.agent_latency_ms IS
    'Time spent invoking the target system (e.g., agent or model) in milliseconds.';

COMMENT ON COLUMN execution_attempts.evaluator_latency_ms IS
    'Time spent running evaluators for this attempt in milliseconds.';

COMMENT ON COLUMN execution_attempts.total_latency_ms IS
    'Total end-to-end processing time for this attempt in milliseconds.';

COMMENT ON COLUMN execution_attempts.token_usage IS
    'Optional structured data capturing token usage or cost metrics for this attempt.';

COMMENT ON COLUMN execution_attempts.outcome_summary IS
    'Compact summary of the attempt outcome, including high-level metrics or flags derived from execution and evaluation.';

COMMENT ON COLUMN execution_attempts.error_message IS
    'Error message associated with this attempt, if it failed or encountered an issue.';

COMMENT ON COLUMN execution_attempts.created_at IS
    'Timestamp when the attempt was created.';

COMMENT ON COLUMN execution_attempts.started_at IS
    'Timestamp when the worker began processing this attempt.';

COMMENT ON COLUMN execution_attempts.completed_at IS
    'Timestamp when the attempt reached a terminal state.';

COMMENT ON COLUMN execution_attempts.updated_at IS
    'Timestamp of the last update to the attempt record.';
