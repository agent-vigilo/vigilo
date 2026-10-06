CREATE TABLE executions (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    run_shard SMALLINT NOT NULL CHECK (run_shard >= 0 AND run_shard < 128),
    chunk_id UUID NOT NULL,

    -- stable identity of the dataset case within a run
    case_id UUID NOT NULL,
    case_hash TEXT NOT NULL,
    profile_group_id TEXT NOT NULL,

    task_type TEXT NOT NULL,
    tags JSONB NOT NULL DEFAULT '[]'::jsonb,

    -- frozen case payload for reproducibility
    input_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    expected_output JSONB NOT NULL DEFAULT '{}'::jsonb,
    case_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- resolved evaluator manifest for this execution
    evaluation_profile_id TEXT NOT NULL,
    evaluation_profile_version TEXT NOT NULL,
    evaluator_manifest JSONB NOT NULL DEFAULT '[]'::jsonb,
    expected_evaluator_count INTEGER NOT NULL DEFAULT 0 CHECK (expected_evaluator_count >= 0),

    -- orchestration state
    status execution_status NOT NULL DEFAULT 'pending',

    current_attempt_no INTEGER NOT NULL DEFAULT 0 CHECK (current_attempt_no >= 0),
    current_attempt_id UUID,

    last_error_message TEXT,
    retry_after TIMESTAMPTZ,
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    last_attempt_completed_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT pk_executions PRIMARY KEY (run_id, run_shard, id),
    CONSTRAINT fk_executions_chunk
        FOREIGN KEY (run_id, run_shard, chunk_id)
        REFERENCES run_chunks(run_id, run_shard, id)
        ON DELETE CASCADE,
    CONSTRAINT uq_execution_run_shard_case UNIQUE (run_id, run_shard, case_id)
) PARTITION BY LIST (run_shard);

DO $$
DECLARE
    partition_index INTEGER;
BEGIN
    FOR partition_index IN 0..127 LOOP
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF executions FOR VALUES IN (%s)',
            'executions_p' || lpad(partition_index::text, 3, '0'),
            partition_index
        );
    END LOOP;
END $$;

CREATE INDEX idx_executions_run_status ON executions(run_id, run_shard, status);

CREATE INDEX idx_executions_run_chunk ON executions(run_id, run_shard, chunk_id);

CREATE INDEX idx_executions_run_current_attempt_id ON executions(run_id, run_shard, current_attempt_id);

CREATE INDEX idx_executions_run_case_hash ON executions(run_id, run_shard, case_hash);

CREATE INDEX idx_executions_run_retry
    ON executions(run_id, run_shard, retry_after)
    WHERE status = 'retry_scheduled';

COMMENT ON TABLE executions IS
    'Represents a single evaluation of a dataset case against the target system. Each execution is part of a run, may have multiple attempts due to retries or failures, and is list partitioned by run_shard for chunk-local scale-out.';

COMMENT ON INDEX idx_executions_run_chunk IS
    'Locates all executions owned by one chunk for receipt totals and retirement of unfinished attempt authority.';

COMMENT ON COLUMN executions.id IS
    'Unique identifier for the execution.';

COMMENT ON COLUMN executions.run_id IS
    'Reference to the run this execution belongs to. Determines shared configuration and aggregation context.';

COMMENT ON COLUMN executions.run_shard IS
    'Logical shard inherited from the source run chunk. Workers include this key in hot queries so one chunk stays partition-local.';

COMMENT ON COLUMN executions.chunk_id IS
    'Source run chunk that owns this execution. Used with run_id and run_shard to preserve chunk-local placement.';

COMMENT ON COLUMN executions.case_id IS
    'Identifier of the dataset case within the run. Unique per run and used to correlate input, expected output, and results.';

COMMENT ON COLUMN executions.case_hash IS
    'Content hash of the immutable case payload used for this execution, enabling reproducibility and regression comparability.';

COMMENT ON COLUMN executions.profile_group_id IS
    'Resolved profile case-group id applied to this execution. Multiple automatic matches are stored as a deterministic comma-separated id list.';

COMMENT ON COLUMN executions.task_type IS
    'Logical task category for the execution (e.g., classification, generation, tool-use). Used for routing or conditional evaluation.';

COMMENT ON COLUMN executions.tags IS
    'Optional tags associated with the dataset case for filtering, grouping, or analysis.';

COMMENT ON COLUMN executions.input_payload IS
    'Serialized input provided to the target system for this execution. Represents the dataset case input.';

COMMENT ON COLUMN executions.expected_output IS
    'Optional expected output or reference answer for the dataset case. Used by certain evaluators for correctness checks.';

COMMENT ON COLUMN executions.case_metadata IS
    'Additional metadata associated with the dataset case, such as difficulty, source, or annotations.';

COMMENT ON COLUMN executions.evaluation_profile_id IS
    'Identifier of the evaluation profile applied to this execution.';

COMMENT ON COLUMN executions.evaluation_profile_version IS
    'Version of the evaluation profile used to ensure consistent evaluator configuration.';

COMMENT ON COLUMN executions.evaluator_manifest IS
    'Resolved list of evaluators to be applied to this execution. Stored as a snapshot to ensure reproducibility and to verify completeness.';

COMMENT ON COLUMN executions.expected_evaluator_count IS
    'Number of evaluators expected to run for this execution, derived from the evaluator manifest.';

COMMENT ON COLUMN executions.status IS
    'Current lifecycle state of the execution. Tracks progress from scheduling through processing, evaluation, and terminal outcome. See execution_status enum for details.';

COMMENT ON COLUMN executions.current_attempt_no IS
    'Sequence number of the current attempt for this execution. Increments on each retry.';

COMMENT ON COLUMN executions.current_attempt_id IS
    'Reference to the currently active attempt. Used to determine the authoritative attempt for this execution.';

COMMENT ON COLUMN executions.last_error_message IS
    'Most recent error encountered during execution processing. Useful for debugging failures and retry behavior.';

COMMENT ON COLUMN executions.retry_after IS
    'Earliest timestamp when a retry-scheduled execution may receive another attempt.';

COMMENT ON COLUMN executions.retry_count IS
    'Number of retry transitions scheduled for this execution after failed attempts.';

COMMENT ON COLUMN executions.last_attempt_completed_at IS
    'Timestamp when the latest authoritative attempt completed, whether terminal or retry-scheduled.';

COMMENT ON COLUMN executions.created_at IS
    'Timestamp when the execution was created.';

COMMENT ON COLUMN executions.started_at IS
    'Timestamp when the execution was first claimed and processing began.';

COMMENT ON COLUMN executions.completed_at IS
    'Timestamp when the execution reached a terminal state.';

COMMENT ON COLUMN executions.updated_at IS
    'Timestamp of the last update to the execution record.';
