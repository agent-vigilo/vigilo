CREATE TABLE evaluator_results (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    run_shard SMALLINT NOT NULL CHECK (run_shard >= 0 AND run_shard < 128),
    execution_id UUID NOT NULL,
    attempt_id UUID NOT NULL,

    binding_id TEXT NOT NULL CHECK (btrim(binding_id) <> ''),
    evaluator_id UUID NOT NULL,
    evaluator_version TEXT NOT NULL,
    evaluator_profile_id TEXT NOT NULL,
    evaluator_profile_version TEXT NOT NULL,
    evaluator_interface_version TEXT,
    evaluator_runtime_version TEXT,

    dimension TEXT NOT NULL CHECK (btrim(dimension) <> ''),
    outcome evaluator_outcome NOT NULL,
    judgment evaluation_status,
    blocking BOOLEAN NOT NULL DEFAULT false,

    measurement_kind TEXT,
    raw_boolean BOOLEAN,
    raw_numeric DOUBLE PRECISION,
    raw_ordinal TEXT,
    raw_unit TEXT,
    normalized_score DOUBLE PRECISION,
    normalization_policy_hash TEXT NOT NULL CHECK (btrim(normalization_policy_hash) <> ''),
    pass_threshold DOUBLE PRECISION NOT NULL CHECK (pass_threshold >= 0.0 AND pass_threshold <= 1.0),
    weight DOUBLE PRECISION NOT NULL DEFAULT 1.0 CHECK (weight >= 0),

    error_code TEXT,
    error_message TEXT,
    abstention_category TEXT,
    abstention_reason TEXT,
    raw_evaluator_output JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT pk_evaluator_results PRIMARY KEY (run_id, run_shard, id),
    CONSTRAINT fk_evaluator_results_execution
        FOREIGN KEY (run_id, run_shard, execution_id)
        REFERENCES executions(run_id, run_shard, id) ON DELETE CASCADE,
    CONSTRAINT fk_evaluator_results_attempt
        FOREIGN KEY (run_id, run_shard, attempt_id)
        REFERENCES execution_attempts(run_id, run_shard, id) ON DELETE CASCADE,
    CONSTRAINT chk_evaluator_result_normalized_score
        CHECK (normalized_score IS NULL OR (normalized_score >= 0.0 AND normalized_score <= 1.0)),
    CONSTRAINT chk_evaluator_result_raw_numeric
        CHECK (raw_numeric IS NULL OR raw_numeric NOT IN ('NaN'::float8, 'Infinity'::float8, '-Infinity'::float8)),
    CONSTRAINT chk_evaluator_result_measurement_shape CHECK (
        (measurement_kind IS NULL AND raw_boolean IS NULL AND raw_numeric IS NULL
            AND raw_ordinal IS NULL AND raw_unit IS NULL)
        OR
        (measurement_kind = 'binary' AND raw_boolean IS NOT NULL AND raw_numeric IS NULL
            AND raw_ordinal IS NULL AND raw_unit IS NULL)
        OR
        (measurement_kind = 'numeric' AND raw_boolean IS NULL AND raw_numeric IS NOT NULL
            AND raw_ordinal IS NULL)
        OR
        (measurement_kind = 'ordinal' AND raw_boolean IS NULL AND raw_numeric IS NULL
            AND raw_ordinal IS NOT NULL AND btrim(raw_ordinal) <> '' AND raw_unit IS NULL)
    ),
    CONSTRAINT chk_evaluator_result_outcome_shape CHECK (
        (outcome = 'completed' AND judgment IN ('passed', 'failed')
            AND measurement_kind IS NOT NULL AND normalized_score IS NOT NULL
            AND error_code IS NULL AND error_message IS NULL
            AND abstention_category IS NULL AND abstention_reason IS NULL)
        OR
        (outcome = 'error' AND judgment IS NULL
            AND normalized_score IS NULL AND error_code IS NOT NULL AND error_message IS NOT NULL
            AND abstention_category IS NULL AND abstention_reason IS NULL)
        OR
        (outcome = 'abstained' AND judgment IS NULL AND measurement_kind IS NULL
            AND normalized_score IS NULL
            AND error_code IS NULL AND error_message IS NULL
            AND abstention_category IS NOT NULL)
    ),
    CONSTRAINT uq_attempt_evaluator_binding
        UNIQUE (run_id, run_shard, attempt_id, binding_id)
) PARTITION BY LIST (run_shard);

DO $$
DECLARE partition_index INTEGER;
BEGIN
    FOR partition_index IN 0..127 LOOP
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF evaluator_results FOR VALUES IN (%s)',
            'evaluator_results_p' || lpad(partition_index::text, 3, '0'),
            partition_index
        );
    END LOOP;
END $$;

CREATE INDEX idx_evaluator_results_run_execution_id
    ON evaluator_results(run_id, run_shard, execution_id);

CREATE INDEX idx_evaluator_results_run_dimension
    ON evaluator_results(run_id, run_shard, dimension);

CREATE INDEX idx_evaluator_results_run_outcome
    ON evaluator_results(run_id, run_shard, outcome);

CREATE INDEX idx_evaluator_results_run_evaluator_id
    ON evaluator_results(run_id, run_shard, evaluator_id);

CREATE INDEX idx_evaluator_results_run_attempt
    ON evaluator_results(run_id, run_shard, attempt_id);

COMMENT ON TABLE evaluator_results IS
    'One immutable invocation result per profile binding and execution attempt. Host-owned policy produces judgment and normalized_score.';

COMMENT ON COLUMN evaluator_results.binding_id IS
    'Stable profile binding identifier and invocation idempotency key.';

COMMENT ON COLUMN evaluator_results.outcome IS
    'Evaluator execution outcome. This is separate from the host-derived judgment.';

COMMENT ON COLUMN evaluator_results.judgment IS
    'Host-derived passed or failed judgment after normalization and pass_threshold.';

COMMENT ON COLUMN evaluator_results.measurement_kind IS
    'Raw evaluator measurement type: binary, numeric, or ordinal.';

COMMENT ON COLUMN evaluator_results.normalization_policy_hash IS
    'BLAKE3 hash of the binding normalization policy used to derive normalized_score.';
