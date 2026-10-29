CREATE TABLE evaluator_diagnostics (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL,
    run_shard SMALLINT NOT NULL CHECK (run_shard >= 0 AND run_shard < 128),
    evaluator_result_id UUID NOT NULL,
    diagnostic_index INTEGER NOT NULL CHECK (diagnostic_index >= 0),
    severity severity NOT NULL,
    category TEXT NOT NULL CHECK (btrim(category) <> ''),
    reason TEXT,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    tags TEXT[] NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT pk_evaluator_diagnostics PRIMARY KEY (run_id, run_shard, id),
    CONSTRAINT fk_evaluator_diagnostics_result
        FOREIGN KEY (run_id, run_shard, evaluator_result_id)
        REFERENCES evaluator_results(run_id, run_shard, id) ON DELETE CASCADE,
    CONSTRAINT uq_evaluator_diagnostic_index
        UNIQUE (run_id, run_shard, evaluator_result_id, diagnostic_index)
) PARTITION BY LIST (run_shard);

DO $$
DECLARE partition_index INTEGER;
BEGIN
    FOR partition_index IN 0..127 LOOP
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF evaluator_diagnostics FOR VALUES IN (%s)',
            'evaluator_diagnostics_p' || lpad(partition_index::text, 3, '0'),
            partition_index
        );
    END LOOP;
END $$;

CREATE INDEX idx_evaluator_diagnostics_result
    ON evaluator_diagnostics(run_id, run_shard, evaluator_result_id);

CREATE INDEX idx_evaluator_diagnostics_category
    ON evaluator_diagnostics(run_id, run_shard, category);

COMMENT ON TABLE evaluator_diagnostics IS
    'Zero or more non-authoritative diagnostic findings attached to an evaluator invocation result.';
