CREATE TABLE run_shard_summaries (
    run_id UUID NOT NULL,
    run_shard SMALLINT NOT NULL CHECK (run_shard >= 0 AND run_shard < 128),
    expected_execution_count INTEGER NOT NULL CHECK (expected_execution_count >= 0),
    execution_count INTEGER NOT NULL DEFAULT 0 CHECK (execution_count >= 0),
    terminal_execution_count INTEGER NOT NULL DEFAULT 0 CHECK (terminal_execution_count >= 0),
    aggregate_count INTEGER NOT NULL DEFAULT 0 CHECK (aggregate_count >= 0),
    passed_execution_count INTEGER NOT NULL DEFAULT 0 CHECK (passed_execution_count >= 0),
    failed_execution_count INTEGER NOT NULL DEFAULT 0 CHECK (failed_execution_count >= 0),
    errored_execution_count INTEGER NOT NULL DEFAULT 0 CHECK (errored_execution_count >= 0),
    skipped_execution_count INTEGER NOT NULL DEFAULT 0 CHECK (skipped_execution_count >= 0),
    missing_aggregate_count INTEGER NOT NULL DEFAULT 0 CHECK (missing_aggregate_count >= 0),
    evaluator_result_count BIGINT NOT NULL DEFAULT 0 CHECK (evaluator_result_count >= 0),
    blocking_failure_count BIGINT NOT NULL DEFAULT 0 CHECK (blocking_failure_count >= 0),
    score_count BIGINT NOT NULL DEFAULT 0 CHECK (score_count >= 0),
    score_sum DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    min_score DOUBLE PRECISION,
    max_score DOUBLE PRECISION,
    scorecard JSONB NOT NULL DEFAULT '{"version":1,"policy_hash":"","entries":[]}'::jsonb,
    failed_chunk_count INTEGER NOT NULL DEFAULT 0 CHECK (failed_chunk_count >= 0),
    cancelled_chunk_count INTEGER NOT NULL DEFAULT 0 CHECK (cancelled_chunk_count >= 0),
    planned_chunk_hash TEXT NOT NULL DEFAULT '',
    sealed BOOLEAN NOT NULL DEFAULT false,
    planned_chunk_count INTEGER NOT NULL DEFAULT 0 CHECK (planned_chunk_count >= 0),
    settled_chunk_count INTEGER NOT NULL DEFAULT 0 CHECK (settled_chunk_count >= 0),
    missing_execution_count INTEGER NOT NULL DEFAULT 0 CHECK (missing_execution_count >= 0),
    unresolved_execution_count INTEGER NOT NULL DEFAULT 0 CHECK (unresolved_execution_count >= 0),
    aggregation_state JSONB,
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_summary_seal CHECK (
        NOT sealed OR (status IN ('completed', 'failed')
            AND planned_chunk_count > 0 AND settled_chunk_count = planned_chunk_count)
    ),
    CONSTRAINT pk_run_shard_summaries PRIMARY KEY (run_id, run_shard),
    CONSTRAINT fk_run_shard_summaries_snapshot
        FOREIGN KEY (run_id, run_shard)
        REFERENCES run_snapshots(run_id, run_shard)
        ON DELETE CASCADE
);

CREATE INDEX idx_run_shard_summaries_run_status
    ON run_shard_summaries(run_id, status);

CREATE INDEX idx_pending_settlement_summaries
    ON run_shard_summaries(updated_at, run_id, run_shard)
    WHERE NOT sealed;

CREATE FUNCTION protect_sealed_shard_summary() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.sealed AND NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'sealed shard summary is immutable for run %, shard %',
            OLD.run_id, OLD.run_shard;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER protect_sealed_shard_summary
BEFORE UPDATE ON run_shard_summaries
FOR EACH ROW EXECUTE FUNCTION protect_sealed_shard_summary();

COMMENT ON TABLE run_shard_summaries IS
    'Shard-local execution progress summary used by coordinators to roll up global run state without scanning all execution rows in control storage.';

COMMENT ON COLUMN run_shard_summaries.expected_execution_count IS
    'Expected execution count for this run shard, copied from the local run snapshot.';

COMMENT ON COLUMN run_shard_summaries.execution_count IS
    'Executions represented by incorporated terminal chunk receipts.';

COMMENT ON COLUMN run_shard_summaries.terminal_execution_count IS
    'Terminal executions represented by incorporated chunk receipts.';

COMMENT ON COLUMN run_shard_summaries.aggregate_count IS
    'Authoritative terminal execution aggregates represented by incorporated chunk receipts.';

COMMENT ON COLUMN run_shard_summaries.missing_aggregate_count IS
    'Terminal executions missing the current-attempt aggregate required for final scoring.';

COMMENT ON COLUMN run_shard_summaries.skipped_execution_count IS
    'Terminal executions whose current aggregate has overall_status skipped.';

COMMENT ON COLUMN run_shard_summaries.evaluator_result_count IS
    'Evaluator result rows represented by execution aggregates for this run shard.';

COMMENT ON COLUMN run_shard_summaries.blocking_failure_count IS
    'Blocking failure entries represented by execution aggregates for this run shard.';

COMMENT ON COLUMN run_shard_summaries.score_count IS
    'Number of scored execution aggregates included in score_sum/min/max.';

COMMENT ON COLUMN run_shard_summaries.scorecard IS
    'Versioned bounded run-gate rollup computed from current authoritative attempts on this shard.';

COMMENT ON COLUMN run_shard_summaries.aggregation_state IS
    'Resumable canonical receipt-fold checkpoint; updated atomically with summary counters under a transaction-scoped coordinator claim.';

COMMENT ON COLUMN run_shard_summaries.sealed IS
    'True only after exact planned chunk membership has settled and been incorporated; execution coverage may remain incomplete.';

COMMENT ON COLUMN run_shard_summaries.missing_execution_count IS
    'Planned cases in incorporated terminal chunks for which no execution row was created.';

COMMENT ON COLUMN run_shard_summaries.unresolved_execution_count IS
    'Existing executions in incorporated terminal chunks without an authoritative terminal aggregate; retained partial/stale evidence is not scored.';

COMMENT ON COLUMN run_shard_summaries.status IS
    'Running until every planned chunk receipt is incorporated; then completed or failed according to retained outcomes and operational completeness.';
