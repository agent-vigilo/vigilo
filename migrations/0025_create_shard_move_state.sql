CREATE TABLE shard_move_operations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL,
    run_shard SMALLINT NOT NULL CHECK (run_shard >= 0 AND run_shard < 128),
    source_database_alias TEXT NOT NULL REFERENCES database_placements(alias),
    target_database_alias TEXT NOT NULL REFERENCES database_placements(alias),
    starting_route_version BIGINT NOT NULL CHECK (starting_route_version > 0),
    status TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'completed', 'aborted', 'failed')),
    phase TEXT NOT NULL DEFAULT 'reserved'
        CHECK (phase IN (
            'reserved', 'backfill', 'catch_up', 'draining', 'cutover',
            'completed', 'aborted', 'failed'
        )),
    target_reset_at TIMESTAMPTZ,
    copied_row_count BIGINT NOT NULL DEFAULT 0 CHECK (copied_row_count >= 0),
    copied_byte_count BIGINT NOT NULL DEFAULT 0 CHECK (copied_byte_count >= 0),
    error_message TEXT,
    claim_generation BIGINT NOT NULL DEFAULT 0 CHECK (claim_generation >= 0),
    claim_token UUID,
    claimed_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,

    CONSTRAINT ck_shard_move_operation_aliases
        CHECK (source_database_alias <> target_database_alias),
    CONSTRAINT ck_shard_move_operation_claim
        CHECK (
            (claim_token IS NULL AND claimed_until IS NULL)
            OR (claim_token IS NOT NULL AND claimed_until IS NOT NULL)
        )
);

CREATE UNIQUE INDEX uq_shard_move_operations_active_shard
    ON shard_move_operations(run_id, run_shard)
    WHERE status = 'active';

CREATE INDEX idx_shard_move_operations_claim
    ON shard_move_operations(status, claimed_until, created_at)
    WHERE status = 'active';

CREATE TABLE shard_move_table_progress (
    move_id UUID NOT NULL REFERENCES shard_move_operations(id) ON DELETE CASCADE,
    table_name TEXT NOT NULL CHECK (table_name IN (
        'dataset_versions', 'runs',
        'run_chunks', 'run_snapshots', 'executions', 'execution_attempts',
        'execution_aggregates', 'evaluator_results', 'evaluator_diagnostics',
        'run_shard_summaries', 'run_chunk_settlements', 'run_chunk_settlement_consumptions'
    )),
    completed_page_count BIGINT NOT NULL CHECK (completed_page_count > 0),
    last_start_after_key TEXT,
    last_end_key TEXT NOT NULL,
    copied_row_count BIGINT NOT NULL CHECK (copied_row_count > 0),
    copied_byte_count BIGINT NOT NULL CHECK (copied_byte_count > 0),
    last_page_checksum TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (move_id, table_name)
);

CREATE TABLE shard_move_captures (
    move_id UUID PRIMARY KEY,
    run_id UUID NOT NULL,
    run_shard SMALLINT NOT NULL CHECK (run_shard >= 0 AND run_shard < 128),
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, run_shard)
);

CREATE TABLE shard_move_dirty_keys (
    move_id UUID NOT NULL REFERENCES shard_move_captures(move_id) ON DELETE CASCADE,
    table_name TEXT NOT NULL CHECK (table_name IN (
        'run_chunks', 'run_snapshots', 'executions',
        'execution_attempts', 'execution_aggregates', 'evaluator_results',
        'evaluator_diagnostics', 'run_shard_summaries', 'run_chunk_settlements', 'run_chunk_settlement_consumptions'
    )),
    row_key JSONB NOT NULL,
    change_version BIGINT NOT NULL DEFAULT 1 CHECK (change_version > 0),
    first_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (move_id, table_name, row_key)
);

CREATE INDEX idx_shard_move_dirty_keys_replay
    ON shard_move_dirty_keys(move_id, table_name, last_changed_at, row_key);

CREATE FUNCTION record_shard_move_dirty_key()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    source_row JSONB := COALESCE(to_jsonb(NEW), to_jsonb(OLD));
    active_move_id UUID;
    logical_table TEXT := TG_ARGV[0];
    row_key JSONB := '{}'::jsonb;
    argument_index INTEGER;
BEGIN
    SELECT capture.move_id
    INTO active_move_id
    FROM shard_move_captures capture
    WHERE capture.run_id = (source_row->>'run_id')::uuid
      AND capture.run_shard = (source_row->>'run_shard')::smallint
      AND capture.active;

    IF active_move_id IS NULL THEN
        RETURN COALESCE(NEW, OLD);
    END IF;

    FOR argument_index IN 1..TG_NARGS - 1 LOOP
        row_key := row_key || jsonb_build_object(
            TG_ARGV[argument_index],
            source_row->TG_ARGV[argument_index]
        );
    END LOOP;

    INSERT INTO shard_move_dirty_keys (move_id, table_name, row_key)
    VALUES (active_move_id, logical_table, row_key)
    ON CONFLICT ON CONSTRAINT shard_move_dirty_keys_pkey DO UPDATE
    SET change_version = shard_move_dirty_keys.change_version + 1,
        last_changed_at = now();

    RETURN COALESCE(NEW, OLD);
END;
$$;


CREATE TRIGGER capture_run_chunks_changes
AFTER INSERT OR UPDATE OR DELETE ON run_chunks
FOR EACH ROW EXECUTE FUNCTION record_shard_move_dirty_key(
    'run_chunks', 'run_id', 'run_shard', 'id'
);

CREATE TRIGGER capture_run_snapshots_changes
AFTER INSERT OR UPDATE OR DELETE ON run_snapshots
FOR EACH ROW EXECUTE FUNCTION record_shard_move_dirty_key(
    'run_snapshots', 'run_id', 'run_shard'
);

CREATE TRIGGER capture_executions_changes
AFTER INSERT OR UPDATE OR DELETE ON executions
FOR EACH ROW EXECUTE FUNCTION record_shard_move_dirty_key(
    'executions', 'run_id', 'run_shard', 'id'
);

CREATE TRIGGER capture_execution_attempts_changes
AFTER INSERT OR UPDATE OR DELETE ON execution_attempts
FOR EACH ROW EXECUTE FUNCTION record_shard_move_dirty_key(
    'execution_attempts', 'run_id', 'run_shard', 'id'
);

CREATE TRIGGER capture_execution_aggregates_changes
AFTER INSERT OR UPDATE OR DELETE ON execution_aggregates
FOR EACH ROW EXECUTE FUNCTION record_shard_move_dirty_key(
    'execution_aggregates', 'run_id', 'run_shard', 'execution_id'
);

CREATE TRIGGER capture_evaluator_results_changes
AFTER INSERT OR UPDATE OR DELETE ON evaluator_results
FOR EACH ROW EXECUTE FUNCTION record_shard_move_dirty_key(
    'evaluator_results', 'run_id', 'run_shard', 'id'
);

CREATE TRIGGER capture_evaluator_diagnostics_changes
AFTER INSERT OR UPDATE OR DELETE ON evaluator_diagnostics
FOR EACH ROW EXECUTE FUNCTION record_shard_move_dirty_key(
    'evaluator_diagnostics', 'run_id', 'run_shard', 'id'
);

CREATE TRIGGER capture_run_shard_summaries_changes
AFTER INSERT OR UPDATE OR DELETE ON run_shard_summaries
FOR EACH ROW EXECUTE FUNCTION record_shard_move_dirty_key(
    'run_shard_summaries', 'run_id', 'run_shard'
);

COMMENT ON TABLE shard_move_operations IS
    'Control-plane state for one resumable online run-shard move.';

COMMENT ON TABLE shard_move_table_progress IS
    'Compact durable backfill cursor and counters for one move table. A target page may replay before this control acknowledgement without duplicating rows.';

COMMENT ON TABLE shard_move_captures IS
    'Source-local switch that enables same-transaction dirty-key capture for one run shard.';

COMMENT ON TABLE shard_move_dirty_keys IS
    'Move-scoped keys changed after capture began. Replay rereads current source state instead of retaining payload history.';

COMMENT ON COLUMN shard_move_operations.claim_generation IS
    'Monotonic claimant generation. Every successful move claim increments it so a stale process cannot reinstall older target authority.';
