-- Shared dirty-key capture is installed after every participating table exists.
-- Table-specific integrity triggers remain with their owning table definitions.

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

CREATE TRIGGER capture_run_chunk_settlement_consumptions_changes
AFTER INSERT OR UPDATE OR DELETE ON run_chunk_settlement_consumptions
FOR EACH ROW EXECUTE FUNCTION record_shard_move_dirty_key(
    'run_chunk_settlement_consumptions', 'run_id', 'run_shard', 'chunk_id'
);

CREATE TRIGGER capture_run_chunk_settlements_changes
AFTER INSERT OR UPDATE OR DELETE ON run_chunk_settlements
FOR EACH ROW EXECUTE FUNCTION record_shard_move_dirty_key(
    'run_chunk_settlements', 'run_id', 'run_shard', 'chunk_id'
);
