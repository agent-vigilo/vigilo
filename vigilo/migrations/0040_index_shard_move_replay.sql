ALTER TABLE shard_move_dirty_keys ADD COLUMN is_deleted BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE shard_move_operations
    ADD COLUMN destination_control_revision BIGINT,
    ADD COLUMN destination_control_after UUID,
    ADD CONSTRAINT ck_shard_move_destination_control_cursor CHECK (
        (destination_control_revision IS NULL AND destination_control_after IS NULL)
        OR (destination_control_revision IS NOT NULL AND destination_control_revision > 0
            AND destination_control_after IS NOT NULL)
    );

-- Recover the operation of existing journal entries from their indexed source
-- keys before the new trigger starts marking deletions.
DO $$
DECLARE
    logical_table text;
    suffix text;
    identity text;
BEGIN
    FOR logical_table, suffix IN
        SELECT * FROM (VALUES
            ('run_chunks', 'id'),
            ('run_snapshots', NULL),
            ('executions', 'id'),
            ('execution_attempts', 'id'),
            ('execution_aggregates', 'execution_id'),
            ('evaluator_results', 'id'),
            ('evaluator_diagnostics', 'id'),
            ('run_chunk_settlements', 'chunk_id'),
            ('run_chunk_settlement_consumptions', 'chunk_id'),
            ('run_shard_summaries', NULL)
        ) AS shard_table(name, key_column)
    LOOP
        identity := CASE WHEN suffix IS NULL THEN ''
            ELSE format('AND source.%I = (journal.row_key->>%L)::uuid', suffix, suffix)
        END;
        EXECUTE format(
            'UPDATE shard_move_dirty_keys journal SET is_deleted = true
             WHERE journal.table_name = %L AND NOT EXISTS (
                SELECT 1 FROM %I source
                WHERE source.run_id = (journal.row_key->>''run_id'')::uuid
                  AND source.run_shard = (journal.row_key->>''run_shard'')::smallint
                  %s
             )', logical_table, logical_table, identity
        );
    END LOOP;
END;
$$;

DROP INDEX idx_shard_move_dirty_keys_replay;
CREATE INDEX idx_shard_move_dirty_keys_replay
    ON shard_move_dirty_keys(move_id, is_deleted, table_name, last_changed_at, row_key);

CREATE OR REPLACE FUNCTION record_shard_move_dirty_key()
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

    INSERT INTO shard_move_dirty_keys (tenant_id, move_id, table_name, row_key, is_deleted)
    VALUES ((source_row->>'tenant_id')::uuid, active_move_id, logical_table, row_key, TG_OP = 'DELETE')
    ON CONFLICT ON CONSTRAINT shard_move_dirty_keys_pkey DO UPDATE
    SET change_version = shard_move_dirty_keys.change_version + 1,
        last_changed_at = now(),
        is_deleted = EXCLUDED.is_deleted;

    RETURN COALESCE(NEW, OLD);
END;
$$;

COMMENT ON COLUMN shard_move_dirty_keys.is_deleted IS
    'Latest captured action for a key; indexed so replay can page inserts and deletes in dependency order.';
