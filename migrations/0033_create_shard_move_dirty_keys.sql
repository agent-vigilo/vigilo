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

COMMENT ON TABLE shard_move_dirty_keys IS
    'Move-scoped keys changed after capture began. Replay rereads current source state instead of retaining payload history.';
