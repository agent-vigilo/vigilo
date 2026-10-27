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

COMMENT ON TABLE shard_move_table_progress IS
    'Compact durable backfill cursor and counters for one move table. A target page may replay before this control acknowledgement without duplicating rows.';
