CREATE TABLE run_chunk_settlement_consumptions (
    run_id UUID NOT NULL,
    run_shard SMALLINT NOT NULL,
    chunk_id UUID NOT NULL,
    ordinal_start INTEGER NOT NULL CHECK (ordinal_start >= 0),
    consumed BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (run_id, run_shard, chunk_id),
    FOREIGN KEY (run_id, run_shard, chunk_id)
        REFERENCES run_chunk_settlements(run_id, run_shard, chunk_id) ON DELETE CASCADE
);

CREATE INDEX idx_pending_chunk_settlement_consumptions
    ON run_chunk_settlement_consumptions(run_id, run_shard, ordinal_start, chunk_id)
    WHERE NOT consumed;

COMMENT ON TABLE run_chunk_settlement_consumptions IS
    'Pending receipt work created at settlement; consumption commits atomically with progress counters. Final scores are folded separately in canonical planned order.';
