CREATE TABLE shard_move_captures (
    move_id UUID PRIMARY KEY,
    run_id UUID NOT NULL,
    run_shard SMALLINT NOT NULL CHECK (run_shard >= 0 AND run_shard < 128),
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, run_shard)
);

COMMENT ON TABLE shard_move_captures IS
    'Source-local switch that enables same-transaction dirty-key capture for one run shard.';
