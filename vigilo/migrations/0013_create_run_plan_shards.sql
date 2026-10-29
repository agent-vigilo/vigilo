CREATE TABLE run_plan_shards (
    run_id UUID NOT NULL REFERENCES run_plans(run_id) ON DELETE CASCADE,
    run_shard SMALLINT NOT NULL CHECK (run_shard >= 0 AND run_shard < 128),
    database_alias TEXT NOT NULL REFERENCES database_placements(alias),
    case_count INTEGER NOT NULL CHECK (case_count > 0),
    chunk_count INTEGER NOT NULL CHECK (chunk_count > 0),
    planned_chunk_hash TEXT NOT NULL,
    case_projection_hash TEXT NOT NULL,
    PRIMARY KEY (run_id, run_shard)
);

COMMENT ON TABLE run_plan_shards IS
    'Compact verified shard totals, membership digests and frozen initial placements; these rows do not authorize worker dispatch.';
