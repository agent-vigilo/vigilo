CREATE TABLE run_scorecards (
    run_id UUID PRIMARY KEY REFERENCES runs(id) ON DELETE CASCADE,
    schema_version SMALLINT NOT NULL CHECK (schema_version > 0),
    aggregation_policy_hash TEXT NOT NULL CHECK (btrim(aggregation_policy_hash) <> ''),
    shard_count INTEGER NOT NULL CHECK (shard_count > 0),
    passed BOOLEAN NOT NULL,
    scorecard JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_run_scorecard_shape CHECK (
        jsonb_typeof(scorecard) = 'object'
        AND jsonb_typeof(scorecard->'gates') = 'array'
    )
);

COMMENT ON TABLE run_scorecards IS
    'Authoritative immutable run-level scorecard merged from bounded shard-local rollups during fenced finalization.';

COMMENT ON COLUMN run_scorecards.aggregation_policy_hash IS
    'Frozen run policy hash shared by every contributing shard scorecard.';
