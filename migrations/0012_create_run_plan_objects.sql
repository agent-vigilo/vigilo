CREATE TABLE run_plan_objects (
    run_id UUID NOT NULL REFERENCES run_plans(run_id) ON DELETE CASCADE,
    object_key TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('configuration', 'chunks', 'index', 'gates', 'manifest', 'probe')),
    level SMALLINT NOT NULL CHECK (level >= 0 AND level < 8),
    range_start INTEGER CHECK (range_start >= 0),
    range_end INTEGER CHECK (range_end > range_start),
    reference JSONB NOT NULL CHECK (octet_length(reference::text) <= 8192),
    acknowledged BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (run_id, object_key),
    UNIQUE (run_id, kind, level, range_start),
    CHECK ((range_start IS NULL) = (range_end IS NULL)),
    CHECK ((kind = 'probe') = (range_start IS NULL))
);

CREATE INDEX run_plan_unacknowledged ON run_plan_objects(run_id, object_key) WHERE NOT acknowledged;

COMMENT ON TABLE run_plan_objects IS
    'Exact file ownership recorded before remote writes, with bounded indexed replay; never stores case payloads or per-case membership.';
