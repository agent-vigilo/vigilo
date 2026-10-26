CREATE TABLE run_plans (
    run_id UUID PRIMARY KEY REFERENCES runs(id) ON DELETE CASCADE,
    dataset_upload_id UUID NOT NULL REFERENCES dataset_catalog(id),
    status TEXT NOT NULL DEFAULT 'preparing' CHECK (status IN ('preparing', 'ready')),
    storage_binding_hash TEXT NOT NULL,
    limits JSONB NOT NULL CHECK (octet_length(limits::text) <= 4096),
    execution_plan JSONB NOT NULL CHECK (octet_length(execution_plan::text) <= 1048576),
    assignments JSONB NOT NULL CHECK (jsonb_array_length(assignments) BETWEEN 1 AND 128 AND octet_length(assignments::text) <= 32768),
    generation BIGINT NOT NULL DEFAULT 0 CHECK (generation >= 0),
    chunk_cursor INTEGER NOT NULL DEFAULT 0 CHECK (chunk_cursor >= 0),
    manifest JSONB CHECK (octet_length(manifest::text) <= 8192),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((status = 'ready') = (manifest IS NOT NULL))
);
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
COMMENT ON TABLE run_plans IS
    'Lease-fenced preparation of immutable execution plans. Ready means verified plan publication; dispatch additionally requires execution placement admission.';
COMMENT ON COLUMN run_plans.chunk_cursor IS
    'Number of contiguous chunk identities acknowledged in object storage; preparation replays deterministic plans after interruption.';
COMMENT ON TABLE run_plan_objects IS
    'Exact file ownership recorded before remote writes, with bounded indexed replay; never stores case payloads or per-case membership.';
COMMENT ON TABLE run_plan_shards IS
    'Compact verified shard totals, membership digests and frozen initial placements; these rows do not authorize worker dispatch.';
