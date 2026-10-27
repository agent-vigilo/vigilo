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
    execution_requested BOOLEAN NOT NULL DEFAULT false,
    admission_cursor INTEGER NOT NULL DEFAULT 0 CHECK (admission_cursor >= 0 AND admission_cursor <= chunk_cursor),
    admission_state JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (octet_length(admission_state::text) <= 131072),
    manifest JSONB CHECK (octet_length(manifest::text) <= 8192),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((status = 'ready') = (manifest IS NOT NULL))
);

COMMENT ON TABLE run_plans IS
    'Lease-fenced preparation of immutable execution plans. Ready means verified plan publication; dispatch additionally requires execution placement admission.';

COMMENT ON COLUMN run_plans.chunk_cursor IS
    'Number of contiguous chunk identities acknowledged in object storage; preparation replays deterministic plans after interruption.';
