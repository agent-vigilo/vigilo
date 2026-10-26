-- Control-plane ownership and bounded file metadata only; no case payload rows.
CREATE TABLE dataset_uploads (
    id UUID PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'staging' CHECK (status IN ('staging','finalizing','finalized','aborted','expired')),
    storage_alias TEXT NOT NULL CHECK (length(storage_alias) BETWEEN 1 AND 128),
    binding_hash TEXT NOT NULL CHECK (binding_hash ~ '^[0-9a-f]{64}$'),
    identity JSONB NOT NULL CHECK (octet_length(identity::text) <= 4096),
    limits JSONB NOT NULL CHECK (octet_length(limits::text) <= 4096),
    case_cursor INTEGER NOT NULL DEFAULT 0 CHECK (case_cursor >= 0),
    batch_count INTEGER NOT NULL DEFAULT 0 CHECK (batch_count >= 0),
    generation BIGINT NOT NULL DEFAULT 0 CHECK (generation >= 0),
    lease_owner UUID,
    leased_until TIMESTAMPTZ,
    expires_at TIMESTAMPTZ NOT NULL DEFAULT (now() + interval '7 days'),
    cleanup_after TIMESTAMPTZ,
    cleaned_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((lease_owner IS NULL) = (leased_until IS NULL)),
    CHECK ((status IN ('aborted','expired')) = (cleanup_after IS NOT NULL))
);
CREATE INDEX dataset_upload_cleanup ON dataset_uploads (cleanup_after, id)
    WHERE status IN ('aborted','expired') AND cleaned_at IS NULL;
CREATE INDEX dataset_upload_expiry ON dataset_uploads (expires_at, id)
    WHERE status IN ('staging','finalizing');

CREATE TABLE dataset_upload_objects (
    upload_id UUID NOT NULL REFERENCES dataset_uploads(id),
    object_key TEXT NOT NULL CHECK (octet_length(object_key) BETWEEN 1 AND 1024),
    kind TEXT NOT NULL CHECK (kind IN ('batch','index','source','manifest','probe')),
    level SMALLINT NOT NULL DEFAULT 0 CHECK (level BETWEEN 0 AND 7),
    range_start INTEGER CHECK (range_start >= 0),
    range_end INTEGER,
    reference JSONB NOT NULL CHECK (octet_length(reference::text) <= 8192),
    acknowledged BOOLEAN NOT NULL DEFAULT false,
    cleaned BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (upload_id, object_key),
    UNIQUE (upload_id, kind, level, range_start),
    CHECK ((range_start IS NULL AND range_end IS NULL AND kind = 'probe') OR
           (range_start IS NOT NULL AND range_end > range_start AND kind <> 'probe'))
);
CREATE INDEX dataset_upload_pending_cleanup ON dataset_upload_objects (upload_id, object_key)
    WHERE NOT cleaned;

CREATE TABLE dataset_catalog (
    id UUID PRIMARY KEY REFERENCES dataset_uploads(id),
    storage_alias TEXT NOT NULL,
    binding_hash TEXT NOT NULL,
    dataset_hash TEXT NOT NULL CHECK (dataset_hash ~ '^[0-9a-f]{64}$'),
    dataset_version_id UUID NOT NULL,
    limits JSONB NOT NULL,
    manifest JSONB NOT NULL CHECK (octet_length(manifest::text) <= 8192),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX dataset_catalog_reuse ON dataset_catalog (storage_alias, binding_hash, dataset_hash);
COMMENT ON TABLE dataset_upload_objects IS 'Exact owned attempt keys recorded before remote I/O; progress is acknowledged after byte and semantic verification.';
COMMENT ON TABLE dataset_catalog IS 'Finalized immutable dataset references, published under an upload generation fence.';
