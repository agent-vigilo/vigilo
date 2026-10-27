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
