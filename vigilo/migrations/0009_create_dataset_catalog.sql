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

COMMENT ON TABLE dataset_catalog IS 'Finalized immutable dataset references, published under an upload generation fence.';
