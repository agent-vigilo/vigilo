CREATE TABLE shard_move_captures (
    tenant_id UUID NOT NULL DEFAULT NULLIF(current_setting('vigilo.tenant_id', true), '')::uuid,
    move_id UUID PRIMARY KEY,
    run_id UUID NOT NULL,
    run_shard SMALLINT NOT NULL CHECK (run_shard >= 0 AND run_shard < 128),
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, run_shard),
    CONSTRAINT fk_shard_move_captures_owner FOREIGN KEY (tenant_id, run_id) REFERENCES runs(tenant_id, id) ON DELETE CASCADE,
    UNIQUE (tenant_id, move_id)
);

COMMENT ON TABLE shard_move_captures IS
    'Source-local switch that enables same-transaction dirty-key capture for one run shard.';
