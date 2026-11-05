CREATE TABLE run_control_deliveries (
    tenant_id UUID NOT NULL,
    run_id UUID NOT NULL,
    database_alias TEXT NOT NULL REFERENCES database_placements(alias),
    revision BIGINT NOT NULL CHECK (revision > 0),
    execution_control execution_control NOT NULL,
    applied_at TIMESTAMPTZ,
    quiescent_at TIMESTAMPTZ,
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    claim_token UUID,
    claimed_until TIMESTAMPTZ,
    last_error TEXT CHECK (octet_length(last_error) <= 4096),
    PRIMARY KEY (tenant_id,run_id,database_alias),
    FOREIGN KEY (tenant_id,run_id) REFERENCES runs(tenant_id,id) ON DELETE CASCADE,
    CHECK ((claim_token IS NULL) = (claimed_until IS NULL))
);
CREATE INDEX idx_run_control_delivery_pending
    ON run_control_deliveries(available_at,run_id,database_alias)
    WHERE applied_at IS NULL OR (execution_control <> 'enabled' AND quiescent_at IS NULL);
COMMENT ON TABLE run_control_deliveries IS
    'Revision-fenced direct database control delivery. Separate from broker publication; application and worker quiescence are distinct observations.';
