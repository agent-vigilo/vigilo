CREATE INDEX idx_runs_tenant_created_id
    ON runs (tenant_id, created_at DESC, id DESC);
