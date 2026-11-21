CREATE TABLE evaluator_http_policies (
    tenant_id UUID NOT NULL DEFAULT NULLIF(current_setting('vigilo.tenant_id', true), '')::uuid,
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    evaluator_id UUID NOT NULL REFERENCES evaluators(id) ON DELETE CASCADE,
    uri TEXT NOT NULL,
    action evaluator_http_policy_action NOT NULL,
    method TEXT NOT NULL CHECK (method ~ '^[A-Z-]{1,32}$'),
    networks TEXT[] NOT NULL DEFAULT '{}' CHECK (cardinality(networks) <= 64),
    CHECK (octet_length(uri) <= 8192),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_evaluator_http_policies_evaluator_uri_action
        UNIQUE (tenant_id, evaluator_id, uri, method, action)
);

CREATE INDEX idx_evaluator_http_policies_evaluator
    ON evaluator_http_policies(tenant_id, evaluator_id);

CREATE INDEX idx_evaluator_http_policies_action
    ON evaluator_http_policies(tenant_id, action);

COMMENT ON TABLE evaluator_http_policies IS
    'Per-evaluator outbound HTTP URI policy rules used by the host runtime to allow or deny requests.';

COMMENT ON COLUMN evaluator_http_policies.evaluator_id IS
    'Evaluator identity this policy rule applies to.';

COMMENT ON COLUMN evaluator_http_policies.uri IS
    'Exact scheme/host/effective-port/path URI for outbound HTTP policy matching.';

COMMENT ON COLUMN evaluator_http_policies.action IS
    'Policy action: allow (whitelist) or deny (blacklist).';
