CREATE TABLE evaluator_http_policies (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    evaluator_id UUID NOT NULL REFERENCES evaluators(id) ON DELETE CASCADE,
    uri TEXT NOT NULL,
    action evaluator_http_policy_action NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_evaluator_http_policies_evaluator_uri_action
        UNIQUE (evaluator_id, uri, action)
);

CREATE INDEX idx_evaluator_http_policies_evaluator
    ON evaluator_http_policies(evaluator_id);

CREATE INDEX idx_evaluator_http_policies_action
    ON evaluator_http_policies(action);

COMMENT ON TABLE evaluator_http_policies IS
    'Per-evaluator outbound HTTP URI policy rules used by the host runtime to allow or deny requests.';

COMMENT ON COLUMN evaluator_http_policies.evaluator_id IS
    'Evaluator identity this policy rule applies to.';

COMMENT ON COLUMN evaluator_http_policies.uri IS
    'URI pattern or exact URI for outbound HTTP policy matching.';

COMMENT ON COLUMN evaluator_http_policies.action IS
    'Policy action: allow (whitelist) or deny (blacklist).';
