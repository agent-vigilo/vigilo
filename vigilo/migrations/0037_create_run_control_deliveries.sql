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

-- A scheduler can claim bounded work without direct access to delivery rows.
-- Bind relations to the installation schema, not the caller's search path.
DO $install$
DECLARE installation_schema TEXT := current_schema();
BEGIN
    EXECUTE format($definition$
CREATE FUNCTION %1$I.vigilo_claim_control_deliveries(requested_run UUID)
RETURNS TABLE (
    tenant_id UUID, run_id UUID, database_alias TEXT, revision BIGINT,
    execution_control %1$I.execution_control, claim_token UUID, applied BOOLEAN
)
LANGUAGE sql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
    WITH candidates AS (
        SELECT q.tenant_id,q.run_id,q.database_alias
        FROM %1$I.run_control_deliveries q
        WHERE (requested_run IS NULL OR q.run_id=requested_run)
          AND q.available_at<=clock_timestamp()
          AND (q.claimed_until IS NULL OR q.claimed_until<clock_timestamp())
          AND (q.applied_at IS NULL OR (q.execution_control<>'enabled' AND q.quiescent_at IS NULL))
        ORDER BY q.available_at,q.run_id,q.database_alias
        LIMIT 32 FOR UPDATE SKIP LOCKED
    )
    UPDATE %1$I.run_control_deliveries d
    SET claim_token=gen_random_uuid(),claimed_until=clock_timestamp()+interval '60 seconds',attempts=d.attempts+1
    FROM candidates c
    WHERE d.tenant_id=c.tenant_id AND d.run_id=c.run_id AND d.database_alias=c.database_alias
    RETURNING d.tenant_id,d.run_id,d.database_alias,d.revision,d.execution_control,d.claim_token,d.applied_at IS NOT NULL;
$function$;
$definition$, installation_schema);
    EXECUTE format('REVOKE ALL ON FUNCTION %I.vigilo_claim_control_deliveries(UUID) FROM PUBLIC', installation_schema);
END;
$install$;

-- Settlement eligibility depends on all delivery destinations, so install this
-- shared run/control function only after the delivery table exists.
DO $install$
DECLARE installation_schema TEXT := current_schema();
BEGIN
    EXECUTE format($definition$
CREATE FUNCTION %1$I.vigilo_claim_revocation_settlements(requested_run UUID)
RETURNS TABLE(run_id UUID, revision BIGINT, claim_token UUID)
LANGUAGE sql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
    WITH candidates AS (
        SELECT r.id FROM %1$I.runs r
        WHERE (requested_run IS NULL OR r.id=requested_run)
          AND r.execution_control='revoked'
          AND r.control_settled_revision<r.control_revision
          AND (r.control_settlement_until IS NULL OR r.control_settlement_until<clock_timestamp())
          AND NOT EXISTS (
              SELECT 1 FROM %1$I.run_control_deliveries d
              WHERE d.tenant_id=r.tenant_id AND d.run_id=r.id
                AND (d.revision<>r.control_revision OR d.quiescent_at IS NULL)
          )
        ORDER BY r.updated_at,r.id LIMIT 16 FOR UPDATE SKIP LOCKED
    )
    UPDATE %1$I.runs r
    SET control_settlement_token=gen_random_uuid(),control_settlement_until=clock_timestamp()+interval '60 seconds'
    FROM candidates c WHERE r.id=c.id
    RETURNING r.id,r.control_revision,r.control_settlement_token;
$function$;
$definition$, installation_schema);
    EXECUTE format('REVOKE ALL ON FUNCTION %I.vigilo_claim_revocation_settlements(UUID) FROM PUBLIC', installation_schema);
END;
$install$;
