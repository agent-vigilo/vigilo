CREATE TABLE run_plans (
    tenant_id UUID NOT NULL DEFAULT NULLIF(current_setting('vigilo.tenant_id', true), '')::uuid,
    run_id UUID PRIMARY KEY,
    dataset_upload_id UUID NOT NULL,
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
    accepted_manifest JSONB CHECK (octet_length(accepted_manifest::text) <= 8192),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((status = 'ready') = (manifest IS NOT NULL)),
    CHECK (accepted_manifest IS NULL OR (status = 'ready' AND accepted_manifest = manifest AND execution_requested)),
    CONSTRAINT fk_run_plans_owner FOREIGN KEY (tenant_id, run_id) REFERENCES runs(tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, dataset_upload_id) REFERENCES dataset_catalog(tenant_id, id)
);

COMMENT ON TABLE run_plans IS
    'Lease-fenced preparation of immutable execution plans. Ready means verified plan publication; dispatch additionally requires execution placement admission.';

COMMENT ON COLUMN run_plans.chunk_cursor IS
    'Number of contiguous chunk identities acknowledged in object storage; preparation replays deterministic plans after interruption.';

COMMENT ON COLUMN run_plans.accepted_manifest IS
    'Exact immutable manifest approved for execution. Execution intent alone does not authorize admission or recovery.';

-- Restricted discovery and lease acquisition share the installation schema.
-- Administrative provisioning assigns the non-login owner and caller grants.
DO $install$
DECLARE installation_schema TEXT := current_schema();
BEGIN
    EXECUTE format($definition$
CREATE FUNCTION %1$I.vigilo_ready_admissions(candidate_limit INTEGER)
RETURNS TABLE(run_id UUID)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
BEGIN
    IF candidate_limit IS NULL OR candidate_limit < 0 OR candidate_limit > 1024 THEN
        RAISE EXCEPTION 'invalid admission candidate limit' USING ERRCODE = '22023';
    END IF;
    RETURN QUERY
    SELECT r.id FROM %1$I.runs r JOIN %1$I.run_plans p ON p.run_id=r.id
    WHERE r.status='creating' AND r.execution_control='enabled'
      AND p.status='ready' AND p.execution_requested AND p.accepted_manifest=p.manifest
      AND (r.coordinator_leased_until IS NULL OR r.coordinator_leased_until<=clock_timestamp())
    ORDER BY r.updated_at,r.id LIMIT candidate_limit;
END;
$function$;
$definition$, installation_schema);
    EXECUTE format($definition$
CREATE FUNCTION %1$I.vigilo_claim_admission(requested_run UUID, requested_owner UUID)
RETURNS TABLE(tenant_id UUID, run_id UUID, owner_id UUID, generation BIGINT)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
BEGIN
    IF requested_run IS NULL OR requested_owner IS NULL
       OR requested_owner='00000000-0000-0000-0000-000000000000'::uuid THEN
        RAISE EXCEPTION 'invalid admission identity' USING ERRCODE = '22023';
    END IF;
    -- Serialize with cancellation and acceptance before checking the live lease.
    PERFORM r.id FROM %1$I.runs r WHERE r.id=requested_run FOR UPDATE;
    RETURN QUERY
    WITH claimed AS (
        UPDATE %1$I.runs r
        SET coordinator_id=requested_owner,
            coordinator_leased_until=clock_timestamp()+interval '300 seconds',
            coordinator_heartbeat_at=clock_timestamp(),updated_at=clock_timestamp()
        WHERE r.id=requested_run AND r.status='creating' AND r.execution_control='enabled'
          AND (r.coordinator_leased_until IS NULL OR r.coordinator_leased_until<=clock_timestamp())
          AND EXISTS(SELECT 1 FROM %1$I.run_plans p WHERE p.run_id=r.id
                     AND p.status='ready' AND p.execution_requested AND p.accepted_manifest=p.manifest)
        RETURNING r.tenant_id,r.id,r.coordinator_id
    )
    UPDATE %1$I.run_plans p SET generation=p.generation+1,updated_at=clock_timestamp()
    FROM claimed c WHERE p.run_id=c.id
    RETURNING c.tenant_id,c.id,c.coordinator_id,p.generation;
END;
$function$;
$definition$, installation_schema);
    EXECUTE format('REVOKE ALL ON FUNCTION %I.vigilo_ready_admissions(INTEGER) FROM PUBLIC', installation_schema);
    EXECUTE format('REVOKE ALL ON FUNCTION %I.vigilo_claim_admission(UUID,UUID) FROM PUBLIC', installation_schema);
END;
$install$;
