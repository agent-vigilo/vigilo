CREATE TABLE run_shard_dispatch_cursors (
    tenant_id UUID NOT NULL DEFAULT NULLIF(current_setting('vigilo.tenant_id', true), '')::uuid,
    run_id UUID NOT NULL,
    run_shard SMALLINT NOT NULL CHECK (run_shard >= 0 AND run_shard < 128),
    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'drained')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT pk_run_shard_dispatch_cursors PRIMARY KEY (run_id, run_shard),
    CONSTRAINT fk_run_shard_dispatch_cursors_owner FOREIGN KEY (tenant_id, run_id) REFERENCES runs(tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX idx_run_shard_dispatch_cursors_open
    ON run_shard_dispatch_cursors(updated_at, run_id, run_shard)
    WHERE status = 'open';

CREATE INDEX idx_run_shard_dispatch_cursors_run_status
    ON run_shard_dispatch_cursors(run_id, status);

COMMENT ON TABLE run_shard_dispatch_cursors IS
    'Coordinator-owned shard cursors that bound dispatch scans to one run_id + run_shard partition at a time.';

COMMENT ON COLUMN run_shard_dispatch_cursors.run_id IS
    'Run whose pending chunks are being dispatched for this logical shard.';

COMMENT ON COLUMN run_shard_dispatch_cursors.run_shard IS
    'Logical run shard selected by the coordinator dispatch cursor.';

COMMENT ON COLUMN run_shard_dispatch_cursors.status IS
    'Dispatch cursor lifecycle. open means the shard may still have undispatched pending chunks; drained means dispatch has exhausted this run shard.';

COMMENT ON INDEX idx_run_shard_dispatch_cursors_open IS
    'Hot index for coordinator dispatch cursor claims ordered by cursor age.';


-- Execute privileges are deliberately absent for ordinary callers. Deployment
-- administration assigns a narrowly privileged function owner and caller grants.
-- The row lock belongs to the caller transaction, including rollback on failure.
DO $install$
DECLARE
    installation_schema TEXT := current_schema();
BEGIN
    -- Resolve the installation schema once, never from a caller's search path.
    EXECUTE format($definition$
CREATE FUNCTION %1$I.vigilo_claim_dispatch(excluded_aliases TEXT[])
RETURNS TABLE (
    tenant_id UUID, run_id UUID, run_shard SMALLINT, database_alias TEXT,
    placement_status TEXT, route_version BIGINT, write_epoch BIGINT
)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
BEGIN
    IF excluded_aliases IS NULL OR cardinality(excluded_aliases) > 128
       OR array_position(excluded_aliases, NULL) IS NOT NULL THEN
        RAISE EXCEPTION 'invalid excluded placement list' USING ERRCODE = '22023';
    END IF;
    RETURN QUERY
SELECT
            r.tenant_id,
            c.run_id,
            c.run_shard,
            sp.database_alias,
            sp.status AS placement_status,
            sp.route_version,
            sp.write_epoch
        FROM %1$I.run_shard_dispatch_cursors c
        JOIN %1$I.runs r
          ON r.id = c.run_id
        JOIN %1$I.shard_placements sp
          ON sp.run_id = c.run_id
         AND sp.run_shard = c.run_shard
        JOIN %1$I.database_placements dp
          ON dp.alias = sp.database_alias
        WHERE c.status = 'open'
          AND r.execution_control = 'enabled'
          AND sp.status = 'active'
          AND dp.status IN ('active', 'draining')
          AND dp.role IN ('shard', 'control_and_shard')
          AND NOT (sp.database_alias = ANY(excluded_aliases))
          AND (
              r.status = 'running'::%1$I.run_status
              OR (
                  r.status = 'pending'::%1$I.run_status
                  AND (
                      r.coordinator_leased_until IS NULL
                      OR r.coordinator_leased_until < now()
                  )
              )
          )
        ORDER BY c.updated_at ASC, c.run_id ASC, c.run_shard ASC
        LIMIT 1
        FOR UPDATE OF c SKIP LOCKED;
END;
$function$;
$definition$, installation_schema);
    EXECUTE format('REVOKE ALL ON FUNCTION %I.vigilo_claim_dispatch(TEXT[]) FROM PUBLIC', installation_schema);
    EXECUTE format('COMMENT ON FUNCTION %I.vigilo_claim_dispatch(TEXT[]) IS %L', installation_schema,
        'Claims at most one eligible dispatch cursor until the caller transaction ends. Explicit execute grant required; does not grant table access.');
END;
$install$;

-- Restricted scheduler entry point; relations bind to the installation schema.
DO $install$
DECLARE installation_schema TEXT := current_schema();
BEGIN
    EXECUTE format($definition$
CREATE FUNCTION %1$I.vigilo_next_finalization_candidate(p_excluded uuid[])
RETURNS TABLE(id uuid, run_key text, aggregation_policy_hash text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
BEGIN
    IF p_excluded IS NULL OR cardinality(p_excluded) > 100000
       OR array_ndims(p_excluded) > 1 OR array_position(p_excluded, NULL) IS NOT NULL THEN
        RAISE EXCEPTION 'invalid excluded run list' USING ERRCODE = '22023';
    END IF;
    RETURN QUERY
SELECT r.id, r.run_key, r.aggregation_policy_hash
        FROM %1$I.runs r
        WHERE r.status IN ('running'::%1$I.run_status, 'finalizing'::%1$I.run_status)
          AND r.execution_control = 'enabled'
          AND (
              r.status <> 'finalizing'::%1$I.run_status
              OR r.coordinator_leased_until IS NULL
              OR r.coordinator_leased_until < now()
          )
          AND EXISTS (
              SELECT 1
              FROM %1$I.run_shard_dispatch_cursors c
              WHERE c.run_id = r.id
          )
          AND NOT EXISTS (
              SELECT 1
              FROM %1$I.run_shard_dispatch_cursors c
              WHERE c.run_id = r.id
                AND c.status = 'open'
          )
          AND NOT (r.id = ANY(p_excluded::uuid[]))
        ORDER BY COALESCE(r.coordinator_heartbeat_at, r.updated_at) ASC, r.id ASC
        LIMIT 1;
END;
$function$;
$definition$, installation_schema);
    EXECUTE format('REVOKE ALL ON FUNCTION %I.vigilo_next_finalization_candidate(uuid[]) FROM PUBLIC', installation_schema);
END;
$install$;

-- Restricted scheduler read; aggregate/configuration output only.
DO $install$
DECLARE installation_schema TEXT := current_schema();
BEGIN
    EXECUTE format($definition$
CREATE FUNCTION %1$I.vigilo_dispatch_backlog()
RETURNS bigint
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
SELECT COUNT(*)::bigint
        FROM %1$I.run_shard_dispatch_cursors c
        JOIN %1$I.runs r
          ON r.id = c.run_id
        JOIN %1$I.shard_placements sp
          ON sp.run_id = c.run_id
         AND sp.run_shard = c.run_shard
        JOIN %1$I.database_placements dp
          ON dp.alias = sp.database_alias
        WHERE c.status = 'open'
          AND r.execution_control = 'enabled'
          AND sp.status = 'active'
          AND dp.status IN ('active', 'draining')
          AND dp.role IN ('shard', 'control_and_shard')
          AND (
              r.status = 'running'::%1$I.run_status
              OR (
                  r.status = 'pending'::%1$I.run_status
                  AND (
                      r.coordinator_leased_until IS NULL
                      OR r.coordinator_leased_until < now()
                  )
              )
          )
$function$;
$definition$, installation_schema);
    EXECUTE format('REVOKE ALL ON FUNCTION %I.vigilo_dispatch_backlog() FROM PUBLIC', installation_schema);
END;
$install$;

-- Restricted scheduler read; aggregate/configuration output only.
DO $install$
DECLARE installation_schema TEXT := current_schema();
BEGIN
    EXECUTE format($definition$
CREATE FUNCTION %1$I.vigilo_finalization_backlog()
RETURNS TABLE(candidate_count bigint, oldest_candidate_lag_seconds bigint)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
WITH candidates AS (
            SELECT r.updated_at
            FROM %1$I.runs r
            WHERE r.status IN ('running'::%1$I.run_status, 'finalizing'::%1$I.run_status)
          AND r.execution_control = 'enabled'
              AND (
                  r.status <> 'finalizing'::%1$I.run_status
                  OR r.coordinator_leased_until IS NULL
                  OR r.coordinator_leased_until < now()
              )
              AND EXISTS (
                  SELECT 1
                  FROM %1$I.run_shard_dispatch_cursors c
                  WHERE c.run_id = r.id
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM %1$I.run_shard_dispatch_cursors c
                  WHERE c.run_id = r.id
                    AND c.status = 'open'
              )
        )
        SELECT
            COUNT(*)::bigint AS candidate_count,
            FLOOR(EXTRACT(EPOCH FROM now() - MIN(updated_at)))::bigint
                AS oldest_candidate_lag_seconds
        FROM candidates
$function$;
$definition$, installation_schema);
    EXECUTE format('REVOKE ALL ON FUNCTION %I.vigilo_finalization_backlog() FROM PUBLIC', installation_schema);
END;
$install$;
