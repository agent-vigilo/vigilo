CREATE TABLE outbox_delivery_queue (
    tenant_id UUID NOT NULL DEFAULT NULLIF(current_setting('vigilo.tenant_id', true), '')::uuid,
    event_id UUID NOT NULL,
    claim_shard SMALLINT NOT NULL CHECK (claim_shard >= 0 AND claim_shard < 64),

    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claim_token UUID,
    claimed_until TIMESTAMPTZ,
    publish_attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (publish_attempt_count >= 0),
    error_message TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT pk_outbox_delivery_queue PRIMARY KEY (claim_shard, event_id),
    FOREIGN KEY (tenant_id, event_id) REFERENCES outbox_events(tenant_id, id) ON DELETE CASCADE
) PARTITION BY LIST (claim_shard);

DO $$
DECLARE
    shard_index INTEGER;
BEGIN
    FOR shard_index IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF outbox_delivery_queue FOR VALUES IN (%s)',
            'outbox_delivery_queue_s' || lpad(shard_index::text, 2, '0'),
            shard_index
        );
    END LOOP;
END $$;

CREATE INDEX idx_outbox_delivery_queue_available
    ON outbox_delivery_queue(claim_shard, available_at, event_id);

CREATE INDEX idx_outbox_delivery_queue_available_global
    ON outbox_delivery_queue(available_at, event_id);

CREATE INDEX idx_outbox_delivery_queue_claim_token
    ON outbox_delivery_queue(claim_shard, event_id, claim_token)
    WHERE claim_token IS NOT NULL;

CREATE FUNCTION enqueue_outbox_delivery()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO outbox_delivery_queue (tenant_id, event_id, claim_shard)
    VALUES (
        NEW.tenant_id,
        NEW.id,
        (get_byte(uuid_send(NEW.id), 15)::int % 64)::smallint
    )
    ON CONFLICT DO NOTHING;

    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_outbox_events_enqueue_delivery
AFTER INSERT ON outbox_events
FOR EACH ROW
EXECUTE FUNCTION enqueue_outbox_delivery();

COMMENT ON TABLE outbox_delivery_queue IS
    'Hot delivery queue for outbox events that still need publication or retry. Rows are deleted after confirmed broker publication.';

COMMENT ON INDEX idx_outbox_delivery_queue_available IS
    'Hot index for high-throughput outbox publishers claiming available delivery rows by shard and availability time.';

COMMENT ON INDEX idx_outbox_delivery_queue_available_global IS
    'Hot index for publisher scans ordered by availability time across delivery shards.';

COMMENT ON FUNCTION enqueue_outbox_delivery() IS
    'Creates the hot delivery-queue row for each newly inserted outbox ledger event inside the same transaction.';

COMMENT ON COLUMN outbox_delivery_queue.event_id IS
    'Reference to the durable outbox event payload and identity row.';

COMMENT ON COLUMN outbox_delivery_queue.claim_shard IS
    'Stable shard used to distribute hot delivery claims across queue partitions.';

COMMENT ON COLUMN outbox_delivery_queue.available_at IS
    'Timestamp indicating when the event is eligible for publication. Used for delayed delivery or retry backoff.';

COMMENT ON COLUMN outbox_delivery_queue.claim_token IS
    'Opaque token assigned when a publisher claims the delivery row. Mark-published and retry updates must present the same token so stale publishers cannot overwrite a newer claim.';

COMMENT ON COLUMN outbox_delivery_queue.claimed_until IS
    'Lease deadline for the current publisher claim.';

COMMENT ON COLUMN outbox_delivery_queue.publish_attempt_count IS
    'Number of times this delivery row has been claimed for publication.';

COMMENT ON COLUMN outbox_delivery_queue.error_message IS
    'Error message from the most recent failed publication attempt, if any.';

-- Restricted publisher claim; table references bind to the installation schema.
DO $install$
DECLARE installation_schema TEXT := current_schema();
BEGIN
    EXECUTE format($definition$
CREATE FUNCTION %1$I.vigilo_claim_outbox(p_limit bigint, p_lease_seconds integer)
RETURNS TABLE(
    id uuid, event_type text, aggregate_type text, aggregate_id uuid,
    dedupe_key text, payload jsonb, status text, claim_shard smallint,
    available_at timestamptz, claim_token uuid, claimed_until timestamptz,
    publish_attempt_count integer, published_at timestamptz, error_message text,
    created_at timestamptz, updated_at timestamptz, tenant_id uuid
)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
BEGIN
    IF p_limit IS NULL OR p_limit < 1 OR p_limit > 1000000
       OR p_lease_seconds IS NULL OR p_lease_seconds < 1 OR p_lease_seconds > 86400 THEN
        RAISE EXCEPTION 'invalid outbox claim limits' USING ERRCODE = '22023';
    END IF;
    RETURN QUERY
WITH claim AS (
            SELECT q.claim_shard, q.event_id
            FROM %1$I.outbox_delivery_queue q
            WHERE q.available_at <= now()
            ORDER BY q.available_at ASC, q.event_id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT p_limit
        ),
        leased AS (
            UPDATE %1$I.outbox_delivery_queue q
            SET claim_token = gen_random_uuid(),
                claimed_until = now() + (p_lease_seconds::int * interval '1 second'),
                available_at = now() + (p_lease_seconds::int * interval '1 second'),
                publish_attempt_count = q.publish_attempt_count + 1,
                updated_at = now()
            FROM claim
            WHERE q.claim_shard = claim.claim_shard
              AND q.event_id = claim.event_id
            RETURNING
                q.claim_shard,
                q.event_id,
                q.available_at,
                q.claim_token,
                q.claimed_until,
                q.publish_attempt_count,
                q.error_message
        )
        SELECT
            e.id,
            e.event_type,
            e.aggregate_type,
            e.aggregate_id,
            e.dedupe_key,
            e.payload,
            e.status::text as status,
            leased.claim_shard,
            leased.available_at,
            leased.claim_token,
            leased.claimed_until,
            leased.publish_attempt_count,
            e.published_at,
            COALESCE(leased.error_message, e.error_message) AS error_message,
            e.created_at,
            e.updated_at,
            e.tenant_id
        FROM leased
        JOIN %1$I.outbox_events e
          ON e.id = leased.event_id
        ORDER BY leased.available_at ASC, e.id ASC;
END;
$function$;
$definition$, installation_schema);
    EXECUTE format('REVOKE ALL ON FUNCTION %I.vigilo_claim_outbox(bigint,integer) FROM PUBLIC', installation_schema);
END;
$install$;

-- Restricted scheduler read; aggregate/configuration output only.
DO $install$
DECLARE installation_schema TEXT := current_schema();
BEGIN
    EXECUTE format($definition$
CREATE FUNCTION %1$I.vigilo_outbox_backlog()
RETURNS bigint
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
SELECT COUNT(*)::bigint
        FROM %1$I.outbox_delivery_queue
        WHERE available_at <= now()
$function$;
$definition$, installation_schema);
    EXECUTE format('REVOKE ALL ON FUNCTION %I.vigilo_outbox_backlog() FROM PUBLIC', installation_schema);
END;
$install$;
