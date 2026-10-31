CREATE TABLE outbox_delivery_queue (
    event_id UUID NOT NULL REFERENCES outbox_events(id) ON DELETE CASCADE,
    claim_shard SMALLINT NOT NULL CHECK (claim_shard >= 0 AND claim_shard < 64),

    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claim_token UUID,
    claimed_until TIMESTAMPTZ,
    publish_attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (publish_attempt_count >= 0),
    error_message TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT pk_outbox_delivery_queue PRIMARY KEY (claim_shard, event_id)
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
    INSERT INTO outbox_delivery_queue (event_id, claim_shard)
    VALUES (
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
