CREATE TABLE run_chunk_settlements (
    run_id UUID NOT NULL,
    run_shard SMALLINT NOT NULL,
    chunk_id UUID NOT NULL,
    ordinal_start INTEGER NOT NULL CHECK (ordinal_start >= 0),
    ordinal_end INTEGER NOT NULL CHECK (ordinal_end > ordinal_start),
    aggregation_policy_hash TEXT NOT NULL CHECK (btrim(aggregation_policy_hash) <> ''),
    disposition TEXT NOT NULL CHECK (disposition IN ('completed', 'failed', 'cancelled')),
    summary JSONB NOT NULL CHECK (jsonb_typeof(summary) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, run_shard, chunk_id),
    FOREIGN KEY (run_id, run_shard, chunk_id)
        REFERENCES run_chunks(run_id, run_shard, id) ON DELETE CASCADE,
    CHECK (octet_length(summary::text) <= 1048576)
);

CREATE INDEX idx_chunk_settlements_order
    ON run_chunk_settlements(run_id, run_shard, ordinal_start, chunk_id);

CREATE TABLE run_chunk_settlement_consumptions (
    run_id UUID NOT NULL,
    run_shard SMALLINT NOT NULL,
    chunk_id UUID NOT NULL,
    ordinal_start INTEGER NOT NULL CHECK (ordinal_start >= 0),
    consumed BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (run_id, run_shard, chunk_id),
    FOREIGN KEY (run_id, run_shard, chunk_id)
        REFERENCES run_chunk_settlements(run_id, run_shard, chunk_id) ON DELETE CASCADE
);

CREATE INDEX idx_pending_chunk_settlement_consumptions
    ON run_chunk_settlement_consumptions(run_id, run_shard, ordinal_start, chunk_id)
    WHERE NOT consumed;

CREATE TRIGGER capture_run_chunk_settlement_consumptions_changes
AFTER INSERT OR UPDATE OR DELETE ON run_chunk_settlement_consumptions
FOR EACH ROW EXECUTE FUNCTION record_shard_move_dirty_key(
    'run_chunk_settlement_consumptions', 'run_id', 'run_shard', 'chunk_id'
);

COMMENT ON TABLE run_chunk_settlement_consumptions IS
    'Pending receipt work created at settlement; consumption commits atomically with progress counters. Final scores are folded separately in canonical planned order.';

CREATE FUNCTION protect_chunk_settlement() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'chunk settlement is immutable for run %, shard %, chunk %',
            OLD.run_id, OLD.run_shard, OLD.chunk_id;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER protect_chunk_settlement
BEFORE UPDATE ON run_chunk_settlements
FOR EACH ROW EXECUTE FUNCTION protect_chunk_settlement();

CREATE TRIGGER capture_run_chunk_settlements_changes
AFTER INSERT OR UPDATE OR DELETE ON run_chunk_settlements
FOR EACH ROW EXECUTE FUNCTION record_shard_move_dirty_key(
    'run_chunk_settlements', 'run_id', 'run_shard', 'chunk_id'
);

COMMENT ON TABLE run_chunk_settlements IS
    'Immutable terminal chunk receipts, committed with writer fencing and retained evidence; coordinators fold them in planned ordinal order.';

CREATE FUNCTION protect_settled_chunk_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.id, NEW.run_id, NEW.run_shard, NEW.ordinal_start, NEW.ordinal_end, NEW.status)
        IS DISTINCT FROM
       (OLD.id, OLD.run_id, OLD.run_shard, OLD.ordinal_start, OLD.ordinal_end, OLD.status)
       AND EXISTS (SELECT 1 FROM run_chunk_settlements
                   WHERE run_id = OLD.run_id AND run_shard = OLD.run_shard AND chunk_id = OLD.id) THEN
        RAISE EXCEPTION 'settled chunk identity and disposition are immutable for chunk %', OLD.id;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER protect_settled_chunk_identity
BEFORE UPDATE ON run_chunks
FOR EACH ROW EXECUTE FUNCTION protect_settled_chunk_identity();
