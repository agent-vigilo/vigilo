CREATE TABLE run_chunks (
    id UUID NOT NULL,
    run_id UUID NOT NULL,
    run_shard SMALLINT NOT NULL CHECK (run_shard >= 0 AND run_shard < 128),
    dataset_version_id UUID NOT NULL,
    profile_group_id TEXT NOT NULL,
    ordinal_start INTEGER NOT NULL CHECK (ordinal_start >= 0),
    ordinal_end INTEGER NOT NULL CHECK (ordinal_end > ordinal_start),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'leased', 'completed', 'failed', 'cancelled')),
    dispatched_at TIMESTAMPTZ,
    lease_token UUID,
    leased_until TIMESTAMPTZ,
    recovery_count INTEGER NOT NULL DEFAULT 0 CHECK (recovery_count >= 0),
    last_recovered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT fk_run_chunks_run_dataset
        FOREIGN KEY (run_id, dataset_version_id)
        REFERENCES runs(id, dataset_version_id)
        ON DELETE CASCADE,

    CONSTRAINT chk_run_chunks_lease_state CHECK (
        (status = 'leased' AND lease_token IS NOT NULL AND leased_until IS NOT NULL)
        OR (status <> 'leased' AND lease_token IS NULL AND leased_until IS NULL)
    ),
    CONSTRAINT pk_run_chunks PRIMARY KEY (run_id, run_shard, id)
) PARTITION BY LIST (run_shard);

DO $$
DECLARE
    partition_index INTEGER;
BEGIN
    FOR partition_index IN 0..127 LOOP
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF run_chunks FOR VALUES IN (%s)',
            'run_chunks_p' || lpad(partition_index::text, 3, '0'),
            partition_index
        );
    END LOOP;
END $$;

CREATE INDEX idx_run_chunks_run_status_leased_until
    ON run_chunks(run_id, run_shard, status, leased_until);

CREATE INDEX idx_run_chunks_planned_order
    ON run_chunks(run_id, run_shard, ordinal_start, id);

CREATE INDEX idx_run_chunks_undispatched
    ON run_chunks(run_id, run_shard, ordinal_start, id)
    WHERE status = 'pending' AND dispatched_at IS NULL;

CREATE INDEX idx_run_chunks_expired_leases
    ON run_chunks(leased_until, run_id, run_shard, recovery_count)
    WHERE status = 'leased';

COMMENT ON TABLE run_chunks IS
    'Chunk-level scheduling units for run processing, list partitioned by run_shard so large runs can spread across 128 logical shards while workers stay chunk-local.';

COMMENT ON COLUMN run_chunks.id IS
    'Unique chunk identifier used in work dispatch and worker claiming.';

COMMENT ON COLUMN run_chunks.run_id IS
    'Owning run for this chunk.';

COMMENT ON COLUMN run_chunks.run_shard IS
    'Logical shard for this chunk. All execution, attempt, aggregate, and evaluator-result rows produced from the chunk carry the same shard key.';

COMMENT ON COLUMN run_chunks.dataset_version_id IS
    'Dataset version identifier used to resolve chunk case membership.';

COMMENT ON COLUMN run_chunks.profile_group_id IS
    'Chunk scheduling label. Per-case profile group routing is resolved from case_blobs.case_group and run profile matching during worker execution.';

COMMENT ON COLUMN run_chunks.ordinal_start IS
    'Inclusive starting dataset ordinal for this chunk.';

COMMENT ON COLUMN run_chunks.ordinal_end IS
    'Exclusive ending dataset ordinal for this chunk.';

COMMENT ON COLUMN run_chunks.status IS
    'Chunk processing lifecycle status.';

COMMENT ON COLUMN run_chunks.dispatched_at IS
    'Timestamp when the coordinator first made this chunk visible to workers by enqueueing a run.chunk.ready event. NULL means the chunk has not been dispatched yet.';

COMMENT ON COLUMN run_chunks.lease_token IS
    'Opaque claim-generation token. Heartbeats extend the deadline without changing this token; recovery or reassignment replaces it so stale workers cannot write.';

COMMENT ON COLUMN run_chunks.leased_until IS
    'Lease expiration timestamp for worker ownership of this chunk.';

COMMENT ON COLUMN run_chunks.recovery_count IS
    'Number of times an expired worker lease has been recovered and requeued by the coordinator.';

COMMENT ON COLUMN run_chunks.last_recovered_at IS
    'Timestamp of the most recent coordinator recovery for an expired worker lease.';

COMMENT ON COLUMN run_chunks.created_at IS
    'Timestamp when this chunk was created.';

COMMENT ON COLUMN run_chunks.updated_at IS
    'Timestamp of the last state update for this chunk.';

COMMENT ON INDEX idx_run_chunks_expired_leases IS
    'Hot index for coordinator recovery scans over expired leased chunks.';
