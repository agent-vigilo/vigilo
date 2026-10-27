CREATE TABLE run_shard_dispatch_cursors (
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    run_shard SMALLINT NOT NULL CHECK (run_shard >= 0 AND run_shard < 128),
    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'drained')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT pk_run_shard_dispatch_cursors PRIMARY KEY (run_id, run_shard)
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
