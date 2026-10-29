CREATE TABLE shard_move_operations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL,
    run_shard SMALLINT NOT NULL CHECK (run_shard >= 0 AND run_shard < 128),
    source_database_alias TEXT NOT NULL REFERENCES database_placements(alias),
    target_database_alias TEXT NOT NULL REFERENCES database_placements(alias),
    starting_route_version BIGINT NOT NULL CHECK (starting_route_version > 0),
    status TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'completed', 'aborted', 'failed')),
    phase TEXT NOT NULL DEFAULT 'reserved'
        CHECK (phase IN (
            'reserved', 'backfill', 'catch_up', 'draining', 'cutover',
            'completed', 'aborted', 'failed'
        )),
    target_reset_at TIMESTAMPTZ,
    copied_row_count BIGINT NOT NULL DEFAULT 0 CHECK (copied_row_count >= 0),
    copied_byte_count BIGINT NOT NULL DEFAULT 0 CHECK (copied_byte_count >= 0),
    error_message TEXT,
    claim_generation BIGINT NOT NULL DEFAULT 0 CHECK (claim_generation >= 0),
    claim_token UUID,
    claimed_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,

    CONSTRAINT ck_shard_move_operation_aliases
        CHECK (source_database_alias <> target_database_alias),
    CONSTRAINT ck_shard_move_operation_claim
        CHECK (
            (claim_token IS NULL AND claimed_until IS NULL)
            OR (claim_token IS NOT NULL AND claimed_until IS NOT NULL)
        )
);

CREATE UNIQUE INDEX uq_shard_move_operations_active_shard
    ON shard_move_operations(run_id, run_shard)
    WHERE status = 'active';

CREATE INDEX idx_shard_move_operations_claim
    ON shard_move_operations(status, claimed_until, created_at)
    WHERE status = 'active';

COMMENT ON TABLE shard_move_operations IS
    'Control-plane state for one resumable online run-shard move.';

COMMENT ON COLUMN shard_move_operations.claim_generation IS
    'Monotonic claimant generation. Every successful move claim increments it so a stale process cannot reinstall older target authority.';
