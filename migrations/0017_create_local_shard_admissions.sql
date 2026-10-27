CREATE TABLE local_shard_admissions (
    run_id UUID NOT NULL,
    run_shard SMALLINT NOT NULL CHECK (run_shard >= 0 AND run_shard < 128),
    database_alias TEXT NOT NULL,
    write_epoch BIGINT NOT NULL CHECK (write_epoch > 0),
    state TEXT NOT NULL CHECK (state IN ('open', 'draining', 'prepared', 'closed')),
    redirect_database_alias TEXT,
    move_id UUID,
    move_claim_generation BIGINT CHECK (move_claim_generation > 0),
    move_claim_token UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT pk_local_shard_admissions PRIMARY KEY (run_id, run_shard),
    CONSTRAINT ck_local_shard_admissions_redirect CHECK (
        redirect_database_alias IS NULL
        OR redirect_database_alias <> database_alias
    ),
    CONSTRAINT ck_local_shard_admissions_move_fence CHECK (
        (
            state = 'prepared'
            AND move_id IS NOT NULL
            AND move_claim_generation IS NOT NULL
            AND move_claim_token IS NOT NULL
        )
        OR (
            state <> 'prepared'
            AND move_id IS NULL
            AND move_claim_generation IS NULL
            AND move_claim_token IS NULL
        )
    )
);

COMMENT ON TABLE local_shard_admissions IS
    'Execution-local write authority for one run shard. Runtime writes validate this row in their local transaction; control metadata remains the desired topology.';

COMMENT ON COLUMN local_shard_admissions.run_id IS
    'Run whose logical shard is protected by this execution-database admission row.';

COMMENT ON COLUMN local_shard_admissions.run_shard IS
    'Logical shard number within the run. The primary key permits one local authority record per run shard in this physical database.';

COMMENT ON COLUMN local_shard_admissions.database_alias IS
    'Configured alias this physical database must represent for the admission to match a route hint. It is stored locally without depending on the control routing catalog.';

COMMENT ON COLUMN local_shard_admissions.write_epoch IS
    'Execution ownership generation accepted by this database. Older route hints cannot authorize writes after ownership changes.';

COMMENT ON COLUMN local_shard_admissions.state IS
    'Local admission state: open accepts claims and settlement, draining accepts settlement only, and prepared or closed reject runtime writes. The move workflow may populate a prepared target under its exclusive fence.';

COMMENT ON COLUMN local_shard_admissions.redirect_database_alias IS
    'Informational move counterpart for diagnostics and stale-route errors. Workers still refresh authoritative routing from the control database.';

COMMENT ON COLUMN local_shard_admissions.move_id IS
    'Soft reference to the control-owned move operation preparing this target. Cross-database placement prevents a physical foreign key.';

COMMENT ON COLUMN local_shard_admissions.move_claim_generation IS
    'Monotonic move claimant generation installed on this prepared target.';

COMMENT ON COLUMN local_shard_admissions.move_claim_token IS
    'Opaque token authorizing target reset, copy, and replay writes for the installed move generation.';

COMMENT ON COLUMN local_shard_admissions.created_at IS
    'Time this physical database first recorded local authority for the run shard.';

COMMENT ON COLUMN local_shard_admissions.updated_at IS
    'Time the local alias, epoch, state, or redirect metadata last changed.';

COMMENT ON CONSTRAINT pk_local_shard_admissions ON local_shard_admissions IS
    'Ensures one locally enforced admission state for each run_id and run_shard pair in this physical database.';

COMMENT ON CONSTRAINT ck_local_shard_admissions_redirect ON local_shard_admissions IS
    'Prevents a local admission from redirecting stale work back to the same configured database alias.';

COMMENT ON CONSTRAINT ck_local_shard_admissions_move_fence ON local_shard_admissions IS
    'Prepared targets require complete move authority; runtime admissions cannot retain mover authority.';
