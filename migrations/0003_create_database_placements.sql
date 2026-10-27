CREATE TABLE database_placements (
    alias TEXT PRIMARY KEY,
    database_url_env TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('control', 'shard', 'control_and_shard')),
    status TEXT NOT NULL CHECK (status IN ('provisioning', 'active', 'draining', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX uq_database_placements_single_active_control
    ON database_placements ((true))
    WHERE status = 'active'
      AND role IN ('control', 'control_and_shard');

INSERT INTO database_placements (alias, database_url_env, role, status)
VALUES ('primary', 'DATABASE_URL', 'control_and_shard', 'active');

COMMENT ON TABLE database_placements IS
    'Catalog of configured PostgreSQL database placements. A placement is a named PostgreSQL target; its alias is stable routing metadata and database_url_env names the environment variable containing the connection URL.';

COMMENT ON COLUMN database_placements.alias IS
    'Stable routing alias used by control-plane and shard-placement metadata. The default alias is primary.';

COMMENT ON COLUMN database_placements.database_url_env IS
    'Name of the environment variable containing the database URL for this placement.';

COMMENT ON COLUMN database_placements.role IS
    'Placement role. control stores authoritative run metadata and routing catalog rows, shard stores shard-local execution data, and control_and_shard stores both in the same database. Only one active control-capable placement is allowed.';

COMMENT ON COLUMN database_placements.status IS
    'Placement lifecycle. Provisioning placements await readiness verification, active placements may receive new ownership, draining placements serve existing ownership while it is evacuated, and disabled placements serve no runtime work.';

COMMENT ON INDEX uq_database_placements_single_active_control IS
    'Ensures there is at most one active placement with control-plane authority. Multiple active shard-only placements are allowed.';
