ALTER TABLE shard_rebalance_operations
    DROP CONSTRAINT shard_rebalance_operations_status_check,
    ADD COLUMN library_max_items INTEGER,
    ADD COLUMN planning_state JSONB,
    ADD CONSTRAINT shard_rebalance_operations_status_check
        CHECK (status IN ('planning', 'planned', 'running', 'completed', 'cancelled', 'failed')),
    ADD CONSTRAINT chk_shard_rebalance_planning_state
        CHECK ((status = 'planning') = (planning_state IS NOT NULL)),
    ADD CONSTRAINT chk_shard_rebalance_library_limit
        CHECK (
            (library_max_items IS NULL AND status <> 'planning')
            OR (library_max_items IS NOT NULL AND library_max_items BETWEEN 1 AND 256
                AND planned_item_count <= library_max_items)
        );

ALTER TABLE shard_rebalance_items ADD COLUMN move_operation_id UUID;

CREATE TABLE shard_rebalance_plan_placements (
    operation_id UUID NOT NULL REFERENCES shard_rebalance_operations(id) ON DELETE CASCADE,
    database_alias TEXT NOT NULL,
    eligible_count BIGINT NOT NULL DEFAULT 0 CHECK (eligible_count >= 0),
    PRIMARY KEY (operation_id, database_alias)
);

CREATE INDEX idx_shard_placements_rebalance_page
    ON shard_placements(database_alias, run_id, run_shard)
    INCLUDE (route_version)
    WHERE status = 'active';

CREATE INDEX idx_database_placements_rebalance_page
    ON database_placements(alias)
    WHERE status = 'active' AND role IN ('shard', 'control_and_shard');

COMMENT ON COLUMN shard_rebalance_operations.planning_state IS
    'Durable bounded library planning cursor. Items become claimable when planning is sealed.';
COMMENT ON COLUMN shard_rebalance_items.move_operation_id IS
    'Stable single-shard move identity carried across library apply claims and retries.';
COMMENT ON TABLE shard_rebalance_plan_placements IS
    'Per-placement counts accumulated in bounded pages for fill-target plans.';
