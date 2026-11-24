CREATE INDEX idx_database_placements_serviceable_alias
    ON database_placements (alias)
    WHERE status IN ('active', 'draining');
