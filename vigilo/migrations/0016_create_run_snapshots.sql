CREATE TABLE run_snapshots (
    run_id UUID NOT NULL,
    run_shard SMALLINT NOT NULL CHECK (run_shard >= 0 AND run_shard < 128),
    run_key TEXT NOT NULL,
    dataset_id UUID NOT NULL,
    dataset_version_id UUID NOT NULL,
    dataset_version TEXT NOT NULL,
    evaluation_profile_id TEXT NOT NULL,
    evaluation_profile_version TEXT NOT NULL,
    profile_version_id TEXT NOT NULL,
    profile_hash TEXT NOT NULL,
    aggregation_policy_id TEXT NOT NULL,
    aggregation_policy_version TEXT NOT NULL,
    aggregation_policy_hash TEXT NOT NULL,
    agent_provider TEXT NOT NULL,
    agent_name TEXT NOT NULL,
    agent_version TEXT,
    prompt_config_id TEXT NOT NULL,
    prompt_config_version TEXT NOT NULL,
    config_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    expected_execution_count INTEGER NOT NULL DEFAULT 0 CHECK (expected_execution_count >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT pk_run_snapshots PRIMARY KEY (run_id, run_shard)
);

CREATE INDEX idx_run_snapshots_run
    ON run_snapshots(run_id);

COMMENT ON TABLE run_snapshots IS
    'Immutable run context prepared in execution storage before a coordinator dispatches worker-visible chunk events for a run shard.';

COMMENT ON COLUMN run_snapshots.run_id IS
    'Authoritative control-plane run id copied into execution storage for local worker queries.';

COMMENT ON COLUMN run_snapshots.run_shard IS
    'Logical shard this local run snapshot applies to.';

COMMENT ON COLUMN run_snapshots.run_key IS
    'Stable external run key copied from the control-plane run row.';

COMMENT ON COLUMN run_snapshots.dataset_version_id IS
    'Dataset version used by local run chunks in this execution placement.';

COMMENT ON COLUMN run_snapshots.config_snapshot IS
    'Verified immutable plan references, storage binding and limits, exact settlement manifest, and bounded gate policy/counts needed locally without reading control storage.';

COMMENT ON COLUMN run_snapshots.expected_execution_count IS
    'Expected execution count for this run shard in this execution placement.';
