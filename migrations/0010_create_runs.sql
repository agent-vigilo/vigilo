CREATE TABLE runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- external/run-level identity
    run_key TEXT NOT NULL UNIQUE,

    -- human/lookup metadata
    name TEXT,
    description TEXT,

    -- what is being evaluated
    dataset_id UUID NOT NULL,
    dataset_version_id UUID NOT NULL REFERENCES dataset_versions(dataset_version_id),
    dataset_version TEXT NOT NULL,
    evaluation_profile_id TEXT NOT NULL,
    evaluation_profile_version TEXT NOT NULL,
    profile_version_id TEXT NOT NULL,
    profile_hash TEXT NOT NULL,
    aggregation_policy_id TEXT NOT NULL,
    aggregation_policy_version TEXT NOT NULL,
    aggregation_policy_hash TEXT NOT NULL,

    -- agent + prompt/config identity
    agent_provider TEXT NOT NULL,
    agent_name TEXT NOT NULL,
    agent_version TEXT,
    prompt_config_id TEXT NOT NULL,
    prompt_config_version TEXT NOT NULL,

    -- compact config snapshot for reproducibility
    config_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- orchestration state
    status run_status NOT NULL DEFAULT 'pending',
    gate_status gate_status NOT NULL DEFAULT 'unknown',

    -- optional run-scoped coordinator ownership
    coordinator_id UUID,
    coordinator_leased_until TIMESTAMPTZ,
    coordinator_heartbeat_at TIMESTAMPTZ,

    -- counts materialized during finalization; source of truth is execution state
    expected_execution_count INTEGER NOT NULL DEFAULT 0 CHECK (expected_execution_count >= 0),
    terminal_execution_count INTEGER NOT NULL DEFAULT 0 CHECK (terminal_execution_count >= 0),
    passed_execution_count INTEGER NOT NULL DEFAULT 0 CHECK (passed_execution_count >= 0),
    failed_execution_count INTEGER NOT NULL DEFAULT 0 CHECK (failed_execution_count >= 0),
    errored_execution_count INTEGER NOT NULL DEFAULT 0 CHECK (errored_execution_count >= 0),

    -- summary payload for quick read/display
    summary JSONB NOT NULL DEFAULT '{}'::jsonb,

    error_message TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    dispatched_at TIMESTAMPTZ,
    finalized_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_runs_id_dataset_version_id UNIQUE (id, dataset_version_id)
);

CREATE INDEX idx_runs_status ON runs(status);

CREATE INDEX idx_runs_status_created_at ON runs(status, created_at);

CREATE INDEX idx_runs_status_updated_at ON runs(status, updated_at);

CREATE INDEX idx_runs_gate_status ON runs(gate_status);

CREATE INDEX idx_runs_dataset ON runs(dataset_id, dataset_version);

CREATE INDEX idx_runs_dataset_version_id ON runs(dataset_version_id);

CREATE INDEX idx_runs_coordinator_lease ON runs(coordinator_leased_until)
    WHERE status IN ('creating', 'pending', 'running', 'finalizing');

CREATE INDEX idx_runs_creating_recovery ON runs(updated_at,id) WHERE status='creating';

COMMENT ON TABLE runs IS
    'Represents a single evaluation run against a versioned agent target. A run defines the dataset, evaluation profile, and aggregation policy, and tracks lifecycle state from creation through finalization and gate decision.';

COMMENT ON COLUMN runs.run_key IS
    'External identifier for the run. Must be unique and is typically used for idempotent creation or integration with external systems.';

COMMENT ON COLUMN runs.name IS
    'Optional human-readable name for the run.';

COMMENT ON COLUMN runs.description IS
    'Optional description providing context about the run purpose or configuration.';

COMMENT ON COLUMN runs.dataset_id IS
    'Identifier of the dataset used to generate executions for this run.';

COMMENT ON COLUMN runs.dataset_version_id IS
    'Identifier for the dataset version, used for internal tracking.';

COMMENT ON COLUMN runs.dataset_version IS
    'Version of the dataset to ensure reproducibility of test cases.';

COMMENT ON COLUMN runs.evaluation_profile_id IS
    'Identifier of the evaluation profile defining which evaluators are applied.';

COMMENT ON COLUMN runs.evaluation_profile_version IS
    'Version of the evaluation profile to ensure consistent evaluator configuration.';

COMMENT ON COLUMN runs.profile_version_id IS
    'Identifier for the profile version, used for internal tracking.';

COMMENT ON COLUMN runs.profile_hash IS
    'Hash of the profile configuration for integrity verification.';

COMMENT ON COLUMN runs.aggregation_policy_id IS
    'Identifier of the aggregation policy used to combine evaluator results into scores and gate decisions.';

COMMENT ON COLUMN runs.aggregation_policy_version IS
    'Version of the aggregation policy to ensure consistent scoring and gating behavior.';

COMMENT ON COLUMN runs.aggregation_policy_hash IS
    'Hash of the aggregation policy configuration for integrity verification.';

COMMENT ON COLUMN runs.agent_provider IS
    'Provider or platform of the evaluated target (e.g., OpenAI, internal service).';

COMMENT ON COLUMN runs.agent_name IS
    'Logical name of the evaluated agent or model. Represents the target under evaluation.';

COMMENT ON COLUMN runs.agent_version IS
    'Version or deployment identifier of the evaluated target. Used to distinguish different releases of the same agent.';

COMMENT ON COLUMN runs.prompt_config_id IS
    'Identifier of the agent configuration used for this run, including prompts, system instructions, and runtime settings.';

COMMENT ON COLUMN runs.prompt_config_version IS
    'Version of the agent configuration used for this run. Ensures reproducibility across prompt and configuration changes.';

COMMENT ON COLUMN runs.config_snapshot IS
    'Compact frozen configuration snapshot capturing profile and durable dataset references needed for reproducibility and auditability.';

COMMENT ON COLUMN runs.status IS
    'Lifecycle state of the run. Creating is durable but non-dispatchable; pending begins only after all execution placements are seeded.';

COMMENT ON COLUMN runs.gate_status IS
    'Final evaluation decision derived from execution results and aggregation policy. Independent of run lifecycle state.';

COMMENT ON COLUMN runs.coordinator_id IS
    'Identifier of the process currently responsible for coordinating this run. Used in distributed execution environments.';

COMMENT ON COLUMN runs.coordinator_leased_until IS
    'Lease expiration timestamp for the coordinator. Used to detect abandoned or stalled runs.';

COMMENT ON COLUMN runs.coordinator_heartbeat_at IS
    'Last heartbeat timestamp from the coordinator process, used for liveness detection.';

COMMENT ON COLUMN runs.expected_execution_count IS
    'Total number of executions generated for this run. Used to determine completeness.';

COMMENT ON COLUMN runs.terminal_execution_count IS
    'Number of executions that reached a terminal state, materialized when the run is finalized.';

COMMENT ON COLUMN runs.passed_execution_count IS
    'Number of finalized executions that passed according to aggregation policy.';

COMMENT ON COLUMN runs.failed_execution_count IS
    'Number of finalized executions that failed according to aggregation policy.';

COMMENT ON COLUMN runs.errored_execution_count IS
    'Number of finalized executions that encountered system or evaluation errors.';

COMMENT ON COLUMN runs.summary IS
    'Aggregated run-level summary including metrics, scores, and dimension breakdowns derived from execution aggregates.';

COMMENT ON COLUMN runs.error_message IS
    'Error message associated with a run-level failure, if applicable.';

COMMENT ON COLUMN runs.created_at IS
    'Timestamp when the run was created.';

COMMENT ON COLUMN runs.started_at IS
    'Timestamp when execution processing began.';

COMMENT ON COLUMN runs.dispatched_at IS
    'Timestamp when executions were dispatched to workers.';

COMMENT ON COLUMN runs.finalized_at IS
    'Timestamp when run aggregation and finalization logic completed.';

COMMENT ON COLUMN runs.completed_at IS
    'Timestamp when the run reached a terminal state.';

COMMENT ON COLUMN runs.updated_at IS
    'Timestamp of the last update to the run record.';
