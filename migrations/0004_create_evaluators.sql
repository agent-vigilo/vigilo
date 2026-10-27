CREATE TABLE evaluators (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- logical identity
    namespace TEXT NOT NULL,
    name TEXT NOT NULL,
    version TEXT NOT NULL,

    -- content identity
    content_hash TEXT NOT NULL,

    -- wasm artifact
    wasm_bytes BYTEA NOT NULL,
    wasm_size_bytes BIGINT NOT NULL CHECK (wasm_size_bytes >= 0),

    -- interface/runtime compatibility metadata
    interface_name TEXT NOT NULL CHECK (btrim(interface_name) <> ''),
    interface_version TEXT NOT NULL CHECK (btrim(interface_version) <> ''),
    wit_world TEXT NOT NULL CHECK (btrim(wit_world) <> ''),
    abi_contract_hash TEXT NOT NULL CHECK (btrim(abi_contract_hash) <> ''),
    abi_adapter TEXT NOT NULL CHECK (btrim(abi_adapter) <> ''),
    runtime TEXT NOT NULL,
    runtime_version TEXT NOT NULL,
    runtime_fingerprint TEXT NOT NULL,

    -- optional descriptive metadata
    description TEXT,
    tags JSONB NOT NULL DEFAULT '[]'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- lifecycle
    state evaluator_state NOT NULL DEFAULT 'active',
    state_reason TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- one published semantic version per namespace/name
    CONSTRAINT uq_evaluators_namespace_name_version
        UNIQUE (namespace, name, version),

    -- prevent duplicate wasm content within a namespace
    CONSTRAINT uq_evaluators_namespace_hash
        UNIQUE (namespace, content_hash)
);

CREATE INDEX idx_evaluators_namespace_name
    ON evaluators(namespace, name);

CREATE INDEX idx_evaluators_namespace_state
    ON evaluators(namespace, state);

CREATE INDEX idx_evaluators_interface
    ON evaluators(interface_name, interface_version);

COMMENT ON TABLE evaluators IS
    'Registry of versioned evaluator artifacts. Stores WASM binaries and associated metadata used by workers to load and execute evaluators at runtime.';

COMMENT ON COLUMN evaluators.id IS
    'Unique identifier for the evaluator.';

COMMENT ON COLUMN evaluators.namespace IS
    'Logical ownership boundary for evaluators, typically a project, team, or organization namespace.';

COMMENT ON COLUMN evaluators.name IS
    'Evaluator name within a namespace. Combined with namespace and version to form a human-readable evaluator identity.';

COMMENT ON COLUMN evaluators.version IS
    'Version label for the evaluator artifact. Intended for human-readable release/version tracking.';

COMMENT ON COLUMN evaluators.content_hash IS
    'Stable hash of the evaluator WASM artifact used for deduplication and integrity checks. Unique within a namespace.';

COMMENT ON COLUMN evaluators.wasm_bytes IS
    'Raw WASM artifact bytes loaded by the runtime for evaluator execution.';

COMMENT ON COLUMN evaluators.wasm_size_bytes IS
    'Size of the stored WASM artifact in bytes.';

COMMENT ON COLUMN evaluators.interface_name IS
    'Verified evaluator package and interface implemented by the artifact.';

COMMENT ON COLUMN evaluators.interface_version IS
    'Verified immutable evaluator ABI version implemented by the artifact.';

COMMENT ON COLUMN evaluators.wit_world IS
    'Verified WIT world used to instantiate the evaluator artifact.';

COMMENT ON COLUMN evaluators.abi_contract_hash IS
    'BLAKE3 hash of the immutable host-supported WIT contract verified during publication.';

COMMENT ON COLUMN evaluators.abi_adapter IS
    'Versioned host adapter used to instantiate and translate this evaluator ABI.';

COMMENT ON COLUMN evaluators.runtime IS
    'Expected runtime family used to execute the evaluator artifact, such as wasmtime.';

COMMENT ON COLUMN evaluators.runtime_version IS
    'Expected runtime version or compatibility band for the evaluator artifact.';

COMMENT ON COLUMN evaluators.runtime_fingerprint IS
    'Exact runtime compatibility fingerprint used to ensure the evaluator artifact is executed by a compatible host runtime.';

COMMENT ON COLUMN evaluators.description IS
    'Optional human-readable description of the evaluator.';

COMMENT ON COLUMN evaluators.tags IS
    'Optional list of tags used for discovery, grouping, or filtering evaluators.';

COMMENT ON COLUMN evaluators.metadata IS
    'Extensible JSON metadata for evaluator-specific annotations, capabilities, or publishing information.';

COMMENT ON COLUMN evaluators.state IS
    'Evaluator lifecycle state (active, yanked, deprecated, disabled, removed) aligned to package-management semantics.';

COMMENT ON COLUMN evaluators.state_reason IS
    'Optional user-provided reason for the current evaluator state.';

COMMENT ON COLUMN evaluators.created_at IS
    'Timestamp when the evaluator was created.';

COMMENT ON COLUMN evaluators.updated_at IS
    'Timestamp of the last update to the evaluator.';
