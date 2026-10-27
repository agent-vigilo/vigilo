# Vigilo

Command Line Interface application for operating Agent Vigilo.

## Package Contents

Quick-search overview of the main modules under `vigilo/src`.

| Module | Summary |
| --- | --- |
| `main` | CLI binary entry point: argument parsing, logging setup, context creation, and exit codes. |
| `cli` | Clap application definition, global flags, and command dispatch. |
| `cli::commands` | Top-level command router for setup, dataset, evaluator, run, database, rebalance, coordinator, and worker commands. |
| `cli::commands::setup` | Applies database migrations and optionally publishes built-in evaluators. |
| `cli::commands::evaluators` | Publish, search, show, test, and state-management commands for evaluator registry entries. |
| `cli::commands::dataset` | Upload, resume, inspect, abort, and clean immutable dataset uploads. |
| `cli::commands::shard` | Database registration and lifecycle, routed shard inspection/movement, and rebalance commands. |
| `cli::commands::run` | Run creation, validation, status, watch, cancellation, result summary, and export commands. |
| `cli::commands::coordinator` | Coordinator process modes, orchestration cycle, and placement-scoped failure handling for creation recovery, dispatch, finalization, and outbox publishing. |
| `cli::commands::worker` | Worker process modes and chunk execution flow for agent calls and evaluator processing. |
| `cli::args` | Shared clap value parsers for existing files and directories. |
| `context` | Lazy process-wide service container for database, blob storage, HTTP, messaging, output, registry, and Wasm runtime. |
| `context::database` | Lifetime PostgreSQL connection configuration and lazy pools with live placement status and role admission. |
| `context::http` | Lazy shared `reqwest` client initialization. |
| `context::messaging` | Lazy RabbitMQ client initialization. |
| `context::output` | Structured stdout writer for JSON and TOON command output. |
| `context::registry` | In-memory cache for compiled evaluator Wasm components. |
| `context::storage` | Resolves configured storage owners and shares bounded adapter instances. |
| `context::wasm` | Wasmtime setup, evaluator artifact preparation, WIT mapping, and evaluator test execution. |
| `contracts` | Host-side runtime contracts for run inputs, evaluator I/O, evaluator refs, and aggregation. |
| `contracts::run` | Run profile, agent config, aggregation policy, and dataset input payloads. |
| `contracts::evaluator` | Canonical evaluator `input` and `output` payloads plus normalization helpers. |
| `contracts::evaluator_ref` | Parser for fully qualified evaluator ids: `<namespace>/<name>:<version>`. |
| `contracts::case_storage` | Immutable dataset/run-plan formats, object references, limits, encoding, and validation. |
| `contracts::aggregation` | Runtime aggregation policy for normalized evaluator findings. |
| `db` | Database access layer split into migrations, table helpers, and multi-table workflows. |
| `db::migrations` | SQL migration runner. |
| `db::tables` | Narrow row-oriented database helpers. |
| `db::workflows` | Transactional and concurrency-aware database operations for runs, chunks, executions, and cancellation. |
| `db::workflows::dataset_upload` | Upload leases, grouped file ownership/checkpoints, finalized catalog publication, and cleanup records. |
| `db::workflows::run_plan` | Run preparation ownership, grouped file metadata, shard totals, admission progress, and activation. |
| `db::workflows::run_creation` | Compact creation progress queries and shared recovery-reporting types. |
| `models` | Persistence model structs that mirror database rows and drafts. |
| `blob_storage` | Provider-neutral object I/O contract and bounded S3-compatible adapter. |
| `dataset_ingestion` | Bounded input parsing/spooling, immutable batch publication, resumable uploads, and exact cleanup. |
| `run_planning` | Verifies finalized datasets and publishes immutable configuration, chunk plans, and gate counts. |
| `run_admission` | Seeds and verifies execution-local metadata, recovers requested admission, and activates dispatch after exact coverage. |
| `case_reader` | Verifies worker assignments and complete touched case files, with bounded immutable-byte reuse. |
| `evaluator_abi` | Registry and adapters for evaluator identity, bindings, validation, mapping, and execution. |
| `agent_client` | Worker-side HTTP client for invoking configured agent endpoints and normalizing responses. |
| `evaluators` | Built-in evaluator discovery, build, bootstrap, and publishing workflows. |
| `manifest` | `Vigilo.toml` evaluator manifest parsing. |
| `mq` | RabbitMQ topology, publishing, worker consumption, retry, and quarantine handling. |
| `outbox` | Durable outbox event publication from database rows to the message broker. |
| `runtime` | Generic service runner for long-running coordinator and worker processes. |
