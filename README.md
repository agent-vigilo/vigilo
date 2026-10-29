[![Build](https://github.com/agent-vigilo/vigilo/actions/workflows/build.yaml/badge.svg?branch=main)](https://github.com/agent-vigilo/vigilo/actions/workflows/build.yaml)
[![Coverage Status](https://codecov.io/gh/agent-vigilo/vigilo/graph/badge.svg?token=CL93O7DW9C)](https://codecov.io/gh/agent-vigilo/vigilo)
[![Crates.io](https://img.shields.io/crates/v/vigilo.svg)](https://crates.io/crates/vigilo)
[![Documentation](https://img.shields.io/badge/docs-agentvigilo.com-blue.svg)](https://agentvigilo.com/docs/guides/getting-started)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="web/static/img/logo-dark.svg" />
    <img src="web/static/img/logo.svg" alt="Agent Vigilo logo" width="160" height="95" />
  </picture>
</p>

# Agent Vigilo

**Distributed AI evaluation infrastructure and deployment gating for generative AI systems.**

Agent Vigilo provides versioned WASM evaluators, durable evaluation runs, worker/coordinator execution, normalized results, and pass/fail gates that can sit in CI or release workflows.

It focuses on the parts of AI evaluation that become hard as systems grow: idempotent distributed work, durable event delivery, evaluator isolation, retry-safe persistence, and auditable results.

## Why It Matters

- **Run evaluations like infrastructure**: Immutable datasets and run plans in object storage, PostgreSQL-backed state, RabbitMQ work distribution, and Rust workers.
- **Ship versioned evaluators**: publish WASI Preview 2 WebAssembly evaluators with strict WIT contracts.
- **Protect the runtime**: Wasmtime fuel, memory, timeout, log, and concurrency limits isolate evaluator execution.
- **Avoid lost events**: durable outbox ledger plus hot delivery queue, RabbitMQ publisher confirms, and idempotency keys.
- **Gate deployments**: turn evaluator measurements and host-owned profile policy into dimension scores, total aggregate scores, and reproducible release decisions.

## How Results Are Calculated

Each evaluator invocation returns one measurement or abstention plus optional diagnostics. The profile binding owns normalization, threshold, dimension, weight, requiredness, and blocking policy. Missing, errored, abstained, duplicated, or invalid required output produces an errored execution aggregate with no score. An execution passes when completeness is satisfied, `aggregate_score >= min_execution_score`, and no host-derived blocking result fails.

Run scorecard gates enforce dimension or evaluator thresholds across the full run, including configured case-group and tag slices. Shards publish bounded rollups; the coordinator merges those rollups and persists the authoritative scorecard without scanning execution rows centrally.

See [Measurement Normalization](web/docs/configuration/measurement-normalization.mdx) for copyable binary, numeric, curve, threshold, and ordinal policy examples.

A run completes after exact chunk settlement, including when retained results are partial. Missing coverage or failed evaluation policy fails the release gate. Immutable admission conflicts can mark the run failed before dispatch.

## Start Here

- [Getting started](https://agentvigilo.com/docs/guides/getting-started): run your first evaluation.
- [CLI reference](https://agentvigilo.com/docs/guides/cli-reference): canonical commands for evaluators, runs, databases, and rebalancing.
- [Architecture overview](https://agentvigilo.com/docs/architecture/structure/): containers, components, flows, and state diagrams.
- [Scale-out and shard migration](https://agentvigilo.com/docs/architecture/scaling): 128 logical run shards and expansion guidance.
- [Worker runtime](https://agentvigilo.com/docs/architecture/structure/components/worker/): chunk claiming, evaluator execution, and result persistence.
- [Runtime limits](https://agentvigilo.com/docs/configuration/runtime-limits): Wasm evaluator sandbox and worker concurrency controls.
- [Outbox lifecycle](https://agentvigilo.com/docs/architecture/state/outbox-lifecycle/): durable event publication and retry behavior.
- [Publishing evaluators](https://agentvigilo.com/docs/guides/publishing): build and publish versioned WASM evaluators.
- [Evaluator compatibility](https://agentvigilo.com/docs/guides/evaluator-compatibility): versioned WIT ABIs, host adapters, and immutable run pinning.

## Core Stack

Rust, Tokio, PostgreSQL, SQLx, RabbitMQ, S3-compatible object storage, Wasmtime, WASI Preview 2, WIT, Docusaurus.

## Development Checks

GitHub Actions is the source of truth for build verification. To install the optional local Git hooks:

```bash
chmod +x scripts/hooks/pre-commit scripts/hooks/pre-push
git config core.hooksPath scripts/hooks
```

### Test Tiers

| Tier | Scope | Required services | GitHub Actions job |
| --- | --- | --- | --- |
| Unit and contract | Service-free Rust behavior across the workspace | None | `Unit and Contract Tests` |
| Database integration | SQL, migrations, transactions, advisory locks, leases, and concurrency | One PostgreSQL server; `DATABASE_URL` must use a role that can create test databases | `Database Integration Tests` |
| Migration | Greenfield schema application through the CLI setup path | One fresh PostgreSQL database | `Migration Tests` |
| End-to-end | Routing and the real distributed runtime across process and protocol boundaries | Two PostgreSQL servers, RabbitMQ, S3-compatible object storage, evaluator Wasm, and the test HTTP agent | `End-to-End Tests` |
| Blob storage integration | Signed object I/O, transport faults, upload/run-plan recovery, and exact cleanup | PostgreSQL and disposable Ceph Object Gateway | `Blob Storage Integration Tests` |
| Performance service integration | Perf topology ownership, collector reset, and cleanup integration | Docker Engine with Compose; images are declared in `infra/performance/compose.yml` | `Performance Service Integration Tests` |
| Evaluator compatibility | Fixture-backed evaluator ABI compatibility | Built Wasm fixtures; no live services | `Evaluator Compatibility Tests` |
| Documentation | Website typecheck and production build | Node.js and installed website dependencies | `Documentation Checks` |

Run the service-free tier with:

```bash
cargo test --workspace --locked --lib --bins
```

The repository-local performance harness is a separate opt-in workflow. Its
static check remains service-free and validates that performance tooling stays
out of the shipped dependency graph:

```bash
cargo perf check
```

The explicit live-service integration tier is:

```bash
cargo test -p xtask --locked --features performance-services --test performance_services -- --nocapture
```

It is feature-gated and therefore does not expand normal workspace, database,
migration, or end-to-end test commands. Build, workload, artifact, and
isolation details are in [`performance/README.md`](performance/README.md).

The signed object-storage tier is also explicitly opt-in. Configure the endpoint,
bucket, and credentials using the [development storage instructions](infra/dev/README.md#local-object-storage), then run:

```bash
cargo test -p vigilo --locked --features blob-storage-services --test blob_storage_services -- --nocapture
```

This tier checks Ceph persistence and transport faults. Dataset ingestion also
uses a disposable PostgreSQL database initialized through `setup`:

```bash
cargo test -p vigilo --locked --features blob-storage-services --test dataset_ingestion_services -- --nocapture
```

The ingestion tier kills and resumes real upload and run-plan CLI processes,
checks frozen inputs and finalized reuse, verifies the dispatch boundary, and
checks exact cleanup. Both storage tiers contribute to the same aggregate
coverage report and 80% gate.

After setting `DATABASE_URL`, run every PostgreSQL-backed SQLx test with:

```bash
cargo test -p vigilo --locked --bin vigilo -- --ignored --nocapture --test-threads=4
```

Within the `vigilo` binary target, `#[ignore]` is reserved for this database
integration tier so the command remains complete.

The end-to-end tier runs `multi_database_routing` and `multi_database_e2e`
separately because they require the complete distributed dependency set. The
workflow builds the evaluator Wasm and supplies the required database,
messaging, storage-owner configuration (`VIGILO_STORAGE_CONFIG`), and opt-in environment variables.

The pre-commit hook runs nightly rustfmt only. The pre-push hook runs clippy, the service-free Rust tier, and the web typecheck. Database integration, blob storage integration, performance service integration, migration, end-to-end, evaluator compatibility, and documentation checks run as separate required CI jobs.

The unit and contract, performance service integration, database integration,
blob storage integration, and end-to-end jobs collect coverage in parallel. The
Linux unit pass includes the service-free performance contract check; Docker-backed
service ownership tests run in their own integration job. A final job merges
the five reports, requires at least 80% aggregate line
coverage, and uploads that report under the `rust-all` Codecov flag. Project
coverage and the repository badge therefore reflect code reached through unit,
integration, and distributed runtime tests.
Only PostgreSQL test fixture sources are excluded; production query and table
modules remain in the coverage denominator.

SQLx creates each isolated PostgreSQL test database with `CREATE DATABASE`,
which clones PostgreSQL's built-in `template1` database. The PostgreSQL coverage
job migrates `template1` once, so each test validates the recorded migrations
instead of rebuilding every partition and index. The separate migration job
applies the complete migration set to an empty database. The
[migration layout](migrations/README.md) documents dependency order and file ownership.

Accumulate the same reports locally after starting the services and setting the
environment variables required by each tier:

```bash
cargo llvm-cov clean --workspace
cargo llvm-cov --no-report --workspace --locked --lib --bins
cargo llvm-cov run --no-report -p xtask --locked -- perf check
cargo llvm-cov --no-report -p xtask --locked --features performance-services --test performance_services -- --nocapture
cargo llvm-cov --no-report -p vigilo --locked --features blob-storage-services --test blob_storage_services -- --nocapture
cargo llvm-cov --no-report -p vigilo --locked --features blob-storage-services --test dataset_ingestion_services -- --nocapture
cargo llvm-cov --no-report -p vigilo --locked --bin vigilo -- --ignored --nocapture --test-threads=4
cargo llvm-cov --no-report -p vigilo --locked --test multi_database_routing -- --nocapture
VIGILO_E2E_MULTI_DATABASE=1 cargo llvm-cov --no-report -p vigilo --locked --test multi_database_e2e -- --nocapture
cargo llvm-cov report --fail-under-lines 80 --ignore-filename-regex '[/\\]postgres_tests(\.rs|[/\\])' --lcov --output-path lcov.info
```

The integration commands require the database, messaging, evaluator Wasm, and
test agent dependencies described above. Configure the repository secret
`CODECOV_TOKEN` for the aggregate CI upload.

## Project Status

Agent Vigilo is an active systems project focused on reliable AI evaluation, LLM evaluation workflows, agent testing, and deployment gates. The implementation favors explicit contracts, durable state transitions, and operational diagrams over black-box orchestration.

## License

[Apache 2.0](LICENSE)
