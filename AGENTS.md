# Agent Guidance

This file records Vigilo-specific constraints that are easy to miss. A nearer
`AGENTS.md` adds requirements for its subtree.

## Engineering Goals

- Build reliable, scalable, and maintainable behavior.
- Reliability requires explicit contracts, fail-closed validation, bounded work,
  deterministic cleanup, error context, and tested failure handling.
- Scalability requires batching, streaming, pagination, and bounded concurrency;
  avoid per-item I/O, full-data materialization, and global coordination.
- Maintainability requires cohesive modules, explicit dependency direction, one
  source of truth, a small public surface, and the simplest clear implementation.

## Working Method

- Leave all changes unstaged. Do not stage changes or create commits; the user
  handles every commit manually.
- Inspect the relevant contract, implementation, callers, and tests before editing;
  surface consequential ambiguity that the repository cannot resolve.
- Define observable success and make the smallest compatible change. Match local
  naming, errors, modules, and ownership; avoid speculative flexibility and
  unrelated cleanup.
- Use SOLID only as review vocabulary, not a mandate for traits or layers. Add an
  abstraction only for demonstrated variation, isolation, or duplicated complexity.

## Architecture Contracts

- Keep evaluator interface vocabulary as `input` and `output`. Preserve the
  strict evaluator identifier format `<namespace>/<name>:<version>`.
- Treat `wit/evaluator/v1.0.0/evaluator.wit` as the sole pre-release source of
  truth for the evaluator ABI, and freeze a versioned WIT contract on release.
- Keep each ABI's bindings, identity, validation, mapping, execution, and fixture
  under `vigilo/src/evaluator_abi/`; register it in `vigilo/src/evaluator_abi.rs`.
  Adding an ABI must not change worker, aggregation, database, or profile policy.
- A new ABI requires its versioned WIT, adapter, real Wasm fixture marked with
  `package.metadata.vigilo.abi-fixture`, registry entry, compatibility-matrix
  coverage, and docs. Retain old adapters while published artifacts or reproducible
  runs need them; unknown identities and changed hashes fail closed.
- Keep evaluator execution contracts in `vigilo/src/contracts/`, not persistence
  models. Update host mappings, evaluator examples, and docs with WIT changes.
- Do not rename public CLI flags or terms unless the task explicitly requires it.
- Performance work follows `performance/README.md`. Production code must not
  depend on `xtask/perf`; keep `cargo perf check` service-free, require an exact
  correctness oracle before timing, and require bounded, exact fixture cleanup.

## Testing And Verification

- Test observable contracts, including positive and negative outcomes and relevant
  boundary, timeout, retry, and cleanup cases. Do not couple tests to private layout.
- For a reproducible defect, first add or identify a focused regression test and
  confirm that it fails for the expected reason. Then implement the fix and confirm
  that the test passes. If pre-fix reproduction is impossible, document why.
- Lock completed behavior and stable invariants without prescribing unfinished
  phases; current unsupported behavior is not automatically a permanent contract.
- Use the boundary that proves the claim: units for pure logic, real Wasm fixtures
  for ABI behavior, PostgreSQL for persistence, performance services for topology
  ownership, and end-to-end tests for distributed runtime behavior.
- Service-free tests must not require Docker, PostgreSQL, RabbitMQ, external network
  access, or platform-specific shell tools. Fixtures must be deterministic, bounded,
  and cross-platform. Reserve `#[ignore]` in the `vigilo` binary for PostgreSQL;
  put other opt-in services in a named target.
- Reproduce a reported regression with its exact command, environment, and
  platform when available. Report required checks that did not run and why;
  never claim a live integration path was verified unless that tier ran.
- Preserve the test tiers and aggregate 80% line-coverage gate in
  [README.md](README.md#test-tiers). Add meaningful cases; do not weaken the gate
  or expand exclusions to accommodate new code.
- For ABI changes, run fixture-backed compatibility coverage with
  `--features evaluator-abi-fixtures`; do not mark these tests ignored.
- Run the affected test first. For Rust, CI, migration, evaluator, or workflow
  changes, finish with:

```bash
cargo +nightly fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked --lib --bins
```

- Run `cargo perf check` for performance harness changes. For service or topology
  changes, also run `cargo test -p xtask --locked --features performance-services --test performance_services -- --nocapture`.
- For SQL, locking, leases, sharding, or database workflow changes, run
  `cargo test -p vigilo --locked --bin vigilo -- --ignored --nocapture --test-threads=4`;
  apply changed migrations through setup against a fresh PostgreSQL database.
- Run the named end-to-end tier for distributed routing or runtime changes when
  its full dependency set is available. Run `npm --prefix web run typecheck` and
  `npm --prefix web run build` for web documentation changes.

## Documentation

- Before v1, public documentation describes one greenfield architecture in present
  tense. Keep implementation progress and future work in private planning files;
  do not mix current/target states or promise backward compatibility, upgrades,
  or migration support unless explicitly requested.
- Do not reference names, identifiers, or content that exist only in Git-ignored
  paths from non-ignored files. Documentation-site content must not reference
  private plans; describe behavior directly and link to non-ignored sources.
- Give every substantial Rust module `//!` documentation with a high-level
  overview of its system role, responsibilities, boundaries, main flow, and
  important ownership or correctness invariants.
- Give public APIs and non-obvious internal functions `///` documentation when
  purpose, inputs, outputs, errors, side effects, lifecycle, or concurrency is not
  clear from the signature. Comments explain why or a constraint, not syntax.
- Update user documentation with CLI, configuration, schema, contract, or
  operational changes. Link to one source of truth instead of duplicating it;
  keep examples small, runnable, and versioned.
- Update the corresponding MDX and Mermaid sources under `web/docs/architecture/`
  when component boundaries, command/control flow, state transitions, ownership,
  failure handling, or scaling behavior changes. Keep structure diagrams,
  paired execution/decision flows, and lifecycle diagrams consistent with their text.

## Toolchain

- Build and test on the repository's stable toolchain. Format with nightly
  rustfmt because `rustfmt.toml` uses unstable options; do not weaken those
  settings to make stable rustfmt pass.
