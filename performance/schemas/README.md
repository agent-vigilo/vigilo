# Performance Configuration And Artifact Schemas

This directory documents the contracts consumed and produced by `cargo perf`.
The Rust types in `xtask/src/perf/model.rs` and their owning command modules are
authoritative; this document is the maintainer-facing field reference.

## Execution Flow

1. The workload registry defines which measurements exist and what makes each
   execution valid.
2. A profile selects exact workload tuples and sets campaign sampling limits.
3. The runner verifies that the selected binary manifests advertise every
   required workload capability.
4. The runner executes the resolved profile and writes raw samples, block
   records, comparisons, and a report under `target/perf/runs`.

Service-backed samples use a binary-specific prepared database template, then
clone it into a fresh database and pair it with a fresh broker scope. Template
structural counts are verified before use.

An unknown workload, tuple, schema, or capability is an unsupported result. The
runner never skips required work and reports success for the remainder.

## Versioning

The version identifiers describe different contracts:

| Identifier | Meaning |
| --- | --- |
| `workload-registry/v1` | Shape of the registry document. |
| Registry `revision` | Content revision of the catalog using that shape. |
| `startup.cli-help.v1` | Semantic version of one workload definition. |
| `reference-v1` | Semantic version of one campaign composition. |
| Git commit and digests | Exact runner, source, binary, and setup inputs. |

A runner refactor that preserves measurement semantics does not require a new
workload version. A fixture, oracle, measured region, or workload meaning change
does. Retain the old ID and add `.v2`; do not silently redefine `.v1`.

## Workload Registry

`performance/registry/workloads-v1.toml` is the typed catalog referenced by all
profiles.

### Registry Fields

| Field | Meaning |
| --- | --- |
| `schema_id` | Registry document shape. The current runner accepts only `workload-registry/v1`. |
| `revision` | Positive catalog content revision. It does not identify a Vigilo release. |
| `[constants]` | Audited Vigilo limits used to shape fixtures and projections. |
| `[[workloads]]` | Stable workload contracts available for profile selection. |

### Workload Fields

| Field | Meaning |
| --- | --- |
| `id` | Stable semantic workload ID recorded in profiles and result artifacts. |
| `owner` | Repository component responsible for the protected production boundary. |
| `status` | `implemented` when the runner can execute the workload; `planned` reserves the contract but fails if selected. |
| `capability` | Exact capability required in every selected build manifest. This is separate from `id` so future adapters can map one workload to version-specific binary capabilities. |
| `fixture` | Versioned fixture catalog used to generate deterministic setup and expected counts. |
| `tuples` | Allowed fixture shapes. Profiles must select one exact value from this list. |
| `unit` | Denominator used to interpret the result, such as one process start, case, evaluation, chunk, or run. |
| `oracle` | Stable name of the exact correctness validation required after execution. Timing is invalid if the oracle fails. |
| `required_metrics` | External observations required for a valid result. Missing required measurements cannot be treated as a pass. |
| `statement_budgets` | Optional map from registered tuple to positive absolute SQL statement ceiling. Frozen with the registry; checked by `cargo perf check-queries` independently of timing. |
| `watchdog_ms` | Hard outer deadline for one execution. Exceeding it terminates the process tree. |
| `planning_duration_ms` | Conservative duration estimate used to prove the campaign fits its profile cap. It is not an execution timeout. |
| `preconditioning` | `none`, or `one-per-binary` for one unmeasured execution of each binary before sampling. |
| `command` | Arguments passed directly to the measured Vigilo executable without shell interpretation. Currently used by the startup workload. |
| `help_signatures` | Required substrings in startup stdout. A missing signature is a fixture/capability mismatch. |
| `scaling_model` | Optional fixed-plus-slope or explicit stepped model contract for a scalable component. |
| `reliability` | Optional bounded duration, observation interval, recovery deadline, RSS/descriptor ceilings, throughput retention, and attempt/delivery amplification policy. Required by soak and recovery timing modes. |

### Scaling Model Fields

| Field | Meaning |
| --- | --- |
| `kind` | `fixed_plus_slope` for one intercept and slope, or `stepped` for independently estimated declared cardinalities. |
| `input_dimension` | Independent quantity represented by each point. |
| `max_residual_fraction` | Maximum sample-level relative residual accepted by `cargo perf model`. |
| `discontinuities` | Measured inputs that begin known stepped regions; forbidden for continuous models. |
| `points` | Complete one-to-one mapping of registered tuples to positive inputs and exact observations. |
| `points.exact` | Required HTTP, worker-queue, or durable-count values. Unknown keys and count drift invalidate the sample. |

For `startup.cli-help.v1`, the runner launches the release executable with
`--help`, requires exit `0` and the configured help signatures, and records wall
time, child CPU, peak RSS, and executable size. One execution per binary is
discarded before measurement to equalize host executable-loading effects.

The four service-backed workloads resolve `fixture` through
`performance/fixtures/<fixture>.toml`. The catalog fixes evaluator identity,
agent response size, workload cardinalities, and lifecycle limits. Runtime URLs
and ownership markers are injected while rendering; they are not credentials.

### Audited Constants

Registry constants mirror production limits that affect fixture boundaries or
capacity calculations. `cargo perf check` detects drift for the constants it
audits; the runner does not scrape Rust source during a campaign.

| Fields | Purpose |
| --- | --- |
| `database_connections_per_target`, `database_acquire_timeout_ms`, `database_operation_deadline_ms` | Database pool and operation bounds. |
| `run_chunk_size`, `creation_case_page_size`, `creation_page_budget` | Run-creation chunking, paging, and per-pass work. |
| `case_blob_group_size`, `membership_group_size`, `chunk_insert_group_size` | Database insert grouping boundaries. |
| `coordinator_tick_ms`, `coordinator_create_recovery_budget`, `coordinator_dispatch_budget`, `coordinator_finalization_budget` | Coordinator cadence and bounded work per pass. |
| `dispatch_window_size`, `lease_recovery_batch_size` | Dispatch and expired-lease recovery boundaries. |
| `outbox_batch_size`, `outbox_publish_parallelism` | Durable outbox claim and publication limits. |
| `worker_default_inflight_chunks`, `worker_heartbeat_ms` | Worker claim capacity and lease heartbeat. |
| `case_concurrency`, `evaluator_concurrency`, `wasm_concurrency`, `wasm_max_memory_mib` | Worker and Wasm concurrency/resource bounds. |
| `result_insert_group_size` | Evaluator-result database insert grouping. |

## Profiles

Files under `performance/profiles` configure campaigns. They select registry
entries but do not implement workloads.

### Profile Fields

| Field | Meaning |
| --- | --- |
| `schema_id` | Profile document shape. The current runner accepts only `profile/v1`. |
| `id` | Stable profile identity recorded in every campaign artifact. |
| `description` | Human-readable campaign purpose. |
| `requires_workload_selection` | Requires at least one explicit `--workload` instead of running the entire profile. |
| `campaign_cap_secs` | Hard wall-clock limit for the complete campaign. |
| `schedule_seed` | Stable input to deterministic `ABBA`/`BAAB` block ordering. |
| `max_artifact_bytes` | Maximum total size of the generated run directory. |
| `max_stdout_bytes`, `max_stderr_bytes` | Maximum output retained from each process; total observed bytes are still recorded. |
| `max_residual_orientation_effect` | Optional limit for unexplained ordering bias after counterbalancing. |
| `budget_reference` | Reviewed policy required by an all-`gating` profile; absent from informative, calibration, and capacity profiles. |
| `[[workloads]]` | Ordered workload tuple selections executed by the profile. |

### Profile Workload Fields

| Field | Meaning |
| --- | --- |
| `id` | Workload ID that must exist in the registry and profile. |
| `tuple` | One exact fixture shape declared by that registry entry. |
| `blocks` | Positive even number of blocks for sampled timing. Reliability modes require exactly one observation. A comparison block contains four executions. |
| `timing` | `informative` for ordinary measurements, `calibration` for canonical no-change evidence, `capacity` for a single-build staircase, `gating` for a budgeted fixed-load comparison, or `soak`/`recovery` for one long-running operational observation. Modes cannot be mixed. |

`developer-v1` requires explicit workload selection. Other profiles run their
complete declared list unless repeated `--workload` options filter it. An
unknown or out-of-profile selection fails before provisioning.

## Environment Configuration

Files under `performance/environments` describe a host contract. They are not
service credentials or provisioning scripts.

| Field | Meaning |
| --- | --- |
| `schema_id` | Environment document shape, currently `environment/v1`. |
| `id` | Stable host contract identity. Results from different IDs are not canonical blocking comparisons. |
| `canonical` | Whether results may support calibrated blocking verdicts. |
| `provider`, `region`, `availability_zone_id`, `instance_type`, `market` | Infrastructure provider, stable placement, on-demand market, and host shape. |
| `os`, `ami_id`, `ami_name`, `architecture`, `vcpus`, `memory_mib` | Required operating system image and hardware characteristics. |
| `storage`, `root_volume_*` | Human-readable and structured EBS size, type, IOPS, and throughput contract. |
| `validity` | Readiness observations required before accepting a canonical campaign. |

## Deployment Configuration

Files under `performance/deployments` are versioned inputs to `cargo perf project`.
They describe a proposed workload and independently sourced usable
limits; they are not service credentials or measured fleet-capacity claims.

| Field | Meaning |
| --- | --- |
| `schema_id`, `id`, `description` | `deployment/v1`, stable input identity, and purpose. |
| `target_utilization` | Fraction of measured per-worker rate used after two-worker efficiency. |
| `bootstrap_resamples`, `bootstrap_seed` | Deterministic whole-model resampling count and seed. |
| `capacity_fixture` | Fixture identity used to judge whether the deployment exactly matches measured evidence. |
| `[workload]` | Peak rate, duty cycle, concurrency, weighted run/payload/evaluator distributions, and result/diagnostic sizes. Distribution fractions must sum to one. |
| `[agent]` | Mean/p95 latency and connection policy used for HTTP concurrency demand. |
| `[amplification]` | Attempts, successful attempts, chunk deliveries, durable events, publish attempts, acknowledgements, retries, and quarantine multipliers. |
| `[configuration]` | Chunk size, worker/Wasm concurrency, database-pool size, coordinator cadence, and weighted placements. |
| `[[limits]]` | Typed resource capacity, usable fraction, provenance class, source, date, hardware, and configuration. Missing required limits remain unknown. |
| `[[boundedness]]` | Required operational path, whether it has a finite bound, and the source of that claim. |
| `[staging]` | Optional small staging run ID, worker count, observed useful rate, and maximum accepted model error. |

## Generated Artifact Schemas

The harness owns these additive machine-readable contracts:

| Schema | Responsibility |
| --- | --- |
| `environment/v1` | Observed host and collector identity plus validity limitations. |
| `build-manifest/v1` | Immutable executable, source, toolchain, dependency, setup-asset, and capability provenance. |
| `service-topology/v1` | Run-owned Compose inventory and redacted service endpoint provenance. |
| `sample/v1` | Raw execution position, process and scoped external measurements, durable counts, and validation state. |
| `query-budget-check/v1` | Frozen absolute statement ceilings, source digests, per-tuple counts, database-role maxima, and acceptance verdicts. |
| `comparison/v1` | Balanced estimator inputs, effects, confidence intervals, diagnostics, and verdict. |
| `report/v1` | Campaign status, failures, comparison summaries, and artifact links. |
| `calibration/v1` | Canonical no-change noise bounds, approximate power, repeatability, and recommended independent block counts. |
| `capacity-calibration/v1` | Bounded one/two-worker staircase points, knees or lower bounds, and scale efficiency. |
| `performance-budget/v1` | Reviewed environment-specific workload/tuple/metric budgets and minimum block counts. |
| `performance-baseline/v1` | Digested index of calibration, capacity, budget, profile, and build-manifest evidence. |
| `component-models/v1` | Accepted or rejected component fits, coefficients/steps, residuals, and evidence counts. |
| `projections/v1` | Resolved deployment input, joint-bootstrap intervals, visible equations, demand/limit table, bottleneck, staging error, and confidence label. |
| `reliability/v1` | Interval process/progress observations, exact useful work, amplification, injected fault, recovery time, and bounded verdict. |
| `performance-suppressions/v1` | Metric-specific temporary suppression ownership, issue, reason, and expiry. Empty by default. |

Every persisted document carries its schema ID. Readers reject an unknown
schema ID and preserve unknown additive fields. Retained version fixtures are
required before a schema is revised; format migrations are deferred until a
second version exists.

`campaign.json` links only to run-relative files. Before measurement, the
harness freezes the validated workload registry, exact resolved profile,
optional reviewed budget policy, and verified build-manifest metadata beneath
`provenance/`. Offline calibration, projection, and confirmation revalidate
those frozen inputs and refuse escaped paths, incomplete links, or identities
that differ from the campaign.

### Process Memory

Linux samples use `linux-proc-exec-hwm-wait4-cpu-v4`: RSS comes from `/proc` VmHWM
after the kernel's `PF_FORKNOEXEC` flag clears; `wait4` supplies CPU/exit status.
Missing RSS is unavailable; Linux does not
substitute `wait4` RSS, which can include the harness address space inherited
before exec. Required-metric validation rejects missing observations.

### Sample External Measurements

`sample.external.sql_databases` records each distinct database name, its
`control`/`execution`/`shared` role, complete statement-category counts, and SQL
execution/planning milliseconds. Categories are `read`, `dml`, `mixed` (WITH),
`transaction`, and `other`; they classify syntax, not physical mutations. Their
sum equals `sql_calls`. Shared databases are never attributed to only one plane.
Missing detailed counters cannot satisfy a normal query-budget check.

`query-budget-check/v1` reports source campaign/sample and registry digests,
whether budgets were applied retrospectively, and each tuple's repetition count,
ceiling, observed minimum/maximum, independent per-role maxima and pass/fail result.
Reports occupy a separate directory; source campaigns remain unchanged. See
[Creation Statement Budgets](../README.md#creation-statement-budgets).

`sample.external` records PostgreSQL calls/time/rows, WAL bytes, database-size
delta, per-public-table `table_writes` deltas (`inserted`, `updated`, `deleted`),
gateway `storage_counters` deltas (`put_obj_ops`, `put_obj_bytes`,
`get_obj_ops`, `get_obj_bytes`, `del_obj_ops`),
HTTP request/byte/peak-concurrency counts, RabbitMQ worker-delivery
ready/unacknowledged counts, peak sampled service memory/CPU observations, and
an exact `durable_counts` map from the workload oracle. `query_diagnostics`
retains normalized statement fingerprints, plan count/time, execution time,
shared/temp buffer counters, and per-statement WAL records/images/bytes. These
diagnostics are queried only after process timing ends and are never gates.
Collectors reset after unmeasured setup and before every measured process
region.

`services.json` is the teardown authority for one campaign. It records the
Compose project, redacted endpoints, agent URL, and exact container/network/volume IDs.
Cleanup also requires matching live `io.vigilo.performance=true` and
`io.vigilo.run-id` labels.
