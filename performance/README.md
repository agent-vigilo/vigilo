# Vigilo Performance Harness

`cargo perf` is a repository-local Cargo alias backed by the `xtask` workspace
package. It is not installed globally and it is not shipped with Vigilo.

The harness includes component scaling models, canonical noise analysis,
bounded one/two-worker capacity calibration, reviewed budget publication,
named deployment capacity projections with limit provenance, and confirmed
regression gates. It also owns resident-process soak and controlled-recovery
checks. Startup remains service-free; run creation,
coordinator, HTTP agent, worker/Wasm, persistence, and lifecycle measurements
use a fresh run-owned PostgreSQL database clone, RabbitMQ vhost/namespace, and
deterministic HTTP agent for every sample.
Cancellation, terminal reads, exports, shard movement, rebalance, and logical
placement workloads add routed database clones only when their selected tuple
requires them. Their setup remains outside the measured process boundary.

## Module Overview

- `build` snapshots the release executable and workspace-independent setup
  assets, then records their digests and supported CLI capabilities.
- `config` and `model` own typed registry, profile, sample, comparison,
  calibration, scaling, and diagnostic contracts.
- `workload`, `fixture`, and `service` prepare isolated databases and broker
  scopes, run supported release commands, collect external observations, and
  enforce exact oracles.
- `process` bounds and reaps process trees; `schedule` and `stats` implement
  counterbalanced sampling and comparison estimates.
- `scaling` accepts fixed-plus-slope or explicit stepped models only from
  repeated valid samples; `diagnostics` renders non-gating PostgreSQL statement,
  planning, buffer, and WAL evidence captured after process timing.
- `calibration`, `command`, `report`, `artifact`, and `check` analyze evidence,
  orchestrate campaigns, persist artifacts, render results, and enforce
  repository isolation.
- `projection` combines raw bounded-capacity evidence with a named workload mix,
  jointly bootstraps worker and dependency demand, and rejects unsupported
  capacity labels.
- `reliability` evaluates interval progress, process resources, exact terminal
  work, queue settlement, amplification, and recovery deadlines independently
  from ordinary timing statistics.

## Layout And Configuration

- `performance/registry/workloads-v1.toml` defines the available workload
  contracts: fixture tuples, correctness oracles, required metrics, limits, and
  implementation status.
- `performance/profiles/*.toml` select workload tuples and define block counts,
  scheduling, timing policy, and campaign limits.
- `performance/environments/*.toml` describe hosts on which results may be
  considered comparable.
- `performance/fixtures/*.toml` define deterministic logical input shapes.
- `performance/deployments/*.toml` define named workload mixes, topology,
  amplification assumptions, boundedness, and independently sourced limits.
- `performance/suppressions-v1.toml` is the only suppression registry. Entries
  must identify one workload tuple and metric, owner, issue, reason, and expiry.
- `infra/performance/compose.yml` defines the private PostgreSQL and RabbitMQ
  topology created for one campaign; infrastructure definitions remain under
  `infra`.
- `infra/performance/aws/template.yaml` provisions the pinned ephemeral EC2 host
  implementing the canonical AWS environment contract.
- `xtask` implements `cargo perf`; generated builds and results stay under
  `target/perf`.

The `performance` directory is for the external process and service harness.
Criterion targets are added under `vigilo/benches` only when production already
has a stable library surface worth measuring. Vigilo currently keeps runtime,
database, and CLI modules binary-private, so there is no Criterion target and no
API is widened solely for benchmarking.

See [`schemas/README.md`](schemas/README.md) for the registry, profile,
environment, and generated-artifact field reference.

## Profiles

| Profile | Purpose |
| --- | --- |
| `developer-v1` | Fast local diagnosis of explicitly selected workloads using two balanced blocks. |
| `ingestion-baseline-v1` | Initial run creation across case counts, payload sizes, and reuse of a finalized dataset, with physical row-write counters. |
| `pr-v1` | Pull-request correctness checks and short, informative timing canaries. |
| `reference-v1` | Broader repeatable comparison across the complete MVP workload matrix. |
| `calibration-v1` | Stable no-change campaign used to measure canonical-host and harness noise. |
| `capacity-v1` | Bounded one/two-worker load staircase, separate from fixed-load comparisons. |
| `component-smoke-v1` | Explicitly selected, small exact-oracle checks for each component driver. |
| `component-reference-v1` | Repeated samples at every registered model point and batching boundary. |
| `component-nightly-v1` | Broader orthogonal payload, latency, and large-cardinality diagnostics. |
| `admin-smoke-v1` | Explicitly selected exact-oracle checks for routed administration and large-data drivers. |
| `admin-nightly-v1` | Scheduled cancellation, read/export, movement, placement, and creation-boundary evidence. |
| `recovery-v1` | One controlled RabbitMQ restart with resident worker/coordinator reconnect and exact pre/post-fault work. |
| `soak-v1` | One 30-minute steady resident topology with interval progress and resource bounds. |

`reference-v2` is generated only after a canonical calibration passes review.
It references a versioned budget policy and contains only tuples whose block
counts and wall-time budgets have calibration evidence. Generated candidates
remain under `target/perf/baselines` until their profile and budget files are
reviewed and checked into `performance/`.

Profiles configure campaigns; they do not implement workloads. A selection
fails before provisioning if its workload, tuple, fixture, or binary capability
is unavailable.

Calibration compares identical immutable builds under a stable workload and
sampling configuration. Because the expected product difference is zero, its
observed A/B effects estimate normal environmental and measurement variation.
`cargo perf calibrate noise` converts the comparison into repeatability,
orientation, noise-bound, estimated-power, and recommended-block evidence. The
block recommendation uses the observed 95% bootstrap interval width and the
reviewed power target; it is an approximation over independent blocks, not a
claim that executions inside one block are independent. Reviewed practical
budgets come from `performance/budgets/review-targets-v1.toml`; calibration
validates that the host can resolve those budgets and never raises them to make
noise pass.

## Quick Start

### 1. Validate The Harness

Validate the harness, package boundary, profiles, scheduler, and process cleanup:

```bash
cargo perf check
```

### 2. Create A Test Subject

`cargo perf build` does not run a performance test. It compiles one Vigilo
revision outside the measured interval and saves everything needed to test that
exact revision together:

```bash
cargo perf build --output target/perf/builds/current
```

Here, `current` is only a local name for the snapshot. The command creates:

| Path | Purpose |
| --- | --- |
| `target/perf/builds/current/release/vigilo` | Release executable that is measured; it ends in `.exe` on Windows. |
| `target/perf/builds/current/build-manifest.json` | Binary digest, source revision, toolchain, dependencies, capabilities, and setup-asset digests. |
| `target/perf/builds/current/setup-assets/` | Frozen migrations, evaluator contract, evaluator source, and evaluator Wasm used by service workloads. |

The default `--source .` means the current checkout. For A/B testing,
`--source` can instead name a separate baseline worktree. `--output` must be a
unique directory below this workspace's `target/perf/builds`; it is not the
test-results directory. Use `--force` only when deliberately replacing a local
snapshot.

A clean production release can take several minutes because Vigilo enables LTO
and one codegen unit. Build once per revision and reuse the snapshot. `run` and
`compare` verify its manifest and never invoke Cargo inside a measured campaign.

### 3. Run The First Test

On Linux and macOS, run:

```bash
cargo perf run \
  --profile developer-v1 \
  --workload startup.cli-help.v1 \
  --bin target/perf/builds/current/release/vigilo \
  --build-manifest target/perf/builds/current/build-manifest.json \
  --output target/perf/runs/first-startup
```

On Windows, use the same command with
`target/perf/builds/current/release/vigilo.exe`. The `--bin` and
`--build-manifest` paths must come from the same snapshot directory.

This startup test is service-free. A pass means the executable identity,
capability, process, output, and exact help oracle all passed. Its timing is
informative rather than a performance-regression verdict.

### 4. Compare Two Revisions

Compare two immutable snapshots on the same host:

```bash
cargo perf compare \
  --profile developer-v1 \
  --workload startup.cli-help.v1 \
  --baseline-bin <baseline-vigilo-binary> \
  --baseline-build-manifest <baseline-build-manifest.json> \
  --candidate-bin <candidate-vigilo-binary> \
  --candidate-build-manifest <candidate-build-manifest.json>
```

Each comparison uses two counterbalanced blocks: one `ABBA` and one `BAAB`, for
eight measured executions. Readiness and the one-per-binary startup
preconditioning runs are recorded separately and excluded from measurements.
The schedule seed and logical block index make position order reproducible.

Select any workload present in the chosen profile. Docker is provisioned
lazily, so startup-only campaigns do not start services.

### Recommended First Run

Start with a correctness smoke rather than the full reference or soak profile:

1. Run `cargo perf check`. Resolve missing tools or invalid contracts before
   measuring anything.
2. Build one immutable snapshot as shown above. Reuse that snapshot for every
   command in this first run.
3. Run `developer-v1` with `startup.cli-help.v1`. This proves manifest,
   subprocess, output-bound, and reporting behavior without Docker.
4. With Docker Engine and Compose available, run the small production-path
   profile:

   ```bash
   cargo perf run --profile pr-v1 \
     --bin <vigilo-binary> \
     --build-manifest target/perf/builds/current/build-manifest.json \
     --output target/perf/runs/first-production-path
   ```

5. Read `summary.md` and inspect any failed workload's raw sample and exact
   oracle before looking at timing. Regenerate the view with:

   ```bash
   cargo perf report --run-dir target/perf/runs/first-production-path
   ```

An exit `0` from `pr-v1` establishes that the selected release commands and
service paths completed exact work under the harness. Its timing is
informative; it does not establish that performance is unchanged. Do not begin
with `reference-v1`, capacity, recovery, or soak unless the small production-
path run is already reliable.

Fit the registered component models after a complete component reference run:

```bash
cargo perf model --run-dir target/perf/runs/<component-run>
```

The command writes `component-models.json` and rejects missing repetitions,
exact-count drift, negative coefficients, or residuals above the registered
tolerance. Render the post-timing database evidence separately:

```bash
cargo perf diagnose --run-dir target/perf/runs/<component-run>
```

`diagnostics.md` is investigative output and never changes a correctness,
comparison, budget, or model verdict.

## Ingestion Measurements

`run.create-ingestion.v1` measures one initial `run create` invocation on a
single PostgreSQL placement that owns both control and execution data. The
`ingestion-baseline-v1` profile uses 1,000/10,000/50,000-case points, 1/16 KiB
per-case padding, and fresh/reused datasets. Reuse preparation creates the first
run before collector reset; only the second run is measured. Counts stay below
the default 64,000-case creation handoff so all selected runs reach `pending`.

```bash
cargo perf run --profile ingestion-baseline-v1 \
  --workload run.create-ingestion.v1 \
  --bin target/perf/builds/current/release/vigilo \
  --build-manifest target/perf/builds/current/build-manifest.json \
  --output target/perf/runs/ingestion-measurement
```

On Linux, memory is sampled from the child's post-exec `/proc` high-water mark;
`wait4` provides CPU and exit status. If RSS cannot be observed, it stays
unavailable and workloads requiring it fail validation.

The exact oracle checks actual case blobs, dataset membership, run-local case
membership, payload bytes, chunk counts, completed creation, and zero premature
execution/dispatch. External counters are captured before these oracle reads.
`sql_rows` includes reads; `table_writes` reports physical inserted/updated/deleted
tuples independently of bulk statement grouping. SQL calls include the collector's
WAL-position query, and WAL includes server background work in the isolated
PostgreSQL instance. Input padding is repeated `x`, so PostgreSQL compression can
make WAL and database growth much smaller than submitted bytes. Retain this
fixture characteristic when comparing results; it is not an incompressible-data
capacity test. Reuse is data reuse in a freshly cloned sample database, not a
promise of warm operating-system caches.

Local runs provide informative evidence with raw samples and provenance. They do
not establish fleet throughput or satisfy canonical publication requirements.

## Calibration And Baselines

Publishable campaigns require the `aws-m6i-2xlarge-al2023-v1` Linux host contract
and an externally validated `VIGILO_PERF_CANONICAL_VALIDATED=1` declaration.
Do not set that declaration on local or shared CI hosts. Local runs may produce
informative diagnostics, but publication rejects their evidence.

Run `calibration-v1` as a same-build comparison, then analyze it:

```bash
cargo perf compare --profile calibration-v1 \
  --baseline-bin <vigilo-binary> --baseline-build-manifest <build-manifest.json> \
  --candidate-bin <same-vigilo-binary> --candidate-build-manifest <same-build-manifest.json>
cargo perf calibrate noise --run-dir target/perf/runs/<calibration-run>
```

Run the separately bounded capacity staircase for the same build:

```bash
cargo perf run --profile capacity-v1 \
  --bin <vigilo-binary> --build-manifest <build-manifest.json>
cargo perf calibrate capacity --run-dir target/perf/runs/<capacity-run>
```

Capacity steps use one and two workers at predeclared load multipliers
`1`, `2`, `4`, `8`, and `16`. The analyzer identifies the first step with less
than 10% throughput gain and more than 25% p95 terminal-latency growth, or 90%
normalized per-worker process CPU. Shared-service CPU at or above 85%
invalidates the staircase instead of being labeled as worker capacity. When no
knee appears, the artifact records only an observed rate lower bound.

After review, publish an immutable candidate baseline:

```bash
cargo perf calibrate publish \
  --calibration target/perf/runs/<calibration-run>/calibration.json \
  --capacity target/perf/runs/<capacity-run>/capacity.json \
  --build-manifest <build-manifest.json> \
  --approved-by <review-identity>
```

Publication refuses mismatched build digests, unrepeatable noise, invalid
capacity evidence, and an existing output directory. It emits digested evidence,
`reference-v2`, its budget policy, and the frozen build manifest beneath
`target/perf/baselines/reference-v2`.

## Regression Testing With MadSim

MadSim and `cargo perf` answer different questions and must remain separate.
MadSim validates deterministic state, scheduling, timeout, retry, and fault
outcomes under virtual time. It must not write virtual durations into
`samples.jsonl`, calibration evidence, or regression budgets. `cargo perf`
measures the real release executable, operating-system processes, PostgreSQL,
RabbitMQ, HTTP, and Wasmtime on the canonical host.

The simulation rollout must not change Tokio, task supervision, shared workflow
engines, or another measured production path until canonical calibration has
published and review has accepted a gating profile such as `reference-v2` plus
an immutable direct-Tokio baseline. Every later production-path change runs the
affected workload IDs as an immutable same-host comparison under that published
policy; resident lifecycle changes also run the applicable recovery or soak
profile.

Recurring simulation has separate fixed-regression and discovery lanes. Fixed
corpus seeds repeat; scheduled exploration uses persisted deterministic,
non-overlapping epochs so later runs search new scenario/runtime pairs. A
bounded longevity family also keeps one simulated system across repeated work,
expiry, reconnect, fault/heal, and restart epochs to detect logical accumulation
and starvation. Real RSS, descriptor, socket, dependency, and throughput
behavior remains the authority of `system.soak.v1`, `system.recovery.v1`, and
the release-binary comparison suite.

Before calling a result a performance regression test:

1. Establish canonical repeatability, publish and review a gating profile such
   as `reference-v2`, and commit its profile and budget policy.
2. Make the MadSim implementation expose a stable test command, named scenarios,
   fixed seeds, and exact logical outcomes. A failing or flaky simulation is a
   correctness failure; do not interpret performance measurements until it is
   resolved.
3. Record which real workload contracts correspond to each simulated scenario.
   Use the narrowest affected set:

| Simulated behavior | Real performance contracts to select |
| --- | --- |
| Run creation or dispatch scheduling | `run.create.v1`, `coordinator.dispatch.v1` |
| Worker execution, retry, or evaluator scheduling | `worker.execute-wasm.v1`, `system.lifecycle.v1` |
| Broker outage or reconnect | `coordinator.recovery.v1`, `system.recovery.v1` |
| Routed database or administrative behavior | Matching `admin-smoke-v1` or `admin-nightly-v1` workload IDs |

For a candidate change, use this order:

1. Run the documented MadSim command with the checked-in scenarios and fixed
   seeds. Require exact state and operation counts, not virtual elapsed time.
2. On the exclusive canonical host, build immutable baseline and candidate
   snapshots from the merge base and candidate revisions.
3. Compare the affected workloads with the published gating profile:

   ```bash
   cargo perf compare \
     --profile <published-gating-profile> \
     --workload <affected-workload-id> \
     --baseline-bin <baseline-vigilo-binary> \
     --baseline-build-manifest <baseline-build-manifest.json> \
     --candidate-bin <candidate-vigilo-binary> \
     --candidate-build-manifest <candidate-build-manifest.json> \
     --output target/perf/runs/candidate-comparison
   ```

4. If the first over-budget interval exits `2`, repeat the identical campaign
   with an independent schedule using
   `--confirmation-of target/perf/runs/candidate-comparison`. A matching second
   interval is the performance regression. Do not rerun arbitrary times or
   change the budget after seeing the result.
5. When the MadSim scenario covers outage or sustained operation, also run the
   real `recovery-v1` or `soak-v1` profile against the candidate. Those profiles
   enforce operational safety bounds; they are not substitutes for the A/B
   latency and throughput comparison.

Acceptance requires all applicable layers to pass: MadSim exact outcomes, real
workload exact oracles, the published A/B regression budget, and relevant
recovery or soak bounds. Report the conclusion as "no regression detected at
the published budgets and confidence," not "no performance change."

## Capacity Projection

After `cargo perf calibrate capacity` creates `capacity.json`, project one named
deployment from the same run directory:

```bash
cargo perf project \
  --run target/perf/runs/<capacity-run> \
  --deployment performance/deployments/planning-example-v1.toml
```

The deployment input declares peak and average traffic, run/payload/evaluator
mixes, agent latency, retry and message amplification, concurrency and placement
configuration, operational boundedness, and resource limits. Every limit has a
raw capacity, usable fraction, date, hardware/configuration, and `measured`,
`provider_documented`, or `operator_declared` provenance. The checked-in file is
an illustrative shape whose limits must be replaced before use.

The command reproduces the reduced capacity points from `samples.jsonl`, rejects
materially falling throughput and shared-service saturation, and resamples all
workload points together. `projections.json` records one/two-worker knees,
scale efficiency, worker count, CPU, memory, PostgreSQL, RabbitMQ, HTTP, Wasm,
storage, retry, and coordinator demand with formulas and 95% intervals.
`projection.md` includes the complete resolved input and a limit/bottleneck
table.

Projection confidence is explicit:

- `invalid` means required behavior is unbounded, evidence is inconsistent,
  nonlinear or saturated, or a supplied staging point exceeds its error limit.
- `directional` means demand is useful but a required limit is missing, the
  workload differs from the measured fixture, or the worker estimate exceeds
  the measured one/two-worker range.
- `planning` means the model is accepted inside the measured range with complete
  limits, but it is not a direct canonical staging match.
- `calibrated` additionally requires canonical matching evidence and an accepted
  staging observation.

The current coordinator RabbitMQ publish/confirm operation has no encompassing
deadline. The checked-in deployment therefore declares that path unbounded and
cannot receive a supported confidence label. A real staging observation may be
added as `[staging]`; its projected and observed rate, relative error, and
acceptance limit are retained in the result. Estimates beyond two workers
remain directional even when that small staging check passes.

## Reliability Runs

Reliability profiles are single-build operational checks. They execute once and
never enter `ABBA`/`BAAB` comparison statistics:

```bash
cargo perf run --profile recovery-v1 \
  --bin <vigilo-binary> --build-manifest <build-manifest.json>
cargo perf run --profile soak-v1 \
  --bin <vigilo-binary> --build-manifest <build-manifest.json>
```

The soak keeps one real `coordinator start` and one real `worker start` process
alive, creates small terminal runs at fixed intervals for at least 30 minutes,
and samples liveness, RSS, and Linux file descriptors. It requires monotonic
useful progress, retained end-window throughput, exact completed cases and
attempts, a drained broker scope, bounded delivery amplification, and orderly
harness-owned shutdown.

The recovery workload completes useful work, restarts the run-owned RabbitMQ
application without replacing its container, ports, volume, or ownership
identity, and requires the same resident processes to complete new useful work
before the configured deadline. Fault injection and recovery time are explicit
evidence; an early process exit, lost work, stranded delivery, or excess
amplification fails.
`reliability/<workload>-<tuple>.json` is the machine contract and
`reliability.md` is its derived operator view. The absolute resource ceilings
are safety bounds, not reviewed regression budgets; trend-based leak budgets
remain subject to canonical repeatability evidence.

## GitHub Actions

The normal `Build` workflow keeps `cargo perf check` and the service ownership
fixture active on hosted runners. `.github/workflows/performance.yaml` defines
the real canonical comparison, nightly component/recovery/capacity run, an
optional configured projection, and weekly soak on an exclusive Linux runner
labelled `vigilo-performance`.

The self-hosted jobs are inert in a new repository. Provision the canonical AWS
host with `infra/performance/aws/template.yaml` as documented in
`infra/performance/aws/README.md`, then install a Linux Actions runner version
`2.327.1` or newer:

1. Confirm it matches `performance/environments/aws-m6i-2xlarge-al2023-v1.toml`
   and carries the `self-hosted`, `linux`, `x64`, and `vigilo-performance`
   labels.
2. Set repository variable `VIGILO_PERF_RUNNER_ENABLED=true`; this enables only
   manual `workflow_dispatch` runs.
3. Run `canonical`, `nightly`, `recovery`, and `soak` manually and review their
   job summaries and artifacts.
4. After external host certification, set
   `VIGILO_PERF_CANONICAL_VALIDATED=true` so evidence may identify as canonical.
5. To add nightly capacity projections, commit a reviewed deployment input and
   set `VIGILO_PERF_DEPLOYMENT` to its repository-relative path. Leaving the
   variable unset skips projection while retaining capacity evidence; the
   checked-in planning example is intentionally invalid and must not be used.
6. Calibrate, review, and commit a generated gating profile and budget policy,
   then set `VIGILO_PERF_REFERENCE_PROFILE` to that profile ID. Until then it
   defaults to informative `reference-v1`.
7. Set `VIGILO_PERF_SCHEDULES_ENABLED=true` to activate main, nightly, and
   weekly triggers.

The workflow uses Node 24 releases of `actions/checkout` and
`actions/upload-artifact`; an older runner will not execute them. No workflow
edit is required during activation.

No performance badge or required status check is installed until canonical
runs have established repeatability. The jobs serialize through one workflow
concurrency group, append Markdown results to the Actions summary, and retain
bounded artifacts for 14 or 30 days. Suppressions cannot disable a job and are
rejected when blank, wildcarded, unowned, issue-free, or expired.

## MVP Workloads

| Workload | Measured region | Exact postcondition |
| --- | --- | --- |
| `startup.cli-help.v1` | One release process executing `--help` | Exit `0` and frozen help signatures. |
| `run.create.v1` | `run create` for 1,001 cases | One pending run, 1,001 case memberships, zero executions, and 11 chunks. |
| `coordinator.dispatch.v1` | One `coordinator once` cycle | One legal start and all 512 chunks dispatched to the run-owned broker scope. |
| `worker.execute-wasm.v1` | One `worker once` pass | Exact attempts/results for either 8x1 or 1x8, one completed chunk, and drained delivery. |
| `system.lifecycle.v1` | Create through terminal state with one or two worker processes | One completed passing run, exact execution/result counts, and drained delivery. |

## Component Workloads

The component registry crosses selected production discontinuities without
generating a Cartesian product:

| Family | Registered boundaries | Exact observations |
| --- | --- | --- |
| Run creation | Case/page/group points from 1 through 10,000 | Runs, chunks, cases, and zero premature executions. |
| Dispatch | 1, 512, and 513 chunks | Dispatched chunks, durable events, and worker deliveries. |
| HTTP agent | 1, 8, and 16 requests; 1/64 KiB payloads; 0/10 ms delay | Requests, bytes, peak concurrency, attempts, and results. |
| Wasm and persistence | Evaluators 1/8/9 and cases 1/8/9/100 | Attempts, evaluator results, HTTP calls, and drained work. |
| Outbox | Batches 1/64/65/256/1,000 with parallelism 1/8/64 | Exact published worker deliveries and durable event rows. |
| Recovery | Expired leases 1/1,000/1,001 | Recovery counts, recovery events, and redeliveries. |
| Finalization | Terminal runs 1/64/65 | Completed runs, evaluator results, and completion events. |
| Cancellation | 1/4/17 logical routes and 1,024/8,192 open executions | Exact cancelled rows, terminal state, and idempotent replay. |
| Status and results | 250/251 terminal executions over one/two routes | Completed status plus exact execution, result, and diagnostic counts. |
| JSON and JSONL export | 250/251 terminal executions with a 250-row page | Exact record types/counts, output bytes, first/last byte, and peak RSS. |
| Shard move | 1,000/1,001 narrow rows and payloads around 4 MiB | Verified route switch, idempotent replay, and actual per-table rows, bytes, and page counts. |
| Rebalance | One/eight persisted items across two databases | One claimed item per resumable apply pass, exact verification, and every route on the target. |
| Placement | 1/8/16/32 logical placements on one PostgreSQL server | Exact bounded dispatch work and a fixed-cost-plus-placement slope. |
| Creation limits | Both sides of grouping and the 64-page handoff | Exact pending or recoverable-creating state and materialized case count. |

The router/cache contract remains registered but unavailable because the
release CLI cannot keep its process-local cache alive across calls. Adding a
benchmark-only command or exposing binary-private modules would measure a new
path rather than the shipped one, so selection fails closed until a legitimate
reusable production boundary exists.

Migration, evaluator publication, fixture creation, database cloning, and
worker queue preparation occur before collector reset and are excluded from
measurement. Lifecycle intentionally includes creation and every runtime
process.

Routed samples reset and aggregate PostgreSQL statement counts, rows, database
growth, and diagnostics across every participating database. Cluster WAL is
counted once because the reference topology uses one PostgreSQL server. JSONL
must remain below its 256 MiB peak-RSS contract at the registered page boundary;
JSON intentionally materializes one document and has a separate 512 MiB tested
limit. These limits are correctness guards for the registered fixture size, not
reviewed regression budgets or production capacity claims.

## Performance Test Tier

The service ownership/reset/cleanup integration test is explicit and opt-in:

```bash
cargo test -p xtask --locked --features performance-services --test performance_services -- --nocapture
```

It verifies a collector sentinel is visible once, absent after reset in the
next sample, and followed by exact resource cleanup. It is not part of normal
unit, PostgreSQL, migration, or end-to-end tiers.

## Results

Run output is written only below `target/perf/runs`. Each directory contains:

- `campaign.json` and `environment.json` for execution identity and host provenance
- `provenance/` with the validated registry, exact resolved profile, optional
  budget policy, and build-manifest metadata frozen before measurement
- `readiness.jsonl`, `samples.jsonl`, and `blocks.jsonl` for raw evidence
- `comparisons.jsonl` and per-workload comparison JSON for A/B results
- `report.json` as the report contract
- `summary.md` as the concise human-readable result
- `component-models.json` after `cargo perf model`
- `diagnostics.md` after `cargo perf diagnose`
- `projections.json` and `projection.md` after `cargo perf project`
- `reliability/*.json` and `reliability.md` for soak or recovery evidence

Use `cargo perf report --run-dir <run-directory>` to regenerate the terminal and
Markdown views from `report.json` and, when present, `projections.json` and
reliability evidence.

Exit `0` means all required correctness checks passed. Exit `1` means a
candidate crash, timeout, output overflow, or exact-oracle failure. Exit `2`
means the campaign was invalid, unsupported, inconclusive, or incomplete.
Timing remains informative in profiles without a reviewed budget; reviewed
numerical regression budgets apply only through a published gating profile. An
initial over-budget confidence interval exits `2`; repeat it with
`--confirmation-of <prior-run-directory>`. The confirmation must match the
frozen registry, resolved profile, budget policy, build digests, and canonical
environment and uses an independent schedule seed. A second over-budget
interval exits `1`.

## Isolation

- `xtask` has no dependency on the `vigilo` crate.
- `vigilo` has no normal or build dependency on performance tooling.
- The startup workload uses only the supported release executable and `--help`.
- Service workloads accept only dynamically assigned loopback endpoints.
- Each campaign gets a unique Compose project, network, labelled volumes,
  database-name prefix, RabbitMQ vhost, and queue namespace.
- Build snapshots include digested migrations, WIT, evaluator metadata, and
  evaluator Wasm; the harness never mutates the source copies.
- Offline calibration, projection, and confirmation load campaign inputs from
  the retained run-relative `provenance/` snapshot, not mutable repository or
  build-directory files.
- Process output, time, and artifacts are bounded. Timeout cleanup targets the
  Windows Job Object or Unix process group and always reaps the child.
- A workspace host lease prevents release builds and measurement campaigns from
  overlapping and contaminating one another; a stale owner is recovered by PID.

Normal teardown compares the complete live container, network, and volume
inventory to `services.json` and rechecks the run, performance, Compose project,
and component labels. Failed startup permits only a bounded subset of that fixed
topology before rollback. Both paths verify that no project-owned or run-owned
resources remain after Compose removal; any unexpected resource refuses
destructive cleanup.
