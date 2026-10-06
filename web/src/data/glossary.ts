export type GlossaryEntry = {
  id: string;
  term: string;
  definition: string;
  scope: string;
};

export type GlossarySection = {
  id: string;
  entries: readonly GlossaryEntry[];
};

export const glossarySections: readonly GlossarySection[] = [
  {
    id: 'runtime-roles',
    entries: [
      {id: 'vigilo-cli', term: 'Vigilo CLI', definition: 'Command entry point for evaluator publishing, run management, shard administration, and runtime service modes.', scope: 'A command can perform work directly or run a coordinator or worker process.'},
      {id: 'coordinator', term: 'Coordinator', definition: 'Process that advances durable creation recovery, lease recovery, dispatch, finalization, and outbox publication.', scope: 'Many coordinators can run concurrently because database claims divide work.'},
      {id: 'worker', term: 'Worker', definition: 'Process that consumes chunk-ready broker messages, claims chunks, invokes the agent target and evaluators, and persists results.', scope: 'Many workers can run concurrently; a chunk claim selects one current owner.'},
      {id: 'bounded-work', term: 'Bounded work', definition: 'Work limited by a configured item count, concurrency count, time budget, or retry budget.', scope: 'Prevents one cycle, placement, chunk, or evaluator from consuming unlimited resources.'},
      {id: 'container', term: 'C4 container', definition: 'Independently runnable process, data store, or infrastructure service in an architecture deployment view.', scope: 'A C4 container is a deployment boundary, not necessarily an operating-system container.'},
      {id: 'command-flow', term: 'Command flow', definition: 'Decisions and side effects produced by one CLI command.', scope: 'One flow follows one command from input through persistence and external calls.'},
      {id: 'toon-output', term: 'TOON output', definition: 'Compact structured CLI encoding intended for agent inspection and language-model tool workflows.', scope: 'Select with `-q -f toon`; use `-f json` when exact standard JSON parsing is required.'},
    ],
  },
  {
    id: 'evaluation-results',
    entries: [
      {id: 'agent-target', term: 'Agent target', definition: 'HTTP service or workflow whose behavior is being evaluated. It may wrap a model, prompt pipeline, or multi-step agent.', scope: 'One versioned target configuration per run profile.'},
      {id: 'run-profile', term: 'Run profile', definition: 'Versioned configuration for the agent target, evaluator bindings, scoring, persistence, retries, and gate behavior.', scope: 'One selected profile per run, replicated into immutable shard-local run snapshots; distinct from performance and evaluator artifact build profiles.'},
      {id: 'dataset', term: 'Dataset', definition: 'Versioned collection of evaluation cases.', scope: 'One dataset version per run.'},
      {id: 'case', term: 'Test case', definition: 'One immutable case input, optional expected output, and routing metadata from a dataset.', scope: 'A test case can be evaluated once in a run.'},
      {id: 'input', term: 'Case input', definition: 'Dataset case data sent to the configured agent target.', scope: 'One required input value per test case; distinct from the complete evaluator input envelope.'},
      {id: 'expected-output', term: 'Expected output', definition: 'Optional oracle data available to evaluators but not sent to the agent target.', scope: 'Zero or one expected value per dataset case.'},
      {id: 'agent-output', term: 'Agent output', definition: 'Captured actual result from the agent target, including optional text or structured data, tool calls, trace, raw provider output, and metadata.', scope: 'Passed to evaluators as `input.actual`; distinct from expected output.'},
      {id: 'evaluator-input', term: 'Evaluator input', definition: 'Versioned ABI `input` envelope containing run, execution, and attempt identity, the test case, actual agent output, and evaluator-specific configuration.', scope: 'One input envelope per evaluator binding invocation.'},
      {id: 'evaluator-output', term: 'Evaluator output', definition: 'Successful ABI `output` envelope containing evaluator identity, a completed or abstained outcome, diagnostics, and metadata.', scope: 'Evaluator errors are returned outside the output envelope.'},
      {id: 'task-type', term: 'Task type', definition: 'Case label used by automatic run-profile matching.', scope: 'One task type per dataset case.'},
      {id: 'tags', term: 'Tags', definition: 'Case labels used by `tags_any` and `tags_all` profile matching rules.', scope: 'Zero or more tags per dataset case.'},
      {id: 'case-group', term: 'Case group', definition: 'Run-profile rule that selects evaluator bindings and aggregation policy for matching cases.', scope: 'One explicit group or one or more automatically matched groups per case.'},
      {id: 'evaluator', term: 'Evaluator', definition: 'Versioned Wasm component that examines an agent output and returns one measurement or abstention plus optional diagnostics.', scope: 'Identified by `<namespace>/<name>:<version>`.'},
      {id: 'evaluator-binding', term: 'Evaluator binding', definition: 'Stable profile entry that assigns an evaluator measurement to host-owned normalization, threshold, requiredness, dimension, weight, and blocking policy.', scope: 'Identified by `evaluators[].id`; many bindings can apply to one case.'},
      {id: 'evaluation-plan', term: 'Evaluation plan', definition: 'Per-case resolution of matching case groups, evaluator bindings and configuration, dimensions, and aggregation policy.', scope: 'Distinct from the run-wide evaluator execution plan that pins artifacts, ABIs, adapters, runtime, and policy hash.'},
      {id: 'evaluator-identifier', term: 'Evaluator identifier', definition: 'Immutable published identity in `<namespace>/<name>:<version>` format.', scope: 'One identifier selects one versioned evaluator artifact.'},
      {id: 'measurement', term: 'Measurement', definition: 'The single raw observation returned by a completed evaluator invocation.', scope: 'Binary, numeric, or ordinal; the profile explicitly maps it to utility and judgment.'},
      {id: 'normalization-policy', term: 'Normalization policy', definition: 'Profile-owned mapping from a raw evaluator measurement to a score between 0.0 and 1.0.', scope: 'Binary, numeric linear, numeric curve, numeric threshold, or ordinal mapping; invalid values are rejected.'},
      {id: 'normalized-score', term: 'Normalized score', definition: 'Host-derived utility from 0.0 through 1.0 produced by applying one evaluator binding\'s normalization policy to a completed raw measurement.', scope: 'One score per completed binding result; errors and abstentions have no score.'},
      {id: 'judgment', term: 'Judgment', definition: 'Host-derived `passed` or `failed` result from comparing a normalized score with its evaluator binding threshold.', scope: 'Only completed measurements receive a judgment.'},
      {id: 'evaluator-outcome', term: 'Evaluator outcome', definition: 'Persisted invocation state: `completed`, `error`, or `abstained`.', scope: 'The ABI output carries completed or abstained outcomes; an `evaluator-error` is returned outside that output and persisted as error.'},
      {id: 'finding', term: 'Diagnostic finding', definition: 'Non-authoritative evaluator observation with severity, category, reason, evidence, and tags.', scope: 'Zero or more per invocation; diagnostics cannot score or block.'},
      {id: 'evaluator-completeness', term: 'Evaluator completeness', definition: 'Execution-level check that every required binding produced exactly one valid, normalized measurement.', scope: 'Errors, abstentions, missing results, duplicates, or invalid measurements withhold authoritative scores.'},
      {id: 'dimension', term: 'Dimension', definition: 'Profile-owned scoring bucket, such as `format` or `quality`.', scope: 'Host-normalized binding results are grouped into dimensions before total scoring.'},
      {id: 'dimension-score', term: 'Dimension score', definition: 'The `min_score` or `weighted_mean` result for one dimension of one execution.', scope: 'Zero or one score per configured dimension and execution.'},
      {id: 'aggregate-score', term: 'Aggregate score', definition: 'Weighted total of an execution\'s dimension scores.', scope: 'Zero or one total score per execution.'},
      {id: 'run-scorecard', term: 'Run scorecard', definition: 'Authoritative run-wide dimension and evaluator gate results merged from shard-local counters.', scope: 'One immutable scorecard per completed run; includes coverage, score, error, abstention, and pass-rate metrics.'},
      {id: 'scorecard-gate', term: 'Scorecard gate', definition: 'Run-wide rule over one dimension or evaluator binding and an optional case-group or tag slice.', scope: 'Evaluated from merged shard counters; a violated threshold or required slice with no matches fails the run gate.'},
      {id: 'blocking-result', term: 'Blocking result', definition: 'Host-derived failed binding result that can fail an execution independently of its aggregate score.', scope: 'Blocking comes only from evaluator binding or dimension policy.'},
      {id: 'execution', term: 'Execution', definition: 'Durable evaluation of one dataset case against the agent target.', scope: 'One expected execution per case in a run.'},
      {id: 'attempt', term: 'Attempt', definition: 'One worker\'s effort to complete an execution. Retries create later attempts.', scope: 'Many attempts can belong to one execution; only the current attempt is authoritative.'},
      {id: 'current-attempt', term: 'Current attempt', definition: 'Attempt ID and number selected by an execution as its current worker effort.', scope: 'Terminal writes also require the matching worker ID and a live attempt lease; zero or one attempt is authoritative per execution.'},
      {id: 'run', term: 'Run', definition: 'One durable evaluation of a dataset version with a run profile and agent target.', scope: 'A run contains chunks and expected executions.'},
      {id: 'cardinality', term: 'Cardinality', definition: 'Exact number of items in a set or the multiplicity of a relationship, such as one run containing many chunks.', scope: 'In performance workloads, cardinality is the declared input size for a scaling dimension, such as cases, chunks, or events.'},
      {id: 'run-status', term: 'Run status', definition: 'Operational lifecycle state: `creating`, `pending`, `running`, `finalizing`, `completed`, `failed`, or `cancelled`.', scope: 'One current value per run.'},
      {id: 'gate-status', term: 'Gate status', definition: 'Policy outcome such as `unknown`, `pass`, or `fail`.', scope: 'One current value per run; distinct from run status.'},
    ],
  },
  {
    id: 'work-routing',
    entries: [
      {id: 'chunk', term: 'Chunk', definition: 'Bounded range of dataset cases processed under one worker claim.', scope: 'Many chunks per run; each chunk belongs to one run shard.'},
      {id: 'in-flight-chunk', term: 'In-flight chunk', definition: 'Chunk-ready broker delivery currently being processed by one worker process.', scope: 'Bounded per worker process by `max_inflight_chunks`.'},
      {id: 'prefetch', term: 'Prefetch', definition: 'RabbitMQ limit on unacknowledged deliveries reserved by one consumer.', scope: 'Configured per worker consumer.'},
      {id: 'chunk-parallelism', term: 'Chunk parallelism', definition: 'Number of case executions processed concurrently inside one claimed chunk.', scope: 'Bounded independently from in-flight chunk count.'},
      {id: 'run-shard', term: 'Run shard', definition: 'Stable logical segment numbered `0..127` and stored as `run_shard`; it keeps a chunk and its execution-owned rows together.', scope: 'A run uses only the shards assigned to its chunks.'},
      {id: 'run-snapshot', term: 'Run snapshot', definition: 'Immutable execution-database copy of the run context required for shard-local worker execution.', scope: 'One snapshot per used `run_id + run_shard`.'},
      {id: 'run-shard-summary', term: 'Run shard summary', definition: 'Bounded progress and scorecard rollup built from immutable chunk receipts; sealed after exact planned membership settles.', scope: 'Status and results can show provisional progress. Finalization requires one valid seal per planned shard, with missing and unresolved results explicit.'},
      {id: 'run-creation-plan', term: 'Run creation plan', definition: 'Durable, non-dispatchable control record used to seed exact shard-local chunks and cases and resume interrupted multi-database creation.', scope: 'One creation plan per creating run until all shard-local materialization is verified.'},
      {id: 'control-database', term: 'Control database', definition: 'PostgreSQL role that owns global run state, placement metadata, dispatch cursors, creation plans, and control outbox records.', scope: 'Exactly one active control-capable database placement.'},
      {id: 'execution-database', term: 'Execution database', definition: 'PostgreSQL role that owns shard-local chunks, snapshots, executions, attempts, results, summaries, and chunk-ready outbox records.', scope: 'One or more shard-capable placements; the control database may also serve this role.'},
      {id: 'database-alias', term: 'Database alias', definition: 'Stable name such as `primary` or `shard_001` used instead of a connection URL.', scope: 'One alias per database placement.'},
      {id: 'database-placement', term: 'Database placement', definition: 'Catalog entry that maps a database alias to a secret environment-variable name, role, and status.', scope: 'One row per configured PostgreSQL target.'},
      {id: 'database-placement-status', term: 'Database placement status', definition: 'Admission lifecycle for a PostgreSQL target: `provisioning` is registered but non-routable, `active` accepts and serves ownership, `draining` serves existing ownership only, and `disabled` serves none.', scope: 'One status per database placement; activation verifies readiness before routing.'},
      {id: 'placement-drain', term: 'Placement drain', definition: 'Guarded transition that stops new shard ownership before routes are moved away and a database placement is disabled.', scope: 'The drain does not move rows by itself.'},
      {id: 'database-router', term: 'Database router', definition: 'Process-local `DatabaseRouter` that reads placement metadata and resolves control or execution pools.', scope: 'One lazily initialized router per Vigilo process; it does not choose new shard assignments.'},
      {id: 'database-circuit-breaker', term: 'Database circuit breaker', definition: 'Process-local admission guard that temporarily skips one unavailable database alias without changing durable routing.', scope: 'One independent circuit per contacted execution database alias and process.'},
      {id: 'shard-placement', term: 'Shard placement', definition: 'Control-plane mapping from `run_id + run_shard` to a database alias, lifecycle, route version, and write epoch.', scope: 'One row per used run shard.'},
      {id: 'shard-placement-lifecycle', term: 'Shard placement lifecycle', definition: 'Route movement phase: `active`, `copying`, `draining`, or `moving`.', scope: 'Distinct from database placement status and execution-database local shard admission state.'},
      {id: 'route-hint', term: 'Route hint', definition: 'Message-carried database alias and write epoch used as a fast path to an execution database.', scope: 'Local admission validates the hint, so it is not durable routing authority.'},
      {id: 'execution-route', term: 'Execution route', definition: 'Resolved shard placement plus the PostgreSQL pool for its current database alias.', scope: 'Resolved for one `run_id + run_shard`.'},
      {id: 'route-version', term: 'Route version', definition: 'Monotonically increasing control-plane CAS generation changed by every route alias or lifecycle update.', scope: 'One current value per shard placement; not a schema or deployment version.'},
      {id: 'write-epoch', term: 'Write epoch', definition: 'Monotonically increasing execution-ownership generation carried by routed work and validated in the destination database.', scope: 'Changes only when ownership moves or is restored.'},
      {id: 'local-shard-admission', term: 'Local shard admission', definition: 'Execution-database authority row containing the accepted write epoch and `open`, `draining`, `prepared`, or `closed` state.', scope: 'One row per locally known `run_id + run_shard`; checked in the write transaction.'},
      {id: 'dispatch-cursor', term: 'Dispatch cursor', definition: 'Control-database progress for dispatching one run shard.', scope: 'One cursor per used run shard after creation; `drained` forbids further dispatch.'},
      {id: 'chunk-dispatch-window', term: 'Chunk dispatch window', definition: 'Bounded set of pending chunks selected from one run shard in one dispatch operation.', scope: 'One window can create one `run.chunk.ready` outbox event record per selected chunk.'},
      {id: 'coordinator-cycle', term: 'Coordinator cycle', definition: 'Ordered iteration of creation recovery, lease recovery, chunk dispatch, finalization, and outbox publication.', scope: 'Repeats for `coordinator start`; runs once for `coordinator once`.'},
      {id: 'coordinator-pass', term: 'Coordinator pass', definition: 'One bounded stage within a coordinator cycle, such as dispatch or outbox publication.', scope: 'A pass can visit multiple database aliases.'},
      {id: 'shard-move', term: 'Shard move', definition: 'Targeted relocation of one `run_id + run_shard` route and its shard-owned rows to another database alias.', scope: 'One run shard per move operation.'},
      {id: 'rebalance-plan', term: 'Rebalance plan', definition: 'Persisted set of targeted shard moves for a capacity or placement-drain operation.', scope: 'One plan contains many rebalance items.'},
      {id: 'rebalance-item', term: 'Rebalance item', definition: 'Claimable plan item for moving one specific `run_id + run_shard`.', scope: 'One shard move per item; concurrent apply processes can claim different items.'},
    ],
  },
  {
    id: 'ownership-concurrency',
    entries: [
      {id: 'state', term: 'State', definition: 'Persisted lifecycle value used to determine which transitions are valid.', scope: 'One current lifecycle value per stateful record.'},
      {id: 'transition', term: 'Transition', definition: 'Guarded database change from one state to another.', scope: 'A transition applies only when its authority and current-state predicates hold.'},
      {id: 'owner', term: 'Owner', definition: 'Process or claim that currently has guarded authority to perform a state transition.', scope: 'Ownership is temporary unless represented by durable placement state.'},
      {id: 'claim', term: 'Claim', definition: 'Successful transition that gives a process temporary authority over one work item.', scope: 'Examples include chunk, dispatch-cursor, outbox-delivery, and rebalance-item claims.'},
      {id: 'lease', term: 'Lease', definition: 'Time-bounded claim authority that becomes recoverable after its deadline.', scope: 'Expiry permits recovery but does not alone prevent a stale write.'},
      {id: 'claim-token', term: 'Claim token', definition: 'Opaque value issued with a claim and required to settle or renew that exact claim.', scope: 'A newer claim gets a different token, fencing the previous owner.'},
      {id: 'fencing-token', term: 'Fencing token', definition: 'Value whose equality proves that an owner or route is still current.', scope: 'Checked on protected mutations; claim tokens, route versions, and write epochs fence different ownership boundaries.'},
      {id: 'row-lock', term: 'Row lock', definition: 'PostgreSQL lock on selected table rows, usually held until the transaction ends.', scope: 'PostgreSQL enforces conflicting row-lock modes.'},
      {id: 'admission-lock', term: 'Admission lock', definition: 'Transaction-scoped PostgreSQL advisory lock used cooperatively by shard writers.', scope: 'Normal writers take the shared form; shard movement takes the exclusive form for one `run_id + run_shard`.'},
      {id: 'route-fence', term: 'Route fence', definition: 'Expected database alias, placement status, and route version validated immediately before a routed write.', scope: 'One expected fence per resolved execution route.'},
      {id: 'route-cas', term: 'Route CAS', definition: 'Compare-and-swap update that changes a route only while its stored alias, lifecycle, and route version still match.', scope: 'Serializes concurrent control-plane route changes.'},
      {id: 'compare-and-swap', term: 'Compare-and-swap', definition: 'Update that succeeds only if stored values still equal expected values.', scope: 'Used to change a route or settle a claim without overwriting newer state.'},
      {id: 'idempotent-operation', term: 'Idempotent operation', definition: 'Operation that can be repeated without duplicating its logical effect.', scope: 'Retries may execute more than once while producing one durable outcome.'},
      {id: 'no-op', term: 'No-op', definition: 'Valid path that changes nothing because another process already advanced the state.', scope: 'A no-op is a successful convergence outcome, not necessarily an error.'},
      {id: 'recovery', term: 'Recovery', definition: 'Reassignment or repair after a lease expires or a durable workflow stops mid-operation.', scope: 'Recovery preserves retry limits and invalidates stale owners.'},
      {id: 'stale-attempt', term: 'Stale attempt', definition: 'Attempt that is no longer authoritative because its lease expired, its chunk was recovered, or a later attempt superseded it.', scope: 'Retained for history but rejected as a current writer.'},
    ],
  },
  {
    id: 'events-messaging',
    entries: [
      {id: 'outbox-event-record', term: 'Outbox event record', definition: 'Durable `outbox_events` row inserted in the same database transaction as the state change it describes.', scope: 'One logical event per unique `dedupe_key`.'},
      {id: 'outbox-delivery-row', term: 'Outbox delivery row', definition: 'Temporary publish work in `outbox_delivery_queue`.', scope: 'One active delivery row per unpublished outbox event record.'},
      {id: 'publish-claim', term: 'Publish claim', definition: 'Time-bounded ownership of an outbox delivery row, fenced by `claim_token`.', scope: 'One current publisher claim per delivery row.'},
      {id: 'broker', term: 'Broker', definition: 'RabbitMQ transport that carries published messages from coordinators to workers.', scope: 'Transport is at-least-once; durable authority remains in PostgreSQL.'},
      {id: 'broker-circuit-breaker', term: 'Broker circuit breaker', definition: 'Process-local availability guard that pauses RabbitMQ operations after transport failures and probes for recovery.', scope: 'Keeps broker outages separate from message retry budgets and durable database authority.'},
      {id: 'broker-message', term: 'Broker message', definition: 'RabbitMQ message created from an outbox event record after publication.', scope: 'May be delivered more than once.'},
      {id: 'worker-delivery', term: 'Worker delivery', definition: 'One broker delivery that identifies a chunk for a worker to claim.', scope: 'A delivery is acknowledged, delayed, requeued, or quarantined after processing.'},
      {id: 'event-type', term: 'Event type', definition: 'Semantic event name such as `run.started`, `run.chunk.ready`, or `run.completed`.', scope: 'Stored on the outbox record and used for broker routing.'},
      {id: 'dedupe-key', term: 'Dedupe key', definition: 'Stable identity for one logical outbox event record, also published as the AMQP message ID.', scope: 'Unique in the outbox ledger.'},
      {id: 'at-least-once-delivery', term: 'At-least-once delivery', definition: 'Delivery guarantee that permits redelivery after uncertain acknowledgement.', scope: 'Consumers must use database claims and idempotency guards.'},
      {id: 'publisher-confirm', term: 'Publisher confirm', definition: 'RabbitMQ acknowledgement that a published message reached the broker.', scope: 'Required before the outbox event record is marked published.'},
      {id: 'message-settlement', term: 'Message settlement', definition: 'Worker acknowledgement, delayed redelivery, or requeue decision for one broker delivery.', scope: 'One settlement outcome per received delivery.'},
      {id: 'quarantine', term: 'Quarantine', definition: 'Run-owned holding path for invalid or retry-exhausted worker deliveries that must not re-enter normal processing.', scope: 'Retains the failed delivery for diagnosis without treating it as completed work.'},
    ],
  },
  {
    id: 'performance-testing',
    entries: [
      {id: 'performance-harness', term: 'Performance harness', definition: 'Repository-local `cargo perf` tooling that builds test subjects, executes versioned workloads, validates correctness, measures supported boundaries, and writes evidence.', scope: 'Lives in `xtask` and `performance`; production Vigilo does not depend on it.'},
      {id: 'performance-campaign', term: 'Performance campaign', definition: 'One bounded `cargo perf run` or `cargo perf compare` invocation over a resolved performance profile and one or two test subjects.', scope: 'Its campaign ID and artifacts are separate from a Vigilo evaluation run.'},
      {id: 'performance-profile', term: 'Performance profile', definition: 'Versioned campaign composition selecting exact workload tuples, block counts, timing mode, schedule, and resource limits.', scope: 'Configures the performance harness; distinct from an evaluation run profile and an evaluator artifact build profile.'},
      {id: 'performance-workload', term: 'Performance workload', definition: 'Versioned contract for one supported behavior, including its measured region, allowed tuples, fixture, exact oracle, metrics, limits, and binary capability.', scope: 'A profile selects workloads; it does not implement them.'},
      {id: 'workload-tuple', term: 'Workload tuple', definition: 'One named, exact fixture and configuration shape allowed by a performance workload.', scope: 'Workload ID plus tuple ID identifies one comparison, budget, or model point; tuples are not command instructions or an expanded Cartesian product.'},
      {id: 'exact-correctness-oracle', term: 'Exact correctness oracle', definition: 'Required post-execution validation of the expected process, output, service, and durable effects of a performance sample.', scope: 'A sample contributes timing only after its oracle passes; this is distinct from a dataset case\'s optional expected output.'},
      {id: 'performance-test-subject', term: 'Performance test subject', definition: 'Exact release executable and frozen setup assets produced together by `cargo perf build` for later measurement.', scope: 'Created outside the measured interval and reused by `run` or `compare`; creating it does not run a performance test.'},
      {id: 'performance-build-manifest', term: 'Performance build manifest', definition: 'Versioned provenance and compatibility record for a test subject\'s executable digest, source revision, toolchain, dependencies, setup assets, and workload capabilities.', scope: 'Stored as `build-manifest.json`; distinct from Cargo and evaluator manifests.'},
      {id: 'measured-sample', term: 'Measured sample', definition: 'One scheduled execution with process, resource, and exact external observations plus a validation classification.', scope: 'Readiness and preconditioning observations are recorded separately and excluded from timing statistics.'},
      {id: 'counterbalanced-block', term: 'Counterbalanced block', definition: 'Four baseline/candidate executions ordered as `ABBA` or `BAAB` to estimate change while controlling position effects.', scope: 'Opposite orientations are paired; blocks, not executions inside one block, are the independent sampling basis.'},
      {id: 'informative-timing', term: 'Informative timing', definition: 'Measured performance evidence reported without a numerical regression gate.', scope: 'Useful for diagnosis and canaries, but it cannot establish that performance is unchanged.'},
      {id: 'practical-performance-budget', term: 'Performance budget', definition: 'Reviewed practical maximum harmful relative effect for one canonical environment, workload tuple, and metric, with a required minimum block count.', scope: 'Noise calibration tests whether the host can resolve the budget; calibration does not invent or raise it.'},
      {id: 'gating-profile', term: 'Gating profile', definition: 'Performance profile whose workloads all use gating timing and resolve exact entries from one published budget policy.', scope: 'Only compatible canonical evidence can produce its numerical regression verdicts.'},
      {id: 'noise-calibration', term: 'Noise calibration', definition: 'Canonical same-build comparison used to estimate environmental and measurement variation, repeatability, power, and required block counts.', scope: 'Supports review of practical budgets; it remains separate from capacity calibration.'},
      {id: 'canonical-environment', term: 'Canonical environment', definition: 'Externally validated exclusive host satisfying a versioned performance environment contract.', scope: 'Only matching canonical evidence may support published blocking performance verdicts.'},
      {id: 'performance-baseline', term: 'Performance baseline', definition: 'Immutable provenance index binding reviewed noise and capacity evidence, a budget policy, a gating profile, and one build manifest.', scope: 'Distinct from the baseline executable role in an individual A/B comparison.'},
      {id: 'performance-verdict', term: 'Performance verdict', definition: 'Disposition derived from correctness, evidence validity, confidence bounds, and any applicable budget.', scope: 'Values are `pass`, `regression`, `improvement`, `informative`, `inconclusive`, or `invalid`; a first over-budget interval is inconclusive until an independent matching confirmation.'},
      {id: 'capacity-staircase', term: 'Capacity staircase', definition: 'Single-build campaign that holds worker count fixed while increasing offered load through registered steps.', scope: 'Measures bounded one- and two-worker behavior; it is separate from fixed-load A/B regression testing.'},
      {id: 'worker-knee', term: 'Worker knee', definition: 'First valid load step where throughput flattens while latency grows materially, or normalized per-worker CPU reaches its reviewed ceiling.', scope: 'Shared-service saturation invalidates the point; if no knee appears, only the highest observed rate lower bound is reported.'},
      {id: 'component-scaling-model', term: 'Component scaling model', definition: 'Accepted fixed-cost-plus-slope or explicit stepped relationship fitted from repeated valid component samples.', scope: 'Incomplete repetitions, exact-count drift, negative coefficients, nonlinearity, or excessive residuals reject the model.'},
      {id: 'model-point', term: 'Model point', definition: 'Registered mapping from one workload tuple to a positive scaling input and exact external observations.', scope: 'Every tuple in a modeled workload must have exactly one point.'},
      {id: 'capacity-projection', term: 'Capacity projection', definition: 'Estimate that combines bounded measured capacity with a named deployment\'s workload, topology, amplification, and independently sourced limits.', scope: 'Confidence is `invalid`, `directional`, `planning`, or `calibrated`; it does not claim unmeasured fleet capacity.'},
      {id: 'amplification', term: 'Amplification', definition: 'Ratio of attempts, deliveries, events, acknowledgements, retries, or other external work to intended useful work.', scope: 'Exact and bounded amplification detects duplicate or runaway work independently from timing.'},
      {id: 'reliability-run', term: 'Reliability run', definition: 'Single-build operational campaign that checks sustained progress or controlled dependency recovery with resident processes.', scope: 'Soak and recovery evidence uses safety bounds and does not enter fixed-load A/B statistics.'},
      {id: 'performance-diagnostics', term: 'Performance diagnostics', definition: 'Post-timing PostgreSQL statement, planning, buffer, and WAL evidence rendered for investigation.', scope: 'Diagnostics never change correctness, comparison, budget, or component-model verdicts.'},
    ],
  },
  {
    id: 'evaluator-packaging',
    entries: [
      {id: 'wit', term: 'WIT', definition: 'Wasm Interface Type language used to define Component Model interfaces and worlds.', scope: 'Versioned evaluator contracts live under `wit/evaluator/<version>/evaluator.wit` and become immutable when released.'},
      {id: 'wit-world', term: 'WIT world', definition: 'WIT boundary that groups the evaluator\'s imported and exported interfaces.', scope: 'The evaluator implements `evaluator-world`.'},
      {id: 'evaluator-abi', term: 'Evaluator ABI', definition: 'Exact versioned WIT binary contract implemented by an evaluator WebAssembly component.', scope: 'Identified by package, world, interface, version, and immutable contract hash.'},
      {id: 'evaluator-adapter', term: 'Evaluator host adapter', definition: 'Version-specific host binding that validates, invokes, and maps one supported evaluator ABI.', scope: 'Selected from the run\'s immutable execution plan.'},
      {id: 'evaluator-execution-plan', term: 'Evaluator execution plan', definition: 'Hashed run snapshot of the exact evaluator ids, artifact hashes, ABI identities, adapters, runtime versions, and scoring policy hash.', scope: 'Frozen once per run and verified by every worker placement.'},
      {id: 'wasi-preview-2', term: 'WASI 0.2 (Preview 2)', definition: 'Stable Component Model-based WASI release targeted by evaluator artifacts.', scope: 'Rust target `wasm32-wasip2`; Preview 2 is the earlier name for WASI 0.2.'},
      {id: 'evaluator-artifact', term: 'Evaluator artifact', definition: 'Compiled WebAssembly component stored in the evaluator registry.', scope: 'One immutable artifact content per evaluator identifier.'},
      {id: 'evaluator-registry', term: 'Evaluator registry', definition: 'Durable catalog of published evaluator identities, metadata, contracts, and artifact content.', scope: 'Contains many versioned evaluators.'},
      {id: 'evaluator-state', term: 'Evaluator state', definition: 'Registry lifecycle value: `active`, `yanked`, `deprecated`, `disabled`, or `removed`.', scope: 'One current state per published evaluator identity.'},
      {id: 'package-manifest', term: 'Package manifest', definition: '`Cargo.toml` file that supplies evaluator crate identity and version.', scope: 'One Cargo manifest per evaluator crate.'},
      {id: 'evaluator-manifest', term: 'Evaluator manifest', definition: '`Vigilo.toml` file describing artifact paths, WIT expectations, and publish metadata.', scope: 'One per evaluator package.'},
      {id: 'wit-contract', term: 'WIT contract', definition: 'Immutable versioned interface definition that a compiled evaluator component must implement.', scope: 'Validated by declaration, contract hash, and typed component linking.'},
      {id: 'build-profile', term: 'Build profile', definition: 'Named artifact selection in `Vigilo.toml`, such as `dev` or `release`.', scope: 'Selects a build output; not an evaluation run profile.'},
      {id: 'wasm-store', term: 'Wasm store', definition: 'Fresh Wasmtime execution state created for one evaluator invocation.', scope: 'One isolated store per invocation.'},
      {id: 'fuel', term: 'Fuel', definition: 'Deterministic Wasmtime instruction budget for one evaluator invocation.', scope: 'Exhaustion interrupts evaluator execution.'},
      {id: 'evaluator-semaphore', term: 'Evaluator semaphore', definition: 'Process-local cap on concurrently active Wasm evaluator invocations.', scope: 'One shared semaphore per worker process.'},
      {id: 'publish', term: 'Publish', definition: 'Validate and insert a versioned evaluator artifact into the evaluator registry.', scope: 'Publishing is immutable for one evaluator identifier.'},
    ],
  },
] as const;

const GLOSSARY_ID_PATTERN = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;

function validateGlossary(sections: readonly GlossarySection[]): void {
  const terms = new Set<string>();

  for (const section of sections) {
    if (!GLOSSARY_ID_PATTERN.test(section.id)) {
      throw new Error(`Invalid glossary section ID: ${section.id}`);
    }

    for (const entry of section.entries) {
      if (!GLOSSARY_ID_PATTERN.test(entry.id)) {
        throw new Error(`Invalid glossary entry ID: ${entry.id}`);
      }
      if (![entry.term, entry.definition, entry.scope].every((value) => value.trim())) {
        throw new Error(`Incomplete glossary entry: ${entry.id}`);
      }

      const normalizedTerm = entry.term.toLocaleLowerCase('en-US');
      if (terms.has(normalizedTerm)) {
        throw new Error(`Duplicate glossary term: ${entry.term}`);
      }
      terms.add(normalizedTerm);
    }
  }
}

function indexById<T extends {id: string}>(values: readonly T[], label: string): Map<string, T> {
  const index = new Map<string, T>();
  for (const value of values) {
    if (index.has(value.id)) {
      throw new Error(`Duplicate ${label} ID: ${value.id}`);
    }
    index.set(value.id, value);
  }
  return index;
}

validateGlossary(glossarySections);

export const glossaryEntries = indexById(
  glossarySections.flatMap((section) => section.entries),
  'glossary entry',
);

export const glossarySectionById = indexById(glossarySections, 'glossary section');
