CREATE TYPE evaluator_state AS ENUM (
    'active', -- Selectable and usable by default.
    'yanked', -- Not selected for new use, but existing/pinned users can still use it.
    'deprecated', -- Still usable, but discouraged.
    'disabled', -- Unavailable for execution, but still present in record.
    'removed' -- Tombstone-only state for admin/legal workflows; not normal deletion.
);

COMMENT ON TYPE evaluator_state IS
    'Lifecycle state of an evaluator artifact aligned to package-management semantics.';

CREATE TYPE evaluator_http_policy_action AS ENUM (
  'allow',
  'deny'
);

COMMENT ON TYPE evaluator_http_policy_action IS
    'URI policy action for evaluator outbound HTTP requests; allow=whitelist, deny=blacklist.';

CREATE TYPE run_status AS ENUM (
  'creating', -- Run creation is durably planned but one or more execution placements still need to be seeded.
  'pending', -- Run has been created but no executions have been dispatched yet.
  'running', -- Executions have been dispatched and are actively being processed by workers.
  'finalizing', -- All executions are terminal; run-level aggregation, summary computation, and event emission are in progress.
  'completed', -- Run has finished successfully with a final gate decision (pass or fail) and no remaining work.
  'failed', -- Run failed due to a system-level issue (e.g., infrastructure error, unrecoverable failure), not just failing evaluations.
  'cancelled' -- Run was intentionally stopped before completion; some executions may be incomplete or abandoned.
);

COMMENT ON TYPE run_status IS
    'Lifecycle state of an evaluation run, including durable creation, execution, finalization, and terminal outcomes.';

CREATE TYPE gate_status AS ENUM (
  'unknown', -- Gate result has not yet been determined. Run is still in progress or has not been evaluated.
  'pass', -- Run met all evaluation criteria and policy thresholds. Safe to proceed with downstream actions such as deployment.
  'fail' -- Run did not meet evaluation criteria or violated blocking conditions. Should prevent downstream actions such as deployment.
);

COMMENT ON TYPE gate_status IS
    'Final evaluation outcome of a run based on aggregation policy. Separate from run lifecycle (run_status).';

CREATE TYPE execution_status AS ENUM (
  'pending', -- Execution has been created but not yet claimed or started by a worker.
  'running', -- Execution is actively being processed by a worker, including agent invocation and evaluator execution.
  'awaiting_evaluators', -- Agent output has been produced, but evaluator results are not yet complete or fully persisted.
  'retry_scheduled', -- Execution previously failed but is eligible for retry; a future attempt is expected.
  'completed', -- Execution finished successfully with all required evaluators completed and an aggregate result produced.
  'failed', -- Execution reached a terminal failure state after exhausting retries or encountering a non-recoverable error.
  'timed_out', -- Execution did not complete within allowed time bounds and was marked terminal.
  'cancelled' -- Execution was intentionally stopped before completion, typically due to run cancellation.
);

COMMENT ON TYPE execution_status IS
    'Lifecycle state of a single execution (one dataset case). Tracks progress from scheduling through agent execution, evaluation, and terminal outcome.';

CREATE TYPE attempt_status AS ENUM (
  'pending', -- Attempt has been created but not yet claimed by a worker. No work has started.
  'running', -- Attempt is actively being processed by a worker. The worker is expected to hold a valid lease and may be invoking the agent and running evaluators.
  'completed', -- Attempt finished successfully with agent execution and evaluation complete. Results should be fully persisted.
  'failed_agent_call', -- Attempt failed during agent invocation (e.g., HTTP error, timeout, or upstream failure) before evaluation could complete.
  'failed_evaluation', -- Attempt produced agent output but failed during evaluator execution or result persistence.
  'timed_out', -- Attempt exceeded allowed execution time or lease duration and was marked as terminal. Worker may have crashed or become unresponsive.
  'cancelled', -- Attempt was intentionally terminated before completion, typically due to run cancellation or manual intervention.
  'stale' -- Attempt is no longer authoritative due to lease expiration or reassignment. A newer attempt is expected to replace it.
);

COMMENT ON TYPE attempt_status IS
    'Lifecycle state of a single execution attempt. Represents one worker-owned attempt to process an execution, including leasing, execution, evaluation, and termination conditions.';

CREATE TYPE evaluation_status AS ENUM (
  'passed', -- Evaluator determined the output meets the expected criteria or constraints for this check.
  'failed', -- Evaluator determined the output violates expected criteria, constraints, or correctness requirements.
  'error', -- Evaluator failed to produce a valid result due to an internal error, runtime failure, or invalid input. Does not necessarily indicate the output itself is incorrect.
  'skipped' -- Evaluator was not applied to this execution, typically due to conditional logic, unsupported input, or configuration rules.
);

COMMENT ON TYPE evaluation_status IS
    'Outcome of a single evaluator applied to an execution. Represents the evaluator’s judgment, independent of aggregation or overall execution result.';

CREATE TYPE severity AS ENUM (
  'none', -- No issue detected or not applicable. Used when the evaluator does not report any concern.
  'low', -- Minor issue with limited impact. Typically acceptable depending on policy thresholds.
  'medium', -- Moderate issue that may affect correctness, quality, or user experience. Often requires attention or may trigger policy conditions.
  'high', -- Serious issue with significant impact. Likely to contribute to execution failure or policy violations.
  'critical' -- Severe issue indicating unacceptable behavior (e.g., safety violations, major correctness failures). Typically should block deployment regardless of other scores.
);

COMMENT ON TYPE severity IS
    'Indicates the magnitude or impact of an evaluator finding. Used to qualify failures or issues beyond simple pass/fail outcomes.';

CREATE TYPE evaluator_outcome AS ENUM (
  'completed',
  'error',
  'abstained'
);

COMMENT ON TYPE evaluator_outcome IS
    'Execution outcome of one evaluator invocation, independent from the host-derived quality judgment.';

CREATE TYPE outbox_status AS ENUM (
  'pending', -- Event has been created and is ready to be published, but has not yet been processed by the outbox publisher.
  'published', -- Event has been successfully delivered to the external system or message broker.
  'failed' -- Event publication attempt failed. The event may be retried depending on retry policy and error handling.
);

COMMENT ON TYPE outbox_status IS
    'Represents the delivery state of an outbox event. Used to ensure reliable, idempotent publication of events to external systems.';
