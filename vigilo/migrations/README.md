# Database schema

`vigilo setup` applies the numbered SQL files embedded in the executable in
dependency order. `--migrations-dir` explicitly selects a directory instead.
SQLx locks migration execution and verifies applied checksums before applying
pending migrations transactionally. Use `--skip-evaluators` for database-only setup; default setup also installs the official core evaluator collection. Every database
placement uses the same schema; runtime routing determines which records each
placement owns.

| Files | Responsibility |
| --- | --- |
| 0001–0002 | PostgreSQL extension and shared enum types |
| 0003–0005 | Database placement catalog and evaluator registry |
| 0006–0009 | Dataset identities, upload ownership, object checkpoints, and finalized catalog |
| 0010–0015 | Runs, immutable plan metadata, shard routes, and dispatch cursors |
| 0016–0023 | Local run context, write admission, chunks, executions, attempts, and evaluation evidence |
| 0024–0027 | Immutable chunk settlements, receipt consumption, shard summaries, and run scorecards |
| 0028–0029 | Durable outbox events and partitioned delivery queue |
| 0030–0035 | Shard move ownership, copy progress, local change tracking, and rebalance operations |
| 0036 | Shared change-capture function and triggers for shard moves |
| 0037 | Durable execution-control delivery records |

Each table has one `create_<table>.sql` file containing its complete definition,
partitions, indexes, comments, and associated integrity functions/triggers.
Foreign-key targets precede their dependents. Shared shard-move capture is
installed after all participating tables exist. The outbox enqueue trigger lives
with the delivery queue it populates; settled-chunk protection lives with the
settlement receipts it checks.

This is a greenfield schema: define columns and constraints directly in their
owning `CREATE TABLE`, without historical backfills or upgrade-only patches.
The primary database placement is seeded during setup; application workflows
create dataset, run, dispatch, and execution records.

Runtime ownership and routing are documented in
[Scale-out and shard migration](https://agentvigilo.com/docs/architecture/scaling).


The database-placement migration defines `vigilo_lock_database_placement(text)`.
It returns one alias's role/status and retains a shared row lock until the caller
transaction ends. Activation uses this restricted read because PostgreSQL row
locking requires UPDATE privilege; runtime callers cannot edit the catalog.
The function uses schema-qualified references, a fixed search path, and no PUBLIC
execute grant. Its dedicated non-login owner needs schema usage and SELECT/UPDATE
on database placements; the scheduler receives only an explicit execute grant.

The dispatch-cursor migration also defines `vigilo_claim_dispatch(text[])` in the installation schema.
Its table references are bound to that schema at installation. It selects and locks at most one eligible cursor in the caller transaction using
`SKIP LOCKED`; commit or rollback releases that lock. It accepts at most 128
excluded placement aliases, rejects missing/null lists, and returns routing and
ownership identifiers only. The function uses qualified tables and a fixed search
path, and has no PUBLIC execute grant. Administrative deployments assign a dedicated
non-login function owner and explicitly grant callers execution. The function owner
needs schema usage, SELECT on runs, shard placements, database placements and
dispatch cursors, and UPDATE on dispatch cursors for row locking. Deployments with
row policies must also authorize the owner's bounded discovery. Granting function
execution does not grant callers direct access to those tables.


The run-plan migration defines `vigilo_ready_admissions(integer)` and
`vigilo_claim_admission(uuid, uuid)` in the installation schema. Discovery returns
at most 1024 eligible accepted-run IDs. Claiming locks the run, rechecks the exact
accepted plan, execution control and lease expiry, and atomically returns ownership
and the advanced fencing generation. A live lease, paused/revoked run, or unaccepted
plan cannot be claimed. Caller rollback rolls back the claim and generation.
Both functions have fixed search paths, schema-bound references, and no PUBLIC
execute grant. An administrative deployment assigns a non-login owner with schema
usage and SELECT/UPDATE on runs and run plans, plus appropriate row-policy access;
callers receive only explicit execute grants. Discovery alone grants no lease.

Triggers attached to these writes must qualify their relation references or set
their own safe search path; they must not rely on an ambient application search path.


The control-delivery migration defines `vigilo_claim_control_deliveries(uuid)` in
its installation schema. A supplied run ID narrows selection; NULL selects eligible
work across runs. It atomically claims at most 32 available delivery rows with
60-second leases and fresh tokens, skipping locked rows. Application and quiescence
remain distinct; applied but nonquiescent paused/revoked work remains retryable.
The function has a fixed search path, schema-bound references, and no PUBLIC execute
grant. Administrative deployments assign a non-login owner with schema usage and
SELECT/UPDATE on control deliveries (and appropriate row-policy access), then grant
execution explicitly. Claim permission does not grant direct caller table access.


The control-delivery migration also defines `vigilo_claim_revocation_settlements(uuid)`.
A supplied run ID narrows selection; NULL selects eligible runs. It claims at most
16 revoked runs whose current revision is not settled and whose recorded delivery
destinations all have matching revisions and quiescence acknowledgments. Live
settlement leases are excluded. Claims use fresh tokens, 60-second leases and
`SKIP LOCKED`. Each claim returns the run ownership identity with its revision and
token so cleanup does not need an ownership lookup. Cleanup and its
revision/token-fenced acknowledgment use separate scoped transactions.
The function has schema-bound references, a fixed search path and no PUBLIC execute
grant. Its administrative non-login owner needs schema usage, SELECT/UPDATE on runs,
and SELECT on control deliveries, plus appropriate row-policy access. Callers need
only an explicit execute grant for claiming, not direct table access.

The chunk migration defines `vigilo_claim_chunk_recovery(integer,bigint)`.
It discovers bounded expired chunks in each retry class and acquires shared
transaction-level movement locks before returning distinct run/shard identities with ownership.
Recovery writes follow in a separate statement, rechecking current ownership.
The function binds its table to the installation schema, fixes its search path,
and denies public execution. An explicitly granted caller needs no table access
for discovery; a limited non-login owner needs SELECT on `run_chunks`.
Recovery binds the exact claimed run/owner map in the retained transaction before
its batched writes and terminal settlement. Receipt ownership comes from the local
snapshot; empty discovery performs no follow-up queries.

Finalization uses two restricted scheduler functions:
`vigilo_next_finalization_candidate(uuid[])` in the dispatch-cursor migration
selects one eligible run, preserving heartbeat ordering and cycle exclusions;
`vigilo_claim_finalization(uuid,uuid,integer)` in the run migration acquires its
coordinator lease with `SKIP LOCKED`. Each returns authoritative ownership in
one client statement. Scoped route and summary reads release their transactions
before remote work. Candidate heartbeat updates use their own scope; the terminal
transaction binds the claimed identity and atomically writes the root, scorecard
and completion event. Scorecard and event ownership come from the locked root.
Both bind relations to the installation schema, fix their search path, validate
bounded inputs, and deny public execution. The non-login discovery owner needs
SELECT on runs and dispatch cursors; the claim owner needs SELECT and UPDATE on
runs. Explicit execute grants do not give callers table access. Summary reads,
fairness updates, backlog metrics and final result writes retain separate authority.

The delivery-queue migration defines `vigilo_claim_outbox(bigint,integer)`.
It claims a bounded available batch with `SKIP LOCKED`, assigns fresh tokens,
increments attempt counts and returns the event payloads in one client statement.
The function binds its tables to the installation schema, fixes its search path
and denies public execution. A limited non-login owner needs SELECT on the ledger
and queue, and UPDATE on the queue. Explicit caller grants permit claims without
table access. Publication acknowledgments, rescheduling and backlog metrics retain
their separate authority.

The shard-summary migration defines `vigilo_pending_aggregation(bigint)`.
It returns a bounded, ordered list of run/shard identities, ownership and current
write epochs needing receipt aggregation or final verification. Discovery is one
client statement and does not claim pages or mutate progress. Its limited
non-login owner needs SELECT on summaries, local admissions and settlement
consumptions. The function binds relations to the installation schema, fixes its
search path and denies public execution. Page claims, progress writes and
candidate rotation bind the discovered identity in their own transactions. Page
context shares the existing claim submission; rotation releases page locks before
its separate scheduling update.

Scheduler-wide reads use restricted, read-only functions: the dispatch-cursor
migration owns `vigilo_dispatch_backlog()` and `vigilo_finalization_backlog()`;
the delivery-queue migration owns `vigilo_outbox_backlog()`; the shard-placement
migration owns `vigilo_execution_aliases()` and `vigilo_unserviceable_routes()`.
They return aggregate counts/lag or deployment aliases, preserve existing filters,
and each replace one client statement. Exact counts still scan eligible rows;
a small result is not a constant-time workload. All bind relations to their
installation schema, fix the search path and deny public execution. Function
owners require only SELECT on the referenced relations; callers require explicit
EXECUTE grants, not work-table access.
