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
