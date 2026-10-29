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
