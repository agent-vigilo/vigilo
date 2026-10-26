# Dev Infrastructure

This directory defines two local Docker Compose topologies:

| Topology | Env file | Compose files |
| --- | --- | --- |
| Single database | `infra/dev/.env.single` | `infra/dev/docker-compose.single.yml` |
| Sharded database | `infra/dev/.env.sharded` | `infra/dev/docker-compose.single.yml` + `infra/dev/docker-compose.sharded.yml` |

There is no default `.env` file in this directory. Always pass `--env-file`
so the active topology is explicit.

Compose is idempotent for the same project and file set. Running the same
`docker compose up` command again reuses or recreates the same services; it
does not create a second copy of `postgres`, `coordinator`, or `worker`.

## Service Groups

- Infrastructure: `postgres`, `postgres-shard-001`, `rabbitmq`, `ceph`
- Bootstrap: `setup`, `setup-shard-001`, `configure-shard-001`
- Runtime: `agent`, `coordinator`, `worker`

Bootstrap services are one-shot jobs. Runtime services depend on bootstrap, so
starting `coordinator` or `worker` may also start setup/config services.
In the sharded topology, shard setup waits for primary setup so the shared
Cargo cache is not written by two setup jobs at once.

Compose passes configuration by service responsibility: all Vigilo services
receive database-client settings, only setup receives placement-bootstrap
settings, only the coordinator receives creation and coordinator settings, and
only coordinator and worker services receive `MESSAGING_URL`. This mirrors the
CLI command scopes and prevents unrelated configuration from becoming a
startup dependency.

## Agent Model

The `agent` service runs llama.cpp and expects this host file:

```text
models/qwen2.5-0.5b-instruct-q4_k_m.gguf
```

Set `VIGILO_AGENT_MODEL` in the selected env file when using a different GGUF
filename. Start database-only or bootstrap-only commands when you do not need
the local agent container.

## Single Database

Start Postgres, RabbitMQ, and local S3 storage:

```bash
docker compose --env-file infra/dev/.env.single \
  -f infra/dev/docker-compose.single.yml \
  up -d postgres rabbitmq ceph
```

Run setup:

```bash
docker compose --env-file infra/dev/.env.single \
  -f infra/dev/docker-compose.single.yml \
  up setup
```

Start the full single-database runtime:

```bash
docker compose --env-file infra/dev/.env.single \
  -f infra/dev/docker-compose.single.yml \
  up -d
```

Host-side commands use published ports:

```bash
export DATABASE_URL=postgresql://postgres:password@localhost:5432/agent_vigilo
export MESSAGING_URL=amqp://guest:guest@localhost:5672
```

## Sharded Database

Start Postgres primary, Postgres shard, RabbitMQ, and local S3 storage:

```bash
docker compose --env-file infra/dev/.env.sharded \
  -f infra/dev/docker-compose.single.yml \
  -f infra/dev/docker-compose.sharded.yml \
  up -d postgres postgres-shard-001 rabbitmq ceph
```

Bootstrap both databases and register `shard_001` in the control DB:

```bash
docker compose --env-file infra/dev/.env.sharded \
  -f infra/dev/docker-compose.single.yml \
  -f infra/dev/docker-compose.sharded.yml \
  up setup setup-shard-001 configure-shard-001
```

Start the full sharded runtime:

```bash
docker compose --env-file infra/dev/.env.sharded \
  -f infra/dev/docker-compose.single.yml \
  -f infra/dev/docker-compose.sharded.yml \
  up -d
```

Host-side tests and CLI commands use published ports:

```bash
export DATABASE_URL=postgresql://postgres:password@localhost:5432/agent_vigilo
export VIGILO_TEST_SHARD_001_DATABASE_URL=postgresql://postgres:password@localhost:5433/agent_vigilo
export VIGILO_SHARD_001_DATABASE_URL=postgresql://postgres:password@localhost:5433/agent_vigilo
export MESSAGING_URL=amqp://guest:guest@localhost:5672
```

Run integration tests against infrastructure and bootstrap services only. Stop
runtime services first so `coordinator` and `worker` do not mutate test runs:

```bash
docker compose --env-file infra/dev/.env.sharded \
  -f infra/dev/docker-compose.single.yml \
  -f infra/dev/docker-compose.sharded.yml \
  stop agent coordinator worker
```

Run the CI-equivalent routing test after exporting the host-side variables:

```bash
cargo test -p vigilo --locked --test multi_database_routing -- --nocapture
```

Run the end-to-end multi-database harness when RabbitMQ is also running and the
bundled evaluator WASM has been built:

```bash
cargo build -p sentiment-basic-en --target wasm32-wasip2 --release --locked

VIGILO_E2E_MULTI_DATABASE=1 \
cargo test -p vigilo --locked --test multi_database_e2e -- --nocapture
```

The E2E harness uses a per-run `VIGILO_MQ_NAMESPACE`, so it does not consume
messages left by a previous local runtime stack.

## Local Object Storage

Both topologies include [Ceph Object Gateway](https://docs.ceph.com/en/tentacle/radosgw/)
backed by Ceph `v20.2.4`. A development image adds a pinned Python AWS SDK for S3
bucket bootstrap, readiness, and infrastructure tests. The container supervises
one monitor, manager, BlueStore OSD, and S3 gateway. It creates a local S3 user and
the bucket configured by `DEV_S3_ACCESS_KEY_ID`, `DEV_S3_SECRET_ACCESS_KEY`, and
`DEV_S3_BUCKET` in the selected env file. These are public development credentials.

Storage uses an 8 GiB sparse BlueStore file in the `agent_vigilo_ceph_data` named
volume; actual disk usage grows as data is written. No host disks, privileged
container, or host Ceph installation are required. The OSD targets 1 GiB memory;
allow approximately 4 GiB for the complete development service. The single-node,
single-copy layout provides no storage redundancy and is not a production
configuration. Ceph may report development-layout health warnings; Compose
readiness uses the signed S3 bucket check.

- Host endpoint: `http://localhost:3900` (override `DEV_S3_PORT` if occupied).
- Container endpoint: `http://ceph:3900` on the Compose network.
- Default bucket: `vigilo-dev`.
- Clients use S3 path-style addressing and region `us-east-1`.

Start just object storage, without databases, Vigilo bootstrap jobs, or an agent
model (the first start builds the development image):

```bash
docker compose --env-file infra/dev/.env.single \
  -f infra/dev/docker-compose.single.yml \
  up -d ceph --build --wait --wait-timeout 240
```

The startup script uses a shared readiness deadline, reuses persisted cluster
identity, and checks stored user credentials. A daemon exit stops its peers;
shutdown waits for bounded cleanup. The healthcheck uses the AWS SDK to verify
that the configured credentials can access the configured bucket over S3.

Run the bounded S3 integration smoke suite:

```bash
docker compose --env-file infra/dev/.env.single \
  -f infra/dev/docker-compose.single.yml \
  exec -T ceph python3.12 /opt/vigilo-ceph/test-s3.py
```

Run the Rust adapter acceptance suite from the host after starting Ceph:

```bash
. infra/dev/.env.single
export VIGILO_BLOB_TEST_ENDPOINT="http://127.0.0.1:$DEV_S3_PORT"
export VIGILO_BLOB_TEST_BUCKET="$DEV_S3_BUCKET"
export VIGILO_BLOB_TEST_ACCESS_KEY_ID="$DEV_S3_ACCESS_KEY_ID"
export VIGILO_BLOB_TEST_SECRET_ACCESS_KEY="$DEV_S3_SECRET_ACCESS_KEY"
export VIGILO_BLOB_TEST_INVALID_SECRET_ACCESS_KEY=deliberately-invalid
cargo test -p vigilo --locked --features blob-storage-services \
  --test blob_storage_services -- --nocapture
```

These variables select the test endpoint; they do not configure runtime commands.
The suite creates unique namespaces, checks real signing and immutable replay,
injects bounded transport faults, and removes only its exact keys. Missing test
configuration fails rather than silently skipping. `ceph_fault_contract` requires
the disposable local HTTP endpoint.

To verify service restart persistence, set `VIGILO_BLOB_TEST_RESTART_CONTAINER`
to the exact disposable Ceph container name and select `signed_storage_contract`.
This test restarts that container, so run it without other users of that service.
For AWS acceptance, set the endpoint variable to `aws`, select
`signed_storage_contract`, and supply a disposable general-purpose bucket,
`VIGILO_BLOB_TEST_REGION`, and credentials scoped to that test namespace. Temporary
credentials also use `VIGILO_BLOB_TEST_SESSION_TOKEN`. Do not set the restart
container variable for AWS. A local Ceph pass does not establish AWS acceptance
or production Ceph redundancy.

With the AWS CLI installed, connect from the repository root:

```bash
. infra/dev/.env.single
export AWS_ACCESS_KEY_ID="$DEV_S3_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$DEV_S3_SECRET_ACCESS_KEY"
export AWS_DEFAULT_REGION=us-east-1

aws --endpoint-url "http://localhost:$DEV_S3_PORT" \
  s3api list-objects-v2 --bucket "$DEV_S3_BUCKET" --max-keys 10
```

Use `.env.sharded` and the sharded overlay instead for that topology. The selected
env file supplies Compose settings; it does not automatically export credentials
to host clients. Data survives container recreation and ordinary `down`;
`down -v` deletes it. Changing credentials for an initialized volume fails
startup rather than silently rotating the existing user's keys.

Ceph supplies local object storage for immutable case batches and manifests.
`DEV_S3_*` settings configure the development endpoint and its credentials.
Each provider is validated through the same
[storage contract](https://agentvigilo.com/docs/architecture/case-storage/);
local smoke tests exercise the Ceph endpoint.

## Runtime Dependencies

This command starts the runtime services:

```bash
docker compose --env-file infra/dev/.env.sharded \
  -f infra/dev/docker-compose.single.yml \
  -f infra/dev/docker-compose.sharded.yml \
  up -d agent coordinator worker
```

Compose may also start `setup`, `setup-shard-001`, `configure-shard-001`,
`postgres`, `postgres-shard-001`, and `rabbitmq` because they are dependencies.
That is expected. `postgres` and `postgres-shard-001` are shared database
services, not separate databases for the coordinator and worker.

Inside containers, database URLs use Compose DNS names:

```text
postgres:5432
postgres-shard-001:5432
rabbitmq:5672
```

Host-side tests and CLI commands use `localhost` and the published ports.

## Reset

Use the same file set for shutdown that you used for startup.

Single database:

```bash
docker compose --env-file infra/dev/.env.single \
  -f infra/dev/docker-compose.single.yml \
  down --remove-orphans
```

Sharded database:

```bash
docker compose --env-file infra/dev/.env.sharded \
  -f infra/dev/docker-compose.single.yml \
  -f infra/dev/docker-compose.sharded.yml \
  down --remove-orphans
```

Add `-v` when you need fresh databases, queues, and object storage:

```bash
docker compose --env-file infra/dev/.env.sharded \
  -f infra/dev/docker-compose.single.yml \
  -f infra/dev/docker-compose.sharded.yml \
  down -v --remove-orphans
```

If a setup job failed while Cargo was downloading crates, clear only the Cargo
registry cache and rerun setup:

```bash
docker compose --env-file infra/dev/.env.sharded \
  -f infra/dev/docker-compose.single.yml \
  -f infra/dev/docker-compose.sharded.yml \
  down --remove-orphans

docker volume rm agent_vigilo_dev_agent_vigilo_cargo_registry
```
