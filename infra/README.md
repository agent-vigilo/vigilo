# Infrastructure

System components for running Agent Vigilo.

## Dev Compose

`infra/dev/docker-compose.single.yml` starts Postgres, RabbitMQ, local S3-compatible object storage provided by Ceph Object Gateway, Vigilo services, and a local llama.cpp-backed agent service for the example profile. Add `infra/dev/docker-compose.sharded.yml` for a second PostgreSQL shard placement. See [local object storage](dev/README.md#local-object-storage) for endpoints and credentials.

The agent service expects this model file to exist before startup:

```text
models/qwen2.5-0.5b-instruct-q4_k_m.gguf
```

Set `VIGILO_AGENT_MODEL` in `infra/dev/.env.single` or
`infra/dev/.env.sharded` when using a different GGUF filename.

## Object Storage Contract

The [application blob-storage contract](https://agentvigilo.com/docs/architecture/case-storage/)
is the source of truth for identity, durable references, bounded I/O, publication,
and adapter acceptance.

Application storage is provider-neutral; adapters own provider APIs and connection
settings. The adapter boundary targets S3-compatible storage, Azure Blob, and
native Google Cloud Storage. Object storage holds immutable case batches and
run plans; PostgreSQL holds coordination metadata and execution evidence.

Ceph Object Gateway supplies the local S3-compatible endpoint. Its bootstrap and
administration belong in infrastructure. See [local object storage](dev/README.md#local-object-storage)
for startup, credentials, and infrastructure smoke tests. Each application adapter
requires its own real-backend acceptance before provider support is claimed.

### Self-Hosted Storage

[Ceph Object Gateway](https://docs.ceph.com/en/latest/radosgw/) exposes the
S3-compatible interface over an open-source Ceph storage cluster. Ceph is the
self-hosted storage target and supplies the development S3 endpoint. The bundled
single-node configuration is for local integration testing; production deployment
sizing, redundancy, and operations require separate validation. AWS S3 and Ceph use the S3-compatible adapter behind the provider-neutral
storage contract.

## Performance Compose

`infra/performance/compose.yml` defines the private PostgreSQL, RabbitMQ, and Ceph object-storage
topology used by `cargo perf`. The harness creates a unique Compose project,
loopback ports, database namespace, broker scope, labelled network, and labelled
volumes for each campaign. Operators do not start this topology directly;
service-backed performance workloads provision and remove only their recorded
resources.

## AWS Performance Host

`infra/performance/aws/template.yaml` provisions the ephemeral EC2 host for the
canonical performance environment. Its fixed instance, AMI, Availability Zone,
and EBS settings implement the contract in
`performance/environments/aws-m6i-2xlarge-al2023-v1.toml`; deployment and
teardown commands are documented in `infra/performance/aws/README.md`.
