# Vigilo

**Evaluation infrastructure for AI agents.**

> **Warning: pre-v1 release — API subject to change**
>
> Vigilo is under active development. Public APIs, CLI commands, configuration
> formats, and storage schemas may change before 1.0, including breaking changes.
> Pin the version you deploy and review release notes before updating. Do not
> assume backward compatibility between pre-v1 releases.

Test your agents against your own cases and criteria. Inspect failures, compare
results, and use pass/fail gates to decide what ships.

[Get started](https://agentvigilo.com/docs/guides/getting-started) ·
[Documentation](https://agentvigilo.com/docs/) ·
[GitHub](https://github.com/agent-vigilo/vigilo)

## What You Get

- **Release gates:** define evaluation criteria and use the results in your CI or
  deployment workflow.
- **Durable runs:** distribute work across workers, retry failures, and retain
  results for inspection and export.
- **Versioned evaluators:** use core evaluators or publish your own sandboxed
  WebAssembly components. Runs retain their exact evaluator versions.
- **Self-hosted infrastructure:** run evaluations on your own infrastructure with
  PostgreSQL, RabbitMQ, and S3-compatible object storage.

## Install

```bash
cargo install vigilo --locked
vigilo --help
```

This crate installs the `vigilo` command, including coordinator and worker modes.
It is an application, not a Rust library dependency.

## Run Your First Evaluation

Follow the [getting-started guide](https://agentvigilo.com/docs/guides/getting-started)
to start the local services, configure an agent endpoint, and run the example.
Bring your test cases, choose your evaluators, and define the criteria for passing.

For an existing deployment, `vigilo setup` applies database migrations and
installs the latest compatible core evaluators. Use
`--evaluators YYYY.MM.DD.N` to pin a published collection, or
`--skip-evaluators` for database-only setup.

## Explore

- [CLI reference](https://agentvigilo.com/docs/guides/cli-reference)
- [Create evaluators](https://agentvigilo.com/docs/guides/creating-evaluators)
- [Publish evaluators](https://agentvigilo.com/docs/guides/publishing)
- [Architecture](https://agentvigilo.com/docs/architecture/structure/)
- [Report an issue](https://github.com/agent-vigilo/vigilo/issues)

## License

Licensed under the [Apache License, Version 2.0](https://github.com/agent-vigilo/vigilo/blob/main/LICENSE).
