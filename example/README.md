# Example Project

This example demonstrates a file-based run profile and dataset that target a local llama.cpp-backed agent from the dev Docker Compose stack.

## Files

- `profile.yaml`: Qwen sentiment evaluation profile using `agent.http`
- `dataset.yaml`: minimal sentiment classification dataset

The profile points at `http://agent_vigilo_agent:8080/v1/chat/completions`, the llama.cpp-backed agent service in `infra/dev/docker-compose.single.yml`. Workers send an OpenAI-compatible chat completions request and map the agent response into the evaluator `actual` envelope.

The example uses one `quality` dimension. Each execution must meet `defaults.min_execution_score`, then the run-level `quality_release` scorecard gate requires complete coverage, no errors or abstentions, and a mean quality score of at least `0.85`.

The evaluator returns an ordinal sentiment label. `profile.yaml` explicitly maps `positive`, `neutral`, and `negative` to scores; see the [normalization reference](../web/docs/configuration/measurement-normalization.mdx) for every measurement type.

Each example dataset case sets `case_group: sentiment_classification`, which explicitly routes the case to the matching profile case group. If `case_group` is omitted, Vigilo falls back to `task_type` and tag matching.

## Model

Place the GGUF model at:

```text
models/qwen2.5-0.5b-instruct-q4_k_m.gguf
```

The agent service mounts `./models` into the container as `/models`.

## Evaluator

The profile uses the bundled evaluator `vigilo/sentiment-basic-en:0.1.0`. Build and publish it before creating runs:

```bash
cargo build --manifest-path evaluators/sentiment-basic-en/Cargo.toml --target wasm32-wasip2 --release
DATABASE_URL='postgres://postgres:password@localhost:5432/agent_vigilo' cargo run -p vigilo -- evaluator publish evaluators/sentiment-basic-en --release
```

## Validate

```bash
docker compose --env-file infra/dev/.env.single \
  -f infra/dev/docker-compose.single.yml up -d
DATABASE_URL='postgres://postgres:password@localhost:5432/agent_vigilo' cargo run -p vigilo -- run validate --profile-file example/profile.yaml --dataset-file example/dataset.yaml
```

To create and execute the run, follow the
[Getting Started guide](https://agentvigilo.com/docs/guides/getting-started),
including the storage owner configuration and host credentials.

Notes:

- Inline options (`--profile` / `--dataset`) are supported for quick experiments.
- File options are recommended for large, versioned payloads.
- The profile URL uses the Compose service name because workers run inside the dev Compose network.
