# Evaluators

Core Generative AI evaluators.

Guide: [Creating Evaluators](https://agentvigilo.com/docs/guides/creating-evaluators)

Scoped guidance: `evaluators/AGENTS.md`

Each evaluator is sourced as an independent Rust library crate.

## Build Instructions

Build evaluators for the WASI Preview 2 target (`wasm32-wasip2`).

### Prerequisite

```bash
rustup target add wasm32-wasip2
```

### Build (Dev)

```bash
cargo build --manifest-path "evaluators/<evaluator-name>/Cargo.toml" --target wasm32-wasip2
```

Output:

- `target/wasm32-wasip2/debug/<crate_name>.wasm`

### Build (Release)

```bash
cargo build --manifest-path "evaluators/<evaluator-name>/Cargo.toml" --target wasm32-wasip2 --release
```

Output:

- `target/wasm32-wasip2/release/<crate_name>.wasm`

### Build Any Evaluator

Replace `<evaluator-name>` with the evaluator crate directory/name. Artifact
filenames use underscores in place of hyphens, for example
`sentiment_basic_en.wasm`.

Example:

```bash
cargo build --manifest-path "evaluators/sentiment-basic-en/Cargo.toml" --target wasm32-wasip2 --release
```
