# Sentiment Basic EN Evaluator

Basic English-only lexicon sentiment evaluator for Agent Vigilo.

Use this crate as a single-evaluator template. See
[Creating Evaluators](https://agentvigilo.com/docs/guides/creating-evaluators).

Shared evaluator guidance: `evaluators/AGENTS.md`

## Input

The evaluator consumes a WIT `input` envelope. This evaluator reads text from:

- `input.actual.text` (preferred)
- `input.test_case.input_json.user_message` (fallback)

Text values can be plain text or a JSON object containing a `text` field.

## Limitations

- This evaluator is intentionally basic and intended for example/template use.
- Language handling is advisory and tuned for English terms only.
- It uses a small fixed lexicon and does not perform advanced linguistic analysis.

## Evidence Requirements

The `requirements` export declares no resource-backed response requirements.
This evaluator consumes `input.actual` as described above; it does not read
`input.response-evidence` or call `read-resource`.

## Output

The evaluator returns a structured `output` object:

- `output.evaluator`: identity metadata (`namespace`, `name`, `version`)
- `output.outcome`: one raw ordinal sentiment measurement
- `output.diagnostics`: supporting observations (this evaluator emits one)
- `output.metadata_json`: JSON string with approach, language scope, and maturity hints

The output includes:

- `measurement`: ordinal sentiment label (`positive`, `neutral`, or `negative`)
- diagnostic `severity`: `medium` for negative, otherwise `none`
- diagnostic `category`: `sentiment_classification`
- `evidence_json`: JSON string with `label`, `score`, matches, and normalized text
- `tags`: includes `basic` and `english-only` to make scope explicit

The run profile maps those labels to scores and owns the pass threshold,
dimension, weight, requiredness, and blocking behavior.

## Build

```bash
cargo build --manifest-path evaluators/sentiment-basic-en/Cargo.toml --target wasm32-wasip2 --release
```

## Package And Publish

```bash
vigilo evaluator package evaluators/sentiment-basic-en --release \
  --output sentiment-basic-en-0.1.0.tar.gz
vigilo evaluator publish sentiment-basic-en-0.1.0.tar.gz
```

Packaging uses the source manifest and prebuilt Wasm without a database.
Publication requires `DATABASE_URL`; the archive requires no source checkout
or Rust toolchain. See [Publishing Evaluators](https://agentvigilo.com/docs/guides/publishing).

## Test

```bash
vigilo evaluator test 'vigilo/sentiment-basic-en:0.1.0' --input-file evaluators/sentiment-basic-en/example-input.json
```
