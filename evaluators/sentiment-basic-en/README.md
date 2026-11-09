# Sentiment Basic EN Evaluator

Basic English-only lexicon sentiment evaluator for Agent Vigilo.

Use this crate as a single-evaluator template. See
[Creating Evaluators](https://agentvigilo.com/docs/guides/creating-evaluators).

Shared evaluator guidance: `evaluators/AGENTS.md`

## Input

The evaluator consumes explicitly mapped `text/plain` message parts in
`input.response-evidence.items`, preserving message and part order. Parts are
separated by newlines before lexicon scoring. Other media and tool events are not
scored. Missing, empty, or whitespace-only mapped text is an evaluation error;
the case prompt and `actual.text` are never substitutes.

Configure the agent profile to select the answer, for example `/text` for
`{"text":"Great service"}` or `/choices/0/message/content` for a chat response.
See [Response Mapping](https://agentvigilo.com/docs/configuration/run-profile#explicit-response-contracts).

## Limitations

- This evaluator is intentionally basic and intended for example/template use.
- Language handling is advisory and tuned for English terms only.
- It uses a small fixed lexicon and does not perform advanced linguistic analysis.

## Evidence Requirements

The `requirements` export requires mapped items and `text/plain` content.
Inline text and explicitly mapped body ranges or decoded resources are supported.
Resource reads use the host's invocation-scoped handles in chunks of at most 64 KiB.
Combined text is limited to 4 MiB and must be UTF-8. JSON-looking text is scored as
literal text; it is never parsed for another answer field.

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
- `evidence_json`: JSON string with `label`, `score`, matches, and evaluated text
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

The example file includes a `response` fixture with a UTF-8 `body`, optional
`content_type`, and explicit `configuration`. The test command applies that mapping
and installs fresh host resources, without calling an agent. It does not accept
live resource handles from JSON.

## License

Public evaluator implementations and templates in this directory are licensed
under [Apache 2.0](https://github.com/agent-vigilo/vigilo/blob/main/evaluators/LICENSE).
The Vigilo engine has a separate FSL-1.1-ALv2 license.
