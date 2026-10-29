# Evaluator Crate Guidance

These rules add to the root guidance for crates under `evaluators/`.

## Crate Contract

- One crate implements one evaluator component through one primary `evaluate`
  entrypoint. Keep evaluator logic self-contained.
- Include `Vigilo.toml` and `example-input.json` in the crate root.
- Make results deterministic for identical declared inputs. If the contract
  intentionally permits nondeterminism, make its source explicit and controllable
  in tests.
- Implement the current contract from `vigilo/wit/evaluator/v1.0.0/evaluator.wit`.
  Evaluator crates select an ABI but never contain host adapters or compatibility
  dispatch; those remain under `vigilo/src/evaluator_abi/`.
- Read canonical evaluator `input` fields and return canonical `output`. Do not
  introduce alternate envelope or entrypoint names.
- Return one primary raw `binary`, `numeric`, or `ordinal` measurement, or an
  abstention, plus zero or more diagnostics. Dimension, normalization, threshold,
  weight, ordinal utility, and blocking policy belong outside evaluator code.

## Verification

- Build for `wasm32-wasip2` and validate the component with
  `vigilo evaluator test` using `--input` or `--input-file`.
- Bump the evaluator version before publishing a changed contract shape.

## References

- Example and template: `evaluators/sentiment-basic-en/README.md`
- Creation guide: `web/docs/guides/creating-evaluators.mdx`
- Normalization policy: `web/docs/configuration/measurement-normalization.mdx`
