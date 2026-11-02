//! Evaluator test command implementation.
//!
//! Runs one registry evaluator against canonical evaluator `input` JSON and
//! returns the evaluator's native measurement and diagnostics. Disabled or removed
//! evaluators cannot be tested, and test input must match the host-side
//! evaluator execution contract.

use super::*;

pub(super) async fn exec(
    context: Context,
    evaluator: String,
    input: Option<String>,
    input_file: Option<PathBuf>,
) -> anyhow::Result<()> {
    info!("testing evaluator {}", evaluator);

    let db = context.dbr().await?.control().await?;
    let out = context.out().await?;
    let wasm = context.wasm().await?;
    let evaluator = parse_fully_qualified_evaluator(&evaluator)?;

    let evaluator_record = evaluators::select_evaluator(
        db,
        &evaluator.namespace,
        &evaluator.name,
        &evaluator.version,
    )
    .await?
    .ok_or_else(|| {
        anyhow::anyhow!(
            "evaluator not found: {}/{}:{}",
            evaluator.namespace,
            evaluator.name,
            evaluator.version,
        )
    })?;

    match evaluator_record.state {
        EvaluatorState::Disabled | EvaluatorState::Removed => {
            anyhow::bail!(
                "evaluator {}/{}:{} cannot be tested while in state '{}'",
                evaluator_record.namespace,
                evaluator_record.name,
                evaluator_record.version,
                serde_json::to_string(&evaluator_record.state)?.trim_matches('"'),
            );
        }
        _ => {}
    }

    let input_raw = match (input, input_file) {
        (Some(raw), None) => raw,
        (None, Some(path)) => fs::read_to_string(path)?,
        _ => anyhow::bail!("exactly one of --input or --input-file must be provided"),
    };

    let parsed_input = crate::contracts::evaluator::test_input::decode_input(&input_raw)?;

    let abi = crate::contracts::evaluator_abi::EvaluatorAbiIdentity {
        package: evaluator_record
            .interface_name
            .rsplit_once('/')
            .map(|(package, _)| package.to_string())
            .ok_or_else(|| anyhow::anyhow!("evaluator has no verified ABI package"))?,
        world: evaluator_record.wit_world.clone(),
        interface: evaluator_record
            .interface_name
            .rsplit_once('/')
            .map(|(_, interface)| interface.to_string())
            .ok_or_else(|| anyhow::anyhow!("evaluator has no verified ABI interface"))?,
        version: evaluator_record.interface_version.clone(),
        contract_hash: evaluator_record.abi_contract_hash.clone(),
        adapter: evaluator_record.abi_adapter.clone(),
    };
    let evaluation_output =
        wasm.test_evaluator(&evaluator_record.wasm_bytes, &abi, parsed_input)?;

    let payload = json!({
        "data": {
            "namespace": evaluator_record.namespace,
            "name": evaluator_record.name,
            "version": evaluator_record.version,
            "state": evaluator_record.state,
            "output": evaluation_output,
        }
    });

    out.write_value(&payload)?;

    Ok(())
}
