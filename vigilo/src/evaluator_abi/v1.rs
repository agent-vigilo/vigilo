//! Adapter for `vigilo:evaluator@1.0.0`.

use std::sync::LazyLock;

use serde_json::Value;
use wasmtime::component;

use super::EvaluatorAbiAdapter;
use crate::{
    context::wasm::{
        EvaluatorHost,
        Wasm,
    },
    contracts::{
        evaluator::{
            Abstention,
            DiagnosticFinding,
            EvaluatorIdentity,
            EvaluatorInput,
            EvaluatorOutcome,
            EvaluatorOutput,
            EvaluatorReportedError,
            Measurement,
            Severity,
        },
        evaluator_abi::EvaluatorAbiIdentity,
    },
};

const WIT: &[u8] = include_bytes!("../../wit/evaluator/v1.0.0/evaluator.wit");

mod bindings {
    wasmtime::component::bindgen!({
        path: "wit/evaluator/v1.0.0/evaluator.wit",
        world: "evaluator-world",
    });
}

pub(super) struct V1Adapter;

pub(super) static ADAPTER: V1Adapter = V1Adapter;

static IDENTITY: LazyLock<EvaluatorAbiIdentity> = LazyLock::new(|| EvaluatorAbiIdentity {
    package: "vigilo:evaluator".to_string(),
    world: "evaluator-world".to_string(),
    interface: "evaluator".to_string(),
    version: "1.0.0".to_string(),
    contract_hash: blake3::hash(WIT).to_hex().to_string(),
    adapter: "vigilo-evaluator-v1@1".to_string(),
});

impl EvaluatorAbiAdapter for V1Adapter {
    fn identity(&self) -> &'static EvaluatorAbiIdentity {
        &IDENTITY
    }

    #[cfg(test)]
    fn fixture_artifact(&self) -> &'static str {
        "sentiment_basic_en.wasm"
    }

    fn prepare(
        &self,
        runtime: &Wasm,
        component: &component::Component,
    ) -> anyhow::Result<component::InstancePre<EvaluatorHost>> {
        let mut linker = component::Linker::new(runtime.engine());
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker)?;
        bindings::vigilo::evaluator::executor::add_to_linker::<
            _,
            wasmtime::component::HasSelf<EvaluatorHost>,
        >(&mut linker, |host: &mut EvaluatorHost| host)?;
        let instance_pre = linker.instantiate_pre(component)?;
        // Import linking alone accepts components with missing exports. Check
        // the generated world types before publication without executing Wasm.
        bindings::EvaluatorWorldPre::new(instance_pre.clone())?;
        Ok(instance_pre)
    }

    fn execute(
        &self,
        runtime: &Wasm,
        instance_pre: &component::InstancePre<EvaluatorHost>,
        input: EvaluatorInput,
    ) -> anyhow::Result<EvaluatorOutput> {
        let mut store = runtime.evaluator_store()?;
        let instance = instance_pre.instantiate(&mut store)?;
        let bindings = bindings::EvaluatorWorld::new(&mut store, &instance)?;
        let requirements = bindings
            .vigilo_evaluator_evaluator()
            .call_requirements(&mut store)?;
        validate_available_evidence(&requirements)?;
        let input = map_input(input)?;
        let output = bindings
            .vigilo_evaluator_evaluator()
            .call_evaluate(&mut store, &input)
            .map_err(|err| anyhow::anyhow!("evaluator trapped in wasm sandbox: {}", err))?
            .map_err(|err| {
                anyhow::Error::new(EvaluatorReportedError {
                    code: if err.code.trim().is_empty() {
                        "invalid_evaluator_error".to_string()
                    } else {
                        err.code
                    },
                    message: err.message,
                })
            })?;
        map_output(output)
    }
}

impl bindings::vigilo::evaluator::executor::Host for EvaluatorHost {
    fn read_resource(
        &mut self,
        _handle: u64,
        _offset: u64,
        _max_bytes: u32,
    ) -> Result<Vec<u8>, bindings::vigilo::evaluator::types::ResourceReadError> {
        // No resource table is installed on this execution path. Fail closed;
        // numeric handles must never be interpreted as paths or global identities.
        Err(bindings::vigilo::evaluator::types::ResourceReadError::Unavailable)
    }

    fn trace(&mut self, msg: String) {
        if let Some(msg) = self.capped_log_message(msg) {
            tracing::debug!("evaluator.trace: {}", msg);
        }
    }

    fn debug(&mut self, msg: String) {
        if let Some(msg) = self.capped_log_message(msg) {
            tracing::debug!("evaluator.debug: {}", msg);
        }
    }

    fn warn(&mut self, msg: String) {
        if let Some(msg) = self.capped_log_message(msg) {
            tracing::warn!("evaluator.warn: {}", msg);
        }
    }

    fn error(&mut self, msg: String) {
        if let Some(msg) = self.capped_log_message(msg) {
            tracing::warn!("evaluator.error: {}", msg);
        }
    }

    fn send_http_request(
        &mut self,
        _req: bindings::vigilo::evaluator::executor::HttpRequest,
    ) -> Result<bindings::vigilo::evaluator::executor::HttpResponse, String> {
        Err("send_http_request is not enabled yet; outbound HTTP policy enforcement is not configured".to_string())
    }
}

/// Reject unsatisfied declared evidence before the evaluator can score a case.
/// This execution path exposes only the actual-output contract, not resources.
fn validate_available_evidence(
    requirements: &bindings::vigilo::evaluator::types::EvidenceRequirements,
) -> anyhow::Result<()> {
    if requirements.original_body
        || requirements.mapped_items
        || !requirements.content_media_types.is_empty()
        || requirements.tool_calls
        || requirements.tool_results
    {
        anyhow::bail!("evaluator requires response evidence unavailable on this execution path");
    }
    Ok(())
}

fn parse_json(field_name: &str, raw: &str) -> anyhow::Result<Value> {
    if raw.trim().is_empty() {
        return Ok(Value::Object(Default::default()));
    }
    serde_json::from_str(raw).map_err(|err| anyhow::anyhow!("invalid {} JSON: {}", field_name, err))
}

fn serialize_json(field_name: &str, value: &Value) -> anyhow::Result<String> {
    serde_json::to_string(value)
        .map_err(|err| anyhow::anyhow!("invalid {} JSON value: {}", field_name, err))
}

fn serialize_optional_json(
    field_name: &str,
    value: &Option<Value>,
) -> anyhow::Result<Option<String>> {
    value
        .as_ref()
        .map(|value| serialize_json(field_name, value))
        .transpose()
}

fn map_input(input: EvaluatorInput) -> anyhow::Result<bindings::vigilo::evaluator::types::Input> {
    let tool_calls = input
        .actual
        .tool_calls
        .into_iter()
        .map(|call| {
            Ok(bindings::vigilo::evaluator::types::ToolCall {
                name: call.name,
                arguments_json: serialize_json("tool-call.arguments", &call.arguments)?,
                result_json: serialize_optional_json("tool-call.result", &call.result)?,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let trace = input
        .actual
        .trace
        .into_iter()
        .map(|event| {
            Ok(bindings::vigilo::evaluator::types::AgentTraceEvent {
                kind: event.kind,
                name: event.name,
                payload_json: serialize_json("trace.payload", &event.payload)?,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(bindings::vigilo::evaluator::types::Input {
        run_id: input.run_id,
        execution_id: input.execution_id,
        attempt_id: input.attempt_id,
        test_case: bindings::vigilo::evaluator::types::TestCase {
            id: input.case.id,
            task_type: input.case.task_type,
            case_group: input.case.case_group,
            input_json: serialize_json("case.input", &input.case.input)?,
            expected_json: serialize_optional_json("case.expected", &input.case.expected)?,
            context_json: serialize_optional_json("case.context", &input.case.context)?,
            tags: input.case.tags,
            metadata_json: serialize_json(
                "case.metadata",
                &serde_json::to_value(input.case.metadata)?,
            )?,
        },
        actual: bindings::vigilo::evaluator::types::AgentOutput {
            text: input.actual.text,
            structured_json: serialize_optional_json(
                "actual.structured",
                &input.actual.structured,
            )?,
            tool_calls,
            trace,
            raw_json: serialize_json("actual.raw", &input.actual.raw)?,
            metadata_json: serialize_json("actual.metadata", &input.actual.metadata)?,
        },
        response_evidence: None,
        evaluator_config_json: serialize_json("evaluator_config", &input.evaluator_config)?,
    })
}

fn map_severity(severity: bindings::vigilo::evaluator::types::Severity) -> Severity {
    use bindings::vigilo::evaluator::types::Severity as WitSeverity;
    match severity {
        WitSeverity::None => Severity::None,
        WitSeverity::Low => Severity::Low,
        WitSeverity::Medium => Severity::Medium,
        WitSeverity::High => Severity::High,
        WitSeverity::Critical => Severity::Critical,
    }
}

fn map_measurement(measurement: bindings::vigilo::evaluator::types::Measurement) -> Measurement {
    use bindings::vigilo::evaluator::types::Measurement as WitMeasurement;
    match measurement {
        WitMeasurement::Binary(value) => Measurement::Binary { value },
        WitMeasurement::Numeric(numeric) => Measurement::Numeric {
            value: numeric.value,
            unit: numeric.unit,
        },
        WitMeasurement::Ordinal(value) => Measurement::Ordinal { value },
    }
}

fn map_output(
    output: bindings::vigilo::evaluator::types::Output,
) -> anyhow::Result<EvaluatorOutput> {
    use bindings::vigilo::evaluator::types::EvaluatorOutcome as WitOutcome;

    let outcome = match output.outcome {
        WitOutcome::Completed(measurement) => {
            EvaluatorOutcome::Completed(map_measurement(measurement))
        }
        WitOutcome::Abstained(abstention) => {
            if abstention.category.trim().is_empty() {
                anyhow::bail!("abstention category must not be empty");
            }
            EvaluatorOutcome::Abstained(Abstention {
                category: abstention.category,
                reason: abstention.reason,
            })
        }
    };
    let diagnostics = output
        .diagnostics
        .into_iter()
        .map(|finding| {
            if finding.category.trim().is_empty() {
                anyhow::bail!("diagnostic category must not be empty");
            }
            Ok(DiagnosticFinding {
                severity: map_severity(finding.severity),
                category: finding.category,
                reason: finding.reason,
                evidence: parse_json("evidence-json", &finding.evidence_json)?,
                tags: finding.tags,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(EvaluatorOutput {
        evaluator: EvaluatorIdentity {
            namespace: output.evaluator.namespace,
            name: output.evaluator.name,
            version: output.evaluator.version,
            content_hash: output.evaluator.content_hash,
            interface_version: output.evaluator.interface_version,
        },
        outcome,
        diagnostics,
        metadata: parse_json("metadata-json", &output.metadata_json)?,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::*;
    use crate::contracts::evaluator::{
        AgentOutput,
        AgentTraceEvent,
        TestCase,
        ToolCall,
    };

    #[test]
    fn evidence_requirements_fail_closed_individually() {
        use bindings::vigilo::evaluator::types::EvidenceRequirements;
        let empty = || EvidenceRequirements {
            original_body: false,
            mapped_items: false,
            content_media_types: vec![],
            tool_calls: false,
            tool_results: false,
        };
        assert!(validate_available_evidence(&empty()).is_ok());
        for field in 0..5 {
            let mut requirements = empty();
            match field {
                0 => requirements.original_body = true,
                1 => requirements.mapped_items = true,
                2 => requirements.content_media_types.push("text/plain".into()),
                3 => requirements.tool_calls = true,
                _ => requirements.tool_results = true,
            }
            assert!(validate_available_evidence(&requirements).is_err());
        }
    }

    fn evaluator_input() -> EvaluatorInput {
        EvaluatorInput {
            run_id: "run-1".to_string(),
            execution_id: "execution-1".to_string(),
            attempt_id: "attempt-1".to_string(),
            case: TestCase {
                id: "case-1".to_string(),
                task_type: "classification".to_string(),
                case_group: Some("sentiment".to_string()),
                input: json!({"text": "hello"}),
                expected: Some(json!({"label": "positive"})),
                context: None,
                tags: vec!["example".to_string()],
                metadata: BTreeMap::from([("source".to_string(), json!("test"))]),
            },
            actual: AgentOutput {
                text: Some("positive".to_string()),
                structured: Some(json!({"label": "positive"})),
                tool_calls: vec![ToolCall {
                    name: "lookup".to_string(),
                    arguments: json!({"id": 1}),
                    result: Some(json!({"found": true})),
                }],
                trace: vec![AgentTraceEvent {
                    kind: "tool_call".to_string(),
                    name: Some("lookup".to_string()),
                    payload: json!({"step": 1}),
                }],
                raw: json!({"provider": "test"}),
                metadata: json!({"latency_ms": 12}),
            },
            evaluator_config: json!({"threshold": 0.8}),
        }
    }

    #[test]
    fn input_mapping_preserves_structured_fields() {
        let mapped = map_input(evaluator_input()).unwrap();
        assert_eq!(mapped.run_id, "run-1");
        assert_eq!(mapped.test_case.case_group.as_deref(), Some("sentiment"));
        assert_eq!(
            parse_json("input", &mapped.test_case.input_json).unwrap(),
            json!({"text": "hello"})
        );
        assert_eq!(mapped.actual.tool_calls[0].name, "lookup");
        assert_eq!(mapped.actual.trace[0].kind, "tool_call");
    }

    #[test]
    fn output_mapping_rejects_invalid_diagnostics() {
        let mut output = wit_output();
        output.diagnostics[0].category = " ".to_string();
        assert!(
            map_output(output)
                .unwrap_err()
                .to_string()
                .contains("category")
        );

        let mut output = wit_output();
        output.metadata_json = "{".to_string();
        assert!(
            map_output(output)
                .unwrap_err()
                .to_string()
                .contains("metadata-json")
        );

        let mut output = wit_output();
        output.diagnostics[0].evidence_json = "{".to_string();
        assert!(
            map_output(output)
                .unwrap_err()
                .to_string()
                .contains("evidence-json")
        );
    }

    fn wit_output() -> bindings::vigilo::evaluator::types::Output {
        bindings::vigilo::evaluator::types::Output {
            evaluator: bindings::vigilo::evaluator::types::EvaluatorIdentity {
                namespace: "vigilo".to_string(),
                name: "example".to_string(),
                version: "1.0.0".to_string(),
                content_hash: None,
                interface_version: Some("1.0.0".to_string()),
            },
            outcome: bindings::vigilo::evaluator::types::EvaluatorOutcome::Completed(
                bindings::vigilo::evaluator::types::Measurement::Binary(true),
            ),
            diagnostics: vec![bindings::vigilo::evaluator::types::DiagnosticFinding {
                severity: bindings::vigilo::evaluator::types::Severity::High,
                category: "quality".to_string(),
                reason: None,
                evidence_json: "{}".to_string(),
                tags: Vec::new(),
            }],
            metadata_json: "{}".to_string(),
        }
    }

    #[test]
    fn measurement_and_severity_mappings_cover_all_variants() {
        use bindings::vigilo::evaluator::types::{
            Measurement as WitMeasurement,
            NumericMeasurement,
            Severity as WitSeverity,
        };

        let measurements = [
            (
                WitMeasurement::Binary(true),
                Measurement::Binary { value: true },
            ),
            (
                WitMeasurement::Numeric(NumericMeasurement {
                    value: 0.5,
                    unit: Some("ratio".to_string()),
                }),
                Measurement::Numeric {
                    value: 0.5,
                    unit: Some("ratio".to_string()),
                },
            ),
            (
                WitMeasurement::Ordinal("good".to_string()),
                Measurement::Ordinal {
                    value: "good".to_string(),
                },
            ),
        ];
        for (input, expected) in measurements {
            assert_eq!(map_measurement(input), expected);
        }

        let severities = [
            (WitSeverity::None, Severity::None),
            (WitSeverity::Low, Severity::Low),
            (WitSeverity::Medium, Severity::Medium),
            (WitSeverity::High, Severity::High),
            (WitSeverity::Critical, Severity::Critical),
        ];
        for (input, expected) in severities {
            assert_eq!(map_severity(input), expected);
        }
    }
}
