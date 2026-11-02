//! Real Wasm fixture exercising scoped resource reads through the evaluator ABI.
//! This component is test-only and has no publication or provider dependencies.
wit_bindgen::generate!({path: "../../../wit/evaluator/v1.0.0/evaluator.wit", world: "evaluator-world"});
use exports::vigilo::evaluator::evaluator::{
    Guest,
    Input,
    Output,
};
use vigilo::evaluator::{
    executor,
    types::*,
};

struct Evaluator;
impl Guest for Evaluator {
    fn requirements() -> EvidenceRequirements {
        EvidenceRequirements {
            original_body: true,
            mapped_items: false,
            content_media_types: vec![],
            tool_calls: false,
            tool_results: false,
        }
    }

    fn evaluate(input: Input) -> Result<Output, EvaluatorError> {
        let body = input.response_evidence.expect("required evidence").body;
        let handle = body.resource;
        if let Ok(foreign) = input.evaluator_config_json.parse::<u64>() {
            assert!(matches!(
                executor::read_resource(foreign, 0, 1),
                Err(ResourceReadError::InvalidHandle)
            ));
        }
        assert!(matches!(
            executor::read_resource(handle, 0, 0),
            Err(ResourceReadError::LimitExceeded)
        ));
        assert!(matches!(
            executor::read_resource(handle, 0, 65537),
            Err(ResourceReadError::LimitExceeded)
        ));
        assert!(matches!(
            executor::read_resource(handle, u64::MAX, 1),
            Err(ResourceReadError::OutOfBounds)
        ));
        assert!(matches!(
            executor::read_resource(0, 0, 1),
            Err(ResourceReadError::InvalidHandle)
        ));
        assert!(
            executor::read_resource(handle, body.byte_length, 1)
                .unwrap()
                .is_empty()
        );
        let mut bytes = Vec::new();
        let mut offset = 0;
        while offset < body.byte_length {
            let chunk = executor::read_resource(handle, offset, 3).unwrap();
            assert!(!chunk.is_empty());
            offset += chunk.len() as u64;
            bytes.extend(chunk);
        }
        if input.evaluator_config_json.contains("trap") {
            panic!("requested fixture trap");
        }
        if input.evaluator_config_json.contains("error") {
            return Err(EvaluatorError {
                code: "fixture_error".into(),
                message: "requested fixture error".into(),
            });
        }
        Ok(Output {
            evaluator: EvaluatorIdentity {
                namespace: "fixture".into(),
                name: "response-resource".into(),
                version: "0.1.0".into(),
                content_hash: None,
                interface_version: Some("1.0.0".into()),
            },
            outcome: EvaluatorOutcome::Completed(Measurement::Binary(bytes == [0, 255, 42, 10])),
            diagnostics: vec![],
            metadata_json: handle.to_string(),
        })
    }
}
export!(Evaluator);
