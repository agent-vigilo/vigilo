//! Real Wasm fixture forwarding explicit test requests through the production ABI.
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
            original_body: false,
            mapped_items: false,
            content_media_types: vec![],
            tool_calls: false,
            tool_results: false,
        }
    }
    fn evaluate(input: Input) -> Result<Output, EvaluatorError> {
        let configuration: serde_json::Value =
            serde_json::from_str(&input.evaluator_config_json).unwrap();
        let mut responses = vec![];
        for request in configuration.as_array().unwrap() {
            let result = executor::send_http_request(&executor::HttpRequest {
                method: "POST".into(),
                uri: request["uri"].as_str().unwrap().into(),
                headers: request["headers"]
                    .as_array()
                    .map(|headers| {
                        headers
                            .iter()
                            .map(|h| executor::HttpHeader {
                                name: h[0].as_str().unwrap().into(),
                                value: h[1].as_str().unwrap().into(),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                body: Some(
                    request["body"]
                        .as_str()
                        .unwrap_or("input")
                        .as_bytes()
                        .to_vec(),
                ),
                timeout_ms: request["timeout_ms"].as_u64().map(|ms| ms as u32),
            });
            responses.push(match result {
                Ok(response) => serde_json::json!({"status":response.status, "body":response.body, "headers":response.headers.into_iter().map(|h| (h.name,h.value)).collect::<Vec<_>>()}),
                Err(error) => serde_json::json!({"error":error}),
            });
        }
        Ok(Output {
            evaluator: EvaluatorIdentity {
                namespace: "fixture".into(),
                name: "http-request".into(),
                version: "0.1.0".into(),
                content_hash: None,
                interface_version: Some("1.0.0".into()),
            },
            outcome: EvaluatorOutcome::Completed(Measurement::Binary(true)),
            diagnostics: vec![],
            metadata_json: serde_json::to_string(&responses).unwrap(),
        })
    }
}
export!(Evaluator);
