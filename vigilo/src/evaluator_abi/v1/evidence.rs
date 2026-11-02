//! Bind immutable mapped evidence to one evaluator store and enforce its declared
//! requirements. Mapping failure never blocks an independent raw-body evaluator.
//! Handles are created here, never stored or shared between evaluator invocations.
use std::collections::BTreeMap;

use vigilo::{
    response as r,
    response_mapping::{
        MappedResponse,
        MappedSource,
    },
};

use super::bindings::vigilo::evaluator::types as w;
use crate::{
    context::wasm::EvaluatorHost,
    contracts::evaluator::{
        AgentOutput,
        EvaluatorReportedError,
    },
};

pub(super) fn validate(
    requirements: &w::EvidenceRequirements,
    output: &AgentOutput,
) -> anyhow::Result<()> {
    let needs_mapping = requirements.mapped_items
        || !requirements.content_media_types.is_empty()
        || requirements.tool_calls
        || requirements.tool_results;
    let mapped = output
        .mapping
        .as_deref()
        .and_then(|result| result.as_ref().ok());
    if needs_mapping && let Some(Err(error)) = output.mapping.as_deref() {
        return Err(EvaluatorReportedError {
            code: "response_mapping_error".into(),
            message: error.to_string(),
        }
        .into());
    }
    let items = mapped.and_then(|mapping| mapping.items.as_deref());
    let mut media = Vec::new();
    let mut calls = false;
    let mut results = false;
    for item in items.unwrap_or_default() {
        match item {
            r::OutputItem::Message { content, .. } => {
                media.extend(content.iter().map(|part| part.media_type.as_str()))
            }
            r::OutputItem::ToolCall { arguments, .. } => {
                calls = true;
                media.push(&arguments.media_type);
            }
            r::OutputItem::ToolResult { content, .. } => {
                results = true;
                media.extend(content.iter().map(|part| part.media_type.as_str()));
            }
        }
    }
    if (requirements.original_body && output.captured_body.is_none())
        || (needs_mapping && (items.is_none() || output.captured_body.is_none()))
        || requirements
            .content_media_types
            .iter()
            .any(|required| !media.contains(&required.as_str()))
        || (requirements.tool_calls && !calls)
        || (requirements.tool_results && !results)
    {
        return Err(EvaluatorReportedError {
            code: "missing_response_evidence".into(),
            message: "evaluator requires response evidence unavailable on this execution path"
                .into(),
        }
        .into());
    }
    Ok(())
}

pub(super) fn bind(
    host: &mut EvaluatorHost,
    output: &AgentOutput,
) -> anyhow::Result<Option<w::ResponseEvidence>> {
    let Some(body) = &output.captured_body else {
        return Ok(None);
    };
    let original = host.install_response_body(body.clone())?;
    let mut evidence = w::ResponseEvidence {
        schema_version: "1.0.0".into(),
        body: w::BodyMetadata {
            resource: original.resource.0,
            byte_length: original.byte_length,
            content_type: original.content_type,
        },
        items: None,
        status: None,
        stop_reason: None,
    };
    if let Some(Ok(mapped)) = output.mapping.as_deref() {
        bind_mapping(host, mapped, &mut evidence)?;
    }
    Ok(Some(evidence))
}

fn bind_mapping(
    host: &mut EvaluatorHost,
    mapped: &MappedResponse,
    evidence: &mut w::ResponseEvidence,
) -> anyhow::Result<()> {
    let mut resources = BTreeMap::new();
    let mut part = |part: &r::ContentPart<MappedSource>| -> anyhow::Result<w::ContentPart> {
        let source = match &part.source {
            MappedSource::InlineText { text } => w::ContentSource::InlineText(text.clone()),
            MappedSource::BodyRange {
                offset,
                byte_length,
            } => w::ContentSource::BodyRange(w::BodyRange {
                offset: *offset,
                byte_length: *byte_length,
            }),
            MappedSource::Resource { body } => {
                let digest = *blake3::hash(body.bytes()).as_bytes();
                let handle = match resources.get(&digest) {
                    Some(handle) => *handle,
                    None => {
                        let handle = host.install_decoded_body(body.clone())?;
                        resources.insert(digest, handle);
                        handle
                    }
                };
                w::ContentSource::Resource(w::ResourceContent {
                    handle,
                    byte_length: body.bytes().len() as u64,
                })
            }
        };
        Ok(w::ContentPart {
            media_type: part.media_type.clone(),
            source,
        })
    };
    evidence.items = mapped
        .items
        .as_ref()
        .map(|items| {
            items
                .iter()
                .map(|item| {
                    Ok(match item {
                        r::OutputItem::Message { id, role, content } => {
                            w::OutputItem::Message(w::Message {
                                id: id.clone(),
                                role: role.clone(),
                                content: content
                                    .iter()
                                    .map(&mut part)
                                    .collect::<anyhow::Result<_>>()?,
                            })
                        }
                        r::OutputItem::ToolCall {
                            call_id,
                            name,
                            arguments,
                        } => w::OutputItem::ToolCall(w::ObservedToolCall {
                            call_id: call_id.clone(),
                            name: name.clone(),
                            arguments: part(arguments)?,
                        }),
                        r::OutputItem::ToolResult {
                            call_id,
                            content,
                            is_error,
                        } => w::OutputItem::ToolResult(w::ToolResult {
                            call_id: call_id.clone(),
                            content: content
                                .iter()
                                .map(&mut part)
                                .collect::<anyhow::Result<_>>()?,
                            is_error: *is_error,
                        }),
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()
        })
        .transpose()?;
    evidence.status = mapped.status.map(|status| match status {
        r::CompletionStatus::Completed => w::CompletionStatus::Completed,
        r::CompletionStatus::Incomplete => w::CompletionStatus::Incomplete,
        r::CompletionStatus::Failed => w::CompletionStatus::Failed,
    });
    evidence.stop_reason = mapped.stop_reason.as_ref().map(|reason| w::StopReason {
        category: match reason.category {
            r::StopCategory::EndTurn => w::StopCategory::EndTurn,
            r::StopCategory::ToolCall => w::StopCategory::ToolCall,
            r::StopCategory::Limit => w::StopCategory::Limit,
            r::StopCategory::Refusal => w::StopCategory::Refusal,
            r::StopCategory::Other => w::StopCategory::Other,
        },
        provider_value: reason.provider_value.clone(),
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::*;
    use crate::context::wasm::{
        Config,
        Wasm,
    };

    fn output() -> AgentOutput {
        let mut output: AgentOutput = serde_json::from_value(json!({})).unwrap();
        output.captured_body = Some(r::CapturedBody::new(
            br#"{"text":"hello","data":"AP8qCg=="}"#.to_vec(),
            None,
        ));
        output
    }
    fn requirements() -> w::EvidenceRequirements {
        w::EvidenceRequirements {
            original_body: true,
            mapped_items: false,
            content_media_types: vec![],
            tool_calls: false,
            tool_results: false,
        }
    }
    fn mapping() -> r::ResponseConfiguration {
        serde_json::from_value(json!({"schema_version":"1.0.0","interpretation":{"format":"json","mapping":{"items":[
            {"mode":"single","item":{"type":"message","content":[{"media_type":"text/plain","source":{"representation":"inline_text","selection":{"pointer":"/text"}}}]}},
            {"mode":"single","item":{"type":"tool_call","name":{"source":"literal","value":"tool"},"arguments":{"media_type":"application/octet-stream","source":{"representation":"base64","selection":{"pointer":"/data"}}}}}
        ]}}})).unwrap()
    }
    #[test]
    fn requirements_distinguish_absence_empty_mapping_and_atomic_failure() {
        let mut output = output();
        let mut requirements = requirements();
        assert!(validate(&requirements, &output).is_ok());
        requirements.mapped_items = true;
        assert!(validate(&requirements, &output).is_err());
        output.mapping = Some(Arc::new(Ok(MappedResponse {
            original: output.captured_body.clone().unwrap(),
            items: Some(vec![]),
            status: None,
            stop_reason: None,
        })));
        assert!(validate(&requirements, &output).is_ok());
        let original = output.captured_body.take();
        requirements.original_body = false;
        assert!(validate(&requirements, &output).is_err());
        output.captured_body = original;
        requirements.tool_calls = true;
        assert!(validate(&requirements, &output).is_err());
        let body = r::CapturedBody::new(b"bad json".to_vec(), None);
        output.mapping = Some(Arc::new(vigilo::response_mapping::map_response(
            &body,
            Some(&mapping()),
            Default::default(),
        )));
        let error = validate(&requirements, &output).unwrap_err();
        assert_eq!(
            error.downcast_ref::<EvaluatorReportedError>().unwrap().code,
            "response_mapping_error"
        );
        assert!(validate(&super::tests::requirements(), &output).is_ok());
    }
    #[test]
    fn mapped_order_media_and_decoded_resources_are_bound_per_invocation() {
        let mut output = output();
        output.mapping = Some(Arc::new(vigilo::response_mapping::map_response(
            output.captured_body.as_ref().unwrap(),
            Some(&mapping()),
            Default::default(),
        )));
        assert!(output.mapping.as_deref().unwrap().is_ok());
        let mut requirements = requirements();
        requirements.mapped_items = true;
        requirements.tool_calls = true;
        requirements.content_media_types =
            vec!["text/plain".into(), "application/octet-stream".into()];
        validate(&requirements, &output).unwrap();
        requirements.tool_results = true;
        assert!(validate(&requirements, &output).is_err());
        requirements.tool_results = false;
        requirements.content_media_types.push("image/png".into());
        assert!(validate(&requirements, &output).is_err());
        let runtime = Wasm::new(Config {
            timeout_ms: 0,
            ..Default::default()
        })
        .unwrap();
        let mut first = runtime.evaluator_store().unwrap();
        let evidence = bind(first.data_mut(), &output).unwrap().unwrap();
        let items = evidence.items.unwrap();
        assert!(matches!(&items[0], w::OutputItem::Message(_)));
        let w::OutputItem::ToolCall(call) = &items[1] else {
            panic!()
        };
        let w::ContentSource::Resource(resource) = &call.arguments.source else {
            panic!()
        };
        assert_eq!(
            first
                .data_mut()
                .read_response_resource(resource.handle, 0, 4)
                .unwrap(),
            [0, 255, 42, 10]
        );
        let mut second = runtime.evaluator_store().unwrap();
        bind(second.data_mut(), &output).unwrap();
        assert_eq!(
            second
                .data_mut()
                .read_response_resource(resource.handle, 0, 4),
            Err(r::ResourceReadError::InvalidHandle)
        );
        drop(first);
        assert_eq!(
            second
                .data_mut()
                .read_response_resource(resource.handle, 0, 4),
            Err(r::ResourceReadError::InvalidHandle)
        );
    }
}
