use serde_json::json;

use super::*;

#[test]
fn raw_and_json_are_explicit_and_versions_fail_closed() {
    let raw = json!({"schema_version":"1.0.0", "interpretation":{"format":"raw"}});
    let parsed: ResponseConfiguration = serde_json::from_value(raw.clone()).unwrap();
    assert_eq!(serde_json::to_value(parsed).unwrap(), raw);
    for version in ["1", "1.0", "1.0.1", "2.0.0", "latest"] {
        let mut value = raw.clone();
        value["schema_version"] = json!(version);
        assert!(serde_json::from_value::<ResponseConfiguration>(value).is_err());
    }
    let mut value = raw.clone();
    value["interpretation"]["mapping"] = json!({});
    assert!(serde_json::from_value::<ResponseConfiguration>(value).is_err());
    let mut value = raw;
    value.as_object_mut().unwrap().remove("schema_version");
    assert!(serde_json::from_value::<ResponseConfiguration>(value).is_err());
}

#[test]
fn pointers_preserve_literal_meaning_and_validate_escapes() {
    for value in ["", "/", "/answer", "/a~1b/~0/0", "/雪", "/a b"] {
        assert_eq!(JsonPointer::new(value.into()).unwrap().as_str(), value);
    }
    for value in ["answer", "#/answer", "/~", "/~2", "/~~0"] {
        assert!(JsonPointer::new(value.into()).is_err(), "{value}");
    }
    assert!(JsonPointer::new(format!("/{}", "x".repeat(4095))).is_ok());
    assert!(JsonPointer::new(format!("/{}", "x".repeat(4096))).is_err());
}

fn message() -> serde_json::Value {
    json!({"mode":"single","item":{"type":"message","content":[{
        "media_type":"text/plain", "source":{"representation":"inline_text",
        "selection":{"pointer":"/answer"}}}]}})
}

#[test]
fn mappings_keep_declared_order_types_and_required_defaults() {
    let value = json!({"items":[message(), {"mode":"each", "source":{"pointer":"/results"},
        "item":{"type":"tool_result", "call_id":{"pointer":"/id", "required":false},
        "content":[], "is_error":{"pointer":"/failed"}}}]});
    let mapping: JsonMapping = serde_json::from_value(value).unwrap();
    assert!(matches!(mapping.items[0], ItemMapping::Single { .. }));
    let ItemMapping::Each { source, .. } = &mapping.items[1] else {
        panic!("expected ordered iteration")
    };
    assert!(source.required);
    let encoded = serde_json::to_value(&mapping).unwrap();
    assert_eq!(encoded["items"][1]["item"]["call_id"]["required"], false);
    assert_eq!(
        serde_json::from_value::<JsonMapping>(encoded).unwrap(),
        mapping
    );
    for wrong in [json!(null), json!(123), json!(true)] {
        let mut invalid = message();
        invalid["item"]["content"][0]["source"]["selection"]["pointer"] = wrong;
        assert!(serde_json::from_value::<JsonMapping>(json!({"items":[invalid]})).is_err());
    }
    let mut invalid = message();
    invalid["item"]["content"][0]["source"]["guess"] = json!(true);
    assert!(serde_json::from_value::<JsonMapping>(json!({"items":[invalid]})).is_err());
}

#[test]
fn authoring_limits_and_explicit_operations_are_checked() {
    assert!(serde_json::from_value::<JsonMapping>(json!({"items":vec![message();128]})).is_ok());
    assert!(serde_json::from_value::<JsonMapping>(json!({"items":vec![message();129]})).is_err());
    for representation in ["inline_text", "json_value", "json_string", "base64"] {
        let mut item = message();
        item["item"]["content"][0]["source"]["representation"] = json!(representation);
        assert!(serde_json::from_value::<JsonMapping>(json!({"items":[item]})).is_ok());
    }
    let mut item = message();
    item["item"]["content"][0]["media_type"] = json!("text/plain\r\nInjected: yes");
    assert!(serde_json::from_value::<JsonMapping>(json!({"items":[item]})).is_err());
}

#[test]
fn evidence_preserves_grouping_absence_empty_and_refusal() {
    let body = BodyMetadata {
        resource: ResourceHandle(7),
        byte_length: 4,
        content_type: None,
    };
    let mut evidence = ResponseEvidence {
        schema_version: EvidenceVersion::V1,
        body,
        items: None,
        status: Some(CompletionStatus::Completed),
        stop_reason: Some(StopReason {
            category: StopCategory::Refusal,
            provider_value: Some("refusal".into()),
        }),
    };
    let unavailable = serde_json::to_value(&evidence).unwrap();
    evidence.items = Some(vec![]);
    assert_ne!(serde_json::to_value(&evidence).unwrap(), unavailable);
    evidence.items = Some(vec![
        OutputItem::Message {
            id: None,
            role: None,
            content: vec![
                ContentPart {
                    media_type: "text/plain".into(),
                    source: ContentSource::InlineText {
                        text: "first".into(),
                    },
                },
                ContentPart {
                    media_type: "image/png".into(),
                    source: ContentSource::Resource {
                        handle: ResourceHandle(8),
                        byte_length: 32,
                    },
                },
            ],
        },
        OutputItem::ToolCall {
            call_id: None,
            name: "lookup".into(),
            arguments: ContentPart {
                media_type: "text/plain".into(),
                source: ContentSource::InlineText {
                    text: "{broken".into(),
                },
            },
        },
    ]);
    let encoded = serde_json::to_value(&evidence).unwrap();
    assert_eq!(
        serde_json::from_value::<ResponseEvidence>(encoded.clone()).unwrap(),
        evidence
    );
    let mut unsupported = encoded;
    unsupported["schema_version"] = json!("2.0.0");
    assert!(serde_json::from_value::<ResponseEvidence>(unsupported).is_err());
}
