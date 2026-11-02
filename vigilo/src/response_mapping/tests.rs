use serde_json::{
    Value,
    json,
};

use super::*;

fn config(source: Value) -> ResponseConfiguration {
    serde_json::from_value(json!({"schema_version":"1.0.0","interpretation":{"format":"json","mapping":{"items":[{"mode":"single","item":{"type":"message","content":[{"media_type":"application/json","source":source}]}}]}}})).unwrap()
}
fn source(representation: &str, pointer: &str) -> Value {
    json!({"representation":representation,"selection":{"pointer":pointer}})
}
fn run(bytes: &[u8], configuration: &ResponseConfiguration) -> Result<MappedResponse> {
    map_response(
        &CapturedBody::new(bytes.to_vec(), None),
        Some(configuration),
        Limits::default(),
    )
}
fn part(result: &MappedResponse) -> &MappedSource {
    let OutputItem::Message { content, .. } = &result.items.as_ref().unwrap()[0] else {
        panic!()
    };
    &content[0].source
}
#[test]
fn raw_is_uninterpreted_and_shared() {
    let body = CapturedBody::new(vec![255, 0], Some("application/json".into()));
    let mapped = map_response(&body, None, Limits::default()).unwrap();
    assert!(mapped.items.is_none());
    assert_eq!(body.bytes().as_ptr(), mapped.original.bytes().as_ptr());
    assert_eq!(body.bytes(), mapped.original.bytes());
}
#[test]
fn fragments_preserve_exact_numbers_whitespace_and_pointer_escapes() {
    let body = br#" {"a/b":{"~key":[ 123456789012345678901234567890, { "n": 1e999 } ]}} "#;
    let result = run(body, &config(source("json_value", "/a~1b/~0key/1"))).unwrap();
    let MappedSource::BodyRange {
        offset,
        byte_length,
    } = part(&result)
    else {
        panic!()
    };
    assert_eq!(
        &body[*offset as usize..(*offset + byte_length) as usize],
        br#"{ "n": 1e999 }"#
    );
    for pointer in ["/a~1b/~0key/01", "/a~1b/~0key/+1", "/a~1b/~0key/-"] {
        assert_eq!(
            run(body, &config(source("json_value", pointer)))
                .unwrap_err()
                .kind,
            ErrorKind::MissingField
        );
    }
}
#[test]
fn strict_json_rejects_duplicates_and_invalid_documents() {
    for body in [
        br#"{"x":1,"x":2}"#.as_slice(),
        br#"{"x":{"a":1,"\u0061":2}}"#,
    ] {
        assert_eq!(
            run(body, &config(source("json_value", "")))
                .unwrap_err()
                .kind,
            ErrorKind::DuplicateKey
        );
    }
    for body in [b"{} {}".as_slice(), b"[", b"\xff", b"\xef\xbb\xbf{}"] {
        assert_eq!(
            run(body, &config(source("json_value", "")))
                .unwrap_err()
                .kind,
            ErrorKind::InvalidJson
        );
    }
    for body in [
        b"null".as_slice(),
        b"true",
        b"1e999",
        b"[]",
        b"{}",
        br#""text""#,
    ] {
        run(body, &config(source("json_value", ""))).unwrap();
    }
}
#[test]
fn optional_absence_is_distinct_from_null_wrong_type_and_empty() {
    let mut selection = source("inline_text", "/text");
    selection["selection"]["required"] = json!(false);
    let configuration = config(selection);
    let result = run(b"{}", &configuration).unwrap();
    assert!(
        matches!(&result.items.unwrap()[0], OutputItem::Message {content,..} if content.is_empty())
    );
    for body in [br#"{"text":null}"#.as_slice(), br#"{"text":42}"#] {
        assert_eq!(
            run(body, &configuration).unwrap_err().kind,
            ErrorKind::WrongType
        );
    }
    assert!(
        matches!(part(&run(br#"{"text":""}"#, &configuration).unwrap()), MappedSource::InlineText {text} if text.is_empty())
    );
}
#[test]
fn explicit_decoding_validates_and_preserves_bytes() {
    let result = run(br#"{"data":"AP8="}"#, &config(source("base64", "/data"))).unwrap();
    assert!(matches!(part(&result), MappedSource::Resource {body} if body.bytes() == [0,255]));
    for value in ["AP8", "AP9=", "AP8=\n", "https://example.com"] {
        assert!(
            run(
                serde_json::to_vec(&json!({"data":value}))
                    .unwrap()
                    .as_slice(),
                &config(source("base64", "/data"))
            )
            .is_err()
        );
    }
    let result = run(
        br#"{"data":"{ \"number\": 1e999 }"}"#,
        &config(source("json_string", "/data")),
    )
    .unwrap();
    assert!(
        matches!(part(&result), MappedSource::Resource {body} if body.bytes() == br#"{ "number": 1e999 }"#)
    );
    assert_eq!(
        run(
            br#"{"data":"{} trailing"}"#,
            &config(source("json_string", "/data"))
        )
        .unwrap_err()
        .kind,
        ErrorKind::InvalidJson
    );
}
#[test]
fn ordered_array_mapping_and_atomic_failure() {
    let configuration: ResponseConfiguration = serde_json::from_value(json!({"schema_version":"1.0.0","interpretation":{"format":"json","mapping":{"items":[{"mode":"each","source":{"pointer":"/choices"},"item":{"type":"message","role":{"source":"literal","value":"assistant"},"content":[{"media_type":"text/plain","source":{"representation":"inline_text","selection":{"pointer":"/message/content"}}}]}}]}}})).unwrap();
    let result = run(
        br#"{"choices":[{"message":{"content":"first"}},{"message":{"content":"second"}}]}"#,
        &configuration,
    )
    .unwrap();
    let texts: Vec<_> = result
        .items
        .unwrap()
        .into_iter()
        .map(|item| {
            let OutputItem::Message { content, .. } = item else {
                panic!()
            };
            let MappedSource::InlineText { text } = content.into_iter().next().unwrap().source
            else {
                panic!()
            };
            text
        })
        .collect();
    assert_eq!(texts, ["first", "second"]);
    let body = CapturedBody::new(
        br#"{"choices":[{"message":{"content":"secret"}},{"message":{"content":null}}]}"#.to_vec(),
        None,
    );
    let before = body.bytes().to_vec();
    let error = map_response(&body, Some(&configuration), Limits::default()).unwrap_err();
    assert_eq!(error.producer, Some(0));
    assert_eq!(error.element, Some(1));
    assert!(!error.to_string().contains("secret"));
    assert_eq!(body.bytes(), before);
    assert!(
        map_response(&body, None, Limits::default())
            .unwrap()
            .items
            .is_none()
    );
}
#[test]
fn budgets_bound_all_expansion_dimensions() {
    let configuration = config(source("base64", "/data"));
    let body = CapturedBody::new(br#"{"data":"AP8="}"#.to_vec(), None);
    let cases = [
        Limits {
            max_input_bytes: 0,
            ..Limits::default()
        },
        Limits {
            max_json_nodes: 1,
            ..Limits::default()
        },
        Limits {
            max_json_depth: 1,
            ..Limits::default()
        },
        Limits {
            max_work_bytes: 0,
            ..Limits::default()
        },
        Limits {
            max_items: 0,
            ..Limits::default()
        },
        Limits {
            max_parts: 0,
            ..Limits::default()
        },
        Limits {
            max_output_bytes: 0,
            ..Limits::default()
        },
        Limits {
            max_resources: 0,
            ..Limits::default()
        },
        Limits {
            max_duration: Duration::ZERO,
            ..Limits::default()
        },
    ];
    for limits in cases {
        assert_eq!(
            map_response(&body, Some(&configuration), limits)
                .unwrap_err()
                .kind,
            ErrorKind::LimitExceeded
        );
    }
    map_response(
        &body,
        Some(&configuration),
        Limits {
            max_input_bytes: body.bytes().len(),
            max_json_nodes: 2,
            max_json_depth: 2,
            max_items: 1,
            max_parts: 1,
            max_resources: 1,
            max_output_bytes: "application/json".len() + 2,
            ..Limits::default()
        },
    )
    .unwrap();
}
#[test]
fn completion_values_are_explicit_and_refusal_is_evidence() {
    let mut configuration = config(source("json_value", ""));
    let Interpretation::Json { mapping } = &mut configuration.interpretation else {
        panic!()
    };
    mapping.status = Some(ValueMapping {
        selection: Selection {
            pointer: JsonPointer::new("/status".into()).unwrap(),
            required: true,
        },
        values: [("done".into(), CompletionStatus::Completed)].into(),
    });
    mapping.stop_reason = Some(ValueMapping {
        selection: Selection {
            pointer: JsonPointer::new("/reason".into()).unwrap(),
            required: true,
        },
        values: [("refusal".into(), StopCategory::Refusal)].into(),
    });
    let result = run(br#"{"status":"done","reason":"refusal"}"#, &configuration).unwrap();
    assert_eq!(result.status, Some(CompletionStatus::Completed));
    assert_eq!(result.stop_reason.unwrap().category, StopCategory::Refusal);
    assert_eq!(
        run(
            br#"{"status":"unknown","reason":"refusal"}"#,
            &configuration
        )
        .unwrap_err()
        .kind,
        ErrorKind::UnknownValue
    );
}

#[test]
fn tool_items_and_content_order_use_only_explicit_fields() {
    let configuration: ResponseConfiguration = serde_json::from_value(json!({"schema_version":"1.0.0","interpretation":{"format":"json","mapping":{"items":[
        {"mode":"single","item":{"type":"tool_call","call_id":{"pointer":"/id"},"name":{"source":"field","selection":{"pointer":"/name"}},"arguments":{"media_type":"application/json","source":{"representation":"json_string","selection":{"pointer":"/arguments"}}}}},
        {"mode":"single","item":{"type":"tool_result","call_id":{"pointer":"/id"},"is_error":{"pointer":"/error"},"content":[{"media_type":"text/plain","source":{"representation":"inline_text","selection":{"pointer":"/answer"}}},{"media_type":"application/json","source":{"representation":"original_body"}}]}}
    ]}}})).unwrap();
    let bytes =
        br#"{"id":"c1","name":"lookup","arguments":"{\"id\":1}","error":false,"answer":"done"}"#;
    let result = run(bytes, &configuration).unwrap();
    let items = result.items.unwrap();
    assert!(
        matches!(&items[0], OutputItem::ToolCall {call_id:Some(id),name,arguments} if id == "c1" && name == "lookup" && matches!(&arguments.source, MappedSource::Resource {body} if body.bytes() == br#"{"id":1}"#))
    );
    let OutputItem::ToolResult {
        content, is_error, ..
    } = &items[1]
    else {
        panic!()
    };
    assert_eq!(*is_error, Some(false));
    assert!(matches!(&content[0].source, MappedSource::InlineText {text} if text == "done"));
    assert!(
        matches!(&content[1].source, MappedSource::BodyRange {offset:0,byte_length} if *byte_length == bytes.len() as u64)
    );
    let invalid =
        br#"{"id":"c1","name":"lookup","arguments":"{}","error":"secret","answer":"done"}"#;
    assert_eq!(
        run(invalid, &configuration).unwrap_err().kind,
        ErrorKind::WrongType
    );
    assert!(run(bytes, &configuration).is_ok());
}

#[test]
fn empty_array_is_present_empty_and_empty_text_is_not_trimmed() {
    let mut configuration = config(source("inline_text", ""));
    let Interpretation::Json { mapping } = &mut configuration.interpretation else {
        panic!()
    };
    let ItemMapping::Single { item } = mapping.items.remove(0) else {
        panic!()
    };
    mapping.items.push(ItemMapping::Each {
        source: Selection {
            pointer: JsonPointer::new("".into()).unwrap(),
            required: true,
        },
        item,
    });
    assert!(
        run(b"[]", &configuration)
            .unwrap()
            .items
            .unwrap()
            .is_empty()
    );
    assert!(
        matches!(part(&run(br#"[" \n "]"#,&configuration).unwrap()),MappedSource::InlineText {text} if text == " \n ")
    );
    assert_eq!(
        run(b"{}", &configuration).unwrap_err().kind,
        ErrorKind::WrongType
    );
}

#[test]
fn optional_selection_does_not_hide_invalid_parent_types() {
    let mut selection = source("inline_text", "/message/content");
    selection["selection"]["required"] = json!(false);
    let configuration = config(selection);
    for bytes in [
        br#"{"message":null}"#.as_slice(),
        br#"{"message":42}"#,
        br#"{"message":"wrong"}"#,
    ] {
        assert_eq!(
            run(bytes, &configuration).unwrap_err().kind,
            ErrorKind::WrongType
        );
    }
    assert!(run(b"{}", &configuration).is_ok());
    assert!(run(br#"{"message":{}}"#, &configuration).is_ok());
}
