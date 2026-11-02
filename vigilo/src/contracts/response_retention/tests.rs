use serde_json::json;
use vigilo::response_mapping::{
    Limits,
    map_response,
};

use super::*;

pub(crate) fn packet(
    run_id: Uuid,
    execution_id: Uuid,
    attempt_id: Uuid,
    mode: PersistenceMode,
) -> RetainedResponse {
    let body = CapturedBody::new(
        br#"{"answer":"hello","image":"AP8="}"#.to_vec(),
        Some("application/json".into()),
    );
    let configuration:ResponseConfiguration=serde_json::from_value(json!({"schema_version":"1.0.0","interpretation":{"format":"json","mapping":{"items":[{"mode":"single","item":{"type":"message","content":[
        {"media_type":"text/plain","source":{"representation":"inline_text","selection":{"pointer":"/answer"}}},
        {"media_type":"image/png","source":{"representation":"base64","selection":{"pointer":"/image"}}},
        {"media_type":"image/png","source":{"representation":"base64","selection":{"pointer":"/image"}}}
    ]}}]}}})).unwrap();
    let mapped = map_response(&body, Some(&configuration), Limits::default()).unwrap();
    RetainedResponse::capture(
        provenance(run_id, execution_id, attempt_id),
        mode,
        Some(&body),
        Some(&configuration),
        Some(Ok(&mapped)),
    )
    .unwrap()
}
fn provenance(run_id: Uuid, execution_id: Uuid, attempt_id: Uuid) -> Provenance {
    Provenance {
        run_id,
        execution_id,
        attempt_id,
        case_hash: hash(b"case"),
        profile_hash: hash(b"profile"),
        runtime_artifact_digest: hash(b"runtime"),
        external_dependencies: ExternalDependencies::None,
        execution_plan_hash: plan().hash().unwrap(),
    }
}
#[test]
fn durable_round_trip_preserves_binary_text_order_and_shared_decoding() {
    let packet = packet(
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        PersistenceMode::Full,
    );
    assert_eq!(
        packet.payload.len(),
        packet.metadata.original_length.unwrap() as usize + 7
    );
    let stored = RetainedResponse::from_storage(
        packet.metadata_json().unwrap(),
        packet.payload.as_ref().clone(),
    )
    .unwrap();
    let mapped = stored.reconstruct().unwrap();
    assert_eq!(mapped.original.content_type(), Some("application/json"));
    let OutputItem::Message { content, .. } = &mapped.items.unwrap()[0] else {
        panic!()
    };
    assert!(matches!(&content[0].source,MappedSource::InlineText {text} if text == "hello"));
    let MappedSource::Resource { body: first } = &content[1].source else {
        panic!()
    };
    let MappedSource::Resource { body: second } = &content[2].source else {
        panic!()
    };
    assert_eq!(first.bytes(), [0, 255]);
    assert_eq!(first.bytes().as_ptr(), second.bytes().as_ptr());
    let text = packet.metadata_json().unwrap().to_string();
    assert!(!text.contains("hello"));
    assert!(!text.contains("handle"));
    assert!(!format!("{packet:?}").contains("hello"));
}
#[test]
fn summary_records_explicit_omission_without_content() {
    let packet = packet(
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        PersistenceMode::Summary,
    );
    assert!(packet.payload.is_empty());
    assert!(packet.reconstruct().is_err());
    let metadata = packet.metadata_json().unwrap().to_string();
    assert!(!metadata.contains("hello"));
    assert!(!metadata.contains("/answer"));
    assert!(
        packet
            .missing_dependencies(&plan(), &Default::default())
            .unwrap()
            .contains(&"response_evidence:summary_retention".into())
    );
    RetainedResponse::from_storage(packet.metadata_json().unwrap(), vec![]).unwrap();
}
#[test]
fn corrupt_payload_configuration_versions_and_ranges_fail_closed() {
    let packet = packet(
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        PersistenceMode::Full,
    );
    let mut changed_metadata = packet.metadata_json().unwrap();
    changed_metadata["items"][0]["content"][0]["media_type"] = json!("application/json");
    assert!(
        RetainedResponse::from_storage(changed_metadata, packet.payload.as_ref().clone()).is_err(),
        "metadata changes must not silently alter retained interpretation"
    );
    let mut payload = packet.payload.as_ref().clone();
    payload[0] ^= 1;
    assert!(RetainedResponse::from_storage(packet.metadata_json().unwrap(), payload).is_err());
    let mut metadata = packet.metadata_json().unwrap();
    metadata["schema_version"] = json!("2.0.0");
    assert!(RetainedResponse::from_storage(metadata, packet.payload.as_ref().clone()).is_err());
    let mut metadata = packet.metadata_json().unwrap();
    metadata["configuration_hash"] = json!(hash(b"wrong"));
    assert!(RetainedResponse::from_storage(metadata, packet.payload.as_ref().clone()).is_err());
    let mut metadata = packet.metadata_json().unwrap();
    metadata["items"][0]["content"][1]["source"]["range"]["offset"] = json!(u64::MAX);
    let metadata = resign(metadata);
    assert!(RetainedResponse::from_storage(metadata, packet.payload.as_ref().clone()).is_err());
    assert!(
        packet
            .validate_owner(
                Uuid::now_v7(),
                packet.metadata.provenance.execution_id,
                packet.metadata.provenance.attempt_id
            )
            .is_err()
    );
}
#[test]
fn raw_mapping_failure_and_absent_responses_remain_distinct() {
    let body = CapturedBody::new(b"not json".to_vec(), None);
    let configuration: ResponseConfiguration = serde_json::from_value(
        json!({"schema_version":"1.0.0","interpretation":{"format":"json","mapping":{"items":[]}}}),
    )
    .unwrap();
    let error = map_response(&body, Some(&configuration), Limits::default()).unwrap_err();
    let packet = RetainedResponse::capture(
        provenance(Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7()),
        PersistenceMode::Full,
        Some(&body),
        Some(&configuration),
        Some(Err(&error)),
    )
    .unwrap();
    assert_eq!(packet.reconstruct().unwrap().original.bytes(), b"not json");
    assert!(packet.metadata.mapping_error.is_some());
    let absent = RetainedResponse::capture(
        provenance(Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7()),
        PersistenceMode::Full,
        None,
        None,
        None,
    )
    .unwrap();
    assert!(absent.reconstruct().is_err());
    assert!(
        absent
            .missing_dependencies(&plan(), &Default::default())
            .unwrap()
            .contains(&"original_response".into())
    );
}
#[test]
fn replay_dependency_check_never_treats_identity_as_availability() {
    let mut packet = packet(
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        PersistenceMode::Full,
    );
    assert_eq!(
        packet
            .missing_dependencies(&plan(), &Default::default())
            .unwrap(),
        vec![format!("runtime:{}", hash(b"runtime"))]
    );
    let mut changed_plan = plan();
    changed_plan
        .evaluators
        .push(crate::contracts::evaluator_abi::ResolvedEvaluator {
            evaluator_ref: "vigilo/test:1.0.0".into(),
            evaluator_id: Uuid::nil(),
            content_hash: hash(b"evaluator"),
            abi: crate::contracts::evaluator_abi::EvaluatorAbiIdentity {
                package: "vigilo:evaluator".into(),
                world: "evaluator".into(),
                interface: "evaluator".into(),
                version: "1.0.0".into(),
                contract_hash: hash(b"contract"),
                adapter: "test@1".into(),
            },
            runtime: "wasmtime".into(),
            runtime_version: "test".into(),
            runtime_fingerprint: "test".into(),
        });
    assert!(
        packet
            .missing_dependencies(&changed_plan, &Default::default())
            .is_err()
    );
    let original_plan_hash = packet.metadata.provenance.execution_plan_hash.clone();
    Arc::make_mut(&mut packet.metadata)
        .provenance
        .execution_plan_hash = changed_plan.hash().unwrap();
    let only_runtime = [hash(b"runtime")].into_iter().collect();
    assert_eq!(
        packet
            .missing_dependencies(&changed_plan, &only_runtime)
            .unwrap(),
        vec![format!("evaluator:{}", hash(b"evaluator"))]
    );
    Arc::make_mut(&mut packet.metadata)
        .provenance
        .execution_plan_hash = original_plan_hash;
    let available = [hash(b"runtime")].into_iter().collect();
    assert!(
        packet
            .missing_dependencies(&plan(), &available)
            .unwrap()
            .is_empty()
    );
    Arc::make_mut(&mut packet.metadata)
        .provenance
        .external_dependencies = ExternalDependencies::NotRetained;
    assert_eq!(
        packet.missing_dependencies(&plan(), &available).unwrap(),
        vec!["external_dependency_responses"]
    );
}
#[test]
fn oversized_original_and_mismatched_mapping_are_rejected() {
    let body = CapturedBody::new(vec![0; 4 * 1024 * 1024 + 1], None);
    assert!(
        RetainedResponse::capture(
            provenance(Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7()),
            PersistenceMode::Full,
            Some(&body),
            None,
            None
        )
        .is_err()
    );
    let body = CapturedBody::new(b"one".to_vec(), None);
    let other = CapturedBody::new(b"two".to_vec(), None);
    let mapping = map_response(&other, None, Limits::default()).unwrap();
    assert!(
        RetainedResponse::capture(
            provenance(Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7()),
            PersistenceMode::Full,
            Some(&body),
            None,
            Some(Ok(&mapping))
        )
        .is_err()
    );
}

fn plan() -> EvaluatorExecutionPlan {
    EvaluatorExecutionPlan::new(hash(b"scoring"), vec![])
}

#[test]
fn omitted_persistence_settings_default_to_full_retention() {
    let settings: crate::contracts::run::PersistenceSettings =
        serde_json::from_value(json!({})).unwrap();
    assert!(matches!(settings.mode, PersistenceMode::Full));
    assert!(matches!(
        settings.persist_raw_outputs,
        crate::contracts::run::PersistRawOutputsMode::All
    ));
    assert!(settings.persist_evaluator_evidence);
}

#[test]
fn retained_references_cannot_amplify_reconstruction_beyond_budget() {
    let body = CapturedBody::new(vec![0; 4 * 1024 * 1024], None);
    let packet = RetainedResponse::capture(
        provenance(Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7()),
        PersistenceMode::Full,
        Some(&body),
        None,
        None,
    )
    .unwrap();
    let mut metadata = packet.metadata_json().unwrap();
    metadata["items"] = json!([{"type":"message","id":null,"role":null,"content":(0..5).map(|_| json!({"media_type":"application/octet-stream","source":{"representation":"original_range","offset":0,"byte_length":4*1024*1024}})).collect::<Vec<_>>()}]);
    let metadata = resign(metadata);
    assert!(RetainedResponse::from_storage(metadata, packet.payload.as_ref().clone()).is_err());
}

fn resign(value: serde_json::Value) -> serde_json::Value {
    let mut metadata: Metadata = serde_json::from_value(value).unwrap();
    metadata.metadata_hash.clear();
    metadata.metadata_hash = hash(&bounded_json(&metadata).unwrap());
    serde_json::to_value(metadata).unwrap()
}
