use serde_json::{
    Value,
    json,
};
use uuid::Uuid;

use super::{
    profile::{
        AgentIdentity,
        Profile,
    },
    wire::{
        Cancellation,
        MAX_EVIDENCE_BYTES,
        MAX_OUTCOME_BYTES,
        MAX_RECEIPT_BYTES,
        Outcome,
        PollResponse,
        Receipt,
        Submission,
    },
};

const ID: &str = "a1f0784d-b6c0-4ad8-9e5a-660aec11cb5a";

fn agent() -> AgentIdentity {
    AgentIdentity {
        provider: "acme".into(),
        name: "support".into(),
        version: "1.2.0".into(),
    }
}

fn id() -> Uuid {
    ID.parse().unwrap()
}

fn bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}

#[test]
fn profile_validates_explicit_choices_and_deployment_support() {
    for kind in ["direct_probe", "delegated_trial"] {
        for completion in [
            json!({"kind":"inline"}),
            json!({"kind":"polling","status_url":"https://example.test/status/{provider_reference}"}),
            json!({"kind":"callback"}),
        ] {
            let mut doc = json!({
                "agent": agent(),
                "harness": {
                    "kind":kind, "http":{"url":"https://example.test/submit"}, "completion":completion
                }
            });
            if completion["kind"] != "inline" {
                doc["harness"]["capabilities"] = json!({"idempotent_submission":false});
            }
            let profile: Profile =
                serde_yaml::from_str(&serde_yaml::to_string(&doc).unwrap()).unwrap();
            profile.validate(true).unwrap();
            assert_eq!(
                profile.validate(false).is_ok(),
                completion["kind"] != "callback"
            );

            let mut missing_kind = doc.clone();
            missing_kind["harness"]
                .as_object_mut()
                .unwrap()
                .remove("kind");
            assert!(serde_json::from_value::<Profile>(missing_kind).is_err());

            doc["harness"]["completion"]["unexpected"] = json!(true);
            assert!(serde_json::from_value::<Profile>(doc).is_err());
        }
    }
}

#[test]
fn polling_requires_capabilities_and_pins_the_reference_slot() {
    let mut doc = json!({
        "agent":agent(),
        "harness":{
            "kind":"delegated_trial",
            "http":{"url":"https://example.test/submit"},
            "completion":{"kind":"polling","status_url":"https://example.test/status?ref={provider_reference}"}
        }
    });
    assert!(
        serde_json::from_value::<Profile>(doc.clone())
            .unwrap()
            .validate(false)
            .is_err()
    );
    doc["harness"]["capabilities"] = json!({"idempotent_submission":true});
    serde_json::from_value::<Profile>(doc.clone())
        .unwrap()
        .validate(false)
        .unwrap();
    for status_url in [
        "https://{provider_reference}/status",
        "https://example.test/status?{provider_reference}=yes",
        "https://example.test/{provider_reference}suffix",
        "https://example.test/status#/{provider_reference}",
        "https://example.test/status",
    ] {
        doc["harness"]["completion"]["status_url"] = json!(status_url);
        assert!(
            serde_json::from_value::<Profile>(doc.clone())
                .unwrap()
                .validate(false)
                .is_err()
        );
    }
}

#[test]
fn native_submission_checks_shape_version_and_callback() {
    let mut doc = json!({
        "schema_version":"1.0.0","invocation_id":ID,"requested_agent":agent(),
        "deadline_at":"2026-11-19T05:30:00Z","case":{"input":{"scenario":"refund"}}
    });
    Submission::decode(&bytes(&doc), 8192, false).unwrap();
    assert!(Submission::decode(&bytes(&doc), 8192, true).is_err());
    doc["callback"] = json!({"url":"https://example.test/complete","token":"opaque-secret"});
    assert!(Submission::decode(&bytes(&doc), 8192, false).is_err());
    Submission::decode(&bytes(&doc), 8192, true).unwrap();
    doc["callback"]["url"] = json!("http://example.test/complete");
    assert!(Submission::decode(&bytes(&doc), 8192, true).is_err());
    doc.as_object_mut().unwrap().remove("callback");
    doc["case"]["expected"] = json!("private oracle");
    assert!(Submission::decode(&bytes(&doc), 8192, false).is_err());

    let duplicate = format!(
        r#"{{"schema_version":"1.0.0","schema_version":"1.0.0","invocation_id":"{ID}","status":"accepted"}}"#
    );
    assert!(Receipt::decode(duplicate.as_bytes(), id(), false).is_err());
}

#[test]
fn receipt_and_pending_have_small_distinct_shapes() {
    let mut doc = json!({"schema_version":"1.0.0","invocation_id":ID,"status":"accepted"});
    assert!(Receipt::decode(&bytes(&doc), id(), true).is_err());
    Receipt::decode(&bytes(&doc), id(), false).unwrap();
    doc["provider_reference"] = json!("é".repeat(128));
    assert_eq!(
        Receipt::decode(&bytes(&doc), id(), true)
            .unwrap()
            .provider_reference
            .unwrap()
            .len(),
        256
    );
    doc["provider_reference"] = json!("é".repeat(129));
    assert!(Receipt::decode(&bytes(&doc), id(), true).is_err());
    doc.as_object_mut().unwrap().remove("provider_reference");
    let pad = |value: &Value| {
        let mut body = bytes(value);
        body.resize(MAX_RECEIPT_BYTES + 1, b' ');
        body
    };
    assert!(Receipt::decode(&pad(&doc), id(), false).is_err());
    doc["status"] = json!("pending");
    assert!(matches!(
        PollResponse::decode(&bytes(&doc), id(), &agent(), true).unwrap(),
        PollResponse::Pending
    ));
    assert!(PollResponse::decode(&pad(&doc), id(), &agent(), true).is_err());
    assert!(PollResponse::decode(&bytes(&doc), Uuid::nil(), &agent(), true).is_err());
    assert!(Outcome::decode(&bytes(&doc), id(), &agent(), false).is_err());
}

#[test]
fn final_outcomes_enforce_variant_version_and_target() {
    let mut doc = json!({
        "schema_version":"1.0.0","invocation_id":ID,"status":"completed",
        "evidence":{"content_type":"application/octet-stream","encoding":"base64","body":"AP8="}
    });
    assert!(Outcome::decode(&bytes(&doc), id(), &agent(), true).is_err());
    let Outcome::Completed { evidence, .. } =
        Outcome::decode(&bytes(&doc), id(), &agent(), false).unwrap()
    else {
        panic!("expected completed outcome");
    };
    assert_eq!(evidence.bytes, [0, 255]);
    doc["executed_agent"] = json!(agent());
    assert!(matches!(
        PollResponse::decode(&bytes(&doc), id(), &agent(), true).unwrap(),
        PollResponse::Final(Outcome::Completed { .. })
    ));
    doc["executed_agent"]["version"] = json!("2.0.0");
    assert!(Outcome::decode(&bytes(&doc), id(), &agent(), false).is_err());
    doc["executed_agent"] = json!(agent());
    doc["schema_version"] = json!("1.0.1");
    assert!(Outcome::decode(&bytes(&doc), id(), &agent(), true).is_err());
    doc["schema_version"] = json!("1.0.0");
    doc["evidence"]["body"] = json!("***");
    assert!(Outcome::decode(&bytes(&doc), id(), &agent(), true).is_err());
    doc["evidence"]["body"] = json!("AP8=");
    doc["error"] = json!({"code":"unexpected"});
    assert!(Outcome::decode(&bytes(&doc), id(), &agent(), true).is_err());
}

#[test]
fn evidence_enforces_decoded_and_envelope_byte_limits() {
    use base64::{
        Engine,
        engine::general_purpose::STANDARD,
    };
    let mut doc = json!({
        "schema_version":"1.0.0","invocation_id":ID,"status":"completed",
        "evidence":{"content_type":"application/octet-stream","encoding":"base64","body":""}
    });
    doc["evidence"]["body"] = json!(STANDARD.encode(vec![0; MAX_EVIDENCE_BYTES]));
    Outcome::decode(&bytes(&doc), id(), &agent(), false).unwrap();
    doc["evidence"]["body"] = json!(STANDARD.encode(vec![0; MAX_EVIDENCE_BYTES + 1]));
    assert!(Outcome::decode(&bytes(&doc), id(), &agent(), false).is_err());
    doc["evidence"] = json!({"content_type":"text/plain","encoding":"utf8","body":"é"});
    let Outcome::Completed { evidence, .. } =
        Outcome::decode(&bytes(&doc), id(), &agent(), false).unwrap()
    else {
        panic!("expected completed outcome");
    };
    assert_eq!(evidence.bytes, "é".as_bytes());
    let mut body = bytes(&doc);
    doc["evidence"]["content_type"] = json!("x".repeat(1025));
    assert!(Outcome::decode(&bytes(&doc), id(), &agent(), false).is_err());
    body.resize(MAX_OUTCOME_BYTES + 1, b' ');
    assert!(Outcome::decode(&body, id(), &agent(), false).is_err());
}

#[test]
fn failed_outcomes_and_cancellation_reject_untrusted_fields() {
    let mut doc = json!({
        "schema_version":"1.0.0","invocation_id":ID,"status":"failed",
        "error":{"code":"target_unavailable","message":"é".repeat(256)}
    });
    assert!(matches!(
        Outcome::decode(&bytes(&doc), id(), &agent(), true).unwrap(),
        Outcome::Failed { .. }
    ));
    doc["error"]["message"] = json!("é".repeat(257));
    assert!(Outcome::decode(&bytes(&doc), id(), &agent(), true).is_err());
    doc["error"]["message"] = json!("failed");
    doc["error"]["retryable"] = json!(true);
    assert!(Outcome::decode(&bytes(&doc), id(), &agent(), true).is_err());
    let mut cancel = json!({"schema_version":"1.0.0","invocation_id":ID});
    Cancellation::decode(&bytes(&cancel), 8192).unwrap();
    cancel["provider_reference"] = json!("x".repeat(257));
    assert!(Cancellation::decode(&bytes(&cancel), 8192).is_err());
}

#[test]
fn provider_reported_identity_is_only_read_from_the_explicit_pointer() {
    let pointer = crate::response::JsonPointer::new("/reported".into()).unwrap();
    let check = |body: &Value, required| {
        super::wire::validate_reported_agent(&bytes(body), &pointer, &agent(), required)
    };
    let elsewhere = json!({"executed_agent":agent()});
    assert!(check(&elsewhere, false).is_ok());
    assert!(check(&elsewhere, true).is_err());
    assert!(check(&json!({"reported":agent()}), true).is_ok());
    assert!(check(&json!({"reported":null}), false).is_err());
    let mut stale = agent();
    stale.version = "2.0.0".into();
    assert!(check(&json!({"reported":stale}), false).is_err());
}

#[test]
fn provider_references_are_encoded_as_data_in_the_admitted_slot() {
    use super::profile::poll_url;
    let path = poll_url(
        "https://provider.example/status/{provider_reference}",
        "a/b?c=1#fragment",
    )
    .unwrap();
    assert_eq!(
        path.as_str(),
        "https://provider.example/status/a%2Fb%3Fc%3D1%23fragment"
    );
    let query = poll_url(
        "https://provider.example/status?ref={provider_reference}",
        "a&next=evil",
    )
    .unwrap();
    assert_eq!(
        query.query_pairs().collect::<Vec<_>>(),
        [("ref".into(), "a&next=evil".into())]
    );
    assert!(poll_url("https://provider.example/status/{provider_reference}", "..").is_err());
}
