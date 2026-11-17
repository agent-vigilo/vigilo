//! Service-free validation, input authority separation and projection invariants.
use serde_json::json;

use super::*;
fn root() -> super::super::Client {
    super::super::Client::new(super::super::Config::new(
        "postgres://unused@127.0.0.1:1/unused".into(),
    ))
    .unwrap()
}
fn input() -> String {
    json!({"run_id":"fixture","execution_id":"fixture","attempt_id":"fixture","case":{"id":"case","task_type":"text","input":{}},"actual":{}}).to_string()
}
#[tokio::test]
async fn invalid_configuration_and_cancelled_receipt_do_not_open_services() {
    for namespace in ["", "bad/name", "bad name"] {
        assert!(root().evaluators(namespace, Options::default()).is_err());
    }
    let mut options = Options::default();
    options.wasm.timeout_ms = 0;
    assert!(root().evaluators("vigilo", options).is_err());
    let mut options = Options::default();
    options.http.requests = 0;
    assert!(root().evaluators("vigilo", options).is_err());
    let client = root().evaluators("vigilo", Options::default()).unwrap();
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(matches!(
        client.prepare_package(b"unused".as_slice(), &cancel).await,
        Err(Error::Cancelled)
    ));
    assert!(matches!(
        client
            .test(
                &EvaluatorId::parse("vigilo/example:1.0.0").unwrap(),
                TestInput::from_json(&input()).unwrap(),
                &cancel
            )
            .await,
        Err(Error::Cancelled)
    ));
}
#[test]
fn ids_limits_and_response_fixtures_preserve_explicit_semantics() {
    let id = EvaluatorId::parse("vigilo/example:1.2.3").unwrap();
    assert_eq!(id.to_string(), "vigilo/example:1.2.3");
    for bad in [
        "example",
        "vigilo:example@1.0.0",
        "vigilo/:1.0.0",
        "vigilo/example:",
    ] {
        assert!(EvaluatorId::parse(bad).is_err());
    }
    assert!(EvaluatorId::parse(&format!("vigilo/{}:1.0.0", "x".repeat(4096))).is_err());
    assert!(PageLimits::new(0, 1024).is_err());
    assert!(PageLimits::new(21, 1024).is_err());
    assert!(PageLimits::new(1, 1023).is_err());
    assert!(Cursor::parse(&"x".repeat(16385)).is_err());
    let mut fixture: serde_json::Value = serde_json::from_str(&input()).unwrap();
    fixture["http_policy"] = json!({"allow":"all"});
    fixture["http_invocation"] = json!({"credential":"forged"});
    fixture["response"] = json!({"body":"hello","content_type":"text/plain"});
    let input = TestInput::from_json(&fixture.to_string()).unwrap();
    assert!(input.inner.http_policy.is_none());
    assert!(input.inner.http_invocation.is_none());
    assert_eq!(
        input.inner.actual.captured_body.as_ref().unwrap().bytes(),
        b"hello"
    );
    assert!(TestInput::from_json(&"x".repeat(32 * 1024 * 1024 + 1)).is_err());
}
#[test]
fn lifecycle_state_serialization_and_operator_projection_are_exact() {
    for (state, name) in [
        (State::Active, "active"),
        (State::Yanked, "yanked"),
        (State::Deprecated, "deprecated"),
        (State::Disabled, "disabled"),
        (State::Removed, "removed"),
    ] {
        assert_eq!(serde_json::to_value(state).unwrap(), name);
        let model: crate::models::evaluator::EvaluatorState = state.into();
        assert_eq!(State::from(model), state);
    }
}

#[tokio::test]
async fn cursor_validation_precedes_database_work_and_never_grants_authority() {
    for value in ["", "not-json", "[]", r#"["wrong","name","1.0.0"]"#] {
        assert!(Cursor::parse(value).is_err());
        assert!(serde_json::from_value::<Cursor>(json!(value)).is_err());
    }
    let value =
        serde_json::to_string(&(types::filter_hash("alpha", None), "name", "1.0.0")).unwrap();
    let cursor = Cursor::parse(&value).unwrap();
    let client = root().evaluators("alpha", Options::default()).unwrap();
    assert!(matches!(
        client
            .search("beta", None, PageLimits::default(), Some(&cursor))
            .await,
        Err(Error::InvalidInput(_))
    ));
    assert!(matches!(
        client
            .search(
                "alpha",
                Some("changed"),
                PageLimits::default(),
                Some(&cursor)
            )
            .await,
        Err(Error::InvalidInput(_))
    ));
}
