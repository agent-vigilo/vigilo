use super::*;

fn execution() -> Execution {
    Execution {
        run_id: Uuid::nil(),
        execution_id: Uuid::nil(),
        attempt_id: Uuid::nil(),
    }
}
fn config(source: Source) -> Config {
    Config {
        version: 1,
        allow_http_loopback: false,
        provider: ProviderConfig::Local {
            bindings: BTreeMap::from([(
                "agent-api".into(),
                Binding {
                    source,
                    allowed_origin: "https://agent.example.com".into(),
                    header: "Authorization".into(),
                    prefix: "Bearer ".into(),
                },
            )]),
        },
    }
}

#[tokio::test]
async fn standalone_reads_rotated_secrets_without_service_or_cached_value() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("token");
    tokio::fs::write(&path, "canary-first").await.unwrap();
    let provider = Provider::new(config(Source::File { path: path.clone() })).unwrap();
    let headers = BTreeMap::new();
    let first = provider
        .resolve(
            "agent-api",
            "https://agent.example.com/run",
            &headers,
            execution(),
        )
        .await
        .unwrap();
    assert_eq!(first.value, "Bearer canary-first");
    assert!(first.value.is_sensitive());
    assert!(!format!("{:?}", first.value).contains("canary"));
    tokio::fs::write(&path, "canary-second").await.unwrap();
    let second = provider
        .resolve(
            "agent-api",
            "https://agent.example.com/run",
            &headers,
            execution(),
        )
        .await
        .unwrap();
    assert_eq!(second.value, "Bearer canary-second");
    tokio::fs::remove_file(path).await.unwrap();
    assert!(
        provider
            .resolve(
                "agent-api",
                "https://agent.example.com/run",
                &headers,
                execution()
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn scope_and_override_checks_precede_secret_access() {
    let provider = Provider::new(config(Source::File {
        path: PathBuf::from("missing-secret"),
    }))
    .unwrap();
    for destination in [
        "https://attacker.example.com",
        "https://agent.example.com:8443",
        "https://agent.example.com.evil",
        "http://agent.example.com",
        "https://user@agent.example.com",
        "https://agent.example.com/#fragment",
    ] {
        let err = provider
            .resolve("agent-api", destination, &BTreeMap::new(), execution())
            .await
            .err()
            .unwrap();
        assert!(!err.to_string().contains("file"));
        assert!(!err.retryable);
    }
    let err = provider
        .resolve(
            "agent-api",
            "https://agent.example.com",
            &BTreeMap::from([("aUtHoRiZaTiOn".into(), "canary-override".into())]),
            execution(),
        )
        .await
        .err()
        .unwrap();
    assert!(err.to_string().contains("overrides"));
    assert!(!err.to_string().contains("canary"));
    for reference in ["../secret", "", "other"] {
        assert!(
            provider
                .resolve(
                    reference,
                    "https://agent.example.com",
                    &BTreeMap::new(),
                    execution()
                )
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn file_bounds_and_errors_do_not_expose_values() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source");
    for value in [
        Vec::new(),
        vec![b'x'; MAX_SECRET_BYTES + 1],
        vec![0xff],
        b"canary\r\ninjected: value".to_vec(),
    ] {
        tokio::fs::write(&path, value).await.unwrap();
        let provider = Provider::new(config(Source::File { path: path.clone() })).unwrap();
        let error = provider
            .resolve(
                "agent-api",
                "https://agent.example.com",
                &BTreeMap::new(),
                execution(),
            )
            .await
            .err()
            .unwrap();
        assert!(!format!("{error:?}").contains("canary"));
    }
    assert!(read_file(dir.path(), 100).await.is_err());
    tokio::fs::write(&path, vec![b'x'; MAX_SECRET_BYTES])
        .await
        .unwrap();
    assert_eq!(
        read_file(&path, MAX_SECRET_BYTES).await.unwrap().len(),
        MAX_SECRET_BYTES
    );
}

#[tokio::test]
async fn environment_provider_reads_only_operator_named_source() {
    const KEY: &str = "VIGILO_CREDENTIAL_TEST_CHILD";
    if std::env::var(KEY).ok().as_deref() != Some("canary-environment") {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "agent_client::credentials::tests::environment_provider_reads_only_operator_named_source"])
            .env(KEY, "canary-environment").status().unwrap();
        assert!(status.success());
        return;
    }
    let provider = Provider::new(config(Source::Env { name: KEY.into() })).unwrap();
    let resolved = provider
        .resolve(
            "agent-api",
            "https://agent.example.com",
            &BTreeMap::new(),
            execution(),
        )
        .await
        .unwrap();
    assert_eq!(resolved.value, "Bearer canary-environment");
    let absent = Source::Env {
        name: format!("VIGILO_ABSENT_{}", Uuid::now_v7().simple()),
    };
    assert!(absent.read().await.is_err());
}

#[tokio::test]
async fn config_loading_is_versioned_strict_bounded_and_diagnostic_safe() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("provider.json");
    let valid = serde_json::json!({"version":1,"provider":{"type":"local","bindings":{}}});
    tokio::fs::write(&path, serde_json::to_vec(&valid).unwrap())
        .await
        .unwrap();
    Provider::load(&path).await.unwrap();
    for value in [
        serde_json::json!({"version":2,"provider":{"type":"local","bindings":{}}}),
        serde_json::json!({"version":1,"provider":{"type":"local","bindings":{}},"typo":"canary"}),
    ] {
        tokio::fs::write(&path, serde_json::to_vec(&value).unwrap())
            .await
            .unwrap();
        let err = Provider::load(&path).await.err().unwrap();
        assert!(!err.to_string().contains("canary"));
    }
    tokio::fs::write(&path, vec![b'x'; MAX_CONFIG_BYTES + 1])
        .await
        .unwrap();
    assert!(Provider::load(&path).await.is_err());
}

#[test]
fn origins_headers_and_expiration_fail_closed() {
    assert!(validate_reference(&"a".repeat(128)).is_ok());
    assert!(validate_reference(&"a".repeat(129)).is_err());
    assert!(validate_reference("non-ascii-λ").is_err());
    assert!(binding_value("api-key", "", &"a".repeat(MAX_SECRET_BYTES)).is_ok());
    assert!(binding_value("api-key", "x", &"a".repeat(MAX_SECRET_BYTES)).is_err());
    assert!(binding_value("api-key", "", &"a".repeat(MAX_SECRET_BYTES + 1)).is_err());
    for name in [
        "Host",
        "Content-Length",
        "Cookie",
        "Proxy-Authorization",
        "X-Forwarded-Host",
        "X-Http-Method-Override",
    ] {
        assert!(header(name).is_err());
    }
    for name in ["Authorization", "api-key", "x-api-key"] {
        assert!(header(name).is_ok());
    }
    assert!(url("http://localhost", true).is_err());
    assert!(url("http://192.0.2.1", true).is_err());
    assert!(url("http://127.0.0.1", true).is_ok());
    assert!(url("http://[::1]", true).is_ok());
    assert!(url("http://127.0.0.1", false).is_err());
    let mut resolved = binding_value("Authorization", "Bearer ", "secret").unwrap();
    resolved.expires_at = Some(chrono::Utc::now() - chrono::Duration::seconds(1));
    assert!(resolved.check_validity().is_err());
}

#[tokio::test]
async fn failed_resolutions_release_capacity_and_routing_overrides_fail_before_reads() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("token");
    let provider = Provider::new(config(Source::File { path: path.clone() })).unwrap();
    for name in [
        "Host",
        "Content-Length",
        "Connection",
        "Proxy-Authorization",
        "x-forwarded-host",
    ] {
        let err = provider
            .resolve(
                "agent-api",
                "https://agent.example.com",
                &BTreeMap::from([(name.into(), "canary".into())]),
                execution(),
            )
            .await
            .err()
            .unwrap();
        assert!(err.to_string().contains("routing"));
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        for _ in 0..128 {
            assert!(
                provider
                    .resolve(
                        "agent-api",
                        "https://agent.example.com",
                        &BTreeMap::new(),
                        execution()
                    )
                    .await
                    .is_err()
            );
        }
        tokio::fs::write(&path, "recovered").await.unwrap();
        assert!(
            provider
                .resolve(
                    "agent-api",
                    "https://agent.example.com",
                    &BTreeMap::new(),
                    execution()
                )
                .await
                .is_ok()
        );
    })
    .await
    .unwrap();
}
