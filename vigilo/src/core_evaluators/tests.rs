use super::*;

fn catalog() -> Catalog {
    let abi = crate::evaluator_abi::current_identity();
    serde_json::from_value(serde_json::json!({
        "format": "vigilo/core-evaluators/v1", "collection": "2026.10.28.1",
        "evaluators": [{"name":"sentiment-basic-en", "version":"0.1.0",
            "asset":"sentiment-basic-en-0.1.0.tar.gz", "size_bytes":123,
            "sha256":"a".repeat(64), "artifact_blake3":"b".repeat(64),
            "wit": {"package":abi.package,"world":abi.world,"interface":abi.interface,
                "version":abi.version,"contract_hash":abi.contract_hash}}]
    }))
    .unwrap()
}

#[test]
fn collection_names_are_canonical_dates_with_positive_sequence() {
    for value in ["2026.10.28.1", "2026.10.28.10", "2028.02.29.1"] {
        assert_eq!(parse_collection(value).unwrap(), value);
    }
    for value in [
        "latest",
        "0000.01.01.1",
        "-001.01.01.1",
        "ééé.01.1",
        "2026.2.01.1",
        "2026.02.29.1",
        "2026.10.28.0",
        "2026.10.28.01",
        "../2026.10.28.1",
        "2026.10.28.4294967296",
    ] {
        assert!(parse_collection(value).is_err(), "{value}");
    }
}

#[test]
fn discovery_orders_collection_sequences_numerically_and_excludes_other_releases() {
    let release = |tag: &str, draft, prerelease| Release {
        tag_name: tag.into(),
        draft,
        prerelease,
        assets: vec![],
    };
    let selected = collection_releases(vec![
        release("vigilo/v9.0.0", false, false),
        release("evaluators/2026.10.28.2", false, false),
        release("evaluators/2026.10.29.1", true, false),
        release("evaluators/2026.10.28.10", false, false),
        release("evaluators/2026.10.30.1", false, true),
    ])
    .unwrap();
    assert_eq!(
        selected
            .iter()
            .map(|r| r.tag_name.as_str())
            .collect::<Vec<_>>(),
        ["evaluators/2026.10.28.10", "evaluators/2026.10.28.2"]
    );
    assert!(collection_releases(vec![release("evaluators/bad", false, false)]).is_err());
}

#[test]
fn catalog_binds_assets_versions_and_unique_names_with_bounded_sizes() {
    catalog().validate("2026.10.28.1").unwrap();
    assert!(catalog().validate("2026.10.28.2").is_err());
    for mutate in [
        (|c: &mut Catalog| c.format = "other".into()) as fn(&mut Catalog),
        |c| c.evaluators.clear(),
        |c| c.evaluators.push(catalog().evaluators.remove(0)),
        |c| c.evaluators[0].name = "../bad".into(),
        |c| c.evaluators[0].version = "latest".into(),
        |c| c.evaluators[0].asset = "https://evil.test/package".into(),
        |c| c.evaluators[0].size_bytes = 0,
        |c| c.evaluators[0].size_bytes = MAX_ARCHIVE + 1,
        |c| c.evaluators[0].sha256 = "A".repeat(64),
        |c| c.evaluators[0].artifact_blake3 = "bad".into(),
        |c| c.evaluators[0].wit.contract_hash.clear(),
    ] {
        let mut value = catalog();
        mutate(&mut value);
        assert!(value.validate("2026.10.28.1").is_err());
    }
    let mut value = catalog();
    value.evaluators[0].size_bytes = MAX_ARCHIVE;
    value.validate("2026.10.28.1").unwrap();
}

#[test]
fn unsupported_abi_can_be_skipped_but_changed_known_contract_fails_closed() {
    assert!(catalog().compatible().unwrap());
    let mut value = catalog();
    value.evaluators[0].wit.version = "999.0.0".into();
    assert!(!value.compatible().unwrap());
    value.evaluators.push(catalog().evaluators.remove(0));
    value.evaluators[1].wit.contract_hash = "0".repeat(64);
    assert!(value.compatible().is_err());
}

#[test]
fn asset_urls_have_fixed_origin_and_encode_tag_as_one_segment() {
    let url = asset_url("2026.10.28.1", "sentiment-basic-en-0.1.0.tar.gz").unwrap();
    assert_eq!(
        url.as_str(),
        "https://github.com/agent-vigilo/vigilo/releases/download/evaluators%2F2026.10.28.1/sentiment-basic-en-0.1.0.tar.gz"
    );
}

fn response_body(bytes: Vec<u8>) -> reqwest::Response {
    http::Response::builder()
        .status(200)
        .body(reqwest::Body::from(bytes))
        .unwrap()
        .into()
}

#[tokio::test]
async fn metadata_and_archive_limits_and_checksums_fail_closed() {
    let parsed: serde_json::Value = bounded_json(response_body(b"{}".to_vec()), 2)
        .await
        .unwrap();
    assert_eq!(parsed, serde_json::json!({}));
    assert!(
        bounded_json::<serde_json::Value>(response_body(b"{}".to_vec()), 1)
            .await
            .is_err()
    );
    assert!(
        bounded_json::<serde_json::Value>(response_body(b"{".to_vec()), 2)
            .await
            .is_err()
    );
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("archive");
    let mut entry = catalog().evaluators.remove(0);
    entry.size_bytes = 3;
    entry.sha256 = format!("{:x}", Sha256::digest(b"abc"));
    download(response_body(b"abc".to_vec()), &path, &entry)
        .await
        .unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"abc");
    for bytes in [b"ab".to_vec(), b"abcd".to_vec(), b"abd".to_vec()] {
        assert!(download(response_body(bytes), &path, &entry).await.is_err());
    }
    let owned_path = directory.path().to_owned();
    drop(directory);
    assert!(!owned_path.exists());
}

#[cfg(feature = "evaluator-abi-fixtures")]
#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn collection_publication_replays_without_download_and_rejects_conflicts(pool: sqlx::PgPool) {
    use sqlx::ConnectOptions;

    use crate::context::{
        database,
        output::OutputFormat,
        wasm,
    };
    let context = Context::new(
        database::Config::new(
            pool.connect_options().to_url_lossy().to_string(),
            5,
            Duration::from_secs(10),
            database::CircuitBreakerConfig::default(),
            database::PlacementConfig::default_single_database(),
        ),
        None,
        wasm::Config::default(),
        OutputFormat::Json,
    );
    let source = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../evaluators/sentiment-basic-en");
    let prepared = context
        .wasm()
        .await
        .unwrap()
        .prepare_evaluator(source, "release".into())
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("package.tar.gz");
    let mut entry = catalog().evaluators.remove(0);
    entry.name = prepared.name.clone();
    entry.version = prepared.version.clone();
    entry.artifact_blake3 = prepared.wasm_hash.clone();
    // Create a second valid portable version from the real Wasm fixture. Change
    // only its root package identity, preserving section sizes and executable ABI.
    let mut newer_bytes = prepared.wasm_bytes.clone();
    let (offset, length, mut identity) = wasmparser::Parser::new(0)
        .parse_all(&newer_bytes)
        .find_map(|payload| match payload.unwrap() {
            wasmparser::Payload::CustomSection(section) if section.name() == "vigilo.package" => {
                Some((
                    section.data_offset(),
                    section.data().len(),
                    serde_json::from_slice::<serde_json::Value>(section.data()).unwrap(),
                ))
            }
            _ => None,
        })
        .unwrap();
    let newer_version = "9.9.9";
    assert!(
        cargo_metadata::semver::Version::parse(newer_version).unwrap()
            > cargo_metadata::semver::Version::parse(&entry.version).unwrap()
    );
    identity["version"] = newer_version.into();
    let identity = serde_json::to_vec(&identity).unwrap();
    assert_eq!(
        identity.len(),
        length,
        "fixture package versions must have equal encoded width"
    );
    newer_bytes[offset..offset + length].copy_from_slice(&identity);
    let newer_path = directory.path().join("newer.tar.gz");
    let mut newer_entry = catalog().evaluators.remove(0);
    newer_entry.name = entry.name.clone();
    newer_entry.version = newer_version.into();
    newer_entry.artifact_blake3 = blake3::hash(&newer_bytes).to_hex().to_string();
    crate::evaluator_package::write(
        &newer_path,
        crate::evaluator_package::PackageInput {
            name: prepared.name.clone(),
            version: newer_version.into(),
            description: prepared.description.clone(),
            tags: prepared.tags.clone(),
            metadata: prepared.metadata.clone(),
            abi: prepared.abi.clone(),
            wasm: newer_bytes,
        },
    )
    .unwrap();
    crate::evaluator_package::write(
        &path,
        crate::evaluator_package::PackageInput {
            name: prepared.name,
            version: prepared.version,
            description: prepared.description,
            tags: prepared.tags,
            metadata: prepared.metadata,
            abi: prepared.abi,
            wasm: prepared.wasm_bytes,
        },
    )
    .unwrap();
    let original_hash = entry.artifact_blake3.clone();
    entry.artifact_blake3 = "0".repeat(64);
    assert!(
        publish_archive(&context, path.clone(), &entry)
            .await
            .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM evaluators")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    entry.artifact_blake3 = original_hash;
    publish_archive(&context, newer_path, &newer_entry)
        .await
        .unwrap();
    let newer_before: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(e) FROM evaluators e WHERE version = $1")
            .bind(newer_version)
            .fetch_one(&pool)
            .await
            .unwrap();
    publish_archive(&context, path, &entry).await.unwrap();
    let newer_after: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(e) FROM evaluators e WHERE version = $1")
            .bind(newer_version)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        newer_after, newer_before,
        "installing an older collection must preserve the newer row exactly"
    );
    sqlx::query("UPDATE evaluators SET state = 'disabled' WHERE version = $1")
        .bind(&entry.version)
        .execute(&pool)
        .await
        .unwrap();
    // No proxy listener exists: a replay must finish without trying any HTTP request.
    let client = reqwest::Client::builder()
        .proxy(reqwest::Proxy::all("http://127.0.0.1:1").unwrap())
        .timeout(Duration::from_millis(100))
        .build()
        .unwrap();
    install_entry(&context, &client, "2026.10.28.1", &entry)
        .await
        .unwrap();
    entry.artifact_blake3 = "0".repeat(64);
    let error = install_entry(&context, &client, "2026.10.28.1", &entry)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("conflicts with catalog"));
    let states: Vec<String> =
        sqlx::query_scalar("SELECT state::text FROM evaluators ORDER BY version")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(states, ["disabled", "active"]);
    context
        .dbr()
        .await
        .unwrap()
        .control()
        .await
        .unwrap()
        .close()
        .await;
}

fn release_json(collection: &str) -> serde_json::Value {
    serde_json::json!({"tag_name":format!("evaluators/{collection}"), "draft":false,
        "prerelease":false, "assets":[{"name":"catalog.json","size":2000},
            {"name":"sentiment-basic-en-0.1.0.tar.gz","size":123}]})
}

async fn discover_fixture(
    pin: Option<&str>,
    replies: Vec<serde_json::Value>,
) -> anyhow::Result<Catalog> {
    let mut replies = std::collections::VecDeque::from(replies);
    discover(pin, |url, api| {
        assert_eq!(url.scheme(), "https");
        assert_eq!(
            url.host_str().unwrap(),
            if api { "api.github.com" } else { "github.com" }
        );
        let reply = replies.pop_front().expect("unexpected request");
        std::future::ready(Ok(response_body(serde_json::to_vec(&reply).unwrap())))
    })
    .await
}

#[tokio::test]
async fn latest_compatible_selection_and_explicit_pin_use_collection_catalogs() {
    let supported = serde_json::to_value(catalog()).unwrap();
    let mut unsupported = supported.clone();
    unsupported["collection"] = "2026.10.29.1".into();
    unsupported["evaluators"][0]["wit"]["version"] = "999.0.0".into();
    let selected = discover_fixture(
        None,
        vec![
            serde_json::json!([release_json("2026.10.28.1"), release_json("2026.10.29.1")]),
            unsupported.clone(),
            supported.clone(),
        ],
    )
    .await
    .unwrap();
    assert_eq!(selected.collection, "2026.10.28.1");
    let selected = discover_fixture(
        Some("2026.10.28.1"),
        vec![release_json("2026.10.28.1"), supported],
    )
    .await
    .unwrap();
    assert_eq!(selected.collection, "2026.10.28.1");
    assert!(
        discover_fixture(
            Some("2026.10.29.1"),
            vec![release_json("2026.10.29.1"), unsupported]
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn discovery_rejects_missing_assets_forged_catalogs_and_hidden_releases() {
    for mutate in [
        (|r: &mut serde_json::Value| r["assets"] = serde_json::json!([]))
            as fn(&mut serde_json::Value),
        |r| r["assets"][1]["size"] = 999.into(),
        |r| r["draft"] = true.into(),
        |r| r["prerelease"] = true.into(),
        |r| r["tag_name"] = "evaluators/2026.10.27.1".into(),
    ] {
        let mut release = release_json("2026.10.28.1");
        mutate(&mut release);
        assert!(
            discover_fixture(
                Some("2026.10.28.1"),
                vec![release, serde_json::to_value(catalog()).unwrap()]
            )
            .await
            .is_err()
        );
    }
    let mut forged = serde_json::to_value(catalog()).unwrap();
    forged["collection"] = "2026.10.27.1".into();
    assert!(
        discover_fixture(
            None,
            vec![serde_json::json!([release_json("2026.10.28.1")]), forged]
        )
        .await
        .is_err()
    );
    assert!(
        discover_fixture(None, vec![serde_json::json!([])])
            .await
            .is_err()
    );
    let error = discover(None, |_, _| {
        std::future::ready(Err(anyhow::anyhow!("transport failure")))
    })
    .await;
    assert!(error.is_err());
}

#[tokio::test]
async fn discovery_paginates_and_stops_at_an_explicit_limit() {
    let mut unrelated = release_json("2026.10.28.1");
    unrelated["tag_name"] = "vigilo/v0.1.0".into();
    let full_page = serde_json::json!(vec![unrelated; 100]);
    let selected = discover_fixture(
        None,
        vec![
            full_page.clone(),
            serde_json::json!([release_json("2026.10.28.1")]),
            serde_json::to_value(catalog()).unwrap(),
        ],
    )
    .await
    .unwrap();
    assert_eq!(selected.collection, "2026.10.28.1");
    let error = discover_fixture(None, vec![full_page; MAX_RELEASES as usize])
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("discovery limit reached"));
}
