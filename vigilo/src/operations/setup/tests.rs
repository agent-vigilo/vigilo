//! Public setup boundary checks against an empty PostgreSQL installation schema.

use sqlx::PgPool;

use super::*;
use crate::operations::Config;

#[tokio::test]
async fn malformed_pins_fail_before_connecting() {
    let client = Client::new(Config::new("postgresql://localhost:1/vigilo".into())).unwrap();
    for (collection, catalog_sha256) in
        [("latest", "0".repeat(64)), ("2026.10.28.1", "0".repeat(63))]
    {
        let pin = EvaluatorCollectionPin {
            collection: collection.into(),
            catalog_sha256,
        };
        assert!(matches!(
            install_official_evaluators(&client, &pin, None).await,
            Err(Error::InvalidInput(_))
        ));
    }
}

#[sqlx::test(migrations = false)]
#[ignore = "requires PostgreSQL"]
async fn operator_setup_checks_identity_before_ddl_and_replays_on_the_exact_schema(pool: PgPool) {
    sqlx::query("CREATE SCHEMA setup_fixture")
        .execute(&pool)
        .await
        .unwrap();
    let (system_identifier, database): (i64, String) = sqlx::query_as(
        "SELECT system_identifier, current_database()::text FROM pg_control_system()",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let target = DatabaseIdentity {
        system_identifier,
        database,
        schema: "setup_fixture".into(),
    };
    let mut url = url::Url::parse(&std::env::var("DATABASE_URL").unwrap()).unwrap();
    url.set_path(&target.database);
    url.query_pairs_mut()
        .append_pair("options", "-c search_path=setup_fixture,public");
    let client = Client::new(Config::new(url.to_string())).unwrap();
    for wrong in [
        DatabaseIdentity {
            system_identifier: system_identifier ^ 1,
            ..target.clone()
        },
        DatabaseIdentity {
            database: "unexpected_database".into(),
            ..target.clone()
        },
        DatabaseIdentity {
            schema: "public".into(),
            ..target.clone()
        },
    ] {
        let error = apply_migrations(&client, &wrong, None).await.unwrap_err();
        assert!(matches!(error, Error::Conflict(_)), "{error:?}");
        let history: Option<String> =
            sqlx::query_scalar("SELECT to_regclass('setup_fixture._sqlx_migrations')::text")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(history, None);
    }

    let first = apply_migrations(&client, &target, None).await.unwrap();
    assert_eq!(first.target, target);
    let directory = tempfile::tempdir().unwrap();
    assert!(
        apply_migrations(&client, &target, Some(&directory.path().join("missing")))
            .await
            .is_err()
    );
    let read_history = || {
        sqlx::query_as::<_, (i64, chrono::DateTime<chrono::Utc>)>(
            "SELECT version, installed_on FROM setup_fixture._sqlx_migrations ORDER BY version",
        )
        .fetch_all(&pool)
    };
    let baseline = read_history().await.unwrap();
    assert!(!baseline.is_empty());
    assert_eq!(
        apply_migrations(&client, &target, None).await.unwrap(),
        first
    );
    assert_eq!(baseline, read_history().await.unwrap());

    let mut request = PlacementValidationRequest {
        default_shard_database_alias: "primary".into(),
        database_url_references: BTreeSet::new(),
    };
    assert_eq!(
        validate_placements(&client, &request).await.unwrap(),
        PlacementValidation {
            control_database_alias: "primary".into(),
            default_shard_database_alias: "primary".into(),
            serviceable_placements: 1,
        }
    );
    sqlx::query(
        "INSERT INTO setup_fixture.database_placements(alias, database_url_env, role, status)
        VALUES ('shard_fixture', 'VIGILO_SETUP_FIXTURE_REFERENCE', 'shard', 'active')",
    )
    .execute(&pool)
    .await
    .unwrap();
    request.default_shard_database_alias = "shard_fixture".into();
    assert!(validate_placements(&client, &request).await.is_err());
    request
        .database_url_references
        .insert("VIGILO_SETUP_FIXTURE_REFERENCE".into());
    let validated = validate_placements(&client, &request).await.unwrap();
    assert_eq!(validated.default_shard_database_alias, "shard_fixture");
    assert_eq!(validated.serviceable_placements, 2);
    client.database.control().await.unwrap().close().await;
}
