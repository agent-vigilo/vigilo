//! PostgreSQL coverage for embedded and explicit setup migration sources.

use super::*;

/// CI's template1 already contains the production schema. A private schema in
/// SQLx's per-test database exercises a fresh migration history without dropping
/// thousands of inherited partitions in one lock-heavy transaction.
async fn isolated_schema(pool: &PgPool) -> PgPool {
    sqlx::query("CREATE SCHEMA setup_fixture")
        .execute(pool)
        .await
        .unwrap();
    let options = (*pool.connect_options())
        .clone()
        .options([("search_path", "setup_fixture")]);
    PgPool::connect_with(options).await.unwrap()
}

#[sqlx::test(migrations = false)]
#[ignore = "requires PostgreSQL"]
async fn embedded_setup_is_repeatable_and_does_not_publish_evaluators(pool: PgPool) {
    let pool = isolated_schema(&pool).await;
    migrate(&pool, None).await.unwrap();
    let before: Vec<(i64, chrono::DateTime<chrono::Utc>, Vec<u8>)> = sqlx::query_as(
        "SELECT version, installed_on, checksum FROM _sqlx_migrations ORDER BY version",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(before.len(), EMBEDDED.iter().count());
    migrate(&pool, None).await.unwrap();
    let after: Vec<(i64, chrono::DateTime<chrono::Utc>, Vec<u8>)> = sqlx::query_as(
        "SELECT version, installed_on, checksum FROM _sqlx_migrations ORDER BY version",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(before, after);
    let evaluators: i64 = sqlx::query_scalar("SELECT count(*) FROM evaluators")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(evaluators, 0);
    pool.close().await;
}

#[sqlx::test(migrations = false)]
#[ignore = "requires PostgreSQL"]
async fn explicit_source_applies_pending_only_and_rejects_changed_history(pool: PgPool) {
    let pool = isolated_schema(&pool).await;
    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("0001_create_probe.sql");
    std::fs::write(&first, "CREATE TABLE setup_probe (id integer PRIMARY KEY);").unwrap();
    migrate(&pool, Some(dir.path().to_owned())).await.unwrap();
    let primary: Option<String> =
        sqlx::query_scalar("SELECT to_regclass('database_placements')::text")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        primary.is_none(),
        "override must not apply embedded migrations"
    );
    std::fs::write(
        dir.path().join("0002_seed_probe.sql"),
        "INSERT INTO setup_probe VALUES (1);",
    )
    .unwrap();
    migrate(&pool, Some(dir.path().to_owned())).await.unwrap();
    migrate(&pool, Some(dir.path().to_owned())).await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM setup_probe")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    std::fs::write(&first, "CREATE TABLE setup_probe (id bigint PRIMARY KEY);").unwrap();
    let error = migrate(&pool, Some(dir.path().to_owned()))
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("modified"));
    pool.close().await;
}

#[sqlx::test(migrations = false)]
#[ignore = "requires PostgreSQL"]
async fn failed_migration_rolls_back_and_can_be_retried(pool: PgPool) {
    let pool = isolated_schema(&pool).await;
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("0001_create_probe.sql");
    std::fs::write(
        &file,
        "CREATE TABLE setup_probe (id integer); SELECT missing_column FROM setup_probe;",
    )
    .unwrap();
    assert!(migrate(&pool, Some(dir.path().to_owned())).await.is_err());
    let table: Option<String> = sqlx::query_scalar("SELECT to_regclass('setup_probe')::text")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(table.is_none());
    std::fs::write(file, "CREATE TABLE setup_probe (id integer);").unwrap();
    migrate(&pool, Some(dir.path().to_owned())).await.unwrap();
    pool.close().await;
}

#[sqlx::test(migrations = false)]
#[ignore = "requires PostgreSQL"]
async fn concurrent_setup_serializes_and_missing_override_fails(pool: PgPool) {
    let pool = isolated_schema(&pool).await;
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing");
    assert!(migrate(&pool, Some(missing)).await.is_err());
    let history: Option<String> =
        sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations')::text")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(history.is_none());
    let (first, second) = tokio::join!(migrate(&pool, None), migrate(&pool, None));
    first.unwrap();
    second.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count as usize, EMBEDDED.iter().count());
    pool.close().await;
}
