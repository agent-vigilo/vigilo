//! Schema migration runner for database setup.
//!
//! Embedded SQL is the default source; an explicit directory replaces it.
//! SQLx owns migration locking, history/checksum validation, and transactional
//! application. Setup applies only pending migrations and fails on inconsistent
//! history or invalid SQL before continuing to placement validation.

use std::path::PathBuf;

use anyhow::Context;
use sqlx::{
    PgPool,
    migrate::Migrator,
};

static EMBEDDED: Migrator = sqlx::migrate!("./migrations");

/// Applies pending migrations using embedded SQL or an explicit directory.
///
/// The directory override never falls back to embedded SQL on failure. Applied
/// versions and checksums are checked under SQLx's migration lock, and each
/// pending migration is transactional unless its SQL explicitly opts out.
pub(crate) async fn migrate(db: &PgPool, migrations_dir: Option<PathBuf>) -> anyhow::Result<()> {
    let directory = match migrations_dir {
        Some(path) => Some(
            Migrator::new(path.as_path())
                .await
                .with_context(|| format!("load migrations from {}", path.display()))?,
        ),
        None => None,
    };
    let migrator = directory.as_ref().unwrap_or(&EMBEDDED);
    let mut connection = db.acquire().await.context("acquire migration connection")?;
    // SQLx releases its session lock on success only. Never return a session
    // holding that lock to the pool after an error or cancelled setup.
    connection.close_on_drop();
    let result = migrator.run_direct(&mut *connection).await;
    let close_result = connection.close().await;
    result.context("apply database migrations")?;
    close_result.context("close migration connection")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use sqlx::PgPool;
    use uuid::Uuid;

    use crate::models::{
        database_placement::{
            DEFAULT_DATABASE_ALIAS,
            DEFAULT_DATABASE_URL_ENV,
            DatabasePlacement,
        },
        shard_placement::ShardPlacement,
    };

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
    async fn migrations_seed_primary_database_placement(pool: PgPool) {
        let placement = sqlx::query_as::<_, DatabasePlacement>(
            r#"
            SELECT alias, database_url_env, role, status, created_at, updated_at
            FROM database_placements
            WHERE alias = $1
            "#,
        )
        .bind(DEFAULT_DATABASE_ALIAS)
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(placement.alias, DEFAULT_DATABASE_ALIAS);
        assert_eq!(placement.database_url_env, DEFAULT_DATABASE_URL_ENV);
        assert_eq!(placement.role, "control_and_shard");
        assert_eq!(placement.status, "active");
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
    async fn database_placements_allow_only_one_active_control_placement(pool: PgPool) {
        let second_active_control = sqlx::query(
            r#"
            INSERT INTO database_placements (alias, database_url_env, role, status)
            VALUES ('control_002', 'VIGILO_CONTROL_002_DATABASE_URL', 'control', 'active')
            "#,
        )
        .execute(&pool)
        .await;
        assert!(second_active_control.is_err());

        sqlx::query(
            r#"
            INSERT INTO database_placements (alias, database_url_env, role, status)
            VALUES ('control_disabled', 'VIGILO_DISABLED_CONTROL_DATABASE_URL', 'control', 'disabled')
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            r#"
            INSERT INTO database_placements (alias, database_url_env, role, status)
            VALUES ('shard_001', 'VIGILO_SHARD_001_DATABASE_URL', 'shard', 'active')
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
    async fn shard_placements_enforce_alias_status_and_shard_range(pool: PgPool) {
        let run_id = Uuid::now_v7();

        sqlx::query(
            r#"
            INSERT INTO database_placements (alias, database_url_env, role, status)
            VALUES ('shard_001', 'VIGILO_SHARD_001_DATABASE_URL', 'shard', 'active')
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();

        let placement = sqlx::query_as::<_, ShardPlacement>(
            r#"
            INSERT INTO shard_placements (run_id, run_shard, database_alias, status)
            VALUES ($1::uuid, 42, 'primary', 'active')
            RETURNING run_id, run_shard, database_alias, status, move_target_database_alias,
                      route_version, write_epoch, created_at, updated_at
            "#,
        )
        .bind(run_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(placement.run_id, run_id);
        assert_eq!(placement.run_shard, 42);
        assert_eq!(placement.database_alias, DEFAULT_DATABASE_ALIAS);
        assert_eq!(placement.status, "active");
        assert_eq!(placement.route_version, 1);

        let invalid_shard = sqlx::query(
            r#"
            INSERT INTO shard_placements (run_id, run_shard, database_alias, status)
            VALUES ($1::uuid, 128, 'primary', 'active')
            "#,
        )
        .bind(Uuid::now_v7())
        .execute(&pool)
        .await;
        assert!(invalid_shard.is_err());

        let invalid_alias = sqlx::query(
            r#"
            INSERT INTO shard_placements (run_id, run_shard, database_alias, status)
            VALUES ($1::uuid, 1, 'missing', 'active')
            "#,
        )
        .bind(Uuid::now_v7())
        .execute(&pool)
        .await;
        assert!(invalid_alias.is_err());

        let invalid_status = sqlx::query(
            r#"
            INSERT INTO shard_placements (run_id, run_shard, database_alias, status)
            VALUES ($1::uuid, 2, 'primary', 'paused')
            "#,
        )
        .bind(Uuid::now_v7())
        .execute(&pool)
        .await;
        assert!(invalid_status.is_err());

        let active_with_target = sqlx::query(
            r#"
            INSERT INTO shard_placements (
                run_id,
                run_shard,
                database_alias,
                status,
                move_target_database_alias
            )
            VALUES ($1::uuid, 3, 'primary', 'active', 'shard_001')
            "#,
        )
        .bind(Uuid::now_v7())
        .execute(&pool)
        .await;
        assert!(active_with_target.is_err());

        let moving_without_target = sqlx::query(
            r#"
            INSERT INTO shard_placements (run_id, run_shard, database_alias, status)
            VALUES ($1::uuid, 4, 'primary', 'moving')
            "#,
        )
        .bind(Uuid::now_v7())
        .execute(&pool)
        .await;
        assert!(moving_without_target.is_err());

        let move_target_matches_owner = sqlx::query(
            r#"
            INSERT INTO shard_placements (
                run_id,
                run_shard,
                database_alias,
                status,
                move_target_database_alias
            )
            VALUES ($1::uuid, 5, 'primary', 'draining', 'primary')
            "#,
        )
        .bind(Uuid::now_v7())
        .execute(&pool)
        .await;
        assert!(move_target_matches_owner.is_err());

        sqlx::query(
            r#"
            INSERT INTO shard_placements (
                run_id,
                run_shard,
                database_alias,
                status,
                move_target_database_alias
            )
            VALUES ($1::uuid, 6, 'primary', 'draining', 'shard_001')
            "#,
        )
        .bind(Uuid::now_v7())
        .execute(&pool)
        .await
        .unwrap();
    }
}

#[cfg(test)]
mod setup_tests;
