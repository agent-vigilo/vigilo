// PostgreSQL-backed workflow scenarios and fixtures.

use sqlx::PgPool;

use super::*;

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for local shard admission tests"]
async fn stale_write_epoch_is_rejected_without_mutating_local_state(pool: PgPool) {
    let run_id = Uuid::now_v7();
    upsert_local_shard_admission(
        &pool,
        LocalShardAdmissionDraft {
            run_id,
            run_shard: 7,
            database_alias: "primary".to_string(),
            write_epoch: 2,
            state: LocalShardAdmissionState::Open,
            redirect_database_alias: None,
            move_fence: None,
        },
    )
    .await
    .unwrap();

    let error = validate_local_shard_admission(
        &pool,
        &LocalShardRouteHint {
            run_id,
            run_shard: 7,
            database_alias: "primary".to_string(),
            write_epoch: 1,
        },
        LocalShardWriteKind::NewWork,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error.downcast_ref::<LocalShardAdmissionError>(),
        Some(LocalShardAdmissionError::StaleWriteEpoch {
            expected_write_epoch: 1,
            actual_write_epoch: 2,
            ..
        })
    ));
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for local shard admission tests"]
async fn draining_allows_settlement_but_rejects_new_work(pool: PgPool) {
    let run_id = Uuid::now_v7();
    let hint = LocalShardRouteHint {
        run_id,
        run_shard: 3,
        database_alias: "primary".to_string(),
        write_epoch: 4,
    };
    upsert_local_shard_admission(
        &pool,
        LocalShardAdmissionDraft {
            run_id,
            run_shard: 3,
            database_alias: "primary".to_string(),
            write_epoch: 4,
            state: LocalShardAdmissionState::Draining,
            redirect_database_alias: Some("shard_001".to_string()),
            move_fence: None,
        },
    )
    .await
    .unwrap();

    validate_local_shard_admission(&pool, &hint, LocalShardWriteKind::Settlement)
        .await
        .unwrap();
    let error = validate_local_shard_admission(&pool, &hint, LocalShardWriteKind::NewWork)
        .await
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<LocalShardAdmissionError>(),
        Some(LocalShardAdmissionError::RejectedState { .. })
    ));
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires a PostgreSQL DATABASE_URL for local shard admission tests"]
async fn same_epoch_transition_cannot_reopen_closed_owner(pool: PgPool) {
    let run_id = Uuid::now_v7();
    upsert_local_shard_admission(
        &pool,
        LocalShardAdmissionDraft {
            run_id,
            run_shard: 9,
            database_alias: "primary".to_string(),
            write_epoch: 8,
            state: LocalShardAdmissionState::Closed,
            redirect_database_alias: Some("shard_001".to_string()),
            move_fence: None,
        },
    )
    .await
    .unwrap();

    let error = transition_local_shard_admission(
        &pool,
        LocalShardAdmissionDraft {
            run_id,
            run_shard: 9,
            database_alias: "primary".to_string(),
            write_epoch: 8,
            state: LocalShardAdmissionState::Open,
            redirect_database_alias: None,
            move_fence: None,
        },
        &[LocalShardAdmissionState::Open],
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("rejected transition"));
}
