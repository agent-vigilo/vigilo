//! Elapsed-time renewal and stale-authority checks against PostgreSQL.
use super::*;
#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn verification_reads_renew_by_time_and_expired_authority_is_not_resurrected(db: PgPool) {
    let identity = crate::contracts::case_storage::identity::DatasetIdentity {
        dataset_id: uuid::Uuid::now_v7(),
        dataset_version: None,
        dataset_version_id: uuid::Uuid::now_v7(),
        dataset_hash: "1".repeat(64),
        case_count: 1,
    };
    let row = ledger::begin(
        &db,
        uuid::Uuid::now_v7(),
        "cases",
        &"2".repeat(64),
        &identity,
        &crate::contracts::case_storage::FormatLimits::default(),
    )
    .await
    .unwrap();
    let lease = ledger::claim(&db, row.id, false).await.unwrap();
    let mut read = ReadLease::new();
    read.budget(&db, lease).await.unwrap();
    let first = ledger::load(&db, row.id).await.unwrap().leased_until;
    for _ in 0..128 {
        read.budget(&db, lease).await.unwrap();
    }
    assert_eq!(
        ledger::load(&db, row.id).await.unwrap().leased_until,
        first,
        "reads must not renew per file"
    );
    read.next_renewal = Instant::now();
    read.budget(&db, lease).await.unwrap();
    assert!(ledger::load(&db, row.id).await.unwrap().leased_until > first);
    sqlx::query(
        "UPDATE dataset_uploads SET leased_until=clock_timestamp()-interval '1 second' WHERE id=$1",
    )
    .bind(row.id)
    .execute(&db)
    .await
    .unwrap();
    read.next_renewal = Instant::now();
    assert!(read.budget(&db, lease).await.is_err());
}
