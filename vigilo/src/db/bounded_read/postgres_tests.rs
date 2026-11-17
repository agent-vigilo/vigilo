//! PostgreSQL proof that excluded retrieval projections are not evaluated.
use sqlx::PgPool;
use uuid::Uuid;

use super::*;

async fn fetch(
    pool: &PgPool,
    expression: &str,
    rows: i64,
    bytes: i64,
) -> Result<Vec<Row>, sqlx::Error> {
    let admission = "SELECT $1::uuid IS NOT NULL AND $2::smallint[] IS NOT NULL AND $3::jsonb IS NOT NULL AND $4::jsonb IS NULL AND $6::text<>'' AS valid";
    let candidates = "SELECT 0 AS kind,a.valid,jsonb_build_array(0,g) AS key,100::bigint AS bytes,g::bigint AS ordinal FROM admission a CROSS JOIN generate_series(1,4) g ORDER BY g LIMIT $5";
    let data = format!("SELECT b.kind,b.key,{expression} AS body FROM selected b");
    let query = statement(admission, candidates, &data, true);
    sqlx::query_as(&query)
        .bind(Uuid::nil())
        .bind(vec![0i16])
        .bind(sqlx::types::Json(serde_json::json!([])))
        .bind(None::<sqlx::types::Json<serde_json::Value>>)
        .bind(rows + 1)
        .bind("fixture")
        .bind(bytes)
        .fetch_all(pool)
        .await
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn byte_and_row_lookahead_never_evaluates_excluded_bodies(pool: PgPool) {
    // A body expression that would fail proves the guard runs on the server,
    // before retrieval projection, rather than merely dropping a client row.
    let oversized = fetch(&pool, "(1/(b.ordinal-1))::text", 1, 50)
        .await
        .unwrap();
    assert!(oversized.iter().all(|row| row.4.is_none()));
    let admitted = fetch(&pool, "(1/(b.ordinal-1))::text", 1, 101)
        .await
        .unwrap_err();
    assert!(
        matches!(admitted,sqlx::Error::Database(error) if error.code().as_deref()==Some("22012"))
    );

    // The second body would fail; it is row lookahead, not a selected record.
    let lookahead = fetch(&pool, "(1/(b.ordinal-2))::text", 1, 1_000)
        .await
        .unwrap();
    assert_eq!(lookahead.len(), 2);
    assert_eq!(lookahead[0].4.as_deref(), Some("-1"));
    assert!(lookahead[1].4.is_none());

    let prefix = fetch(&pool, "b.ordinal::text", 3, 203).await.unwrap();
    assert_eq!(prefix.len(), 4);
    assert_eq!(prefix.iter().filter(|row| row.4.is_some()).count(), 2);
    assert_eq!(prefix[0].4.as_deref(), Some("1"));
    assert_eq!(prefix[1].4.as_deref(), Some("2"));
}
