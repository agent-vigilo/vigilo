//! Encoded-size admission and body retrieval in one statement snapshot.
//!
//! Callers supply fixed SQL projections, never request SQL. Candidate CTEs retain
//! only keys, sizes and ordering; bodies enter a materialized CTE only after the
//! per-family prefix fits both budgets. Lookahead metadata reports oversize/full
//! pages without transferring excluded bodies. Native key joins bind retrieval
//! to exactly the admitted records under the statement's single MVCC snapshot.

/// SQL result shared by independent public and operator projections: family,
/// local admission, logical key, encoded size and optional fitting body.
pub(crate) type Row = (
    i32,
    bool,
    Option<sqlx::types::Json<serde_json::Value>>,
    Option<i64>,
    Option<String>,
);

/// Compose fixed fragments. Candidates expose `kind,valid,key,bytes,ordinal`;
/// data exposes `kind,key,body` by joining `selected`. `$5` is row limit plus one;
/// `$7` is the permitted sum of each record's bytes plus one separator. Each
/// family receives its own prefix budget. No rejected body is retained in a CTE.
pub(crate) fn statement(
    admission: &str,
    candidates: &str,
    data: &str,
    single_shard: bool,
) -> String {
    let statement = format!(
        "WITH admission AS ({admission}), candidates AS MATERIALIZED ({candidates}), \
         budget AS MATERIALIZED (SELECT *,sum(bytes+1) OVER(PARTITION BY kind ORDER BY ordinal ROWS UNBOUNDED PRECEDING) AS used FROM candidates), \
         selected AS MATERIALIZED (SELECT * FROM budget WHERE valid AND key IS NOT NULL AND ordinal<$5 AND used<=$7), \
         data AS MATERIALIZED ({data}) \
         SELECT b.kind,b.valid,b.key,b.bytes,d.body FROM budget b LEFT JOIN data d USING(kind,key) ORDER BY b.kind,b.ordinal"
    );
    // Equality exposes a single shard to PostgreSQL's join equivalence classes;
    // a one-element ANY predicate does not provide the same pruning inference.
    // Values stay bound parameters in both shapes.
    if single_shard {
        statement.replace("=ANY($2::smallint[])", "=($2::smallint[])[1]")
    } else {
        statement
    }
}

#[cfg(test)]
#[path = "bounded_read/postgres_tests.rs"]
mod postgres_tests;
