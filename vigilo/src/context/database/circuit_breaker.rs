//! Database-specific circuit-breaker policy.
//!
//! Breakers are keyed by database alias and affect only runtime admission.
//! Durable routing and work remain authoritative in PostgreSQL, and explicit
//! administrative database access bypasses these transient states.

use super::DatabaseOperationTimeout;
pub(super) use crate::circuit_breaker::FailureImpact;
pub(crate) use crate::circuit_breaker::{
    CircuitBreakers as DatabaseCircuitBreakers,
    CircuitOpen,
    CircuitPermit,
    CircuitTransition,
    Config as CircuitBreakerConfig,
};

pub(super) fn classify_error(error: &anyhow::Error) -> FailureImpact {
    if error
        .chain()
        .any(|cause| cause.downcast_ref::<DatabaseOperationTimeout>().is_some())
    {
        return FailureImpact::Unavailable;
    }
    let Some(error) = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<sqlx::Error>())
    else {
        return FailureImpact::Available;
    };

    match error {
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::Protocol(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed
        | sqlx::Error::BeginFailed => FailureImpact::Unavailable,
        sqlx::Error::Database(error) => classify_database_error_code(error.code().as_deref()),
        _ => FailureImpact::Available,
    }
}

pub(crate) fn is_database_contention(error: &anyhow::Error) -> bool {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<sqlx::Error>())
        .and_then(|error| match error {
            sqlx::Error::Database(error) => error.code(),
            _ => None,
        })
        .is_some_and(|code| is_database_contention_code(Some(code.as_ref())))
}

pub(crate) fn is_database_unavailable(error: &anyhow::Error) -> bool {
    classify_error(error) == FailureImpact::Unavailable
}

fn classify_database_error_code(code: Option<&str>) -> FailureImpact {
    let Some(code) = code else {
        return FailureImpact::Available;
    };

    if code == "57014"
        || code.starts_with("08")
        || code.starts_with("53")
        || matches!(code, "57P01" | "57P02" | "57P03" | "58030")
    {
        FailureImpact::Unavailable
    } else {
        FailureImpact::Available
    }
}

fn is_database_contention_code(code: Option<&str>) -> bool {
    matches!(code, Some("40001" | "40P01" | "55P03"))
}

#[cfg(test)]
mod tests {
    use std::time::{
        Duration,
        Instant,
    };

    use sqlx::PgPool;

    use super::*;

    fn config() -> CircuitBreakerConfig {
        CircuitBreakerConfig::new(true, 2, Duration::from_secs(10), Duration::from_secs(40))
            .unwrap()
            .without_jitter()
    }

    #[test]
    fn database_failure_codes_distinguish_outages_from_contention() {
        for code in ["08006", "57P01", "57014"] {
            assert_eq!(
                classify_database_error_code(Some(code)),
                FailureImpact::Unavailable
            );
        }
        for code in ["40001", "23505", "55P03"] {
            assert_eq!(
                classify_database_error_code(Some(code)),
                FailureImpact::Available
            );
        }
        assert_eq!(
            classify_error(&anyhow::Error::new(sqlx::Error::PoolTimedOut)),
            FailureImpact::Unavailable
        );
        assert_eq!(
            classify_error(&anyhow::Error::new(DatabaseOperationTimeout::new(
                "shard_001",
                "test",
                Duration::from_secs(30),
            ))),
            FailureImpact::Unavailable
        );
        for code in ["40001", "40P01", "55P03"] {
            assert!(is_database_contention_code(Some(code)));
        }
        for code in ["57014", "23505"] {
            assert!(!is_database_contention_code(Some(code)));
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
    async fn open_database_circuit_does_not_block_healthy_database_work(pool: PgPool) {
        let breakers = DatabaseCircuitBreakers::new(config());
        let now = Instant::now();
        open(&breakers, "shard_001", now);

        assert!(breakers.acquire("shard_001", now).is_err());
        let healthy = breakers.acquire("primary", now).unwrap();
        let value = sqlx::query_scalar::<_, i32>("SELECT 1")
            .fetch_one(&pool)
            .await
            .unwrap();
        let _ = breakers.record_success(healthy);

        assert_eq!(value, 1);
        assert!(breakers.acquire("primary", now).is_ok());
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx migration tests"]
    async fn successful_database_probe_restores_normal_admission(pool: PgPool) {
        let breakers = DatabaseCircuitBreakers::new(config());
        let opened_at = Instant::now();
        open(&breakers, "primary", opened_at);
        let probe_at = opened_at + Duration::from_secs(10);
        let probe = breakers.acquire("primary", probe_at).unwrap();

        sqlx::query("SELECT 1").execute(&pool).await.unwrap();
        let _ = breakers.record_success(probe);

        assert!(!breakers.acquire("primary", probe_at).unwrap().is_probe());
    }

    fn open(breakers: &DatabaseCircuitBreakers, alias: &str, now: Instant) {
        for _ in 0..2 {
            let permit = breakers.acquire(alias, now).unwrap();
            let _ = breakers.record_failure(permit, now, FailureImpact::Unavailable);
        }
    }
}
