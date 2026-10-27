//! Elapsed-time admission renewal and bounded work under creation authority.
//!
//! Deadlines are minted before renewal. Local operations include pool/lock waits;
//! dropping their future rolls back any open transaction. Checkpoints and final
//! activation independently recheck live generation, including after lock waits.
use std::time::Duration;

use anyhow::Result;
use sqlx::PgPool;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::{
    blob_storage::Budget,
    db::workflows::run_plan::{
        Lease,
        admission,
    },
};

const RENEW_AFTER: Duration = Duration::from_secs(60);
const _: () = assert!(RENEW_AFTER.as_secs() + crate::blob_storage::DEFAULT_OPERATION_SECONDS < 300);

pub(super) struct Clock {
    next_renewal: Instant,
}
impl Clock {
    pub(super) fn new() -> Self {
        Self {
            next_renewal: Instant::now(),
        }
    }

    /// A 60-second renewal cadence plus the 120-second read budget stays inside
    /// the 300-second lease. The first operation explicitly checks authority.
    pub(super) async fn budget(&mut self, db: &PgPool, lease: Lease) -> Result<Budget> {
        let budget = Budget::standard();
        let now = Instant::now();
        if now >= self.next_renewal {
            admission::renew(db, lease).await?;
            self.next_renewal = now + RENEW_AFTER;
        }
        Ok(budget)
    }

    /// Bound the complete database operation, including placement resolution.
    pub(super) async fn database<T>(
        &mut self,
        db: &PgPool,
        lease: Lease,
        cancel: &CancellationToken,
        work: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        let deadline = Instant::now() + Duration::from_secs(30);
        let budget = self.budget(db, lease).await?;
        tokio::select! { biased;
            _ = cancel.cancelled() => anyhow::bail!("run admission cancelled"),
            _ = tokio::time::sleep_until(deadline.min(budget.deadline)) => anyhow::bail!("placement admission database deadline exceeded"),
            result = work => result,
        }
    }
}
