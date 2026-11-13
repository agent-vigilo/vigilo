//! Caller identity for short preparation transactions and bounded metadata reads.
//!
//! The trusted operation binds one caller for a preparation. Lease-fenced writes
//! retain it through checkpoints and publication; accepted execution claims obtain
//! fresh system authority separately. Standalone reads retain their existing
//! submission counts. No transaction is retained across storage I/O.
use sqlx::{
    PgPool,
    Postgres,
    Transaction,
};
use uuid::Uuid;

use super::{
    Object,
    Plan,
    bounded,
    queries,
};
use crate::db::{
    scope,
    scope::UserContext,
};

#[derive(Clone, Copy)]
pub(crate) struct Access<'a> {
    pub(super) db: &'a PgPool,
    pub(super) caller: Option<UserContext>,
}
impl<'a> Access<'a> {
    pub(crate) fn new(db: &'a PgPool, caller: Option<UserContext>) -> Self {
        Self { db, caller }
    }
    pub(super) async fn transaction(
        self,
        run_id: Uuid,
    ) -> anyhow::Result<Transaction<'static, Postgres>> {
        Ok(match self.caller {
            Some(caller) => scope::begin(self.db, caller, run_id).await?,
            None => self.db.begin().await?,
        })
    }
    pub(crate) async fn load(self, id: Uuid) -> anyhow::Result<Option<Plan>> {
        if self.caller.is_none() {
            return super::load(self.db, id).await;
        }
        bounded(async {
            let mut tx = self.transaction(id).await?;
            let row = sqlx::query_as(queries::SELECT_PLAN)
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(row)
        })
        .await
    }
    pub(crate) async fn objects(
        self,
        id: Uuid,
        kind: &str,
        level: i16,
        after: i32,
    ) -> anyhow::Result<Vec<Object>> {
        if self.caller.is_none() {
            return super::objects(self.db, id, kind, level, after).await;
        }
        bounded(async {
            let mut tx = self.transaction(id).await?;
            let rows = sqlx::query_as(queries::SELECT_OBJECTS)
                .bind(id)
                .bind(kind)
                .bind(level)
                .bind(after)
                .fetch_all(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(rows)
        })
        .await
    }
    pub(crate) async fn pending_probes(self, id: Uuid) -> anyhow::Result<Vec<Object>> {
        if self.caller.is_none() {
            return super::pending_probes(self.db, id).await;
        }
        bounded(async {
            let mut tx = self.transaction(id).await?;
            let rows = sqlx::query_as(queries::PENDING_PROBES)
                .bind(id)
                .fetch_all(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(rows)
        })
        .await
    }
}
