//! Caller-scoped dataset ledger access without transactions across remote work.
//!
//! Operator calls retain their existing statement/transaction boundaries. Hosted
//! calls set caller settings inside an owned transaction; pooled
//! connections never retain request identity. Single-statement helpers commit
//! before returning, and dropping a scoped connection rolls back via SQLx.
use std::ops::{
    Deref,
    DerefMut,
};

use sqlx::{
    PgConnection,
    PgPool,
    Postgres,
    Transaction,
    pool::PoolConnection,
};

use crate::db::scope::UserContext;

#[derive(Clone, Copy)]
pub(crate) struct Access<'a> {
    pool: &'a PgPool,
    caller: Option<UserContext>,
}
impl<'a> From<&'a PgPool> for Access<'a> {
    fn from(pool: &'a PgPool) -> Self {
        Self { pool, caller: None }
    }
}
impl<'a> Access<'a> {
    pub(crate) fn scoped(pool: &'a PgPool, caller: UserContext) -> Self {
        Self {
            pool,
            caller: Some(caller),
        }
    }
    pub(crate) async fn begin(self) -> Result<Transaction<'static, Postgres>, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        if let Some(caller) = self.caller {
            // Establish SQLx ownership before fallible setup so cancellation or a
            // rejected setting always queues rollback on the same connection.
            sqlx::query("SELECT set_config('vigilo.tenant_id',$1,true),set_config('vigilo.actor_id',$2,true),set_config('vigilo.run_id','',true),set_config('vigilo.run_batch','',true)")
                .bind(caller.tenant_id().to_string()).bind(caller.actor_id().to_string())
                .execute(&mut *tx).await?;
        }
        Ok(tx)
    }

    pub(crate) async fn connection(self) -> anyhow::Result<Connection> {
        super::bounded(async {
            Ok(if self.caller.is_some() {
                Connection::Scoped(self.begin().await?)
            } else {
                Connection::Operator(self.pool.acquire().await?)
            })
        })
        .await
    }
    /// Validate scoped ownership using the same row already needed by the flow.
    pub(crate) fn verify(self, row: &super::Upload) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.caller.is_none_or(|c| c.tenant_id() == row.tenant_id),
            "dataset upload owner changed"
        );
        Ok(())
    }
    pub(crate) async fn load(self, id: uuid::Uuid) -> anyhow::Result<super::Upload> {
        let mut c = self.connection().await?;
        let row = super::load(&mut *c, id).await?;
        self.verify(&row)?;
        c.finish().await?;
        Ok(row)
    }
    pub(crate) async fn inspect(
        self,
        id: uuid::Uuid,
    ) -> anyhow::Result<(super::Upload, Option<super::Catalog>)> {
        let mut c = self.connection().await?;
        let row = super::load(&mut *c, id).await?;
        self.verify(&row)?;
        let catalog = super::catalog(&mut *c, id).await?;
        c.finish().await?;
        Ok((row, catalog))
    }
}
pub(crate) enum Connection {
    Operator(PoolConnection<Postgres>),
    Scoped(Transaction<'static, Postgres>),
}
impl Deref for Connection {
    type Target = PgConnection;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Operator(c) => c,
            Self::Scoped(c) => c,
        }
    }
}
impl DerefMut for Connection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Operator(c) => c,
            Self::Scoped(c) => c,
        }
    }
}
impl Connection {
    pub(crate) async fn finish(self) -> anyhow::Result<()> {
        super::bounded(async {
            if let Self::Scoped(tx) = self {
                tx.commit().await?;
            }
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires PostgreSQL"]
    async fn dataset_identity_is_transaction_local_and_clears_run_authority(pool: PgPool) {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_with(pool.connect_options().as_ref().clone())
            .await
            .unwrap();
        let caller = UserContext::new(Uuid::now_v7(), Uuid::now_v7()).unwrap();
        let mut c = Access::scoped(&pool, caller).connection().await.unwrap();
        let (tenant, actor, run): (String,String,String)=sqlx::query_as("SELECT COALESCE(current_setting('vigilo.tenant_id',true),''),COALESCE(current_setting('vigilo.actor_id',true),''),COALESCE(current_setting('vigilo.run_id',true),'')")
            .fetch_one(&mut *c).await.unwrap();
        assert_eq!(tenant, caller.tenant_id().to_string());
        assert_eq!(actor, caller.actor_id().to_string());
        assert_eq!(run, "");
        c.finish().await.unwrap();
        let mut c = Access::from(&pool).connection().await.unwrap();
        let actor: Option<String> =
            sqlx::query_scalar("SELECT current_setting('vigilo.actor_id',true)")
                .fetch_one(&mut *c)
                .await
                .unwrap();
        assert!(actor.is_none_or(|s| s.is_empty()));
        drop(c);
        for outcome in 0..3 {
            let mut c = Access::scoped(&pool, caller).connection().await.unwrap();
            if outcome == 1 {
                assert!(sqlx::query("SELECT 1/0").execute(&mut *c).await.is_err());
            }
            if outcome == 2 {
                assert!(
                    tokio::time::timeout(
                        std::time::Duration::from_millis(10),
                        sqlx::query("SELECT pg_sleep(0.1)").execute(&mut *c)
                    )
                    .await
                    .is_err()
                );
            }
            // The same physical connection must be safe after future drop or SQL error.
            drop(c);
            let c = Access::from(&pool).connection().await.unwrap();
            drop(c);
            let actor: Option<String> =
                sqlx::query_scalar("SELECT current_setting('vigilo.actor_id',true)")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert!(actor.is_none_or(|s| s.is_empty()));
        }
        pool.close().await;
    }
}
