//! The three independently configured library roles share the normal distributed
//! workflows. An unavailable execution binding must leave healthy work usable.

use vigilo::operations::{
    Coordinator,
    DatabaseCredential,
    DatabasePurpose,
    RoleDatabaseBinding,
    RoleDatabaseBindings,
    RunClient,
    Worker,
    WorkerConfig,
};

use super::*;

const UNAVAILABLE: &str = "unavailable";

impl Fixture {
    fn role_bindings(
        &self,
        role: usize,
        unavailable: &str,
    ) -> anyhow::Result<RoleDatabaseBindings> {
        let mut databases = BTreeMap::new();
        let mut placements = vec![(PRIMARY_ALIAS, DatabasePurpose::Control, &*self.db.urls[0])];
        if role != 2 {
            placements.extend([
                (SHARD_ALIAS, DatabasePurpose::Execution, &*self.db.urls[1]),
                (UNAVAILABLE, DatabasePurpose::Execution, unavailable),
            ]);
        }
        for (alias, purpose, source) in placements {
            let mut url = url::Url::parse(source)?;
            url.set_username(&self.db.roles[role])
                .map_err(|_| anyhow::anyhow!("fixture username"))?;
            url.set_password(Some("fixture"))
                .map_err(|_| anyhow::anyhow!("fixture password"))?;
            let other_roles = (0..3)
                .filter(|&index| index != role)
                .map(|index| self.db.roles[index].clone())
                .collect::<Vec<_>>()
                .try_into()
                .unwrap();
            databases.insert(
                alias.into(),
                RoleDatabaseBinding {
                    purpose,
                    schema: "public".into(),
                    function_owner: self.db.roles[3].clone(),
                    credential: DatabaseCredential {
                        database_role: self.db.roles[role].clone(),
                        database_url: url.to_string(),
                    },
                    other_roles,
                },
            );
        }
        let mut config = RoleDatabaseBindings::new(
            PRIMARY_ALIAS.into(),
            SHARD_ALIAS.into(),
            "single-default".into(),
            databases,
        );
        config.acquire_timeout = Duration::from_secs(2);
        config.operation_timeout = Duration::from_secs(3);
        Ok(config)
    }

    async fn exercise_roles(&mut self) -> anyhow::Result<()> {
        self.setup_resources().await?;
        sqlx::query("INSERT INTO database_placements(alias,database_url_env,role,status) VALUES($1,'UNAVAILABLE_DATABASE_URL','shard','active')")
            .bind(UNAVAILABLE).execute(&self.db.admin[0]).await?;
        let unavailable = unavailable_postgres_url()?;
        let mut broker = BrokerConfig::new(std::env::var("MESSAGING_URL")?);
        broker.namespace = Some(self.namespace.clone());
        let owner = *self.owners.keys().next().unwrap();
        let caller = UserContext::new(owner, owner).map_err(anyhow::Error::msg)?;
        let user = RunClient::new_for_user(
            self.role_bindings(2, &unavailable)?,
            self.host.clone(),
            caller,
            None,
        )
        .await?;
        let coordinator = Coordinator::new(
            self.role_bindings(0, &unavailable)?,
            self.host.clone(),
            broker.clone(),
            vigilo::operations::CoordinatorOptions {
                tick_seconds: 1,
                ..Default::default()
            },
        )
        .await?;
        let mut config = WorkerConfig::new(
            self.role_bindings(1, &unavailable)?,
            self.host.clone(),
            broker,
        );
        config.agent_credentials_allow_http_loopback = true;
        let worker = Worker::new(config).await?;

        let upload = self.upload(owner, 101).await?;
        let run = Uuid::now_v7();
        self.object_owners.insert(run, owner);
        let prepared = user
            .prepare_run(self.input(run, upload)?, &self.stop)
            .await?;
        let other =
            user.for_caller(UserContext::new(owner, Uuid::now_v7()).map_err(anyhow::Error::msg)?)?;
        assert!(matches!(
            other.describe(&prepared).await,
            Err(vigilo::operations::Error::Conflict(_))
        ));
        let accepted = user.accept_run(prepared, &self.stop).await?;
        assert_eq!(accepted.data.run_id, run);
        let cancel = self.stop.clone();
        self.tasks
            .push(tokio::spawn(async move { coordinator.run(cancel).await }));
        let cancel = self.stop.clone();
        self.tasks
            .push(tokio::spawn(async move { worker.run(cancel).await }));
        self.sample_oracle(run, 101).await?;
        assert_eq!(self.agent.calls.load(Ordering::SeqCst), 101);
        let published: i64 = sqlx::query_scalar("SELECT count(*) FROM outbox_events WHERE aggregate_id=$1 AND event_type='run.completed'")
            .bind(run).fetch_one(&self.db.admin[0]).await?;
        assert_eq!(published, 1);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn role_specific_constructors_advance_healthy_work() -> anyhow::Result<()> {
    if std::env::var(E2E_ENABLED_ENV).ok().as_deref() != Some("1") {
        return Ok(());
    }
    let mut fixture = Fixture::new().await?;
    let run = std::panic::AssertUnwindSafe(Box::pin(fixture.exercise_roles()))
        .catch_unwind()
        .await;
    let cleanup = fixture.cleanup().await;
    match run {
        Ok(run) => run.and(cleanup),
        Err(panic) => {
            cleanup?;
            std::panic::resume_unwind(panic);
        }
    }
}
