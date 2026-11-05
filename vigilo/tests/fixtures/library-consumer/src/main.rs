//! Independent package proving the supported library boundary without CLI or SQL.
use vigilo::operations::{
    Client,
    Config,
    Error,
    RunStatus,
    UserClient,
    UserConfig,
    UserContext,
};

async fn status(client: &Client) -> Result<RunStatus, Error> {
    client
        .run_status("0198f36d-f7db-7d90-998c-d60d498dd979".parse().unwrap())
        .await
}

fn main() {
    // Construction and future creation must work without Tokio, environment, or I/O.
    let client = Client::new(Config::new("postgresql://localhost:1/vigilo".into())).unwrap();
    drop(status(&client));
    let run_id = "0198f36d-f7db-7d90-998c-d60d498dd979".parse().unwrap();
    drop(client.cancel_run(run_id));
    drop(client.control_run(run_id, vigilo::operations::ExecutionControl::Paused));
    let _runs = client.runs("provider='s3'\nalias='cases'\nbucket='cases'\nprefix='consumer'\nregion='us-east-1'\n").unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let restricted = UserClient::new(UserConfig {
            control: Config::new("postgresql://localhost:1/vigilo".into()),
            database_role: "reader".into(),
            execution_databases: Default::default(),
        })
        .unwrap();
        let context = UserContext::new(
            "0198f36d-f7db-7d90-998c-d60d498dd979".parse().unwrap(),
            "0198f36d-f7db-7d90-998c-d60d498dd980".parse().unwrap(),
        )
        .unwrap();
        drop(restricted.run_status(
            context,
            "0198f36d-f7db-7d90-998c-d60d498dd981".parse().unwrap(),
        ));
    });
    // Optional live contract: the host supplies a prepared database and existing run.
    if let Ok(url) = std::env::var("VIGILO_CONSUMER_DATABASE_URL") {
        let client = Client::new(Config::new(url)).unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let missing = client
                .run_status("00000000-0000-0000-0000-000000000000".parse().unwrap())
                .await;
            assert!(matches!(missing, Err(Error::NotFound(_))));
            let result = status(&client).await.unwrap();
            assert_eq!(result.details.status, "creating");
            assert_eq!(result.progress_source, "run_creation");
            assert!(!result.terminal);
        });
    }
}
