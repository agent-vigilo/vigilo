//! Independent package proving the supported library boundary without CLI or SQL.
use vigilo::operations::{
    Client,
    Config,
    Error,
    RunStatus,
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
