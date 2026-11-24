//! Compile the operation boundary from an independent host, without polling I/O.
//!
//! Typed outputs and Send futures must be usable without SQLx, Clap, internal
//! modules or a CLI subprocess. Live authority and distributed behavior belong
//! to the named integration tiers; constructing a future does not prove them.
use std::{
    future::Future,
    sync::Arc,
};

use tokio_util::sync::CancellationToken;
use vigilo::operations::{
    self,
    Client,
    Error,
    HostedRuntime,
    HostedUser,
    PrepareRun,
    PreparedRun,
    RunClient,
    UserClient,
    UserContext,
    datasets,
    evaluators,
    export,
    resources,
    setup,
};

const STORAGE: &str =
    "provider='s3'\nalias='cases'\nbucket='cases'\nprefix='consumer'\nregion='us-east-1'\n";

/// Drop unpolled I/O while checking the exact output and executor compatibility.
fn operation<T>(future: impl Future<Output = Result<T, Error>> + Send) {
    drop(future);
}

struct Host;
impl resources::Resolver for Host {
    fn bindings(&self, _: resources::Request) -> resources::Resolution<'_, resources::Bindings> {
        Box::pin(async { Err(resources::Error::Denied) })
    }
}
impl evaluators::TestResolver for Host {
    fn resolve(
        &self,
        _: evaluators::TestRequest,
    ) -> resources::Resolution<'_, evaluators::TestAuthority> {
        Box::pin(async { Err(resources::Error::Denied) })
    }
}

pub fn check(client: &Client, restricted: &UserClient, caller: UserContext) {
    let id = "0198f36d-f7db-7d90-998c-d60d498dd979".parse().unwrap();
    let cancel = CancellationToken::new();
    operation::<operations::RunStatus>(client.run_status(id));
    operation::<setup::MigrationOutcome>(setup::apply_migrations(
        client,
        &setup::DatabaseIdentity {
            system_identifier: 1,
            database: "vigilo".into(),
            schema: "public".into(),
        },
        None,
    ));
    operation::<setup::PlacementValidation>(setup::validate_placements(
        client,
        &setup::PlacementValidationRequest {
            default_shard_database_alias: "primary".into(),
            database_url_references: Default::default(),
        },
    ));
    operation::<setup::EvaluatorInstallation>(setup::install_official_evaluators(
        client,
        &setup::EvaluatorCollectionPin {
            collection: "2026.10.28.1".into(),
            catalog_sha256: "0".repeat(64),
        },
        None,
    ));
    operation::<operations::RunResults>(client.run_results(id));
    operation::<operations::Cancellation>(client.cancel_run(id));
    operation::<operations::ControlOutcome>(
        client.control_run(id, operations::ExecutionControl::Paused),
    );
    operation::<operations::ControlOutcome>(client.run_control_status(id));
    operation::<export::ExportPage>(client.export_page(
        id,
        export::ExportCollection::Executions,
        Default::default(),
        None,
    ));
    operation::<operations::RunStatus>(restricted.run_status(caller, id));
    operation::<operations::RunResults>(restricted.run_results(caller, id));
    let _ = |page: operations::RunPage| -> Vec<operations::RunListItem> { page.items };
    operation::<operations::RunPage>(
        restricted.list_runs(caller, operations::ListRunsRequest::default()),
    );

    let runs = client.runs(STORAGE).unwrap();
    operation::<PreparedRun>(runs.prepare_run(input(), &cancel));
    operation::<operations::RunSubmission>(runs.create_run(input(), &cancel));
    operation::<operations::RunSubmission>(runs.resume_creation(input(), &cancel));
    // These consume sealed values produced only by verified preparation.
    let _ = prepared_run;
    let _ = prepared_dataset;
    let _ = prepared_package;

    let dataset = client.datasets(STORAGE).unwrap();
    dataset_operations(&dataset, &cancel);
    operation::<datasets::PreparedDataset>(datasets::PreparedDataset::prepare(
        &b"{}"[..],
        datasets::InputFormat::Json,
        datasets::Batching::default(),
        &cancel,
    ));
    operation::<datasets::DatasetStatus>(client.dataset_status(id));
    operation::<datasets::Aborted>(client.abort_dataset_upload(id));
    // Maintenance is bounded and restricted to the operator root.
    drop(client.expire_dataset_uploads());
    let host: Arc<dyn resources::Resolver> = Arc::new(Host);
    let service = restricted.datasets(STORAGE, host).unwrap();
    dataset_operations(&service.user(caller), &cancel);
    operation::<datasets::DatasetStatus>(restricted.dataset_status(caller, id));
    operation::<datasets::Aborted>(restricted.abort_dataset_upload(caller, id));

    let evaluator = client
        .evaluators(
            "vigilo",
            evaluators::Options {
                test_authority: Some(Arc::new(Host)),
                ..Default::default()
            },
        )
        .unwrap();
    evaluator_operations(&evaluator, &cancel);
    operation::<evaluators::EvaluatorService>(restricted.evaluators("vigilo", Default::default()));
    let _ = registry_service;

    let config = || operations::UserConfig {
        control: operations::Config::new("postgresql://localhost:1/vigilo".into()),
        database_role: "reader".into(),
        execution_databases: Default::default(),
    };
    let ordinary = export::ExportClient::new(config()).unwrap();
    operation::<export::ExportPage>(ordinary.page(
        caller,
        id,
        export::ExportCollection::Executions,
        Default::default(),
        None,
    ));
    drop(ordinary.close());
    let sensitive = export::SensitiveExportClient::new(config()).unwrap();
    operation::<export::ExportPage>(sensitive.content_page(
        caller,
        id,
        export::ExportCollection::ExecutionContent,
        Default::default(),
        None,
    ));
    operation::<export::ExportPage>(sensitive.diagnostics_page(
        caller,
        id,
        Default::default(),
        None,
    ));
    drop(sensitive.close());
    let _ = hosted;
    let _ = construction;
    let _ = |event: operations::RunCompletedEventV1| (event.tenant_id, event.run_id);
    let _ = role_construction;
    shared::<Client>();
    shared::<UserClient>();
    shared::<HostedRuntime>();
    shared::<HostedUser>();
    shared::<evaluators::EvaluatorClient>();
    shared::<datasets::DatasetClient>();
    shared::<export::ExportClient>();
    shared::<export::SensitiveExportClient>();
}

fn input() -> PrepareRun {
    PrepareRun {
        run_id: "0198f36d-f7db-7d90-998c-d60d498dd979".parse().unwrap(),
        dataset_upload_id: "0198f36d-f7db-7d90-998c-d60d498dd980".parse().unwrap(),
        profile: serde_json::json!({}),
    }
}
fn prepared_run(client: &RunClient, prepared: PreparedRun, cancel: &CancellationToken) {
    operation::<operations::RunSubmission>(client.describe(&prepared));
    let _ = prepared.run_id();
    let _ = prepared.manifest().digest();
    let _ = prepared.configuration();
    operation::<operations::RunSubmission>(client.accept_run(prepared, cancel));
}
fn dataset_operations(client: &datasets::DatasetClient, cancel: &CancellationToken) {
    let id = input().dataset_upload_id;
    operation::<datasets::Publication>(client.resume(
        id,
        &b"{}"[..],
        datasets::InputFormat::Json,
        cancel,
    ));
    operation::<datasets::Cleanup>(client.cleanup(id, 100, cancel));
}
fn prepared_dataset(
    client: &datasets::DatasetClient,
    prepared: datasets::PreparedDataset,
    cancel: &CancellationToken,
) {
    let _ = prepared.identity();
    operation::<datasets::Publication>(client.upload(prepared, None, cancel));
}
fn evaluator_operations(client: &evaluators::EvaluatorClient, cancel: &CancellationToken) {
    let id = evaluators::EvaluatorId::parse("vigilo/sentiment-basic-en:0.1.0").unwrap();
    operation::<evaluators::Descriptor>(client.inspect(&id));
    operation::<evaluators::Page>(client.search("vigilo", None, Default::default(), None));
    operation::<()>(client.set_state(&id, evaluators::State::Deprecated, None));
    operation::<evaluators::PreparedPackage>(client.prepare_package(&b""[..], cancel));
}
fn prepared_package(
    client: &evaluators::EvaluatorClient,
    prepared: evaluators::PreparedPackage,
    input: evaluators::TestInput,
    cancel: &CancellationToken,
) {
    let _ = (prepared.name(), prepared.version(), prepared.content_hash());
    operation::<evaluators::Publication>(client.publish(prepared));
    let id = evaluators::EvaluatorId::parse("vigilo/sentiment-basic-en:0.1.0").unwrap();
    operation::<evaluators::TestResult>(client.test(&id, input, cancel));
}
fn registry_service(service: &evaluators::EvaluatorService, caller: UserContext) {
    let other = service.for_namespace("fixture").unwrap();
    evaluator_operations(&other.user(caller), &CancellationToken::new());
}
fn hosted(runtime: &HostedRuntime, user: &HostedUser, cancel: CancellationToken) {
    let id = input().run_id;
    let _ = runtime.user(UserContext::new(id, input().dataset_upload_id).unwrap());
    let _ = runtime.datasets(STORAGE);
    operation::<evaluators::EvaluatorService>(runtime.evaluators("vigilo", Default::default()));
    drop(runtime.close());
    let coordinator = runtime.coordinator();
    operation::<()>(coordinator.once());
    operation::<()>(coordinator.run(cancel.clone()));
    let worker = runtime.worker();
    operation::<usize>(worker.once());
    operation::<()>(worker.run(cancel));
    let _ = user.runs();
    operation::<operations::RunStatus>(user.run_status(id));
    operation::<operations::RunResults>(user.run_results(id));
    operation::<operations::ControlOutcome>(
        user.control_run(id, operations::ExecutionControl::Paused),
    );
    operation::<operations::ControlOutcome>(user.run_control_status(id));
    operation::<datasets::DatasetStatus>(user.dataset_status(id));
    operation::<datasets::Aborted>(user.abort_dataset_upload(id));
}

fn shared<T: Send + Sync>() {}
fn construction(config: operations::HostedConfig, pools: &operations::DatabasePools) {
    operation::<HostedRuntime>(HostedRuntime::new(config));
    operation::<()>(pools.validate());
    drop(pools.close());
}

fn role_construction(
    binding: operations::RoleDatabaseBinding,
    broker: operations::BrokerConfig,
    caller: operations::UserContext,
    client: &operations::RunClient,
) {
    let config = operations::RoleDatabaseBindings::new(
        "primary".into(),
        "primary".into(),
        "single-default".into(),
        [("primary".into(), binding)].into(),
    );
    let resources: Arc<dyn resources::Resolver> = Arc::new(Host);
    let worker = operations::WorkerConfig::new(config.clone(), resources.clone(), broker.clone());
    operation::<operations::RunClient>(operations::RunClient::new_for_user(
        config.clone(),
        resources.clone(),
        caller,
        None,
    ));
    operation::<operations::Coordinator>(operations::Coordinator::new(
        config,
        resources,
        broker,
        Default::default(),
    ));
    operation::<operations::Worker>(operations::Worker::new(worker));
    let _: Result<operations::RunClient, Error> = client.for_caller(caller);
}
