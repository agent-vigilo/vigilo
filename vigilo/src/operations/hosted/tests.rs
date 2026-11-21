//! Service-free constructor limits, explicit broker input and cancellation.
use std::collections::BTreeMap;

use super::*;
use crate::operations::{
    DatabaseBinding,
    DatabaseBindings,
    DatabaseCredential,
    DatabasePurpose,
    resources::{
        self,
        Bindings,
        Request,
        Resolution,
        Resolver,
    },
};

struct Deny;
impl Resolver for Deny {
    fn bindings(&self, _: Request) -> Resolution<'_, Bindings> {
        Box::pin(async { Err(resources::Error::Denied) })
    }
}
fn config() -> HostedConfig {
    let credential = |name: &str| DatabaseCredential {
        database_role: name.into(),
        database_url: format!("postgresql://{name}:fixture@127.0.0.1:1/vigilo"),
    };
    let databases = DatabasePools::new(DatabaseBindings::new(
        "primary".into(),
        BTreeMap::from([(
            "primary".into(),
            DatabaseBinding {
                purpose: DatabasePurpose::ControlAndExecution,
                schema: "public".into(),
                function_owner: "owner".into(),
                scheduler: credential("scheduler"),
                execution: credential("execution"),
                user: credential("reader"),
            },
        )]),
    ))
    .unwrap();
    HostedConfig::new(
        databases,
        Arc::new(Deny),
        BrokerConfig::new("amqp://host:fixture@127.0.0.1:1".into()),
        "primary".into(),
        "single-default".into(),
    )
}

#[tokio::test]
async fn invalid_configuration_precedes_database_or_broker_io() {
    for invalid in 0..12 {
        let mut config = config();
        match invalid {
            0 => config.worker_max_inflight_chunks = 0,
            1 => config.policy_max_age_seconds = 0,
            2 => config.worker_host = Some("invalid\nidentity".into()),
            3 => config.coordinator.tick_seconds = 0,
            4 => config.coordinator.run_creation.case_page_budget = 0,
            5 => config.wasm.fuel_per_evaluation = 0,
            6 => config.agent_http.max_response_bytes = 0,
            7 => config.evaluator_http_concurrency = 0,
            8 => config.evaluator_http.request_timeout = Duration::ZERO,
            9 => config.broker.uri = "amqp://host:do-not-log@127.0.0.1:1#invalid".into(),
            10 => config.broker.namespace = Some("invalid namespace".into()),
            _ => config.policy_max_age_seconds = 3601,
        }
        let pools = config.databases.clone();
        let error = HostedRuntime::new(config)
            .await
            .err()
            .expect("invalid configuration");
        assert!(
            matches!(error, Error::InvalidConfiguration(_)),
            "case {invalid}: {error}"
        );
        assert!(!error.to_string().contains("do-not-log"));
        pools.close().await;
    }
}

#[test]
fn explicit_broker_topology_has_no_ambient_namespace() {
    let config = BrokerConfig::new("amqps://host:fixture@broker.example/vhost?heartbeat=30".into());
    let default = config.config().unwrap();
    let mut namespaced = config.clone();
    namespaced.namespace = Some("cell-1".into());
    let scoped = namespaced.config().unwrap();
    assert_ne!(default.worker_queue, scoped.worker_queue);
    assert!(scoped.worker_queue.contains("cell-1"));
    for uri in [
        "https://host:fixture@broker.example",
        "amqp://broker.example",
        "amqp://host:@broker.example",
        "amqp://host:fixture@broker.example/#secret",
    ] {
        assert!(BrokerConfig::new(uri.into()).config().is_err());
    }
}

#[tokio::test]
async fn cancelled_services_admit_no_work_and_need_no_connections() {
    let config = config();
    let context = Context::for_operations(
        config
            .databases
            .router(
                Authority::Scheduler,
                "primary".into(),
                "single-default".into(),
            )
            .unwrap(),
    )
    .with_runtime_dependencies(config.broker.config().unwrap(), config.wasm);
    let coordinator = Coordinator {
        context: context.clone(),
        id: Uuid::now_v7(),
        options: config.coordinator,
        active: Arc::new(tokio::sync::Mutex::new(())),
    };
    let worker = Worker {
        service: Arc::new(worker::Service::new(context, Uuid::now_v7(), None)),
        max_inflight: 1,
        active: Arc::new(tokio::sync::Mutex::new(())),
    };
    let cancel = CancellationToken::new();
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(1), coordinator.run(cancel.clone()))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), worker.run(cancel))
        .await
        .unwrap()
        .unwrap();
    config.databases.close().await;
}

#[test]
fn wasm_limits_preserve_disabled_logging_and_reject_unbounded_settings() {
    let mut limits = WasmOptions {
        max_log_messages: 0,
        ..Default::default()
    };
    assert!(
        limits.validate().is_ok(),
        "standalone logging may be disabled"
    );
    limits.max_memory_bytes = u64::MAX;
    assert!(limits.validate().is_err());
    limits = WasmOptions::default();
    limits.fuel_per_evaluation = u64::MAX;
    assert!(limits.validate().is_err());
    limits = WasmOptions::default();
    limits.timeout_ms = 0;
    assert!(limits.validate().is_err());
}
