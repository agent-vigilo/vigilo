//! Service-free configuration and authority allocation contracts.
use super::*;

pub(super) fn config() -> DatabaseBindings {
    let credential = |role: &str| DatabaseCredential {
        database_url: format!("postgres://{role}:fixture@127.0.0.1:5432/vigilo"),
        database_role: role.into(),
    };
    DatabaseBindings::new(
        "primary".into(),
        BTreeMap::from([(
            "primary".into(),
            DatabaseBinding {
                purpose: DatabasePurpose::ControlAndExecution,
                schema: "public".into(),
                function_owner: "function_owner".into(),
                scheduler: credential("scheduler"),
                execution: credential("execution"),
                user: credential("reader"),
            },
        )]),
    )
}

#[tokio::test]
async fn fixed_bindings_are_lazy_shared_and_close_together() {
    let pools = DatabasePools::new(config()).unwrap();
    let clone = pools.clone();
    assert!(Arc::ptr_eq(&pools.0, &clone.0));
    let client = pools.user_client();
    assert_eq!(pools.0.placements["primary"].user.pool.size(), 0);
    pools.close().await;
    assert!(clone.0.placements["primary"].user.pool.is_closed());
    drop(client);
}

#[tokio::test]
async fn invalid_bindings_fail_before_connecting_without_exposing_credentials() {
    for change in 0..10 {
        let mut c = config();
        match change {
            0 => c.max_connections = 0,
            1 => c.acquire_timeout = Duration::ZERO,
            2 => c.operation_timeout = Duration::ZERO,
            3 => c.control_alias = "missing".into(),
            4 => c.databases.get_mut("primary").unwrap().purpose = DatabasePurpose::Execution,
            5 => c.databases.get_mut("primary").unwrap().schema = "pg_catalog".into(),
            6 => {
                c.databases
                    .get_mut("primary")
                    .unwrap()
                    .execution
                    .database_role = "reader".into()
            }
            7 => {
                c.databases.get_mut("primary").unwrap().user.database_url =
                    "postgres://reader@localhost/vigilo".into()
            }
            8 => {
                c.databases.get_mut("primary").unwrap().user.database_url =
                    "postgres://reader:secret@localhost/vigilo?options=-c%20role=postgres".into()
            }
            _ => {
                c.databases.get_mut("primary").unwrap().user.database_url =
                    "postgres://other:secret@localhost/vigilo".into()
            }
        }
        let error = DatabasePools::new(c).err().expect("invalid binding");
        assert!(!format!("{error:?}").contains("secret"));
    }
}

#[test]
fn authority_function_allowlists_follow_placement_purpose() {
    let c = config();
    let mut binding = c.databases["primary"].clone();
    for purpose in [
        DatabasePurpose::Control,
        DatabasePurpose::Execution,
        DatabasePurpose::ControlAndExecution,
    ] {
        binding.purpose = purpose;
        for authority in [Authority::Scheduler, Authority::Execution, Authority::User] {
            let guard = validation::Guard::new(
                &binding,
                &binding.scheduler,
                authority,
                Duration::from_secs(30),
            );
            let allowed = guard.allowed_signatures();
            if authority == Authority::Scheduler {
                assert!(allowed.contains(&"vigilo_claim_outbox(bigint,integer)"));
                assert_eq!(
                    allowed.contains(&"vigilo_claim_dispatch(text[])"),
                    purpose != DatabasePurpose::Execution
                );
                assert_eq!(
                    allowed.contains(&"vigilo_claim_chunk_recovery(integer,bigint)"),
                    purpose != DatabasePurpose::Control
                );
                assert!(
                    !allowed
                        .contains(&"vigilo_verify_worker_identity(uuid,smallint,uuid,uuid,text)")
                );
            } else if authority == Authority::Execution && purpose != DatabasePurpose::Control {
                assert_eq!(
                    allowed,
                    ["vigilo_verify_worker_identity(uuid,smallint,uuid,uuid,text)"]
                );
            } else {
                assert!(allowed.is_empty());
            }
        }
    }
}

#[test]
fn ambient_connection_options_cannot_override_bindings() {
    const PROBE: &str = "VIGILO_BINDINGS_AMBIENT_PROBE";
    if std::env::var_os(PROBE).is_some() {
        let c = config();
        assert!(connection_options(&c.databases["primary"].scheduler).is_err());
        return;
    }
    let output=std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact","operations::database_bindings::tests::ambient_connection_options_cannot_override_bindings","--nocapture"])
        .env(PROBE,"1").env("PGOPTIONS","-c role=unexpected")
        .output().unwrap();
    assert!(
        output.status.success(),
        "ambient override probe failed: {}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[tokio::test]
async fn internal_routers_preserve_authority_and_share_bound_pool_lifetimes() {
    let mut c = config();
    let mut shard = c.databases["primary"].clone();
    shard.purpose = DatabasePurpose::Execution;
    for credential in [&mut shard.scheduler, &mut shard.execution, &mut shard.user] {
        credential.database_role.push_str("_shard");
        credential.database_url = format!(
            "postgres://{}:fixture@127.0.0.1:1/unused",
            credential.database_role
        );
    }
    c.databases.insert("shard".into(), shard);
    let pools = DatabasePools::new(c).unwrap();
    let mut held = Vec::new();
    for (authority, role) in [
        (Authority::Scheduler, "scheduler"),
        (Authority::Execution, "execution"),
        (Authority::User, "reader"),
    ] {
        let router = pools
            .router(authority, "primary".into(), "single-default".into())
            .unwrap();
        assert!(router.uri.is_empty());
        let context = crate::context::Context::for_operations(router.clone());
        assert_eq!(
            context
                .dbr()
                .await
                .unwrap()
                .control()
                .await
                .unwrap()
                .connect_options()
                .get_username(),
            role
        );
        let routed = router
            .execution_write_route(
                crate::db::workflows::local_shard_admission::LocalShardRouteHint {
                    run_id: uuid::Uuid::now_v7(),
                    run_shard: 1,
                    database_alias: "shard".into(),
                    write_epoch: 1,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            routed.pool.connect_options().get_username(),
            format!("{role}_shard")
        );
        assert_eq!(routed.pool.size(), 0);
        assert!(!router.database_url_env_is_resolved("DATABASE_URL"));
        assert!(
            router.validate_placement_config().await.is_err(),
            "administrative setup cannot use a runtime authority domain"
        );
        held.push((context, routed.pool));
    }
    assert!(
        pools
            .router(Authority::User, "unknown".into(), "single-default".into())
            .is_err()
    );
    pools.close().await;
    for (context, execution) in held {
        assert!(
            context
                .dbr()
                .await
                .unwrap()
                .control()
                .await
                .unwrap()
                .is_closed()
        );
        assert!(execution.is_closed());
    }
}
