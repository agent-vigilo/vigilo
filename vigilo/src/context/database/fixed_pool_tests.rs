//! Fixed routing never discovers credentials or selects another authority.
use std::collections::BTreeMap;

use super::*;

fn config() -> Config {
    Config::new(
        String::new(),
        1,
        Duration::from_secs(1),
        Default::default(),
        PlacementConfig::default_single_database(),
    )
}
fn lazy(role: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy(&format!("postgres://{role}:fixture@127.0.0.1:1/unused"))
        .unwrap()
}

#[tokio::test]
async fn fixed_routes_reuse_exact_pools_without_metadata_or_environment_discovery() {
    let control = lazy("user_control");
    let shard = lazy("user_shard");
    let router = DatabaseRouter::from_fixed_pools(
        config(),
        BTreeMap::from([
            (
                "primary".into(),
                FixedPlacementPool {
                    pool: control.clone(),
                    role: "control_and_shard",
                },
            ),
            (
                "shard".into(),
                FixedPlacementPool {
                    pool: shard.clone(),
                    role: "shard",
                },
            ),
        ]),
    )
    .unwrap();
    assert_eq!(
        router
            .control()
            .await
            .unwrap()
            .connect_options()
            .get_username(),
        "user_control"
    );
    let hint = LocalShardRouteHint {
        run_id: Uuid::now_v7(),
        run_shard: 0,
        database_alias: "shard".into(),
        write_epoch: 1,
    };
    let routed = router.execution_write_route(hint.clone()).await.unwrap();
    assert_eq!(routed.pool.connect_options().get_username(), "user_shard");
    assert_eq!(control.size(), 0);
    assert_eq!(shard.size(), 0);
    control.close().await;
    shard.close().await;
    assert!(
        routed.pool.is_closed(),
        "router must retain the supplied pool, not open a replacement"
    );
    let mut absent = hint;
    absent.database_alias = "unbound".into();
    let error = router.execution_write_route(absent).await.err().unwrap();
    assert!(
        error.to_string().contains("fixed database binding"),
        "{error:#}"
    );
}

#[tokio::test]
async fn fixed_construction_rejects_operator_fallback_and_inconsistent_purposes() {
    for (uri, role) in [
        (
            "postgres://operator:secret@localhost/db",
            "control_and_shard",
        ),
        ("", "control"),
        ("", "shard"),
        ("", "invalid"),
    ] {
        let mut c = config();
        c.uri = uri.into();
        let error = DatabaseRouter::from_fixed_pools(
            c,
            BTreeMap::from([(
                "primary".into(),
                FixedPlacementPool {
                    pool: lazy("reader"),
                    role,
                },
            )]),
        )
        .err()
        .expect("invalid fixed construction");
        assert!(!format!("{error:#}").contains("secret"));
    }
}

#[tokio::test]
async fn control_only_bindings_cannot_be_used_for_message_execution() {
    let mut c = config();
    c.placement_config =
        PlacementConfig::new("primary".into(), "shard".into(), "single-default".into()).unwrap();
    let router = DatabaseRouter::from_fixed_pools(
        c,
        BTreeMap::from([
            (
                "primary".into(),
                FixedPlacementPool {
                    pool: lazy("control"),
                    role: "control",
                },
            ),
            (
                "shard".into(),
                FixedPlacementPool {
                    pool: lazy("data"),
                    role: "shard",
                },
            ),
        ]),
    )
    .unwrap();
    let hint = LocalShardRouteHint {
        run_id: Uuid::now_v7(),
        run_shard: 0,
        database_alias: "primary".into(),
        write_epoch: 1,
    };
    assert!(
        router
            .execution_write_route(hint)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("cannot serve execution work")
    );
    assert_eq!(router.control().await.unwrap().size(), 0);
}
