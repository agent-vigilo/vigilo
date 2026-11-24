//! Public control-catalog contracts and PostgreSQL activation races.

use sqlx::{
    Postgres,
    migrate::MigrateDatabase,
};

use super::*;
use crate::operations::{
    Config,
    setup,
};

const SHARD_REF: &str = "VIGILO_TEST_PLACEMENT_SHARD";
const OTHER_REF: &str = "VIGILO_TEST_PLACEMENT_OTHER";
const UNREACHABLE: &str = "postgresql://operator@127.0.0.1:1/unreachable";

fn registration(alias: &str, reference: &str) -> RegisterDatabasePlacement {
    RegisterDatabasePlacement {
        alias: alias.into(),
        database_url_reference: reference.into(),
    }
}

#[tokio::test]
async fn invalid_page_and_undeclared_binding_fail_without_database_access() {
    let client = Client::new(Config::new(UNREACHABLE.into())).unwrap();
    assert!(matches!(
        PlacementClient::new(
            client.clone(),
            [(DEFAULT_DATABASE_URL_ENV.into(), UNREACHABLE.into())].into()
        ),
        Err(Error::InvalidConfiguration(_))
    ));
    let operator = PlacementClient::new(client, BTreeMap::new()).unwrap();
    for page_size in [0, MAX_PAGE_SIZE + 1] {
        assert!(matches!(
            operator
                .list_database_placements(&ListDatabasePlacementsRequest {
                    page_size,
                    after_alias: None
                })
                .await,
            Err(Error::InvalidInput(_))
        ));
    }
    assert!(matches!(
        operator
            .register_database_placement(&registration("shard", SHARD_REF))
            .await,
        Err(Error::InvalidInput(_))
    ));
}

async fn control_client(pool: &PgPool) -> Client {
    let name: String = sqlx::query_scalar("SELECT current_database()::text")
        .fetch_one(pool)
        .await
        .unwrap();
    let mut url = url::Url::parse(&std::env::var(DEFAULT_DATABASE_URL_ENV).unwrap()).unwrap();
    url.set_path(&name);
    Client::new(Config::new(url.to_string())).unwrap()
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn catalog_pages_and_idempotent_registration_use_only_control(pool: PgPool) {
    let operator = PlacementClient::new(
        control_client(&pool).await,
        [
            (SHARD_REF.into(), UNREACHABLE.into()),
            (OTHER_REF.into(), UNREACHABLE.into()),
        ]
        .into(),
    )
    .unwrap();
    let request = registration("shard_000", SHARD_REF);
    let (one, two) = tokio::join!(
        operator.register_database_placement(&request),
        operator.register_database_placement(&request)
    );
    let (one, two) = (one.unwrap(), two.unwrap());
    assert_ne!(one.inserted, two.inserted);
    assert_eq!(one.placement, two.placement);
    assert_eq!(one.placement.status, "provisioning");
    assert!(matches!(
        operator
            .register_database_placement(&registration("shard_000", OTHER_REF))
            .await,
        Err(Error::Conflict(_))
    ));

    sqlx::query("UPDATE database_placements SET status='disabled', updated_at='2000-01-01 00:00:00+00' WHERE alias='shard_000'")
        .execute(&pool).await.unwrap();
    let replay = operator
        .register_database_placement(&request)
        .await
        .unwrap();
    assert!(!replay.inserted);
    assert_eq!(replay.placement.status, "disabled");
    let untouched: bool = sqlx::query_scalar("SELECT updated_at='2000-01-01 00:00:00+00' FROM database_placements WHERE alias='shard_000'")
        .fetch_one(&pool).await.unwrap();
    assert!(untouched);

    sqlx::query("INSERT INTO database_placements (alias,database_url_env,role,status)
        SELECT 'shard_' || lpad(n::text, 3, '0'), $1, 'shard', 'disabled' FROM generate_series(1, 101) n")
        .bind(SHARD_REF).execute(&pool).await.unwrap();
    let first = operator
        .list_database_placements(&ListDatabasePlacementsRequest {
            page_size: 100,
            after_alias: None,
        })
        .await
        .unwrap();
    assert_eq!(first.items.len(), 100);
    assert_eq!(first.next_after_alias.as_deref(), Some("shard_098"));
    let second = operator
        .list_database_placements(&ListDatabasePlacementsRequest {
            page_size: 100,
            after_alias: first.next_after_alias,
        })
        .await
        .unwrap();
    assert!(second.next_after_alias.is_none());
    let aliases = first
        .items
        .into_iter()
        .chain(second.items)
        .map(|row| row.alias)
        .collect::<Vec<_>>();
    assert_eq!(
        aliases,
        std::iter::once("primary".into())
            .chain((0..=101).map(|n| format!("shard_{n:03}")))
            .collect::<Vec<_>>()
    );

    // The limit is enforced before connecting to any of the declared peers.
    operator
        .register_database_placement(&registration("target", SHARD_REF))
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO database_placements (alias,database_url_env,role,status)
        SELECT 'peer_' || n, $1, 'shard', 'active' FROM generate_series(1, 128) n",
    )
    .bind(SHARD_REF)
    .execute(&pool)
    .await
    .unwrap();
    let identity = DatabaseIdentity::observe(&pool).await.unwrap().0.unwrap();
    assert!(matches!(
        operator
            .activate_database_placement("target", &identity)
            .await,
        Err(Error::Conflict(
            "activation exceeds the serviceable peer limit"
        ))
    ));
    operator.control().await.unwrap().close().await;
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn run_shards_are_exact_and_keep_route_and_write_fences(pool: PgPool) {
    let operator = PlacementClient::new(control_client(&pool).await, BTreeMap::new()).unwrap();
    let run = Uuid::now_v7();
    let other = Uuid::now_v7();
    for id in [run, other] {
        crate::db::test_support::seed_run_owner(&pool, id).await;
    }
    sqlx::query(
        "INSERT INTO database_placements(alias, database_url_env, role, status)
        VALUES ('remote', 'UNBOUND_SHARD_FIXTURE', 'shard', 'active')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO shard_placements (run_id, run_shard, database_alias, status, route_version, write_epoch)
        SELECT $1::uuid, shard, 'remote', 'active', 7, 4 FROM generate_series(127, 0, -1) shard
        UNION ALL SELECT $2::uuid, 0, 'primary', 'active', 1, 1")
        .bind(run).bind(other).execute(&pool).await.unwrap();
    let routes = operator.list_run_shards(run).await.unwrap();
    assert_eq!(routes.len(), 128);
    for (shard, route) in (0..128).zip(routes) {
        assert_eq!(route.run_id, run);
        assert_eq!(route.run_shard, shard);
        assert_eq!(route.database_alias, "remote");
        assert_eq!((route.route_version, route.write_epoch), (7, 4));
    }
    assert!(
        operator
            .list_run_shards(Uuid::now_v7())
            .await
            .unwrap()
            .is_empty()
    );
    operator.control().await.unwrap().close().await;
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL"]
async fn activation_uses_verified_setup_and_fences_duplicate_targets(pool: PgPool) {
    let database_name = format!("vigilo_placement_{}", Uuid::now_v7().simple());
    sqlx::query(&format!(
        "CREATE DATABASE {database_name} TEMPLATE template0"
    ))
    .execute(&pool)
    .await
    .unwrap();
    let mut url = url::Url::parse(&std::env::var(DEFAULT_DATABASE_URL_ENV).unwrap()).unwrap();
    url.set_path(&database_name);
    let url = url.to_string();
    let target = Client::new(Config::new(url.clone())).unwrap();
    let target_pool = target.database.control().await.unwrap();
    let identity = DatabaseIdentity::observe(target_pool)
        .await
        .unwrap()
        .0
        .unwrap();
    let operator = PlacementClient::new(
        control_client(&pool).await,
        [(SHARD_REF.into(), url.clone())].into(),
    )
    .unwrap();
    for alias in ["shard_a", "shard_b"] {
        operator
            .register_database_placement(&registration(alias, SHARD_REF))
            .await
            .unwrap();
    }
    let wrong = DatabaseIdentity {
        database: "some_other_database".into(),
        ..identity.clone()
    };
    assert!(matches!(
        operator
            .activate_database_placement("shard_a", &wrong)
            .await,
        Err(Error::Conflict(_))
    ));
    assert!(matches!(
        operator
            .activate_database_placement("shard_a", &identity)
            .await,
        Err(Error::Dependency(_))
    ));
    setup::apply_migrations(&target, &identity, None)
        .await
        .unwrap();

    let run = Uuid::now_v7();
    crate::db::test_support::seed_run_owner(&pool, run).await;
    sqlx::query("INSERT INTO shard_placements (run_id, run_shard, database_alias, status, move_target_database_alias)
        VALUES ($1, 0, 'primary', 'copying', 'shard_a')")
        .bind(run).execute(&pool).await.unwrap();
    let route = operator.list_run_shards(run).await.unwrap();
    let error = operator
        .activate_database_placement("shard_a", &identity)
        .await
        .unwrap_err();
    assert!(format!("{error:?}").contains("references while provisioning"));
    assert_eq!(operator.list_run_shards(run).await.unwrap(), route);
    sqlx::query("UPDATE shard_placements SET status='active', move_target_database_alias=NULL WHERE run_id=$1")
        .bind(run).execute(&pool).await.unwrap();

    // Give both contenders a repeatable-read default and wait for both to
    // reach the commit fence before releasing the catalog. Activation needs
    // to read the winner after waiting, regardless of that default.
    let mut race_url = url::Url::parse(&operator.client.database.uri).unwrap();
    race_url.query_pairs_mut().append_pair(
        "options",
        "-c default_transaction_isolation=repeatable\\ read",
    );
    let race_operator = PlacementClient::new(
        Client::new(Config::new(race_url.to_string())).unwrap(),
        [(SHARD_REF.into(), url.clone())].into(),
    )
    .unwrap();
    let control = race_operator.control().await.unwrap();
    let isolation: String = sqlx::query_scalar("SHOW transaction_isolation")
        .fetch_one(control)
        .await
        .unwrap();
    assert_eq!(isolation, "repeatable read");
    let mut fence = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE database_placements IN EXCLUSIVE MODE")
        .execute(&mut *fence)
        .await
        .unwrap();
    let barrier = tokio::sync::Barrier::new(2);
    let (operator_ref, identity_ref, barrier) = (&race_operator, &identity, &barrier);
    let probe = move |row, peers: Vec<DatabasePlacement>| async move {
        let migrations = operator_ref
            .verify_readiness(control, &row, &peers, identity_ref)
            .await?;
        barrier.wait().await;
        Ok(migrations)
    };
    let activations = async {
        tokio::join!(
            shard_admin::activate_placement_with(
                control,
                "shard_a",
                Some(MAX_ACTIVATION_PEERS),
                probe
            ),
            shard_admin::activate_placement_with(
                control,
                "shard_b",
                Some(MAX_ACTIVATION_PEERS),
                probe
            )
        )
    };
    let unlock = async {
        loop {
            let waiting: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_locks
                WHERE NOT granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database())")
                .fetch_one(&pool).await.unwrap();
            if waiting >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        fence.commit().await.unwrap();
    };
    let ((first, second), ()) = tokio::time::timeout(ACTIVATION_TIMEOUT, async {
        tokio::join!(activations, unlock)
    })
    .await
    .unwrap();
    let (winner, loser) = match (first, second) {
        (Ok(outcome), Err(error)) => {
            assert!(error.is::<shard_admin::PlacementConflict>());
            (outcome, "shard_b")
        }
        (Err(error), Ok(outcome)) => {
            assert!(error.is::<shard_admin::PlacementConflict>());
            (outcome, "shard_a")
        }
        other => panic!("expected exactly one activation, got {other:?}"),
    };
    assert!(winner.changed && winner.readiness_verified && winner.migration_count.unwrap() > 0);
    let replay = operator
        .activate_database_placement(&winner.placement.alias, &identity)
        .await
        .unwrap();
    assert!(!replay.changed && !replay.readiness_verified);
    assert_eq!(replay.placement.status, "active");
    assert!(matches!(
        operator.activate_database_placement(loser, &identity).await,
        Err(Error::Conflict(
            "target database is already used by a serviceable placement"
        ))
    ));
    let blocked = operator
        .register_database_placement(&registration(loser, SHARD_REF))
        .await
        .unwrap();
    assert_eq!(blocked.placement.status, "provisioning");
    let admissions: i64 = sqlx::query_scalar("SELECT count(*) FROM local_shard_admissions")
        .fetch_one(target_pool)
        .await
        .unwrap();
    assert_eq!(admissions, 0);
    control.close().await;
    operator.control().await.unwrap().close().await;
    target_pool.close().await;
    Postgres::drop_database(&url).await.unwrap();
}
