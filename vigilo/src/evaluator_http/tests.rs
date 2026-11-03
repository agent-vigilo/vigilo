use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{
            AtomicUsize,
            Ordering,
        },
    },
    time::Duration,
};

use super::*;

fn rule() -> Rule {
    Rule {
        uri: "https://judge.example/v1/responses".into(),
        method: "POST".into(),
        action: Action::Allow,
        networks: vec![],
    }
}
#[test]
fn exact_authority_and_deny_precedence() {
    let id = Uuid::nil();
    let snapshot = Snapshot::new(id, vec![rule()]).unwrap();
    assert!(
        snapshot
            .authorize(id, "POST", "https://JUDGE.example:443/v1/responses?model=x")
            .is_ok()
    );
    for (method, uri) in [
        ("GET", rule().uri),
        ("POST", "https://sub.judge.example/v1/responses".into()),
        ("POST", "https://judge.example:444/v1/responses".into()),
        ("POST", "https://judge.example/v1/responses/".into()),
        ("POST", "http://judge.example/v1/responses".into()),
    ] {
        assert!(snapshot.authorize(id, method, &uri).is_err());
    }
    assert!(
        snapshot
            .authorize(Uuid::from_u128(1), "POST", &rule().uri)
            .is_err()
    );
    let mut deny = rule();
    deny.action = Action::Deny;
    for rules in [vec![rule(), deny.clone()], vec![deny, rule()]] {
        assert!(
            Snapshot::new(id, rules)
                .unwrap()
                .authorize(id, "POST", &rule().uri)
                .is_err()
        );
    }
    assert!(
        Snapshot::new(id, vec![])
            .unwrap()
            .authorize(id, "POST", &rule().uri)
            .is_err()
    );
}
#[test]
fn reject_ambiguous_urls_and_malformed_rules() {
    for uri in [
        "https://u:p@judge.example/x",
        "https://@judge.example/x",
        "https://judge.example./x",
        "https://*.example/x",
        "https://judge.example/a/../x",
        "https://judge.example/%2fx",
        "https://judge.example/x#fragment",
        "https://judge.example\\evil/x",
        "https://judge.example/x\n",
        "file:///etc/passwd",
    ] {
        assert!(destination(uri).is_err(), "{uri}");
    }
    for change in 0..5 {
        let mut r = rule();
        match change {
            0 => r.uri.push_str("?x=y"),
            1 => r.method = "post".into(),
            2 => r.networks = vec!["bad".into()],
            3 => r.uri = "http://judge.example/x".into(),
            _ => r.networks = vec!["0.0.0.0/0".into(); 65],
        };
        assert!(Snapshot::new(Uuid::nil(), vec![r]).is_err());
    }
}
#[test]
fn addresses_require_public_or_explicit_permission() {
    let id = Uuid::nil();
    let s = Snapshot::new(id, vec![rule()]).unwrap();
    let grant = s.authorize(id, "POST", &rule().uri).unwrap();
    for ip in ["8.8.8.8", "2606:4700:4700::1111"] {
        assert!(grant.permits_address(ip.parse().unwrap()));
    }
    for ip in [
        "0.0.0.0",
        "10.0.0.1",
        "127.0.0.1",
        "169.254.169.254",
        "100.100.100.200",
        "192.168.0.1",
        "198.18.0.1",
        "203.0.113.1",
        "224.0.0.1",
        "255.255.255.255",
        "::1",
        "::",
        "::ffff:127.0.0.1",
        "fc00::1",
        "fe80::1",
        "2001:db8::1",
        "2002:7f00:1::1",
        "3fff::1",
    ] {
        assert!(!grant.permits_address(ip.parse().unwrap()), "{ip}");
    }
    let mut r = rule();
    r.networks = vec!["10.0.0.0/24".into()];
    r.uri = "http://internal.example/api".into();
    let s = Snapshot::new(id, vec![r.clone()]).unwrap();
    let grant = s.authorize(id, "POST", &r.uri).unwrap();
    assert!(grant.permits_address("10.0.0.1".parse().unwrap()));
    assert!(!grant.permits_address("8.8.8.8".parse().unwrap()));
    assert!(!grant.permits_address("10.0.1.1".parse().unwrap()));
    r.uri = "http://10.0.0.1/api".into();
    let s = Snapshot::new(id, vec![r.clone()]).unwrap();
    assert!(
        !s.authorize(id, "POST", &r.uri)
            .unwrap()
            .permits_address("10.0.0.2".parse().unwrap())
    );
}
#[test]
fn revision_is_order_independent_and_binds_identity() {
    let mut other = rule();
    other.method = "GET".into();
    let a = Snapshot::new(Uuid::nil(), vec![rule(), other.clone()]).unwrap();
    assert_eq!(
        a.revision(),
        Snapshot::new(Uuid::nil(), vec![other.clone(), rule(), rule()])
            .unwrap()
            .revision()
    );
    assert_ne!(
        a.revision(),
        Snapshot::new(Uuid::from_u128(1), vec![rule(), other])
            .unwrap()
            .revision()
    );
}
#[tokio::test]
async fn cache_batches_negative_results_and_coalesces_concurrent_refreshes() {
    let cache = Arc::new(cache::Cache::new(Duration::from_secs(30), 4).unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let ids = vec![Uuid::nil(), Uuid::from_u128(1)];
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..10 {
        let cache = cache.clone();
        let count = count.clone();
        let ids = ids.clone();
        tasks.spawn(async move {
            cache
                .snapshots(&ids, |requested| async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(requested.len(), 2);
                    tokio::task::yield_now().await;
                    Ok(BTreeMap::from([(Uuid::nil(), vec![rule()])]))
                })
                .await
                .unwrap()
        });
    }
    while let Some(result) = tasks.join_next().await {
        let results = result.unwrap();
        assert!(
            results[&ids[1]]
                .authorize(ids[1], "POST", &rule().uri)
                .is_err()
        );
    }
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert!(
        cache
            .snapshots(&[], |_| async { panic!("empty cache query") })
            .await
            .unwrap()
            .is_empty()
    );
}
#[tokio::test]
async fn expired_policy_cannot_authorize_on_refresh_failure_and_old_snapshot_is_immutable() {
    let cache = cache::Cache::new(Duration::from_millis(30), 2).unwrap();
    let id = Uuid::nil();
    let first = cache
        .snapshots(&[id], |_| async {
            Ok(BTreeMap::from([(id, vec![rule()])]))
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(45)).await;
    for _ in 0..2 {
        assert!(
            cache
                .snapshots(&[id], |_| async { anyhow::bail!("unavailable") })
                .await
                .is_err()
        );
    }
    let next = cache
        .snapshots(&[id], |_| async { Ok(BTreeMap::new()) })
        .await
        .unwrap();
    assert_ne!(first[&id].revision(), next[&id].revision());
    assert!(first[&id].authorize(id, "POST", &rule().uri).is_ok());
    assert!(next[&id].authorize(id, "POST", &rule().uri).is_err());
}
