//! Service-free host boundary coverage; no request opens a socket or database.
use std::sync::{
    Mutex,
    atomic::{
        AtomicUsize,
        Ordering,
    },
};

use uuid::Uuid;

use super::*;
use crate::{
    agent_client::credentials::{
        Authority,
        Execution,
        Provider,
    },
    operations::resources::{
        Bindings,
        Credential,
        CredentialRequest,
        Error,
        Resolution,
        StorageAuthentication,
        StorageCredentials,
    },
};

const OWNER: &str = "provider='s3'\nalias='cases'\nbucket='cases'\nprefix='isolated'\nregion='us-east-1'\nendpoint='https://objects.example'\n";
struct Host {
    mode: AtomicUsize,
    generation: AtomicUsize,
    bindings: AtomicUsize,
    credentials: AtomicUsize,
    override_request: Mutex<Option<Request>>,
    entered: tokio::sync::Notify,
}
impl Default for Host {
    fn default() -> Self {
        Self {
            mode: AtomicUsize::new(0),
            generation: AtomicUsize::new(1),
            bindings: AtomicUsize::new(0),
            credentials: AtomicUsize::new(0),
            override_request: Mutex::new(None),
            entered: tokio::sync::Notify::new(),
        }
    }
}
impl Resolver for Host {
    fn bindings(&self, request: Request) -> Resolution<'_, Bindings> {
        Box::pin(async move {
            self.bindings.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            if self.mode.load(Ordering::SeqCst) == 7 {
                return std::future::pending().await;
            }
            if self.mode.load(Ordering::SeqCst) == 1 {
                return Err(Error::Denied);
            }
            let request = self
                .override_request
                .lock()
                .unwrap()
                .clone()
                .unwrap_or(request);
            Bindings::new(&request, OWNER, "[]")
        })
    }
    fn credential(&self, mut request: CredentialRequest) -> Resolution<'_, Credential> {
        Box::pin(async move {
            self.credentials.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            let mode = self.mode.load(Ordering::SeqCst);
            if mode == 1 {
                return Err(Error::Denied);
            }
            if mode == 2 {
                return Err(Error::Unavailable);
            }
            if mode == 7 {
                return std::future::pending().await;
            }
            if mode == 8 {
                request.authority = Authority::Evaluator {
                    evaluator_id: Uuid::from_u128(98),
                    policy_revision: "b".repeat(64),
                };
            }
            let value = format!(
                "fixture-{}-{}",
                request.resources.tenant_id(),
                self.generation.load(Ordering::SeqCst)
            );
            if let Some(other) = self.override_request.lock().unwrap().clone() {
                request.resources = Arc::new(other);
            }
            if mode == 4 {
                request.attempt_id = Uuid::now_v7();
            }
            let origin = if mode == 12 {
                format!("https://{}", "a".repeat(8192))
            } else if mode == 5 {
                "https://wrong.example".into()
            } else {
                reqwest::Url::parse(&request.destination)
                    .unwrap()
                    .origin()
                    .ascii_serialization()
            };
            Ok(Credential {
                request,
                allowed_origin: origin,
                header: if mode == 6 {
                    "host".into()
                } else if mode == 10 {
                    format!("x-{}", "a".repeat(65_536))
                } else {
                    "authorization".into()
                },
                value: if mode == 11 {
                    "x".repeat(16_385)
                } else if mode == 13 {
                    "invalid\nvalue".into()
                } else {
                    value
                },
                expires_at: chrono::Utc::now()
                    + chrono::Duration::seconds(if mode == 3 { -1 } else { 60 }),
            })
        })
    }
    fn storage_credentials(&self, request: Request) -> Resolution<'_, StorageCredentials> {
        Box::pin(async move {
            if self.mode.load(Ordering::SeqCst) == 7 {
                return std::future::pending().await;
            }
            if self.mode.load(Ordering::SeqCst) == 1 {
                return Err(Error::Denied);
            }
            Ok(StorageCredentials {
                request: self
                    .override_request
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or(request),
                authentication: StorageAuthentication::S3 {
                    access_key: "fixture-access".into(),
                    secret_key: if self.mode.load(Ordering::SeqCst) == 9 {
                        "opaque signing material with spaces".into()
                    } else if self.mode.load(Ordering::SeqCst) == 10 {
                        "x".repeat(16_384)
                    } else if self.mode.load(Ordering::SeqCst) == 11 {
                        "x".repeat(16_385)
                    } else {
                        format!("fixture-secret-{}", self.generation.load(Ordering::SeqCst))
                    },
                    session_token: if self.mode.load(Ordering::SeqCst) == 12 {
                        Some(String::new())
                    } else {
                        None
                    },
                },
                expires_at: if self.mode.load(Ordering::SeqCst) == 3 {
                    std::time::SystemTime::now() - Duration::from_secs(1)
                } else {
                    std::time::SystemTime::now() + Duration::from_secs(60)
                },
            })
        })
    }
}
fn request(owner: u128, root: char) -> Request {
    let config: OwnerConfig = toml::from_str(OWNER).unwrap();
    Request::accepted(
        Uuid::from_u128(owner),
        Uuid::from_u128(42),
        &root.to_string().repeat(64),
        config.binding_hash().unwrap(),
    )
    .unwrap()
}
fn context(host: Arc<Host>) -> Context {
    Context::new(
        super::super::database::Config::new(
            "postgresql://localhost/not-opened".into(),
            1,
            Duration::from_secs(1),
            Default::default(),
            super::super::database::PlacementConfig::new(
                "primary".into(),
                "primary".into(),
                "single-default".into(),
            )
            .unwrap(),
        ),
        None,
        Default::default(),
        super::super::output::OutputFormat::Json,
    )
    .with_resource_resolver(host)
}
fn execution(request: Request) -> Execution {
    Execution {
        run_id: request.run_id(),
        execution_id: Uuid::from_u128(10),
        attempt_id: Uuid::from_u128(11),
        resources: Some(Arc::new(request)),
    }
}

#[tokio::test]
async fn bindings_are_exact_for_preparation_and_accepted_work_without_fallback() {
    let host = Arc::new(Host::default());
    let context = context(host.clone());
    let accepted = request(1, 'a');
    let preparation = Request::preparation(
        crate::db::scope::UserContext::new(Uuid::from_u128(1), Uuid::from_u128(2)).unwrap(),
        accepted.run_id(),
        Uuid::from_u128(3),
        accepted.binding_hash().into(),
    );
    assert!(preparation.plan_digest().is_none());
    assert!(preparation.caller().is_some());
    let (prepared, owner) = context
        .preparation_resources(preparation.clone(), None, &CancellationToken::new())
        .await
        .unwrap();
    assert!(prepared.accepted_resources().is_none());
    assert_eq!(owner.binding_hash().unwrap(), preparation.binding_hash());
    let (bound, _) = context
        .execution_resources(
            accepted.clone(),
            &FormatLimits::default(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(bound.accepted_resources().unwrap().as_ref(), &accepted);
    for wrong in [request(2, 'a'), request(1, 'b'), preparation.clone()] {
        *host.override_request.lock().unwrap() = Some(wrong);
        assert!(
            context
                .injected_resources(&accepted, &CancellationToken::new())
                .await
                .is_err()
        );
    }
    *host.override_request.lock().unwrap() = None;
    host.mode.store(1, Ordering::SeqCst);
    assert!(
        context
            .execution_resources(
                accepted.clone(),
                &FormatLimits::default(),
                &CancellationToken::new()
            )
            .await
            .is_err()
    );
    assert!(
        context
            .preparation_resources(preparation.clone(), Some(&owner), &CancellationToken::new())
            .await
            .is_err()
    );
    host.mode.store(0, Ordering::SeqCst);
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let before = host.bindings.load(Ordering::SeqCst);
    assert!(
        context
            .injected_resources(&accepted, &cancelled)
            .await
            .is_err()
    );
    assert_eq!(host.bindings.load(Ordering::SeqCst), before);
    assert!(
        context
            .injected_resources(&request(0, 'a'), &CancellationToken::new())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn host_credentials_bind_owner_root_invocation_and_authority_and_rotate() {
    let host = Arc::new(Host::default());
    let provider = Provider::default().injected(host.clone());
    let headers = std::collections::BTreeMap::new();
    let mut values = Vec::new();
    for owner in [1, 2, 1] {
        let value = provider
            .resolve(
                "shared",
                "https://agent.example/run",
                &headers,
                execution(request(owner, 'a')),
            )
            .await
            .unwrap();
        assert!(value.value.is_sensitive());
        values.push(value.value.to_str().unwrap().to_owned());
    }
    assert_eq!(values[0], values[2]);
    assert_ne!(values[0], values[1]);
    host.generation.store(2, Ordering::SeqCst);
    let next = provider
        .resolve(
            "shared",
            "https://agent.example/run",
            &headers,
            execution(request(1, 'a')),
        )
        .await
        .unwrap();
    assert_ne!(values[0], next.value.to_str().unwrap());
    let judge = Authority::Evaluator {
        evaluator_id: Uuid::from_u128(99),
        policy_revision: "a".repeat(64),
    };
    provider
        .resolve_authorized(
            "shared",
            "https://judge.example/score",
            &headers,
            execution(request(2, 'b')),
            judge,
        )
        .await
        .unwrap();
    for mode in [1, 2, 3, 4, 5, 6, 8, 10, 11, 12, 13] {
        host.mode.store(mode, Ordering::SeqCst);
        let error = provider
            .resolve(
                "shared",
                "https://agent.example/run",
                &headers,
                execution(request(1, 'a')),
            )
            .await
            .err()
            .unwrap();
        assert!(!error.to_string().contains("fixture-secret"));
        assert_eq!(error.retryable, mode == 2);
    }
    host.mode.store(0, Ordering::SeqCst);
    for wrong in [request(2, 'a'), request(1, 'b')] {
        *host.override_request.lock().unwrap() = Some(wrong);
        assert!(
            provider
                .resolve(
                    "shared",
                    "https://agent.example/run",
                    &headers,
                    execution(request(1, 'a'))
                )
                .await
                .is_err()
        );
    }
    *host.override_request.lock().unwrap() = None;
    let mut no_work = execution(request(1, 'a'));
    no_work.resources = None;
    let before = host.credentials.load(Ordering::SeqCst);
    assert!(
        provider
            .resolve("shared", "https://agent.example/run", &headers, no_work)
            .await
            .is_err()
    );
    let mut wrong_run = execution(request(1, 'a'));
    wrong_run.run_id = Uuid::now_v7();
    assert!(
        provider
            .resolve("shared", "https://agent.example/run", &headers, wrong_run)
            .await
            .is_err()
    );
    let collision =
        std::collections::BTreeMap::from([("Authorization".into(), "profile-value".into())]);
    assert!(
        provider
            .resolve(
                "shared",
                "https://agent.example/run",
                &collision,
                execution(request(1, 'a'))
            )
            .await
            .is_err()
    );
    assert!(host.credentials.load(Ordering::SeqCst) >= before);
}

#[tokio::test]
async fn storage_renewal_is_request_bound_expiring_redacted_and_has_no_fallback() {
    let host = Arc::new(Host::default());
    let provider = StorageProvider {
        resolver: host.clone(),
        request: request(1, 'a'),
        permits: Arc::new(tokio::sync::Semaphore::new(1)),
    };
    let first = provider.provide_credentials().await.unwrap();
    assert_eq!(format!("{provider:?}"), "HostStorageCredentials");
    assert!(provider.fallback_on_interrupt().is_none());
    host.generation.store(2, Ordering::SeqCst);
    assert_ne!(
        first.secret_access_key(),
        provider
            .provide_credentials()
            .await
            .unwrap()
            .secret_access_key()
    );
    host.mode.store(9, Ordering::SeqCst);
    assert_eq!(
        provider
            .provide_credentials()
            .await
            .unwrap()
            .secret_access_key(),
        "opaque signing material with spaces"
    );
    host.mode.store(10, Ordering::SeqCst);
    assert_eq!(
        provider
            .provide_credentials()
            .await
            .unwrap()
            .secret_access_key()
            .len(),
        16_384
    );
    for mode in [1, 3, 11, 12] {
        host.mode.store(mode, Ordering::SeqCst);
        assert!(provider.provide_credentials().await.is_err());
    }
    host.mode.store(0, Ordering::SeqCst);
    *host.override_request.lock().unwrap() = Some(request(2, 'a'));
    assert!(provider.provide_credentials().await.is_err());
}

#[test]
fn injected_configuration_rejects_ambient_bindings_and_unknown_fields() {
    let request = request(1, 'a');
    assert!(Bindings::new(&request, OWNER, "[]").is_ok());
    let routed = Bindings::new(&request, OWNER, "[]")
        .unwrap()
        .with_read_endpoint("https://private.objects.example".into())
        .unwrap();
    assert_eq!(routed.owner.binding_hash().unwrap(), request.binding_hash());
    for route in [
        "https://user:secret@objects.example",
        "https://objects.example?secret=x",
        "http://objects.example",
        "file:///tmp/store",
    ] {
        assert!(
            Bindings::new(&request, OWNER, "[]")
                .unwrap()
                .with_read_endpoint(route.into())
                .is_err()
        );
    }

    for extra in [
        "read_endpoint_variable='UNUSED'\n",
        "[credential_variables]\naccess_key='UNUSED'\nsecret_key='UNUSED'\n",
        "unexpected=true\n",
        "read_endpoint_override='https://private.objects.example'\n",
        "credentials_provider='not-a-config-field'\n",
    ] {
        assert!(Bindings::new(&request, &format!("{OWNER}{extra}"), "[]").is_err());
    }
    assert!(
        Bindings::new(
            &request,
            &OWNER.replace("prefix='isolated'", "prefix='other'"),
            "[]"
        )
        .is_err()
    );
    assert!(Bindings::new(&request, OWNER, "{}").is_err());
    assert!(
        Bindings::new(&request, OWNER, "[]")
            .unwrap()
            .with_read_endpoint("x".repeat(64 * 1024 + 1))
            .is_err()
    );
    assert!(
        Request::accepted(
            Uuid::from_u128(1),
            Uuid::nil(),
            &"a".repeat(64),
            request.binding_hash().into()
        )
        .is_err()
    );
    assert!(
        Request::accepted(
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            "not-a-digest",
            request.binding_hash().into()
        )
        .is_err()
    );
}

#[tokio::test]
async fn injected_providers_share_process_admission_and_release_on_cancellation() {
    let host = Arc::new(Host::default());
    host.mode.store(7, Ordering::SeqCst);
    let base = Provider::default();
    let mut tasks = tokio::task::JoinSet::new();
    for owner in 1..=65 {
        let provider = base.injected(host.clone());
        tasks.spawn(async move {
            provider
                .resolve(
                    "shared",
                    "https://agent.example/run",
                    &Default::default(),
                    execution(request(owner, 'a')),
                )
                .await
        });
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let entered = host.entered.notified();
            if host.credentials.load(Ordering::SeqCst) == 64 {
                break;
            }
            entered.await;
        }
    })
    .await
    .unwrap();
    for _ in 0..65 {
        tokio::task::yield_now().await;
    }
    assert_eq!(host.credentials.load(Ordering::SeqCst), 64);
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    host.mode.store(0, Ordering::SeqCst);
    base.injected(host.clone())
        .resolve(
            "shared",
            "https://agent.example/run",
            &Default::default(),
            execution(request(1, 'a')),
        )
        .await
        .unwrap();
    assert_eq!(host.credentials.load(Ordering::SeqCst), 65);
}

#[tokio::test]
async fn resolver_timeouts_are_bounded_and_release_admission() {
    let host = Arc::new(Host::default());
    host.mode.store(7, Ordering::SeqCst);
    let provider = Provider::default().injected(host.clone());
    let context = context(host.clone());
    let resources = request(1, 'a');
    let cancel = CancellationToken::new();
    let storage = StorageProvider {
        resolver: host.clone(),
        request: resources.clone(),
        permits: Arc::new(tokio::sync::Semaphore::new(1)),
    };
    let headers = Default::default();
    let (credential, binding, signing) = tokio::join!(
        provider.resolve(
            "shared",
            "https://agent.example/run",
            &headers,
            execution(resources.clone())
        ),
        context.injected_resources(&resources, &cancel),
        storage.provide_credentials(),
    );
    assert!(credential.err().unwrap().retryable);
    assert!(binding.is_err());
    assert!(signing.is_err());
    host.mode.store(0, Ordering::SeqCst);
    provider
        .resolve(
            "shared",
            "https://agent.example/run",
            &headers,
            execution(resources.clone()),
        )
        .await
        .unwrap();
    assert!(
        context
            .injected_resources(&resources, &cancel)
            .await
            .is_ok()
    );
    assert!(storage.provide_credentials().await.is_ok());
}

#[tokio::test]
async fn resource_binding_admission_is_process_bounded_and_released_on_abort() {
    let host = Arc::new(Host::default());
    host.mode.store(7, Ordering::SeqCst);
    let context = Arc::new(context(host.clone()));
    let config = &context.0.dbr.config;
    let other = Arc::new(
        context
            .authority_view(
                Arc::new(super::super::database::DatabaseRouter::new(config.clone())),
                30,
            )
            .unwrap(),
    );
    let mut tasks = tokio::task::JoinSet::new();
    for owner in 1..=65 {
        let context = if owner % 2 == 0 {
            other.clone()
        } else {
            context.clone()
        };
        tasks.spawn(async move {
            context
                .injected_resources(&request(owner, 'a'), &CancellationToken::new())
                .await
                .map(|_| ())
        });
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let entered = host.entered.notified();
            if host.bindings.load(Ordering::SeqCst) == 64 {
                break;
            }
            entered.await;
        }
    })
    .await
    .unwrap();
    for _ in 0..65 {
        tokio::task::yield_now().await;
    }
    assert_eq!(host.bindings.load(Ordering::SeqCst), 64);
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    host.mode.store(0, Ordering::SeqCst);
    assert!(
        context
            .injected_resources(&request(1, 'a'), &CancellationToken::new())
            .await
            .is_ok()
    );
    assert_eq!(host.bindings.load(Ordering::SeqCst), 65);
}

#[tokio::test]
async fn injected_credentials_require_https_unless_loopback_is_explicit() {
    let host = Arc::new(Host::default());
    for allow in [false, true] {
        let base = Provider::default();
        let base = if allow {
            base.with_http_loopback(true)
        } else {
            base
        };
        let provider = base.injected(host.clone());
        for (uri, loopback) in [
            ("http://127.0.0.1:8080/invoke", true),
            ("http://[::1]:8080/invoke", true),
            ("http://localhost/invoke", false),
            ("http://192.0.2.1/invoke", false),
            ("http://127.0.0.1.evil/invoke", false),
            ("https://agent.example/invoke", false),
        ] {
            let result = provider
                .resolve(
                    "shared",
                    uri,
                    &Default::default(),
                    execution(request(1, 'a')),
                )
                .await;
            assert_eq!(
                result.is_ok(),
                uri.starts_with("https:") || allow && loopback,
                "{uri}, explicit={allow}"
            );
        }
    }
    assert_eq!(
        host.credentials.load(Ordering::SeqCst),
        4,
        "ineligible origins must not resolve credentials"
    );
}
