//! Test-specific host authority with core-owned bounded HTTP transport.
//!
//! A test is not an accepted run. The host receives verified immutable artifact
//! identity and a fresh test ID, then supplies policy and request-local credentials.
//! Core checks reply identity and retains cancellation, destination and I/O budgets.
use std::{
    sync::Arc,
    time::Duration,
};

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    Descriptor,
    Error,
    UserContext,
};
use crate::evaluator_http::{
    self,
    transport::{
        BlockingTransport,
        Credentials,
        Request,
        Response,
        Session,
    },
};

/// Core-constructed identity; never deserialized from evaluator input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TestRequest {
    caller: Option<UserContext>,
    test_id: Uuid,
    evaluator_id: Uuid,
    artifact_digest: String,
}
impl TestRequest {
    pub(super) fn new(caller: Option<UserContext>, e: &Descriptor) -> Self {
        Self {
            caller,
            test_id: Uuid::now_v7(),
            evaluator_id: e.id,
            artifact_digest: e.content_hash.clone(),
        }
    }
    pub fn caller(&self) -> Option<UserContext> {
        self.caller
    }
    pub fn test_id(&self) -> Uuid {
        self.test_id
    }
    pub fn evaluator_id(&self) -> Uuid {
        self.evaluator_id
    }
    pub fn artifact_digest(&self) -> &str {
        &self.artifact_digest
    }
}
/// Host-owned grant for one test only. It contains no process/environment fallback.
pub struct TestAuthority {
    request: TestRequest,
    policy: Arc<evaluator_http::Snapshot>,
    credentials: Arc<dyn Credentials>,
}
impl TestAuthority {
    pub fn new(
        request: &TestRequest,
        rules: Vec<evaluator_http::Rule>,
        credentials: Arc<dyn Credentials>,
    ) -> Result<Self, crate::operations::resources::Error> {
        Ok(Self {
            request: request.clone(),
            policy: Arc::new(
                evaluator_http::Snapshot::new(request.evaluator_id, rules)
                    .map_err(|_| crate::operations::resources::Error::Invalid)?,
            ),
            credentials,
        })
    }
}
/// Authentication and policy/secret management belong to the embedding host.
/// Authorize exact caller, test and artifact identity before returning a grant.
pub trait TestResolver: Send + Sync {
    fn resolve(
        &self,
        request: TestRequest,
    ) -> crate::operations::resources::Resolution<'_, TestAuthority>;
}
pub(super) struct Bridge {
    session: Session,
    credentials: Arc<dyn Credentials>,
    runtime: tokio::runtime::Handle,
    cancel: CancellationToken,
}
impl std::fmt::Debug for Bridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EvaluatorTestHttp")
    }
}
impl BlockingTransport for Bridge {
    fn send(&self, request: Request) -> Result<Response, String> {
        self.runtime.block_on(async{tokio::select!{biased;_=self.cancel.cancelled()=>Err("evaluator test cancelled"),r=self.session.send_resolving(request,self.credentials.as_ref())=>r}}).map_err(str::to_owned)
    }
}
pub(super) async fn resolve(
    resolver: &dyn TestResolver,
    request: TestRequest,
    limits: evaluator_http::transport::Limits,
    permits: Arc<tokio::sync::Semaphore>,
    cancel: &CancellationToken,
) -> Result<(Arc<evaluator_http::Snapshot>, Arc<dyn BlockingTransport>), Error> {
    let authority = tokio::select! {biased;_=cancel.cancelled()=>return Err(Error::Cancelled),r=tokio::time::timeout(Duration::from_secs(10),async{
     let _permit=permits.acquire().await.map_err(|_|crate::operations::resources::Error::Unavailable)?;
     resolver.resolve(request.clone()).await
    })=>r.map_err(|_|Error::Dependency(anyhow::anyhow!("test authority deadline exceeded")))?.map_err(|e|Error::Dependency(e.into()))?};
    if authority.request != request {
        return Err(Error::InvalidConfiguration(
            "test authority identity mismatch",
        ));
    }
    let session = Session::new(authority.policy.clone(), request.evaluator_id, limits)
        .map_err(Error::InvalidConfiguration)?
        .with_concurrency(permits);
    Ok((
        authority.policy,
        Arc::new(Bridge {
            session,
            credentials: authority.credentials,
            runtime: tokio::runtime::Handle::current(),
            cancel: cancel.clone(),
        }),
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{
            AtomicUsize,
            Ordering,
        },
    };

    use super::*;
    struct Empty;
    #[async_trait::async_trait]
    impl Credentials for Empty {
        async fn headers(
            &self,
            _: &str,
            _: &str,
        ) -> Result<reqwest::header::HeaderMap, &'static str> {
            Ok(Default::default())
        }
    }
    struct Host {
        mode: AtomicUsize,
        previous: Mutex<Option<TestRequest>>,
        entered: tokio::sync::Notify,
    }
    impl TestResolver for Host {
        fn resolve(
            &self,
            request: TestRequest,
        ) -> crate::operations::resources::Resolution<'_, TestAuthority> {
            Box::pin(async move {
                self.entered.notify_one();
                match self.mode.load(Ordering::SeqCst) {
                    1 => {
                        let previous = self.previous.lock().unwrap().clone().unwrap();
                        TestAuthority::new(&previous, vec![], Arc::new(Empty))
                    }
                    2 => std::future::pending().await,
                    3 => Err(crate::operations::resources::Error::Denied),
                    _ => {
                        *self.previous.lock().unwrap() = Some(request.clone());
                        TestAuthority::new(&request, vec![], Arc::new(Empty))
                    }
                }
            })
        }
    }
    fn request() -> TestRequest {
        TestRequest {
            caller: Some(UserContext::new(Uuid::from_u128(1), Uuid::from_u128(2)).unwrap()),
            test_id: Uuid::now_v7(),
            evaluator_id: Uuid::from_u128(3),
            artifact_digest: "a".repeat(64),
        }
    }
    #[tokio::test]
    async fn test_authority_rejects_replay_denial_and_bounds_pending_resolution() {
        let host = Arc::new(Host {
            mode: AtomicUsize::new(0),
            previous: Mutex::new(None),
            entered: tokio::sync::Notify::new(),
        });
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let token = CancellationToken::new();
        let (_, bridge) = resolve(
            host.as_ref(),
            request(),
            Default::default(),
            permits.clone(),
            &token,
        )
        .await
        .unwrap();
        assert_eq!(format!("{bridge:?}"), "EvaluatorTestHttp");
        for mode in [1, 3] {
            host.mode.store(mode, Ordering::SeqCst);
            assert!(
                resolve(
                    host.as_ref(),
                    request(),
                    Default::default(),
                    permits.clone(),
                    &token
                )
                .await
                .is_err()
            );
            assert!(permits.try_acquire().is_ok());
        }
        host.mode.store(2, Ordering::SeqCst);
        // This proves core's actual deadline, without a network or platform shell tool.
        assert!(
            resolve(
                host.as_ref(),
                request(),
                Default::default(),
                permits.clone(),
                &token
            )
            .await
            .is_err()
        );
        assert!(permits.try_acquire().is_ok());
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(matches!(
            resolve(
                host.as_ref(),
                request(),
                Default::default(),
                permits.clone(),
                &cancel
            )
            .await,
            Err(Error::Cancelled)
        ));
    }
}
