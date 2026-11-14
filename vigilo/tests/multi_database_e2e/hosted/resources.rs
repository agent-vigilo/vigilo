//! Explicit fixture bindings preserve request identity and record bounded evidence.
//! Different owners use different object prefixes and agent credentials. No grant
//! comes from case input, environment fallback or another caller's cached reply.
use std::{
    collections::BTreeMap,
    sync::Mutex,
    time::{
        Duration,
        SystemTime,
    },
};

use vigilo::operations::resources::*;

use super::*;

#[derive(Default)]
pub(super) struct Host {
    pub owners: BTreeMap<Uuid, String>,
    pub origin: String,
    pub calls: Mutex<Vec<Request>>,
    pub deny: std::sync::atomic::AtomicBool,
}
impl Resolver for Host {
    fn bindings(&self, request: Request) -> Resolution<'_, Bindings> {
        Box::pin(async move {
            if self.deny.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(Error::Denied);
            }
            let owner = self.owners.get(&request.tenant_id()).ok_or(Error::Denied)?;
            if request
                .caller()
                .is_some_and(|c| c.actor_id() != request.tenant_id())
            {
                return Err(Error::Denied);
            }
            let mut calls = self.calls.lock().map_err(|_| Error::Unavailable)?;
            if calls.len() >= 4096 {
                return Err(Error::Unavailable);
            }
            calls.push(request.clone());
            Bindings::new(&request, owner, "[]")
        })
    }
    fn storage_credentials(&self, request: Request) -> Resolution<'_, StorageCredentials> {
        Box::pin(async move {
            if !self.owners.contains_key(&request.tenant_id())
                || self.deny.load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(Error::Denied);
            }
            Ok(StorageCredentials {
                request,
                authentication: StorageAuthentication::S3 {
                    access_key: std::env::var("DEV_S3_ACCESS_KEY_ID")
                        .map_err(|_| Error::Unavailable)?,
                    secret_key: std::env::var("DEV_S3_SECRET_ACCESS_KEY")
                        .map_err(|_| Error::Unavailable)?,
                    session_token: None,
                },
                expires_at: SystemTime::now() + Duration::from_secs(60),
            })
        })
    }
    fn credential(&self, request: CredentialRequest) -> Resolution<'_, Credential> {
        Box::pin(async move {
            let tenant = request.resources.tenant_id();
            if !self.owners.contains_key(&tenant)
                || request.reference != "agent-api"
                || request.resources.plan_digest().is_none()
                || request.authority != CredentialAuthority::Agent
                || self.deny.load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(Error::Denied);
            }
            Ok(Credential {
                allowed_origin: self.origin.clone(),
                header: "authorization".into(),
                value: format!("Bearer {tenant}"),
                expires_at: chrono::Utc::now() + chrono::Duration::seconds(60),
                request,
            })
        })
    }
}

pub(super) async fn storage_client() -> anyhow::Result<aws_sdk_s3::Client> {
    let endpoint = std::env::var("VIGILO_BLOB_TEST_ENDPOINT")
        .or_else(|_| std::env::var("DEV_S3_PORT").map(|p| format!("http://127.0.0.1:{p}")))?;
    Ok(aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::new()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .endpoint_url(endpoint)
            .force_path_style(true)
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                std::env::var("DEV_S3_ACCESS_KEY_ID")?,
                std::env::var("DEV_S3_SECRET_ACCESS_KEY")?,
                None,
                None,
                "hosted-fixture",
            ))
            .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
            .timeout_config(
                aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                    .operation_timeout(Duration::from_secs(10))
                    .build(),
            )
            .build(),
    ))
}
