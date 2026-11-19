//! Frozen standalone polling routes and bounded native HTTP exchanges.
//! Requests carry credential references until delivery and never follow redirects.

use std::{
    collections::BTreeMap,
    time::Duration,
};

use anyhow::{
    Result,
    ensure,
};
use reqwest::{
    Method,
    StatusCode,
};
use serde::{
    Deserialize,
    Serialize,
};
use vigilo::invocation::profile::{
    CancellationEndpoint,
    Completion,
    Harness,
    HttpEndpoint,
};

use super::{
    capture,
    credentials,
    diagnostics,
    failure::{
        Failure,
        Kind,
    },
};
use crate::contracts::run::RunProfile;

/// Transport settings copied once into each accepted execution shard snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Route {
    pub http: HttpEndpoint,
    pub status_url: String,
    pub cancellation: Option<CancellationEndpoint>,
    pub deadline_secs: u32,
    pub submit_timeout_secs: u32,
    pub forward_context: bool,
    pub require_agent: bool,
    pub deduplicates: bool,
}

impl Route {
    pub(crate) fn from_profile(profile: &RunProfile) -> Result<Option<Self>> {
        let (completion, capabilities, forward_context, require_agent) = match &profile.harness {
            Harness::DirectProbe {
                completion,
                capabilities,
                forward_case_context,
                require_executed_agent,
                ..
            } => (
                completion,
                capabilities,
                *forward_case_context,
                *require_executed_agent,
            ),
            Harness::DelegatedTrial {
                completion,
                capabilities,
                forward_case_context,
                ..
            } => (completion, capabilities, *forward_case_context, true),
        };
        let Completion::Polling {
            status_url,
            deadline_secs,
        } = completion
        else {
            return Ok(None);
        };
        let capabilities = capabilities
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missing polling capabilities"))?;
        let http = profile.harness.http().clone();
        if let Some(cancel) = &capabilities.cancellation
            && let Some(reference) = &cancel.credential
        {
            credentials::validate_reference(reference)?;
        }
        Ok(Some(Self {
            submit_timeout_secs: http
                .timeout_secs
                .unwrap_or(profile.defaults.request_timeout_secs),
            http,
            status_url: status_url.clone(),
            cancellation: capabilities.cancellation.clone(),
            deadline_secs: *deadline_secs,
            forward_context,
            require_agent,
            deduplicates: capabilities.idempotent_submission,
        }))
    }
}

pub(crate) struct Message<'a> {
    pub method: Method,
    pub url: &'a str,
    pub headers: &'a BTreeMap<String, String>,
    pub credential: Option<&'a str>,
    pub body: Option<Vec<u8>>,
    pub timeout: Duration,
    pub max_bytes: u64,
    pub expected_status: StatusCode,
}

/// Executes one bounded exchange including credential lookup and body capture.
pub(crate) async fn send(
    provider: &credentials::Provider,
    scope: credentials::Execution,
    message: Message<'_>,
) -> Result<Vec<u8>, Failure> {
    let deadline = tokio::time::Instant::now() + message.timeout;
    tokio::time::timeout_at(deadline, async {
        let mut request = provider
            .client()
            .await?
            .request(message.method, message.url);
        if message
            .headers
            .keys()
            .any(|key| key.eq_ignore_ascii_case(diagnostics::REQUEST_ID_HEADER))
        {
            return Err(Failure::terminal(
                Kind::Configuration,
                "profile overrides the host correlation header",
            ));
        }
        let attempt_id = scope.attempt_id;
        if let Some(reference) = message.credential {
            let resolved = provider
                .resolve(reference, message.url, message.headers, scope)
                .await?;
            resolved.check_validity()?;
            if resolved.name.as_str() == diagnostics::REQUEST_ID_HEADER {
                return Err(Failure::terminal(
                    Kind::Configuration,
                    "credential overrides the host correlation header",
                ));
            }
            request = request.header(resolved.name, resolved.value);
        }
        for (name, value) in message.headers {
            request = request.header(name, value);
        }
        if let Some(body) = message.body {
            request = request
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body);
        }
        let response = request
            .header(diagnostics::REQUEST_ID_HEADER, attempt_id.to_string())
            .send()
            .await
            .map_err(|error| Failure::transport(error, "deferred HTTP request failed", None))?;
        let status = response.status();
        if status != message.expected_status {
            return Err(Failure::http(
                status.as_u16(),
                response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok()),
                chrono::Utc::now(),
                super::failure::DEFAULT_MAX_RETRY_AFTER_SECS,
            ));
        }
        let body = capture::collect(response, message.max_bytes.max(1), deadline).await?;
        if message.max_bytes == 0 && !body.bytes().is_empty() {
            return Err(Failure::terminal(
                Kind::InvalidResponse,
                "cancellation acknowledgement must be empty",
            ));
        }
        Ok(body.bytes().to_vec())
    })
    .await
    .unwrap_or_else(|_| {
        let mut failure = Failure::terminal(Kind::Timeout, "deferred HTTP deadline exceeded");
        failure.retryable = true;
        Err(failure)
    })
}

pub(crate) fn submit_method(route: &Route) -> Result<Method> {
    ensure!(
        route.submit_timeout_secs > 0,
        "submission timeout must be positive"
    );
    Ok(route.http.method.parse()?)
}
