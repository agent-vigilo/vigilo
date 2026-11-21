//! Explicit harness selection and completion capabilities for an invocation profile.
//! The run profile binds a single target identity to this configuration.

use anyhow::{
    Result,
    ensure,
};
use reqwest::Url;
use serde::{
    Deserialize,
    Serialize,
};

/// The exact primary target requested by the run and reported by the harness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentIdentity {
    pub provider: String,
    pub name: String,
    pub version: String,
}

impl AgentIdentity {
    /// Checks that each part of the exact target identity has a usable value.
    pub fn validate(&self) -> Result<()> {
        ensure!(
            [&self.provider, &self.name, &self.version]
                .iter()
                .all(|part| !part.trim().is_empty() && !part.chars().any(char::is_control)),
            "invalid target agent identity"
        );
        Ok(())
    }
}

/// The target and harness binding within a run profile.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub agent: AgentIdentity,
    pub harness: Harness,
}

impl Profile {
    /// Validates endpoint shape, capability declarations and host callback availability.
    /// Deployment network and credential policy is enforced by the caller.
    pub fn validate(&self, callback_available: bool) -> Result<()> {
        self.agent.validate()?;
        self.harness.validate(callback_available)
    }
}

/// Explicitly chooses who drives the interaction with the target.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Harness {
    DirectProbe {
        http: HttpEndpoint,
        completion: Completion,
        capabilities: Option<Capabilities>,
        #[serde(default)]
        require_executed_agent: bool,
        executed_agent_pointer: Option<crate::response::JsonPointer>,
        #[serde(default)]
        forward_case_context: bool,
    },
    DelegatedTrial {
        http: HttpEndpoint,
        completion: Completion,
        capabilities: Option<Capabilities>,
        #[serde(default)]
        forward_case_context: bool,
    },
}

impl Harness {
    /// Endpoint and limits for the initial invocation request.
    pub fn http(&self) -> &HttpEndpoint {
        match self {
            Self::DirectProbe { http, .. } | Self::DelegatedTrial { http, .. } => http,
        }
    }

    /// Completion delivery selected by the profile.
    pub fn completion(&self) -> &Completion {
        match self {
            Self::DirectProbe { completion, .. } | Self::DelegatedTrial { completion, .. } => {
                completion
            }
        }
    }

    /// Mutable endpoint configuration for profile builders.
    pub fn http_mut(&mut self) -> &mut HttpEndpoint {
        match self {
            Self::DirectProbe { http, .. } | Self::DelegatedTrial { http, .. } => http,
        }
    }

    /// Whether the final outcome comes back in the initial response.
    pub fn is_inline(&self) -> bool {
        match self {
            Self::DirectProbe { completion, .. } | Self::DelegatedTrial { completion, .. } => {
                matches!(completion, Completion::Inline {})
            }
        }
    }

    /// Checks the harness destinations and the selected completion requirements.
    pub fn validate(&self, callback_available: bool) -> Result<()> {
        let (http, completion, capabilities) = match self {
            Self::DirectProbe {
                http,
                completion,
                capabilities,
                ..
            }
            | Self::DelegatedTrial {
                http,
                completion,
                capabilities,
                ..
            } => (http, completion, capabilities),
        };
        http.validate()?;
        match completion {
            Completion::Inline {} => {
                ensure!(
                    capabilities.is_none(),
                    "inline completion has no deferred capabilities"
                );
            }
            Completion::Polling {
                status_url,
                deadline_secs,
            } => {
                validate_status_url(status_url)?;
                ensure!(
                    (1..=86_400).contains(deadline_secs),
                    "polling deadline_secs must be between 1 and 86400"
                );
            }
            Completion::Callback { deadline_secs } => {
                ensure!(callback_available, "callback ingress is unavailable");
                ensure!(
                    (1..=86_400).contains(deadline_secs),
                    "callback deadline_secs must be between 1 and 86400"
                );
            }
        }
        if !matches!(completion, Completion::Inline {}) {
            let capabilities = capabilities.as_ref().ok_or_else(|| {
                anyhow::anyhow!("deferred completion requires capability declarations")
            })?;
            if let Some(cancel) = &capabilities.cancellation {
                cancel.validate()?;
            }
        }
        Ok(())
    }
}

/// How the final outcome arrives, independently of the harness kind.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Completion {
    Inline {},
    Polling {
        status_url: String,
        #[serde(default = "default_deadline")]
        deadline_secs: u32,
    },
    Callback {
        #[serde(default = "default_deadline")]
        deadline_secs: u32,
    },
}

fn default_deadline() -> u32 {
    600
}

/// HTTP destination and optional reference to operator-provided credentials.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpEndpoint {
    pub url: String,
    #[serde(default = "post")]
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential: Option<String>,
    #[serde(default)]
    pub headers: std::collections::BTreeMap<String, String>,
    pub timeout_secs: Option<u32>,
    pub max_request_bytes: Option<u64>,
    pub max_response_bytes: Option<u64>,
    pub max_retry_after_secs: Option<u32>,
}

fn post() -> String {
    "POST".into()
}

impl HttpEndpoint {
    fn validate(&self) -> Result<()> {
        endpoint_url(&self.url)?;
        reqwest::Method::from_bytes(self.method.as_bytes())?;
        ensure!(
            self.credential
                .as_ref()
                .is_none_or(|name| !name.trim().is_empty()),
            "empty credential reference"
        );
        Ok(())
    }
}

/// Capabilities declared by a deferred integration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capabilities {
    pub idempotent_submission: bool,
    pub cancellation: Option<CancellationEndpoint>,
}

/// Fixed POST destination for optional best-effort remote cancellation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancellationEndpoint {
    pub url: String,
    pub credential: Option<String>,
}

impl CancellationEndpoint {
    fn validate(&self) -> Result<()> {
        endpoint_url(&self.url)?;
        ensure!(
            self.credential
                .as_ref()
                .is_none_or(|name| !name.trim().is_empty()),
            "empty credential reference"
        );
        Ok(())
    }
}

pub(super) fn endpoint_url(value: &str) -> Result<Url> {
    let url = Url::parse(value).map_err(|_| anyhow::anyhow!("invalid endpoint URL"))?;
    ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.has_host()
            && url.username().is_empty()
            && url.password().is_none()
            && url.fragment().is_none(),
        "endpoint must be an absolute HTTP URL without user info or fragment"
    );
    Ok(url)
}

/// Render the admitted status template with the provider reference as data.
pub fn poll_url(template: &str, reference: &str) -> Result<Url> {
    validate_status_url(template)?;
    ensure!(
        (1..=256).contains(&reference.len()),
        "invalid provider reference"
    );
    let url = endpoint_url(&template.replace("{provider_reference}", "vigilo-provider-reference"))?;
    let path_slot = url
        .path_segments()
        .is_some_and(|mut parts| parts.any(|part| part == "vigilo-provider-reference"));
    ensure!(
        !path_slot || !matches!(reference, "." | ".."),
        "dot-segment reference requires a query slot"
    );
    let mut encoded = String::new();
    use std::fmt::Write;
    for byte in reference.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(byte.into());
        } else {
            write!(encoded, "%{byte:02X}")?;
        }
    }
    endpoint_url(&template.replace("{provider_reference}", &encoded))
}

fn validate_status_url(template: &str) -> Result<()> {
    const SLOT: &str = "{provider_reference}";
    const MARKER: &str = "vigilo-provider-reference";
    ensure!(
        template.matches(SLOT).count() == 1 && !template.contains(MARKER),
        "status URL requires exactly one provider reference placeholder"
    );
    let url = endpoint_url(&template.replace(SLOT, MARKER))?;
    let in_path = url
        .path_segments()
        .is_some_and(|mut segments| segments.any(|segment| segment == MARKER));
    let in_query = url.query().is_some_and(|query| {
        query.split('&').any(|pair| {
            pair.split_once('=')
                .is_some_and(|(_, value)| value == MARKER)
        })
    });
    ensure!(
        in_path || in_query,
        "provider reference must occupy a path segment or query value"
    );
    Ok(())
}
