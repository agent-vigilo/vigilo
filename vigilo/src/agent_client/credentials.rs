//! Process-local credential providers for agent HTTP requests.
//!
//! Profiles carry names only. Trusted operator configuration binds names to local
//! sources or a versioned resolver service. Sources are read per invocation, never
//! persisted or cached. The service owns execution authorization; core enforces
//! transport, destination, header, size, concurrency, and lifetime boundaries.
//! Secrets have no serialization or Debug implementation. This is not protection
//! against a compromised worker or an authorized endpoint reflecting credentials.

use std::{
    collections::BTreeMap,
    path::{
        Path,
        PathBuf,
    },
    time::Duration,
};

use reqwest::{
    Url,
    header::{
        HeaderName,
        HeaderValue,
    },
};
use serde::{
    Deserialize,
    Serialize,
};
use tokio::{
    io::AsyncReadExt,
    sync::{
        OnceCell,
        Semaphore,
    },
};
use uuid::Uuid;

use super::failure::{
    Failure,
    Kind,
};

const MAX_SECRET_BYTES: usize = 16_384;
const MAX_CONFIG_BYTES: usize = 1_048_576;
const MAX_RESPONSE_BYTES: usize = 65_536;
const RESOLUTION_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    pub(crate) version: u32,
    #[serde(default)]
    pub(crate) allow_http_loopback: bool,
    pub(crate) provider: ProviderConfig,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ProviderConfig {
    Local {
        bindings: BTreeMap<String, Binding>,
    },
    Service {
        endpoint: String,
        token: Source,
        grants_directory: PathBuf,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Binding {
    pub(crate) source: Source,
    pub(crate) allowed_origin: String,
    pub(crate) header: String,
    #[serde(default)]
    pub(crate) prefix: String,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Source {
    Env { name: String },
    File { path: PathBuf },
}

/// Resolution context comes from allocated runtime IDs, not profile metadata.
#[derive(Clone, Copy)]
pub(crate) struct Execution {
    pub(crate) run_id: Uuid,
    pub(crate) execution_id: Uuid,
    pub(crate) attempt_id: Uuid,
}

/// Shared bounded provider. No credential material survives a resolution here.
pub(crate) struct Provider {
    config: Option<Config>,
    client: OnceCell<reqwest::Client>,
    permits: Semaphore,
}

impl Default for Provider {
    fn default() -> Self {
        Self {
            config: None,
            client: OnceCell::new(),
            permits: Semaphore::new(64),
        }
    }
}

pub(crate) struct Resolved {
    pub(crate) name: HeaderName,
    pub(crate) value: HeaderValue,
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl Resolved {
    pub(crate) fn check_validity(&self) -> Result<(), Failure> {
        if self
            .expires_at
            .is_some_and(|expiry| expiry <= chrono::Utc::now())
        {
            return Err(configuration("credential expired before invocation"));
        }
        Ok(())
    }
}

fn configuration(message: &'static str) -> Failure {
    Failure::terminal(Kind::Configuration, message)
}

fn unavailable() -> Failure {
    Failure::credential_unavailable()
}

/// References are bounded names, never paths or destinations.
pub(crate) fn validate_reference(reference: &str) -> Result<(), Failure> {
    if reference.is_empty()
        || reference.len() > 128
        || !reference
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
    {
        return Err(configuration("invalid agent credential reference"));
    }
    Ok(())
}

fn url(value: &str, allow_loopback: bool) -> Result<Url, Failure> {
    if value.len() > 8192 {
        return Err(configuration("credential destination exceeds its limit"));
    }
    let url = Url::parse(value).map_err(|_| configuration("invalid credential destination"))?;
    let loopback = url.host_str().is_some_and(|host| {
        host.trim_matches(['[', ']'])
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
    });
    if url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || !(url.scheme() == "https" || (allow_loopback && loopback && url.scheme() == "http"))
    {
        return Err(configuration(
            "credential destinations require HTTPS without userinfo or fragments",
        ));
    }
    Ok(url)
}

fn origin_matches(origin: &str, destination: &Url, allow_loopback: bool) -> Result<(), Failure> {
    let allowed = url(origin, allow_loopback)?;
    if allowed.path() != "/"
        || allowed.query().is_some()
        || allowed.origin() != destination.origin()
    {
        return Err(configuration("credential destination is not authorized"));
    }
    Ok(())
}

fn header(name: &str) -> Result<HeaderName, Failure> {
    let name = HeaderName::from_bytes(name.as_bytes())
        .map_err(|_| configuration("invalid credential header"))?;
    // Only authentication headers are supported; never permit transport/routing
    // control, cookies, or proxy authentication via a credential binding.
    if name != reqwest::header::AUTHORIZATION
        && name.as_str() != "api-key"
        && !name.as_str().starts_with("x-")
    {
        return Err(configuration(
            "credential header must be Authorization, api-key, or an x- header",
        ));
    }
    if name.as_str().starts_with("x-forwarded-")
        || name.as_str() == "x-http-method-override"
        || name.as_str() == super::diagnostics::REQUEST_ID_HEADER
    {
        return Err(configuration("routing headers cannot carry credentials"));
    }
    Ok(name)
}

fn binding_value(name: &str, prefix: &str, secret: &str) -> Result<Resolved, Failure> {
    if secret.is_empty() || prefix.len().saturating_add(secret.len()) > MAX_SECRET_BYTES {
        return Err(configuration(
            "credential value is empty or exceeds its limit",
        ));
    }
    let mut value = HeaderValue::from_str(&format!("{prefix}{secret}"))
        .map_err(|_| configuration("credential value is not a valid HTTP header"))?;
    value.set_sensitive(true);
    Ok(Resolved {
        name: header(name)?,
        value,
        expires_at: None,
    })
}

async fn read_file(path: &Path, maximum: usize) -> Result<Vec<u8>, Failure> {
    // Operator-controlled paths may be symlinks (e.g. projected secret volumes).
    // Reject special files; never intentionally wait on a FIFO or device.
    if !tokio::fs::metadata(path)
        .await
        .map_err(|_| configuration("credential file is unavailable"))?
        .is_file()
    {
        return Err(configuration("credential source must be a regular file"));
    }
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|_| configuration("credential file is unavailable"))?;
    let mut bytes = Vec::new();
    file.take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| configuration("credential file cannot be read"))?;
    if bytes.len() > maximum {
        return Err(configuration("credential file exceeds its limit"));
    }
    Ok(bytes)
}

impl Source {
    async fn read(&self) -> Result<String, Failure> {
        let value = match self {
            Self::Env { name } => std::env::var(name)
                .map_err(|_| configuration("credential environment value is unavailable"))?,
            Self::File { path } => String::from_utf8(read_file(path, MAX_SECRET_BYTES).await?)
                .map_err(|_| configuration("credential file must contain UTF-8"))?,
        };
        if value.is_empty() || value.len() > MAX_SECRET_BYTES {
            return Err(configuration(
                "credential value is empty or exceeds its limit",
            ));
        }
        // Do not trim secrets implicitly. Files must contain the exact value.
        Ok(value)
    }
}

impl Provider {
    /// Load nonsecret operator configuration once, bounded and without exposing
    /// parser input in diagnostics. Secret sources remain dynamically resolved.
    pub(crate) async fn load(path: &Path) -> Result<Self, Failure> {
        tokio::time::timeout(RESOLUTION_TIMEOUT, async {
            let bytes = read_file(path, MAX_CONFIG_BYTES).await?;
            let config: Config = serde_json::from_slice(&bytes)
                .map_err(|_| configuration("invalid credential provider configuration"))?;
            Self::new(config)
        })
        .await
        .map_err(|_| unavailable())?
    }

    pub(crate) fn new(config: Config) -> Result<Self, Failure> {
        if config.version != 1 {
            return Err(configuration(
                "unsupported credential configuration version",
            ));
        }
        match &config.provider {
            ProviderConfig::Local { bindings } => {
                for (reference, binding) in bindings {
                    validate_reference(reference)?;
                    let destination = url(&binding.allowed_origin, config.allow_http_loopback)?;
                    origin_matches(
                        &binding.allowed_origin,
                        &destination,
                        config.allow_http_loopback,
                    )?;
                    header(&binding.header)?;
                    if binding.prefix.len() >= MAX_SECRET_BYTES
                        || HeaderValue::from_str(&binding.prefix).is_err()
                    {
                        return Err(configuration("invalid credential header prefix"));
                    }
                }
            }
            ProviderConfig::Service { endpoint, .. } => {
                url(endpoint, config.allow_http_loopback)?;
            }
        }
        Ok(Self {
            config: Some(config),
            ..Self::default()
        })
    }

    /// Credential-bearing traffic never follows redirects or ambient proxies.
    pub(crate) async fn client(&self) -> Result<&reqwest::Client, Failure> {
        self.client
            .get_or_try_init(|| async {
                reqwest::Client::builder()
                    .redirect(reqwest::redirect::Policy::none())
                    .retry(reqwest::retry::never())
                    .no_proxy()
                    .build()
                    .map_err(|_| configuration("credential HTTP client initialization failed"))
            })
            .await
    }

    /// Resolve with a shared deadline including permit wait, file reads and HTTP.
    /// No retries, caches or serialized secret values are owned by this provider.
    pub(crate) async fn resolve(
        &self,
        reference: &str,
        destination: &str,
        headers: &BTreeMap<String, String>,
        execution: Execution,
    ) -> Result<Resolved, Failure> {
        validate_reference(reference)?;
        let config = self
            .config
            .as_ref()
            .ok_or_else(|| configuration("agent credential provider is not configured"))?;
        let destination = url(destination, config.allow_http_loopback)?;
        for name in headers.keys() {
            if [
                "host",
                "connection",
                "transfer-encoding",
                "content-length",
                "proxy-authorization",
                "proxy-connection",
                "upgrade",
                "trailer",
                "te",
            ]
            .iter()
            .any(|blocked| name.eq_ignore_ascii_case(blocked))
                || name.to_ascii_lowercase().starts_with("x-forwarded-")
                || name.eq_ignore_ascii_case("x-http-method-override")
            {
                return Err(configuration(
                    "profile cannot override authenticated request routing",
                ));
            }
        }
        tokio::time::timeout(RESOLUTION_TIMEOUT, async {
            let _permit = self.permits.acquire().await.map_err(|_| unavailable())?;
            let resolved = match &config.provider {
                ProviderConfig::Local { bindings } => {
                    let binding = bindings
                        .get(reference)
                        .ok_or_else(|| configuration("agent credential binding is unavailable"))?;
                    origin_matches(
                        &binding.allowed_origin,
                        &destination,
                        config.allow_http_loopback,
                    )?;
                    let name = header(&binding.header)?;
                    check_collision(headers, &name)?;
                    binding_value(
                        &binding.header,
                        &binding.prefix,
                        &binding.source.read().await?,
                    )?
                }
                ProviderConfig::Service {
                    endpoint,
                    token,
                    grants_directory,
                } => {
                    self.resolve_service(
                        endpoint,
                        token,
                        grants_directory,
                        (reference, &destination),
                        execution,
                    )
                    .await?
                }
            };
            check_collision(headers, &resolved.name)?;
            resolved.check_validity()?;
            Ok(resolved)
        })
        .await
        .map_err(|_| unavailable())?
    }

    async fn resolve_service(
        &self,
        endpoint: &str,
        token: &Source,
        grants_directory: &Path,
        target: (&str, &Url),
        execution: Execution,
    ) -> Result<Resolved, Failure> {
        let (reference, destination) = target;
        let grant = Source::File {
            path: grants_directory.join(execution.run_id.to_string()),
        }
        .read()
        .await?;
        let token = binding_value("Authorization", "Bearer ", &token.read().await?)?;
        let request = ServiceRequest {
            version: 1,
            credential: reference,
            destination: destination.as_str(),
            run_id: execution.run_id,
            execution_id: execution.execution_id,
            attempt_id: execution.attempt_id,
            grant: &grant,
        };
        let mut response = self
            .client()
            .await?
            .post(endpoint)
            .header(token.name, token.value)
            .json(&request)
            .send()
            .await
            .map_err(|_| unavailable())?;
        let status = response.status().as_u16();
        if matches!(status, 408 | 425 | 429 | 500 | 502 | 503 | 504) {
            return Err(unavailable());
        }
        if status != 200 {
            return Err(Failure::terminal(
                Kind::Authentication,
                "credential service denied resolution",
            ));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| unavailable())? {
            if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
                return Err(configuration(
                    "credential service response exceeds its limit",
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        let response: ServiceResponse = serde_json::from_slice(&bytes)
            .map_err(|_| configuration("invalid credential service response"))?;
        if response.version != 1 {
            return Err(configuration(
                "unsupported credential service response version",
            ));
        }
        origin_matches(
            &response.allowed_origin,
            destination,
            self.config.as_ref().is_some_and(|c| c.allow_http_loopback),
        )?;
        let mut result = binding_value(&response.header, "", &response.value)?;
        result.expires_at = Some(response.expires_at);
        Ok(result)
    }
}

fn check_collision(headers: &BTreeMap<String, String>, name: &HeaderName) -> Result<(), Failure> {
    if headers
        .keys()
        .any(|key| key.eq_ignore_ascii_case(name.as_str()))
    {
        return Err(configuration(
            "profile overrides a managed credential header",
        ));
    }
    Ok(())
}

#[derive(Serialize)]
struct ServiceRequest<'a> {
    version: u32,
    credential: &'a str,
    destination: &'a str,
    run_id: Uuid,
    execution_id: Uuid,
    attempt_id: Uuid,
    grant: &'a str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ServiceResponse {
    version: u32,
    allowed_origin: String,
    header: String,
    value: String,
    expires_at: chrono::DateTime<chrono::Utc>,
}

#[cfg(test)]
#[path = "credentials/tests.rs"]
mod tests;
