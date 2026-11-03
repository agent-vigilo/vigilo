//! Evaluator outbound HTTP authorization, independent of CLI and ABI bindings.
//!
//! Operators supply exact destination grants. Immutable snapshots deny by default,
//! with deny overriding allow. Authorization returns a destination capability whose
//! address check must be applied to the actual connection; it does not send traffic.
//! The worker cache batches loading and freezes snapshots for execution attempts.
//! The transport consumes authorized destinations under invocation budgets; host
//! adapters own credentials, cancellation and runtime scheduling.

use std::net::IpAddr;

use anyhow::{
    Result,
    ensure,
};
use ipnet::IpNet;
use reqwest::Url;
use serde::{
    Deserialize,
    Serialize,
};
use uuid::Uuid;

pub mod cache;

/// One operator-authored rule. Empty networks allow only public HTTPS addresses.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    pub uri: String,
    pub method: String,
    pub action: Action,
    #[serde(default)]
    pub networks: Vec<String>,
}

/// Explicit deny overrides any matching allow rule.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    Allow,
    Deny,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct Destination {
    scheme: String,
    host: String,
    port: u16,
    path: String,
}

fn destination(uri: &str) -> Result<(Url, Destination)> {
    ensure!(uri.len() <= 8192, "HTTP policy URI exceeds limit");
    ensure!(
        !uri.chars()
            .any(|c| c.is_control() || c.is_whitespace() || c == '\\'),
        "ambiguous HTTP URI"
    );
    let location = uri.split('?').next().unwrap_or(uri);
    ensure!(
        !location.contains('%'),
        "encoded destination is unsupported"
    );
    ensure!(
        !location.split('/').any(|part| part == "." || part == ".."),
        "dot path segments are unsupported"
    );
    let url = Url::parse(uri).map_err(|_| anyhow::anyhow!("invalid HTTP URI"))?;
    ensure!(
        matches!(url.scheme(), "https" | "http"),
        "unsupported HTTP scheme"
    );
    let authority = uri
        .split_once("://")
        .map(|(_, rest)| rest.split(['/', '?', '#']).next().unwrap_or(""))
        .unwrap_or("");
    ensure!(
        !authority.contains('@') && url.username().is_empty() && url.password().is_none(),
        "HTTP userinfo is forbidden"
    );
    ensure!(url.fragment().is_none(), "HTTP fragments are forbidden");
    let host = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("missing HTTP host"))?
        .to_owned();
    ensure!(
        !host.ends_with('.') && !host.contains('*'),
        "ambiguous HTTP host"
    );
    let key = Destination {
        scheme: url.scheme().into(),
        host,
        port: url.port_or_known_default().unwrap(),
        path: url.path().into(),
    };
    Ok((url, key))
}

#[derive(Clone, Debug)]
struct CompiledRule {
    destination: Destination,
    method: String,
    action: Action,
    networks: Vec<IpNet>,
}

/// Immutable authorization for one verified evaluator identity.
#[derive(Clone, Debug)]
pub struct Snapshot {
    evaluator_id: Uuid,
    revision: String,
    rules: Vec<CompiledRule>,
}

impl Snapshot {
    /// Validate a bounded rule set; malformed grants fail closed, never partially load.
    pub fn new(evaluator_id: Uuid, rules: Vec<Rule>) -> Result<Self> {
        ensure!(rules.len() <= 128, "too many evaluator HTTP rules");
        let mut compiled = Vec::new();
        let mut canonical = Vec::new();
        for rule in rules {
            let (url, dest) = destination(&rule.uri)?;
            ensure!(
                url.query().is_none(),
                "policy URI must not contain query parameters"
            );
            ensure!(
                rule.method.len() <= 32
                    && !rule.method.is_empty()
                    && rule
                        .method
                        .bytes()
                        .all(|b| b.is_ascii_uppercase() || b == b'-'),
                "invalid policy HTTP method"
            );
            ensure!(rule.networks.len() <= 64, "too many policy networks");
            let mut networks = rule
                .networks
                .iter()
                .map(|n| {
                    n.parse::<IpNet>()
                        .map_err(|_| anyhow::anyhow!("invalid policy network"))
                })
                .collect::<Result<Vec<_>>>()?;
            networks.sort();
            networks.dedup();
            ensure!(
                rule.action == Action::Deny || dest.scheme == "https" || !networks.is_empty(),
                "HTTP requires explicit network permission"
            );
            canonical.push(serde_json::to_string(&(
                &dest,
                &rule.method,
                rule.action,
                networks.iter().map(ToString::to_string).collect::<Vec<_>>(),
            ))?);
            compiled.push(CompiledRule {
                destination: dest,
                method: rule.method,
                action: rule.action,
                networks,
            });
        }
        canonical.sort();
        canonical.dedup();
        let revision = blake3::hash(&serde_json::to_vec(&(evaluator_id, canonical))?)
            .to_hex()
            .to_string();
        Ok(Self {
            evaluator_id,
            revision,
            rules: compiled,
        })
    }
    /// Verified evaluator identity to which this snapshot is bound.
    pub fn evaluator_id(&self) -> Uuid {
        self.evaluator_id
    }

    /// Content-derived revision, including evaluator identity and canonical rules.
    pub fn revision(&self) -> &str {
        &self.revision
    }

    /// Match the evaluator, method and exact destination before resolving DNS.
    /// Query parameters do not alter destination authority. Redirects require a new
    /// capability and are deliberately not supported by the HTTP executor contract.
    pub fn authorize(
        &self,
        evaluator_id: Uuid,
        method: &str,
        uri: &str,
    ) -> Result<AuthorizedDestination> {
        ensure!(
            evaluator_id == self.evaluator_id,
            "evaluator policy identity mismatch"
        );
        let (url, dest) = destination(uri)?;
        let matches = self
            .rules
            .iter()
            .filter(|r| r.destination == dest && r.method == method)
            .collect::<Vec<_>>();
        ensure!(
            !matches.iter().any(|r| r.action == Action::Deny),
            "evaluator HTTP explicitly denied"
        );
        let allows = matches
            .into_iter()
            .filter(|r| r.action == Action::Allow)
            .map(|r| r.networks.clone())
            .collect::<Vec<_>>();
        ensure!(!allows.is_empty(), "evaluator HTTP destination not granted");
        Ok(AuthorizedDestination { url, allows })
    }
}

/// Authorized destination, not a DNS-precheck result. A sender must connect only
/// to addresses accepted here, preserve TLS hostname verification and disable
/// redirects/proxies that could replace that destination. The bounded transport
/// consumes this capability before resolving credentials or opening connections.
#[derive(Debug)]
pub struct AuthorizedDestination {
    url: Url,
    allows: Vec<Vec<IpNet>>,
}
impl AuthorizedDestination {
    /// Parsed request URL; authorization does not change query parameters.
    pub fn url(&self) -> &Url {
        &self.url
    }
    /// Reject disallowed addresses; mixed DNS answers must never be used unchecked.
    pub fn permits_address(&self, address: IpAddr) -> bool {
        let address = match address {
            IpAddr::V6(v) => v.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(address),
            _ => address,
        };
        if let Some(host) = self
            .url
            .host_str()
            .and_then(|h| h.trim_matches(['[', ']']).parse::<IpAddr>().ok())
        {
            let host = match host {
                IpAddr::V6(v) => v.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(host),
                _ => host,
            };
            if host != address {
                return false;
            }
        }
        self.allows.iter().any(|ranges| {
            if ranges.is_empty() {
                public_address(address)
            } else {
                ranges.iter().any(|net| net.contains(&address))
            }
        })
    }
}

fn public_address(address: IpAddr) -> bool {
    let excluded: &[&str] = match address {
        IpAddr::V4(_) => &[
            "0.0.0.0/8",
            "10.0.0.0/8",
            "100.64.0.0/10",
            "127.0.0.0/8",
            "169.254.0.0/16",
            "172.16.0.0/12",
            "192.0.0.0/24",
            "192.0.2.0/24",
            "192.88.99.0/24",
            "192.168.0.0/16",
            "198.18.0.0/15",
            "198.51.100.0/24",
            "203.0.113.0/24",
            "224.0.0.0/3",
        ],
        IpAddr::V6(_) => {
            if !"2000::/3".parse::<IpNet>().unwrap().contains(&address) {
                return false;
            }
            &["2001::/23", "2001:db8::/32", "2002::/16", "3fff::/20"]
        }
    };
    !excluded
        .iter()
        .any(|net| net.parse::<IpNet>().unwrap().contains(&address))
}

#[cfg(test)]
mod tests;

/// Asynchronous transport for operator-authorized evaluator requests.
pub mod transport;
