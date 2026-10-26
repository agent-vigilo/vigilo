//! Owner configuration kept separate from durable references and case policy.
//!
//! A binding hash pins the alias's physical namespace for an upload. Credential
//! variable names may rotate without changing identity; endpoint/bucket/prefix
//! changes fail closed rather than silently redirecting recorded references.
use anyhow::{
    Result,
    ensure,
};
static RESOURCES: std::sync::OnceLock<std::sync::Arc<Resources>> = std::sync::OnceLock::new();
use serde::{
    Deserialize,
    Serialize,
};
use tokio_util::sync::CancellationToken;

use super::{
    Budget,
    Resources,
    s3::{
        S3Config,
        S3Storage,
    },
};
use crate::contracts::case_storage::{
    FormatLimits,
    ObjectRef,
    StorageFormat,
};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CredentialVariables {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
}
#[derive(Clone, Deserialize)]
#[serde(tag = "provider", rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) enum OwnerConfig {
    S3 {
        alias: String,
        bucket: String,
        prefix: String,
        region: String,
        endpoint: Option<String>,
        #[serde(default)]
        allow_http: bool,
        #[serde(default)]
        path_style: bool,
        credential_variables: Option<CredentialVariables>,
    },
}
impl OwnerConfig {
    pub fn alias(&self) -> &str {
        match self {
            Self::S3 { alias, .. } => alias,
        }
    }
    pub fn binding_hash(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Binding<'a> {
            provider: &'a str,
            alias: &'a str,
            bucket: &'a str,
            prefix: &'a str,
            region: &'a str,
            endpoint: &'a Option<String>,
            path_style: bool,
        }
        match self {
            Self::S3 {
                alias,
                bucket,
                prefix,
                region,
                endpoint,
                path_style,
                ..
            } => Ok(blake3::hash(&serde_json::to_vec(&Binding {
                provider: "s3",
                alias,
                bucket,
                prefix,
                region,
                endpoint,
                path_style: *path_style,
            })?)
            .to_hex()
            .to_string()),
        }
    }
    pub fn validate(&self, limits: &FormatLimits) -> Result<()> {
        limits.validate()?;
        match self {
            Self::S3 {
                alias,
                bucket,
                prefix,
                region,
                endpoint,
                allow_http,
                ..
            } => {
                ObjectRef::for_bytes(
                    alias.clone(),
                    format!("{prefix}/probe"),
                    StorageFormat::DatasetManifest,
                    b"probe-a",
                    limits,
                )?;
                ensure!(
                    prefix.len() <= 256
                        && !bucket.is_empty()
                        && bucket.len() <= 63
                        && bucket.bytes().all(|c| c.is_ascii_lowercase()
                            || c.is_ascii_digit()
                            || b".-".contains(&c)),
                    "invalid storage namespace"
                );
                ensure!(
                    !region.is_empty()
                        && region.len() <= 128
                        && region
                            .bytes()
                            .all(|c| c.is_ascii_alphanumeric() || c == b'-'),
                    "invalid storage region"
                );
                if let Some(endpoint) = endpoint {
                    let url = reqwest::Url::parse(endpoint)?;
                    ensure!(
                        url.username().is_empty()
                            && url.password().is_none()
                            && url.query().is_none()
                            && url.fragment().is_none()
                            && url.host_str().is_some()
                            && (url.scheme() == "https" || (*allow_http && url.scheme() == "http")),
                        "invalid storage endpoint"
                    );
                }
            }
        }
        Ok(())
    }
    pub async fn open(
        &self,
        probe_key: String,
        limits: FormatLimits,
        budget: Budget,
        cancel: &CancellationToken,
    ) -> Result<Box<dyn super::BlobStorage>> {
        self.validate(&limits)?;
        match self {
            Self::S3 {
                alias,
                bucket,
                prefix,
                region,
                endpoint,
                allow_http,
                path_style,
                credential_variables,
            } => Ok(Box::new(
                S3Storage::open(
                    S3Config {
                        alias: alias.clone(),
                        probe_key,
                        bucket: bucket.clone(),
                        prefix: prefix.clone(),
                        region: region.clone(),
                        endpoint: endpoint.clone(),
                        allow_http: *allow_http,
                        path_style: *path_style,
                        credential_variables: credential_variables.as_ref().map(|v| {
                            (
                                v.access_key.clone(),
                                v.secret_key.clone(),
                                v.session_token.clone(),
                            )
                        }),
                    },
                    limits,
                    RESOURCES.get_or_init(Resources::standard).clone(),
                    budget,
                    cancel,
                )
                .await?,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn binding_pins_namespace_but_allows_credential_rotation() {
        let text = "provider='s3'\nalias='cases'\nbucket='cases'\nprefix='vigilo'\nregion='us-east-1'\nendpoint='http://127.0.0.1:3900'\nallow_http=true\npath_style=true\n";
        let a: OwnerConfig = toml::from_str(text).unwrap();
        a.validate(&FormatLimits::default()).unwrap();
        let b: OwnerConfig = toml::from_str(&format!(
            "{text}\n[credential_variables]\naccess_key='A'\nsecret_key='B'\n"
        ))
        .unwrap();
        assert_eq!(a.binding_hash().unwrap(), b.binding_hash().unwrap());
        let moved: OwnerConfig =
            toml::from_str(&text.replace("prefix='vigilo'", "prefix='elsewhere'")).unwrap();
        assert_ne!(a.binding_hash().unwrap(), moved.binding_hash().unwrap());
        assert!(
            toml::from_str::<OwnerConfig>(&text.replace("provider='s3'", "provider='unknown'"))
                .is_err()
        );
    }
    #[test]
    fn unsafe_endpoint_and_owner_configuration_is_rejected() {
        for endpoint in [
            "https://user:secret@example.com",
            "https://example.com?secret=x",
            "file:///tmp/store",
            "http://example.com",
        ] {
            let text = format!(
                "provider='s3'\nalias='cases'\nbucket='cases'\nprefix='vigilo'\nregion='us-east-1'\nendpoint='{endpoint}'\n"
            );
            let owner: OwnerConfig = toml::from_str(&text).unwrap();
            assert!(owner.validate(&FormatLimits::default()).is_err());
        }
    }
}
