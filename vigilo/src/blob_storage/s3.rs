//! S3 API transport for AWS S3 and Ceph RGW.
//!
//! SDK signing and credential resolution stay here. Single conditional PUTs
//! stream private files (all supported objects are at most 64 MiB), so there are
//! no multipart IDs to leak or abort. The outer store owns retries, integrity,
//! admission and endpoint capability checks; this transport issues one request.

use aws_sdk_s3::{
    Client,
    config::{
        BehaviorVersion,
        Credentials,
        Region,
        RequestChecksumCalculation,
        ResponseChecksumValidation,
        retry::RetryConfig,
        timeout::TimeoutConfig,
    },
    primitives::ByteStream,
};

use super::*;

/// Logical owner configuration, not part of a persisted object reference.
/// Omit endpoint for AWS; allow HTTP only for a deliberately local endpoint.
/// Credential variable names select a static/session pair; None uses the AWS
/// default credential chain (including workload-role credentials).
pub(crate) struct S3Config {
    pub alias: String,
    /// Unique caller-owned key, recorded before connection for crash cleanup.
    pub probe_key: String,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub allow_http: bool,
    pub path_style: bool,
    pub credential_variables: Option<(String, String, Option<String>)>,
}

pub(crate) struct S3Transport {
    client: Client,
    bucket: String,
    prefix: String,
}
pub(crate) type S3Storage = Store<S3Transport>;
impl S3Storage {
    /// Resolve credentials within the caller deadline and prove endpoint
    /// capabilities. The caller retains its recorded probe key for crash cleanup.
    pub async fn open(
        config: S3Config,
        limits: FormatLimits,
        resources: Arc<Resources>,
        budget: Budget,
        cancel: &CancellationToken,
    ) -> Result<Self> {
        limits
            .validate()
            .map_err(|_| StorageError::new(ErrorKind::Invalid))?;
        let op = Operation::new(budget, cancel)?;
        let transport = op.wait(S3Transport::build(&config)).await?;
        Store::connect(
            transport,
            config.alias,
            limits,
            resources,
            budget,
            cancel,
            config.probe_key,
        )
        .await
    }
}
impl S3Transport {
    async fn build(config: &S3Config) -> Result<Self> {
        let invalid = || StorageError::new(ErrorKind::Invalid);
        // Validate the namespace and owner with the same logical-key contract.
        ObjectRef::for_bytes(
            config.alias.clone(),
            format!("{}/probe", config.prefix),
            StorageFormat::DatasetManifest,
            b"x",
            &FormatLimits::default(),
        )
        .map_err(|_| invalid())?;
        if config.bucket.is_empty()
            || config.bucket.len() > 63
            || !config
                .bucket
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || b".-".contains(&c))
            || config.region.is_empty()
            || config.region.len() > 128
            || !config
                .region
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-')
        {
            return Err(invalid());
        }
        if let Some(endpoint) = &config.endpoint {
            let url = reqwest::Url::parse(endpoint).map_err(|_| invalid())?;
            if !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
                || url.host_str().is_none()
                || !(url.scheme() == "https" || (config.allow_http && url.scheme() == "http"))
            {
                return Err(invalid());
            }
        }
        let timeout = TimeoutConfig::builder()
            .connect_timeout(Duration::from_secs(5))
            .read_timeout(Duration::from_secs(15))
            .operation_timeout(Duration::from_secs(120))
            .build();
        let mut builder = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(config.region.clone()));
        if let Some((access, secret, token)) = &config.credential_variables {
            let access = std::env::var(access).map_err(|_| invalid())?;
            let secret = std::env::var(secret).map_err(|_| invalid())?;
            let token = token
                .as_ref()
                .map(std::env::var)
                .transpose()
                .map_err(|_| invalid())?;
            if access.is_empty()
                || secret.is_empty()
                || token.as_ref().is_some_and(String::is_empty)
            {
                return Err(invalid());
            }
            builder = builder.credentials_provider(Credentials::new(
                access,
                secret,
                token,
                None,
                "configured-environment",
            ));
        } else {
            let shared = aws_config::defaults(BehaviorVersion::latest())
                .region(Region::new(config.region.clone()))
                .retry_config(RetryConfig::disabled())
                .timeout_config(timeout.clone())
                .load()
                .await;
            builder =
                builder.credentials_provider(shared.credentials_provider().ok_or_else(invalid)?);
        }
        builder = builder
            .retry_config(RetryConfig::disabled())
            .timeout_config(timeout)
            .force_path_style(config.path_style)
            // BLAKE3 is checked independently, including exact replay. Avoid SDK
            // checksum framing which is not uniform across S3 API providers.
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .response_checksum_validation(ResponseChecksumValidation::WhenRequired);
        if let Some(endpoint) = &config.endpoint {
            builder = builder.endpoint_url(endpoint);
        }
        Ok(Self {
            client: Client::from_conf(builder.build()),
            bucket: config.bucket.clone(),
            prefix: config.prefix.clone(),
        })
    }
    fn request_config(no_progress: Duration) -> aws_sdk_s3::config::Builder {
        aws_sdk_s3::config::Builder::new()
            .timeout_config(
                TimeoutConfig::builder()
                    .connect_timeout(Duration::from_secs(5))
                    .read_timeout(no_progress)
                    .build(),
            )
            .stalled_stream_protection(
                aws_sdk_s3::config::StalledStreamProtectionConfig::enabled()
                    .grace_period(no_progress)
                    .build(),
            )
    }
    fn key(&self, key: &str) -> Result<String> {
        let key = format!("{}/{}", self.prefix, key);
        if key.len() > 1024 {
            return Err(StorageError::new(ErrorKind::Invalid));
        }
        Ok(key)
    }
}

/// Status and SDK failure class are sufficient for decisions. Never retain an
/// SDK diagnostic that may contain authorization headers or a signed URL.
fn classify<E: std::fmt::Debug>(error: aws_sdk_s3::error::SdkError<E>) -> StorageError {
    let status = error.raw_response().map(|r| r.status().as_u16());
    let kind = match status {
        Some(401 | 403) => ErrorKind::Unauthorized,
        Some(404) => ErrorKind::Missing,
        Some(412) => ErrorKind::Conflict,
        Some(405 | 501) => ErrorKind::Unsupported,
        Some(408 | 409 | 429 | 500 | 502 | 503 | 504) => ErrorKind::Transient,
        None if matches!(
            error,
            aws_sdk_s3::error::SdkError::TimeoutError(_)
                | aws_sdk_s3::error::SdkError::DispatchFailure(_)
                | aws_sdk_s3::error::SdkError::ResponseError(_)
        ) =>
        {
            ErrorKind::Transient
        }
        _ => ErrorKind::Invalid,
    };
    let mut result = StorageError::new(kind);
    result.status = status;
    result
}
#[async_trait]
impl Transport for S3Transport {
    async fn put(
        &self,
        reference: &ObjectRef,
        path: &Path,
        buffer: usize,
        no_progress: Duration,
    ) -> Result<()> {
        let body = ByteStream::read_from()
            .path(path)
            .buffer_size(buffer)
            .build()
            .await
            .map_err(|_| StorageError::new(ErrorKind::Local))?;
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(self.key(&reference.key)?)
            .if_none_match("*")
            .content_length(reference.byte_count as i64)
            .body(body)
            .customize()
            .config_override(Self::request_config(no_progress))
            .send()
            .await
            .map_err(classify)?;
        Ok(())
    }
    async fn get(&self, reference: &ObjectRef, no_progress: Duration) -> Result<Download> {
        let result = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.key(&reference.key)?)
            .set_version_id(reference.revision.clone())
            .customize()
            .config_override(Self::request_config(no_progress))
            .send()
            .await
            .map_err(classify)?;
        let length = result
            .content_length()
            .and_then(|n| u64::try_from(n).ok())
            .ok_or_else(|| StorageError::new(ErrorKind::Corrupt))?;
        Ok(Download {
            length,
            revision: result.version_id,
            reader: Box::new(result.body.into_async_read()),
        })
    }
    async fn delete_object(&self, key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(self.key(key)?)
            .send()
            .await
            .map_err(classify)?;
        Ok(())
    }
}
