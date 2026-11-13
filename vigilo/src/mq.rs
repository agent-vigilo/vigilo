//! RabbitMQ client wrapper used by coordinators and workers.
//!
//! The client lazily opens a broker session with a publish channel, declares
//! topology for each fresh session, and recreates the session after connection
//! or channel loss. A process-local circuit breaker prevents repeated broker
//! operations during outages and admits one recovery probe after its cooldown.
//! Long-lived worker consumers use their own channel while delivery
//! acknowledgements use the message's channel-scoped acker.

use std::{
    sync::Arc,
    time::{
        Duration,
        Instant,
    },
};

use anyhow::Context as _;
use lapin::{
    BasicProperties,
    Channel,
    Connection,
    ConnectionProperties,
    Error as LapinError,
    ExchangeKind,
    acker::Acker,
    options::{
        BasicAckOptions,
        BasicConsumeOptions,
        BasicGetOptions,
        BasicPublishOptions,
        BasicQosOptions,
        ConfirmSelectOptions,
        ExchangeDeclareOptions,
        QueueBindOptions,
        QueueDeclareOptions,
    },
    publisher_confirm::Confirmation,
    types::{
        AMQPValue,
        FieldTable,
        LongString,
        ShortString,
    },
};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{
    debug,
    info,
    warn,
};
use uuid::Uuid;

use crate::circuit_breaker::{
    CircuitBreakers,
    CircuitPermit,
    CircuitTransition,
    Config as CircuitBreakerConfig,
    FailureImpact,
};

const BROKER_CIRCUIT_KEY: &str = "rabbitmq";
const WORKER_ROUTING_KEY: &str = "run.chunk.ready";
const WORKER_MAX_RETRIES: i32 = 8;
const WORKER_RETRY_BUCKETS: [(&str, i32); 8] = [
    ("vigilo.worker.retry.5s", 5_000),
    ("vigilo.worker.retry.10s", 10_000),
    ("vigilo.worker.retry.20s", 20_000),
    ("vigilo.worker.retry.40s", 40_000),
    ("vigilo.worker.retry.80s", 80_000),
    ("vigilo.worker.retry.160s", 160_000),
    ("vigilo.worker.retry.320s", 320_000),
    ("vigilo.worker.retry.640s", 640_000),
];
const HEADER_RETRY_COUNT: &str = "x-vigilo-retry-count";
const HEADER_FIRST_FAILED_AT: &str = "x-vigilo-first-failed-at";
const HEADER_LAST_FAILED_AT: &str = "x-vigilo-last-failed-at";
const HEADER_LAST_ERROR: &str = "x-vigilo-last-error";
const HEADER_ERROR_CLASS: &str = "x-vigilo-error-class";
const HEADER_ORIGINAL_EXCHANGE: &str = "x-vigilo-original-exchange";
const HEADER_ORIGINAL_ROUTING_KEY: &str = "x-vigilo-original-routing-key";

/// RabbitMQ connection and routing configuration.
///
/// The exchange is a durable topic exchange used for all runtime events. The
/// worker queue receives `run.chunk.ready` events consumed by worker commands.
#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub(crate) uri: String,
    pub(crate) circuit_breaker_config: CircuitBreakerConfig,
    pub(crate) exchange: String,
    pub(crate) event_queue: String,
    pub(crate) worker_queue: String,
    pub(crate) worker_retry_exchange: String,
    pub(crate) worker_quarantine_exchange: String,
    pub(crate) worker_quarantine_queue: String,
}

impl Config {
    /// Builds default queue/exchange names around a caller-provided broker URI.
    ///
    /// Set `VIGILO_MQ_NAMESPACE` to isolate broker topology for integration
    /// tests or parallel local stacks. When unset, the historic queue and
    /// exchange names are preserved.
    pub(crate) fn new(uri: String) -> Self {
        let namespace = std::env::var("VIGILO_MQ_NAMESPACE")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());

        Self::explicit(uri, namespace.as_deref())
    }

    /// Host configuration selects topology explicitly, without environment lookup.
    pub(crate) fn explicit(uri: String, namespace: Option<&str>) -> Self {
        if let Some(namespace) = namespace {
            let prefix = format!("vigilo.{}", namespace);
            return Self {
                uri,
                circuit_breaker_config: CircuitBreakerConfig::default(),
                exchange: format!("{prefix}.events"),
                event_queue: format!("{prefix}.events.domain"),
                worker_queue: format!("{prefix}.worker"),
                worker_retry_exchange: format!("{prefix}.worker.retry"),
                worker_quarantine_exchange: format!("{prefix}.worker.quarantine"),
                worker_quarantine_queue: format!("{prefix}.worker.quarantine"),
            };
        }

        Self {
            uri,
            circuit_breaker_config: CircuitBreakerConfig::default(),
            exchange: "vigilo.events".to_string(),
            event_queue: "vigilo.events.domain".to_string(),
            worker_queue: "vigilo.worker".to_string(),
            worker_retry_exchange: "vigilo.worker.retry".to_string(),
            worker_quarantine_exchange: "vigilo.worker.quarantine".to_string(),
            worker_quarantine_queue: "vigilo.worker.quarantine".to_string(),
        }
    }

    pub(crate) fn with_circuit_breaker(
        mut self,
        circuit_breaker_config: CircuitBreakerConfig,
    ) -> Self {
        self.circuit_breaker_config = circuit_breaker_config;
        self
    }
}

/// Raw RabbitMQ worker delivery with payload bytes and delivery metadata.
pub(crate) struct RawConsumedMessage {
    pub(crate) delivery_tag: u64,
    pub(crate) acker: Acker,
    pub(crate) body: Vec<u8>,
    pub(crate) properties: BasicProperties,
    pub(crate) exchange: String,
    pub(crate) routing_key: String,
    pub(crate) redelivered: bool,
}

/// A JSON message fetched from the worker queue.
///
/// The `raw` delivery must be passed back to `ack`, retry, or quarantine after
/// the caller has processed the payload.
pub(crate) struct ConsumedMessage {
    pub(crate) raw: RawConsumedMessage,
    pub(crate) payload: Value,
}

impl ConsumedMessage {
    pub(crate) fn delivery_tag(&self) -> u64 {
        self.raw.delivery_tag
    }
}

/// Lazily initialized RabbitMQ broker session.
///
/// The client owns connection setup and broker declarations so command code can
/// work in terms of application events instead of `lapin` primitives. The
/// cached session is invalidated after connection/channel loss and rebuilt by
/// the next admitted broker operation. The broker circuit covers publishing,
/// one-shot consumption, and consumer creation; message retry policy remains
/// independent.
pub(crate) struct Client {
    config: Config,
    circuit_breakers: CircuitBreakers,
    session: Mutex<Option<Arc<BrokerSession>>>,
}

#[derive(Debug, Error)]
#[error("rabbitmq circuit is open; retry after {retry_after:?}")]
struct BrokerCircuitOpen {
    retry_after: Duration,
}

#[derive(Debug, Error)]
enum PublishError {
    #[error(
        "rabbitmq publish was confirmed but returned unroutable for routing key '{routing_key}': {reason}"
    )]
    Returned { routing_key: String, reason: String },
    #[error(
        "rabbitmq publish was negatively acknowledged for routing key '{routing_key}': {reason}"
    )]
    Nacked { routing_key: String, reason: String },
    #[error("rabbitmq publish confirmation was not requested")]
    ConfirmationNotRequested,
}

struct BrokerSession {
    connection: Connection,
    publish_channel: Channel,
}

impl Client {
    /// Creates a client shell; no network connection is opened until first use.
    pub(crate) fn new(config: Config) -> Self {
        let circuit_breakers = CircuitBreakers::new(config.circuit_breaker_config);
        Self {
            config,
            circuit_breakers,
            session: Mutex::new(None),
        }
    }

    fn is_unavailable_lapin_error(err: &LapinError) -> bool {
        matches!(
            err,
            LapinError::InvalidChannel(_)
                | LapinError::InvalidChannelState(_)
                | LapinError::InvalidConnectionState(_)
                | LapinError::IOError(_)
                | LapinError::MissingHeartbeatError
        )
    }

    fn is_broker_unavailable(error: &anyhow::Error) -> bool {
        error
            .chain()
            .filter_map(|cause| cause.downcast_ref::<LapinError>())
            .any(Self::is_unavailable_lapin_error)
    }

    fn acquire_broker_operation(
        &self,
        operation: &'static str,
        now: Instant,
    ) -> anyhow::Result<CircuitPermit> {
        let permit = self
            .circuit_breakers
            .acquire(BROKER_CIRCUIT_KEY, now)
            .map_err(|open| BrokerCircuitOpen {
                retry_after: open.retry_after,
            })?;
        if permit.is_probe() {
            info!(operation, "probing half-open rabbitmq circuit");
        }
        Ok(permit)
    }

    fn finish_broker_operation<T>(
        &self,
        operation: &'static str,
        permit: CircuitPermit,
        now: Instant,
        result: anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        match result {
            Ok(value) => {
                if matches!(
                    self.circuit_breakers.record_success(permit),
                    Some(CircuitTransition::Closed)
                ) {
                    info!(operation, "closed rabbitmq circuit after successful probe");
                }
                Ok(value)
            }
            Err(error) => {
                let impact = if Self::is_broker_unavailable(&error) {
                    FailureImpact::Unavailable
                } else {
                    FailureImpact::Available
                };
                if let Some(CircuitTransition::Opened { retry_after }) =
                    self.circuit_breakers.record_failure(permit, now, impact)
                {
                    warn!(
                        operation,
                        retry_after_ms = retry_after.as_millis() as u64,
                        "opened rabbitmq circuit after availability failures"
                    );
                }
                Err(error)
            }
        }
    }

    /// Drops the cached broker session so the next operation reconnects and
    /// re-declares topology.
    pub(crate) async fn invalidate_session(&self) {
        let mut session = self.session.lock().await;
        if session.take().is_some() {
            warn!("invalidated rabbitmq session; next operation will reconnect");
        }
    }

    async fn get_or_connect_session(&self) -> anyhow::Result<Arc<BrokerSession>> {
        let mut session = self.session.lock().await;
        if let Some(existing) = session.as_ref() {
            return Ok(existing.clone());
        }

        let connected = Arc::new(self.connect_session().await?);
        *session = Some(connected.clone());
        Ok(connected)
    }

    async fn connect_session(&self) -> anyhow::Result<BrokerSession> {
        debug!("initializing rabbitmq connection");
        let connection = Connection::connect(&self.config.uri, ConnectionProperties::default())
            .await
            .context("rabbitmq connection failed")?;
        let publish_channel = connection
            .create_channel()
            .await
            .context("rabbitmq channel creation failed")?;

        self.declare_topology(&publish_channel).await?;
        publish_channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .context("rabbitmq confirm-select failed")?;

        Ok(BrokerSession {
            connection,
            publish_channel,
        })
    }

    fn retry_queue_name(&self, retry_bucket_name: &str) -> String {
        retry_bucket_name.replacen("vigilo.worker.retry", &self.config.worker_retry_exchange, 1)
    }

    /// Ensures durable queues and bindings exist on the provided channel.
    async fn declare_topology(&self, channel: &Channel) -> anyhow::Result<()> {
        channel
            .exchange_declare(
                &self.config.exchange,
                ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    auto_delete: false,
                    internal: false,
                    nowait: false,
                    passive: false,
                },
                FieldTable::default(),
            )
            .await
            .context("rabbitmq exchange declaration failed")?;

        channel
            .queue_declare(
                &self.config.event_queue,
                QueueDeclareOptions {
                    passive: false,
                    durable: true,
                    exclusive: false,
                    auto_delete: false,
                    nowait: false,
                },
                FieldTable::default(),
            )
            .await
            .context("rabbitmq event queue declaration failed")?;

        channel
            .queue_bind(
                &self.config.event_queue,
                &self.config.exchange,
                "run.*",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .context("rabbitmq event queue binding failed")?;

        channel
            .queue_declare(
                &self.config.worker_queue,
                QueueDeclareOptions {
                    passive: false,
                    durable: true,
                    exclusive: false,
                    auto_delete: false,
                    nowait: false,
                },
                FieldTable::default(),
            )
            .await
            .context("rabbitmq queue declaration failed")?;

        channel
            .queue_bind(
                &self.config.worker_queue,
                &self.config.exchange,
                WORKER_ROUTING_KEY,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .context("rabbitmq queue binding failed")?;

        channel
            .exchange_declare(
                &self.config.worker_retry_exchange,
                ExchangeKind::Direct,
                ExchangeDeclareOptions {
                    durable: true,
                    auto_delete: false,
                    internal: false,
                    nowait: false,
                    passive: false,
                },
                FieldTable::default(),
            )
            .await
            .context("rabbitmq worker retry exchange declaration failed")?;

        channel
            .queue_bind(
                &self.config.worker_queue,
                &self.config.worker_retry_exchange,
                WORKER_ROUTING_KEY,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .context("rabbitmq worker retry return binding failed")?;

        for (queue_name, ttl_ms) in WORKER_RETRY_BUCKETS {
            let queue_name = self.retry_queue_name(queue_name);
            let mut retry_args = FieldTable::default();
            retry_args.insert("x-message-ttl".into(), AMQPValue::LongInt(ttl_ms));
            retry_args.insert(
                "x-dead-letter-exchange".into(),
                AMQPValue::LongString(LongString::from(self.config.worker_retry_exchange.clone())),
            );
            retry_args.insert(
                "x-dead-letter-routing-key".into(),
                AMQPValue::LongString(LongString::from(WORKER_ROUTING_KEY)),
            );

            channel
                .queue_declare(
                    &queue_name,
                    QueueDeclareOptions {
                        passive: false,
                        durable: true,
                        exclusive: false,
                        auto_delete: false,
                        nowait: false,
                    },
                    retry_args,
                )
                .await
                .with_context(|| {
                    format!(
                        "rabbitmq worker retry queue declaration failed for '{}'",
                        queue_name
                    )
                })?;

            channel
                .queue_bind(
                    &queue_name,
                    &self.config.worker_retry_exchange,
                    &queue_name,
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await
                .with_context(|| {
                    format!(
                        "rabbitmq worker retry queue binding failed for '{}'",
                        queue_name
                    )
                })?;
        }

        channel
            .exchange_declare(
                &self.config.worker_quarantine_exchange,
                ExchangeKind::Direct,
                ExchangeDeclareOptions {
                    durable: true,
                    auto_delete: false,
                    internal: false,
                    nowait: false,
                    passive: false,
                },
                FieldTable::default(),
            )
            .await
            .context("rabbitmq worker quarantine exchange declaration failed")?;

        channel
            .queue_declare(
                &self.config.worker_quarantine_queue,
                QueueDeclareOptions {
                    passive: false,
                    durable: true,
                    exclusive: false,
                    auto_delete: false,
                    nowait: false,
                },
                FieldTable::default(),
            )
            .await
            .context("rabbitmq worker quarantine queue declaration failed")?;

        channel
            .queue_bind(
                &self.config.worker_quarantine_queue,
                &self.config.worker_quarantine_exchange,
                &self.config.worker_quarantine_queue,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .context("rabbitmq worker quarantine queue binding failed")?;

        Ok(())
    }

    async fn publish_bytes_with_properties(
        &self,
        exchange: &str,
        routing_key: &str,
        body: &[u8],
        properties: BasicProperties,
    ) -> anyhow::Result<()> {
        let publish_result = self
            .publish_bytes_with_properties_once(exchange, routing_key, body, properties.clone())
            .await;
        if let Err(err) = &publish_result
            && Self::is_broker_unavailable(err)
        {
            warn!(
                error = %err,
                "rabbitmq publish failed on current session; reconnecting and retrying once"
            );
            self.invalidate_session().await;
            return self
                .publish_bytes_with_properties_once(exchange, routing_key, body, properties)
                .await;
        }

        publish_result
    }

    async fn publish_bytes_with_properties_once(
        &self,
        exchange: &str,
        routing_key: &str,
        body: &[u8],
        properties: BasicProperties,
    ) -> anyhow::Result<()> {
        let operation = "publish";
        let permit = self.acquire_broker_operation(operation, Instant::now())?;
        let result = async {
            let session = self.get_or_connect_session().await?;
            let channel = &session.publish_channel;
            let confirmation = channel
                .basic_publish(
                    exchange,
                    routing_key,
                    BasicPublishOptions {
                        mandatory: true,
                        ..BasicPublishOptions::default()
                    },
                    body,
                    properties,
                )
                .await
                .context("rabbitmq publish failed")?
                .await
                .context("rabbitmq publish confirmation failed")?;

            match confirmation {
                Confirmation::Ack(None) => Ok(()),
                Confirmation::Ack(Some(returned)) => Err(PublishError::Returned {
                    routing_key: routing_key.to_string(),
                    reason: returned
                        .error()
                        .map(|error| error.to_string())
                        .unwrap_or_else(|| returned.reply_text.to_string()),
                }
                .into()),
                Confirmation::Nack(returned) => Err(PublishError::Nacked {
                    routing_key: routing_key.to_string(),
                    reason: returned
                        .as_deref()
                        .and_then(|message| message.error())
                        .map(|error| error.to_string())
                        .unwrap_or_else(|| "broker negatively acknowledged publish".to_string()),
                }
                .into()),
                Confirmation::NotRequested => Err(PublishError::ConfirmationNotRequested.into()),
            }
        }
        .await;

        self.finish_broker_operation(operation, permit, Instant::now(), result)
    }

    /// Publishes a JSON payload to the configured topic exchange.
    pub(crate) async fn publish_json(
        &self,
        routing_key: &str,
        payload: &Value,
        message_id: &str,
    ) -> anyhow::Result<()> {
        let body = serde_json::to_vec(payload).context("failed to serialize message payload")?;

        self.publish_bytes_with_properties(
            &self.config.exchange,
            routing_key,
            &body,
            BasicProperties::default()
                .with_content_type("application/json".into())
                .with_delivery_mode(2)
                .with_message_id(message_id.to_string().into()),
        )
        .await
    }

    /// Fetches one available worker message without creating a long-lived consumer.
    ///
    /// Workers currently poll with `basic_get`, which keeps the command model
    /// simple for one-shot worker mode.
    pub(crate) async fn consume_worker_message(
        &self,
    ) -> anyhow::Result<Option<RawConsumedMessage>> {
        let consume_result = self.consume_worker_message_once().await;
        if let Err(err) = &consume_result
            && Self::is_broker_unavailable(err)
        {
            warn!(
                error = %err,
                "rabbitmq basic_get failed on current session; reconnecting and retrying once"
            );
            self.invalidate_session().await;
            return self.consume_worker_message_once().await;
        }

        consume_result
    }

    async fn consume_worker_message_once(&self) -> anyhow::Result<Option<RawConsumedMessage>> {
        let operation = "basic_get";
        let permit = self.acquire_broker_operation(operation, Instant::now())?;
        let result = async {
            let session = self.get_or_connect_session().await?;
            let channel = &session.publish_channel;
            let maybe_delivery = channel
                .basic_get(&self.config.worker_queue, BasicGetOptions::default())
                .await
                .context("rabbitmq consume failed")?;

            Ok(maybe_delivery.map(|delivery| RawConsumedMessage {
                delivery_tag: delivery.delivery_tag,
                acker: delivery.acker.clone(),
                body: delivery.data.clone(),
                properties: delivery.properties.clone(),
                exchange: delivery.exchange.to_string(),
                routing_key: delivery.routing_key.to_string(),
                redelivered: delivery.redelivered,
            }))
        }
        .await;

        self.finish_broker_operation(operation, permit, Instant::now(), result)
    }

    /// Creates a long-lived consumer stream for worker messages using `basic_consume`.
    pub(crate) async fn consume_worker_stream(
        &self,
        consumer_tag_prefix: &str,
        prefetch: u16,
    ) -> anyhow::Result<lapin::Consumer> {
        let stream_result = self
            .consume_worker_stream_once(consumer_tag_prefix, prefetch)
            .await;
        if let Err(err) = &stream_result
            && Self::is_broker_unavailable(err)
        {
            warn!(
                error = %err,
                "rabbitmq consumer creation failed on current session; reconnecting and retrying once"
            );
            self.invalidate_session().await;
            return self
                .consume_worker_stream_once(consumer_tag_prefix, prefetch)
                .await;
        }

        stream_result
    }

    async fn consume_worker_stream_once(
        &self,
        consumer_tag_prefix: &str,
        prefetch: u16,
    ) -> anyhow::Result<lapin::Consumer> {
        let operation = "create_consumer";
        let permit = self.acquire_broker_operation(operation, Instant::now())?;
        let result = async {
            let session = self.get_or_connect_session().await?;
            let channel = session
                .connection
                .create_channel()
                .await
                .context("rabbitmq consumer channel creation failed")?;
            self.declare_topology(&channel).await?;
            channel
                .basic_qos(prefetch, BasicQosOptions { global: false })
                .await
                .context("rabbitmq qos configuration failed")?;

            let consumer_tag = format!("{}-{}", consumer_tag_prefix, Uuid::now_v7());
            channel
                .basic_consume(
                    &self.config.worker_queue,
                    &consumer_tag,
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await
                .context("rabbitmq consumer creation failed")
        }
        .await;

        self.finish_broker_operation(operation, permit, Instant::now(), result)
    }

    /// Acknowledges successful processing of one delivery.
    pub(crate) async fn ack(&self, message: &RawConsumedMessage) -> anyhow::Result<()> {
        match message.acker.ack(BasicAckOptions::default()).await {
            Ok(()) => Ok(()),
            Err(err) if Self::is_unavailable_lapin_error(&err) => {
                warn!(
                    delivery_tag = message.delivery_tag,
                    error = %err,
                    "rabbitmq ack failed after connection/channel loss; message may redeliver"
                );
                self.invalidate_session().await;
                Ok(())
            }
            Err(error) => Err(anyhow::Error::new(error).context("rabbitmq ack failed")),
        }
    }

    fn retry_count(properties: &BasicProperties) -> i32 {
        properties
            .headers()
            .as_ref()
            .and_then(|headers| headers.inner().get(HEADER_RETRY_COUNT))
            .and_then(|value| value.as_long_int())
            .unwrap_or(0)
            .max(0)
    }

    fn retry_queue_for_attempt(&self, attempt: i32) -> String {
        let index = usize::try_from(attempt.max(1).saturating_sub(1)).unwrap_or(usize::MAX);
        let queue_name = WORKER_RETRY_BUCKETS
            .get(index)
            .or_else(|| WORKER_RETRY_BUCKETS.last())
            .map(|(queue, _)| *queue)
            .unwrap_or("vigilo.worker.retry.30m");

        self.retry_queue_name(queue_name)
    }

    fn retry_queue_for_delay_seconds(&self, delay_seconds: i64) -> String {
        let delay_ms = delay_seconds.max(1).saturating_mul(1_000);
        let queue_name = WORKER_RETRY_BUCKETS
            .iter()
            .find(|(_, ttl_ms)| i64::from(*ttl_ms) >= delay_ms)
            .or_else(|| WORKER_RETRY_BUCKETS.last())
            .map(|(queue, _)| *queue)
            .unwrap_or("vigilo.worker.retry.30m");

        self.retry_queue_name(queue_name)
    }

    pub(crate) fn can_retry_worker_message(&self, message: &RawConsumedMessage) -> bool {
        Self::retry_count(&message.properties) < WORKER_MAX_RETRIES
    }

    fn message_headers(message: &RawConsumedMessage) -> FieldTable {
        message
            .properties
            .headers()
            .as_ref()
            .cloned()
            .unwrap_or_default()
    }

    fn insert_string_header(headers: &mut FieldTable, key: &str, value: impl Into<String>) {
        headers.insert(
            ShortString::from(key),
            AMQPValue::LongString(LongString::from(value.into())),
        );
    }

    fn retry_properties(
        message: &RawConsumedMessage,
        retry_count: i32,
        reason: &str,
        error_class: &str,
    ) -> BasicProperties {
        let mut headers = Self::message_headers(message);
        let now = chrono::Utc::now().to_rfc3339();
        if !headers.contains_key(HEADER_FIRST_FAILED_AT) {
            Self::insert_string_header(&mut headers, HEADER_FIRST_FAILED_AT, now.clone());
        }
        headers.insert(
            ShortString::from(HEADER_RETRY_COUNT),
            AMQPValue::LongInt(retry_count),
        );
        Self::insert_string_header(&mut headers, HEADER_LAST_FAILED_AT, now);
        Self::insert_string_header(&mut headers, HEADER_LAST_ERROR, reason);
        Self::insert_string_header(&mut headers, HEADER_ERROR_CLASS, error_class);
        Self::insert_string_header(&mut headers, HEADER_ORIGINAL_EXCHANGE, &message.exchange);
        Self::insert_string_header(
            &mut headers,
            HEADER_ORIGINAL_ROUTING_KEY,
            &message.routing_key,
        );

        message
            .properties
            .clone()
            .with_headers(headers)
            .with_delivery_mode(2)
    }

    pub(crate) async fn retry_worker_message(
        &self,
        message: &RawConsumedMessage,
        reason: &str,
        error_class: &str,
    ) -> anyhow::Result<()> {
        let next_retry_count = Self::retry_count(&message.properties).saturating_add(1);
        if next_retry_count > WORKER_MAX_RETRIES {
            self.quarantine_worker_message(
                message,
                &format!(
                    "worker message exhausted {} retries: {}",
                    WORKER_MAX_RETRIES, reason
                ),
                error_class,
            )
            .await?;
            return Ok(());
        }

        let retry_queue = self.retry_queue_for_attempt(next_retry_count);
        let properties = Self::retry_properties(message, next_retry_count, reason, error_class);
        self.publish_bytes_with_properties(
            &self.config.worker_retry_exchange,
            &retry_queue,
            &message.body,
            properties,
        )
        .await?;
        self.ack(message).await?;

        Ok(())
    }

    pub(crate) async fn delay_worker_message(
        &self,
        message: &RawConsumedMessage,
        delay_seconds: i64,
        reason: &str,
        error_class: &str,
    ) -> anyhow::Result<()> {
        let retry_queue = self.retry_queue_for_delay_seconds(delay_seconds);
        let retry_count = Self::retry_count(&message.properties);
        let properties = Self::retry_properties(message, retry_count, reason, error_class);
        self.publish_bytes_with_properties(
            &self.config.worker_retry_exchange,
            &retry_queue,
            &message.body,
            properties,
        )
        .await?;
        self.ack(message).await?;

        Ok(())
    }

    pub(crate) async fn quarantine_worker_message(
        &self,
        message: &RawConsumedMessage,
        reason: &str,
        error_class: &str,
    ) -> anyhow::Result<()> {
        let retry_count = Self::retry_count(&message.properties);
        let mut headers = Self::message_headers(message);
        let now = chrono::Utc::now().to_rfc3339();
        Self::insert_string_header(&mut headers, HEADER_LAST_FAILED_AT, now.clone());
        Self::insert_string_header(&mut headers, HEADER_LAST_ERROR, reason);
        Self::insert_string_header(&mut headers, HEADER_ERROR_CLASS, error_class);
        Self::insert_string_header(&mut headers, HEADER_ORIGINAL_EXCHANGE, &message.exchange);
        Self::insert_string_header(
            &mut headers,
            HEADER_ORIGINAL_ROUTING_KEY,
            &message.routing_key,
        );

        let original_payload = serde_json::from_slice::<Value>(&message.body)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&message.body).to_string()));
        let envelope = serde_json::json!({
            "reason": reason,
            "error_class": error_class,
            "retry_count": retry_count,
            "original_exchange": message.exchange,
            "original_routing_key": message.routing_key,
            "redelivered": message.redelivered,
            "failed_at": now,
            "original_payload": original_payload,
        });
        let body =
            serde_json::to_vec(&envelope).context("failed to serialize quarantine payload")?;

        self.publish_bytes_with_properties(
            &self.config.worker_quarantine_exchange,
            &self.config.worker_quarantine_queue,
            &body,
            BasicProperties::default()
                .with_content_type("application/json".into())
                .with_delivery_mode(2)
                .with_headers(headers),
        )
        .await?;
        self.ack(message).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::{
            Arc,
            Mutex,
        },
    };

    use super::*;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn mq_config_preserves_default_topology_without_namespace() {
        let config = config_with_namespace(None);

        assert_eq!(config.uri, "amqp://localhost");
        assert_eq!(config.exchange, "vigilo.events");
        assert_eq!(config.event_queue, "vigilo.events.domain");
        assert_eq!(config.worker_queue, "vigilo.worker");
        assert_eq!(config.worker_retry_exchange, "vigilo.worker.retry");
        assert_eq!(config.worker_quarantine_queue, "vigilo.worker.quarantine");
    }

    #[test]
    fn mq_config_scopes_topology_with_namespace() {
        let config = config_with_namespace(Some(" test-123 "));

        assert_eq!(config.exchange, "vigilo.test-123.events");
        assert_eq!(config.event_queue, "vigilo.test-123.events.domain");
        assert_eq!(config.worker_queue, "vigilo.test-123.worker");
        assert_eq!(config.worker_retry_exchange, "vigilo.test-123.worker.retry");
        assert_eq!(
            config.worker_quarantine_queue,
            "vigilo.test-123.worker.quarantine"
        );
    }

    #[test]
    fn mq_config_ignores_an_empty_namespace() {
        let config = config_with_namespace(Some("  "));

        assert_eq!(config.exchange, "vigilo.events");
        assert_eq!(config.worker_queue, "vigilo.worker");
    }

    #[test]
    fn lapin_availability_errors_are_strictly_classified() {
        for error in [
            LapinError::InvalidChannel(7),
            LapinError::IOError(Arc::new(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "reset",
            ))),
            LapinError::MissingHeartbeatError,
        ] {
            assert!(Client::is_unavailable_lapin_error(&error));
        }

        assert!(!Client::is_unavailable_lapin_error(
            &LapinError::ChannelsLimitReached
        ));
    }

    #[test]
    fn broker_unavailability_requires_a_typed_lapin_cause() {
        assert!(Client::is_broker_unavailable(&broker_unavailable_error()));

        for message in [
            "invalid channel state",
            "invalid connection state",
            "IO error: reset",
            "heartbeat timed out",
            "connection closed",
            "channel closed",
        ] {
            let error = anyhow::anyhow!(message).context("worker receive failed");
            assert!(!Client::is_broker_unavailable(&error), "{message}");
        }

        let reachable = anyhow::Error::new(LapinError::ChannelsLimitReached)
            .context("rabbitmq channel creation failed");
        assert!(!Client::is_broker_unavailable(&reachable));
    }

    #[test]
    fn availability_failures_open_the_messaging_circuit() {
        let client = client_with_circuit_breaker(circuit_breaker_config());
        let now = Instant::now();
        let permit = client.acquire_broker_operation("publish", now).unwrap();

        let result = client.finish_broker_operation::<()>(
            "publish",
            permit,
            now,
            Err(broker_unavailable_error()),
        );

        assert!(result.is_err());
        let error = client.acquire_broker_operation("publish", now).unwrap_err();
        assert_eq!(
            error
                .downcast_ref::<BrokerCircuitOpen>()
                .map(|open| open.retry_after),
            Some(Duration::from_secs(10))
        );
        assert!(!Client::is_broker_unavailable(&error));
    }

    #[test]
    fn permanent_broker_failures_do_not_open_the_messaging_circuit() {
        let client = client_with_circuit_breaker(circuit_breaker_config());
        let now = Instant::now();
        let permit = client.acquire_broker_operation("publish", now).unwrap();

        let result = client.finish_broker_operation::<()>(
            "publish",
            permit,
            now,
            Err(PublishError::Nacked {
                routing_key: "run.completed".to_string(),
                reason: "rejected".to_string(),
            }
            .into()),
        );

        assert!(result.is_err());
        assert!(client.acquire_broker_operation("publish", now).is_ok());
    }

    #[test]
    fn reachable_publish_failure_closes_a_half_open_circuit() {
        let client = client_with_circuit_breaker(circuit_breaker_config());
        let opened_at = Instant::now();
        let permit = client
            .acquire_broker_operation("publish", opened_at)
            .unwrap();
        let _ = client.finish_broker_operation::<()>(
            "publish",
            permit,
            opened_at,
            Err(broker_unavailable_error()),
        );
        let probe_at = opened_at + Duration::from_secs(10);
        let probe = client
            .acquire_broker_operation("publish", probe_at)
            .unwrap();

        let result = client.finish_broker_operation::<()>(
            "publish",
            probe,
            probe_at,
            Err(PublishError::Returned {
                routing_key: "missing.route".to_string(),
                reason: "no route".to_string(),
            }
            .into()),
        );

        assert!(result.is_err());
        assert!(
            !client
                .acquire_broker_operation("publish", probe_at)
                .unwrap()
                .is_probe()
        );
    }

    #[test]
    fn successful_messaging_probe_restores_normal_admission() {
        let client = client_with_circuit_breaker(circuit_breaker_config());
        let opened_at = Instant::now();
        let permit = client
            .acquire_broker_operation("create_consumer", opened_at)
            .unwrap();
        let _ = client.finish_broker_operation::<()>(
            "create_consumer",
            permit,
            opened_at,
            Err(anyhow::Error::new(LapinError::MissingHeartbeatError)
                .context("rabbitmq consumer creation failed")),
        );
        let probe_at = opened_at + Duration::from_secs(10);

        let probe = client
            .acquire_broker_operation("create_consumer", probe_at)
            .unwrap();
        assert!(probe.is_probe());
        assert!(
            client
                .acquire_broker_operation("publish", probe_at)
                .is_err()
        );
        client
            .finish_broker_operation("create_consumer", probe, probe_at, Ok(()))
            .unwrap();

        assert!(
            !client
                .acquire_broker_operation("publish", probe_at)
                .unwrap()
                .is_probe()
        );
    }

    #[test]
    fn retry_count_handles_missing_malformed_and_negative_headers() {
        let cases = [
            (BasicProperties::default(), 0),
            (properties_with_retry_value(AMQPValue::LongInt(3)), 3),
            (properties_with_retry_value(AMQPValue::LongInt(-1)), 0),
            (
                properties_with_retry_value(AMQPValue::LongString(LongString::from("3"))),
                0,
            ),
        ];

        for (properties, expected) in cases {
            assert_eq!(Client::retry_count(&properties), expected);
        }
    }

    #[test]
    fn worker_retry_budget_stops_at_the_configured_limit() {
        let client = client();

        for retry_count in [0, WORKER_MAX_RETRIES - 1] {
            let message = raw_message(properties_with_retry_value(AMQPValue::LongInt(retry_count)));
            assert!(client.can_retry_worker_message(&message));
        }

        for retry_count in [WORKER_MAX_RETRIES, WORKER_MAX_RETRIES + 1] {
            let message = raw_message(properties_with_retry_value(AMQPValue::LongInt(retry_count)));
            assert!(!client.can_retry_worker_message(&message));
        }
    }

    #[test]
    fn retry_attempts_use_bounded_exponential_buckets() {
        let client = client();

        for (attempt, expected) in [
            (i32::MIN, "test.worker.retry.5s"),
            (0, "test.worker.retry.5s"),
            (1, "test.worker.retry.5s"),
            (2, "test.worker.retry.10s"),
            (8, "test.worker.retry.640s"),
            (i32::MAX, "test.worker.retry.640s"),
        ] {
            assert_eq!(client.retry_queue_for_attempt(attempt), expected);
        }
    }

    #[test]
    fn explicit_delays_round_up_and_clamp_to_retry_buckets() {
        let client = client();

        for (delay_seconds, expected) in [
            (i64::MIN, "test.worker.retry.5s"),
            (5, "test.worker.retry.5s"),
            (6, "test.worker.retry.10s"),
            (320, "test.worker.retry.320s"),
            (321, "test.worker.retry.640s"),
            (i64::MAX, "test.worker.retry.640s"),
        ] {
            assert_eq!(
                client.retry_queue_for_delay_seconds(delay_seconds),
                expected
            );
        }
    }

    #[test]
    fn retry_properties_preserve_identity_and_record_failure_context() {
        let first_failed_at = "2026-01-02T03:04:05Z";
        let mut headers = FieldTable::default();
        headers.insert(
            "custom".into(),
            AMQPValue::LongString(LongString::from("value")),
        );
        headers.insert(
            HEADER_FIRST_FAILED_AT.into(),
            AMQPValue::LongString(LongString::from(first_failed_at)),
        );
        let properties = BasicProperties::default()
            .with_content_type("application/json".into())
            .with_message_id("message-1".into())
            .with_headers(headers);
        let message = raw_message(properties);

        let updated = Client::retry_properties(&message, 2, "agent unavailable", "transient");

        assert_eq!(updated.content_type(), message.properties.content_type());
        assert_eq!(updated.message_id(), message.properties.message_id());
        assert_eq!(updated.delivery_mode(), &Some(2));
        assert_eq!(long_string_header(&updated, "custom"), Some("value"));
        assert_eq!(
            long_string_header(&updated, HEADER_FIRST_FAILED_AT),
            Some(first_failed_at)
        );
        assert_eq!(
            header(&updated, HEADER_RETRY_COUNT),
            Some(&AMQPValue::LongInt(2))
        );
        assert_eq!(
            long_string_header(&updated, HEADER_LAST_ERROR),
            Some("agent unavailable")
        );
        assert_eq!(
            long_string_header(&updated, HEADER_ERROR_CLASS),
            Some("transient")
        );
        assert_eq!(
            long_string_header(&updated, HEADER_ORIGINAL_EXCHANGE),
            Some("events")
        );
        assert_eq!(
            long_string_header(&updated, HEADER_ORIGINAL_ROUTING_KEY),
            Some(WORKER_ROUTING_KEY)
        );
        assert!(long_string_header(&updated, HEADER_LAST_FAILED_AT).is_some());
    }

    fn config_with_namespace(namespace: Option<&str>) -> Config {
        let _guard = ENV_LOCK.lock().unwrap();
        let original = std::env::var_os("VIGILO_MQ_NAMESPACE");
        unsafe {
            match namespace {
                Some(namespace) => std::env::set_var("VIGILO_MQ_NAMESPACE", namespace),
                None => std::env::remove_var("VIGILO_MQ_NAMESPACE"),
            }
        }

        let config = Config::new("amqp://localhost".to_string());

        unsafe {
            match original {
                Some(original) => std::env::set_var("VIGILO_MQ_NAMESPACE", original),
                None => std::env::remove_var("VIGILO_MQ_NAMESPACE"),
            }
        }
        config
    }

    fn broker_unavailable_error() -> anyhow::Error {
        anyhow::Error::new(LapinError::IOError(Arc::new(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "reset",
        ))))
        .context("rabbitmq operation failed")
    }

    fn client() -> Client {
        client_with_circuit_breaker(CircuitBreakerConfig::default())
    }

    fn circuit_breaker_config() -> CircuitBreakerConfig {
        CircuitBreakerConfig::new(true, 1, Duration::from_secs(10), Duration::from_secs(40))
            .unwrap()
            .without_jitter()
    }

    fn client_with_circuit_breaker(circuit_breaker_config: CircuitBreakerConfig) -> Client {
        Client::new(Config {
            uri: "amqp://localhost".to_string(),
            circuit_breaker_config,
            exchange: "test.events".to_string(),
            event_queue: "test.events.domain".to_string(),
            worker_queue: "test.worker".to_string(),
            worker_retry_exchange: "test.worker.retry".to_string(),
            worker_quarantine_exchange: "test.worker.quarantine".to_string(),
            worker_quarantine_queue: "test.worker.quarantine".to_string(),
        })
    }

    fn properties_with_retry_value(value: AMQPValue) -> BasicProperties {
        let mut headers = FieldTable::default();
        headers.insert(HEADER_RETRY_COUNT.into(), value);
        BasicProperties::default().with_headers(headers)
    }

    fn raw_message(properties: BasicProperties) -> RawConsumedMessage {
        RawConsumedMessage {
            delivery_tag: 1,
            acker: Acker::default(),
            body: br#"{"run_id":"run-1"}"#.to_vec(),
            properties,
            exchange: "events".to_string(),
            routing_key: WORKER_ROUTING_KEY.to_string(),
            redelivered: false,
        }
    }

    fn header<'a>(properties: &'a BasicProperties, key: &str) -> Option<&'a AMQPValue> {
        properties
            .headers()
            .as_ref()
            .and_then(|headers| headers.inner().get(key))
    }

    fn long_string_header<'a>(properties: &'a BasicProperties, key: &str) -> Option<&'a str> {
        match header(properties, key)? {
            AMQPValue::LongString(value) => std::str::from_utf8(value.as_bytes()).ok(),
            _ => None,
        }
    }
}
