//! Shared typed Clap option groups and parsing helpers.
//!
//! Commands flatten only the subsystem groups they consume. This keeps
//! unrelated environment variables out of one-shot administration commands
//! while preserving one validated mapping from CLI values to runtime config.

use std::time::Duration;

use clap::Args;

use crate::{
    circuit_breaker::{
        Config as SharedCircuitBreakerConfig,
        DEFAULT_FAILURE_THRESHOLD,
        DEFAULT_INITIAL_OPEN,
        DEFAULT_MAX_OPEN,
    },
    context::{
        database::{
            CircuitBreakerConfig,
            DatabaseOperationTimeoutConfig,
        },
        wasm,
    },
    db::workflows::run_creation,
    mq,
};

pub(crate) const DEFAULT_DATABASE_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Broker connection options shared by coordinator and worker processes.
#[derive(Debug, Clone, Args)]
pub(crate) struct MessagingOptions {
    /// Messaging URL (connection string)
    #[arg(long, env = "MESSAGING_URL")]
    pub(crate) messaging_url: String,

    #[command(flatten)]
    circuit_breaker: MessagingCircuitBreakerOptions,
}

impl MessagingOptions {
    pub(crate) fn config(&self) -> anyhow::Result<mq::Config> {
        Ok(mq::Config::new(self.messaging_url.clone())
            .with_circuit_breaker(self.circuit_breaker.config()?))
    }
}

/// Process-local messaging circuit-breaker policy.
#[derive(Debug, Clone, Copy, Args)]
struct MessagingCircuitBreakerOptions {
    /// Whether the messaging circuit breaker is enabled
    #[arg(long = "messaging-circuit-breaker-enabled", env = "VIGILO_MESSAGING_CIRCUIT_BREAKER_ENABLED", default_value_t = true, action = clap::ArgAction::Set)]
    messaging_circuit_breaker_enabled: bool,

    /// Consecutive availability failures before opening the messaging circuit
    #[arg(long = "messaging-circuit-failure-threshold", env = "VIGILO_MESSAGING_CIRCUIT_FAILURE_THRESHOLD", default_value_t = DEFAULT_FAILURE_THRESHOLD, value_parser = clap::value_parser!(u32).range(1..=100))]
    messaging_circuit_failure_threshold: u32,

    /// Initial seconds the unavailable messaging circuit remains open
    #[arg(long = "messaging-circuit-initial-open-seconds", env = "VIGILO_MESSAGING_CIRCUIT_INITIAL_OPEN_SECONDS", default_value_t = DEFAULT_INITIAL_OPEN.as_secs(), value_parser = clap::value_parser!(u64).range(1..=3600))]
    messaging_circuit_initial_open_seconds: u64,

    /// Maximum seconds the unavailable messaging circuit remains open
    #[arg(long = "messaging-circuit-max-open-seconds", env = "VIGILO_MESSAGING_CIRCUIT_MAX_OPEN_SECONDS", default_value_t = DEFAULT_MAX_OPEN.as_secs(), value_parser = clap::value_parser!(u64).range(1..=86400))]
    messaging_circuit_max_open_seconds: u64,
}

impl MessagingCircuitBreakerOptions {
    fn config(self) -> anyhow::Result<SharedCircuitBreakerConfig> {
        SharedCircuitBreakerConfig::new(
            self.messaging_circuit_breaker_enabled,
            self.messaging_circuit_failure_threshold,
            Duration::from_secs(self.messaging_circuit_initial_open_seconds),
            Duration::from_secs(self.messaging_circuit_max_open_seconds),
        )
    }
}

impl Default for MessagingCircuitBreakerOptions {
    fn default() -> Self {
        Self {
            messaging_circuit_breaker_enabled: true,
            messaging_circuit_failure_threshold: DEFAULT_FAILURE_THRESHOLD,
            messaging_circuit_initial_open_seconds: DEFAULT_INITIAL_OPEN.as_secs(),
            messaging_circuit_max_open_seconds: DEFAULT_MAX_OPEN.as_secs(),
        }
    }
}

/// Process-local execution-database circuit-breaker policy.
#[derive(Debug, Clone, Copy, Args)]
pub(crate) struct CircuitBreakerOptions {
    /// Whether execution database circuit breakers are enabled
    #[arg(long = "database-circuit-breaker-enabled", env = "VIGILO_DATABASE_CIRCUIT_BREAKER_ENABLED", default_value_t = true, action = clap::ArgAction::Set)]
    enabled: bool,

    /// Consecutive availability failures before opening a database circuit
    #[arg(long = "database-circuit-failure-threshold", env = "VIGILO_DATABASE_CIRCUIT_FAILURE_THRESHOLD", default_value_t = DEFAULT_FAILURE_THRESHOLD, value_parser = clap::value_parser!(u32).range(1..=100))]
    failure_threshold: u32,

    /// Initial seconds an unavailable database circuit remains open
    #[arg(long = "database-circuit-initial-open-seconds", env = "VIGILO_DATABASE_CIRCUIT_INITIAL_OPEN_SECONDS", default_value_t = DEFAULT_INITIAL_OPEN.as_secs(), value_parser = clap::value_parser!(u64).range(1..=3600))]
    initial_open_seconds: u64,

    /// Maximum seconds an unavailable database circuit remains open
    #[arg(long = "database-circuit-max-open-seconds", env = "VIGILO_DATABASE_CIRCUIT_MAX_OPEN_SECONDS", default_value_t = DEFAULT_MAX_OPEN.as_secs(), value_parser = clap::value_parser!(u64).range(1..=86400))]
    max_open_seconds: u64,
}

impl CircuitBreakerOptions {
    pub(crate) fn config(self) -> anyhow::Result<CircuitBreakerConfig> {
        CircuitBreakerConfig::new(
            self.enabled,
            self.failure_threshold,
            Duration::from_secs(self.initial_open_seconds),
            Duration::from_secs(self.max_open_seconds),
        )
    }
}

impl Default for CircuitBreakerOptions {
    fn default() -> Self {
        Self {
            enabled: true,
            failure_threshold: DEFAULT_FAILURE_THRESHOLD,
            initial_open_seconds: DEFAULT_INITIAL_OPEN.as_secs(),
            max_open_seconds: DEFAULT_MAX_OPEN.as_secs(),
        }
    }
}

/// Deadline policy for database work that can be retried from durable state.
#[derive(Debug, Clone, Copy, Args)]
pub(crate) struct DatabaseOperationTimeoutOptions {
    /// Maximum wall-clock seconds for one runtime database operation
    #[arg(long = "database-operation-timeout-seconds", env = "VIGILO_DATABASE_OPERATION_TIMEOUT_SECONDS", default_value_t = DEFAULT_DATABASE_OPERATION_TIMEOUT.as_secs(), value_parser = clap::value_parser!(u64).range(1..=3600))]
    operation_timeout_seconds: u64,
}

impl DatabaseOperationTimeoutOptions {
    pub(crate) fn config(self) -> anyhow::Result<DatabaseOperationTimeoutConfig> {
        DatabaseOperationTimeoutConfig::new(Duration::from_secs(self.operation_timeout_seconds))
    }
}

impl Default for DatabaseOperationTimeoutOptions {
    fn default() -> Self {
        Self {
            operation_timeout_seconds: DEFAULT_DATABASE_OPERATION_TIMEOUT.as_secs(),
        }
    }
}

/// Placement selection policy used only while creating or bootstrapping runs.
#[derive(Debug, Clone, Args)]
pub(crate) struct PlacementOptions {
    /// Default shard-capable placement alias for newly created run shards
    #[arg(
        long,
        env = "VIGILO_DEFAULT_SHARD_DATABASE_ALIAS",
        default_value = "primary"
    )]
    pub(crate) default_shard_database_alias: String,

    /// Shard assignment policy for newly created runs
    #[arg(
        long,
        env = "VIGILO_SHARD_ASSIGNMENT_POLICY",
        default_value = "single-default"
    )]
    pub(crate) shard_assignment_policy: String,
}

impl Default for PlacementOptions {
    fn default() -> Self {
        Self {
            default_shard_database_alias: "primary".to_string(),
            shard_assignment_policy: "single-default".to_string(),
        }
    }
}

/// Bounded run-creation paging used by immediate creation and recovery.
#[derive(Debug, Clone, Copy, Args)]
pub(crate) struct RunCreationOptions {
    /// Maximum planned cases per chunk-metadata insert, with at least one whole chunk
    #[arg(long = "run-creation-case-batch-size", env = "VIGILO_RUN_CREATION_CASE_BATCH_SIZE", default_value_t = run_creation::DEFAULT_CASE_BATCH_SIZE as u64, value_parser = clap::value_parser!(u64).range(1..=1_000_000))]
    case_batch_size: u64,

    /// Maximum creation pages written for one placement attempt
    #[arg(long = "run-creation-case-page-budget", env = "VIGILO_RUN_CREATION_CASE_PAGE_BUDGET", default_value_t = run_creation::DEFAULT_CASE_PAGE_BUDGET as u64, value_parser = clap::value_parser!(u64).range(1..=100_000))]
    case_page_budget: u64,
}

impl RunCreationOptions {
    pub(crate) fn config(self) -> run_creation::Config {
        run_creation::Config {
            case_batch_size: self.case_batch_size as usize,
            case_page_budget: self.case_page_budget as usize,
        }
    }
}

impl Default for RunCreationOptions {
    fn default() -> Self {
        Self {
            case_batch_size: run_creation::DEFAULT_CASE_BATCH_SIZE as u64,
            case_page_budget: run_creation::DEFAULT_CASE_PAGE_BUDGET as u64,
        }
    }
}

/// Wasmtime resource and concurrency limits for evaluator execution.
#[derive(Debug, Clone, Copy, Args)]
pub(crate) struct WasmOptions {
    /// Maximum linear memory bytes per Wasm evaluator invocation
    #[arg(long = "wasm-max-memory-bytes", env = "VIGILO_WASM_MAX_MEMORY_BYTES", default_value_t = wasm::DEFAULT_MAX_MEMORY_BYTES, value_parser = clap::value_parser!(u64).range(65_536..=1_073_741_824))]
    max_memory_bytes: u64,

    /// Maximum table elements per Wasm evaluator invocation
    #[arg(long = "wasm-max-table-elements", env = "VIGILO_WASM_MAX_TABLE_ELEMENTS", default_value_t = wasm::DEFAULT_MAX_TABLE_ELEMENTS, value_parser = clap::value_parser!(u64).range(1..=10_000_000))]
    max_table_elements: u64,

    /// Maximum component instances per Wasm evaluator invocation
    #[arg(long = "wasm-max-instances", env = "VIGILO_WASM_MAX_INSTANCES", default_value_t = wasm::DEFAULT_MAX_INSTANCES, value_parser = clap::value_parser!(u64).range(1..=1024))]
    max_instances: u64,

    /// Maximum linear memories per Wasm evaluator invocation
    #[arg(long = "wasm-max-memories", env = "VIGILO_WASM_MAX_MEMORIES", default_value_t = wasm::DEFAULT_MAX_MEMORIES, value_parser = clap::value_parser!(u64).range(1..=64))]
    max_memories: u64,

    /// Maximum tables per Wasm evaluator invocation
    #[arg(long = "wasm-max-tables", env = "VIGILO_WASM_MAX_TABLES", default_value_t = wasm::DEFAULT_MAX_TABLES, value_parser = clap::value_parser!(u64).range(1..=256))]
    max_tables: u64,

    /// Fuel budget per Wasm evaluator invocation
    #[arg(long = "wasm-fuel-per-evaluation", env = "VIGILO_WASM_FUEL_PER_EVALUATION", default_value_t = wasm::DEFAULT_FUEL_PER_EVALUATION, value_parser = clap::value_parser!(u64).range(1..=10_000_000_000))]
    fuel_per_evaluation: u64,

    /// Wall-clock timeout in milliseconds per Wasm evaluator invocation
    #[arg(long = "wasm-timeout-ms", env = "VIGILO_WASM_TIMEOUT_MS", default_value_t = wasm::DEFAULT_TIMEOUT_MS, value_parser = clap::value_parser!(u64).range(1..=600_000))]
    timeout_ms: u64,

    /// Epoch ticker interval in milliseconds used for Wasm timeout traps
    #[arg(long = "wasm-epoch-tick-interval-ms", env = "VIGILO_WASM_EPOCH_TICK_INTERVAL_MS", default_value_t = wasm::DEFAULT_EPOCH_TICK_INTERVAL_MS, value_parser = clap::value_parser!(u64).range(1..=1_000))]
    epoch_tick_interval_ms: u64,

    /// Maximum active Wasm evaluator executions per process
    #[arg(long = "wasm-max-concurrent-evaluations", env = "VIGILO_WASM_MAX_CONCURRENT_EVALUATIONS", default_value_t = wasm::DEFAULT_MAX_CONCURRENT_EVALUATIONS, value_parser = clap::value_parser!(u64).range(1..=1024))]
    max_concurrent_evaluations: u64,

    /// Maximum bytes logged per evaluator host log message
    #[arg(long = "wasm-max-log-message-bytes", env = "VIGILO_WASM_MAX_LOG_MESSAGE_BYTES", default_value_t = wasm::DEFAULT_MAX_LOG_MESSAGE_BYTES, value_parser = clap::value_parser!(u64).range(1..=1_048_576))]
    max_log_message_bytes: u64,

    /// Maximum evaluator host log messages per invocation
    #[arg(long = "wasm-max-log-messages", env = "VIGILO_WASM_MAX_LOG_MESSAGES", default_value_t = wasm::DEFAULT_MAX_LOG_MESSAGES, value_parser = clap::value_parser!(u32).range(0..=100_000))]
    max_log_messages: u32,
}

impl WasmOptions {
    pub(crate) fn config(self) -> wasm::Config {
        wasm::Config {
            max_memory_bytes: self.max_memory_bytes,
            max_table_elements: self.max_table_elements,
            max_instances: self.max_instances,
            max_memories: self.max_memories,
            max_tables: self.max_tables,
            fuel_per_evaluation: self.fuel_per_evaluation,
            timeout_ms: self.timeout_ms,
            epoch_tick_interval_ms: self.epoch_tick_interval_ms,
            max_concurrent_evaluations: self.max_concurrent_evaluations,
            max_log_message_bytes: self.max_log_message_bytes,
            max_log_messages: self.max_log_messages,
        }
    }
}

impl Default for WasmOptions {
    fn default() -> Self {
        let config = wasm::Config::default();
        Self {
            max_memory_bytes: config.max_memory_bytes,
            max_table_elements: config.max_table_elements,
            max_instances: config.max_instances,
            max_memories: config.max_memories,
            max_tables: config.max_tables,
            fuel_per_evaluation: config.fuel_per_evaluation,
            timeout_ms: config.timeout_ms,
            epoch_tick_interval_ms: config.epoch_tick_interval_ms,
            max_concurrent_evaluations: config.max_concurrent_evaluations,
            max_log_message_bytes: config.max_log_message_bytes,
            max_log_messages: config.max_log_messages,
        }
    }
}

/// Parser functions intended for direct use from clap `value_parser` fields.
pub mod parsers {
    use std::path::PathBuf;

    pub(crate) fn parse_dir(s: &str) -> Result<PathBuf, String> {
        let p = PathBuf::from(s);
        if p.is_dir() {
            Ok(p)
        } else {
            Err(format!("'{}' is not a valid directory", s))
        }
    }

    pub(crate) fn parse_filepath(s: &str) -> Result<PathBuf, String> {
        let p = PathBuf::from(s);
        if p.is_file() {
            Ok(p)
        } else {
            Err(format!("'{}' is not a valid filepath", s))
        }
    }
}

/// Process-local byte ceilings for calls to agents under test.
#[derive(Debug, Clone, Copy, Args)]
pub(crate) struct AgentHttpOptions {
    /// Maximum serialized agent request body in bytes
    #[arg(long, env = "VIGILO_AGENT_MAX_REQUEST_BYTES", default_value_t = crate::agent_client::limits::DEFAULT_BODY_BYTES, value_parser = clap::value_parser!(u64).range(1..=1_073_741_824))]
    agent_max_request_bytes: u64,
    /// Maximum agent response body in bytes, including error responses
    #[arg(long, env = "VIGILO_AGENT_MAX_RESPONSE_BYTES", default_value_t = crate::agent_client::limits::DEFAULT_BODY_BYTES, value_parser = clap::value_parser!(u64).range(1..=1_073_741_824))]
    agent_max_response_bytes: u64,
}

impl AgentHttpOptions {
    pub(crate) fn config(self) -> crate::agent_client::limits::Limits {
        crate::agent_client::limits::Limits {
            max_request_bytes: self.agent_max_request_bytes,
            max_response_bytes: self.agent_max_response_bytes,
        }
    }
}
