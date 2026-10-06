//! Multi-table database workflows.
//!
//! Workflow modules own operations where table state must move together:
//! creating runs, leasing work, processing executions, dispatching events, and
//! finalizing aggregate run status. These functions are the place for
//! concurrency guards such as leases, attempt authority checks, and idempotent
//! outbox writes.
//!
//! Reading guide:
//! - `run_create` plans shard assignments and provides idempotent seed writes.
//! - `run_creation` persists and recovers the cross-database creation state
//!   machine before dispatch cursors make a run visible.
//! - `run_dispatch` starts dispatchable runs, prepares execution-local run
//!   snapshots, and owns chunk visibility plus expired chunk lease recovery.
//! - `run_shard_summary` settles chunks and seals resumable rollups used by later
//!   global finalization.
//! - `run_status` combines the control run row with routed shard-summary
//!   progress for status/watch reads.
//! - `run_results` combines routed shard summaries for run-level reporting.
//! - `run_export` pages routed execution artifacts for run export.
//! - `shard_admin` owns placement catalog guardrails for admin commands.
//! - `chunk_processing` is worker-owned chunk lease claim/release/completion.
//! - `execution_processing` allocates case attempts, persists evaluator
//!   evidence, schedules retries, and applies current-attempt guarded terminal
//!   transitions.
//! - `run_finalize` combines routed shard summaries into one completed run.
//! - `run_cancel` closes open work and emits an idempotent cancellation event.
//! - `run_profile_validation` verifies profile/dataset/evaluator compatibility
//!   before durable run work is created.

pub(crate) mod case_projection;
pub(crate) mod chunk_processing;
pub(crate) mod execution_processing;
pub(crate) mod local_shard_admission;
pub(crate) mod run_cancel;
pub(crate) mod run_create;
pub(crate) mod run_creation;
pub(crate) mod run_dispatch;
pub(crate) mod run_export;
pub(crate) mod run_finalize;
pub(crate) mod run_profile_validation;
pub(crate) mod run_results;
pub(crate) mod run_shard_summary;
pub(crate) mod run_status;
pub(crate) mod shard_admin;
