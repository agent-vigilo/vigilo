//! Persistence model types.
//!
//! These structs mirror database rows and insert/update payloads. They are kept
//! separate from evaluator execution contracts so persistence concerns do not
//! leak into the evaluator ABI.

#![allow(dead_code)]

#[cfg(test)]
pub(crate) mod case_blob;
pub(crate) mod database_placement;
#[cfg(test)]
pub(crate) mod dataset_version_case;
pub(crate) mod evaluator;
pub(crate) mod evaluator_result;
pub(crate) mod execution;
pub(crate) mod execution_aggregate;
pub(crate) mod execution_attempt;
pub(crate) mod outbox_event;
pub(crate) mod run;
pub(crate) mod run_chunk;
pub(crate) mod run_shard_case;
pub(crate) mod shard_placement;
