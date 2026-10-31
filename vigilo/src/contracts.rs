//! Runtime contract modules.
//!
//! Contracts describe payloads and policies that cross execution boundaries:
//! run profiles/datasets, evaluator inputs/outputs, evaluator references, and
//! aggregation behavior. Treat versioned contracts under `vigilo/wit/evaluator/` as
//! evaluator ABI sources of truth and keep host-side execution contracts here
//! rather than in persistence models.
//!
//! Guidelines:
//! - keep evaluator vocabulary as `input` and `output`
//! - preserve fully qualified evaluator ids as `<namespace>/<name>:<version>`
//! - update host mappings, evaluator examples, and docs together when WIT
//!   shapes change

pub(crate) mod aggregation;
pub(crate) mod case_storage;
pub(crate) mod evaluator;
pub(crate) mod evaluator_abi;
pub(crate) mod evaluator_ref;
pub(crate) mod normalization;
pub(crate) mod run;
pub(crate) mod scorecard;
