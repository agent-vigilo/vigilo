//! Binary contract modules reused by the opt-in storage acceptance target.
//! Explicit source paths keep runtime internals out of the public library API.

#[path = "../../src/contracts/aggregation.rs"]
pub(crate) mod aggregation;
#[path = "../../src/contracts/case_storage.rs"]
pub(crate) mod case_storage;
#[path = "../../src/contracts/evaluator.rs"]
pub(crate) mod evaluator;
#[path = "../../src/contracts/evaluator_abi.rs"]
pub(crate) mod evaluator_abi;
#[path = "../../src/contracts/evaluator_ref.rs"]
pub(crate) mod evaluator_ref;
#[path = "../../src/contracts/normalization.rs"]
pub(crate) mod normalization;
#[path = "../../src/contracts/run.rs"]
pub(crate) mod run;
#[path = "../../src/contracts/scorecard.rs"]
pub(crate) mod scorecard;
