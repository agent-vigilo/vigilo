//! Provider-neutral case-storage contracts for immutable datasets and run plans.
//!
//! This boundary owns bounded encodings, byte integrity, logical identity, and
//! local structural validation. Callers encode complete objects before upload,
//! verify references to obtain immutable byte proofs before decoding, validate
//! decoded content explicitly, and validate parent/child links before
//! publication. No provider, database, queue, or workflow policy belongs here.
//! Whole-dataset uniqueness, profile executability, and publication fencing are
//! ingestion responsibilities; local validation never implies a finalized run.

// Case-storage contracts are exercised independently until ingestion consumes them.
#![allow(dead_code)]

// Explicit paths also let the opt-in service target reuse these binary contracts.
#[path = "case_storage/codec.rs"]
mod codec;
#[path = "case_storage/formats.rs"]
mod formats;
#[path = "case_storage/identity.rs"]
pub(crate) mod identity;
#[path = "case_storage/limits.rs"]
mod limits;

#[allow(unused_imports)]
pub(crate) use codec::{
    CaseBatchBuilder,
    DecodedCaseBatch,
    decode,
    decode_case_batch,
    encode,
    encode_batch,
};
pub(crate) use formats::*;
pub(crate) use limits::{
    BatchLimits,
    FormatLimits,
};

#[cfg(test)]
#[path = "case_storage/tests.rs"]
mod tests;
