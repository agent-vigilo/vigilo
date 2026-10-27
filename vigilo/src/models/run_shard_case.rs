//! Logical case assignment identity used by canonical membership hashing.
//!
//! Planning and workers fold the same fields to prove exact chunk membership.
//! These values are transient and are never persisted as per-case SQL rows.

use serde::{
    Deserialize,
    Serialize,
};
use uuid::Uuid;

/// One transient case assignment in a canonical membership fold.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RunShardCaseDraft {
    pub(crate) run_id: Uuid,
    pub(crate) run_shard: i16,
    pub(crate) dataset_version_id: Uuid,
    pub(crate) case_id: Uuid,
    pub(crate) case_ordinal: i32,
    pub(crate) case_hash: String,
}
