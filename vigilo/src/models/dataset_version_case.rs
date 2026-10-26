//! Test-only ordered case identity fixtures.
//!
//! Golden tests independently fold case IDs, ordinals and content hashes into
//! dataset-version identity without any database membership table.

use serde::{
    Deserialize,
    Serialize,
};
use uuid::Uuid;

/// One ordered case identity in a dataset fixture.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DatasetVersionCaseDraft {
    /// Case id as it appears in the dataset contract.
    pub(crate) case_id: Uuid,
    /// Zero-based ordinal used to preserve dataset ordering.
    pub(crate) case_ordinal: i32,
    /// Normalized case content hash.
    pub(crate) case_hash: String,
}
