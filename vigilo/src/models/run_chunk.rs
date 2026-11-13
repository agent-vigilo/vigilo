//! Run chunk persistence models.
//!
//! Run chunks are scheduling units for converting dataset cases into
//! executions. They keep worker/coordinator passes bounded by grouping an
//! ordinal range of dataset cases for one profile group.

use chrono::{
    DateTime,
    Utc,
};
use serde::{
    Deserialize,
    Serialize,
};
use uuid::Uuid;

pub(crate) const RUN_SHARD_COUNT: i16 = 128;

pub(crate) fn run_shard_for_chunk_index(chunk_index: usize) -> i16 {
    (chunk_index % RUN_SHARD_COUNT as usize) as i16
}

/// Insert payload for one run scheduling chunk.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub(crate) struct RunChunkDraft {
    /// Deterministic chunk id generated during run planning.
    pub(crate) chunk_id: Uuid,
    /// Logical shard assigned to this chunk.
    pub(crate) run_shard: i16,
    /// Chunk scheduling label; per-case profile routing is resolved at runtime.
    pub(crate) profile_group_id: String,
    /// Inclusive start ordinal in the dataset version.
    pub(crate) ordinal_start: i32,
    /// Exclusive end ordinal in the dataset version.
    pub(crate) ordinal_end: i32,
}

/// Persisted run scheduling chunk.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub(crate) struct RunChunk {
    /// Persisted owner validated by the execution-local claim.
    pub(crate) tenant_id: Uuid,
    /// Control observed in the existing claim or heartbeat statement.
    pub(crate) execution_control: crate::operations::ExecutionControl,
    /// Route generation admitted for this in-memory lease; not persisted on the chunk row.
    #[serde(skip, default)]
    #[sqlx(default)]
    pub(crate) write_epoch: i64,
    /// Chunk id, unique within the run and shard key.
    pub(crate) id: Uuid,
    /// Parent run id.
    pub(crate) run_id: Uuid,
    /// Logical shard for chunk-local worker processing.
    pub(crate) run_shard: i16,
    /// Dataset version whose cases are covered by this chunk.
    pub(crate) dataset_version_id: Uuid,
    /// Chunk scheduling label; per-case profile routing is resolved at runtime.
    pub(crate) profile_group_id: String,
    /// Inclusive start ordinal in the dataset version.
    pub(crate) ordinal_start: i32,
    /// Exclusive end ordinal in the dataset version.
    pub(crate) ordinal_end: i32,
    /// Current chunk scheduling status.
    pub(crate) status: String,
    /// Stable ownership token for the current lease generation.
    pub(crate) lease_token: Option<Uuid>,
    /// Worker/coordinator lease expiration, when leased.
    pub(crate) leased_until: Option<DateTime<Utc>>,
    /// Time this chunk row was inserted.
    pub(crate) created_at: DateTime<Utc>,
    /// Time this chunk row was last updated.
    pub(crate) updated_at: DateTime<Utc>,
}
