//! Immutable planned chunk membership and resumable canonical hash folding.
//!
//! Creation records one bounded manifest per shard before deleting its temporary
//! chunk plan. Coordinators fold the same ordered identities with their receipts;
//! equality proves membership, rather than merely comparing execution counts.

#[cfg(test)]
use std::collections::BTreeMap;

use serde::{
    Deserialize,
    Serialize,
};
use uuid::Uuid;

#[cfg(test)]
use crate::models::run_chunk::RunChunkDraft;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
pub(crate) struct ChunkIdentity {
    pub(crate) chunk_id: Uuid,
    pub(crate) run_shard: i16,
    pub(crate) ordinal_start: i32,
    pub(crate) ordinal_end: i32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Manifest {
    pub(crate) chunk_count: i32,
    pub(crate) case_count: i32,
    pub(crate) hash: String,
    pub(crate) last: Option<ChunkIdentity>,
}

impl Manifest {
    pub(crate) fn push(&mut self, chunk: &ChunkIdentity) -> anyhow::Result<()> {
        anyhow::ensure!(
            chunk.ordinal_start >= 0 && chunk.ordinal_end > chunk.ordinal_start,
            "invalid planned chunk range"
        );
        if let Some(last) = &self.last {
            anyhow::ensure!(
                last.run_shard == chunk.run_shard && last.ordinal_end <= chunk.ordinal_start,
                "overlapping or unordered planned chunks"
            );
        }
        self.chunk_count = self
            .chunk_count
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("planned chunk count overflow"))?;
        self.case_count = self
            .case_count
            .checked_add(chunk.ordinal_end - chunk.ordinal_start)
            .ok_or_else(|| anyhow::anyhow!("planned case count overflow"))?;
        self.hash = blake3::hash(&serde_json::to_vec(&(
            "vigilo/chunk-plan:1",
            &self.hash,
            chunk,
        ))?)
        .to_hex()
        .to_string();
        self.last = Some(chunk.clone());
        Ok(())
    }
}

/// Freezes each shard's exact ordered chunk membership into the run snapshot.
#[cfg(test)]
pub(crate) fn manifests(chunks: &[RunChunkDraft]) -> anyhow::Result<serde_json::Value> {
    let mut ordered = chunks.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|chunk| (chunk.run_shard, chunk.ordinal_start, chunk.chunk_id));
    let mut plans = BTreeMap::<i16, Manifest>::new();
    for chunk in ordered {
        plans
            .entry(chunk.run_shard)
            .or_default()
            .push(&ChunkIdentity {
                chunk_id: chunk.chunk_id,
                run_shard: chunk.run_shard,
                ordinal_start: chunk.ordinal_start,
                ordinal_end: chunk.ordinal_end,
            })?;
    }
    Ok(serde_json::json!({"version": 1, "shards": plans}))
}

pub(crate) fn snapshot_manifest(
    snapshot: &serde_json::Value,
    shard: i16,
) -> anyhow::Result<Manifest> {
    let plan = snapshot
        .get("settlement_plan")
        .ok_or_else(|| anyhow::anyhow!("missing settlement plan for shard {shard}"))?;
    anyhow::ensure!(plan["version"] == 1, "unsupported chunk plan version");
    let value = plan["shards"]
        .get(shard.to_string())
        .ok_or_else(|| anyhow::anyhow!("chunk plan omits shard {shard}"))?;
    Ok(serde_json::from_value(value.clone())?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(id: u128, start: i32, end: i32) -> ChunkIdentity {
        ChunkIdentity {
            chunk_id: Uuid::from_u128(id),
            run_shard: 3,
            ordinal_start: start,
            ordinal_end: end,
        }
    }

    #[test]
    fn manifest_restarts_exactly_and_distinguishes_equal_counts() {
        let mut expected = Manifest::default();
        expected.push(&chunk(1, 0, 2)).unwrap();
        let mut resumed: Manifest =
            serde_json::from_value(serde_json::to_value(&expected).unwrap()).unwrap();
        expected.push(&chunk(2, 100, 102)).unwrap();
        resumed.push(&chunk(2, 100, 102)).unwrap();
        assert_eq!(resumed, expected);
        let mut different = Manifest::default();
        different.push(&chunk(1, 0, 2)).unwrap();
        different.push(&chunk(3, 100, 102)).unwrap();
        assert_eq!(different.case_count, expected.case_count);
        assert_ne!(different.hash, expected.hash);
    }

    #[test]
    fn manifest_rejects_duplicates_overlap_wrong_shard_and_overflow() {
        for invalid in [
            chunk(1, 0, 2),
            chunk(2, 1, 3),
            chunk(2, 2, 2),
            ChunkIdentity {
                run_shard: 4,
                ..chunk(2, 2, 3)
            },
        ] {
            let mut manifest = Manifest::default();
            manifest.push(&chunk(1, 0, 2)).unwrap();
            assert!(manifest.push(&invalid).is_err());
        }
        let mut manifest = Manifest {
            case_count: i32::MAX,
            ..Default::default()
        };
        assert!(manifest.push(&chunk(1, 0, 1)).is_err());
    }
}
