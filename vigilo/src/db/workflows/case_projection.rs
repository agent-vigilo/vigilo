//! Canonical case-assignment hashing shared by planning and worker verification.
//!
//! Production folds one transient case identity at a time. Test-only projection
//! fixtures independently check exact coverage, order and stable hash encoding;
//! no per-case assignment is persisted in SQL.

#[cfg(test)]
use std::collections::{
    BTreeMap,
    BTreeSet,
};

#[cfg(test)]
use uuid::Uuid;

use crate::models::run_shard_case::RunShardCaseDraft;
#[cfg(test)]
use crate::models::{
    dataset_version_case::DatasetVersionCaseDraft,
    run_chunk::RunChunkDraft,
};

#[cfg(test)]
pub(crate) fn project_cases_for_chunks(
    run_id: Uuid,
    dataset_version_id: Uuid,
    dataset_cases: &[DatasetVersionCaseDraft],
    chunks: &[RunChunkDraft],
) -> anyhow::Result<Vec<RunShardCaseDraft>> {
    let cases_by_ordinal = dataset_cases
        .iter()
        .map(|case| (case.case_ordinal, case))
        .collect::<BTreeMap<_, _>>();
    if cases_by_ordinal.len() != dataset_cases.len() {
        anyhow::bail!("canonical dataset contains duplicate case ordinals");
    }

    let mut claimed_ordinals = BTreeSet::new();
    let mut projection = Vec::new();
    for chunk in chunks {
        if chunk.ordinal_start < 0 || chunk.ordinal_end <= chunk.ordinal_start {
            anyhow::bail!(
                "chunk {} has invalid ordinal range [{}, {})",
                chunk.chunk_id,
                chunk.ordinal_start,
                chunk.ordinal_end
            );
        }
        for ordinal in chunk.ordinal_start..chunk.ordinal_end {
            if !claimed_ordinals.insert(ordinal) {
                anyhow::bail!("case ordinal {ordinal} is assigned to multiple chunks");
            }
            let case = cases_by_ordinal.get(&ordinal).ok_or_else(|| {
                anyhow::anyhow!(
                    "chunk {} references missing canonical case ordinal {ordinal}",
                    chunk.chunk_id
                )
            })?;
            projection.push(RunShardCaseDraft {
                run_id,
                run_shard: chunk.run_shard,
                dataset_version_id,
                case_id: case.case_id,
                case_ordinal: ordinal,
                case_hash: case.case_hash.clone(),
            });
        }
    }
    projection.sort_by_key(|row| (row.case_ordinal, row.case_id));
    Ok(projection)
}

/// Hashes immutable projection identity using a versioned binary encoding.
#[cfg(test)]
pub(crate) fn projection_hash(rows: &[RunShardCaseDraft]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"vigilo/run-shard-cases/v1");
    for row in rows {
        update_projection_hash(&mut hasher, row);
    }
    hasher.finalize().to_hex().to_string()
}

pub(crate) fn update_projection_hash(hasher: &mut blake3::Hasher, row: &RunShardCaseDraft) {
    hasher.update(&row.run_shard.to_be_bytes());
    hasher.update(&row.case_ordinal.to_be_bytes());
    hasher.update(row.case_id.as_bytes());
    let hash_bytes = row.case_hash.as_bytes();
    hasher.update(&(hash_bytes.len() as u64).to_be_bytes());
    hasher.update(hash_bytes);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dataset_case(ordinal: i32) -> DatasetVersionCaseDraft {
        DatasetVersionCaseDraft {
            case_id: Uuid::from_u128(ordinal as u128 + 1),
            case_ordinal: ordinal,
            case_hash: format!("hash-{ordinal}"),
        }
    }

    fn chunk(run_shard: i16, start: i32, end: i32) -> RunChunkDraft {
        RunChunkDraft {
            chunk_id: Uuid::now_v7(),
            run_shard,
            profile_group_id: "default".to_string(),
            ordinal_start: start,
            ordinal_end: end,
        }
    }

    #[test]
    fn projection_contains_only_cases_owned_by_the_placement_chunks() {
        let run_id = Uuid::now_v7();
        let dataset_version_id = Uuid::now_v7();
        let cases = (0..8).map(dataset_case).collect::<Vec<_>>();
        let chunks = vec![chunk(3, 0, 2), chunk(7, 5, 8)];

        let projected =
            project_cases_for_chunks(run_id, dataset_version_id, &cases, &chunks).unwrap();

        assert_eq!(
            projected
                .iter()
                .map(|row| (row.run_shard, row.case_ordinal))
                .collect::<Vec<_>>(),
            vec![(3, 0), (3, 1), (7, 5), (7, 6), (7, 7)]
        );
        assert!(
            projected
                .iter()
                .all(|row| row.run_id == run_id && row.dataset_version_id == dataset_version_id)
        );
    }

    #[test]
    fn projection_rejects_overlapping_chunk_ranges() {
        let error = project_cases_for_chunks(
            Uuid::now_v7(),
            Uuid::now_v7(),
            &(0..5).map(dataset_case).collect::<Vec<_>>(),
            &[chunk(1, 0, 3), chunk(2, 2, 5)],
        )
        .unwrap_err();

        assert!(error.to_string().contains("ordinal 2"));
    }

    #[test]
    fn projection_hash_is_stable_and_order_sensitive() {
        let run_id = Uuid::from_u128(10);
        let dataset_version_id = Uuid::from_u128(20);
        let cases = (0..3).map(dataset_case).collect::<Vec<_>>();
        let projection =
            project_cases_for_chunks(run_id, dataset_version_id, &cases, &[chunk(4, 0, 3)])
                .unwrap();
        let mut reversed = projection.clone();
        reversed.reverse();

        assert_eq!(projection_hash(&projection), projection_hash(&projection));
        assert_ne!(projection_hash(&projection), projection_hash(&reversed));
    }
}
