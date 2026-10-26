//! Run creation planning and idempotent seed persistence helpers.
//!
//! Durable creation orchestration lives in `run_creation`; this module owns the
//! placement policy and repeatable writes used for control and execution seed
//! transactions. Dispatch owns chunk-ready event creation, so seed retries
//! cannot make work visible to workers. Bulk paths keep statement size and bind
//! counts bounded for large datasets.

use std::collections::{
    BTreeMap,
    BTreeSet,
};

use sqlx::{
    Postgres,
    QueryBuilder,
};
use uuid::Uuid;

use crate::{
    context::database::{
        self,
        ShardAssignmentPolicy,
    },
    models::{
        case_blob::CaseBlobDraft,
        database_placement::{
            DATABASE_PLACEMENT_ROLE_CONTROL_AND_SHARD,
            DATABASE_PLACEMENT_ROLE_SHARD,
        },
        dataset_version_case::DatasetVersionCaseDraft,
        run::RunDraft,
        run_chunk::RunChunkDraft,
        shard_placement::SHARD_PLACEMENT_STATUS_ACTIVE,
    },
};

mod queries;

pub(crate) use queries::{
    bulk_insert_case_blobs,
    bulk_insert_dataset_membership,
    bulk_insert_run_chunks,
    bulk_insert_run_shard_dispatch_cursors,
    bulk_insert_shard_placements,
    insert_run_create,
    upsert_dataset_version,
};

const CASE_BLOB_INSERT_CHUNK_SIZE: usize = 500;
const DATASET_MEMBERSHIP_INSERT_CHUNK_SIZE: usize = 2_000;
const RUN_CHUNK_INSERT_CHUNK_SIZE: usize = 2_000;

/// Deterministic seed data differs from a row already stored under the same id.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(crate) struct RunSeedInvariantError(String);

pub(crate) fn seed_invariant_error(message: impl Into<String>) -> anyhow::Error {
    RunSeedInvariantError(message.into()).into()
}

pub(crate) fn is_seed_invariant_error(error: &anyhow::Error) -> bool {
    error.downcast_ref::<RunSeedInvariantError>().is_some()
}

fn jsonb_text(value: &serde_json::Value) -> String {
    serde_json::to_string(value).expect("serializing serde_json::Value should not fail")
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct RunShardPlacementAssignment {
    pub(crate) run_shard: i16,
    pub(crate) database_alias: String,
}

/// Chooses initial execution placements for the run shards used by a new run.
///
/// The returned assignments are persisted to `shard_placements`; runtime
/// routing reads those stored rows instead of recomputing this policy.
pub(crate) async fn assign_run_shard_placements(
    database_router: &database::DatabaseRouter,
    chunks: &[RunChunkDraft],
) -> anyhow::Result<Vec<RunShardPlacementAssignment>> {
    let run_shards = chunks
        .iter()
        .map(|chunk| chunk.run_shard)
        .collect::<BTreeSet<_>>();

    let aliases = match database_router.shard_assignment_policy() {
        ShardAssignmentPolicy::SingleDefault => {
            vec![
                database_router
                    .default_execution_database_alias()
                    .to_string(),
            ]
        }
        ShardAssignmentPolicy::SpreadActive => {
            let mut aliases = database_router
                .active_shard_capable_database_aliases()
                .await?;
            if aliases.is_empty() {
                anyhow::bail!("no active shard-capable database placements are configured");
            }
            if let Some(default_idx) = aliases
                .iter()
                .position(|alias| alias == database_router.default_execution_database_alias())
            {
                aliases.swap(0, default_idx);
            }
            aliases
        }
    };

    Ok(assign_run_shards_to_aliases(&run_shards, &aliases))
}

pub(crate) fn assign_run_shards_to_aliases(
    run_shards: &BTreeSet<i16>,
    aliases: &[String],
) -> Vec<RunShardPlacementAssignment> {
    if aliases.is_empty() {
        return Vec::new();
    }

    run_shards
        .iter()
        .enumerate()
        .map(|(idx, run_shard)| RunShardPlacementAssignment {
            run_shard: *run_shard,
            database_alias: aliases[idx % aliases.len()].clone(),
        })
        .collect()
}

pub(crate) fn group_chunks_by_assigned_alias(
    chunks: &[RunChunkDraft],
    assignments: &[RunShardPlacementAssignment],
) -> anyhow::Result<BTreeMap<String, Vec<RunChunkDraft>>> {
    let aliases_by_shard = assignments
        .iter()
        .map(|assignment| (assignment.run_shard, assignment.database_alias.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut grouped = BTreeMap::<String, Vec<RunChunkDraft>>::new();

    for chunk in chunks {
        let Some(alias) = aliases_by_shard.get(&chunk.run_shard) else {
            anyhow::bail!(
                "missing shard placement assignment for run_shard {}",
                chunk.run_shard
            );
        };
        grouped
            .entry(alias.clone())
            .or_default()
            .push(chunk.clone());
    }

    Ok(grouped)
}

#[cfg(test)]
#[path = "run_create/postgres_tests.rs"]
mod postgres_tests;
#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn chunk(run_shard: i16, ordinal: i32) -> RunChunkDraft {
        RunChunkDraft {
            chunk_id: Uuid::now_v7(),
            run_shard,
            profile_group_id: "default".to_string(),
            ordinal_start: ordinal,
            ordinal_end: ordinal + 1,
        }
    }

    #[test]
    fn assign_run_shards_to_aliases_spreads_in_order() {
        let run_shards = [0, 1, 2, 3, 4].into_iter().collect::<BTreeSet<_>>();
        let aliases = vec!["primary".to_string(), "shard_001".to_string()];

        let assignments = assign_run_shards_to_aliases(&run_shards, &aliases);

        assert_eq!(
            assignments,
            vec![
                RunShardPlacementAssignment {
                    run_shard: 0,
                    database_alias: "primary".to_string(),
                },
                RunShardPlacementAssignment {
                    run_shard: 1,
                    database_alias: "shard_001".to_string(),
                },
                RunShardPlacementAssignment {
                    run_shard: 2,
                    database_alias: "primary".to_string(),
                },
                RunShardPlacementAssignment {
                    run_shard: 3,
                    database_alias: "shard_001".to_string(),
                },
                RunShardPlacementAssignment {
                    run_shard: 4,
                    database_alias: "primary".to_string(),
                },
            ]
        );
    }

    #[test]
    fn shard_assignment_handles_empty_inputs() {
        let shards = [0].into_iter().collect::<BTreeSet<_>>();

        assert!(assign_run_shards_to_aliases(&shards, &[]).is_empty());
        assert!(
            assign_run_shards_to_aliases(&BTreeSet::new(), &["primary".to_string()]).is_empty()
        );
    }

    #[test]
    fn chunks_are_grouped_by_shard_assignment_in_input_order() {
        let chunks = vec![chunk(1, 0), chunk(0, 1), chunk(1, 2)];
        let assignments = vec![
            RunShardPlacementAssignment {
                run_shard: 0,
                database_alias: "primary".to_string(),
            },
            RunShardPlacementAssignment {
                run_shard: 1,
                database_alias: "shard_001".to_string(),
            },
        ];

        let grouped = group_chunks_by_assigned_alias(&chunks, &assignments).unwrap();

        assert_eq!(
            grouped["shard_001"]
                .iter()
                .map(|chunk| chunk.ordinal_start)
                .collect::<Vec<_>>(),
            vec![0, 2]
        );
        assert_eq!(grouped["primary"][0].ordinal_start, 1);
    }

    #[test]
    fn chunk_grouping_rejects_an_unassigned_shard() {
        let error = group_chunks_by_assigned_alias(&[chunk(7, 0)], &[]).unwrap_err();

        assert!(error.to_string().contains("run_shard 7"));
    }

    #[test]
    fn seed_invariant_errors_are_distinguishable_from_other_failures() {
        let invariant = seed_invariant_error("immutable seed mismatch");

        assert!(is_seed_invariant_error(&invariant));
        assert!(!is_seed_invariant_error(&anyhow::anyhow!(
            "database unavailable"
        )));
        assert_eq!(invariant.to_string(), "immutable seed mismatch");
    }
}
