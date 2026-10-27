//! Independent routing, membership and settlement oracles across shard wrap.
use super::*;
use crate::{
    contracts::case_storage::identity::case_hash,
    db::workflows::{
        case_projection::projection_hash,
        run_shard_summary::chunk_plan_manifests,
    },
    models::run_chunk::RunChunkDraft,
};
fn profile() -> RunProfile {
    serde_yaml::from_str(include_str!("../../../../example/profile.yaml")).unwrap()
}
fn record(ordinal: i32) -> CaseRecord {
    let source = json!({"id":Uuid::from_u128(ordinal as u128+1),"task_type":"classification","tags":if ordinal%2==0{vec!["selected"]}else{vec![]},"input":{"text":"hello"}});
    let case = serde_json::from_value(source.clone()).unwrap();
    CaseRecord {
        case_ordinal: ordinal,
        case_hash: case_hash(&case).unwrap(),
        source,
    }
}
fn assignments(count: i16) -> Vec<RunShardPlacementAssignment> {
    (0..count)
        .map(|run_shard| RunShardPlacementAssignment {
            run_shard,
            database_alias: format!("shard_{}", run_shard % 2),
        })
        .collect()
}
#[test]
fn streamed_plan_preserves_chunk_projection_settlement_and_gate_counts_across_shard_wrap() {
    let mut profile = profile();
    profile.scorecard.gates[0].tags_all = vec!["selected".into()];
    let run = Uuid::from_u128(123);
    let version = Uuid::from_u128(456);
    let assignments = assignments(128);
    let mut accumulator = Accumulator::new(run, version, &profile, &assignments).unwrap();
    let mut chunks = Vec::new();
    for ordinal in 0..12901 {
        if let Some(chunk) = accumulator.push(&record(ordinal)).unwrap() {
            chunks.push(chunk);
        }
    }
    chunks.push(accumulator.finish_chunk().unwrap().unwrap());
    accumulator.validate_complete(12901).unwrap();
    assert_eq!(accumulator.chunk_count, 130);
    assert_eq!(accumulator.evaluator_count, 12901);
    assert_eq!(accumulator.shards.len(), 128);
    assert_eq!(accumulator.shards[&0].plan.case_count, 200);
    assert_eq!(accumulator.shards[&1].plan.case_count, 101);
    assert_eq!(accumulator.shards[&0].gates["quality_release"], 100);
    assert_eq!(accumulator.shards[&1].gates["quality_release"], 51);
    let drafts = chunks
        .iter()
        .map(|c| RunChunkDraft {
            chunk_id: c.chunk_id,
            run_shard: c.run_shard,
            profile_group_id: c.profile_group_id.clone(),
            ordinal_start: c.ordinal_start,
            ordinal_end: c.ordinal_end,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        accumulator.settlement_snapshot().unwrap(),
        chunk_plan_manifests(&drafts).unwrap()
    );
    for chunk in &chunks {
        let rows = (chunk.ordinal_start..chunk.ordinal_end)
            .map(|ordinal| {
                let record = record(ordinal);
                RunShardCaseDraft {
                    run_id: run,
                    run_shard: chunk.run_shard,
                    dataset_version_id: version,
                    case_id: Uuid::from_u128(ordinal as u128 + 1),
                    case_ordinal: ordinal,
                    case_hash: record.case_hash,
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(chunk.case_projection_hash, projection_hash(&rows));
    }
    let mut replay = Accumulator::new(run, version, &profile, &assignments).unwrap();
    let mut first = None;
    for ordinal in 0..100 {
        first = replay.push(&record(ordinal)).unwrap().or(first);
    }
    assert_eq!(first.unwrap(), chunks[0]);
}
#[test]
fn planning_rejects_invalid_membership_routing_placement_and_empty_gate_coverage() {
    let profile = profile();
    let a = assignments(1);
    let make = || Accumulator::new(Uuid::nil(), Uuid::nil(), &profile, &a).unwrap();
    let mut gap = make();
    assert!(gap.push(&record(1)).is_err());
    let mut corrupt = record(0);
    corrupt.case_hash = "f".repeat(64);
    assert!(make().push(&corrupt).is_err());
    let mut unmatched = record(0);
    unmatched.source["task_type"] = json!("unknown");
    unmatched.case_hash =
        case_hash(&serde_json::from_value(unmatched.source.clone()).unwrap()).unwrap();
    assert!(make().push(&unmatched).is_err());
    let mut incomplete = make();
    incomplete.push(&record(0)).unwrap();
    assert!(incomplete.validate_complete(1).is_err());
    incomplete.finish_chunk().unwrap();
    incomplete.validate_complete(1).unwrap();
    assert!(incomplete.validate_complete(2).is_err());
    assert!(
        Accumulator::new(
            Uuid::nil(),
            Uuid::nil(),
            &profile,
            &[a[0].clone(), a[0].clone()]
        )
        .is_err()
    );
    let mut extra = Accumulator::new(Uuid::nil(), Uuid::nil(), &profile, &assignments(2)).unwrap();
    extra.push(&record(0)).unwrap();
    extra.finish_chunk().unwrap();
    assert!(extra.validate_complete(1).is_err());
    let mut oversized = profile.clone();
    oversized.scorecard.gates[0].id = "g".repeat(1025);
    assert!(Accumulator::new(Uuid::nil(), Uuid::nil(), &oversized, &a).is_err());
    let mut gated = profile.clone();
    gated.scorecard.gates[0].tags_all = vec!["absent".into()];
    let mut empty = Accumulator::new(Uuid::nil(), Uuid::nil(), &gated, &a).unwrap();
    empty.push(&record(0)).unwrap();
    empty.finish_chunk().unwrap();
    assert!(empty.validate_complete(1).is_err());
}
