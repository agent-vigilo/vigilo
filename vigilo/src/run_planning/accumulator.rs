//! Constant-size planning state: one current chunk and at most 128 shard folds,
//! each with the profile's at most 64 gate denominators. No case lists survive.
use std::collections::{
    BTreeMap,
    BTreeSet,
};

use anyhow::{
    Result,
    ensure,
};
use serde_json::json;
use uuid::Uuid;

use crate::{
    contracts::{
        case_storage::{
            CaseRecord,
            RunChunk,
            identity::uuid_from_json,
        },
        run::RunProfile,
    },
    db::workflows::{
        case_projection::update_projection_hash,
        run_create::RunShardPlacementAssignment,
        run_profile_validation::{
            case_matches_gate,
            validate_resolved_case,
        },
        run_shard_summary::plan::{
            ChunkIdentity,
            Manifest,
        },
    },
    models::{
        run_chunk::run_shard_for_chunk_index,
        run_shard_case::RunShardCaseDraft,
    },
};
pub(super) struct ShardState {
    pub plan: Manifest,
    pub gates: BTreeMap<String, i32>,
    pub projection: blake3::Hasher,
    pub alias: String,
}
pub(super) struct Accumulator<'a> {
    run_id: Uuid,
    version: Uuid,
    profile: &'a RunProfile,
    aliases: BTreeMap<i16, String>,
    pub shards: BTreeMap<i16, ShardState>,
    pub chunk_count: i32,
    pub evaluator_count: u64,
    ordinal: i32,
    chunk_hash: blake3::Hasher,
    chunk_start: i32,
}
fn projection() -> blake3::Hasher {
    let mut hash = blake3::Hasher::new();
    hash.update(b"vigilo/run-shard-cases/v1");
    hash
}
impl<'a> Accumulator<'a> {
    pub(super) fn new(
        run_id: Uuid,
        version: Uuid,
        profile: &'a RunProfile,
        assignments: &[RunShardPlacementAssignment],
    ) -> Result<Self> {
        ensure!(
            profile.scorecard.gates.len() <= 64,
            "too many scorecard gates"
        );
        ensure!(
            profile
                .scorecard
                .gates
                .iter()
                .all(|gate| !gate.id.is_empty() && gate.id.len() <= 1024)
                && profile
                    .scorecard
                    .gates
                    .iter()
                    .map(|gate| &gate.id)
                    .collect::<BTreeSet<_>>()
                    .len()
                    == profile.scorecard.gates.len(),
            "gate IDs must be unique and contain 1..=1024 bytes"
        );
        let aliases = assignments
            .iter()
            .map(|a| (a.run_shard, a.database_alias.clone()))
            .collect::<BTreeMap<_, _>>();
        ensure!(
            aliases.len() == assignments.len()
                && !aliases.is_empty()
                && aliases.len() <= 128
                && aliases.keys().all(|s| (0..128).contains(s)),
            "invalid or duplicate shard assignments"
        );
        Ok(Self {
            run_id,
            version,
            profile,
            aliases,
            shards: BTreeMap::new(),
            chunk_count: 0,
            evaluator_count: 0,
            ordinal: 0,
            chunk_hash: projection(),
            chunk_start: 0,
        })
    }
    pub(super) fn push(&mut self, record: &CaseRecord) -> Result<Option<RunChunk>> {
        ensure!(
            record.case_ordinal == self.ordinal,
            "case ordinal gap or overlap"
        );
        let case = record.validate()?;
        let count = validate_resolved_case(self.profile, &case)?;
        self.evaluator_count = self
            .evaluator_count
            .checked_add(u64::try_from(count)?)
            .ok_or_else(|| anyhow::anyhow!("evaluator count overflow"))?;
        let shard = run_shard_for_chunk_index(usize::try_from(self.chunk_count)?);
        let alias = self
            .aliases
            .get(&shard)
            .ok_or_else(|| anyhow::anyhow!("missing placement for shard {shard}"))?
            .clone();
        let state = self.shards.entry(shard).or_insert_with(|| ShardState {
            plan: Manifest::default(),
            gates: self
                .profile
                .scorecard
                .gates
                .iter()
                .map(|g| (g.id.clone(), 0))
                .collect(),
            projection: projection(),
            alias,
        });
        let row = RunShardCaseDraft {
            run_id: self.run_id,
            run_shard: shard,
            dataset_version_id: self.version,
            case_id: case.id,
            case_ordinal: self.ordinal,
            case_hash: record.case_hash.clone(),
        };
        update_projection_hash(&mut self.chunk_hash, &row);
        update_projection_hash(&mut state.projection, &row);
        for gate in &self.profile.scorecard.gates {
            if case_matches_gate(self.profile, &case, gate) {
                let count = state.gates.get_mut(&gate.id).expect("configured gate");
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("gate count overflow"))?;
            }
        }
        self.ordinal = self
            .ordinal
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("case count overflow"))?;
        if self.ordinal - self.chunk_start == 100 {
            self.finish_chunk()
        } else {
            Ok(None)
        }
    }
    pub(super) fn finish_chunk(&mut self) -> Result<Option<RunChunk>> {
        if self.ordinal == self.chunk_start {
            return Ok(None);
        }
        let shard = run_shard_for_chunk_index(usize::try_from(self.chunk_count)?);
        let id = uuid_from_json(&json!({"run_id":self.run_id,"chunk_index":self.chunk_count}))?;
        let chunk = RunChunk {
            chunk_index: self.chunk_count,
            chunk_id: id,
            run_shard: shard,
            ordinal_start: self.chunk_start,
            ordinal_end: self.ordinal,
            profile_group_id: "default".into(),
            case_projection_hash: self.chunk_hash.finalize().to_hex().to_string(),
        };
        self.shards
            .get_mut(&shard)
            .expect("case creates shard")
            .plan
            .push(&ChunkIdentity {
                chunk_id: id,
                run_shard: shard,
                ordinal_start: self.chunk_start,
                ordinal_end: self.ordinal,
            })?;
        self.chunk_count = self
            .chunk_count
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("chunk count overflow"))?;
        self.chunk_start = self.ordinal;
        self.chunk_hash = projection();
        Ok(Some(chunk))
    }
    pub(super) fn validate_complete(&self, count: i32) -> Result<()> {
        ensure!(
            count > 0 && self.ordinal == count && self.chunk_start == count,
            "incomplete case planning"
        );
        ensure!(
            self.aliases.keys().copied().collect::<BTreeSet<_>>()
                == self.shards.keys().copied().collect(),
            "placement coverage differs from planned shards"
        );
        for gate in &self.profile.scorecard.gates {
            ensure!(
                self.shards
                    .values()
                    .map(|s| i64::from(s.gates[&gate.id]))
                    .sum::<i64>()
                    > 0,
                "scorecard gate '{}' does not match any dataset cases",
                gate.id
            );
        }
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn settlement_snapshot(&self) -> Result<serde_json::Value> {
        Ok(
            json!({"version":1,"shards":self.shards.iter().map(|(s,state)|(s.to_string(),&state.plan)).collect::<BTreeMap<_,_>>()}),
        )
    }
}
#[cfg(test)]
mod tests;
