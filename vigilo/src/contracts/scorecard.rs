//! Deterministic run-level scorecard merging and gate evaluation.

use std::collections::BTreeMap;

use serde::{
    Deserialize,
    Serialize,
};

const SCORECARD_VERSION: u16 = 1;

/// Mergeable statistics for one configured run-level gate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct ScorecardEntry {
    pub(crate) id: String,
    pub(crate) dimension: String,
    pub(crate) binding_id: Option<String>,
    pub(crate) case_group: Option<String>,
    pub(crate) tags_all: Vec<String>,
    pub(crate) min_mean_score: Option<f64>,
    pub(crate) score_threshold: Option<f64>,
    pub(crate) min_pass_rate: Option<f64>,
    pub(crate) min_coverage: Option<f64>,
    pub(crate) max_error_rate: Option<f64>,
    pub(crate) max_abstention_rate: Option<f64>,
    pub(crate) expected_count: i64,
    pub(crate) scored_count: i64,
    pub(crate) passed_count: i64,
    pub(crate) error_count: i64,
    pub(crate) abstained_count: i64,
    pub(crate) score_sum: f64,
    pub(crate) min_score: Option<f64>,
    pub(crate) max_score: Option<f64>,
}

/// Shard-local scorecard transported to the coordinator.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct ShardScorecard {
    pub(crate) version: u16,
    pub(crate) run_shard: i16,
    pub(crate) policy_hash: String,
    pub(crate) entries: Vec<ScorecardEntry>,
}

/// Result of applying one configured gate to merged statistics.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ScorecardGateResult {
    pub(crate) id: String,
    pub(crate) dimension: String,
    pub(crate) binding_id: Option<String>,
    pub(crate) case_group: Option<String>,
    pub(crate) tags_all: Vec<String>,
    pub(crate) passed: bool,
    pub(crate) failures: Vec<String>,
    pub(crate) expected_count: i64,
    pub(crate) scored_count: i64,
    pub(crate) passed_count: i64,
    pub(crate) error_count: i64,
    pub(crate) abstained_count: i64,
    pub(crate) mean_score: Option<f64>,
    pub(crate) min_score: Option<f64>,
    pub(crate) max_score: Option<f64>,
    pub(crate) coverage: Option<f64>,
    pub(crate) pass_rate: Option<f64>,
    pub(crate) error_rate: Option<f64>,
    pub(crate) abstention_rate: Option<f64>,
}

/// Authoritative run-level scorecard persisted during finalization.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct RunScorecard {
    pub(crate) version: u16,
    pub(crate) policy_hash: String,
    pub(crate) shard_count: usize,
    pub(crate) passed: bool,
    pub(crate) gates: Vec<ScorecardGateResult>,
}

fn checked_rate(numerator: i64, denominator: i64) -> Option<f64> {
    (denominator > 0).then_some(numerator as f64 / denominator as f64)
}

fn validate_entry(entry: &ScorecardEntry) -> Result<(), String> {
    if entry.id.trim().is_empty() || entry.dimension.trim().is_empty() {
        return Err("scorecard gate id and dimension must not be empty".to_string());
    }
    let thresholds = [
        entry.min_mean_score,
        entry.score_threshold,
        entry.min_pass_rate,
        entry.min_coverage,
        entry.max_error_rate,
        entry.max_abstention_rate,
    ];
    if thresholds
        .into_iter()
        .flatten()
        .any(|value| !value.is_finite() || !(0.0..=1.0).contains(&value))
        || entry.score_threshold.is_some() != entry.min_pass_rate.is_some()
        || thresholds.into_iter().all(|value| value.is_none())
    {
        return Err(format!(
            "scorecard gate '{}' has invalid policy thresholds",
            entry.id
        ));
    }
    let counts = [
        entry.expected_count,
        entry.scored_count,
        entry.passed_count,
        entry.error_count,
        entry.abstained_count,
    ];
    if counts.iter().any(|count| *count < 0)
        || entry.scored_count > entry.expected_count
        || entry.passed_count > entry.scored_count
        || entry.error_count > entry.expected_count
        || entry.abstained_count > entry.expected_count
    {
        return Err(format!(
            "scorecard gate '{}' has invalid counters",
            entry.id
        ));
    }
    if !entry.score_sum.is_finite()
        || entry.score_sum < 0.0
        || entry.score_sum > entry.scored_count as f64 + f64::EPSILON
        || entry
            .min_score
            .into_iter()
            .chain(entry.max_score)
            .any(|score| !score.is_finite() || !(0.0..=1.0).contains(&score))
    {
        return Err(format!("scorecard gate '{}' has invalid scores", entry.id));
    }
    if (entry.scored_count == 0) != entry.min_score.is_none()
        || (entry.scored_count == 0) != entry.max_score.is_none()
    {
        return Err(format!(
            "scorecard gate '{}' score extrema do not match scored_count",
            entry.id
        ));
    }
    if entry
        .min_score
        .zip(entry.max_score)
        .is_some_and(|(min, max)| min > max)
    {
        return Err(format!(
            "scorecard gate '{}' minimum score exceeds its maximum",
            entry.id
        ));
    }
    Ok(())
}

fn same_policy(left: &ScorecardEntry, right: &ScorecardEntry) -> bool {
    left.id == right.id
        && left.dimension == right.dimension
        && left.binding_id == right.binding_id
        && left.case_group == right.case_group
        && left.tags_all == right.tags_all
        && left.min_mean_score == right.min_mean_score
        && left.score_threshold == right.score_threshold
        && left.min_pass_rate == right.min_pass_rate
        && left.min_coverage == right.min_coverage
        && left.max_error_rate == right.max_error_rate
        && left.max_abstention_rate == right.max_abstention_rate
}

fn merge_entry(target: &mut ScorecardEntry, source: &ScorecardEntry) -> Result<(), String> {
    if !same_policy(target, source) {
        return Err(format!(
            "scorecard gate '{}' policy differs across shards",
            source.id
        ));
    }
    validate_entry(source)?;
    target.expected_count = target
        .expected_count
        .checked_add(source.expected_count)
        .ok_or_else(|| format!("scorecard gate '{}' expected_count overflowed", target.id))?;
    target.scored_count = target
        .scored_count
        .checked_add(source.scored_count)
        .ok_or_else(|| format!("scorecard gate '{}' scored_count overflowed", target.id))?;
    target.passed_count = target
        .passed_count
        .checked_add(source.passed_count)
        .ok_or_else(|| format!("scorecard gate '{}' passed_count overflowed", target.id))?;
    target.error_count = target
        .error_count
        .checked_add(source.error_count)
        .ok_or_else(|| format!("scorecard gate '{}' error_count overflowed", target.id))?;
    target.abstained_count = target
        .abstained_count
        .checked_add(source.abstained_count)
        .ok_or_else(|| format!("scorecard gate '{}' abstained_count overflowed", target.id))?;
    target.score_sum += source.score_sum;
    if !target.score_sum.is_finite() {
        return Err(format!(
            "scorecard gate '{}' score_sum overflowed",
            target.id
        ));
    }
    target.min_score = match (target.min_score, source.min_score) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (left, right) => left.or(right),
    };
    target.max_score = match (target.max_score, source.max_score) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    };
    Ok(())
}

/// Accumulates one immutable chunk's gate statistics in canonical chunk order.
/// Both inputs must describe the same shard and exact configured gate set.
pub(crate) fn accumulate_chunk_scorecard(
    target: &mut ShardScorecard,
    chunk: ShardScorecard,
) -> Result<(), String> {
    if target.version != chunk.version
        || target.run_shard != chunk.run_shard
        || target.policy_hash != chunk.policy_hash
        || target.entries.len() != chunk.entries.len()
    {
        return Err("chunk scorecard identity or gate set mismatch".to_owned());
    }
    for (target, source) in target.entries.iter_mut().zip(&chunk.entries) {
        validate_entry(source)?;
        if target.id != source.id {
            return Err("chunk scorecard gate order mismatch".to_owned());
        }
        merge_entry(target, source)?;
    }
    Ok(())
}

fn evaluate_entry(entry: ScorecardEntry) -> ScorecardGateResult {
    let mean_score =
        (entry.scored_count > 0).then_some(entry.score_sum / entry.scored_count as f64);
    let coverage = checked_rate(entry.scored_count, entry.expected_count);
    let pass_rate = entry
        .score_threshold
        .and_then(|_| checked_rate(entry.passed_count, entry.scored_count));
    let error_rate = checked_rate(entry.error_count, entry.expected_count);
    let abstention_rate = checked_rate(entry.abstained_count, entry.expected_count);
    let mut failures = Vec::new();

    if entry.expected_count == 0 {
        failures.push("no matching executions".to_string());
    }
    if entry
        .min_mean_score
        .is_some_and(|minimum| mean_score.is_none_or(|actual| actual < minimum))
    {
        failures.push("mean score below minimum".to_string());
    }
    if entry
        .min_coverage
        .is_some_and(|minimum| coverage.is_none_or(|actual| actual < minimum))
    {
        failures.push("coverage below minimum".to_string());
    }
    if entry
        .min_pass_rate
        .is_some_and(|minimum| pass_rate.is_none_or(|actual| actual < minimum))
    {
        failures.push("pass rate below minimum".to_string());
    }
    if entry
        .max_error_rate
        .is_some_and(|maximum| error_rate.is_none_or(|actual| actual > maximum))
    {
        failures.push("error rate above maximum".to_string());
    }
    if entry
        .max_abstention_rate
        .is_some_and(|maximum| abstention_rate.is_none_or(|actual| actual > maximum))
    {
        failures.push("abstention rate above maximum".to_string());
    }

    ScorecardGateResult {
        id: entry.id,
        dimension: entry.dimension,
        binding_id: entry.binding_id,
        case_group: entry.case_group,
        tags_all: entry.tags_all,
        passed: failures.is_empty(),
        failures,
        expected_count: entry.expected_count,
        scored_count: entry.scored_count,
        passed_count: entry.passed_count,
        error_count: entry.error_count,
        abstained_count: entry.abstained_count,
        mean_score,
        min_score: entry.min_score,
        max_score: entry.max_score,
        coverage,
        pass_rate,
        error_rate,
        abstention_rate,
    }
}

/// Merges bounded shard scorecards without reading execution-owned rows.
pub(crate) fn merge_shard_scorecards(
    expected_policy_hash: &str,
    shards: &[ShardScorecard],
) -> Result<RunScorecard, String> {
    let mut entries = BTreeMap::<String, ScorecardEntry>::new();
    let mut expected_gate_ids = None;
    let mut ordered_shards = shards.iter().collect::<Vec<_>>();
    ordered_shards.sort_by_key(|shard| shard.run_shard);
    for (index, shard) in ordered_shards.iter().enumerate() {
        if index > 0 && shard.run_shard == ordered_shards[index - 1].run_shard {
            return Err(format!("duplicate shard scorecard {}", shard.run_shard));
        }
        if shard.version != SCORECARD_VERSION {
            return Err(format!(
                "unsupported shard scorecard version {}",
                shard.version
            ));
        }
        if shard.policy_hash != expected_policy_hash {
            return Err("shard scorecard policy hash does not match the run".to_string());
        }
        let mut shard_ids = BTreeMap::new();
        for entry in &shard.entries {
            if shard_ids.insert(entry.id.as_str(), ()).is_some() {
                return Err(format!("shard scorecard repeats gate '{}'", entry.id));
            }
            if let Some(target) = entries.get_mut(&entry.id) {
                merge_entry(target, entry)?;
            } else {
                validate_entry(entry)?;
                entries.insert(entry.id.clone(), entry.clone());
            }
        }
        let shard_gate_ids = shard_ids.keys().copied().collect::<Vec<_>>();
        if let Some(expected) = &expected_gate_ids {
            if expected != &shard_gate_ids {
                return Err("shard scorecards contain different gate sets".to_string());
            }
        } else {
            expected_gate_ids = Some(shard_gate_ids);
        }
    }

    let gates = entries
        .into_values()
        .map(evaluate_entry)
        .collect::<Vec<_>>();
    Ok(RunScorecard {
        version: SCORECARD_VERSION,
        policy_hash: expected_policy_hash.to_string(),
        shard_count: shards.len(),
        passed: gates.iter().all(|gate| gate.passed),
        gates,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry() -> ScorecardEntry {
        ScorecardEntry {
            id: "safety".to_string(),
            dimension: "safety".to_string(),
            binding_id: None,
            case_group: None,
            tags_all: vec!["jailbreak".to_string()],
            min_mean_score: Some(0.9),
            score_threshold: Some(0.8),
            min_pass_rate: Some(1.0),
            min_coverage: Some(1.0),
            max_error_rate: Some(0.0),
            max_abstention_rate: Some(0.0),
            expected_count: 1,
            scored_count: 1,
            passed_count: 1,
            error_count: 0,
            abstained_count: 0,
            score_sum: 1.0,
            min_score: Some(1.0),
            max_score: Some(1.0),
        }
    }

    #[test]
    fn merges_counts_and_evaluates_gates() {
        let scorecard = merge_shard_scorecards(
            "hash",
            &[
                ShardScorecard {
                    version: 1,
                    run_shard: 0,
                    policy_hash: "hash".to_string(),
                    entries: vec![entry()],
                },
                ShardScorecard {
                    version: 1,
                    run_shard: 1,
                    policy_hash: "hash".to_string(),
                    entries: vec![entry()],
                },
            ],
        )
        .unwrap();

        assert!(scorecard.passed);
        assert_eq!(scorecard.gates[0].expected_count, 2);
        assert_eq!(scorecard.gates[0].mean_score, Some(1.0));
    }

    #[test]
    fn low_slice_score_fails_despite_a_strong_other_shard() {
        let strong = entry();
        let mut weak = entry();
        weak.passed_count = 0;
        weak.score_sum = 0.7;
        weak.min_score = Some(0.7);
        weak.max_score = Some(0.7);

        let scorecard = merge_shard_scorecards(
            "hash",
            &[
                ShardScorecard {
                    version: 1,
                    run_shard: 0,
                    policy_hash: "hash".to_string(),
                    entries: vec![strong],
                },
                ShardScorecard {
                    version: 1,
                    run_shard: 1,
                    policy_hash: "hash".to_string(),
                    entries: vec![weak],
                },
            ],
        )
        .unwrap();

        assert!(!scorecard.passed);
        assert!(
            scorecard.gates[0]
                .failures
                .contains(&"mean score below minimum".to_string())
        );
        assert!(
            scorecard.gates[0]
                .failures
                .contains(&"pass rate below minimum".to_string())
        );
    }

    #[test]
    fn chunk_accumulation_preserves_counts_and_rejects_changed_gate_identity() {
        let source = ShardScorecard {
            version: 1,
            run_shard: 3,
            policy_hash: "hash".into(),
            entries: vec![entry()],
        };
        let mut total = source.clone();
        accumulate_chunk_scorecard(&mut total, source.clone()).unwrap();
        assert_eq!(
            total.entries[0].expected_count,
            source.entries[0].expected_count * 2
        );
        assert_eq!(
            total.entries[0].scored_count,
            source.entries[0].scored_count * 2
        );
        assert_eq!(
            total.entries[0].score_sum,
            source.entries[0].score_sum * 2.0
        );
        assert_eq!(total.entries[0].min_score, source.entries[0].min_score);
        assert_eq!(total.entries[0].max_score, source.entries[0].max_score);
        let mut invalid = source.clone();
        invalid.entries[0].id = "other".into();
        assert!(accumulate_chunk_scorecard(&mut total, invalid).is_err());
        let mut invalid = source.clone();
        invalid.run_shard = 4;
        assert!(accumulate_chunk_scorecard(&mut total, invalid).is_err());
        let mut invalid = source;
        invalid.entries[0].min_coverage = Some(0.25);
        assert!(accumulate_chunk_scorecard(&mut total, invalid).is_err());
    }

    #[test]
    fn rejects_stale_or_inconsistent_shards() {
        let wrong_hash = merge_shard_scorecards(
            "current",
            &[ShardScorecard {
                version: 1,
                run_shard: 0,
                policy_hash: "stale".to_string(),
                entries: vec![entry()],
            }],
        );
        assert!(wrong_hash.is_err());

        let mut changed = entry();
        changed.min_coverage = Some(0.5);
        let inconsistent = merge_shard_scorecards(
            "hash",
            &[
                ShardScorecard {
                    version: 1,
                    run_shard: 0,
                    policy_hash: "hash".to_string(),
                    entries: vec![entry()],
                },
                ShardScorecard {
                    version: 1,
                    run_shard: 1,
                    policy_hash: "hash".to_string(),
                    entries: vec![changed],
                },
            ],
        );
        assert!(inconsistent.is_err());
    }
}
