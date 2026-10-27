//! Absolute statement-budget acceptance for completed creation campaigns.
//!
//! Reads frozen registry budgets and correctness-verified samples without changing
//! their verdict or bytes. Complete per-database categories must reconcile with
//! the historical total. Retrospective checks explicitly apply today's budgets
//! to old total-only evidence and cannot establish plane-specific acceptance.
//! Reports own a new artifact directory and retain source and policy provenance.
use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    fs,
    io::{
        BufRead,
        BufReader,
    },
    path::PathBuf,
};

use anyhow::{
    Context,
    Result,
    ensure,
};
use clap::Args;
use serde::Serialize;

use super::{
    EXIT_PASS,
    EXIT_REGRESSION,
    artifact,
    config,
    model::{
        CampaignManifest,
        ExternalMeasurements,
        Sample,
        SampleState,
    },
    provenance,
};

#[derive(Debug, Args)]
pub struct QueryBudgetArgs {
    /// Completed single-binary campaign; every selected tuple needs a budget.
    #[arg(long)]
    run_dir: PathBuf,
    /// New report directory under target/perf/runs; input evidence is never overwritten.
    #[arg(long)]
    output: Option<PathBuf>,
    /// Apply current registry budgets to historical total-only evidence explicitly.
    #[arg(long)]
    retrospective: bool,
}

#[derive(Serialize)]
struct Verdict {
    workload: String,
    tuple: String,
    samples: usize,
    ceiling: u64,
    minimum: u64,
    maximum: u64,
    passed: bool,
    /// Maxima are independent per role, not additive maxima from one sample.
    role_maxima: BTreeMap<String, u64>,
}

/// Checks all measured samples; missing, inconsistent or partial evidence is invalid.
/// An over-budget but otherwise valid campaign returns exit 1, not success.
pub fn execute(args: QueryBudgetArgs) -> Result<u8> {
    let root = artifact::workspace_root()?;
    let run_dir = artifact::require_artifact_path(&root, &args.run_dir)?;
    let campaign: CampaignManifest =
        serde_json::from_slice(&fs::read(run_dir.join("campaign.json"))?)?;
    ensure!(
        campaign.kind == "run" && campaign.status == "pass" && campaign.completed_at.is_some(),
        "query budgets require a completed, correctness-passing single-binary campaign"
    );
    let frozen = provenance::load(&run_dir, &campaign)?;
    let registry = if args.retrospective {
        config::load_registry(&root)?
    } else {
        frozen.registry
    };
    let input = run_dir.join("samples.jsonl");
    ensure!(
        fs::metadata(&input)?.len() <= 64 * 1024 * 1024,
        "sample evidence exceeds 64 MiB"
    );
    let mut groups = BTreeMap::<(String, String), Vec<ExternalMeasurements>>::new();
    let mut identities = BTreeSet::new();
    let mut measured = 0_u64;
    for line in BufReader::new(fs::File::open(&input)?).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let sample: Sample = serde_json::from_str(&line)?;
        ensure!(
            sample.run_id == campaign.run_id,
            "sample belongs to another campaign"
        );
        if !sample.measured {
            continue;
        }
        ensure!(
            sample.validation.state == SampleState::Valid
                && sample.process.exit_code == Some(0)
                && !sample.process.timed_out
                && !sample.process.stdout_truncated
                && !sample.process.stderr_truncated,
            "invalid or failed sample cannot satisfy a query budget"
        );
        ensure!(
            identities.insert((
                sample.workload_id.clone(),
                sample.tuple_id.clone(),
                sample.block_id,
                sample.position
            )),
            "duplicate measured sample"
        );
        measured += 1;
        groups
            .entry((sample.workload_id, sample.tuple_id))
            .or_default()
            .push(sample.external);
    }
    ensure!(
        measured == campaign.planned_measured_executions && measured > 0,
        "incomplete measurement campaign"
    );
    ensure!(
        groups.len() == frozen.profile.workloads.len(),
        "unexpected or missing workload tuples"
    );
    let mut verdicts = Vec::new();
    for selected in &frozen.profile.workloads {
        let samples = groups
            .remove(&(selected.id.clone(), selected.tuple.clone()))
            .context("missing selected tuple")?;
        // Single-binary balanced schedules measure two positions per block.
        ensure!(
            samples.len() == selected.blocks as usize * 2 && samples.len() >= 4,
            "missing required repetitions"
        );
        let workload = registry
            .workloads
            .iter()
            .find(|w| w.id == selected.id)
            .context("missing workload budget")?;
        let ceiling = *workload
            .statement_budgets
            .get(&selected.tuple)
            .context("missing tuple statement budget")?;
        verdicts.push(assess(
            &selected.id,
            &selected.tuple,
            ceiling,
            &samples,
            !args.retrospective,
        )?);
    }
    let passed = verdicts.iter().all(|v| v.passed);
    let output = artifact::create_run_dir(&root, args.output.as_deref(), "query-budget")?;
    artifact::atomic_json(&output.join("registry.json"), &registry)?;
    artifact::atomic_json(
        &output.join("query-budget.json"),
        &serde_json::json!({
            "schema_id":"query-budget-check/v1","source_run":campaign.run_id,
            "source_samples_digest":artifact::digest_file(&input)?,
            "source_campaign_digest":artifact::digest_file(&run_dir.join("campaign.json"))?,
            "registry_digest":artifact::digest_file(&output.join("registry.json"))?,
            "retrospective":args.retrospective,"passed":passed,"verdicts":verdicts
        }),
    )?;
    let mut text = String::from("# SQL statement budget check\n\n");
    text.push_str(if args.retrospective {"Retrospective total-only check using current budgets; no separate-plane acceptance.\n\n"} else {"Frozen campaign budgets; all per-database categories reconciled. Shared databases remain shared.\n\n"});
    text.push_str("| Tuple | Samples | Statements min–max | Ceiling | Result |\n| --- | ---: | ---: | ---: | --- |\n");
    for v in &verdicts {
        text.push_str(&format!(
            "| {} | {} | {}–{} | {} | {} |\n",
            v.tuple,
            v.samples,
            v.minimum,
            v.maximum,
            v.ceiling,
            if v.passed { "PASS" } else { "FAIL" }
        ));
    }
    artifact::atomic_text(&output.join("summary.md"), &text)?;
    print!("{text}");
    println!("Report: {}", output.display());
    Ok(if passed { EXIT_PASS } else { EXIT_REGRESSION })
}

/// Every repetition must fit: a low median cannot conceal one over-budget run.
fn assess(
    workload: &str,
    tuple: &str,
    ceiling: u64,
    samples: &[ExternalMeasurements],
    require_details: bool,
) -> Result<Verdict> {
    ensure!(
        ceiling > 0 && samples.len() >= 4,
        "missing budget or repetitions"
    );
    let mut counts = Vec::new();
    let mut role_maxima = BTreeMap::<String, u64>::new();
    for sample in samples {
        let (total, roles) = validate_counts(sample, require_details)?;
        counts.push(total);
        for (role, count) in roles {
            let maximum = role_maxima.entry(role).or_default();
            *maximum = (*maximum).max(count);
        }
    }
    let minimum = *counts.iter().min().context("empty tuple")?;
    let maximum = *counts.iter().max().context("empty tuple")?;
    Ok(Verdict {
        workload: workload.into(),
        tuple: tuple.into(),
        samples: samples.len(),
        ceiling,
        minimum,
        maximum,
        passed: maximum <= ceiling,
        role_maxima,
    })
}

/// Reconciles complete syntactic categories, never interpreting SELECT as proof
/// of no writes. Physical tuple mutations are a distinct measurement.
fn validate_counts(
    sample: &ExternalMeasurements,
    require_details: bool,
) -> Result<(u64, BTreeMap<String, u64>)> {
    let total = sample.sql_calls.context("missing SQL counter")?;
    ensure!(total > 0, "empty SQL measurement");
    if sample.sql_databases.is_empty() {
        ensure!(
            !require_details,
            "missing per-database statement accounting"
        );
        return Ok((total, BTreeMap::new()));
    }
    let mut seen = BTreeSet::new();
    let mut roles = BTreeMap::<String, u64>::new();
    let mut sum = 0_u64;
    for db in &sample.sql_databases {
        ensure!(
            !db.database.is_empty() && seen.insert(&db.database),
            "duplicate or empty database counter"
        );
        ensure!(
            matches!(db.role.as_str(), "control" | "execution" | "shared"),
            "unknown database role"
        );
        ensure!(
            db.execution_ms.is_finite()
                && db.execution_ms >= 0.0
                && db.planning_ms.is_finite()
                && db.planning_ms >= 0.0,
            "invalid SQL time"
        );
        for (category, count) in &db.calls {
            ensure!(
                matches!(
                    category.as_str(),
                    "read" | "dml" | "mixed" | "transaction" | "other"
                ),
                "unknown statement category"
            );
            sum = sum.checked_add(*count).context("statement overflow")?;
            let role = roles.entry(db.role.clone()).or_default();
            *role = role.checked_add(*count).context("role counter overflow")?;
        }
    }
    ensure!(sum == total, "statement categories disagree with total");
    Ok((total, roles))
}

#[cfg(test)]
mod tests {
    use super::{
        super::model::DatabaseStatements,
        *,
    };
    fn counters() -> ExternalMeasurements {
        ExternalMeasurements {
            sql_calls: Some(12),
            sql_databases: vec![DatabaseStatements {
                database: "owned_control".into(),
                role: "shared".into(),
                calls: BTreeMap::from([
                    ("read".into(), 5),
                    ("dml".into(), 3),
                    ("transaction".into(), 4),
                ]),
                ..Default::default()
            }],
            ..Default::default()
        }
    }
    #[test]
    fn one_bad_repetition_fails_even_when_the_median_passes() {
        let mut samples = vec![counters(); 4];
        assert!(
            assess("workload", "tuple", 12, &samples, true)
                .unwrap()
                .passed
        );
        samples[3].sql_calls = Some(13);
        samples[3].sql_databases[0].calls.insert("other".into(), 1);
        let verdict = assess("workload", "tuple", 12, &samples, true).unwrap();
        assert!(!verdict.passed);
        assert_eq!((verdict.minimum, verdict.maximum), (12, 13));
        samples[3].sql_calls = None;
        assert!(assess("workload", "tuple", 12, &samples, true).is_err());
        assert!(assess("workload", "tuple", 12, &samples[..3], true).is_err());
    }

    #[test]
    fn complete_categories_reconcile_and_preserve_shared_attribution() {
        let (total, roles) = validate_counts(&counters(), true).unwrap();
        assert_eq!(total, 12);
        assert_eq!(roles, BTreeMap::from([("shared".into(), 12)]));
    }
    #[test]
    fn missing_inconsistent_duplicate_and_overflowed_measurements_fail_closed() {
        assert!(validate_counts(&ExternalMeasurements::default(), false).is_err());
        let mut s = counters();
        s.sql_databases.clear();
        assert!(validate_counts(&s, true).is_err());
        assert!(validate_counts(&s, false).is_ok());
        let mut s = counters();
        s.sql_calls = Some(13);
        assert!(validate_counts(&s, true).is_err());
        let mut s = counters();
        s.sql_databases.push(s.sql_databases[0].clone());
        assert!(validate_counts(&s, true).is_err());
        let mut s = counters();
        s.sql_databases[0].calls.insert("other".into(), u64::MAX);
        assert!(validate_counts(&s, true).is_err());
        let mut s = counters();
        s.sql_databases[0].calls.insert("unclassified".into(), 0);
        assert!(validate_counts(&s, true).is_err());
    }
}
