//! Versioned deterministic inputs for service-backed performance workloads.
//!
//! Fixture catalogs store only stable logical shapes. Runtime-specific values,
//! such as the run-scoped agent URL, are injected while rendering profile and
//! dataset files beneath the campaign artifact directory.

use std::{
    fs,
    path::{
        Path,
        PathBuf,
    },
};

use anyhow::{
    Context,
    Result,
    bail,
};
use serde::Deserialize;
use serde_json::{
    Value,
    json,
};
use uuid::Uuid;

const FIXTURE_SCHEMA: &str = "performance-fixtures/v1";
const FIXTURE_NAMESPACE: Uuid = Uuid::from_u128(0x8f17098f_9fd0_4c9f_9164_957940ab5e8d);

/// Complete deterministic performance fixture catalog.
#[derive(Debug, Clone, Deserialize)]
pub struct FixtureCatalog {
    /// Fixture document shape.
    pub schema_id: String,
    /// Stable fixture identity referenced by workloads.
    pub id: String,
    /// Published evaluator used by worker workloads.
    pub evaluator_ref: String,
    /// Deterministic successful agent response prefix.
    pub agent_response_text: String,
    /// Target response payload size for HTTP byte accounting.
    pub agent_payload_bytes: usize,
    /// Run-creation fixture shape.
    pub run_create: RunCreateFixture,
    /// Coordinator fixture shape.
    pub coordinator: CoordinatorFixture,
    /// Worker fixture shape.
    pub worker: WorkerFixture,
    /// End-to-end lifecycle fixture shape.
    pub lifecycle: LifecycleFixture,
}

/// Run-creation workload cardinalities.
#[derive(Debug, Clone, Deserialize)]
pub struct RunCreateFixture {
    /// Number of cases submitted to `run create`.
    pub cases: usize,
    /// Exact chunks expected after creation.
    pub expected_chunks: i64,
}

/// Coordinator dispatch workload cardinalities.
#[derive(Debug, Clone, Deserialize)]
pub struct CoordinatorFixture {
    /// Chunks made ready by the measured coordinator cycle.
    pub chunks: usize,
    /// Cases represented by each chunk.
    pub cases_per_chunk: usize,
}

/// Orthogonal worker fixture dimensions.
#[derive(Debug, Clone, Deserialize)]
pub struct WorkerFixture {
    /// Case-heavy tuple case count.
    pub cases_many: usize,
    /// Evaluator-heavy tuple evaluator-binding count.
    pub evaluators_many: usize,
}

/// End-to-end lifecycle bounds.
#[derive(Debug, Clone, Deserialize)]
pub struct LifecycleFixture {
    /// Cases processed by the lifecycle workload.
    pub cases: usize,
    /// Maximum worker passes before declaring liveness failure.
    pub worker_pass_limit: usize,
    /// Maximum coordinator cycles before declaring liveness failure.
    pub coordinator_cycle_limit: usize,
    /// Maximum coordinator cycles for the bounded capacity staircase.
    pub capacity_cycle_limit: usize,
}

/// Rendered run input paths and their deterministic case count.
pub struct RunInputs {
    /// Profile YAML passed to `vigilo run create`.
    pub profile: PathBuf,
    /// Dataset YAML passed to `vigilo run create`.
    pub dataset: PathBuf,
}

/// Loads and validates a fixture catalog by stable ID.
pub fn load(root: &Path, id: &str) -> Result<FixtureCatalog> {
    if !id
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        bail!("invalid fixture ID: {id}");
    }
    let path = root.join("performance/fixtures").join(format!("{id}.toml"));
    let fixture: FixtureCatalog = toml::from_str(
        &fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?,
    )
    .with_context(|| format!("parse {}", path.display()))?;
    if fixture.schema_id != FIXTURE_SCHEMA || fixture.id != id {
        bail!("fixture {id} has an incompatible identity or schema");
    }
    if fixture.evaluator_ref.is_empty()
        || fixture.agent_response_text.is_empty()
        || fixture.agent_payload_bytes == 0
        || fixture.run_create.cases == 0
        || fixture.run_create.expected_chunks == 0
        || fixture.coordinator.chunks == 0
        || fixture.coordinator.cases_per_chunk == 0
        || fixture.worker.cases_many == 0
        || fixture.worker.evaluators_many == 0
        || fixture.lifecycle.cases == 0
        || fixture.lifecycle.worker_pass_limit == 0
        || fixture.lifecycle.coordinator_cycle_limit == 0
        || fixture.lifecycle.capacity_cycle_limit == 0
    {
        bail!("fixture {id} contains a zero or empty required value");
    }
    Ok(fixture)
}

/// Writes deterministic run profile and dataset inputs for a workload setup.
pub fn write_run_inputs(
    directory: &Path,
    fixture: &FixtureCatalog,
    identity: &str,
    agent_url: &str,
    case_count: usize,
    evaluator_count: usize,
) -> Result<RunInputs> {
    write_run_inputs_with_payload(
        directory,
        fixture,
        identity,
        agent_url,
        case_count,
        evaluator_count,
        0,
    )
}

/// Writes deterministic inputs with an optional per-case string payload.
///
/// The payload is used only by byte-boundary workloads; normal fixtures retain
/// their compact input shape and do not allocate padding.
pub fn write_run_inputs_with_payload(
    directory: &Path,
    fixture: &FixtureCatalog,
    identity: &str,
    agent_url: &str,
    case_count: usize,
    evaluator_count: usize,
    payload_bytes: usize,
) -> Result<RunInputs> {
    if case_count == 0 || evaluator_count == 0 {
        bail!("run inputs require positive case and evaluator counts");
    }
    fs::create_dir_all(directory)?;
    let profile = directory.join("profile.yaml");
    let mut dataset = directory.join("dataset.yaml");
    let profile_value = profile_value(fixture, identity, agent_url, evaluator_count);
    let dataset_value = dataset_value(identity, case_count, payload_bytes);
    fs::write(&profile, serde_yaml::to_string(&profile_value)?)?;
    let encoded = serde_yaml::to_string(&dataset_value)?;
    if encoded.len() <= 8 * 1024 * 1024 {
        fs::write(&dataset, encoded)?;
    } else {
        use std::io::Write;
        dataset = directory.join("dataset.jsonl");
        let mut output = std::io::BufWriter::new(fs::File::create(&dataset)?);
        let mut fields = dataset_value.clone();
        fields
            .as_object_mut()
            .expect("fixture object")
            .remove("cases");
        serde_json::to_writer(
            &mut output,
            &json!({"schema":"vigilo/dataset-upload/v1","fields":fields}),
        )?;
        output.write_all(b"\n")?;
        for case in dataset_value["cases"].as_array().expect("fixture cases") {
            serde_json::to_writer(&mut output, case)?;
            output.write_all(b"\n")?;
        }
        output.flush()?;
    }
    Ok(RunInputs { profile, dataset })
}

fn profile_value(
    fixture: &FixtureCatalog,
    identity: &str,
    agent_url: &str,
    evaluator_count: usize,
) -> Value {
    let evaluators = (0..evaluator_count)
        .map(|index| {
            json!({
                "id": format!("sentiment-{index:02}"),
                "ref": fixture.evaluator_ref,
                "required": true,
                "dimension": "quality",
                "blocking": true,
                "weight": 1.0,
                "normalization": {
                    "method": "ordinal",
                    "values": {"positive": 1.0, "neutral": 0.5, "negative": 0.0}
                },
                "pass_threshold": 0.5
            })
        })
        .collect::<Vec<_>>();
    json!({
        "profile_id": format!("perf_{identity}"),
        "profile_version": "1.0.0",
        "description": "Deterministic performance fixture.",
        "defaults": {
            "max_attempts": 1,
            "request_timeout_secs": 30,
            "fail_on_any_blocking_failure": true,
            "min_execution_score": 0.5
        },
        "persistence": {
            "mode": "full",
            "persist_raw_outputs": "all",
            "persist_evaluator_evidence": true
        },
        "agent": {
            "provider": "vigilo-performance",
            "name": "deterministic-agent",
            "http": {"url": agent_url}
        },
        "case_groups": [{
            "id": "sentiment",
            "description": "Deterministic performance sentiment cases.",
            "applies_to": {"task_type": "classification"},
            "evaluators": evaluators,
            "aggregation": {
                "dimensions": {
                    "quality": {"method": "min_score", "blocking": true, "weight": 1.0}
                }
            }
        }]
    })
}

fn dataset_value(identity: &str, case_count: usize, payload_bytes: usize) -> Value {
    let dataset_id = stable_uuid(&format!("{identity}:dataset"));
    let padding = "x".repeat(payload_bytes);
    let cases = (0..case_count)
        .map(|ordinal| {
            json!({
                "id": stable_uuid(&format!("{identity}:case:{ordinal}")),
                "task_type": "classification",
                "input": {
                    "user_message": format!("case {ordinal}: good reliable input"),
                    "payload": padding,
                },
                "expected": {"label": "positive"},
                "tags": ["performance"],
                "metadata": {"ordinal": ordinal}
            })
        })
        .collect::<Vec<_>>();
    json!({
        "dataset_id": dataset_id,
        "dataset_version": "1.0.0",
        "cases": cases
    })
}

fn stable_uuid(value: &str) -> Uuid {
    Uuid::new_v5(&FIXTURE_NAMESPACE, value.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> FixtureCatalog {
        FixtureCatalog {
            schema_id: FIXTURE_SCHEMA.into(),
            id: "mvp-v1".into(),
            evaluator_ref: "vigilo/example:1.0.0".into(),
            agent_response_text: "good".into(),
            agent_payload_bytes: 1024,
            run_create: RunCreateFixture {
                cases: 2,
                expected_chunks: 1,
            },
            coordinator: CoordinatorFixture {
                chunks: 2,
                cases_per_chunk: 100,
            },
            worker: WorkerFixture {
                cases_many: 8,
                evaluators_many: 8,
            },
            lifecycle: LifecycleFixture {
                cases: 100,
                worker_pass_limit: 8,
                coordinator_cycle_limit: 8,
                capacity_cycle_limit: 32,
            },
        }
    }

    #[test]
    fn generated_inputs_are_structurally_deterministic() {
        let fixture = fixture();
        let first = dataset_value("same", 2, 0);
        let second = dataset_value("same", 2, 0);
        assert_eq!(first, second);
        assert_eq!(
            profile_value(&fixture, "same", "http://127.0.0.1", 8)["case_groups"][0]["evaluators"]
                .as_array()
                .unwrap()
                .len(),
            8
        );
    }

    #[test]
    fn repository_fixture_loads_and_invalid_ids_fail_closed() {
        let root = crate::perf::artifact::workspace_root().unwrap();
        let loaded = load(&root, "mvp-v1").unwrap();
        assert_eq!(loaded.id, "mvp-v1");
        assert!(load(&root, "../mvp-v1").is_err());
        assert!(load(&root, "missing").is_err());
    }

    #[test]
    fn rendered_inputs_have_requested_cardinality() {
        let directory = tempfile::tempdir().unwrap();
        let inputs = write_run_inputs(
            directory.path(),
            &fixture(),
            "stable",
            "http://127.0.0.1:1234",
            3,
            2,
        )
        .unwrap();
        let profile: Value =
            serde_yaml::from_str(&fs::read_to_string(inputs.profile).unwrap()).unwrap();
        let dataset: Value =
            serde_yaml::from_str(&fs::read_to_string(inputs.dataset).unwrap()).unwrap();
        assert_eq!(
            profile["case_groups"][0]["evaluators"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(dataset["cases"].as_array().unwrap().len(), 3);
        assert!(
            write_run_inputs(
                directory.path(),
                &fixture(),
                "bad",
                "http://localhost",
                0,
                1
            )
            .is_err()
        );
    }
}
