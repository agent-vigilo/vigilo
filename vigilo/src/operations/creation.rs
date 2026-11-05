//! Immutable run preparation and typed lifecycle results shared by interfaces.
//!
//! Preparation freezes inputs and verifies published objects without approving
//! execution. Only the trusted host can hand the private prepared value back for
//! acceptance. File parsing, process signals and output belong to the interface.
use anyhow::{
    Context as _,
    ensure,
};
use serde_json::{
    Value,
    json,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::lifecycle::{
    CreationDetails,
    CreationMetadata,
    PlanReference,
    PreparedRun,
};
use crate::{
    blob_storage::config::OwnerConfig,
    context::Context,
    contracts::{
        case_storage::identity::hash_serialized_json,
        evaluator_abi::EvaluatorExecutionPlan,
        run::RunProfile,
    },
    db::workflows::{
        dataset_upload,
        run_create,
        run_plan,
        run_profile_validation,
    },
    models::{
        run::RunDraft,
        run_chunk::RunChunkDraft,
    },
    run_planning::{
        self,
        Request,
        Session,
    },
};
const DEFAULT_CHUNK_SIZE: usize = 100;
/// Request-local inputs already read by either creation entry point. Finalized
/// datasets cannot be aborted or expired. Only immutable plan fields cross this
/// handoff; status, cursors, publication and lease authority are read live later.
pub(crate) struct Preparation {
    payload: Value,
    upload: dataset_upload::Upload,
    catalog: dataset_upload::Catalog,
    frozen_plan: Option<FrozenPlanInputs>,
}
struct FrozenPlanInputs {
    execution: EvaluatorExecutionPlan,
    assignments: Vec<run_create::RunShardPlacementAssignment>,
}
impl From<run_plan::Plan> for FrozenPlanInputs {
    fn from(plan: run_plan::Plan) -> Self {
        Self {
            execution: plan.execution_plan.0,
            assignments: plan.assignments.0,
        }
    }
}
impl Preparation {
    pub(crate) fn new(
        payload: Value,
        upload: dataset_upload::Upload,
        catalog: dataset_upload::Catalog,
        existing: Option<run_plan::Plan>,
    ) -> anyhow::Result<Self> {
        ensure!(
            upload.status == "finalized" && upload.id == catalog.id,
            "dataset is not finalized or catalog identity differs"
        );
        Ok(Self {
            payload,
            upload,
            catalog,
            frozen_plan: existing.map(FrozenPlanInputs::from),
        })
    }
}

/// Computes the hash of all profile policy that can change scoring or gating.
pub(crate) fn compute_aggregation_policy_hash(profile: &RunProfile) -> anyhow::Result<String> {
    let groups = profile
        .case_groups
        .iter()
        .map(|group| {
            json!({
                "id": group.id,
                "applies_to": group.applies_to,
                "evaluators": group.evaluators,
                "aggregation": group.aggregation,
            })
        })
        .collect::<Vec<_>>();

    hash_serialized_json(&json!({
        "defaults": {
            "fail_on_any_blocking_failure": profile.defaults.fail_on_any_blocking_failure,
            "min_execution_score": profile.defaults.min_execution_score,
        },
        "case_groups": groups,
        "scorecard": profile.scorecard,
    }))
}

/// Continues creation with immutable inputs. Every publication/admission mutation
/// retains its live database fence; the final response reads committed state.
pub(crate) async fn prepare(
    context: &Context,
    client_id: Uuid,
    inputs: Preparation,
    owner: &OwnerConfig,
    id: Uuid,
    cancel: &CancellationToken,
) -> anyhow::Result<PreparedRun> {
    let Preparation {
        payload,
        upload,
        catalog,
        frozen_plan,
    } = inputs;
    let upload_id = upload.id;
    let profile: RunProfile = serde_json::from_value(payload.clone())?;
    let router = context.dbr().await?;
    let db = router.control().await?;
    ensure!(
        owner.alias() == upload.storage_alias && owner.binding_hash()? == upload.binding_hash,
        "dataset storage owner binding changed"
    );
    owner.validate(&upload.limits)?;
    let policy = compute_aggregation_policy_hash(&profile)?;
    let profile_hash = hash_serialized_json(&payload)?;
    // Absence is not immutable: another creator may have frozen this run while
    // we uploaded its dataset. Resolve only after checking for that durable plan.
    let frozen_plan = match frozen_plan {
        Some(plan) => Some(plan),
        None => run_plan::load(db, id).await?.map(FrozenPlanInputs::from),
    };
    let execution = if let Some(plan) = frozen_plan.as_ref() {
        plan.execution.clone()
    } else {
        EvaluatorExecutionPlan::new(
            policy.clone(),
            run_profile_validation::resolve_evaluators(db, &profile).await?,
        )
    };
    ensure!(
        execution.aggregation_policy_hash == policy,
        "run replay changed aggregation policy"
    );
    let identity = &upload.identity;
    let version = identity
        .dataset_version
        .clone()
        .unwrap_or_else(|| identity.dataset_version_id.to_string());
    let profile_version_id = format!("{}/{}", profile.profile_id, profile.profile_version);
    let snapshot = json!({
        "dataset_ref":identity.0,"dataset_manifest":catalog.manifest.0,
        "dataset_version_id":identity.dataset_version_id,"dataset_hash":identity.dataset_hash,
        "profile_version_id":profile_version_id,"profile_hash":profile_hash,
        "aggregation_policy_hash":policy,"execution_plan_hash":execution.hash()?,
        "storage_binding_hash":upload.binding_hash,"chunk_size":DEFAULT_CHUNK_SIZE,
    });
    let draft = RunDraft {
        run_key: id.to_string(),
        name: None,
        description: None,
        dataset_id: identity.dataset_id,
        dataset_version: version,
        dataset_version_id: identity.dataset_version_id,
        evaluation_profile_id: profile.profile_id.clone(),
        evaluation_profile_version: profile.profile_version.clone(),
        profile_version_id,
        profile_hash,
        aggregation_policy_id: "profile_case_group_aggregation".into(),
        aggregation_policy_version: "v5".into(),
        aggregation_policy_hash: policy,
        agent_provider: profile.agent.provider.clone(),
        agent_name: profile.agent.name.clone(),
        agent_version: profile.agent.version.clone(),
        prompt_config_id: profile
            .agent
            .prompt_config_id
            .clone()
            .unwrap_or_else(|| "default".into()),
        prompt_config_version: profile
            .agent
            .prompt_config_version
            .clone()
            .unwrap_or_else(|| "v1".into()),
        config_snapshot: snapshot.clone(),
        expected_execution_count: identity.case_count,
    };
    let assignments = if let Some(existing) = &frozen_plan {
        existing.assignments.clone()
    } else {
        let count = (identity.case_count - 1) / 100 + 1;
        let representatives = (0..count.min(128))
            .map(|index| RunChunkDraft {
                chunk_id: Uuid::nil(),
                run_shard: index as i16,
                profile_group_id: "default".into(),
                ordinal_start: 0,
                ordinal_end: 1,
            })
            .collect::<Vec<_>>();
        run_create::assign_run_shard_placements(router, &representatives).await?
    };
    let plan = run_plan::begin(
        db,
        id,
        upload_id,
        &upload.binding_hash,
        &upload.limits,
        &draft,
        &execution,
        &assignments,
        false,
    )
    .await?;
    ensure!(
        (plan.status == "ready") == plan.manifest.is_some(),
        "inconsistent run-plan publication state"
    );
    let reference = if let Some(reference) = plan.manifest.as_ref() {
        reference.0.clone()
    } else {
        let lease = run_plan::claim(db, id).await?;
        let result = async {
            let store = run_planning::connect(db, &plan, lease, owner, cancel).await?;
            run_planning::prepare(
                &Session {
                    db,
                    store: store.as_ref(),
                    lease,
                    limits: &plan.limits,
                    cancel,
                    alias: owner.alias(),
                    last_renewed: tokio::sync::Mutex::new(tokio::time::Instant::now()),
                },
                Request {
                    plan: &plan,
                    upload: &upload,
                    dataset: &catalog.manifest,
                    profile: &profile,
                    profile_payload: &payload,
                    assignments: &plan.assignments,
                    snapshot,
                },
            )
            .await
        }
        .await;
        // Publication releases ownership in its commit. Only errors (including
        // an uncertain commit) need a separate fenced cleanup attempt; preserve
        // the original failure if cleanup is unavailable too.
        if let Err(error) = &result {
            let _ = run_plan::release(db, lease, Some(&format!("{error:#}"))).await;
        }
        result?
    };
    let plan = run_plan::load(db, id)
        .await?
        .context("published plan disappeared")?;
    let store = std::sync::Arc::from(
        owner
            .reader(
                upload.limits.0.clone(),
                crate::blob_storage::Budget::standard(),
                cancel,
            )
            .await?,
    );
    let reader =
        crate::case_reader::RunReader::open(store, &reference, upload.limits.0.clone(), id, cancel)
            .await?;
    let executable = &reader.configuration.snapshot["executability"];
    let details = CreationDetails {
        run_id: id,
        run_key: id.to_string(),
        status: "creating".into(),
        error_message: None,
        plan_status: "ready".into(),
        run_plan: PlanReference(reference),
        dataset_version_id: identity.dataset_version_id,
        profile_version_id: draft.profile_version_id,
        profile_hash: draft.profile_hash,
        dataset_hash: identity.dataset_hash.clone(),
        aggregation_policy_hash: draft.aggregation_policy_hash,
        execution_plan_hash: execution.hash()?,
    };
    let metadata = CreationMetadata {
        case_count: identity.case_count,
        chunk_count: reader.manifest.chunk_count,
        chunk_size: DEFAULT_CHUNK_SIZE,
        dispatchable: false,
        shard_placement_count: plan.assignments.len(),
        shard_assignment_policy: router.shard_assignment_policy().as_str().into(),
        default_execution_database_alias: router.default_execution_database_alias().into(),
        execution_database_aliases: plan
            .assignments
            .iter()
            .map(|a| a.database_alias.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect(),
        creation_placement_count: 0,
        creation_placements_pending: 0,
        creation_placements_seeded: 0,
        creation_placements_failed: 0,
        creation_seed_attempt_count: 0,
        expected_evaluator_executions: executable["expected_evaluator_execution_count"].as_u64(),
        resolved_evaluator_refs: executable["runnable_evaluator_ref_count"].as_u64(),
    };
    Ok(PreparedRun {
        client_id,
        plan,
        reader,
        details,
        metadata,
    })
}
