//! Dataset-reference run preparation at the CLI boundary.
//!
//! Freezes evaluator artifacts and compact run identity before object publication.
//! The caller retries with the same run ID and profile after interruption; no
//! background process needs access to its local configuration file. Preparation
//! is explicit and non-dispatchable until execution admission verifies placements.
use anyhow::{
    Context as _,
    ensure,
};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::{
    db::workflows::{
        dataset_upload,
        run_plan,
    },
    run_planning::{
        self,
        Request,
        Session,
    },
};

/// A bounded profile source; input configuration cannot become a dataset-sized
/// allocation merely because a wrong file was supplied to plan preparation.
fn profile_input(inline: Option<String>, file: Option<PathBuf>) -> anyhow::Result<Value> {
    use std::io::Read;
    let raw = match (inline, file) {
        (Some(raw), None) => raw,
        (None, Some(path)) => {
            let mut bytes = Vec::new();
            std::fs::File::open(path)?
                .take(8 * 1024 * 1024 + 1)
                .read_to_end(&mut bytes)?;
            String::from_utf8(bytes)?
        }
        _ => anyhow::bail!("exactly one profile source is required"),
    };
    ensure!(raw.len() <= 8 * 1024 * 1024, "profile exceeds 8 MiB");
    let value = canonical_json(&crate::dataset_ingestion::input::yaml(raw.as_bytes())?);
    ensure!(
        serde_json::to_vec(&value)?.len() <= 8 * 1024 * 1024,
        "profile exceeds encoded 8 MiB limit"
    );
    Ok(value)
}
pub(super) async fn exec(
    context: Context,
    profile: Option<String>,
    profile_file: Option<PathBuf>,
    upload_id: Uuid,
    storage_config: PathBuf,
    run_id: Option<Uuid>,
) -> anyhow::Result<()> {
    let id = run_id.unwrap_or_else(Uuid::now_v7);
    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    let task = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal.cancel();
        }
    });
    let result = prepare(
        context,
        profile,
        profile_file,
        upload_id,
        storage_config,
        id,
        &cancel,
    )
    .await
    .with_context(|| format!("run plan {id}; retry with --run-id {id} and the same inputs"));
    task.abort();
    result
}
async fn prepare(
    context: Context,
    profile: Option<String>,
    profile_file: Option<PathBuf>,
    upload_id: Uuid,
    storage_config: PathBuf,
    id: Uuid,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let payload = profile_input(profile, profile_file)?;
    let profile: RunProfile = serde_json::from_value(payload.clone())?;
    let owner = super::super::dataset::owner(&storage_config)?;
    let router = context.dbr().await?;
    let db = router.control().await?;
    let upload = dataset_upload::load(db, upload_id).await?;
    let catalog = dataset_upload::catalog(db, upload_id)
        .await?
        .context("dataset is not finalized")?;
    ensure!(
        upload.status == "finalized"
            && owner.alias() == upload.storage_alias
            && owner.binding_hash()? == upload.binding_hash,
        "dataset storage owner binding changed"
    );
    owner.validate(&upload.limits)?;
    let policy = compute_aggregation_policy_hash(&profile)?;
    let profile_hash = hash_serialized_json(&payload)?;
    let existing = run_plan::load(db, id).await?;
    let execution = if let Some(plan) = existing.as_ref() {
        plan.execution_plan.0.clone()
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
    let assignments = if let Some(existing) = &existing {
        existing.assignments.0.clone()
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
            let store = run_planning::connect(db, &plan, lease, &owner, cancel).await?;
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
        let error = result.as_ref().err().map(|error| format!("{error:#}"));
        let release = run_plan::release(db, lease, error.as_deref()).await;
        let reference = result?;
        release?;
        reference
    };
    context.out().await?.write_value(&json!({"data":{"run_id":id,"run_key":id.to_string(),"status":"creating","plan_status":"ready","run_plan":reference,"dataset_version_id":identity.dataset_version_id,"profile_hash":draft.profile_hash,"dataset_hash":identity.dataset_hash,"aggregation_policy_hash":draft.aggregation_policy_hash,"execution_plan_hash":execution.hash()?},"meta":{"case_count":identity.case_count,"chunk_count":(identity.case_count-1)/100+1,"chunk_size":100,"dispatchable":false}}))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;
    use crate::cli::App;
    #[test]
    fn blob_run_sources_require_explicit_preparation_and_owner_without_changing_direct_inputs() {
        let id = "018f1111-1111-7111-8111-111111111111";
        let base = [
            "vigilo",
            "--database-url",
            "postgresql://unused",
            "run",
            "create",
            "--profile",
            "{}",
            "--dataset-upload-id",
            id,
            "--storage-config",
            "storage.toml",
            "--prepare-only",
        ];
        assert!(App::try_parse_from(base).is_ok());
        assert!(App::try_parse_from(&base[..base.len() - 1]).is_err());
        assert!(
            App::try_parse_from([
                "vigilo",
                "--database-url",
                "postgresql://unused",
                "run",
                "create",
                "--profile",
                "{}",
                "--dataset",
                "{}"
            ])
            .is_ok()
        );
        let mut conflicting = base.to_vec();
        conflicting.extend(["--dataset", "{}"]);
        assert!(App::try_parse_from(conflicting).is_err());
        assert!(
            App::try_parse_from([
                "vigilo",
                "--database-url",
                "postgresql://unused",
                "run",
                "create",
                "--profile",
                "{}",
                "--dataset",
                "{}",
                "--prepare-only"
            ])
            .is_err()
        );
        assert!(
            App::try_parse_from([
                "vigilo",
                "--database-url",
                "postgresql://unused",
                "run",
                "create",
                "--profile",
                "{}",
                "--dataset-upload-id",
                id,
                "--prepare-only"
            ])
            .is_err()
        );
    }
    #[test]
    fn bounded_profile_source_rejects_missing_conflicting_and_oversized_inputs() {
        assert!(profile_input(None, None).is_err());
        assert!(profile_input(Some("{}".into()), Some("missing".into())).is_err());
        assert!(profile_input(Some(" ".repeat(8 * 1024 * 1024 + 1)), None).is_err());
        assert!(profile_input(Some("not: [valid".into()), None).is_err());
        assert!(profile_input(Some("duplicate: 1\nduplicate: 2".into()), None).is_err());
        assert_eq!(
            profile_input(Some("b: 2\na: 1".into()), None).unwrap(),
            json!({"a":1,"b":2})
        );
    }
}
