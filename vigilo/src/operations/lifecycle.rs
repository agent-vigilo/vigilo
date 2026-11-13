//! Run lifecycle operations for trusted embedding hosts.
//!
//! One client reuses process services and owns its storage configuration. Request
//! inputs contain logical identifiers and a bounded profile, never paths or
//! credentials. Prepared values are private-construction proofs tied to that
//! client; output projections carry no authority. Cancellation of a future does
//! not undo committed preparation or durable acceptance.
use anyhow::Context as _;
use serde::Serialize;
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    Error,
    creation,
};
use crate::{
    blob_storage::config::OwnerConfig,
    case_reader::RunReader,
    context::Context,
    contracts::{
        case_storage::ObjectRef,
        run::RunProfile,
    },
    db::workflows::{
        dataset_upload,
        run_creation,
        run_plan,
    },
};

/// Caller-owned identity and immutable inputs. Reusing an ID requires identical inputs.
pub struct PrepareRun {
    pub run_id: Uuid,
    pub dataset_upload_id: Uuid,
    pub profile: Value,
}

/// Verified immutable object reference. It is reporting data, not approval.
#[derive(Clone, Debug, Serialize)]
#[serde(transparent)]
pub struct PlanReference(pub(crate) ObjectRef);
impl PlanReference {
    /// Digest of the exact immutable manifest bytes.
    pub fn digest(&self) -> &str {
        &self.0.digest
    }
}

/// Immutable identity plus current lifecycle state, without persistence internals.
#[derive(Clone, Debug, Serialize)]
pub struct CreationDetails {
    pub run_id: Uuid,
    pub run_key: String,
    pub status: String,
    pub error_message: Option<String>,
    pub plan_status: String,
    pub run_plan: PlanReference,
    pub dataset_version_id: Uuid,
    pub profile_version_id: String,
    pub profile_hash: String,
    pub dataset_hash: String,
    pub aggregation_policy_hash: String,
    pub execution_plan_hash: String,
}
/// Bounded preparation and admission progress. Placement counts do not count cases.
#[derive(Clone, Debug, Serialize)]
pub struct CreationMetadata {
    pub case_count: i32,
    pub chunk_count: i32,
    pub chunk_size: usize,
    pub dispatchable: bool,
    pub shard_placement_count: usize,
    pub shard_assignment_policy: String,
    pub default_execution_database_alias: String,
    pub execution_database_aliases: Vec<String>,
    pub creation_placement_count: i64,
    pub creation_placements_pending: i64,
    pub creation_placements_seeded: i64,
    pub creation_placements_failed: i64,
    pub creation_seed_attempt_count: i64,
    pub expected_evaluator_executions: Option<u64>,
    pub resolved_evaluator_refs: Option<u64>,
}
/// Typed creation response with the CLI's established serialized field names.
#[derive(Clone, Debug, Serialize)]
pub struct RunSubmission {
    pub data: CreationDetails,
    pub meta: CreationMetadata,
}

/// Privately constructed prepared work. The host authorizes these exact inputs
/// before calling `accept_run`. It cannot be constructed from a request body.
pub struct PreparedRun {
    pub(crate) client_id: Uuid,
    pub(crate) plan: run_plan::Plan,
    pub(crate) reader: RunReader,
    pub(crate) details: CreationDetails,
    pub(crate) metadata: CreationMetadata,
}
impl PreparedRun {
    /// Caller-selected durable run identity.
    pub fn run_id(&self) -> Uuid {
        self.details.run_id
    }
    /// Verified root describing the immutable prepared work.
    pub fn manifest(&self) -> &PlanReference {
        &self.details.run_plan
    }
    /// Frozen profile, evaluator identities and capability ceilings for host review.
    /// Secret values are not part of this document.
    pub fn configuration(&self) -> &Value {
        &self.reader.configuration.snapshot
    }
}

/// Run operations sharing fixed services. Storage settings are supplied at trusted
/// construction, not selected through ordinary operation input.
#[derive(Clone)]
pub struct RunClient {
    context: Context,
    owner: Option<OwnerConfig>,
    options: run_creation::Config,
    id: Uuid,
    caller: Option<crate::db::scope::UserContext>,
}
impl RunClient {
    /// Trusted construction binds the fixed credential domain. A supplied caller
    /// requires a user-role router; standalone construction supplies no caller.
    pub(crate) fn from_context(
        context: Context,
        owner: OwnerConfig,
        options: run_creation::Config,
        caller: Option<crate::db::scope::UserContext>,
    ) -> Self {
        Self {
            context,
            owner: Some(owner),
            options,
            id: Uuid::now_v7(),
            caller,
        }
    }

    /// Internal construction pending the complete hosted runtime acceptance gate.
    #[allow(dead_code)]
    pub(crate) fn from_hosted(
        context: Context,
        options: run_creation::Config,
        caller: crate::db::scope::UserContext,
    ) -> Self {
        Self {
            context,
            owner: None,
            options,
            id: Uuid::now_v7(),
            caller: Some(caller),
        }
    }

    /// Prepares and verifies a plan without permitting dispatch. Retry with the
    /// same logical input after interruption. The caller owns cancellation.
    pub async fn prepare_run(
        &self,
        input: PrepareRun,
        cancel: &CancellationToken,
    ) -> Result<PreparedRun, Error> {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if input.run_id.is_nil() || input.dataset_upload_id.is_nil() {
            return Err(Error::InvalidInput(
                "run and dataset upload IDs must be nonnil",
            ));
        }
        if serde_json::to_vec(&input.profile)
            .map_err(|_| Error::InvalidInput("invalid profile"))?
            .len()
            > 8 * 1024 * 1024
        {
            return Err(Error::InvalidInput("profile exceeds 8 MiB"));
        }
        serde_json::from_value::<RunProfile>(input.profile.clone())
            .map_err(|_| Error::InvalidInput("invalid run profile"))?;
        self.prepare_input(input, cancel)
            .await
            .map_err(Error::Dependency)
    }
    async fn prepare_input(
        &self,
        input: PrepareRun,
        cancel: &CancellationToken,
    ) -> anyhow::Result<PreparedRun> {
        let db = self.context.dbr().await?.control().await?;
        let (upload, catalog) = if let Some(caller) = self.caller {
            let mut tx = crate::db::scope::begin(db, caller, input.run_id).await?;
            let upload = dataset_upload::load(&mut *tx, input.dataset_upload_id).await?;
            let catalog = dataset_upload::catalog(&mut *tx, input.dataset_upload_id)
                .await?
                .context("dataset is not finalized")?;
            tx.commit().await?;
            (upload, catalog)
        } else {
            let upload = dataset_upload::load(db, input.dataset_upload_id).await?;
            let catalog = dataset_upload::catalog(db, input.dataset_upload_id)
                .await?
                .context("dataset is not finalized")?;
            (upload, catalog)
        };
        let inputs = creation::Preparation::new(input.profile, upload, catalog, None)?;
        self.prepare_inputs(inputs, input.run_id, cancel).await
    }
    pub(crate) async fn prepare_inputs(
        &self,
        inputs: creation::Preparation,
        id: Uuid,
        cancel: &CancellationToken,
    ) -> anyhow::Result<PreparedRun> {
        let (context, owner) = if let Some(caller) = self.caller {
            let (upload, binding) = inputs.resource_identity();
            let request =
                crate::operations::resources::Request::preparation(caller, id, upload, binding);
            self.context
                .preparation_resources(request, self.owner.as_ref(), cancel)
                .await?
        } else {
            anyhow::ensure!(
                !self.context.uses_injected_resources(),
                "injected preparation requires caller context"
            );
            (
                self.context.clone(),
                self.owner
                    .clone()
                    .context("standalone storage configuration is required")?,
            )
        };
        // The streaming planner has a large nested future. Keep its frame off
        // embedding hosts' request-task stacks; this allocates once per run.
        Box::pin(creation::prepare(
            &context,
            self.id,
            inputs,
            &owner,
            id,
            cancel,
            self.caller,
        ))
        .await
    }
    fn validate(&self, prepared: &PreparedRun) -> Result<(), Error> {
        if self.id != prepared.client_id {
            return Err(Error::Conflict("prepared run belongs to another client"));
        }
        Ok(())
    }
    /// Reads progress without granting execution authority.
    pub async fn describe(&self, prepared: &PreparedRun) -> Result<RunSubmission, Error> {
        self.validate(prepared)?;
        self.report(prepared).await.map_err(Error::Dependency)
    }
    async fn report(&self, prepared: &PreparedRun) -> anyhow::Result<RunSubmission> {
        let db = self.context.dbr().await?.control().await?;
        let state = if let Some(caller) = self.caller {
            let mut tx = crate::db::scope::begin(db, caller, prepared.run_id()).await?;
            let state = run_creation::select_creation_state(&mut *tx, prepared.run_id()).await?;
            tx.commit().await?;
            state
        } else {
            run_creation::select_creation_state(db, prepared.run_id()).await?
        }
        .context("run disappeared while reporting creation")?;
        let mut data = prepared.details.clone();
        data.status = state.status;
        data.error_message = state.error;
        let mut meta = prepared.metadata.clone();
        meta.dispatchable = matches!(data.status.as_str(), "pending" | "running");
        meta.creation_placement_count = state.progress.placement_count;
        meta.creation_placements_pending = state.progress.pending_placement_count;
        meta.creation_placements_seeded = state.progress.seeded_placement_count;
        meta.creation_placements_failed = state.progress.failed_placement_count;
        meta.creation_seed_attempt_count = state.progress.attempt_count;
        Ok(RunSubmission { data, meta })
    }
    /// Trusted acceptance boundary. Authorize the prepared immutable configuration
    /// before this call. Committed acceptance survives disconnect and recovery;
    /// a creating response means bounded admission remains pending.
    pub async fn accept_run(
        &self,
        prepared: PreparedRun,
        cancel: &CancellationToken,
    ) -> Result<RunSubmission, Error> {
        self.validate(&prepared)?;
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        self.accept(prepared, cancel)
            .await
            .map_err(Error::Dependency)
    }
    async fn accept(
        &self,
        prepared: PreparedRun,
        cancel: &CancellationToken,
    ) -> anyhow::Result<RunSubmission> {
        anyhow::ensure!(!cancel.is_cancelled(), "run acceptance cancelled");
        let db = self.context.dbr().await?.control().await?;
        if let Some(caller) = self.caller {
            let mut tx = crate::db::scope::begin(db, caller, prepared.run_id()).await?;
            let status: String = sqlx::query_scalar("SELECT status::text FROM runs WHERE id=$1")
                .bind(prepared.run_id())
                .fetch_one(&mut *tx)
                .await?;
            if status == "creating" {
                run_plan::admission::accept(
                    &mut *tx,
                    prepared.run_id(),
                    &prepared.reader.reference,
                )
                .await?;
            }
            tx.commit().await?;
            // Acceptance is durable; background admission obtains its own system
            // claim. Never run scheduler work with caller credentials or identity.
            return self.report(&prepared).await;
        }
        let status: String = sqlx::query_scalar("SELECT status::text FROM runs WHERE id=$1")
            .bind(prepared.run_id())
            .fetch_one(db)
            .await?;
        if status == "creating" {
            run_plan::admission::accept(db, prepared.run_id(), &prepared.reader.reference).await?;
            if let Err(error) = crate::run_admission::resume_verified(
                &self.context,
                &prepared.plan,
                &prepared.reader,
                self.options,
                cancel,
            )
            .await
            {
                tracing::warn!(run_id=%prepared.run_id(),error=%error,"execution admission stopped before dispatch");
            }
        }
        self.report(&prepared).await
    }
    /// Standalone operator convenience: prepare and accept the same immutable work.
    pub async fn create_run(
        &self,
        input: PrepareRun,
        cancel: &CancellationToken,
    ) -> Result<RunSubmission, Error> {
        let prepared = self.prepare_run(input, cancel).await?;
        self.accept_run(prepared, cancel).await
    }
    /// Resumes interrupted creation with the same ID and inputs. This does not
    /// resume a paused execution or change a submitted run's configuration.
    pub async fn resume_creation(
        &self,
        input: PrepareRun,
        cancel: &CancellationToken,
    ) -> Result<RunSubmission, Error> {
        self.create_run(input, cancel).await
    }
}

#[cfg(test)]
impl RunClient {
    /// Build a real verified-reader fixture tied to this client's private identity.
    /// Production preparation constructs this proof only after complete validation.
    pub(crate) fn prepared_for_test(
        &self,
        plan: run_plan::Plan,
        reader: RunReader,
        draft: &crate::models::run::RunDraft,
        dataset_hash: String,
    ) -> PreparedRun {
        let details = CreationDetails {
            run_id: plan.run_id,
            run_key: draft.run_key.clone(),
            status: "creating".into(),
            error_message: None,
            plan_status: "ready".into(),
            run_plan: PlanReference(reader.reference.clone()),
            dataset_version_id: draft.dataset_version_id,
            profile_version_id: draft.profile_version_id.clone(),
            profile_hash: draft.profile_hash.clone(),
            dataset_hash,
            aggregation_policy_hash: draft.aggregation_policy_hash.clone(),
            execution_plan_hash: plan.execution_plan.hash().unwrap(),
        };
        let metadata = CreationMetadata {
            case_count: reader.dataset.case_count,
            chunk_count: reader.manifest.chunk_count,
            chunk_size: 100,
            dispatchable: false,
            shard_placement_count: plan.assignments.len(),
            shard_assignment_policy: "single-default".into(),
            default_execution_database_alias: "primary".into(),
            execution_database_aliases: vec!["primary".into()],
            creation_placement_count: 0,
            creation_placements_pending: 0,
            creation_placements_seeded: 0,
            creation_placements_failed: 0,
            creation_seed_attempt_count: 0,
            expected_evaluator_executions: None,
            resolved_evaluator_refs: None,
        };
        PreparedRun {
            client_id: self.id,
            plan,
            reader,
            details,
            metadata,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client() -> RunClient {
        super::super::Client::new(super::super::Config::new("postgresql://localhost:1/vigilo".into())).unwrap()
            .runs("provider='s3'\nalias='cases'\nbucket='cases'\nprefix='tests'\nregion='us-east-1'\n[credential_variables]\naccess_key='TEST_ACCESS'\nsecret_key='TEST_SECRET'\n").unwrap()
    }
    #[tokio::test]
    async fn invalid_input_and_precancelled_work_never_require_services() {
        let client = client();
        let cancel = CancellationToken::new();
        let input = || PrepareRun {
            run_id: Uuid::nil(),
            dataset_upload_id: Uuid::now_v7(),
            profile: serde_json::json!({}),
        };
        assert!(matches!(
            client.prepare_run(input(), &cancel).await,
            Err(Error::InvalidInput(_))
        ));
        cancel.cancel();
        assert!(matches!(
            client.prepare_run(input(), &cancel).await,
            Err(Error::Cancelled)
        ));
        let valid_ids = PrepareRun {
            run_id: Uuid::now_v7(),
            dataset_upload_id: Uuid::now_v7(),
            profile: serde_json::json!({"secret":"must not be returned"}),
        };
        let error = client
            .prepare_run(valid_ids, &CancellationToken::new())
            .await
            .err()
            .unwrap();
        assert!(matches!(error, Error::InvalidInput(_)));
        assert!(!error.to_string().contains("must not be returned"));
    }
    #[test]
    fn cloned_clients_share_authority_and_independent_clients_do_not() {
        let client = client();
        assert_eq!(client.id, client.clone().id);
        assert_ne!(client.id, super::tests::client().id);
    }
}
