//! Evaluator publication into the built-in Vigilo registry namespace.
//!
//! Prepares an existing evaluator artifact, validates immutable identity, and
//! inserts it idempotently. Building artifacts and database setup are separate.

use std::path::PathBuf;

use tracing::info;

use crate::{
    context::Context,
    db::tables::evaluators as evaluator_table,
    models::evaluator::{
        Evaluator,
        EvaluatorDraft,
    },
};

const EVALUATOR_NAMESPACE: &str = "vigilo";

/// Result of attempting to publish one evaluator package.
#[derive(Debug)]
pub(crate) enum PublishEvaluatorOutcome {
    Inserted(Evaluator),
    Skipped(Evaluator),
}

impl PublishEvaluatorOutcome {
    pub(crate) fn evaluator(&self) -> &Evaluator {
        match self {
            Self::Inserted(evaluator) | Self::Skipped(evaluator) => evaluator,
        }
    }
}

/// Publishes one evaluator package into the built-in `vigilo` namespace.
pub(crate) async fn publish_vigilo_evaluator(
    context: &Context,
    evaluator_path: PathBuf,
    profile: String,
) -> anyhow::Result<PublishEvaluatorOutcome> {
    let component = context
        .wasm()
        .await?
        .prepare_evaluator(evaluator_path, profile)?;

    publish_prepared(context, component).await
}

/// Publishes an artifact already validated by the host runtime.
pub(crate) async fn publish_prepared(
    context: &Context,
    component: crate::context::wasm::Component,
) -> anyhow::Result<PublishEvaluatorOutcome> {
    let draft = EvaluatorDraft {
        namespace: EVALUATOR_NAMESPACE.to_string(),
        name: component.name,
        version: component.version,
        content_hash: component.wasm_hash,
        wasm_bytes: component.wasm_bytes,
        interface_name: component.interface_name,
        interface_version: component.interface_version,
        wit_world: component.wit_world,
        abi_contract_hash: component.abi.contract_hash,
        abi_adapter: component.abi.adapter,
        runtime: component.runtime,
        runtime_version: component.runtime_version,
        runtime_fingerprint: component.runtime_fingerprint,
        description: component.description,
        tags: component.tags,
        metadata: component.metadata,
    };

    publish_evaluator_draft(context, draft).await
}

async fn publish_evaluator_draft(
    context: &Context,
    draft: EvaluatorDraft,
) -> anyhow::Result<PublishEvaluatorOutcome> {
    // Evaluator registry rows are control-database metadata. Keep this lookup
    // choice inside the evaluator publishing workflow.
    let db = context.dbr().await?.control().await?;

    if let Some(existing) =
        evaluator_table::select_evaluator(db, &draft.namespace, &draft.name, &draft.version).await?
    {
        if existing.content_hash == draft.content_hash {
            info!(
                "evaluator {}/{}:{} already exists, skipping",
                existing.namespace, existing.name, existing.version
            );
            return Ok(PublishEvaluatorOutcome::Skipped(existing));
        }

        anyhow::bail!(
            "evaluator {}/{}:{} already exists with a different content hash",
            draft.namespace,
            draft.name,
            draft.version,
        );
    }

    if let Some(existing) =
        evaluator_table::select_evaluator_by_content_hash(db, &draft.namespace, &draft.content_hash)
            .await?
    {
        anyhow::bail!(
            "evaluator content hash already exists as {}/{}:{}, refusing to publish duplicate content as {}/{}:{}",
            existing.namespace,
            existing.name,
            existing.version,
            draft.namespace,
            draft.name,
            draft.version,
        );
    }

    let evaluator = evaluator_table::insert_evaluator(db, &draft).await?;
    info!(
        "successfully published evaluator: {}/{}:{}",
        evaluator.namespace, evaluator.name, evaluator.version,
    );

    Ok(PublishEvaluatorOutcome::Inserted(evaluator))
}

#[cfg(all(test, feature = "evaluator-abi-fixtures"))]
mod postgres_tests;
