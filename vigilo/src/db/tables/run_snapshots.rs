//! Run snapshot table access.
//!
//! Snapshots copy the immutable run context workers need into the execution
//! placement before chunk-ready events become visible.

use serde::Deserialize;
#[cfg(test)]
use sqlx::PgPool;
#[cfg(test)]
use uuid::Uuid;

use crate::contracts::{
    evaluator_abi::EvaluatorExecutionPlan,
    run::RunProfile,
};

/// Immutable worker context copied to an execution placement.
#[derive(Debug, Deserialize)]
pub(crate) struct WorkerRunSnapshot {
    pub(crate) profile: RunProfile,
    pub(crate) aggregation_policy_hash: String,
    pub(crate) execution_plan_hash: String,
    pub(crate) execution_plan: EvaluatorExecutionPlan,
}

/// Finds and verifies the worker context from a run snapshot.
///
/// Worker hot paths use this instead of reading the authoritative control
/// `runs` row.
#[cfg(test)]
pub(crate) async fn select_worker_run_snapshot(
    db: &PgPool,
    run_id: Uuid,
    run_shard: i16,
) -> anyhow::Result<Option<WorkerRunSnapshot>> {
    let snapshot = sqlx::query_scalar::<_, serde_json::Value>(
        r#"
        SELECT config_snapshot
        FROM run_snapshots
        WHERE run_id = $1::uuid
          AND run_shard = $2
        "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .fetch_optional(db)
    .await?;

    let Some(snapshot) = snapshot else {
        return Ok(None);
    };
    Ok(Some(decode_worker_snapshot(snapshot)?))
}

/// Validates frozen evaluator identity after loading a stored configuration.
pub(crate) fn decode_worker_snapshot(
    snapshot: serde_json::Value,
) -> anyhow::Result<WorkerRunSnapshot> {
    let snapshot: WorkerRunSnapshot = serde_json::from_value(snapshot)?;
    if snapshot.execution_plan.version != 1 {
        anyhow::bail!(
            "unsupported evaluator execution plan version {}",
            snapshot.execution_plan.version
        );
    }
    if snapshot.execution_plan.hash()? != snapshot.execution_plan_hash {
        anyhow::bail!("run snapshot evaluator execution plan hash mismatch");
    }
    if snapshot.execution_plan.aggregation_policy_hash != snapshot.aggregation_policy_hash {
        anyhow::bail!("run snapshot scoring policy and evaluator execution plan disagree");
    }
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    #[cfg(test)]
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::*;
    use crate::contracts::evaluator_abi::{
        EvaluatorExecutionPlan,
        ResolvedEvaluator,
    };

    #[sqlx::test(migrations = "../migrations")]
    #[ignore = "requires a PostgreSQL DATABASE_URL for sqlx snapshot tests"]
    async fn select_worker_run_snapshot_verifies_execution_plan(pool: PgPool) {
        let run_id = Uuid::now_v7();
        let dataset_id = Uuid::now_v7();
        let dataset_version_id = Uuid::now_v7();
        let profile = serde_yaml::from_str::<serde_json::Value>(include_str!(
            "../../../../example/profile.yaml"
        ))
        .unwrap();
        let execution_plan = EvaluatorExecutionPlan::new(
            "aggregation-hash".to_string(),
            vec![ResolvedEvaluator {
                evaluator_ref: "vigilo/sentiment-basic-en:0.1.0".to_string(),
                evaluator_id: Uuid::now_v7(),
                content_hash: "content-hash".to_string(),
                abi: crate::evaluator_abi::current_identity(),
                runtime: "wasmtime".to_string(),
                runtime_version: "44.0.0".to_string(),
                runtime_fingerprint: "runtime-fingerprint".to_string(),
            }],
        );
        let execution_plan_hash = execution_plan.hash().unwrap();
        let config_snapshot = json!({
            "profile": profile,
            "aggregation_policy_hash": "aggregation-hash",
            "execution_plan_hash": execution_plan_hash,
            "execution_plan": execution_plan,
        });

        sqlx::query(
            r#"
            INSERT INTO run_snapshots (
                run_id,
                run_shard,
                run_key,
                dataset_id,
                dataset_version_id,
                dataset_version,
                evaluation_profile_id,
                evaluation_profile_version,
                profile_version_id,
                profile_hash,
                aggregation_policy_id,
                aggregation_policy_version,
                aggregation_policy_hash,
                agent_provider,
                agent_name,
                prompt_config_id,
                prompt_config_version,
                config_snapshot,
                expected_execution_count
            )
            VALUES (
                $1::uuid,
                7,
                'run-key',
                $2::uuid,
                $3::uuid,
                'dataset',
                'profile',
                '1.0.0',
                'profile-version',
                'profile-hash',
                'aggregation',
                '1.0.0',
                'aggregation-hash',
                'example',
                'agent',
                'prompt',
                '1.0.0',
                $4::jsonb,
                1
            )
            "#,
        )
        .bind(run_id)
        .bind(dataset_id)
        .bind(dataset_version_id)
        .bind(&config_snapshot)
        .execute(&pool)
        .await
        .unwrap();

        let selected = select_worker_run_snapshot(&pool, run_id, 7)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(selected.execution_plan_hash, execution_plan_hash);

        sqlx::query(
            "UPDATE run_snapshots SET config_snapshot = jsonb_set(config_snapshot, '{execution_plan_hash}', '\"tampered\"'::jsonb) WHERE run_id = $1 AND run_shard = 7",
        )
        .bind(run_id)
        .execute(&pool)
        .await
        .unwrap();
        assert!(select_worker_run_snapshot(&pool, run_id, 7).await.is_err());
    }
}
