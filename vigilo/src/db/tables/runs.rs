//! Run table access.
//!
//! Runs are the top-level evaluation jobs. The table stores run identity,
//! compact configuration snapshots, coordinator lease fields, finalization
//! counters, and final gate status. Multi-step run state changes live in
//! workflows.

use sqlx::PgPool;
use uuid::Uuid;

use crate::models::run::{
    Run,
    RunDraft,
    RunPatch,
};

/// Inserts a new run and assigns a time-sortable database id.
pub(crate) async fn insert_run(db: &PgPool, draft: &RunDraft) -> anyhow::Result<Run> {
    let run = sqlx::query_as::<_, Run>(
        r#"
        INSERT INTO runs (
            id,
            run_key, name, description, dataset_id, dataset_version, dataset_version_id,
            evaluation_profile_id, evaluation_profile_version, profile_version_id, profile_hash,
            aggregation_policy_id, aggregation_policy_version, aggregation_policy_hash,
            agent_provider, agent_name, agent_version,
            prompt_config_id, prompt_config_version,
            config_snapshot,
            expected_execution_count
        )
        VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20::jsonb, $21)
        RETURNING
            id, run_key, name, description,
            dataset_id, dataset_version,
            evaluation_profile_id, evaluation_profile_version,
            aggregation_policy_id, aggregation_policy_version,
            agent_provider, agent_name, agent_version,
            prompt_config_id, prompt_config_version,
            config_snapshot,
            status::text as status,
            gate_status::text as gate_status,
            coordinator_id,
            coordinator_leased_until,
            coordinator_heartbeat_at,
            expected_execution_count,
            terminal_execution_count,
            passed_execution_count,
            failed_execution_count,
            errored_execution_count,
            summary,
            error_message,
            created_at,
            started_at,
            dispatched_at,
            finalized_at,
            completed_at,
            updated_at
        "#,
    )
    .bind(Uuid::now_v7())
    .bind(&draft.run_key)
    .bind(&draft.name)
    .bind(&draft.description)
    .bind(draft.dataset_id)
    .bind(&draft.dataset_version)
    .bind(draft.dataset_version_id)
    .bind(&draft.evaluation_profile_id)
    .bind(&draft.evaluation_profile_version)
    .bind(&draft.profile_version_id)
    .bind(&draft.profile_hash)
    .bind(&draft.aggregation_policy_id)
    .bind(&draft.aggregation_policy_version)
    .bind(&draft.aggregation_policy_hash)
    .bind(&draft.agent_provider)
    .bind(&draft.agent_name)
    .bind(&draft.agent_version)
    .bind(&draft.prompt_config_id)
    .bind(&draft.prompt_config_version)
    .bind(&draft.config_snapshot)
    .bind(draft.expected_execution_count)
    .fetch_one(db)
    .await?;

    Ok(run)
}

/// Finds a run by primary key.
pub(crate) async fn select_run_by_id<'e, E>(db: E, id: Uuid) -> anyhow::Result<Option<Run>>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let run = sqlx::query_as::<_, Run>(
        r#"
        SELECT
            id, run_key, name, description,
            dataset_id, dataset_version,
            evaluation_profile_id, evaluation_profile_version,
            aggregation_policy_id, aggregation_policy_version,
            agent_provider, agent_name, agent_version,
            prompt_config_id, prompt_config_version,
            config_snapshot,
            status::text as status,
            gate_status::text as gate_status,
            coordinator_id,
            coordinator_leased_until,
            coordinator_heartbeat_at,
            expected_execution_count,
            terminal_execution_count,
            passed_execution_count,
            failed_execution_count,
            errored_execution_count,
            summary,
            error_message,
            created_at,
            started_at,
            dispatched_at,
            finalized_at,
            completed_at,
            updated_at
        FROM runs
        WHERE id = $1 AND tenant_id = NULLIF(current_setting('vigilo.tenant_id', true), '')::uuid::uuid
        "#,
    )
    .bind(id)
    .fetch_optional(db)
    .await?;

    Ok(run)
}

/// Finds only the run profile payload from a run's config snapshot.
///
/// Worker hot paths use this instead of loading unrelated run metadata.
pub(crate) async fn select_run_profile_snapshot_by_id(
    db: &PgPool,
    id: Uuid,
) -> anyhow::Result<Option<serde_json::Value>> {
    let profile = sqlx::query_scalar::<_, Option<serde_json::Value>>(
        r#"
        SELECT config_snapshot->'profile' AS profile_snapshot
        FROM runs
        WHERE id = $1::uuid
        "#,
    )
    .bind(id)
    .fetch_optional(db)
    .await?;

    Ok(profile.flatten())
}

/// Finds a run by stable run key.
pub(crate) async fn select_run_by_key(db: &PgPool, run_key: &str) -> anyhow::Result<Option<Run>> {
    let run = sqlx::query_as::<_, Run>(
        r#"
        SELECT
            id, run_key, name, description,
            dataset_id, dataset_version,
            evaluation_profile_id, evaluation_profile_version,
            aggregation_policy_id, aggregation_policy_version,
            agent_provider, agent_name, agent_version,
            prompt_config_id, prompt_config_version,
            config_snapshot,
            status::text as status,
            gate_status::text as gate_status,
            coordinator_id,
            coordinator_leased_until,
            coordinator_heartbeat_at,
            expected_execution_count,
            terminal_execution_count,
            passed_execution_count,
            failed_execution_count,
            errored_execution_count,
            summary,
            error_message,
            created_at,
            started_at,
            dispatched_at,
            finalized_at,
            completed_at,
            updated_at
        FROM runs
        WHERE run_key = $1
        "#,
    )
    .bind(run_key)
    .fetch_optional(db)
    .await?;

    Ok(run)
}

/// Lists runs for management views using limit/offset pagination.
pub(crate) async fn list_runs(db: &PgPool, limit: i64, offset: i64) -> anyhow::Result<Vec<Run>> {
    let runs = sqlx::query_as::<_, Run>(
        r#"
        SELECT
            id, run_key, name, description,
            dataset_id, dataset_version,
            evaluation_profile_id, evaluation_profile_version,
            aggregation_policy_id, aggregation_policy_version,
            agent_provider, agent_name, agent_version,
            prompt_config_id, prompt_config_version,
            config_snapshot,
            status::text as status,
            gate_status::text as gate_status,
            coordinator_id,
            coordinator_leased_until,
            coordinator_heartbeat_at,
            expected_execution_count,
            terminal_execution_count,
            passed_execution_count,
            failed_execution_count,
            errored_execution_count,
            summary,
            error_message,
            created_at,
            started_at,
            dispatched_at,
            finalized_at,
            completed_at,
            updated_at
        FROM runs
        ORDER BY created_at DESC
        LIMIT $1 OFFSET $2
        "#,
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(db)
    .await?;

    Ok(runs)
}

/// Updates high-level run status, gate status, and error message.
pub(crate) async fn update_run_status(
    db: &PgPool,
    id: Uuid,
    patch: &RunPatch,
) -> anyhow::Result<Option<Run>> {
    let run = sqlx::query_as::<_, Run>(
        r#"
        UPDATE runs
        SET status = $2::run_status,
            gate_status = $3::gate_status,
            error_message = $4,
            updated_at = now()
        WHERE id = $1::uuid
        RETURNING
            id, run_key, name, description,
            dataset_id, dataset_version,
            evaluation_profile_id, evaluation_profile_version,
            aggregation_policy_id, aggregation_policy_version,
            agent_provider, agent_name, agent_version,
            prompt_config_id, prompt_config_version,
            config_snapshot,
            status::text as status,
            gate_status::text as gate_status,
            coordinator_id,
            coordinator_leased_until,
            coordinator_heartbeat_at,
            expected_execution_count,
            terminal_execution_count,
            passed_execution_count,
            failed_execution_count,
            errored_execution_count,
            summary,
            error_message,
            created_at,
            started_at,
            dispatched_at,
            finalized_at,
            completed_at,
            updated_at
        "#,
    )
    .bind(id)
    .bind(&patch.status)
    .bind(&patch.gate_status)
    .bind(&patch.error_message)
    .fetch_optional(db)
    .await?;

    Ok(run)
}

/// Deletes a run by primary key.
pub(crate) async fn delete_run_by_id(db: &PgPool, id: Uuid) -> anyhow::Result<u64> {
    let result = sqlx::query(
        r#"
        DELETE FROM runs
        WHERE id = $1::uuid
        "#,
    )
    .bind(id)
    .execute(db)
    .await?;

    Ok(result.rows_affected())
}
