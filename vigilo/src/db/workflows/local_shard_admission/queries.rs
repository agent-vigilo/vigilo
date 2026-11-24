//! PostgreSQL operations for execution-local shard admission.

use sqlx::{
    Executor,
    Postgres,
};
use uuid::Uuid;

use super::{
    LocalShardAdmission,
    LocalShardAdmissionDraft,
    LocalShardAdmissionState,
};

const LOCAL_SHARD_ADMISSION_COLUMNS: &str = r#"
    run_id, run_shard, database_alias, write_epoch, state,
    redirect_database_alias, move_id, move_claim_generation,
    move_claim_token
"#;

pub(crate) async fn select_local_shard_admission<'e, E>(
    executor: E,
    run_id: Uuid,
    run_shard: i16,
) -> anyhow::Result<Option<LocalShardAdmission>>
where
    E: Executor<'e, Database = Postgres>,
{
    Ok(sqlx::query_as::<_, LocalShardAdmission>(&format!(
        r#"
        SELECT {LOCAL_SHARD_ADMISSION_COLUMNS}
        FROM local_shard_admissions
        WHERE run_id = $1
          AND run_shard = $2
        "#
    ))
    .bind(run_id)
    .bind(run_shard)
    .fetch_optional(executor)
    .await?)
}

pub(crate) async fn upsert_local_shard_admission<'e, E>(
    executor: E,
    draft: LocalShardAdmissionDraft,
) -> anyhow::Result<LocalShardAdmission>
where
    E: Executor<'e, Database = Postgres>,
{
    if draft.state == LocalShardAdmissionState::Prepared || draft.move_fence.is_some() {
        anyhow::bail!("prepared shard admission must use the target move fence installer");
    }
    let admission = sqlx::query_as::<_, LocalShardAdmission>(
        r#"
        INSERT INTO local_shard_admissions (
            run_id, run_shard, database_alias, write_epoch, state,
            redirect_database_alias, move_id, move_claim_generation,
            move_claim_token
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (run_id, run_shard) DO UPDATE
        SET database_alias = EXCLUDED.database_alias,
            write_epoch = EXCLUDED.write_epoch,
            state = EXCLUDED.state,
            redirect_database_alias = EXCLUDED.redirect_database_alias,
            move_id = EXCLUDED.move_id,
            move_claim_generation = EXCLUDED.move_claim_generation,
            move_claim_token = EXCLUDED.move_claim_token,
            updated_at = now()
        WHERE local_shard_admissions.write_epoch < EXCLUDED.write_epoch
           OR (
                local_shard_admissions.write_epoch = EXCLUDED.write_epoch
                AND local_shard_admissions.database_alias = EXCLUDED.database_alias
                AND local_shard_admissions.state = EXCLUDED.state
           )
        RETURNING run_id, run_shard, database_alias, write_epoch, state,
                  redirect_database_alias, move_id, move_claim_generation,
                  move_claim_token
        "#,
    )
    .bind(draft.run_id)
    .bind(draft.run_shard)
    .bind(&draft.database_alias)
    .bind(draft.write_epoch)
    .bind(draft.state.as_str())
    .bind(&draft.redirect_database_alias)
    .bind(draft.move_fence.map(|fence| fence.move_id))
    .bind(draft.move_fence.map(|fence| fence.claim_generation))
    .bind(draft.move_fence.map(|fence| fence.claim_token))
    .fetch_one(executor)
    .await?;
    Ok(admission)
}

/// Advances local authority monotonically, with explicit same-epoch states.
pub(crate) async fn transition_local_shard_admission<'e, E>(
    executor: E,
    draft: LocalShardAdmissionDraft,
    allowed_same_epoch_states: &[LocalShardAdmissionState],
) -> anyhow::Result<LocalShardAdmission>
where
    E: Executor<'e, Database = Postgres>,
{
    if draft.state == LocalShardAdmissionState::Prepared || draft.move_fence.is_some() {
        anyhow::bail!("prepared shard admission must use the target move fence installer");
    }
    let allowed_states = allowed_same_epoch_states
        .iter()
        .map(|state| state.as_str())
        .collect::<Vec<_>>();
    let admission = sqlx::query_as::<_, LocalShardAdmission>(
        r#"
        INSERT INTO local_shard_admissions (
            run_id, run_shard, database_alias, write_epoch, state,
            redirect_database_alias, move_id, move_claim_generation,
            move_claim_token
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (run_id, run_shard) DO UPDATE
        SET database_alias = EXCLUDED.database_alias,
            write_epoch = EXCLUDED.write_epoch,
            state = EXCLUDED.state,
            redirect_database_alias = EXCLUDED.redirect_database_alias,
            move_id = EXCLUDED.move_id,
            move_claim_generation = EXCLUDED.move_claim_generation,
            move_claim_token = EXCLUDED.move_claim_token,
            updated_at = now()
        WHERE local_shard_admissions.write_epoch < EXCLUDED.write_epoch
           OR (
                local_shard_admissions.write_epoch = EXCLUDED.write_epoch
                AND local_shard_admissions.state = ANY($10::text[])
           )
        RETURNING run_id, run_shard, database_alias, write_epoch, state,
                  redirect_database_alias, move_id, move_claim_generation,
                  move_claim_token
        "#,
    )
    .bind(draft.run_id)
    .bind(draft.run_shard)
    .bind(&draft.database_alias)
    .bind(draft.write_epoch)
    .bind(draft.state.as_str())
    .bind(&draft.redirect_database_alias)
    .bind(draft.move_fence.map(|fence| fence.move_id))
    .bind(draft.move_fence.map(|fence| fence.claim_generation))
    .bind(draft.move_fence.map(|fence| fence.claim_token))
    .bind(allowed_states)
    .fetch_optional(executor)
    .await?
    .ok_or_else(|| {
        anyhow::anyhow!(
            "local shard admission for run {} shard {} rejected transition to {} epoch {}",
            draft.run_id,
            draft.run_shard,
            draft.state,
            draft.write_epoch
        )
    })?;
    Ok(admission)
}

/// Installs monotonically newer target-side mover authority.
/// An existing admission must be unable to admit workers. Same-epoch retries
/// must match the prepared alias and move identity.
pub(crate) async fn install_local_shard_move_fence<'e, E>(
    executor: E,
    draft: LocalShardAdmissionDraft,
) -> anyhow::Result<LocalShardAdmission>
where
    E: Executor<'e, Database = Postgres>,
{
    let fence = draft
        .move_fence
        .ok_or_else(|| anyhow::anyhow!("prepared shard move admission requires a move fence"))?;
    if draft.state != LocalShardAdmissionState::Prepared {
        anyhow::bail!("target move fence can only install prepared admission");
    }
    if fence.claim_generation <= 0 {
        anyhow::bail!("target move fence generation must be positive");
    }
    let admission = sqlx::query_as::<_, LocalShardAdmission>(
        r#"
        INSERT INTO local_shard_admissions (
            run_id, run_shard, database_alias, write_epoch, state,
            redirect_database_alias, move_id, move_claim_generation,
            move_claim_token
        )
        VALUES ($1, $2, $3, $4, 'prepared', $5, $6, $7, $8)
        ON CONFLICT (run_id, run_shard) DO UPDATE
        SET database_alias = EXCLUDED.database_alias,
            write_epoch = EXCLUDED.write_epoch,
            state = EXCLUDED.state,
            redirect_database_alias = EXCLUDED.redirect_database_alias,
            move_id = EXCLUDED.move_id,
            move_claim_generation = EXCLUDED.move_claim_generation,
            move_claim_token = EXCLUDED.move_claim_token,
            updated_at = now()
        WHERE (local_shard_admissions.write_epoch < EXCLUDED.write_epoch
             AND local_shard_admissions.state IN ('closed', 'prepared'))
           OR (
                local_shard_admissions.write_epoch = EXCLUDED.write_epoch
                AND local_shard_admissions.state = 'prepared'
                AND local_shard_admissions.database_alias = EXCLUDED.database_alias
                AND local_shard_admissions.move_id = EXCLUDED.move_id
                AND (
                    local_shard_admissions.move_claim_generation < EXCLUDED.move_claim_generation
                    OR (
                        local_shard_admissions.move_claim_generation = EXCLUDED.move_claim_generation
                        AND local_shard_admissions.move_claim_token = EXCLUDED.move_claim_token
                    )
                )
           )
        RETURNING run_id, run_shard, database_alias, write_epoch, state,
                  redirect_database_alias, move_id, move_claim_generation,
                  move_claim_token
        "#,
    )
    .bind(draft.run_id)
    .bind(draft.run_shard)
    .bind(&draft.database_alias)
    .bind(draft.write_epoch)
    .bind(&draft.redirect_database_alias)
    .bind(fence.move_id)
    .bind(fence.claim_generation)
    .bind(fence.claim_token)
    .fetch_optional(executor)
    .await?
    .ok_or_else(|| {
        anyhow::anyhow!(
            "local shard admission for run {} shard {} rejected stale move {} generation {}",
            draft.run_id,
            draft.run_shard,
            fence.move_id,
            fence.claim_generation
        )
    })?;
    Ok(admission)
}
