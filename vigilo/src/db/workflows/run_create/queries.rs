//! PostgreSQL operations for run seed persistence.

use super::*;

/// Creates or verifies a dataset version identity.
///
/// Existing dataset version ids must refer to the same dataset and version
/// value; otherwise run creation fails to preserve immutable dataset identity.
///
/// Reuse performs no UPDATE: independent runs must not serialize on this shared
/// immutable row or generate WAL merely to verify an existing identity.
pub(crate) async fn upsert_dataset_version(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    dataset_version_id: Uuid,
    dataset_id: Uuid,
    dataset_version: &str,
) -> anyhow::Result<()> {
    sqlx::query("INSERT INTO dataset_versions(dataset_version_id,dataset_id,dataset_version) VALUES($1,$2,$3) ON CONFLICT DO NOTHING")
        .bind(dataset_version_id).bind(dataset_id).bind(dataset_version).execute(&mut **tx).await?;
    let matches:bool=sqlx::query_scalar("SELECT dataset_id=$2 AND dataset_version=$3 FROM dataset_versions WHERE dataset_version_id=$1")
        .bind(dataset_version_id).bind(dataset_id).bind(dataset_version).fetch_one(&mut **tx).await?;
    if !matches {
        return Err(seed_invariant_error(format!(
            "dataset_version_id '{dataset_version_id}' already exists with different dataset identity"
        )));
    }
    Ok(())
}

/// Inserts or verifies a run row using the caller-provided id.
///
/// The control copy starts in `creating`; execution copies start in `pending`
/// but remain invisible because control dispatch cursors do not exist yet.
/// Repeated seeds accept only an identical immutable run definition.
pub(crate) async fn insert_run_create(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    run_id: Uuid,
    draft: &RunDraft,
    status: &str,
) -> anyhow::Result<()> {
    let rows_affected = sqlx::query(
        r#"
        INSERT INTO runs (
            id,
            run_key,
            name,
            description,
            dataset_id,
            dataset_version,
            evaluation_profile_id,
            evaluation_profile_version,
            aggregation_policy_id,
            aggregation_policy_version,
            agent_provider,
            agent_name,
            agent_version,
            prompt_config_id,
            prompt_config_version,
            config_snapshot,
            expected_execution_count,
            dataset_version_id,
            profile_version_id,
            profile_hash,
            aggregation_policy_hash,
            status
        )
        VALUES (
            $1::uuid,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8,
            $9,
            $10,
            $11,
            $12,
            $13,
            $14,
            $15,
            $16::jsonb,
            $17,
            $18,
            $19,
            $20,
            $21,
            $22::run_status
        )
        ON CONFLICT (id) DO UPDATE
        SET updated_at = runs.updated_at
        WHERE runs.run_key = EXCLUDED.run_key
          AND runs.name IS NOT DISTINCT FROM EXCLUDED.name
          AND runs.description IS NOT DISTINCT FROM EXCLUDED.description
          AND runs.dataset_id = EXCLUDED.dataset_id
          AND runs.dataset_version = EXCLUDED.dataset_version
          AND runs.dataset_version_id = EXCLUDED.dataset_version_id
          AND runs.evaluation_profile_id = EXCLUDED.evaluation_profile_id
          AND runs.evaluation_profile_version = EXCLUDED.evaluation_profile_version
          AND runs.profile_version_id = EXCLUDED.profile_version_id
          AND runs.profile_hash = EXCLUDED.profile_hash
          AND runs.aggregation_policy_id = EXCLUDED.aggregation_policy_id
          AND runs.aggregation_policy_version = EXCLUDED.aggregation_policy_version
          AND runs.aggregation_policy_hash = EXCLUDED.aggregation_policy_hash
          AND runs.agent_provider = EXCLUDED.agent_provider
          AND runs.agent_name = EXCLUDED.agent_name
          AND runs.agent_version IS NOT DISTINCT FROM EXCLUDED.agent_version
          AND runs.prompt_config_id = EXCLUDED.prompt_config_id
          AND runs.prompt_config_version = EXCLUDED.prompt_config_version
          AND runs.config_snapshot = EXCLUDED.config_snapshot
          AND runs.expected_execution_count = EXCLUDED.expected_execution_count
          AND runs.status = EXCLUDED.status
        "#,
    )
    .bind(run_id)
    .bind(&draft.run_key)
    .bind(&draft.name)
    .bind(&draft.description)
    .bind(draft.dataset_id)
    .bind(&draft.dataset_version)
    .bind(&draft.evaluation_profile_id)
    .bind(&draft.evaluation_profile_version)
    .bind(&draft.aggregation_policy_id)
    .bind(&draft.aggregation_policy_version)
    .bind(&draft.agent_provider)
    .bind(&draft.agent_name)
    .bind(&draft.agent_version)
    .bind(&draft.prompt_config_id)
    .bind(&draft.prompt_config_version)
    .bind(&draft.config_snapshot)
    .bind(draft.expected_execution_count)
    .bind(draft.dataset_version_id)
    .bind(&draft.profile_version_id)
    .bind(&draft.profile_hash)
    .bind(&draft.aggregation_policy_hash)
    .bind(status)
    .execute(tx.as_mut())
    .await?
    .rows_affected();

    if rows_affected != 1 {
        return Err(seed_invariant_error(format!(
            "run_id '{}' already exists with different immutable creation data",
            run_id
        )));
    }

    Ok(())
}

/// Inserts pending chunk rows for the run.
///
/// Chunk-ready outbox event records are created by dispatch after the run is
/// marked running.
///
/// Query behavior: bulk inserts run-local chunk ranges in bounded batches. A
/// repeated seed accepts an existing chunk only when all immutable scheduling
/// fields match and the chunk has not been dispatched.
pub(crate) async fn bulk_insert_run_chunks(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    run_id: Uuid,
    dataset_version_id: Uuid,
    chunks: &[RunChunkDraft],
) -> anyhow::Result<()> {
    if chunks.is_empty() {
        return Ok(());
    }

    for chunk_batch in chunks.chunks(RUN_CHUNK_INSERT_CHUNK_SIZE) {
        let mut query_builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO run_chunks (id, run_id, run_shard, dataset_version_id, profile_group_id, ordinal_start, ordinal_end, status) ",
        );

        query_builder.push_values(chunk_batch, |mut b, chunk| {
            b.push_bind(chunk.chunk_id)
                .push_bind(run_id)
                .push_bind(chunk.run_shard)
                .push_bind(dataset_version_id)
                .push_bind(&chunk.profile_group_id)
                .push_bind(chunk.ordinal_start)
                .push_bind(chunk.ordinal_end)
                .push_bind("pending");
        });

        query_builder.push(
            r#"
            ON CONFLICT (run_id, run_shard, id) DO UPDATE
            SET updated_at = run_chunks.updated_at
            WHERE run_chunks.dataset_version_id = EXCLUDED.dataset_version_id
              AND run_chunks.profile_group_id = EXCLUDED.profile_group_id
              AND run_chunks.ordinal_start = EXCLUDED.ordinal_start
              AND run_chunks.ordinal_end = EXCLUDED.ordinal_end
              AND run_chunks.status = 'pending'
              AND run_chunks.dispatched_at IS NULL
            "#,
        );
        let rows_affected = query_builder
            .build()
            .execute(tx.as_mut())
            .await?
            .rows_affected();

        if rows_affected != chunk_batch.len() as u64 {
            return Err(seed_invariant_error(format!(
                "run_id '{}' has a chunk with different immutable creation data",
                run_id
            )));
        }
    }

    Ok(())
}

/// Inserts one dispatch cursor per run shard used by the run's chunk set.
///
/// Dispatch cursors let coordinators claim a `(run_id, run_shard)` pair before
/// selecting chunks, keeping dispatch scans aligned with the `run_chunks`
/// partition key.
pub(crate) async fn bulk_insert_run_shard_dispatch_cursors(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    run_id: Uuid,
    chunks: &[RunChunkDraft],
) -> anyhow::Result<()> {
    if chunks.is_empty() {
        return Ok(());
    }

    let run_shards = chunks
        .iter()
        .map(|chunk| chunk.run_shard)
        .collect::<BTreeSet<_>>();

    let mut query_builder = QueryBuilder::<Postgres>::new(
        "INSERT INTO run_shard_dispatch_cursors (run_id, run_shard, status) ",
    );

    query_builder.push_values(run_shards, |mut b, run_shard| {
        b.push_bind(run_id).push_bind(run_shard).push_bind("open");
    });

    query_builder.push(" ON CONFLICT (run_id, run_shard) DO NOTHING");
    query_builder.build().execute(tx.as_mut()).await?;

    Ok(())
}

/// Inserts one active execution placement row per run shard used by the run.
///
/// The chunk planner already assigned `run_shard` values. This workflow stores
/// the durable routing decision for each distinct `run_id + run_shard` before
/// the run can be dispatched.
pub(crate) async fn bulk_insert_shard_placements(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    run_id: Uuid,
    assignments: &[RunShardPlacementAssignment],
) -> anyhow::Result<()> {
    if assignments.is_empty() {
        return Ok(());
    }

    let database_aliases = assignments
        .iter()
        .map(|assignment| assignment.database_alias.as_str())
        .collect::<BTreeSet<_>>();
    for database_alias in database_aliases {
        validate_active_shard_capable_placement(tx, database_alias).await?;
    }

    let mut query_builder = QueryBuilder::<Postgres>::new(
        "INSERT INTO shard_placements (run_id, run_shard, database_alias, status) ",
    );

    query_builder.push_values(assignments, |mut b, assignment| {
        b.push_bind(run_id)
            .push_bind(assignment.run_shard)
            .push_bind(&assignment.database_alias)
            .push_bind(SHARD_PLACEMENT_STATUS_ACTIVE);
    });

    query_builder.build().execute(tx.as_mut()).await?;

    Ok(())
}

async fn validate_active_shard_capable_placement(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    database_alias: &str,
) -> anyhow::Result<()> {
    let placement = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT role, status
        FROM database_placements
        WHERE alias = $1
        FOR SHARE
        "#,
    )
    .bind(database_alias)
    .fetch_optional(tx.as_mut())
    .await?;

    let Some((role, status)) = placement else {
        anyhow::bail!(
            "database placement alias {} is not configured",
            database_alias
        );
    };

    if status != "active" {
        anyhow::bail!(
            "database placement alias {} has status {}, which cannot receive new shard placements",
            database_alias,
            status
        );
    }

    if !matches!(
        role.as_str(),
        DATABASE_PLACEMENT_ROLE_SHARD | DATABASE_PLACEMENT_ROLE_CONTROL_AND_SHARD
    ) {
        anyhow::bail!(
            "database placement alias {} has role {}, which is not shard-capable",
            database_alias,
            role
        );
    }

    Ok(())
}
