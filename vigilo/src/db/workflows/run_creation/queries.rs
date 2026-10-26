//! PostgreSQL operations for durable run creation.

use super::*;

pub(super) async fn claim_newly_persisted_run(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: Uuid,
    owner_id: Uuid,
    lease_seconds: i32,
) -> anyhow::Result<u64> {
    Ok(sqlx::query(
        r#"
        UPDATE runs
        SET coordinator_id = $2::uuid,
            coordinator_leased_until = now() + make_interval(secs => $3),
            coordinator_heartbeat_at = now(),
            updated_at = now()
        WHERE id = $1::uuid
          AND status = 'creating'::run_status
        "#,
    )
    .bind(run_id)
    .bind(owner_id)
    .bind(lease_seconds)
    .execute(&mut **tx)
    .await?
    .rows_affected())
}

pub(super) async fn fail_pending_creation_placements(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: Uuid,
    owner_id: Uuid,
    error: &str,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE run_creation_placements
        SET status = 'failed',
            last_error = $3,
            updated_at = now()
        WHERE run_id = $1::uuid
          AND status = 'pending'
          AND EXISTS (
              SELECT 1
              FROM runs
              WHERE id = $1::uuid
                AND status = 'creating'::run_status
                AND coordinator_id = $2::uuid
          )
        "#,
    )
    .bind(run_id)
    .bind(owner_id)
    .bind(error)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Returns persisted placement progress for status and watch projections.
pub(crate) async fn select_creation_progress(
    db: &PgPool,
    run_id: Uuid,
) -> anyhow::Result<RunCreationProgress> {
    let row = sqlx::query_as::<
        _,
        (
            i64,
            i64,
            i64,
            i64,
            i64,
            Option<String>,
            Option<String>,
            Option<i32>,
        ),
    >(
        r#"
        WITH placement_progress AS (
        SELECT
            COUNT(*)::bigint AS placement_count,
            COUNT(*) FILTER (WHERE status = 'pending')::bigint,
            COUNT(*) FILTER (WHERE status = 'seeded')::bigint,
            COUNT(*) FILTER (WHERE status = 'failed')::bigint,
            COALESCE(SUM(attempt_count), 0)::bigint,
            (
                SELECT latest.last_error
                FROM run_creation_placements latest
                WHERE latest.run_id = $1::uuid
                  AND latest.last_error IS NOT NULL
                ORDER BY latest.updated_at DESC, latest.database_alias
                LIMIT 1
            )
        FROM run_creation_placements
        WHERE run_id = $1::uuid
        )
        SELECT placement_progress.*, plan.status, plan.chunk_cursor
        FROM placement_progress
        LEFT JOIN run_plans plan ON plan.run_id=$1
        "#,
    )
    .bind(run_id)
    .fetch_one(db)
    .await?;

    Ok(RunCreationProgress {
        placement_count: row.0,
        pending_placement_count: row.1,
        seeded_placement_count: row.2,
        failed_placement_count: row.3,
        attempt_count: row.4,
        last_error: row.5,
        plan_status: row.6,
        plan_chunk_cursor: row.7,
    })
}

pub(super) async fn insert_creation_placements(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    run_id: Uuid,
    projections_by_alias: &BTreeMap<String, Vec<RunShardCaseDraft>>,
) -> anyhow::Result<()> {
    let mut query = QueryBuilder::<Postgres>::new(
        "INSERT INTO run_creation_placements \
         (run_id, database_alias, status, expected_case_count, case_projection_hash) ",
    );
    query.push_values(projections_by_alias, |mut row, (alias, projection)| {
        row.push_bind(run_id)
            .push_bind(alias)
            .push_bind("pending")
            .push_bind(projection.len() as i64)
            .push_bind(case_projection::projection_hash(projection));
    });
    query.build().execute(tx.as_mut()).await?;
    Ok(())
}

pub(super) async fn insert_creation_chunks(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    run_id: Uuid,
    chunks_by_alias: &BTreeMap<String, Vec<RunChunkDraft>>,
) -> anyhow::Result<()> {
    let planned_chunks = chunks_by_alias
        .iter()
        .flat_map(|(alias, chunks)| chunks.iter().map(move |chunk| (alias, chunk)))
        .collect::<Vec<_>>();
    let mut query = QueryBuilder::<Postgres>::new(
        "INSERT INTO run_creation_chunks (run_id, database_alias, chunk_id, run_shard, profile_group_id, ordinal_start, ordinal_end) ",
    );
    query.push_values(planned_chunks, |mut row, (alias, chunk)| {
        row.push_bind(run_id)
            .push_bind(alias)
            .push_bind(chunk.chunk_id)
            .push_bind(chunk.run_shard)
            .push_bind(&chunk.profile_group_id)
            .push_bind(chunk.ordinal_start)
            .push_bind(chunk.ordinal_end);
    });
    query.build().execute(tx.as_mut()).await?;
    Ok(())
}

pub(super) async fn yield_claimed_run(
    db: &PgPool,
    run_id: Uuid,
    owner_id: Uuid,
) -> anyhow::Result<()> {
    let updated = sqlx::query(
        r#"
        UPDATE runs
        SET coordinator_id = NULL,
            coordinator_leased_until = now() + make_interval(secs => $3),
            coordinator_heartbeat_at = NULL,
            error_message = NULL,
            updated_at = now()
        WHERE id = $1::uuid
          AND status = 'creating'::run_status
          AND coordinator_id = $2::uuid
        "#,
    )
    .bind(run_id)
    .bind(owner_id)
    .bind(CREATION_RETRY_DELAY_SECONDS)
    .execute(db)
    .await?
    .rows_affected();
    if updated != 1 {
        anyhow::bail!(
            "run creation '{}' lost ownership while yielding its page budget",
            run_id
        );
    }
    Ok(())
}

pub(super) async fn finish_claimed_run(
    db: &PgPool,
    run_id: Uuid,
    owner_id: Uuid,
) -> anyhow::Result<()> {
    if let Err(error) = activate_claimed_run(db, run_id, owner_id).await {
        if run_create::is_seed_invariant_error(&error) {
            fail_claimed_run(db, run_id, owner_id, &error.to_string()).await?;
        } else {
            defer_run(db, run_id, owner_id, &error.to_string()).await?;
        }
    }
    Ok(())
}

pub(super) async fn select_pending_placements(
    db: &PgPool,
    run_id: Uuid,
    owner_id: Uuid,
) -> anyhow::Result<Vec<String>> {
    let aliases = sqlx::query_scalar::<_, String>(
        r#"
        SELECT creation.database_alias
        FROM run_creation_placements creation
        JOIN runs run ON run.id = creation.run_id
        WHERE creation.run_id = $1::uuid
          AND creation.status = 'pending'
          AND run.status = 'creating'::run_status
          AND run.coordinator_id = $2::uuid
        ORDER BY creation.database_alias
        "#,
    )
    .bind(run_id)
    .bind(owner_id)
    .fetch_all(db)
    .await?;
    Ok(aliases)
}

pub(super) async fn start_placement_attempt(
    db: &PgPool,
    run_id: Uuid,
    owner_id: Uuid,
    database_alias: &str,
    lease_seconds: i32,
) -> anyhow::Result<()> {
    let updated = sqlx::query(
        r#"
        WITH owned_run AS (
            UPDATE runs
            SET coordinator_leased_until = now() + make_interval(secs => $4),
                coordinator_heartbeat_at = now(),
                updated_at = now()
            WHERE id = $1::uuid
              AND status = 'creating'::run_status
              AND coordinator_id = $2::uuid
            RETURNING id
        )
        UPDATE run_creation_placements creation
        SET attempt_count = attempt_count + 1,
            last_error = NULL,
            updated_at = now()
        FROM owned_run
        WHERE creation.run_id = owned_run.id
          AND creation.database_alias = $3
          AND creation.status = 'pending'
        "#,
    )
    .bind(run_id)
    .bind(owner_id)
    .bind(database_alias)
    .bind(effective_creation_lease_seconds(lease_seconds))
    .execute(db)
    .await?
    .rows_affected();
    if updated != 1 {
        anyhow::bail!(
            "run creation '{}' is no longer owned while seeding placement '{}'",
            run_id,
            database_alias
        );
    }
    Ok(())
}

pub(super) async fn select_projection_seed_page(
    control_db: &PgPool,
    run_id: Uuid,
    database_alias: &str,
    after_ordinal: Option<i32>,
    limit: usize,
) -> anyhow::Result<Vec<RunShardCaseDraft>> {
    let rows = sqlx::query_as::<_, RunShardCaseDraft>(
        r#"
        SELECT
            plan.run_id,
            plan.run_shard,
            run.dataset_version_id,
            membership.case_id,
            membership.case_ordinal,
            membership.case_hash
        FROM run_creation_chunks plan
        JOIN runs run ON run.id = plan.run_id
        JOIN dataset_version_cases membership
          ON membership.dataset_version_id = run.dataset_version_id
         AND membership.case_ordinal >= plan.ordinal_start
         AND membership.case_ordinal < plan.ordinal_end
        WHERE plan.run_id = $1::uuid
          AND plan.database_alias = $2
          AND ($3::integer IS NULL OR membership.case_ordinal > $3)
        ORDER BY membership.case_ordinal, membership.case_id
        LIMIT $4
        "#,
    )
    .bind(run_id)
    .bind(database_alias)
    .bind(after_ordinal)
    .bind(limit as i64)
    .fetch_all(control_db)
    .await?;
    Ok(rows)
}

pub(super) async fn select_projection_page_blobs(
    control_db: &PgPool,
    rows: &[RunShardCaseDraft],
) -> anyhow::Result<Vec<CaseBlobDraft>> {
    let expected_hashes = rows
        .iter()
        .map(|row| row.case_hash.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let hashes = expected_hashes.iter().copied().collect::<Vec<_>>();
    let blobs = sqlx::query_as::<_, CaseBlobDraft>(
        r#"
        SELECT
            case_hash, task_type, case_group, input_payload,
            expected_output, context_payload, tags, metadata
        FROM case_blobs
        WHERE case_hash = ANY($1::text[])
        ORDER BY case_hash
        "#,
    )
    .bind(&hashes)
    .fetch_all(control_db)
    .await?;
    if blobs.len() != expected_hashes.len() {
        return Err(run_create::seed_invariant_error(
            "projection page references a missing canonical case blob",
        ));
    }
    Ok(blobs)
}

pub(super) async fn select_placement_seed_progress(
    db: &PgPool,
    run_id: Uuid,
    database_alias: &str,
) -> anyhow::Result<PlacementSeedProgress> {
    sqlx::query_as::<_, PlacementSeedProgress>(
        r#"
        SELECT expected_case_count, seeded_case_count,
               last_seeded_case_ordinal, case_projection_hash
        FROM run_creation_placements
        WHERE run_id = $1::uuid
          AND database_alias = $2
          AND status = 'pending'
        "#,
    )
    .bind(run_id)
    .bind(database_alias)
    .fetch_optional(db)
    .await?
    .ok_or_else(|| {
        anyhow::anyhow!(
            "run creation '{}' placement '{}' is no longer pending",
            run_id,
            database_alias
        )
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn acknowledge_projection_page(
    db: &PgPool,
    run_id: Uuid,
    owner_id: Uuid,
    database_alias: &str,
    expected_count: i64,
    expected_ordinal: Option<i32>,
    page_count: i64,
    page_last_ordinal: i32,
    lease_seconds: i32,
) -> anyhow::Result<()> {
    let updated = sqlx::query(
        r#"
        WITH owned_run AS (
            UPDATE runs
            SET coordinator_leased_until = now() + make_interval(secs => $8),
                coordinator_heartbeat_at = now(),
                updated_at = now()
            WHERE id = $1::uuid
              AND status = 'creating'::run_status
              AND coordinator_id = $2::uuid
            RETURNING id
        )
        UPDATE run_creation_placements creation
        SET seeded_case_count = seeded_case_count + $6,
            last_seeded_case_ordinal = $7,
            updated_at = now()
        FROM owned_run
        WHERE creation.run_id = owned_run.id
          AND creation.database_alias = $3
          AND creation.status = 'pending'
          AND creation.seeded_case_count = $4
          AND creation.last_seeded_case_ordinal IS NOT DISTINCT FROM $5
        "#,
    )
    .bind(run_id)
    .bind(owner_id)
    .bind(database_alias)
    .bind(expected_count)
    .bind(expected_ordinal)
    .bind(page_count)
    .bind(page_last_ordinal)
    .bind(effective_creation_lease_seconds(lease_seconds))
    .execute(db)
    .await?
    .rows_affected();
    if updated != 1 {
        anyhow::bail!(
            "run creation '{}' lost ownership while acknowledging placement '{}' through ordinal {}",
            run_id,
            database_alias,
            page_last_ordinal
        );
    }
    Ok(())
}

pub(super) async fn select_creation_chunks(
    db: &PgPool,
    run_id: Uuid,
    database_alias: &str,
) -> anyhow::Result<Vec<RunChunkDraft>> {
    let chunks = sqlx::query_as::<_, RunChunkDraft>(
        r#"
        SELECT
            chunk_id,
            run_shard,
            profile_group_id,
            ordinal_start,
            ordinal_end
        FROM run_creation_chunks
        WHERE run_id = $1::uuid
          AND database_alias = $2
        ORDER BY run_shard, ordinal_start, chunk_id
        "#,
    )
    .bind(run_id)
    .bind(database_alias)
    .fetch_all(db)
    .await?;
    if chunks.is_empty() {
        return Err(run_create::seed_invariant_error(format!(
            "run creation '{}' has no chunk plan for placement '{}'",
            run_id, database_alias
        )));
    }
    Ok(chunks)
}

pub(super) async fn mark_placement_seeded(
    db: &PgPool,
    run_id: Uuid,
    owner_id: Uuid,
    database_alias: &str,
    lease_seconds: i32,
) -> anyhow::Result<()> {
    let updated = sqlx::query(
        r#"
        WITH owned_run AS (
            UPDATE runs
            SET coordinator_leased_until = now() + make_interval(secs => $4),
                coordinator_heartbeat_at = now(),
                updated_at = now()
            WHERE id = $1::uuid
              AND status = 'creating'::run_status
              AND coordinator_id = $2::uuid
            RETURNING id
        )
        UPDATE run_creation_placements creation
        SET status = 'seeded',
            last_error = NULL,
            seeded_at = now(),
            updated_at = now()
        FROM owned_run
        WHERE creation.run_id = owned_run.id
          AND creation.database_alias = $3
          AND creation.status = 'pending'
          AND creation.seeded_case_count = creation.expected_case_count
        "#,
    )
    .bind(run_id)
    .bind(owner_id)
    .bind(database_alias)
    .bind(effective_creation_lease_seconds(lease_seconds))
    .execute(db)
    .await?
    .rows_affected();
    if updated != 1 {
        anyhow::bail!(
            "run creation '{}' lost ownership before placement '{}' was recorded as seeded",
            run_id,
            database_alias
        );
    }
    Ok(())
}

pub(super) async fn defer_placement(
    db: &PgPool,
    run_id: Uuid,
    owner_id: Uuid,
    database_alias: &str,
    error: &str,
) -> anyhow::Result<()> {
    let mut tx = db.begin().await?;
    let updated = sqlx::query(
        r#"
        WITH owned_run AS (
            UPDATE runs
            SET error_message = $4,
                coordinator_leased_until = now() + make_interval(secs => $5),
                coordinator_heartbeat_at = now(),
                updated_at = now()
            WHERE id = $1::uuid
              AND status = 'creating'::run_status
              AND coordinator_id = $2::uuid
            RETURNING id
        )
        UPDATE run_creation_placements creation
        SET last_error = $4,
            updated_at = now()
        FROM owned_run
        WHERE creation.run_id = owned_run.id
          AND creation.database_alias = $3
          AND creation.status = 'pending'
        "#,
    )
    .bind(run_id)
    .bind(owner_id)
    .bind(database_alias)
    .bind(error)
    .bind(CREATION_RETRY_DELAY_SECONDS)
    .execute(tx.as_mut())
    .await?
    .rows_affected();
    if updated != 1 {
        anyhow::bail!(
            "run creation '{}' lost ownership while deferring placement '{}'",
            run_id,
            database_alias
        );
    }
    tx.commit().await?;
    warn!(run_id = %run_id, database_alias, error, "deferred run creation placement for retry");
    Ok(())
}

pub(super) async fn defer_run(
    db: &PgPool,
    run_id: Uuid,
    owner_id: Uuid,
    error: &str,
) -> anyhow::Result<()> {
    let updated = sqlx::query(
        r#"
        UPDATE runs
        SET error_message = $3,
            coordinator_leased_until = now() + make_interval(secs => $4),
            coordinator_heartbeat_at = now(),
            updated_at = now()
        WHERE id = $1::uuid
          AND status = 'creating'::run_status
          AND coordinator_id = $2::uuid
        "#,
    )
    .bind(run_id)
    .bind(owner_id)
    .bind(error)
    .bind(CREATION_RETRY_DELAY_SECONDS)
    .execute(db)
    .await?
    .rows_affected();
    if updated != 1 {
        anyhow::bail!(
            "run creation '{}' lost ownership while deferring activation",
            run_id
        );
    }
    warn!(run_id = %run_id, error, "deferred run creation activation for retry");
    Ok(())
}

pub(super) async fn fail_placement_and_run(
    db: &PgPool,
    run_id: Uuid,
    owner_id: Uuid,
    database_alias: &str,
    error: &str,
) -> anyhow::Result<()> {
    let mut tx = db.begin().await?;
    let placement_updated = sqlx::query(
        r#"
        UPDATE run_creation_placements
        SET status = 'failed',
            last_error = $4,
            updated_at = now()
        WHERE run_id = $1::uuid
          AND database_alias = $3
          AND status = 'pending'
          AND EXISTS (
              SELECT 1
              FROM runs
              WHERE id = $1::uuid
                AND status = 'creating'::run_status
                AND coordinator_id = $2::uuid
          )
        "#,
    )
    .bind(run_id)
    .bind(owner_id)
    .bind(database_alias)
    .bind(error)
    .execute(tx.as_mut())
    .await?
    .rows_affected();
    if placement_updated != 1 {
        anyhow::bail!(
            "run creation '{}' lost ownership before placement '{}' could fail",
            run_id,
            database_alias
        );
    }
    mark_run_failed(&mut tx, run_id, owner_id, error).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn mark_run_failed(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    run_id: Uuid,
    owner_id: Uuid,
    error: &str,
) -> anyhow::Result<()> {
    let updated = sqlx::query(
        r#"
        UPDATE runs
        SET status = 'failed'::run_status,
            error_message = $3,
            coordinator_id = NULL,
            coordinator_leased_until = NULL,
            coordinator_heartbeat_at = NULL,
            completed_at = now(),
            updated_at = now()
        WHERE id = $1::uuid
          AND status = 'creating'::run_status
          AND coordinator_id = $2::uuid
        "#,
    )
    .bind(run_id)
    .bind(owner_id)
    .bind(error)
    .execute(tx.as_mut())
    .await?
    .rows_affected();
    if updated != 1 {
        anyhow::bail!(
            "run creation '{}' lost ownership before failure was recorded",
            run_id
        );
    }
    Ok(())
}

pub(super) async fn activate_claimed_run(
    db: &PgPool,
    run_id: Uuid,
    owner_id: Uuid,
) -> anyhow::Result<()> {
    let sql_seed:bool=sqlx::query_scalar(
        "SELECT creation_kind='case-projection' AND NOT EXISTS(SELECT 1 FROM run_plans WHERE run_id=$1) FROM runs WHERE id=$1"
    ).bind(run_id).fetch_optional(db).await?.unwrap_or(false);
    anyhow::ensure!(
        sql_seed,
        "immutable run plans require execution admission before activation"
    );
    let mut tx = db.begin().await?;
    let (total, pending, failed) = sqlx::query_as::<_, (i64, i64, i64)>(
        r#"
        SELECT
            COUNT(*)::bigint,
            COUNT(*) FILTER (WHERE status = 'pending')::bigint,
            COUNT(*) FILTER (WHERE status = 'failed')::bigint
        FROM run_creation_placements
        WHERE run_id = $1::uuid
        "#,
    )
    .bind(run_id)
    .fetch_one(tx.as_mut())
    .await?;
    if total == 0 {
        let error = run_create::seed_invariant_error(format!(
            "run creation '{}' has no placement ledger rows",
            run_id
        ));
        tx.rollback().await?;
        return Err(error);
    }
    if pending > 0 || failed > 0 {
        tx.rollback().await?;
        return Ok(());
    }

    let chunks = sqlx::query_as::<_, RunChunkDraft>(
        r#"
        SELECT
            chunk_id,
            run_shard,
            profile_group_id,
            ordinal_start,
            ordinal_end
        FROM run_creation_chunks
        WHERE run_id = $1::uuid
        ORDER BY run_shard, ordinal_start, chunk_id
        "#,
    )
    .bind(run_id)
    .fetch_all(tx.as_mut())
    .await?;
    if chunks.is_empty() {
        let error = run_create::seed_invariant_error(format!(
            "run creation '{}' has no persisted chunk plan",
            run_id
        ));
        tx.rollback().await?;
        return Err(error);
    }
    run_create::bulk_insert_run_shard_dispatch_cursors(&mut tx, run_id, &chunks).await?;

    let activated = sqlx::query(
        r#"
        UPDATE runs
        SET status = 'pending'::run_status,
            error_message = NULL,
            coordinator_id = NULL,
            coordinator_leased_until = NULL,
            coordinator_heartbeat_at = NULL,
            updated_at = now()
        WHERE id = $1::uuid
          AND status = 'creating'::run_status
          AND coordinator_id = $2::uuid
        "#,
    )
    .bind(run_id)
    .bind(owner_id)
    .execute(tx.as_mut())
    .await?
    .rows_affected();
    if activated != 1 {
        anyhow::bail!("run creation '{}' lost ownership before activation", run_id);
    }

    sqlx::query("DELETE FROM run_creation_chunks WHERE run_id = $1::uuid")
        .bind(run_id)
        .execute(tx.as_mut())
        .await?;
    tx.commit().await?;
    debug!(run_id = %run_id, "activated fully seeded run creation");
    Ok(())
}

pub(super) async fn claim_next_creating_run(
    db: &PgPool,
    owner_id: Uuid,
    lease_seconds: i32,
) -> anyhow::Result<Option<Uuid>> {
    let run_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        WITH candidate AS (
            SELECT id
            FROM runs
            WHERE status = 'creating'::run_status
              AND creation_kind = 'case-projection'
              AND NOT EXISTS (SELECT 1 FROM run_plans WHERE run_id = runs.id)
              AND (
                  coordinator_leased_until IS NULL
                  OR coordinator_leased_until < now()
              )
            ORDER BY created_at, id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        UPDATE runs run
        SET coordinator_id = $1::uuid,
            coordinator_leased_until = now() + make_interval(secs => $2),
            coordinator_heartbeat_at = now(),
            updated_at = now()
        FROM candidate
        WHERE run.id = candidate.id
        RETURNING run.id
        "#,
    )
    .bind(owner_id)
    .bind(effective_creation_lease_seconds(lease_seconds))
    .fetch_optional(db)
    .await?;
    Ok(run_id)
}

pub(super) async fn load_seed_material(
    db: &PgPool,
    run_id: Uuid,
) -> anyhow::Result<OwnedRunSeedMaterial> {
    let draft = sqlx::query_as::<_, RunDraft>(
        r#"
        SELECT
            run_key,
            name,
            description,
            dataset_id,
            dataset_version,
            dataset_version_id,
            evaluation_profile_id,
            evaluation_profile_version,
            profile_version_id,
            profile_hash,
            aggregation_policy_id,
            aggregation_policy_version,
            aggregation_policy_hash,
            agent_provider,
            agent_name,
            agent_version,
            prompt_config_id,
            prompt_config_version,
            config_snapshot,
            expected_execution_count
        FROM runs
        WHERE id = $1::uuid
          AND status = 'creating'::run_status
        "#,
    )
    .bind(run_id)
    .fetch_optional(db)
    .await?
    .ok_or_else(|| {
        run_create::seed_invariant_error(format!(
            "creating run '{}' is missing its control run definition",
            run_id
        ))
    })?;

    Ok(OwnedRunSeedMaterial { draft })
}

pub(super) async fn select_creation_outcome(
    db: &PgPool,
    run_id: Uuid,
) -> anyhow::Result<RunCreationOutcome> {
    let (status, error_message) = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT status::text, error_message FROM runs WHERE id = $1::uuid",
    )
    .bind(run_id)
    .fetch_one(db)
    .await?;
    Ok(RunCreationOutcome {
        status,
        progress: select_creation_progress(db, run_id).await?,
        error_message,
    })
}
