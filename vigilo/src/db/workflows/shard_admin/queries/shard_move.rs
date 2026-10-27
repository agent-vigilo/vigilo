//! shard_move queries for shard administration.

use super::super::*;

pub(in crate::db::workflows::shard_admin) async fn select_latest_move_to_target(
    db: &PgPool,
    run_id: Uuid,
    run_shard: i16,
    target_database_alias: &str,
) -> anyhow::Result<Option<ShardMoveOperation>> {
    sqlx::query_as(
        r#"
        SELECT id, run_id, run_shard, source_database_alias,
               target_database_alias, starting_route_version, status, phase,
               target_reset_at, copied_row_count, copied_byte_count,
               claim_generation, claim_token
        FROM shard_move_operations
        WHERE run_id = $1::uuid
          AND run_shard = $2
          AND target_database_alias = $3
          AND status IN ('active', 'completed')
        ORDER BY CASE status WHEN 'active' THEN 0 ELSE 1 END,
                 completed_at DESC NULLS LAST
        LIMIT 1
        "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .bind(target_database_alias)
    .fetch_optional(db)
    .await
    .map_err(Into::into)
}

pub(in crate::db::workflows::shard_admin) async fn select_active_move_id(
    db: &PgPool,
    run_id: Uuid,
    run_shard: i16,
    source_database_alias: &str,
    target_database_alias: &str,
) -> anyhow::Result<Option<Uuid>> {
    sqlx::query_scalar(
        r#"
        SELECT id
        FROM shard_move_operations
        WHERE run_id = $1::uuid
          AND run_shard = $2
          AND source_database_alias = $3
          AND target_database_alias = $4
          AND status = 'active'
        "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .bind(source_database_alias)
    .bind(target_database_alias)
    .fetch_optional(db)
    .await
    .map_err(Into::into)
}

pub(in crate::db::workflows::shard_admin) async fn delete_move_capture(
    tx: &mut Transaction<'_, Postgres>,
    move_id: Uuid,
) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM shard_move_captures WHERE move_id = $1::uuid")
        .bind(move_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(in crate::db::workflows::shard_admin) async fn update_claimed_move_phase(
    db: &PgPool,
    move_id: Uuid,
    claim_token: Uuid,
    phase: &str,
) -> anyhow::Result<u64> {
    Ok(sqlx::query(
        r#"
        UPDATE shard_move_operations
        SET phase = $3,
            claimed_until = now() + make_interval(secs => $4),
            updated_at = now()
        WHERE id = $1::uuid
          AND status = 'active'
          AND claim_token = $2::uuid
        "#,
    )
    .bind(move_id)
    .bind(claim_token)
    .bind(phase)
    .bind(SHARD_MOVE_CLAIM_SECONDS)
    .execute(db)
    .await?
    .rows_affected())
}

pub(in crate::db::workflows::shard_admin) async fn mark_same_database_move_ready(
    db: &PgPool,
    move_id: Uuid,
    claim_token: Uuid,
) -> anyhow::Result<u64> {
    Ok(sqlx::query(
        r#"
        UPDATE shard_move_operations
        SET target_reset_at = COALESCE(target_reset_at, now()),
            phase = 'catch_up',
            updated_at = now()
        WHERE id = $1::uuid
          AND status = 'active'
          AND claim_token = $2::uuid
        "#,
    )
    .bind(move_id)
    .bind(claim_token)
    .execute(db)
    .await?
    .rows_affected())
}

pub(in crate::db::workflows::shard_admin) async fn mark_move_target_reset(
    db: &PgPool,
    move_id: Uuid,
    claim_token: Uuid,
) -> anyhow::Result<u64> {
    Ok(sqlx::query(
        r#"
        UPDATE shard_move_operations
        SET target_reset_at = now(),
            phase = 'backfill',
            claimed_until = now() + make_interval(secs => $3),
            updated_at = now()
        WHERE id = $1::uuid
          AND status = 'active'
          AND claim_token = $2::uuid
          AND target_reset_at IS NULL
        "#,
    )
    .bind(move_id)
    .bind(claim_token)
    .bind(SHARD_MOVE_CLAIM_SECONDS)
    .execute(db)
    .await?
    .rows_affected())
}

pub(in crate::db::workflows::shard_admin) async fn count_dirty_move_keys(
    db: &PgPool,
    move_id: Uuid,
) -> anyhow::Result<i64> {
    sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM shard_move_dirty_keys WHERE move_id = $1::uuid",
    )
    .bind(move_id)
    .fetch_one(db)
    .await
    .map_err(Into::into)
}

pub(in crate::db::workflows::shard_admin) async fn claim_shard_move_operation(
    control_db: &PgPool,
    current: &ShardPlacement,
    target_database_alias: &str,
    claim_token: Uuid,
) -> anyhow::Result<ShardMoveOperation> {
    let mut tx = control_db.begin().await?;
    validate_new_ownership_target(&mut tx, target_database_alias).await?;

    let existing = sqlx::query_as::<_, ShardMoveOperation>(
        r#"
        SELECT id, run_id, run_shard, source_database_alias,
               target_database_alias, starting_route_version, status, phase,
               target_reset_at, copied_row_count, copied_byte_count,
               claim_generation, claim_token
        FROM shard_move_operations
        WHERE run_id = $1::uuid
          AND run_shard = $2
          AND status = 'active'
        FOR UPDATE
        "#,
    )
    .bind(current.run_id)
    .bind(current.run_shard)
    .fetch_optional(tx.as_mut())
    .await?;

    let operation = if let Some(operation) = existing {
        if operation.source_database_alias != current.database_alias
            || operation.target_database_alias != target_database_alias
        {
            anyhow::bail!(
                "run {} shard {} already has an active move from {} to {}",
                current.run_id,
                current.run_shard,
                operation.source_database_alias,
                operation.target_database_alias
            );
        }
        operation
    } else {
        sqlx::query_as::<_, ShardMoveOperation>(
            r#"
            INSERT INTO shard_move_operations (
                run_id, run_shard, source_database_alias,
                target_database_alias, starting_route_version
            )
            VALUES ($1::uuid, $2, $3, $4, $5)
            RETURNING id, run_id, run_shard, source_database_alias,
                      target_database_alias, starting_route_version, status, phase,
                      target_reset_at, copied_row_count, copied_byte_count,
                      claim_generation, claim_token
            "#,
        )
        .bind(current.run_id)
        .bind(current.run_shard)
        .bind(&current.database_alias)
        .bind(target_database_alias)
        .bind(current.route_version)
        .fetch_one(tx.as_mut())
        .await?
    };

    let claimed = sqlx::query_as::<_, ShardMoveOperation>(
        r#"
        UPDATE shard_move_operations
        SET claim_token = $2::uuid,
            claim_generation = claim_generation + 1,
            claimed_until = now() + make_interval(secs => $3),
            updated_at = now()
        WHERE id = $1::uuid
          AND status = 'active'
          AND (
              claim_token IS NULL
              OR claimed_until < now()
              OR claim_token = $2::uuid
          )
        RETURNING id, run_id, run_shard, source_database_alias,
                  target_database_alias, starting_route_version, status, phase,
                  target_reset_at, copied_row_count, copied_byte_count,
                  claim_generation, claim_token
        "#,
    )
    .bind(operation.id)
    .bind(claim_token)
    .bind(SHARD_MOVE_CLAIM_SECONDS)
    .fetch_optional(tx.as_mut())
    .await?
    .ok_or_else(|| {
        anyhow::anyhow!(
            "run {} shard {} move is currently claimed by another worker",
            current.run_id,
            current.run_shard
        )
    })?;
    tx.commit().await?;
    Ok(claimed)
}

pub(in crate::db::workflows::shard_admin) async fn renew_shard_move_claim(
    control_db: &PgPool,
    move_id: Uuid,
    claim_token: Uuid,
) -> anyhow::Result<()> {
    let updated = sqlx::query(
        r#"
        UPDATE shard_move_operations
        SET claimed_until = now() + make_interval(secs => $3),
            updated_at = now()
        WHERE id = $1::uuid
          AND status = 'active'
          AND claim_token = $2::uuid
        "#,
    )
    .bind(move_id)
    .bind(claim_token)
    .bind(SHARD_MOVE_CLAIM_SECONDS)
    .execute(control_db)
    .await?
    .rows_affected();
    if updated != 1 {
        anyhow::bail!("shard move {} lost its operation claim", move_id);
    }
    Ok(())
}

pub(in crate::db::workflows::shard_admin) async fn release_shard_move_claim(
    control_db: &PgPool,
    move_id: Uuid,
    claim_token: Uuid,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE shard_move_operations
        SET claim_token = NULL,
            claimed_until = NULL,
            updated_at = now()
        WHERE id = $1::uuid
          AND claim_token = $2::uuid
        "#,
    )
    .bind(move_id)
    .bind(claim_token)
    .execute(control_db)
    .await?;
    Ok(())
}

pub(in crate::db::workflows::shard_admin) async fn settle_aborted_move_operation(
    control_db: &PgPool,
    move_id: Uuid,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE shard_move_operations
        SET status = 'aborted',
            phase = 'aborted',
            claim_token = NULL,
            claimed_until = NULL,
            completed_at = now(),
            updated_at = now()
        WHERE id = $1::uuid
          AND status = 'active'
        "#,
    )
    .bind(move_id)
    .execute(control_db)
    .await?;
    Ok(())
}

pub(in crate::db::workflows::shard_admin) async fn settle_completed_move_operation(
    control_db: &PgPool,
    move_id: Uuid,
    claim_token: Option<Uuid>,
) -> anyhow::Result<()> {
    let updated = sqlx::query(
        r#"
        UPDATE shard_move_operations
        SET status = 'completed',
            phase = 'completed',
            claim_token = NULL,
            claimed_until = NULL,
            completed_at = COALESCE(completed_at, now()),
            updated_at = now()
        WHERE id = $1::uuid
          AND status = 'active'
          AND ($2::uuid IS NULL OR claim_token = $2::uuid)
        "#,
    )
    .bind(move_id)
    .bind(claim_token)
    .execute(control_db)
    .await?
    .rows_affected();
    if updated != 1 {
        anyhow::bail!("shard move {} could not be marked completed", move_id);
    }
    Ok(())
}

pub(in crate::db::workflows::shard_admin) async fn enable_shard_move_capture(
    source_db: &PgPool,
    move_id: Uuid,
    run_id: Uuid,
    run_shard: i16,
) -> anyhow::Result<()> {
    let mut tx = source_db.begin().await?;
    crate::db::shard_write_fence::lock_exclusive(&mut tx, run_id, run_shard).await?;
    let captured_move = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO shard_move_captures (move_id, run_id, run_shard, active)
        VALUES ($1::uuid, $2::uuid, $3, true)
        ON CONFLICT (run_id, run_shard) DO UPDATE
        SET active = true
        WHERE shard_move_captures.move_id = EXCLUDED.move_id
        RETURNING move_id
        "#,
    )
    .bind(move_id)
    .bind(run_id)
    .bind(run_shard)
    .fetch_optional(tx.as_mut())
    .await?;
    if captured_move != Some(move_id) {
        anyhow::bail!(
            "run {} shard {} already has a different source capture",
            run_id,
            run_shard
        );
    }
    tx.commit().await?;
    Ok(())
}

pub(in crate::db::workflows::shard_admin) async fn select_move_source_page(
    source_db: &PgPool,
    table: &str,
    run_id: Uuid,
    run_shard: i16,
    start_after_key: Option<&str>,
) -> anyhow::Result<Vec<MoveSourceRow>> {
    let key_expression = move_key_expression("source_row", move_table_key_columns(table)?);
    let filter = match table {
        "dataset_versions" => {
            "EXISTS (
                SELECT 1 FROM runs run
                WHERE run.id = $1::uuid
                  AND run.dataset_version_id = source_row.dataset_version_id
            ) AND $2::smallint = $2"
        }
        "runs" => "source_row.id = $1::uuid AND $2::smallint = $2",
        _ => "source_row.run_id = $1::uuid AND source_row.run_shard = $2",
    };
    let sql = format!(
        r#"
        SELECT
            to_jsonb(source_row) AS row,
            {key_expression} AS row_key,
            octet_length(to_jsonb(source_row)::text)::integer AS row_bytes
        FROM {table} source_row
        WHERE {filter}
          AND ($3::text IS NULL OR {key_expression} > $3)
        ORDER BY {key_expression}
        LIMIT $4
        "#
    );
    let mut candidates = sqlx::query_as::<_, MoveSourceRow>(&sql)
        .bind(run_id)
        .bind(run_shard)
        .bind(start_after_key)
        .bind(SHARD_MOVE_COPY_BATCH_SIZE as i64)
        .fetch(source_db);
    let mut page = Vec::new();
    let mut row_bytes = Vec::new();
    while let Some(row) = candidates.try_next().await? {
        row_bytes.push(row.row_bytes.max(0) as usize);
        if bounded_page_len(
            &row_bytes,
            SHARD_MOVE_COPY_BATCH_SIZE,
            SHARD_MOVE_COPY_BATCH_BYTES,
        ) < row_bytes.len()
        {
            break;
        }
        page.push(row);
    }
    Ok(page)
}

pub(in crate::db::workflows::shard_admin) async fn select_last_move_page(
    control_db: &PgPool,
    move_id: Uuid,
    table: &str,
) -> anyhow::Result<(i64, Option<String>)> {
    let page = sqlx::query_as::<_, (i64, String)>(
        r#"
        SELECT completed_page_count, last_end_key
        FROM shard_move_table_progress
        WHERE move_id = $1::uuid
          AND table_name = $2
        "#,
    )
    .bind(move_id)
    .bind(table)
    .fetch_optional(control_db)
    .await?;
    Ok(page
        .map(|(page_count, end_key)| (page_count, Some(end_key)))
        .unwrap_or((0, None)))
}

pub(in crate::db::workflows::shard_admin) async fn record_completed_move_page(
    control_db: &PgPool,
    move_id: Uuid,
    claim_token: Uuid,
    table: &str,
    page_number: i64,
    start_after_key: Option<&str>,
    rows: &[MoveSourceRow],
) -> anyhow::Result<()> {
    let checkpoint = move_page_checkpoint(rows)?;

    let mut tx = control_db.begin().await?;
    let advanced = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO shard_move_table_progress (
            move_id, table_name, completed_page_count,
            last_start_after_key, last_end_key,
            copied_row_count, copied_byte_count, last_page_checksum
        )
        VALUES ($1::uuid, $2, 1, $4, $5, $6, $7, $8)
        ON CONFLICT (move_id, table_name) DO UPDATE
        SET completed_page_count =
                shard_move_table_progress.completed_page_count + 1,
            last_start_after_key = EXCLUDED.last_start_after_key,
            last_end_key = EXCLUDED.last_end_key,
            copied_row_count =
                shard_move_table_progress.copied_row_count
                + EXCLUDED.copied_row_count,
            copied_byte_count =
                shard_move_table_progress.copied_byte_count
                + EXCLUDED.copied_byte_count,
            last_page_checksum = EXCLUDED.last_page_checksum,
            updated_at = now()
        WHERE shard_move_table_progress.completed_page_count = $3
          AND shard_move_table_progress.last_end_key IS NOT DISTINCT FROM $4
        RETURNING completed_page_count
        "#,
    )
    .bind(move_id)
    .bind(table)
    .bind(page_number)
    .bind(start_after_key)
    .bind(&checkpoint.end_key)
    .bind(checkpoint.row_count)
    .bind(checkpoint.byte_count)
    .bind(&checkpoint.checksum)
    .fetch_optional(tx.as_mut())
    .await?;
    if advanced.is_some_and(|page_count| page_count != page_number + 1) {
        anyhow::bail!(
            "shard move {} table {} cannot checkpoint page {} without its predecessor",
            move_id,
            table,
            page_number
        );
    }
    if advanced.is_none() {
        let existing = sqlx::query_as::<_, (i64, Option<String>, String)>(
            r#"
            SELECT completed_page_count, last_start_after_key, last_end_key
            FROM shard_move_table_progress
            WHERE move_id = $1::uuid
              AND table_name = $2
            "#,
        )
        .bind(move_id)
        .bind(table)
        .fetch_optional(tx.as_mut())
        .await?;
        if existing.as_ref()
            != Some(&(
                page_number + 1,
                start_after_key.map(str::to_string),
                checkpoint.end_key.clone(),
            ))
        {
            anyhow::bail!(
                "shard move {} table {} has a non-contiguous page checkpoint",
                move_id,
                table
            );
        }
    }
    let acknowledged_row_count = if advanced.is_some() {
        checkpoint.row_count
    } else {
        0
    };
    let acknowledged_byte_count = if advanced.is_some() {
        checkpoint.byte_count
    } else {
        0
    };
    let updated = sqlx::query(
        r#"
        UPDATE shard_move_operations
        SET phase = 'backfill',
            copied_row_count = copied_row_count + $3,
            copied_byte_count = copied_byte_count + $4,
            claimed_until = now() + make_interval(secs => $5),
            updated_at = now()
        WHERE id = $1::uuid
          AND status = 'active'
          AND claim_token = $2::uuid
        "#,
    )
    .bind(move_id)
    .bind(claim_token)
    .bind(acknowledged_row_count)
    .bind(acknowledged_byte_count)
    .bind(SHARD_MOVE_CLAIM_SECONDS)
    .execute(tx.as_mut())
    .await?
    .rows_affected();
    if updated != 1 {
        anyhow::bail!(
            "shard move {} lost its claim before page acknowledgement",
            move_id
        );
    }
    tx.commit().await?;
    Ok(())
}

pub(in crate::db::workflows::shard_admin) async fn select_dirty_shard_keys_for_table(
    source_db: &PgPool,
    move_id: Uuid,
    table: &str,
    key_columns: &[&str],
    current_row_exists: bool,
    limit: usize,
) -> anyhow::Result<Vec<DirtyShardKey>> {
    let predicate = key_join_predicate("source_row", "journal", key_columns);
    let existence = if current_row_exists {
        "EXISTS"
    } else {
        "NOT EXISTS"
    };
    let sql = format!(
        r#"
        SELECT journal.table_name, journal.row_key, journal.change_version
        FROM shard_move_dirty_keys journal
        WHERE journal.move_id = $1::uuid
          AND journal.table_name = $2
          AND {existence} (
              SELECT 1
              FROM {table} source_row
              WHERE {predicate}
          )
        ORDER BY journal.last_changed_at, journal.row_key
        LIMIT $3
        "#
    );
    Ok(sqlx::query_as::<_, DirtyShardKey>(&sql)
        .bind(move_id)
        .bind(table)
        .bind(limit as i64)
        .fetch_all(source_db)
        .await?)
}

pub(in crate::db::workflows::shard_admin) async fn select_current_rows_for_dirty_keys(
    source_db: &PgPool,
    table: &str,
    key_columns: &[&str],
    keys: &[DirtyShardKey],
) -> anyhow::Result<BTreeMap<String, Value>> {
    if keys.is_empty() {
        return Ok(BTreeMap::new());
    }
    let predicate = key_join_predicate("source_row", "dirty", key_columns);
    let sql = format!(
        r#"
        WITH dirty AS (
            SELECT value AS row_key
            FROM jsonb_array_elements($1::jsonb)
        )
        SELECT dirty.row_key, to_jsonb(source_row)
        FROM dirty
        JOIN {table} source_row ON {predicate}
        "#
    );
    let rows = sqlx::query_as::<_, (Value, Value)>(&sql)
        .bind(Json(Value::Array(
            keys.iter().map(|key| key.row_key.clone()).collect(),
        )))
        .fetch_all(source_db)
        .await?;
    Ok(rows
        .into_iter()
        .map(|(key, row)| (key.to_string(), row))
        .collect())
}

pub(in crate::db::workflows::shard_admin) async fn delete_target_rows_for_dirty_keys(
    tx: &mut Transaction<'_, Postgres>,
    table: &str,
    key_columns: &[&str],
    keys: &[Value],
) -> anyhow::Result<()> {
    if keys.is_empty() {
        return Ok(());
    }
    let predicate = key_join_predicate("target_row", "dirty", key_columns);
    let sql = format!(
        r#"
        WITH dirty AS (
            SELECT value AS row_key
            FROM jsonb_array_elements($1::jsonb)
        )
        DELETE FROM {table} target_row
        USING dirty
        WHERE {predicate}
        "#
    );
    sqlx::query(&sql)
        .bind(Json(Value::Array(keys.to_vec())))
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(in crate::db::workflows::shard_admin) async fn settle_replayed_dirty_keys(
    source_db: &PgPool,
    move_id: Uuid,
    keys: &[DirtyShardKey],
) -> anyhow::Result<()> {
    if keys.is_empty() {
        return Ok(());
    }
    let replayed = keys
        .iter()
        .map(|key| {
            serde_json::json!({
                "table_name": key.table_name,
                "row_key": key.row_key,
                "change_version": key.change_version,
            })
        })
        .collect::<Vec<_>>();
    sqlx::query(
        r#"
        WITH replayed AS (
            SELECT table_name, row_key, change_version
            FROM jsonb_to_recordset($2::jsonb) AS key(
                table_name text,
                row_key jsonb,
                change_version bigint
            )
        )
        DELETE FROM shard_move_dirty_keys journal
        USING replayed
        WHERE journal.move_id = $1::uuid
          AND journal.table_name = replayed.table_name
          AND journal.row_key = replayed.row_key
          AND journal.change_version = replayed.change_version
        "#,
    )
    .bind(move_id)
    .bind(Json(replayed))
    .execute(source_db)
    .await?;
    Ok(())
}

pub(in crate::db::workflows::shard_admin) async fn checkpoint_move_reports(
    control_db: &PgPool,
    move_id: Uuid,
) -> anyhow::Result<Vec<ShardMoveTableReport>> {
    let page_rows = sqlx::query_as::<_, (String, i64)>(
        r#"
        SELECT
            table_name,
            copied_row_count
        FROM shard_move_table_progress
        WHERE move_id = $1::uuid
        "#,
    )
    .bind(move_id)
    .fetch_all(control_db)
    .await?;
    let pages = page_rows.into_iter().collect::<BTreeMap<_, _>>();
    Ok(move_table_names()
        .map(|table| {
            let rows = pages.get(table).copied().unwrap_or_default();
            ShardMoveTableReport {
                table,
                source_row_count: None,
                target_row_count: None,
                copied_row_count: rows as u64,
                source_checksum: None,
                target_checksum: None,
                verification_mode: "checkpoint_and_replay",
                verified: true,
            }
        })
        .collect())
}

/// Removes only the target's non-authoritative rows for one run shard.
///
/// Source ownership remains unchanged while this commits. Deletes run in
/// reverse dependency order. The caller must hold target-side exclusive
/// admission and validate the current move fence in this transaction.
pub(in crate::db::workflows::shard_admin) async fn reset_target_shard_rows(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    run_shard: i16,
) -> anyhow::Result<()> {
    for table in SHARD_TABLES.iter().rev() {
        let sql = format!(
            "DELETE FROM {} WHERE run_id = $1::uuid AND run_shard = $2",
            table.name
        );
        sqlx::query(&sql)
            .bind(run_id)
            .bind(run_shard)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

pub(in crate::db::workflows::shard_admin) async fn copy_json_rows(
    tx: &mut Transaction<'_, Postgres>,
    table: &str,
    rows: Vec<Value>,
) -> anyhow::Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }

    let sql = format!(
        r#"
        INSERT INTO {table}
        SELECT *
        FROM jsonb_populate_recordset(NULL::{table}, $1::jsonb)
        ON CONFLICT DO NOTHING
        "#
    );

    let result = sqlx::query(&sql)
        .bind(Json(Value::Array(rows.clone())))
        .execute(&mut **tx)
        .await?;

    verify_prerequisite_rows(tx, table, &rows).await?;
    Ok(result.rows_affected())
}

pub(in crate::db::workflows::shard_admin) async fn verify_prerequisite_rows(
    tx: &mut Transaction<'_, Postgres>,
    table: &str,
    source_rows: &[Value],
) -> anyhow::Result<()> {
    let (key_column, key_cast) = match table {
        "dataset_versions" => ("dataset_version_id", "uuid"),
        "runs" => ("id", "uuid"),
        _ => anyhow::bail!("unsupported prerequisite table {}", table),
    };
    let sql = format!(
        r#"
        WITH expected AS (
            SELECT value AS row
            FROM jsonb_array_elements($1::jsonb)
        )
        SELECT to_jsonb(target_row)
        FROM expected
        JOIN {table} target_row
          ON target_row.{key_column} = (expected.row->>'{key_column}')::{key_cast}
        "#
    );
    let target_rows = sqlx::query_scalar::<_, Value>(&sql)
        .bind(Json(Value::Array(source_rows.to_vec())))
        .fetch_all(&mut **tx)
        .await?;
    let normalize = |row: &Value| -> anyhow::Result<(String, Value)> {
        let key = row
            .get(key_column)
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("{} prerequisite row is missing {}", table, key_column))?
            .to_string();
        Ok((key, normalized_prerequisite_row(table, row.clone())?))
    };
    let expected = source_rows
        .iter()
        .map(normalize)
        .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
    let actual = target_rows
        .iter()
        .map(normalize)
        .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
    if expected != actual {
        anyhow::bail!(
            "{} prerequisite rows conflict with immutable target data",
            table
        );
    }
    Ok(())
}

pub(in crate::db::workflows::shard_admin) async fn upsert_json_rows(
    tx: &mut Transaction<'_, Postgres>,
    table: &str,
    key_columns: &[&str],
    rows: Vec<Value>,
) -> anyhow::Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    let columns = sqlx::query_scalar::<_, String>(
        r#"
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = $1
        ORDER BY ordinal_position
        "#,
    )
    .bind(table)
    .fetch_all(&mut **tx)
    .await?;
    if columns.is_empty() {
        anyhow::bail!("shard move target table {} was not found", table);
    }
    let quote = |identifier: &str| format!("\"{}\"", identifier.replace('"', "\"\""));
    let conflict_columns = key_columns
        .iter()
        .map(|column| quote(column))
        .collect::<Vec<_>>()
        .join(", ");
    let updates = columns
        .iter()
        .filter(|column| !key_columns.contains(&column.as_str()))
        .map(|column| {
            let quoted = quote(column);
            format!("{quoted} = EXCLUDED.{quoted}")
        })
        .collect::<Vec<_>>();
    let conflict_action = if updates.is_empty() {
        "DO NOTHING".to_string()
    } else {
        format!("DO UPDATE SET {}", updates.join(", "))
    };
    let sql = format!(
        r#"
        INSERT INTO {table}
        SELECT *
        FROM jsonb_populate_recordset(NULL::{table}, $1::jsonb)
        ON CONFLICT ({conflict_columns}) {conflict_action}
        "#
    );
    let result = sqlx::query(&sql)
        .bind(Json(Value::Array(rows)))
        .execute(&mut **tx)
        .await?;
    Ok(result.rows_affected())
}

pub(in crate::db::workflows::shard_admin) async fn prerequisite_table_fingerprint_with<'e, E>(
    executor: E,
    table: &str,
    run_id: Uuid,
    run_shard: i16,
) -> anyhow::Result<TableFingerprint>
where
    E: Executor<'e, Database = Postgres>,
{
    let sql = match table {
        "dataset_versions" => {
            r#"
            SELECT (to_jsonb(dv) - 'created_at' - 'updated_at')::text AS row_json
            FROM dataset_versions dv
            JOIN runs r
              ON r.dataset_version_id = dv.dataset_version_id
            WHERE r.id = $1::uuid
              AND $2::smallint = $2
            ORDER BY row_json
            "#
        }
        "runs" => {
            r#"
            SELECT (
                to_jsonb(r)
                - 'status'
                - 'gate_status'
                - 'coordinator_id'
                - 'coordinator_leased_until'
                - 'coordinator_heartbeat_at'
                - 'terminal_execution_count'
                - 'passed_execution_count'
                - 'failed_execution_count'
                - 'errored_execution_count'
                - 'summary'
                - 'error_message'
                - 'created_at'
                - 'started_at'
                - 'dispatched_at'
                - 'finalized_at'
                - 'completed_at'
                - 'updated_at'
            )::text AS row_json
            FROM runs r
            WHERE r.id = $1::uuid
              AND $2::smallint = $2
            ORDER BY row_json
            "#
        }
        _ => anyhow::bail!("unsupported prerequisite table {}", table),
    };

    let rows = sqlx::query_scalar::<_, String>(sql)
        .bind(run_id)
        .bind(run_shard)
        .fetch(executor);
    fingerprint_ordered_rows(rows).await
}

pub(in crate::db::workflows::shard_admin) async fn table_fingerprint_with<'e, E>(
    executor: E,
    table: &str,
    run_id: Uuid,
    run_shard: i16,
) -> anyhow::Result<TableFingerprint>
where
    E: Executor<'e, Database = Postgres>,
{
    let sql = format!(
        r#"
        SELECT to_jsonb(t)::text AS row_json
        FROM {table} t
        WHERE run_id = $1::uuid
          AND run_shard = $2
        ORDER BY row_json
        "#
    );

    let rows = sqlx::query_scalar::<_, String>(&sql)
        .bind(run_id)
        .bind(run_shard)
        .fetch(executor);
    fingerprint_ordered_rows(rows).await
}
