//! PostgreSQL operations for chunk processing.

use super::*;

/// Claims a pending or expired chunk for a prepared run shard.
///
/// Returns `None` when another worker already owns the current lease or the
/// chunk's run is not currently processable.
///
/// Query behavior: one guarded `UPDATE ... RETURNING` claims ownership only if
/// the chunk is pending or its previous lease has expired and the execution
/// placement has a local run snapshot. Each claim receives an opaque token that
/// remains stable while heartbeats extend its deadline.
pub(super) async fn claim_chunk_for_processing_in_transaction(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: Uuid,
    run_shard: i16,
    chunk_id: Uuid,
    lease_seconds: i32,
) -> anyhow::Result<Option<RunChunk>> {
    let updated = sqlx::query_as::<_, RunChunk>(
        r#"
		UPDATE run_chunks
		SET status = 'leased',
			lease_token = gen_random_uuid(),
			leased_until = now() + ($4::int * interval '1 second'),
			updated_at = now()
		WHERE run_id = $1::uuid
          AND run_shard = $2
		  AND id = $3::uuid
		  AND (
			status = 'pending'
			OR (status = 'leased' AND leased_until < now())
		  )
		  AND EXISTS (
			SELECT 1
			FROM run_snapshots rs
			WHERE rs.run_id = run_chunks.run_id
			  AND rs.run_shard = run_chunks.run_shard
		  )
		RETURNING
			id,
			run_id,
            run_shard,
			dataset_version_id,
			profile_group_id,
			ordinal_start,
			ordinal_end,
			status,
			lease_token,
			leased_until,
			created_at,
			updated_at
		"#,
    )
    .bind(run_id)
    .bind(run_shard)
    .bind(chunk_id)
    .bind(lease_seconds)
    .fetch_optional(&mut **tx)
    .await?;

    Ok(updated)
}

/// Completes an owned lease and returns its locked, persisted receipt identity.
pub(super) async fn mark_chunk_completed(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chunk: &RunChunk,
    lease_token: Uuid,
) -> anyhow::Result<Option<(i32, i32, String)>> {
    Ok(sqlx::query_as(
        r#"
        UPDATE run_chunks
        SET status = 'completed',
            lease_token = NULL,
            leased_until = NULL,
            updated_at = now()
        WHERE run_id = $1::uuid
          AND run_shard = $2
          AND id = $3::uuid
          AND status = 'leased'
          AND lease_token = $4::uuid
          AND leased_until >= now()
          AND EXISTS (
            SELECT 1
            FROM local_shard_admissions admission
            WHERE admission.run_id = run_chunks.run_id
              AND admission.run_shard = run_chunks.run_shard
              AND admission.write_epoch = $5
              AND admission.state IN ('open', 'draining')
          )
        RETURNING ordinal_start, ordinal_end, status
        "#,
    )
    .bind(chunk.run_id)
    .bind(chunk.run_shard)
    .bind(chunk.id)
    .bind(lease_token)
    .bind(chunk.write_epoch)
    .fetch_optional(&mut **tx)
    .await?)
}

/// Fails an owned lease and returns its locked, persisted receipt identity.
pub(super) async fn mark_chunk_failed(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chunk: &RunChunk,
    lease_token: Uuid,
) -> anyhow::Result<Option<(i32, i32, String)>> {
    Ok(sqlx::query_as(
        r#"
        UPDATE run_chunks
        SET status = 'failed',
            lease_token = NULL,
            leased_until = NULL,
            updated_at = now()
        WHERE run_id = $1::uuid
          AND run_shard = $2
          AND id = $3::uuid
          AND status = 'leased'
          AND lease_token = $4::uuid
          AND leased_until >= now()
          AND EXISTS (SELECT 1 FROM local_shard_admissions a
              WHERE a.run_id = run_chunks.run_id AND a.run_shard = run_chunks.run_shard
                AND a.write_epoch = $5 AND a.state IN ('open', 'draining'))
        RETURNING ordinal_start, ordinal_end, status
        "#,
    )
    .bind(chunk.run_id)
    .bind(chunk.run_shard)
    .bind(chunk.id)
    .bind(lease_token)
    .bind(chunk.write_epoch)
    .fetch_optional(&mut **tx)
    .await?)
}

/// Extends a currently owned chunk lease and returns the updated lease token.
///
/// Lease ownership is represented by the opaque claim token. Heartbeats retain
/// that token while advancing the deadline.
///
/// Query behavior: updates an unexpired lease only for its current token and
/// while the execution placement still has a local run snapshot. `None` means
/// the worker lost authority and should acknowledge the stale message.
pub(crate) async fn extend_chunk_lease(
    db: &PgPool,
    chunk: &RunChunk,
    lease_seconds: i32,
) -> anyhow::Result<Option<RunChunk>> {
    let lease_token = require_chunk_lease_token(chunk)?;
    let mut updated = sqlx::query_as::<_, RunChunk>(
        r#"
		UPDATE run_chunks
		SET leased_until = now() + ($4::int * interval '1 second'),
			updated_at = now()
		WHERE run_id = $1::uuid
          AND run_shard = $2
		  AND id = $3::uuid
		  AND status = 'leased'
		  AND lease_token = $5::uuid
		  AND leased_until >= now()
		  AND EXISTS (
			SELECT 1
			FROM local_shard_admissions admission
			WHERE admission.run_id = run_chunks.run_id
			  AND admission.run_shard = run_chunks.run_shard
			  AND admission.write_epoch = $6
			  AND admission.state IN ('open', 'draining')
		  )
		  AND EXISTS (
			SELECT 1
			FROM run_snapshots rs
			WHERE rs.run_id = run_chunks.run_id
			  AND rs.run_shard = run_chunks.run_shard
		  )
		RETURNING
			id,
			run_id,
            run_shard,
			dataset_version_id,
			profile_group_id,
			ordinal_start,
			ordinal_end,
			status,
			lease_token,
			leased_until,
			created_at,
			updated_at
		"#,
    )
    .bind(chunk.run_id)
    .bind(chunk.run_shard)
    .bind(chunk.id)
    .bind(lease_seconds)
    .bind(lease_token)
    .bind(chunk.write_epoch)
    .fetch_optional(db)
    .await?;

    if let Some(updated) = &mut updated {
        updated.write_epoch = chunk.write_epoch;
    }
    Ok(updated)
}

/// Loads the shard-local cases covered by a claimed chunk's ordinal range.
///
/// Query behavior: reads only the run-and-shard projection routed to this
/// execution database, joined to immutable case blobs and ordered by ordinal.
/// This function does not claim work; callers must already own the chunk lease.
pub(crate) async fn load_chunk_case_batch(
    db: &PgPool,
    chunk: &RunChunk,
) -> anyhow::Result<Vec<WorkerCaseBatchItem>> {
    let rows = sqlx::query_as::<_, WorkerCaseBatchItem>(
        r#"
		SELECT
			projected.case_id,
			projected.case_hash,
			projected.case_ordinal,
			cb.task_type,
			cb.case_group,
			cb.input_payload,
			cb.expected_output,
			cb.context_payload,
			cb.tags,
			cb.metadata
		FROM run_shard_cases projected
		JOIN case_blobs cb ON projected.case_hash = cb.case_hash
		WHERE projected.run_id = $1::uuid
		  AND projected.run_shard = $2
		  AND projected.dataset_version_id = $3::uuid
		  AND projected.case_ordinal >= $4
		  AND projected.case_ordinal < $5
		ORDER BY projected.case_ordinal
		"#,
    )
    .bind(chunk.run_id)
    .bind(chunk.run_shard)
    .bind(chunk.dataset_version_id)
    .bind(chunk.ordinal_start)
    .bind(chunk.ordinal_end)
    .fetch_all(db)
    .await?;

    validate_chunk_case_ordinals(
        chunk.ordinal_start,
        chunk.ordinal_end,
        rows.iter().map(|row| row.case_ordinal),
    )?;
    Ok(rows)
}

/// Releases a leased chunk back to pending if the caller still owns the lease.
///
/// Query behavior: clears the lease and returns the chunk to `pending` only for
/// the current lease token. Workers use this for recoverable processing
/// failures and planned execution retry waits.
pub(crate) async fn release_chunk_as_pending(db: &PgPool, chunk: &RunChunk) -> anyhow::Result<u64> {
    let lease_token = require_chunk_lease_token(chunk)?;
    let result = sqlx::query(
        r#"
		UPDATE run_chunks
		SET status = 'pending',
			lease_token = NULL,
			leased_until = NULL,
			updated_at = now()
		WHERE run_id = $1::uuid
          AND run_shard = $2
		  AND id = $3::uuid
		  AND status = 'leased'
		  AND lease_token = $4::uuid
		  AND leased_until >= now()
		  AND EXISTS (
			SELECT 1
			FROM local_shard_admissions admission
			WHERE admission.run_id = run_chunks.run_id
			  AND admission.run_shard = run_chunks.run_shard
			  AND admission.write_epoch = $5
			  AND admission.state IN ('open', 'draining')
		  )
		"#,
    )
    .bind(chunk.run_id)
    .bind(chunk.run_shard)
    .bind(chunk.id)
    .bind(lease_token)
    .bind(chunk.write_epoch)
    .execute(db)
    .await?;

    Ok(result.rows_affected())
}
