//! PostgreSQL operations for run dispatch.

use super::*;

/// Selects the next control-database dispatch route.
///
/// This query does not claim execution rows. It chooses one open
/// `(run_id, run_shard)` cursor whose shard placement is active and whose
/// database placement can hold execution data. The execution dispatch query
/// then locks and advances that exact cursor on the resolved placement. Aliases
/// that already failed in the current coordinator cycle are excluded so they
/// cannot consume the remaining dispatch budget.
#[cfg(test)]
pub(crate) async fn select_next_dispatch_route(
    db: &PgPool,
    excluded_database_aliases: &[String],
) -> anyhow::Result<Option<DispatchRoute>> {
    let Some(claim) = claim_next_dispatch_route(db, excluded_database_aliases).await? else {
        return Ok(None);
    };
    let route = claim.route;
    claim.control_tx.rollback().await?;
    Ok(Some(route))
}

/// Claims the next control cursor and keeps its transaction open through the
/// routed execution write.
pub(crate) async fn claim_next_dispatch_route(
    db: &PgPool,
    excluded_database_aliases: &[String],
) -> anyhow::Result<Option<ClaimedDispatchRoute>> {
    let mut control_tx = db.begin().await?;
    let route = sqlx::query_as::<_, DispatchRoute>(
        r#"
        SELECT
            c.run_id,
            c.run_shard,
            sp.database_alias,
            sp.status AS placement_status,
            sp.route_version,
            sp.write_epoch
        FROM run_shard_dispatch_cursors c
        JOIN runs r
          ON r.id = c.run_id
        JOIN shard_placements sp
          ON sp.run_id = c.run_id
         AND sp.run_shard = c.run_shard
        JOIN database_placements dp
          ON dp.alias = sp.database_alias
        WHERE c.status = 'open'
          AND sp.status = 'active'
          AND dp.status IN ('active', 'draining')
          AND dp.role IN ('shard', 'control_and_shard')
          AND NOT (sp.database_alias = ANY($1::text[]))
          AND (
              r.status = 'running'::run_status
              OR (
                  r.status = 'pending'::run_status
                  AND (
                      r.coordinator_leased_until IS NULL
                      OR r.coordinator_leased_until < now()
                  )
              )
          )
        ORDER BY c.updated_at ASC, c.run_id ASC, c.run_shard ASC
        LIMIT 1
        FOR UPDATE OF c SKIP LOCKED
        "#,
    )
    .bind(excluded_database_aliases)
    .fetch_optional(&mut *control_tx)
    .await?;

    let Some(route) = route else {
        control_tx.rollback().await?;
        return Ok(None);
    };

    Ok(Some(ClaimedDispatchRoute { route, control_tx }))
}

/// Counts currently dispatchable control-database cursor rows.
///
/// This mirrors [`select_next_dispatch_route`] without ordering or row return
/// data. Coordinator structured logs use it as a backlog gauge for scale
/// monitoring.
pub(crate) async fn count_dispatch_cursor_backlog(db: &PgPool) -> anyhow::Result<i64> {
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM run_shard_dispatch_cursors c
        JOIN runs r
          ON r.id = c.run_id
        JOIN shard_placements sp
          ON sp.run_id = c.run_id
         AND sp.run_shard = c.run_shard
        JOIN database_placements dp
          ON dp.alias = sp.database_alias
        WHERE c.status = 'open'
          AND sp.status = 'active'
          AND dp.status IN ('active', 'draining')
          AND dp.role IN ('shard', 'control_and_shard')
          AND (
              r.status = 'running'::run_status
              OR (
                  r.status = 'pending'::run_status
                  AND (
                      r.coordinator_leased_until IS NULL
                      OR r.coordinator_leased_until < now()
                  )
              )
          )
        "#,
    )
    .fetch_one(db)
    .await?;

    Ok(count)
}

pub(crate) async fn prepare_dispatch_run_snapshot_with(
    tx: &mut Transaction<'_, Postgres>,
    route: &DispatchRoute,
    coordinator_id: Uuid,
    lease_seconds: i32,
) -> anyhow::Result<Option<DispatchRunSnapshot>> {
    let started = sqlx::query_as::<_, DispatchRunSnapshot>(
        r#"
        WITH started_run AS (
            UPDATE runs r
            SET status = 'running'::run_status,
                coordinator_id = $3::uuid,
                coordinator_leased_until = now() + ($4::int * interval '1 second'),
                coordinator_heartbeat_at = now(),
                started_at = COALESCE(r.started_at, now()),
                dispatched_at = COALESCE(r.dispatched_at, now()),
                updated_at = now()
            WHERE r.id = $1::uuid
              AND r.status = 'pending'::run_status
              AND (
                  r.coordinator_leased_until IS NULL
                  OR r.coordinator_leased_until < now()
              )
            RETURNING r.*
        ),
        started_event AS (
            INSERT INTO outbox_events (event_type, aggregate_type, aggregate_id, dedupe_key, payload)
            SELECT
                'run.started',
                'run',
                started_run.id,
                format('run:%s:started', started_run.id),
                jsonb_build_object('run_id', started_run.id)
            FROM started_run
            ON CONFLICT (dedupe_key) DO NOTHING
            RETURNING id
        )
        SELECT
            started_run.id AS run_id,
            $2::smallint AS run_shard,
            started_run.run_key,
            started_run.dataset_id,
            started_run.dataset_version_id,
            started_run.dataset_version,
            started_run.evaluation_profile_id,
            started_run.evaluation_profile_version,
            started_run.profile_version_id,
            started_run.profile_hash,
            started_run.aggregation_policy_id,
            started_run.aggregation_policy_version,
            started_run.aggregation_policy_hash,
            started_run.agent_provider,
            started_run.agent_name,
            started_run.agent_version,
            started_run.prompt_config_id,
            started_run.prompt_config_version,
            started_run.config_snapshot,
            (SELECT COUNT(*)::bigint FROM started_event) AS run_started_event_records_inserted
        FROM started_run
        "#,
    )
    .bind(route.run_id)
    .bind(route.run_shard)
    .bind(coordinator_id)
    .bind(lease_seconds)
    .fetch_optional(&mut **tx)
    .await?;

    if started.is_some() {
        return Ok(started);
    }

    let running = sqlx::query_as::<_, DispatchRunSnapshot>(
        r#"
        SELECT
            r.id AS run_id,
            $2::smallint AS run_shard,
            r.run_key,
            r.dataset_id,
            r.dataset_version_id,
            r.dataset_version,
            r.evaluation_profile_id,
            r.evaluation_profile_version,
            r.profile_version_id,
            r.profile_hash,
            r.aggregation_policy_id,
            r.aggregation_policy_version,
            r.aggregation_policy_hash,
            r.agent_provider,
            r.agent_name,
            r.agent_version,
            r.prompt_config_id,
            r.prompt_config_version,
            r.config_snapshot,
            0::bigint AS run_started_event_records_inserted
        FROM runs r
        WHERE r.id = $1::uuid
          AND r.status = 'running'::run_status
        FOR SHARE
        "#,
    )
    .bind(route.run_id)
    .bind(route.run_shard)
    .fetch_optional(&mut **tx)
    .await?;

    Ok(running)
}

#[cfg(test)]
pub(super) async fn claim_exact_dispatch_cursor(
    control_db: &PgPool,
    route: &DispatchRoute,
) -> Result<Option<Transaction<'static, Postgres>>, RoutedDispatchError> {
    let mut control_tx = control_db
        .begin()
        .await
        .map_err(anyhow::Error::from)
        .map_err(RoutedDispatchError::Control)?;
    let cursor_locked = sqlx::query_scalar::<_, i32>(
        r#"
        SELECT 1
        FROM run_shard_dispatch_cursors
        WHERE run_id = $1::uuid
          AND run_shard = $2
          AND status = 'open'
        FOR UPDATE SKIP LOCKED
        "#,
    )
    .bind(route.run_id)
    .bind(route.run_shard)
    .fetch_optional(&mut *control_tx)
    .await
    .map_err(anyhow::Error::from)
    .map_err(RoutedDispatchError::Control)?
    .is_some();

    if cursor_locked {
        Ok(Some(control_tx))
    } else {
        control_tx
            .rollback()
            .await
            .map_err(anyhow::Error::from)
            .map_err(RoutedDispatchError::Control)?;
        Ok(None)
    }
}
pub(super) async fn recover_expired_chunks(
    tx: &mut Transaction<'_, Postgres>,
    max_recoveries: i32,
    batch_size: i64,
    admitted_runs: &[Uuid],
    admitted_shards: &[i16],
) -> anyhow::Result<(i64, Vec<(Uuid, i16, Uuid)>)> {
    // Query outline:
    //
    // expired         - recoverable leased chunks whose lease is past due.
    // recovered       - clear lease, increment recovery_count, return rows.
    // stale_recovered_attempts
    //                 - mark current running attempts stale before requeue.
    // recovery_events - insert a fresh, recovery-scoped chunk-ready outbox record.
    let recovered = sqlx::query_scalar::<_, i64>(
        r#"
        WITH expired AS (
            SELECT
                rc.run_id,
                rc.run_shard,
                rc.id,
                rc.recovery_count + 1 AS next_recovery_count
            FROM run_chunks rc
            JOIN run_snapshots rs
              ON rs.run_id = rc.run_id
             AND rs.run_shard = rc.run_shard
            JOIN local_shard_admissions admission
              ON admission.run_id = rc.run_id
             AND admission.run_shard = rc.run_shard
             AND admission.state = 'open'
            JOIN UNNEST($3::uuid[], $4::smallint[]) AS admitted(run_id,run_shard)
              ON admitted.run_id = rc.run_id AND admitted.run_shard = rc.run_shard
            WHERE rc.status = 'leased'
              AND rc.leased_until < now()
              AND rc.recovery_count < $1
            ORDER BY rc.leased_until ASC, rc.run_id ASC, rc.id ASC
            FOR UPDATE OF rc SKIP LOCKED
            LIMIT $2
        ),
        recovered AS (
            UPDATE run_chunks rc
            SET status = 'pending',
                lease_token = NULL,
                leased_until = NULL,
                recovery_count = expired.next_recovery_count,
                last_recovered_at = now(),
                updated_at = now()
            FROM expired
            WHERE rc.run_id = expired.run_id
              AND rc.run_shard = expired.run_shard
              AND rc.id = expired.id
            RETURNING rc.run_id, rc.run_shard, rc.id, rc.recovery_count
        ),
        stale_recovered_attempts AS (
            UPDATE execution_attempts ea
            SET status = 'stale'::attempt_status,
                error_message = COALESCE(
                    ea.error_message,
                    'attempt lease expired with recovered chunk'
                ),
                completed_at = COALESCE(ea.completed_at, now()),
                leased_until = NULL,
                updated_at = now()
            FROM recovered
            JOIN executions e
              ON e.run_id = recovered.run_id
             AND e.run_shard = recovered.run_shard
             AND e.chunk_id = recovered.id
            WHERE ea.run_id = recovered.run_id
              AND ea.run_shard = recovered.run_shard
              AND ea.execution_id = e.id
              AND ea.id = e.current_attempt_id
              AND ea.status = 'running'::attempt_status
            RETURNING ea.id
        ),
        recovery_events AS (
            INSERT INTO outbox_events (event_type, aggregate_type, aggregate_id, dedupe_key, payload)
            SELECT
                'run.chunk.ready',
                'run',
                recovered.run_id,
                format(
                    'run:%s:chunk:%s:ready:recovery:%s',
                    recovered.run_id,
                    recovered.id,
                    recovered.recovery_count
                ),
                jsonb_build_object(
                    'run_id', recovered.run_id,
                    'run_shard', recovered.run_shard,
                    'chunk_id', recovered.id,
                    'database_alias', admission.database_alias,
                    'write_epoch', admission.write_epoch,
                    'recovery_count', recovered.recovery_count
                )
            FROM recovered
            JOIN local_shard_admissions admission
              ON admission.run_id = recovered.run_id
             AND admission.run_shard = recovered.run_shard
             AND admission.state = 'open'
            ON CONFLICT (dedupe_key) DO NOTHING
            RETURNING id
        )
        SELECT COUNT(*)::bigint
        FROM recovered
        "#,
    )
    .bind(max_recoveries)
    .bind(batch_size)
    .bind(admitted_runs)
    .bind(admitted_shards)
    .fetch_one(&mut **tx)
    .await?;

    // Query outline:
    //
    // expired - leased chunks past due that have exhausted recovery attempts.
    // failed  - clear the dead lease and make the chunk terminal.
    // stale_failed_attempts
    //         - mark current running attempts stale with the failed chunk.
    let failed_rows = sqlx::query_as::<_, (Uuid, i16, Uuid)>(
        r#"
        WITH expired AS (
            SELECT rc.run_id, rc.run_shard, rc.id
            FROM run_chunks rc
            JOIN run_snapshots rs
              ON rs.run_id = rc.run_id
             AND rs.run_shard = rc.run_shard
            JOIN UNNEST($3::uuid[], $4::smallint[]) AS admitted(run_id,run_shard)
              ON admitted.run_id = rc.run_id AND admitted.run_shard = rc.run_shard
            WHERE rc.status = 'leased'
              AND rc.leased_until < now()
              AND rc.recovery_count >= $1
              AND EXISTS (SELECT 1 FROM local_shard_admissions a WHERE a.run_id = rc.run_id
                  AND a.run_shard = rc.run_shard AND a.state IN ('open','draining'))
            ORDER BY rc.leased_until ASC, rc.run_id ASC, rc.id ASC
            FOR UPDATE OF rc SKIP LOCKED
            LIMIT $2
        ),
        failed AS (
            UPDATE run_chunks rc
            SET status = 'failed',
                lease_token = NULL,
                leased_until = NULL,
                updated_at = now()
            FROM expired
            WHERE rc.run_id = expired.run_id
              AND rc.run_shard = expired.run_shard
              AND rc.id = expired.id
            RETURNING rc.run_id, rc.run_shard, rc.id
        ),
        stale_failed_attempts AS (
            UPDATE execution_attempts ea
            SET status = 'stale'::attempt_status,
                error_message = COALESCE(
                    ea.error_message,
                    'attempt lease expired with failed chunk'
                ),
                completed_at = COALESCE(ea.completed_at, now()),
                leased_until = NULL,
                updated_at = now()
            FROM expired
            JOIN executions e
              ON e.run_id = expired.run_id
             AND e.run_shard = expired.run_shard
             AND e.chunk_id = expired.id
            WHERE ea.run_id = expired.run_id
              AND ea.run_shard = expired.run_shard
              AND ea.execution_id = e.id
              AND ea.id = e.current_attempt_id
              AND ea.status = 'running'::attempt_status
            RETURNING ea.id
        )
        SELECT run_id, run_shard, id
        FROM failed
        "#,
    )
    .bind(max_recoveries)
    .bind(batch_size)
    .bind(admitted_runs)
    .bind(admitted_shards)
    .fetch_all(&mut **tx)
    .await?;

    Ok((recovered, failed_rows))
}

pub(super) async fn dispatch_run_window(
    execution_tx: &mut Transaction<'static, Postgres>,
    chunk_window_size: i64,
    route: &DispatchRoute,
    snapshot: &DispatchRunSnapshot,
) -> Result<Option<DispatchedRunWindow>, RoutedDispatchError> {
    // Query outline:
    //
    // snapshot_input    - control-owned run context for the selected route.
    // snapshot_upsert   - local execution snapshot prepared before dispatch.
    // claimed          - common run projection for the claimed shard cursor.
    // selected_chunks  - bounded undispatched pending chunk window for one shard.
    // chunk_events     - idempotent queue-visible chunk-ready ledger rows.
    // marked_chunks    - cursor update proving those chunks were dispatched.
    // remaining_chunks - tells the control cursor whether another pass is needed.
    let dispatched = sqlx::query_as::<_, DispatchedRunWindow>(
        r#"
        WITH snapshot_input AS (
            SELECT
                $2::uuid AS run_id,
                $3::smallint AS run_shard,
                $4::text AS run_key,
                $5::uuid AS dataset_id,
                $6::uuid AS dataset_version_id,
                $7::text AS dataset_version,
                $8::text AS evaluation_profile_id,
                $9::text AS evaluation_profile_version,
                $10::text AS profile_version_id,
                $11::text AS profile_hash,
                $12::text AS aggregation_policy_id,
                $13::text AS aggregation_policy_version,
                $14::text AS aggregation_policy_hash,
                $15::text AS agent_provider,
                $16::text AS agent_name,
                $17::text AS agent_version,
                $18::text AS prompt_config_id,
                $19::text AS prompt_config_version,
                $20::jsonb AS config_snapshot,
                $21::text AS database_alias,
                $22::bigint AS write_epoch
        ),
        snapshot_upsert AS (
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
                agent_version,
                prompt_config_id,
                prompt_config_version,
                config_snapshot,
                expected_execution_count
            )
            SELECT
                snapshot_input.run_id,
                snapshot_input.run_shard,
                snapshot_input.run_key,
                snapshot_input.dataset_id,
                snapshot_input.dataset_version_id,
                snapshot_input.dataset_version,
                snapshot_input.evaluation_profile_id,
                snapshot_input.evaluation_profile_version,
                snapshot_input.profile_version_id,
                snapshot_input.profile_hash,
                snapshot_input.aggregation_policy_id,
                snapshot_input.aggregation_policy_version,
                snapshot_input.aggregation_policy_hash,
                snapshot_input.agent_provider,
                snapshot_input.agent_name,
                snapshot_input.agent_version,
                snapshot_input.prompt_config_id,
                snapshot_input.prompt_config_version,
                snapshot_input.config_snapshot,
                COALESCE((
                    SELECT SUM(rc.ordinal_end - rc.ordinal_start)::int
                    FROM run_chunks rc
                    WHERE rc.run_id = snapshot_input.run_id
                      AND rc.run_shard = snapshot_input.run_shard
                ), 0)
            FROM snapshot_input
            ON CONFLICT (run_id, run_shard) DO UPDATE
            SET run_key = EXCLUDED.run_key,
                dataset_id = EXCLUDED.dataset_id,
                dataset_version_id = EXCLUDED.dataset_version_id,
                dataset_version = EXCLUDED.dataset_version,
                evaluation_profile_id = EXCLUDED.evaluation_profile_id,
                evaluation_profile_version = EXCLUDED.evaluation_profile_version,
                profile_version_id = EXCLUDED.profile_version_id,
                profile_hash = EXCLUDED.profile_hash,
                aggregation_policy_id = EXCLUDED.aggregation_policy_id,
                aggregation_policy_version = EXCLUDED.aggregation_policy_version,
                aggregation_policy_hash = EXCLUDED.aggregation_policy_hash,
                agent_provider = EXCLUDED.agent_provider,
                agent_name = EXCLUDED.agent_name,
                agent_version = EXCLUDED.agent_version,
                prompt_config_id = EXCLUDED.prompt_config_id,
                prompt_config_version = EXCLUDED.prompt_config_version,
                config_snapshot = EXCLUDED.config_snapshot,
                expected_execution_count = EXCLUDED.expected_execution_count,
                updated_at = now()
            RETURNING run_id, run_shard, run_key
        ),
        claimed AS (
            SELECT
                snapshot_upsert.run_id AS id,
                snapshot_upsert.run_key,
                snapshot_upsert.run_shard
            FROM snapshot_upsert
        ),
        selected_chunks AS (
            SELECT rc.run_id, rc.run_shard, rc.id
            FROM run_chunks rc
            JOIN claimed
              ON claimed.id = rc.run_id
             AND claimed.run_shard = rc.run_shard
            WHERE rc.status = 'pending'
              AND rc.dispatched_at IS NULL
            ORDER BY rc.ordinal_start ASC, rc.id ASC
            LIMIT $1
        ),
        chunk_events AS (
            INSERT INTO outbox_events (event_type, aggregate_type, aggregate_id, dedupe_key, payload)
            SELECT
                'run.chunk.ready',
                'run',
                selected_chunks.run_id,
                format('run:%s:chunk:%s:ready', selected_chunks.run_id, selected_chunks.id),
                jsonb_build_object(
                    'run_id', selected_chunks.run_id,
                    'run_shard', selected_chunks.run_shard,
                    'chunk_id', selected_chunks.id,
                    'database_alias', snapshot_input.database_alias,
                    'write_epoch', snapshot_input.write_epoch
                )
            FROM selected_chunks
            CROSS JOIN snapshot_input
            ON CONFLICT (dedupe_key) DO NOTHING
            RETURNING id
        ),
        marked_chunks AS (
            UPDATE run_chunks rc
            SET dispatched_at = COALESCE(rc.dispatched_at, now()),
                updated_at = now()
            FROM selected_chunks
            WHERE rc.run_id = selected_chunks.run_id
              AND rc.run_shard = selected_chunks.run_shard
              AND rc.id = selected_chunks.id
            RETURNING rc.id
        ),
        remaining_chunks AS (
            SELECT EXISTS (
                SELECT 1
                FROM run_chunks rc
                JOIN claimed
                  ON claimed.id = rc.run_id
                 AND claimed.run_shard = rc.run_shard
                WHERE rc.status = 'pending'
                  AND rc.dispatched_at IS NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM selected_chunks
                      WHERE selected_chunks.run_id = rc.run_id
                        AND selected_chunks.run_shard = rc.run_shard
                        AND selected_chunks.id = rc.id
                  )
            ) AS has_remaining
        )
        SELECT
            claimed.id,
            claimed.run_key,
            claimed.run_shard,
            (SELECT COUNT(*)::bigint FROM chunk_events) AS chunk_ready_event_records_inserted,
            (SELECT COUNT(*)::bigint FROM marked_chunks) AS chunks_marked_dispatched,
            $23::bigint AS run_started_event_records_inserted,
            remaining_chunks.has_remaining AS has_remaining_chunks
        FROM claimed, remaining_chunks
        "#,
    )
    .bind(chunk_window_size)
    .bind(route.run_id)
    .bind(route.run_shard)
    .bind(&snapshot.run_key)
    .bind(snapshot.dataset_id)
    .bind(snapshot.dataset_version_id)
    .bind(&snapshot.dataset_version)
    .bind(&snapshot.evaluation_profile_id)
    .bind(&snapshot.evaluation_profile_version)
    .bind(&snapshot.profile_version_id)
    .bind(&snapshot.profile_hash)
    .bind(&snapshot.aggregation_policy_id)
    .bind(&snapshot.aggregation_policy_version)
    .bind(&snapshot.aggregation_policy_hash)
    .bind(&snapshot.agent_provider)
    .bind(&snapshot.agent_name)
    .bind(&snapshot.agent_version)
    .bind(&snapshot.prompt_config_id)
    .bind(&snapshot.prompt_config_version)
    .bind(&snapshot.config_snapshot)
    .bind(&route.database_alias)
    .bind(route.write_epoch)
    .bind(snapshot.run_started_event_records_inserted)
    .fetch_optional(&mut **execution_tx)
    .await
    .map_err(anyhow::Error::from)
    .map_err(RoutedDispatchError::ExecutionWrite)?;

    Ok(dispatched)
}

pub(super) async fn update_dispatch_cursor(
    control_tx: &mut Transaction<'static, Postgres>,
    run_id: Uuid,
    run_shard: i16,
    status: &str,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE run_shard_dispatch_cursors
        SET status = $3,
            updated_at = now()
        WHERE run_id = $1::uuid
          AND run_shard = $2
        "#,
    )
    .bind(run_id)
    .bind(run_shard)
    .bind(status)
    .execute(&mut **control_tx)
    .await?;
    Ok(())
}
