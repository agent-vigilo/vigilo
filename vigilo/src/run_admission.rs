//! Resumable execution admission of published immutable run plans.
//!
//! Creation leases fence compact control checkpoints. Bounded chunk pages seed
//! execution-local scheduling rows and verified references, without case rows.
//! Remote writes are immutable and idempotent across lost acknowledgements.
//! Placement-wide ordered pages fold each shard independently before routes
//! become dispatchable. Renewals follow elapsed time; control checkpoints and
//! activation always check live generation and ownership.
mod lease;

use std::{
    collections::BTreeMap,
    sync::Arc,
};

use anyhow::{
    Context as _,
    Result,
    ensure,
};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    blob_storage::{
        BlobStorage,
        Budget,
    },
    case_reader::{
        IndexCursor,
        RunReader,
        document,
    },
    context::Context,
    contracts::case_storage::*,
    db::workflows::{
        run_create,
        run_plan::{
            self,
            Plan,
            admission as ledger,
        },
        run_shard_summary::plan::{
            ChunkIdentity,
            Manifest,
        },
    },
    models::{
        run::RunDraft,
        run_chunk::RunChunkDraft,
    },
};

/// Seeds bounded pages; activation releases its lease atomically on completion.
/// Deferred work and errors attempt a separate fenced release.
/// Returns true only after exact activation. The coordinator resumes ready plans.
#[cfg(test)]
pub(crate) async fn resume(
    context: &Context,
    plan: &Plan,
    store: Arc<dyn BlobStorage>,
    config: crate::db::workflows::run_creation::Config,
    cancel: &CancellationToken,
) -> Result<bool> {
    ensure!(
        config.case_page_budget > 0 && config.case_batch_size > 0,
        "admission budgets must be positive"
    );
    let budget = Budget::standard();
    with_claim(context, plan, |lease| async move {
        let reader = open_reader(plan, std::future::ready(Ok(store)), budget, cancel).await?;
        admit(context, plan, &reader, config, cancel, lease).await
    })
    .await
}

/// Reuses roots already verified by the foreground creator, avoiding duplicate
/// storage reads just to emit its existing creation response metadata.
pub(crate) async fn resume_verified(
    context: &Context,
    plan: &Plan,
    reader: &RunReader,
    config: crate::db::workflows::run_creation::Config,
    cancel: &CancellationToken,
) -> Result<bool> {
    ensure!(
        config.case_page_budget > 0
            && config.case_batch_size > 0
            && plan
                .manifest
                .as_ref()
                .is_some_and(|r| r.0 == reader.reference),
        "reader differs from published run plan"
    );
    with_claim(context, plan, |lease| async move {
        admit(context, plan, reader, config, cancel, lease).await
    })
    .await
}

/// One recovery owner covers adapter opening, root reads, gate reads and local
/// admission. Confirmed activation already released ownership atomically;
/// deferred work and errors (including uncertain commits) attempt release.
/// A replaced owner cannot release its successor's generation.
async fn with_claim<F, Fut>(context: &Context, plan: &Plan, work: F) -> Result<bool>
where
    F: FnOnce(run_plan::Lease) -> Fut,
    Fut: std::future::Future<Output = Result<bool>>,
{
    let db = context.dbr().await?.control().await?;
    if !plan.execution_requested {
        ledger::request(db, plan.run_id).await?;
    }
    let lease = ledger::claim(db, plan.run_id).await?;
    let result = work(lease).await;
    // Only the confirmed activation commit returns true. Another write must not
    // turn committed success into a reported failure if the database goes away.
    if matches!(&result, Ok(true)) {
        return Ok(true);
    }
    let error = result.as_ref().err().map(|e| format!("{e:#}"));
    if let Err(error) = &result
        && run_create::is_seed_invariant_error(error)
        && let Err(failure) = ledger::fail(db, lease, &format!("{error:#}")).await
    {
        tracing::warn!(run_id=%plan.run_id,error=%failure,"could not mark failed admission");
    }
    let release = run_plan::release(db, lease, error.as_deref()).await;
    let complete = result?;
    release?;
    Ok(complete)
}

/// Include adapter initialization and all root reads in one pre-claim deadline.
/// Cancellation drops the read future; the surrounding owner still releases.
async fn open_reader(
    plan: &Plan,
    store: impl std::future::Future<Output = Result<Arc<dyn BlobStorage>>>,
    budget: Budget,
    cancel: &CancellationToken,
) -> Result<RunReader> {
    let read = async {
        let store = store.await?;
        RunReader::open(
            store,
            plan.manifest
                .as_ref()
                .context("run plan is not published")?,
            plan.limits.0.clone(),
            plan.run_id,
            cancel,
        )
        .await
    };
    tokio::select! { biased;
        _ = cancel.cancelled() => anyhow::bail!("run admission cancelled"),
        _ = tokio::time::sleep_until(budget.deadline) => anyhow::bail!("run admission metadata deadline exceeded"),
        result = read => result,
    }
}

async fn admit(
    context: &Context,
    plan: &Plan,
    reader: &RunReader,
    config: crate::db::workflows::run_creation::Config,
    cancel: &CancellationToken,
    lease: run_plan::Lease,
) -> Result<bool> {
    let router = context.dbr().await?;
    let db = router.control().await?;
    ensure!(
        reader.manifest.execution_plan_hash == plan.execution_plan.hash()?,
        "published evaluator identity changed"
    );
    let draft: RunDraft = sqlx::query_as("SELECT * FROM runs WHERE id=$1 AND status='creating'")
        .bind(plan.run_id)
        .fetch_one(db)
        .await?;
    ensure!(
        draft.profile_hash == reader.manifest.profile_hash
            && draft.aggregation_policy_hash == reader.manifest.aggregation_policy_hash
            && draft.dataset_version_id == reader.dataset.dataset_version_id,
        "control run differs from published plan"
    );
    let gates = reader
        .configuration
        .snapshot
        .pointer("/profile/scorecard/gates")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let gate_ids = gates
        .as_array()
        .context("gate configuration is not an array")?;
    let mut snapshot_template = draft.config_snapshot.clone();
    snapshot_template["storage_limits"] = serde_json::to_value(&reader.limits)?;
    snapshot_template["profile"] = json!({"scorecard":{"gates":gates}});
    let mut gate_counts = BTreeMap::new();
    let mut expected = BTreeMap::new();
    let mut clock = lease::Clock::new();
    // Keep the whole gate traversal bounded, not a fresh full allowance per file.
    let metadata_deadline = Budget::standard().deadline;
    for shard in &reader.manifest.shards {
        let last_index = i32::from(shard.run_shard) + (shard.chunk_count - 1) * 128;
        let last = ChunkIdentity {
            chunk_id: identity::uuid_from_json(
                &json!({"run_id":plan.run_id,"chunk_index":last_index}),
            )?,
            run_shard: shard.run_shard,
            ordinal_start: last_index * 100,
            ordinal_end: ((i64::from(last_index) + 1) * 100)
                .min(i64::from(reader.dataset.case_count)) as i32,
        };
        let manifest = Manifest {
            chunk_count: shard.chunk_count,
            case_count: shard.case_count,
            hash: shard.planned_chunk_hash.clone(),
            last: Some(last),
        };
        let mut counts = BTreeMap::new();
        ensure!(shard.gate_counts.entry_count <= 64, "too many gate counts");
        let mut chain = GateCountChainValidator::new(
            &shard.gate_counts,
            shard.run_shard,
            shard.case_count,
            &reader.limits,
        )?;
        while let Some(reference) = chain.next().cloned() {
            let mut budget = clock.budget(db, lease).await?;
            budget.deadline = budget.deadline.min(metadata_deadline);
            let page: GateCountPage = document(
                reader.store.as_ref(),
                &reference,
                &reader.limits,
                budget,
                cancel,
            )
            .await?;
            chain.accept(&reference, &page, &reader.limits)?;
            for entry in page.entries {
                counts.insert(entry.gate_id, entry.planned_count);
            }
        }
        chain.finish()?;
        ensure!(
            gate_ids.len() == counts.len()
                && gate_ids
                    .iter()
                    .all(|g| g["id"].as_str().is_some_and(|id| counts.contains_key(id))),
            "gate counts differ from configuration"
        );
        gate_counts.insert(shard.run_shard, serde_json::to_value(counts)?);
        expected.insert(shard.run_shard, manifest);
    }
    ensure!(
        expected.len() == plan.assignments.len()
            && plan
                .assignments
                .iter()
                .all(|a| expected.contains_key(&a.run_shard)),
        "placement assignments differ from plan"
    );
    let seed = ledger::placement::Seed {
        run_id: plan.run_id,
        draft: &draft,
        snapshot: &snapshot_template,
        expected: &expected,
        gate_counts: &gate_counts,
    };
    async {
        let fresh = run_plan::load(db, plan.run_id)
            .await?
            .context("plan disappeared")?;
        let mut state: BTreeMap<i16, Manifest> = serde_json::from_value(fresh.admission_state.0)?;
        if fresh.admission_cursor < fresh.chunk_cursor {
            let mut cursor = IndexCursor::new(
                reader.manifest.chunks.clone(),
                IndexKind::RunChunks,
                fresh.admission_cursor,
                reader.manifest.chunk_count,
            )?;
            let mut next = fresh.admission_cursor;
            for _ in 0..config.case_page_budget {
                let budget = clock.budget(db, lease).await?;
                let Some((entry, bytes)) = cursor
                    .next(reader.store.as_ref(), &reader.limits, budget, cancel)
                    .await?
                else {
                    break;
                };
                let page: RunChunkPage = decode(&entry.object.verify(&bytes, &reader.limits)?)?;
                page.validate(&reader.limits)?;
                ensure!(
                    page.run_id == plan.run_id
                        && page.chunk_index_start == entry.range_start
                        && page.chunk_index_end == entry.range_end
                        && page.chunk_index_start == next,
                    "admission chunk page is not contiguous"
                );
                let mut placements = BTreeMap::<&str, Vec<RunChunkDraft>>::new();
                for chunk in &page.chunks {
                    ensure!(
                        chunk.run_shard == (chunk.chunk_index % 128) as i16
                            && chunk.ordinal_start == chunk.chunk_index * 100
                            && chunk.ordinal_end
                                == ((i64::from(chunk.chunk_index) + 1) * 100)
                                    .min(i64::from(reader.dataset.case_count))
                                    as i32
                            && chunk.chunk_id
                                == identity::uuid_from_json(
                                    &json!({"run_id":plan.run_id,"chunk_index":chunk.chunk_index})
                                )?,
                        "chunk differs from frozen scheduling policy"
                    );
                    let assignment = plan
                        .assignments
                        .iter()
                        .find(|a| a.run_shard == chunk.run_shard)
                        .context("unassigned chunk shard")?;
                    state
                        .entry(chunk.run_shard)
                        .or_default()
                        .push(&ChunkIdentity {
                            chunk_id: chunk.chunk_id,
                            run_shard: chunk.run_shard,
                            ordinal_start: chunk.ordinal_start,
                            ordinal_end: chunk.ordinal_end,
                        })?;
                    placements
                        .entry(&assignment.database_alias)
                        .or_default()
                        .push(RunChunkDraft {
                            chunk_id: chunk.chunk_id,
                            run_shard: chunk.run_shard,
                            ordinal_start: chunk.ordinal_start,
                            ordinal_end: chunk.ordinal_end,
                            profile_group_id: chunk.profile_group_id.clone(),
                        });
                }
                for (alias, chunks) in placements {
                    clock
                        .database(
                            db,
                            lease,
                            cancel,
                            router.run_database_operation(alias, "immutable_plan_seed", async {
                                let local = router.execution_database(alias).await?;
                                let mut tx = local.begin().await?;
                                sqlx::query("SET LOCAL statement_timeout='30s'")
                                    .execute(&mut *tx)
                                    .await?;
                                let status = if alias == router.control_database_alias() {
                                    "creating"
                                } else {
                                    "pending"
                                };
                                seed.write(
                                    &mut tx,
                                    alias,
                                    status,
                                    &chunks,
                                    (config.case_batch_size / 100).max(1),
                                )
                                .await?;
                                tx.commit().await?;
                                Ok(())
                            }),
                        )
                        .await?;
                }
                ledger::acknowledge(db, lease, next, page.chunk_index_end, &state).await?;
                next = page.chunk_index_end;
                if next == reader.manifest.chunk_count {
                    break;
                }
            }
        }
        let fresh = run_plan::load(db, plan.run_id)
            .await?
            .context("plan disappeared")?;
        if fresh.admission_cursor != fresh.chunk_cursor {
            return Ok(false);
        }
        if state != expected {
            return Err(run_create::seed_invariant_error(
                "admitted membership differs from frozen plan",
            ));
        }
        let mut placements = BTreeMap::<&str, Vec<i16>>::new();
        for assignment in plan.assignments.iter() {
            placements
                .entry(&assignment.database_alias)
                .or_default()
                .push(assignment.run_shard);
        }
        for (alias, mut shards) in placements {
            shards.sort_unstable();
            let mut actual = BTreeMap::<i16, Manifest>::new();
            let mut after = (-1_i16, -1_i32, Uuid::nil());
            loop {
                let page: Vec<(Uuid, i16, i32, i32, bool)> = clock
                    .database(
                        db,
                        lease,
                        cancel,
                        router.run_database_operation(alias, "verify_admitted_chunks", async {
                            // Resolve current placement state once for this bounded page,
                            // never retain mutable placement permission across pages.
                            let local = router.execution_database(alias).await?;
                            ledger::placement::read_chunks(
                                &local,
                                plan.run_id,
                                draft.dataset_version_id,
                                alias,
                                &shards,
                                after,
                            )
                            .await
                        }),
                    )
                    .await?;
                if page.is_empty() {
                    break;
                }
                for (chunk_id, run_shard, ordinal_start, ordinal_end, valid) in page {
                    if !valid {
                        return Err(run_create::seed_invariant_error(
                            "local chunk immutable metadata or admission changed",
                        ));
                    }
                    actual
                        .entry(run_shard)
                        .or_default()
                        .push(&ChunkIdentity {
                            chunk_id,
                            run_shard,
                            ordinal_start,
                            ordinal_end,
                        })
                        .map_err(|error| {
                            run_create::seed_invariant_error(format!(
                                "local chunk coverage is invalid: {error:#}"
                            ))
                        })?;
                    after = (run_shard, ordinal_start, chunk_id);
                }
            }
            if actual.len() != shards.len()
                || shards
                    .iter()
                    .any(|shard| actual.get(shard) != expected.get(shard))
            {
                return Err(run_create::seed_invariant_error(
                    "local placement has incomplete or unexpected chunk membership",
                ));
            }
        }
        ensure!(!cancel.is_cancelled(), "run admission cancelled");
        ledger::activate(db, lease, &expected).await?;
        Ok(true)
    }
    .await
}

/// Resumes one bounded discovery page. A failed run yields its lease and does
/// not prevent independent ready plans from progressing on the next cycle.
pub(crate) async fn recover(
    context: &Context,
    limit: usize,
    config: crate::db::workflows::run_creation::Config,
) -> Result<crate::db::workflows::run_creation::RunCreationRecoveryStats> {
    ensure!(
        config.case_batch_size > 0 && config.case_page_budget > 0,
        "creation budgets must be positive"
    );
    let db = context.dbr().await?.control().await?;
    let ids:Vec<Uuid>=sqlx::query_scalar("SELECT r.id FROM runs r JOIN run_plans p ON p.run_id=r.id WHERE r.status='creating' AND p.status='ready' AND p.execution_requested AND (r.coordinator_leased_until IS NULL OR r.coordinator_leased_until<=clock_timestamp()) ORDER BY r.updated_at,r.id LIMIT $1")
        .bind(i64::try_from(limit.min(1024))?).fetch_all(db).await?;
    let mut stats = crate::db::workflows::run_creation::RunCreationRecoveryStats::default();
    for id in ids {
        stats.claimed_runs += 1;
        let budget = Budget::standard();
        let result = async {
            let plan = run_plan::load(db, id).await?.context("plan disappeared")?;
            let plan = &plan;
            with_claim(context, plan, |lease| async move {
                let cancel = CancellationToken::new();
                let reader = open_reader(
                    plan,
                    context.storage(&plan.storage_binding_hash, &plan.limits),
                    budget,
                    &cancel,
                )
                .await?;
                admit(context, plan, &reader, config, &cancel, lease).await
            })
            .await
        }
        .await;
        match result {
            Ok(true) => stats.completed_runs += 1,
            Ok(false) => stats.deferred_runs += 1,
            Err(error) => {
                stats.deferred_runs += 1;
                tracing::warn!(run_id=%id,error=%error,"execution admission deferred");
            }
        }
    }
    Ok(stats)
}
