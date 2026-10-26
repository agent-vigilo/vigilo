//! Resumable execution admission of published immutable run plans.
//!
//! Creation leases fence compact control checkpoints. Bounded chunk pages seed
//! execution-local scheduling rows and verified references, without case rows.
//! Remote writes are immutable and idempotent across lost acknowledgements.
//! Exact local membership is verified before routes become dispatchable.
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

/// Seeds a bounded number of pages and yields its lease on every outcome.
/// Returns true only after exact activation. The coordinator resumes ready plans.
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
    let reader = RunReader::open(
        store,
        plan.manifest
            .as_ref()
            .context("run plan is not published")?,
        plan.limits.0.clone(),
        plan.run_id,
        cancel,
    )
    .await?;
    resume_verified(context, plan, &reader, config, cancel).await
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
    let metadata_budget = Budget::standard();
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
        let budget = metadata_budget;
        while let Some(reference) = chain.next().cloned() {
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
    if !plan.execution_requested {
        ledger::request(db, plan.run_id).await?;
    }
    let lease = ledger::claim(db, plan.run_id).await?;
    let result=async {
        let fresh=run_plan::load(db,plan.run_id).await?.context("plan disappeared")?;
        let mut state:BTreeMap<i16,Manifest>=serde_json::from_value(fresh.admission_state.0)?;
        if fresh.admission_cursor < fresh.chunk_cursor {
            let mut cursor=IndexCursor::new(reader.manifest.chunks.clone(),IndexKind::RunChunks,fresh.admission_cursor,reader.manifest.chunk_count)?;
            let mut next=fresh.admission_cursor;
            for _ in 0..config.case_page_budget {
                ledger::renew(db,lease).await?;
                let Some((entry,bytes))=cursor.next(reader.store.as_ref(),&reader.limits,Budget::standard(),cancel).await? else {break};
                let page:RunChunkPage=decode(&entry.object.verify(&bytes,&reader.limits)?)?;
                page.validate(&reader.limits)?;
                ensure!(page.run_id == plan.run_id && page.chunk_index_start == entry.range_start && page.chunk_index_end == entry.range_end
                    && page.chunk_index_start == next, "admission chunk page is not contiguous");
                let mut placements=BTreeMap::<&str,Vec<RunChunkDraft>>::new();
                for chunk in &page.chunks {
                    ensure!(chunk.run_shard == (chunk.chunk_index % 128) as i16 && chunk.ordinal_start == chunk.chunk_index*100
                        && chunk.ordinal_end == ((i64::from(chunk.chunk_index)+1)*100).min(i64::from(reader.dataset.case_count)) as i32
                        && chunk.chunk_id == identity::uuid_from_json(&json!({"run_id":plan.run_id,"chunk_index":chunk.chunk_index}))?, "chunk differs from frozen scheduling policy");
                    let assignment=plan.assignments.iter().find(|a|a.run_shard == chunk.run_shard).context("unassigned chunk shard")?;
                    state.entry(chunk.run_shard).or_default().push(&ChunkIdentity { chunk_id:chunk.chunk_id,run_shard:chunk.run_shard,ordinal_start:chunk.ordinal_start,ordinal_end:chunk.ordinal_end })?;
                    placements.entry(&assignment.database_alias).or_default().push(RunChunkDraft { chunk_id:chunk.chunk_id,run_shard:chunk.run_shard,
                        ordinal_start:chunk.ordinal_start,ordinal_end:chunk.ordinal_end,profile_group_id:chunk.profile_group_id.clone() });
                }
                for (alias,chunks) in placements {
                    ledger::renew(db,lease).await?;
                    router.run_database_operation(alias,"immutable_plan_seed",async {
                        let local=router.execution_database(alias).await?;
                        let mut tx=local.begin().await?;
                        sqlx::query("SET LOCAL statement_timeout='30s'").execute(&mut *tx).await?;
                        run_create::upsert_dataset_version(&mut tx,draft.dataset_version_id,draft.dataset_id,&draft.dataset_version).await?;
                        let status=if alias == router.control_database_alias() {"creating"} else {"pending"};
                        run_create::insert_run_create(&mut tx,plan.run_id,&draft,status).await?;
                        let shards=chunks.iter().map(|c|c.run_shard).collect::<std::collections::BTreeSet<_>>();
                        for shard in shards {
                            // Retain policy once, materializing only the current
                            // local snapshot instead of one copy per shard.
                            let mut snapshot=snapshot_template.clone();
                            snapshot["settlement_plan"]=json!({"version":1,"shards":{shard.to_string():expected[&shard]}});
                            snapshot["gate_planned_counts"]=gate_counts[&shard].clone();
                            crate::db::shard_write_fence::lock_shared(&mut tx,plan.run_id,shard).await?;
                            sqlx::query("INSERT INTO local_shard_admissions(run_id,run_shard,database_alias,write_epoch,state) VALUES($1,$2,$3,1,'open') ON CONFLICT DO NOTHING")
                                .bind(plan.run_id).bind(shard).bind(alias).execute(&mut *tx).await?;
                            let admitted:bool=sqlx::query_scalar("SELECT database_alias=$3 AND write_epoch=1 AND state='open' FROM local_shard_admissions WHERE run_id=$1 AND run_shard=$2")
                                .bind(plan.run_id).bind(shard).bind(alias).fetch_one(&mut *tx).await?;
                            if !admitted {return Err(run_create::seed_invariant_error("execution admission changed while seeding"));}
                            sqlx::query("INSERT INTO run_snapshots(run_id,run_shard,run_key,dataset_id,dataset_version_id,dataset_version,evaluation_profile_id,evaluation_profile_version,profile_version_id,profile_hash,aggregation_policy_id,aggregation_policy_version,aggregation_policy_hash,agent_provider,agent_name,agent_version,prompt_config_id,prompt_config_version,config_snapshot,expected_execution_count) SELECT id,$2,run_key,dataset_id,dataset_version_id,dataset_version,evaluation_profile_id,evaluation_profile_version,profile_version_id,profile_hash,aggregation_policy_id,aggregation_policy_version,aggregation_policy_hash,agent_provider,agent_name,agent_version,prompt_config_id,prompt_config_version,$3,$4 FROM runs WHERE id=$1 ON CONFLICT DO NOTHING")
                                .bind(plan.run_id).bind(shard).bind(&snapshot).bind(expected[&shard].case_count).execute(&mut *tx).await?;
                            let matches:bool=sqlx::query_scalar("SELECT config_snapshot=$3 AND expected_execution_count=$4 FROM run_snapshots WHERE run_id=$1 AND run_shard=$2")
                                .bind(plan.run_id).bind(shard).bind(&snapshot).bind(expected[&shard].case_count).fetch_one(&mut *tx).await?;
                            if !matches {return Err(run_create::seed_invariant_error("local run snapshot changed"));}
                        }
                        for batch in chunks.chunks((config.case_batch_size / 100).max(1)) {
                            run_create::bulk_insert_run_chunks(&mut tx,plan.run_id,draft.dataset_version_id,batch).await?;
                        }
                        tx.commit().await?;
                        Ok(())
                    }).await?;
                }
                ledger::acknowledge(db,lease,next,page.chunk_index_end,&state).await?;
                next=page.chunk_index_end;
                if next == reader.manifest.chunk_count {break;}
            }
        }
        let fresh=run_plan::load(db,plan.run_id).await?.context("plan disappeared")?;
        if fresh.admission_cursor != fresh.chunk_cursor {return Ok(false);}
        if state != expected {return Err(run_create::seed_invariant_error("admitted membership differs from frozen plan"));}
        for assignment in plan.assignments.iter() {
            ledger::renew(db,lease).await?;
            let local=router.execution_database(&assignment.database_alias).await?;
            let mut actual=Manifest::default();
            loop {
                let last=actual.last.as_ref().map_or(-1,|c|c.ordinal_start);
                let last_id=actual.last.as_ref().map_or(Uuid::nil(),|c|c.chunk_id);
                let page:Vec<(Uuid,i16,i32,i32,bool)>=router.run_database_operation(&assignment.database_alias,"verify_admitted_chunks",async {
                    Ok(sqlx::query_as("SELECT id,run_shard,ordinal_start,ordinal_end,dataset_version_id=$5 AND profile_group_id='default' AND status='pending' AND dispatched_at IS NULL FROM run_chunks WHERE run_id=$1 AND run_shard=$2 AND (ordinal_start,id)>($3,$4) ORDER BY ordinal_start,id LIMIT 256")
                        .bind(plan.run_id).bind(assignment.run_shard).bind(last).bind(last_id).bind(draft.dataset_version_id).fetch_all(&local).await?)
                }).await?;
                if page.is_empty() {break;}
                for (chunk_id,run_shard,ordinal_start,ordinal_end,valid) in page {
                    if !valid {return Err(run_create::seed_invariant_error("local chunk immutable metadata changed"));}
                    actual.push(&ChunkIdentity{chunk_id,run_shard,ordinal_start,ordinal_end}).map_err(|error|run_create::seed_invariant_error(format!("local chunk coverage is invalid: {error:#}")))?;
                }
                ledger::renew(db,lease).await?;
            }
            if actual != expected[&assignment.run_shard] {return Err(run_create::seed_invariant_error("local placement has incomplete or unexpected chunk membership"));}
        }
        ledger::activate(db,lease,&expected).await?;
        Ok(true)
    }.await;
    let error = result.as_ref().err().map(|e| format!("{e:#}"));
    if let Err(error) = &result
        && run_create::is_seed_invariant_error(error)
    {
        ledger::fail(db, lease, &format!("{error:#}")).await?;
    }
    let release = run_plan::release(db, lease, error.as_deref()).await;
    let complete = result?;
    release?;
    Ok(complete)
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
        let plan = run_plan::load(db, id).await?.context("plan disappeared")?;
        let result = async {
            let store = context
                .storage(&plan.storage_binding_hash, &plan.limits)
                .await?;
            resume(context, &plan, store, config, &CancellationToken::new()).await
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
