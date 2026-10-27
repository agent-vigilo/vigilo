//! Bounded execution-local admission writes and exact membership reads.
//!
//! The caller owns the transaction and resolves current placement authority.
//! Shared shard fences precede every write in deterministic order. Inserts and
//! exact replay checks operate on sets, preserving cancellation tombstones and
//! immutable snapshots. One common snapshot template is transmitted per group;
//! per-shard policy is assembled in SQL rather than copied across client memory.
use std::collections::{
    BTreeMap,
    BTreeSet,
};

use anyhow::{
    Context,
    Result,
    ensure,
};
use serde::Serialize;
use serde_json::Value;
use sqlx::{
    Postgres,
    Transaction,
    types::Json,
};
use uuid::Uuid;

use crate::{
    db::{
        shard_write_fence,
        workflows::{
            run_create,
            run_shard_summary::plan::Manifest,
        },
    },
    models::{
        run::RunDraft,
        run_chunk::RunChunkDraft,
    },
};

const MAX_SHARDS: usize = 64;
const SNAPSHOT_GROUP_BYTES: usize = 8 * 1024 * 1024;
const MAX_CHUNKS: usize = 256;
const CHUNK_GROUP_BYTES: usize = 256 * 1024;

#[derive(Serialize)]
struct Shard<'a> {
    run_shard: i16,
    manifest: &'a Manifest,
    gate_counts: &'a Value,
    case_count: i32,
}

/// Immutable inputs shared by each bounded placement transaction.
pub(crate) struct Seed<'a> {
    pub run_id: Uuid,
    pub draft: &'a RunDraft,
    pub snapshot: &'a Value,
    pub expected: &'a BTreeMap<i16, Manifest>,
    pub gate_counts: &'a BTreeMap<i16, Value>,
}
impl Seed<'_> {
    /// Lock shards before run-row writes, then seed and compare exact metadata.
    /// The caller supplies the transaction and an outer deadline. `row_limit`
    /// preserves its case-count policy; metadata caps can split inserts further.
    pub(crate) async fn write(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        alias: &str,
        status: &str,
        chunks: &[RunChunkDraft],
        row_limit: usize,
    ) -> Result<()> {
        ensure!(
            !chunks.is_empty() && chunks.len() <= 4096 && row_limit > 0,
            "invalid placement page bounds"
        );
        let shards = chunks
            .iter()
            .map(|c| c.run_shard)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        shard_write_fence::lock_shared_many(tx, self.run_id, &shards).await?;
        run_create::upsert_dataset_version(
            tx,
            self.draft.dataset_version_id,
            self.draft.dataset_id,
            &self.draft.dataset_version,
        )
        .await?;
        run_create::insert_run_create(tx, self.run_id, self.draft, status).await?;
        let template_bytes = serde_json::to_vec(self.snapshot)?.len();
        let mut group = Vec::new();
        let mut bytes = 0_usize;
        for run_shard in shards {
            let manifest = self
                .expected
                .get(&run_shard)
                .context("unassigned snapshot shard")?;
            let shard = Shard {
                run_shard,
                manifest,
                gate_counts: self
                    .gate_counts
                    .get(&run_shard)
                    .context("missing gate counts")?,
                case_count: manifest.case_count,
            };
            let size = template_bytes
                .checked_add(serde_json::to_vec(&shard)?.len())
                .context("snapshot group byte overflow")?;
            // A single already-admitted configuration may exceed the group
            // target. Keep it alone; never reject previously valid configuration.
            if !group.is_empty()
                && (group.len() == MAX_SHARDS || bytes.saturating_add(size) > SNAPSHOT_GROUP_BYTES)
            {
                self.snapshots(tx, alias, &group).await?;
                group.clear();
                bytes = 0;
            }
            bytes = bytes
                .checked_add(size)
                .context("snapshot group byte overflow")?;
            group.push(shard);
        }
        if !group.is_empty() {
            self.snapshots(tx, alias, &group).await?;
        }
        let mut start = 0;
        let mut bytes = 0_usize;
        for (index, chunk) in chunks.iter().enumerate() {
            let size = serde_json::to_vec(chunk)?.len();
            ensure!(
                size <= CHUNK_GROUP_BYTES,
                "chunk metadata exceeds admission limit"
            );
            if index > start
                && (index - start == row_limit.min(MAX_CHUNKS) || bytes + size > CHUNK_GROUP_BYTES)
            {
                run_create::bulk_insert_run_chunks(
                    tx,
                    self.run_id,
                    self.draft.dataset_version_id,
                    &chunks[start..index],
                )
                .await?;
                start = index;
                bytes = 0;
            }
            bytes += size;
        }
        run_create::bulk_insert_run_chunks(
            tx,
            self.run_id,
            self.draft.dataset_version_id,
            &chunks[start..],
        )
        .await
    }

    async fn snapshots(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        alias: &str,
        shards: &[Shard<'_>],
    ) -> Result<()> {
        let argument = serde_json::to_value(shards)?;
        sqlx::query("INSERT INTO local_shard_admissions(run_id,run_shard,database_alias,write_epoch,state) SELECT $1,s.run_shard,$2,1,'open' FROM jsonb_to_recordset($3) AS s(run_shard smallint) ON CONFLICT DO NOTHING")
            .bind(self.run_id).bind(alias).bind(Json(&argument)).execute(&mut **tx).await?;
        let admitted:bool=sqlx::query_scalar("SELECT count(*)=jsonb_array_length($3) AND coalesce(bool_and(a.database_alias=$2 AND a.write_epoch=1 AND a.state='open'),false) FROM jsonb_to_recordset($3) AS s(run_shard smallint) JOIN local_shard_admissions a ON a.run_id=$1 AND a.run_shard=s.run_shard")
            .bind(self.run_id).bind(alias).bind(Json(&argument)).fetch_one(&mut **tx).await?;
        if !admitted {
            return Err(run_create::seed_invariant_error(
                "execution admission changed while seeding",
            ));
        }
        let insert = format!(
            "INSERT INTO run_snapshots(run_id,run_shard,run_key,dataset_id,dataset_version_id,dataset_version,evaluation_profile_id,evaluation_profile_version,profile_version_id,profile_hash,aggregation_policy_id,aggregation_policy_version,aggregation_policy_hash,agent_provider,agent_name,agent_version,prompt_config_id,prompt_config_version,config_snapshot,expected_execution_count) SELECT r.id,s.run_shard,r.run_key,r.dataset_id,r.dataset_version_id,r.dataset_version,r.evaluation_profile_id,r.evaluation_profile_version,r.profile_version_id,r.profile_hash,r.aggregation_policy_id,r.aggregation_policy_version,r.aggregation_policy_hash,r.agent_provider,r.agent_name,r.agent_version,r.prompt_config_id,r.prompt_config_version,{SNAPSHOT},s.case_count FROM runs r CROSS JOIN jsonb_to_recordset($2) AS s({FIELDS}) WHERE r.id=$1 ON CONFLICT DO NOTHING"
        );
        sqlx::query(&insert)
            .bind(self.run_id)
            .bind(Json(&argument))
            .bind(self.snapshot)
            .execute(&mut **tx)
            .await?;
        let replay = format!(
            "SELECT count(*)=jsonb_array_length($2) AND coalesce(bool_and(r.config_snapshot={SNAPSHOT} AND r.expected_execution_count=s.case_count),false) FROM jsonb_to_recordset($2) AS s({FIELDS}) JOIN run_snapshots r ON r.run_id=$1 AND r.run_shard=s.run_shard"
        );
        let matches: bool = sqlx::query_scalar(&replay)
            .bind(self.run_id)
            .bind(Json(&argument))
            .bind(self.snapshot)
            .fetch_one(&mut **tx)
            .await?;
        if !matches {
            return Err(run_create::seed_invariant_error(
                "local run snapshot changed",
            ));
        }
        Ok(())
    }
}
const FIELDS: &str = "run_shard smallint,manifest jsonb,gate_counts jsonb,case_count integer";
const SNAPSHOT: &str = "$3::jsonb || jsonb_build_object('settlement_plan',jsonb_build_object('version',1,'shards',jsonb_build_object(s.run_shard::text,s.manifest)),'gate_planned_counts',s.gate_counts)";

/// Read one ordered page across a placement's assigned shards. The caller folds
/// each shard independently and resolves placement permission for every page.
pub(crate) async fn read_chunks(
    db: &sqlx::PgPool,
    run_id: Uuid,
    dataset_version_id: Uuid,
    alias: &str,
    shards: &[i16],
    after: (i16, i32, Uuid),
) -> Result<Vec<(Uuid, i16, i32, i32, bool)>> {
    ensure!(
        !shards.is_empty() && shards.len() <= 128,
        "invalid placement verification shard count"
    );
    Ok(sqlx::query_as("SELECT c.id,c.run_shard,c.ordinal_start,c.ordinal_end,c.dataset_version_id=$6 AND c.profile_group_id='default' AND c.status='pending' AND c.dispatched_at IS NULL AND EXISTS(SELECT 1 FROM local_shard_admissions a WHERE a.run_id=c.run_id AND a.run_shard=c.run_shard AND a.database_alias=$7 AND a.write_epoch=1 AND a.state='open') FROM run_chunks c WHERE c.run_id=$1 AND c.run_shard=ANY($2) AND (c.run_shard,c.ordinal_start,c.id)>($3,$4,$5) ORDER BY c.run_shard,c.ordinal_start,c.id LIMIT $8")
        .bind(run_id).bind(shards).bind(after.0).bind(after.1).bind(after.2).bind(dataset_version_id).bind(alias).bind(MAX_CHUNKS as i64).fetch_all(db).await?)
}
