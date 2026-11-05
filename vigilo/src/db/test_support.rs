//! Minimal persisted ownership roots for database fixtures that exercise only
//! routing or snapshot behavior. Existing run contents are never overwritten.
use sqlx::PgPool;
use uuid::Uuid;

pub(crate) async fn seed_run_owner(pool: &PgPool, run: Uuid) {
    sqlx::query("INSERT INTO dataset_versions(dataset_version_id,dataset_id,dataset_version) VALUES($1,$1,'fixture') ON CONFLICT DO NOTHING")
        .bind(run).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO runs(id,run_key,dataset_id,dataset_version_id,dataset_version,evaluation_profile_id,evaluation_profile_version,profile_version_id,profile_hash,aggregation_policy_id,aggregation_policy_version,aggregation_policy_hash,agent_provider,agent_name,prompt_config_id,prompt_config_version) VALUES($1,$1::text,$1,$1,'fixture','profile','1','profile:1','hash','policy','1','hash','test','agent','prompt','1') ON CONFLICT(id) DO NOTHING")
        .bind(run).execute(pool).await.unwrap();
}
