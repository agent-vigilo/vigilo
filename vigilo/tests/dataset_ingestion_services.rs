//! Named PostgreSQL + Ceph CLI acceptance. Real child-process termination tests
//! durable recovery; every storage deletion is restricted to recorded exact keys.
//! Tests require explicit disposable service configuration and never silently skip.
mod run_plans;

use std::{
    path::Path,
    process::Stdio,
    time::Duration,
};

use anyhow::{
    Context,
    Result,
    ensure,
};
use serde_json::{
    Value,
    json,
};
use tokio::{
    io::AsyncWriteExt,
    process::{
        Child,
        Command,
    },
};
use uuid::Uuid;
fn env(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("missing service-test variable {name}"))
}
fn command() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_vigilo"));
    c.arg("-q")
        .kill_on_drop(true)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    c
}
async fn output(args: &[String]) -> Result<std::process::Output> {
    Ok(tokio::time::timeout(Duration::from_secs(180), command().args(args).output()).await??)
}
async fn success(args: &[String]) -> Result<Value> {
    let out = output(args).await?;
    ensure!(
        out.status.success(),
        "dataset command failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(serde_json::from_slice(&out.stdout)?)
}
fn args(op: &str, id: Option<Uuid>, source: &Path, owner: &Path) -> Vec<String> {
    let mut a = vec!["dataset".into(), op.into()];
    if op == "upload" {
        if let Some(id) = id {
            a.extend(["--upload-id".into(), id.to_string()]);
        }
        a.extend(["--batch-cases".into(), "10".into()]);
    } else {
        a.push(id.unwrap().to_string());
    }
    a.extend([
        "--input-file".into(),
        source.display().to_string(),
        "--input-format".into(),
        "jsonl".into(),
        "--storage-config".into(),
        owner.display().to_string(),
    ]);
    a
}
async fn kill_after_progress(db: &sqlx::PgPool, id: Uuid, child: &mut Child) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if let Some((status, cursor)) = sqlx::query_as::<_, (String, i32)>(
                "SELECT status,case_cursor FROM dataset_uploads WHERE id=$1",
            )
            .bind(id)
            .fetch_optional(db)
            .await?
            {
                ensure!(
                    status != "finalized",
                    "fixture completed before the intended crash"
                );
                if cursor > 0 {
                    child.kill().await?;
                    return Ok::<_, anyhow::Error>(());
                }
            }
            if let Some(status) = child.try_wait()? {
                anyhow::bail!("upload exited before progress: {status}");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    Ok(())
}
#[tokio::test]
async fn cli_upload_survives_process_death_reuses_dataset_and_cleans_abandoned_keys() -> Result<()>
{
    let db = sqlx::postgres::PgPoolOptions::new()
        .max_connections(3)
        .connect(&env("DATABASE_URL")?)
        .await?;
    let endpoint = env("VIGILO_BLOB_TEST_ENDPOINT")?;
    ensure!(
        endpoint.starts_with("http://127.0.0.1:"),
        "requires disposable local Ceph"
    );
    let root = tempfile::tempdir()?;
    let source = root.path().join("dataset.jsonl");
    let config = root.path().join("storage.toml");
    let prefix = format!("ingestion-acceptance/{}", Uuid::now_v7());
    let owner = format!(
        "provider='s3'\nalias='cases'\nbucket='{}'\nprefix='{prefix}'\nregion='us-east-1'\nendpoint='{endpoint}'\nallow_http=true\npath_style=true\n[credential_variables]\naccess_key='VIGILO_BLOB_TEST_ACCESS_KEY_ID'\nsecret_key='VIGILO_BLOB_TEST_SECRET_ACCESS_KEY'\n",
        env("VIGILO_BLOB_TEST_BUCKET")?
    );
    tokio::fs::write(&config, owner).await?;
    let dataset_id = Uuid::now_v7();
    let count = 1200;
    let mut file = tokio::fs::File::create(&source).await?;
    file.write_all(format!("{}\n",json!({"schema":"vigilo/dataset-upload/v1","fields":{"dataset_id":dataset_id,"dataset_version":"acceptance","unknown":{"preserved":true}}})).as_bytes()).await?;
    for _ in 0..count {
        file.write_all(format!("{}\n",json!({"id":Uuid::now_v7(),"task_type":"generation","input":{"prompt":"x".repeat(1024)}})).as_bytes()).await?;
    }
    file.flush().await?;
    drop(file);
    let id = Uuid::now_v7();
    let abandoned = Uuid::now_v7();
    let client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::new()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                env("VIGILO_BLOB_TEST_ACCESS_KEY_ID")?,
                env("VIGILO_BLOB_TEST_SECRET_ACCESS_KEY")?,
                None,
                None,
                "acceptance",
            ))
            .endpoint_url(endpoint)
            .force_path_style(true)
            .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
            .timeout_config(
                aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                    .operation_timeout(Duration::from_secs(10))
                    .build(),
            )
            .build(),
    );
    let result:Result<()> = async {
        let mut child=command().args(args("upload",Some(id),&source,&config)).spawn()?;kill_after_progress(&db,id,&mut child).await?;
        // The process is confirmed dead. Advance only its test lease rather than
        // waiting five minutes or weakening production lease/cancellation rules.
        sqlx::query("UPDATE dataset_uploads SET leased_until=clock_timestamp()-interval '1 second' WHERE id=$1").bind(id).execute(&db).await?;
        let saved:i64=sqlx::query_scalar("SELECT count(*) FROM dataset_upload_objects WHERE upload_id=$1 AND kind='batch' AND acknowledged").bind(id).fetch_one(&db).await?;ensure!(saved>0,"no acknowledged batches before crash");
        let resumed=success(&args("resume",Some(id),&source,&config)).await?;ensure!(resumed["data"]["id"]==id.to_string(),"resume changed catalog identity");
        let cursor:i32=sqlx::query_scalar("SELECT case_cursor FROM dataset_uploads WHERE id=$1 AND status='finalized'").bind(id).fetch_one(&db).await?;ensure!(cursor==count,"resume lost cases");
        let reused=success(&args("upload",None,&source,&config)).await?;ensure!(reused["data"]["id"]==id.to_string() && reused["meta"]["reused"]==true,"unchanged dataset was uploaded again");
        ensure!(!output(&["dataset".into(),"abort".into(),id.to_string()]).await?.status.success(),"published dataset was abortable");
        let wrong=root.path().join("wrong.toml");tokio::fs::write(&wrong,tokio::fs::read_to_string(&config).await?.replace(&prefix,"redirected")).await?;
        ensure!(!output(&args("resume",Some(id),&source,&wrong)).await?.status.success(),"changed owner redirected an upload");
        let mut child=command().args(args("upload",Some(abandoned),&source,&config)).spawn()?;kill_after_progress(&db,abandoned,&mut child).await?;
        success(&["dataset".into(),"abort".into(),abandoned.to_string()]).await?;
        let cleanup=vec!["dataset".into(),"cleanup".into(),abandoned.to_string(),"--storage-config".into(),config.display().to_string(),"--limit".into(),"7".into()];
        ensure!(!output(&cleanup).await?.status.success(),"cleanup skipped the stale-write quarantine");
        sqlx::query("UPDATE dataset_uploads SET cleanup_after=clock_timestamp()-interval '1 second' WHERE id=$1").bind(abandoned).execute(&db).await?;
        let mut done=false;for _ in 0..32 {if success(&cleanup).await?["data"]["cleanup_complete"]==true{done=true;break;}}ensure!(done,"bounded cleanup did not complete");
        let remaining:i64=sqlx::query_scalar("SELECT count(*) FROM dataset_upload_objects WHERE upload_id=$1 AND NOT cleaned").bind(abandoned).fetch_one(&db).await?;ensure!(remaining==0,"owned keys left uncleaned");
        let manifest_key=resumed["data"]["manifest"]["key"].as_str().context("missing manifest reference")?;
        client.head_object().bucket(env("VIGILO_BLOB_TEST_BUCKET")?).key(format!("{prefix}/{manifest_key}")).send().await.map_err(|_|anyhow::anyhow!("abandoned cleanup removed published content"))?;
        // Changed source is rejected even for a finalized upload ID.
        let original=tokio::fs::read(&source).await?;let changed=String::from_utf8(original)?.replacen("acceptance","changed",1);tokio::fs::write(&source,changed).await?;ensure!(!output(&args("resume",Some(id),&source,&config)).await?.status.success(),"changed resume source accepted");Ok(())
    }.await;
    // Fixture authority is separate from application cleanup. Remove only these
    // two invocation IDs' recorded keys, including the deliberately published one.
    for upload in [id, abandoned] {
        let mut after = String::new();
        loop {
            let keys:Vec<String>=sqlx::query_scalar("SELECT object_key FROM dataset_upload_objects WHERE upload_id=$1 AND object_key>$2 ORDER BY object_key LIMIT 128").bind(upload).bind(&after).fetch_all(&db).await?;
            if keys.is_empty() {
                break;
            }
            for key in keys {
                client
                    .delete_object()
                    .bucket(env("VIGILO_BLOB_TEST_BUCKET")?)
                    .key(format!("{prefix}/{key}"))
                    .send()
                    .await
                    .map_err(|_| anyhow::anyhow!("exact fixture deletion failed: {key}"))?;
                after = key;
            }
        }
        sqlx::query("DELETE FROM dataset_catalog WHERE id=$1")
            .bind(upload)
            .execute(&db)
            .await?;
        sqlx::query("DELETE FROM dataset_upload_objects WHERE upload_id=$1")
            .bind(upload)
            .execute(&db)
            .await?;
        sqlx::query("DELETE FROM dataset_uploads WHERE id=$1")
            .bind(upload)
            .execute(&db)
            .await?;
    }
    result
}
