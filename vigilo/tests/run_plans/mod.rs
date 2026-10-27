//! Signed PostgreSQL/Ceph plan preparation through the actual CLI. The evaluator
//! registry row supplies metadata only; no evaluator execution is claimed here.
use super::*;
#[tokio::test]
async fn cli_plan_recovers_process_death_freezes_inputs_and_never_dispatches() -> Result<()> {
    let db = sqlx::PgPool::connect(&env("DATABASE_URL")?).await?;
    let root = tempfile::tempdir()?;
    let source = root.path().join("dataset.jsonl");
    let config = root.path().join("storage.toml");
    let profile_file = root.path().join("profile.json");
    let prefix = format!("plan-acceptance/{}", Uuid::now_v7());
    let owner = format!(
        "provider='s3'\nalias='cases'\nbucket='{}'\nprefix='{prefix}'\nregion='us-east-1'\nendpoint='{}'\nread_endpoint_variable='VIGILO_PLAN_READ_ENDPOINT'\nallow_http=true\npath_style=true\n[credential_variables]\naccess_key='VIGILO_BLOB_TEST_ACCESS_KEY_ID'\nsecret_key='VIGILO_BLOB_TEST_SECRET_ACCESS_KEY'\n",
        env("VIGILO_BLOB_TEST_BUCKET")?,
        env("VIGILO_BLOB_TEST_ENDPOINT")?
    );
    tokio::fs::write(&config, &owner).await?;
    let namespace = format!("plan-fixture-{}", Uuid::now_v7());
    let evaluator = Uuid::now_v7();
    let mut profile: Value = serde_yaml::from_str(include_str!("../../../example/profile.yaml"))?;
    profile["case_groups"][0]["evaluators"][0]["ref"] = json!(format!("{namespace}/score:1.0.0"));
    profile["agent"]["http"]["url"] = json!("http://127.0.0.1:1");
    tokio::fs::write(&profile_file, serde_json::to_vec(&profile)?).await?;
    let contract_hash = blake3::hash(include_bytes!(
        "../../../wit/evaluator/v1.0.0/evaluator.wit"
    ))
    .to_hex()
    .to_string();
    sqlx::query("INSERT INTO evaluators(id,namespace,name,version,content_hash,wasm_bytes,wasm_size_bytes,interface_name,interface_version,wit_world,abi_contract_hash,abi_adapter,runtime,runtime_version,runtime_fingerprint) VALUES($1,$2,'score','1.0.0',$3,''::bytea,0,'vigilo:evaluator/evaluator','1.0.0','evaluator-world',$4,'vigilo-evaluator-v1@1','wasmtime','44.0.0','fixture')").bind(evaluator).bind(&namespace).bind("1".repeat(64)).bind(contract_hash).execute(&db).await?;
    let upload = Uuid::now_v7();
    let run = Uuid::now_v7();
    let rejected = Uuid::now_v7();
    let dataset = Uuid::now_v7();
    let count = 1200;
    let mut file = tokio::fs::File::create(&source).await?;
    file.write_all(format!("{}\n",json!({"schema":"vigilo/dataset-upload/v1","fields":{"dataset_id":dataset,"dataset_version":"plan-fixture"}})).as_bytes()).await?;
    for _ in 0..count {
        file.write_all(
            format!(
                "{}\n",
                json!({"id":Uuid::now_v7(),"task_type":"classification","input":{"text":"hello"}})
            )
            .as_bytes(),
        )
        .await?;
    }
    file.flush().await?;
    drop(file);
    let plan_args = |id: Uuid| {
        vec![
            "run".into(),
            "create".into(),
            "--profile-file".into(),
            profile_file.display().to_string(),
            "--dataset-upload-id".into(),
            upload.to_string(),
            "--storage-config".into(),
            config.display().to_string(),
            "--run-id".into(),
            id.to_string(),
            "--prepare-only".into(),
        ]
    };
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
            .endpoint_url(env("VIGILO_BLOB_TEST_ENDPOINT")?)
            .force_path_style(true)
            .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
            .timeout_config(
                aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                    .operation_timeout(Duration::from_secs(10))
                    .build(),
            )
            .build(),
    );
    let result:Result<()>=async{
        ensure!(!output(&plan_args(rejected)).await?.status.success(),"unfinished dataset accepted");
        success(&args("upload",Some(upload),&source,&config)).await?;
        let mut child=command().args(plan_args(run)).spawn()?;
        tokio::time::timeout(Duration::from_secs(60),async{loop{
            let cursor:Option<i32>=sqlx::query_scalar("SELECT chunk_cursor FROM run_plans WHERE run_id=$1").bind(run).fetch_optional(&db).await?;
            if cursor.is_some_and(|v|v>0){child.kill().await?;break;}
            if let Some(status)=child.try_wait()?{anyhow::bail!("planning exited before crash point: {status}");}
            tokio::time::sleep(Duration::from_millis(5)).await;
        }Ok::<_,anyhow::Error>(())}).await??;
        // The process is confirmed dead; expire only its test lease.
        sqlx::query("UPDATE runs SET coordinator_leased_until=clock_timestamp()-interval '1 second' WHERE id=$1").bind(run).execute(&db).await?;
        sqlx::query("UPDATE evaluators SET state='disabled' WHERE id=$1").bind(evaluator).execute(&db).await?;
        let ready=success(&plan_args(run)).await?;ensure!(ready["data"]["plan_status"]=="ready"&&ready["data"]["status"]=="creating"&&ready["meta"]["dispatchable"]==false,"plan crossed dispatch boundary");
        let wrong_read=command().args(plan_args(run)).env("VIGILO_PLAN_READ_ENDPOINT","http://127.0.0.1:1").output().await?;
        ensure!(!wrong_read.status.success(),"an unavailable read route was accepted");
        let same=success(&plan_args(run)).await?;ensure!(same["data"]["run_plan"]==ready["data"]["run_plan"],"replay replaced immutable plan");
        let status=success(&["run".into(),"status".into(),run.to_string()]).await?;ensure!(status["data"]["creation_progress"]["plan_status"]=="ready"&&status["data"]["creation_progress"]["plan_chunk_cursor"]==12,"status lost planning progress");
        let no_work:bool=sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM run_shard_dispatch_cursors WHERE run_id=$1) AND NOT EXISTS(SELECT 1 FROM shard_placements WHERE run_id=$1)").bind(run).fetch_one(&db).await?;ensure!(no_work,"preparation copied case rows or enabled dispatch");
        profile["description"]=json!("changed");tokio::fs::write(&profile_file,serde_json::to_vec(&profile)?).await?;ensure!(!output(&plan_args(run)).await?.status.success(),"changed profile resumed");
        let cancelled=success(&["run".into(),"cancel".into(),run.to_string()]).await?;ensure!(cancelled["data"]["status"]=="cancelled","prepared run cancellation failed");
        Ok(())
    }.await;
    if let Err(error) = &result {
        eprintln!("plan acceptance failed: {error:#}");
    }
    // Exact fixture cleanup is separate from production retention. These run and
    // upload IDs and prefix belong exclusively to this invocation.
    for id in [run, rejected] {
        let mut after = String::new();
        loop {
            let keys:Vec<String>=sqlx::query_scalar("SELECT object_key FROM run_plan_objects WHERE run_id=$1 AND object_key>$2 ORDER BY object_key LIMIT 128").bind(id).bind(&after).fetch_all(&db).await?;
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
                    .map_err(|_| anyhow::anyhow!("exact plan fixture deletion failed"))?;
                after = key;
            }
        }
        sqlx::query("DELETE FROM runs WHERE id=$1")
            .bind(id)
            .execute(&db)
            .await?;
    }
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
                .map_err(|_| anyhow::anyhow!("exact dataset fixture deletion failed"))?;
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
    sqlx::query("DELETE FROM dataset_versions WHERE dataset_id=$1")
        .bind(dataset)
        .execute(&db)
        .await?;
    sqlx::query("DELETE FROM evaluators WHERE id=$1")
        .bind(evaluator)
        .execute(&db)
        .await?;
    result
}
