//! Foreground dataset upload, explicit resume, inspection and exact cleanup.
//!
//! Operators/job schedulers retain the original input and retry `resume` after a
//! crash. No coordinator claims local files. Ctrl-C cancels preparation/storage;
//! the lease is yielded where possible, and durable object ownership survives.
//! Credentials live in a separate owner-config file through environment names.
use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use clap::{
    Args,
    Subcommand,
};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    blob_storage::config::OwnerConfig,
    cli::Executable,
    context::Context,
    contracts::case_storage::FormatLimits,
    dataset_ingestion::input::InputFormat,
    db::workflows::dataset_upload as ledger,
    operations::datasets::{
        DatasetClient,
        PreparedDataset,
        workflow,
    },
};

#[derive(Debug, Args)]
pub(crate) struct Command {
    #[command(subcommand)]
    operation: Operation,
}
#[derive(Debug, Args)]
struct Source {
    #[arg(long)]
    input_file: PathBuf,
    #[arg(long, value_enum)]
    input_format: InputFormat,
    #[arg(long)]
    storage_config: PathBuf,
}
#[derive(Debug, Subcommand)]
enum Operation {
    /// Validate and store an immutable dataset; output its finalized reference
    Upload {
        #[command(flatten)]
        source: Source,
        #[arg(long)]
        upload_id: Option<Uuid>,
        #[arg(long,default_value_t=100,value_parser=clap::value_parser!(u32).range(1..=65536))]
        batch_cases: u32,
        #[arg(long,default_value_t=8*1024*1024,value_parser=clap::value_parser!(u32).range(256..=67108864))]
        batch_bytes: u32,
    },
    /// Resume the exact logical input under the upload's recorded batching policy
    Resume {
        upload_id: Uuid,
        #[command(flatten)]
        source: Source,
    },
    /// Inspect compact upload progress and its finalized catalog reference
    Status { upload_id: Uuid },
    /// Revoke upload authority; finalized datasets cannot be aborted
    Abort { upload_id: Uuid },
    /// Expire one bounded page of abandoned uploads; suitable for a job scheduler
    Expire,
    /// Remove one bounded page of an aborted/expired upload's exact owned keys
    Cleanup {
        upload_id: Uuid,
        #[arg(long)]
        storage_config: PathBuf,
        #[arg(long,default_value_t=100,value_parser=clap::value_parser!(u32).range(1..=256))]
        limit: u32,
    },
}
pub(super) fn owner(path: &std::path::Path) -> Result<OwnerConfig> {
    OwnerConfig::load(path)
}
#[async_trait]
impl Executable for Command {
    async fn exec(self, context: Context) -> Result<()> {
        let cancel = CancellationToken::new();
        let signal = cancel.clone();
        let task = tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                signal.cancel();
            }
        });
        let result = exec(self.operation, context, &cancel).await;
        task.abort();
        result
    }
}
async fn exec(operation: Operation, context: Context, cancel: &CancellationToken) -> Result<()> {
    let db = context.dbr().await?.control().await?;
    let output = context.out().await?;
    match operation {
        Operation::Status { upload_id } => {
            let (row, catalog) = workflow::inspect(db.into(), upload_id).await?;
            output.write_value(&json!({"data":row,"catalog":catalog}))?;
        }
        Operation::Abort { upload_id } => {
            ledger::abort(db, upload_id).await?;
            output.write_value(&json!({"data":{"upload_id":upload_id,"status":"aborted"}}))?;
        }
        Operation::Expire => {
            output
                .write_value(&json!({"data":{"expired_upload_ids":ledger::expire(db).await?}}))?;
        }
        Operation::Cleanup {
            upload_id,
            storage_config,
            limit,
        } => {
            let owner = owner(&storage_config)?;
            let client = DatasetClient::operator(db.clone(), owner);
            let result = client.cleanup(upload_id, limit, cancel).await?;
            output.write_value(&json!({"data":result}))?;
        }
        Operation::Upload {
            source,
            upload_id,
            batch_cases,
            batch_bytes,
        } => {
            let mut limits = FormatLimits::default();
            limits.batch.max_cases = batch_cases as usize;
            limits.batch.max_bytes = batch_bytes as usize;
            limits.resident_batch_bytes = limits.batch.max_bytes * limits.resident_batches;
            upload(db, output, source, upload_id, limits, cancel).await?;
        }
        Operation::Resume { upload_id, source } => {
            let client = DatasetClient::operator(db.clone(), owner(&source.storage_config)?);
            let result = client
                .resume_file(upload_id, source.input_file, source.input_format, cancel)
                .await?;
            write_publication(output, &result)?;
        }
    }
    Ok(())
}
async fn upload(
    db: &sqlx::PgPool,
    output: &crate::context::output::Buffer,
    source: Source,
    upload_id: Option<Uuid>,
    limits: FormatLimits,
    cancel: &CancellationToken,
) -> Result<()> {
    let owner = owner(&source.storage_config)?;
    owner.validate(&limits)?;
    let prepared =
        PreparedDataset::file(source.input_file, source.input_format, limits, cancel).await?;
    let client = DatasetClient::operator(db.clone(), owner);
    let result = client.upload_inner(prepared, upload_id, cancel).await?;
    write_publication(output, &result)
}
fn write_publication(
    output: &crate::context::output::Buffer,
    result: &crate::operations::datasets::workflow::Published,
) -> Result<()> {
    let value = if result.reused {
        json!({"data":result.catalog,"meta":{"reused":true}})
    } else {
        json!({"data":result.catalog,"meta":{"case_count":result.identity.case_count,"reused":false}})
    };
    output.write_value(&value)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use crate::cli::App;
    fn parse(arguments: &[&str]) -> bool {
        let mut args = vec!["vigilo", "--database-url", "postgresql://unused", "dataset"];
        args.extend_from_slice(arguments);
        App::try_parse_from(args).is_ok()
    }
    #[test]
    fn dataset_commands_parse_without_resolving_storage_or_services() {
        let id = "018f1111-1111-7111-8111-111111111111";
        for op in ["status", "abort"] {
            assert!(parse(&[op, id]));
        }
        assert!(parse(&["expire"]));
        assert!(parse(&[
            "cleanup",
            id,
            "--storage-config",
            "storage.toml",
            "--limit",
            "1"
        ]));
        for format in ["json", "yaml", "jsonl"] {
            assert!(parse(&[
                "upload",
                "--input-file",
                "cases",
                "--input-format",
                format,
                "--storage-config",
                "storage.toml"
            ]));
        }
        assert!(parse(&[
            "resume",
            id,
            "--input-file",
            "cases.jsonl",
            "--input-format",
            "jsonl",
            "--storage-config",
            "storage.toml"
        ]));
    }
    #[test]
    fn dataset_cli_rejects_missing_inputs_and_unbounded_overrides() {
        assert!(!parse(&["upload"]));
        assert!(!parse(&[
            "upload",
            "--input-file",
            "cases.jsonl",
            "--input-format",
            "unknown",
            "--storage-config",
            "storage.toml"
        ]));
        for (flag, value) in [
            ("--batch-cases", "0"),
            ("--batch-cases", "65537"),
            ("--batch-bytes", "255"),
            ("--batch-bytes", "67108865"),
        ] {
            assert!(!parse(&[
                "upload",
                "--input-file",
                "cases.jsonl",
                "--input-format",
                "jsonl",
                "--storage-config",
                "storage.toml",
                flag,
                value
            ]));
        }
        assert!(!parse(&[
            "cleanup",
            "018f1111-1111-7111-8111-111111111111",
            "--storage-config",
            "storage.toml",
            "--limit",
            "0"
        ]));
    }
}
