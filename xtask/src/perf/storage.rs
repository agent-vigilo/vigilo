//! Independent object-storage oracle for campaign-owned immutable fixtures.
//!
//! This is outside measured processes and shares no production codec or adapter
//! implementation. It verifies exact bytes and contiguous file membership before
//! accepting samples or seeding synthetic administration fixtures. Reads retain
//! one bounded file, SQL metadata is paged, and teardown owns the entire isolated
//! campaign volume rather than scanning storage prefixes.
use std::time::Duration;

use anyhow::{
    Context as _,
    Result,
    ensure,
};
use aws_sdk_s3::{
    Client,
    config::{
        BehaviorVersion,
        Credentials,
        Region,
        retry::RetryConfig,
        timeout::TimeoutConfig,
    },
    primitives::ByteStream,
};
use postgres::{
    Client as PgClient,
    NoTls,
};
use serde_json::{
    Value,
    json,
};
use tokio::runtime::Runtime;

pub(super) struct Reader {
    runtime: Runtime,
    client: Client,
    prefix: String,
}
impl Reader {
    pub fn new(endpoint: &str, prefix: &str) -> Result<Self> {
        let url = url::Url::parse(endpoint)?;
        ensure!(
            url.scheme() == "http" && url.host_str() == Some("127.0.0.1"),
            "performance storage must be loopback"
        );
        let runtime = Runtime::new()?;
        let client = {
            let _entered = runtime.enter();
            Client::from_conf(
                aws_sdk_s3::config::Builder::new()
                    .behavior_version(BehaviorVersion::latest())
                    .region(Region::new("us-east-1"))
                    .endpoint_url(endpoint)
                    .force_path_style(true)
                    .credentials_provider(Credentials::new(
                        "vigilo_perf",
                        "vigilo_perf_storage_secret",
                        None,
                        None,
                        "performance-fixture",
                    ))
                    .retry_config(RetryConfig::disabled())
                    .timeout_config(
                        TimeoutConfig::builder()
                            .connect_timeout(Duration::from_secs(5))
                            .read_timeout(Duration::from_secs(15))
                            .operation_timeout(Duration::from_secs(120))
                            .build(),
                    )
                    .build(),
            )
        };
        Ok(Self {
            runtime,
            client,
            prefix: prefix.into(),
        })
    }
    /// Proves signed object ownership and exact deletion in the topology tier.
    pub fn probe(&self) -> Result<()> {
        let key = format!("{}/topology-probe/{}", self.prefix, uuid::Uuid::now_v7());
        self.runtime.block_on(async {
            self.client
                .put_object()
                .bucket("vigilo-perf")
                .key(&key)
                .if_none_match("*")
                .body(ByteStream::from_static(b"sentinel"))
                .send()
                .await?;
            let checked = async {
                let object = self
                    .client
                    .get_object()
                    .bucket("vigilo-perf")
                    .key(&key)
                    .send()
                    .await?;
                ensure!(
                    object.body.collect().await?.into_bytes().as_ref() == b"sentinel",
                    "storage topology sentinel changed"
                );
                Ok::<_, anyhow::Error>(())
            }
            .await;
            let removed = self
                .client
                .delete_object()
                .bucket("vigilo-perf")
                .key(&key)
                .send()
                .await;
            checked?;
            removed?;
            Ok(())
        })
    }
    fn read(&self, reference: &Value) -> Result<Vec<u8>> {
        ensure!(
            reference["storage_alias"] == "cases" && reference["hash_scheme"] == "blake3-256",
            "unexpected fixture storage reference"
        );
        let key = reference["key"].as_str().context("missing object key")?;
        let count = reference["byte_count"]
            .as_u64()
            .context("missing object byte count")?;
        ensure!(
            count > 0 && count <= 64 * 1024 * 1024,
            "fixture object exceeds 64 MiB"
        );
        self.runtime.block_on(async {
            let mut object = self
                .client
                .get_object()
                .bucket("vigilo-perf")
                .key(format!("{}/{key}", self.prefix))
                .send()
                .await?;
            let mut bytes = Vec::new();
            while let Some(part) = object.body.try_next().await? {
                ensure!(
                    bytes.len() as u64 + part.len() as u64 <= count,
                    "object exceeds advertised size"
                );
                bytes.extend_from_slice(&part);
            }
            verify_bytes(reference, &bytes)?;
            Ok(bytes)
        })
    }
    /// Iterates the exact acknowledged batch ledger, validating immutable files
    /// and original ordinal order independently of the application reader.
    pub fn batches(
        &self,
        control_url: &str,
        run_id: &str,
        mut accept: impl FnMut(Vec<Value>) -> Result<()>,
    ) -> Result<i64> {
        let mut db = PgClient::connect(control_url, NoTls)?;
        let identity=db.query_one("SELECT r.dataset_id::text,r.expected_execution_count,p.dataset_upload_id::text FROM runs r JOIN run_plans p ON p.run_id=r.id WHERE r.id=$1::text::uuid",&[&run_id])?;
        let dataset: String = identity.get(0);
        let expected: i32 = identity.get(1);
        let upload: String = identity.get(2);
        let mut next = 0_i32;
        let mut after = -1_i32;
        loop {
            let files=db.query("SELECT reference,range_start,range_end FROM dataset_upload_objects WHERE upload_id=$1::text::uuid AND kind='batch' AND acknowledged AND range_start>$2 ORDER BY range_start LIMIT 64",&[&upload,&after])?;
            if files.is_empty() {
                break;
            }
            for file in files {
                let reference: Value = file.get(0);
                let start: i32 = file.get(1);
                let end: i32 = file.get(2);
                ensure!(
                    start == next && end > start,
                    "fixture batch ledger has a gap or overlap"
                );
                let bytes = self.read(&reference)?;
                let records = parse_batch(&bytes, &dataset, start, end)?;
                accept(records)?;
                next = end;
                after = start;
            }
        }
        ensure!(
            next == expected,
            "stored fixture has {next} cases, expected {expected}"
        );
        Ok(i64::from(next))
    }
}
fn verify_bytes(reference: &Value, bytes: &[u8]) -> Result<()> {
    ensure!(
        reference["byte_count"].as_u64() == Some(bytes.len() as u64)
            && reference["digest"].as_str() == Some(blake3::hash(bytes).to_hex().as_str()),
        "fixture object integrity mismatch"
    );
    Ok(())
}
fn parse_batch(bytes: &[u8], dataset: &str, start: i32, end: i32) -> Result<Vec<Value>> {
    ensure!(bytes.last() == Some(&b'\n'), "fixture batch is truncated");
    let mut lines = bytes[..bytes.len() - 1].split(|c| *c == b'\n');
    let header: Value = serde_json::from_slice(lines.next().context("missing batch header")?)?;
    ensure!(
        header["schema"] == "vigilo/case-batch/v1"
            && header["dataset_id"] == dataset
            && header["ordinal_start"] == start
            && header["ordinal_end"] == end,
        "fixture batch identity mismatch"
    );
    let mut records = Vec::new();
    for line in lines {
        let record: Value = serde_json::from_slice(line)?;
        ensure!(
            record["case_ordinal"] == start + i32::try_from(records.len())?
                && record["source"].is_object(),
            "fixture case ordinal mismatch"
        );
        let source = &record["source"];
        let mut tags = source["tags"].as_array().cloned().unwrap_or_default();
        tags.sort_by_key(Value::to_string);
        let payload = json!({"task_type":source["task_type"],"case_group":source["case_group"],"input":source["input"],"expected_output":source["expected"],"context":source["context"],"tags":tags,"metadata":source.get("metadata").cloned().unwrap_or_else(||json!({}))});
        ensure!(
            record["case_hash"].as_str()
                == Some(
                    blake3::hash(&serde_json::to_vec(&payload)?)
                        .to_hex()
                        .as_str()
                ),
            "fixture normalized case hash mismatch"
        );
        records.push(record);
    }
    ensure!(
        records.len() == usize::try_from(end - start)?,
        "fixture batch count mismatch"
    );
    Ok(records)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn storage_oracle_rejects_corruption_truncation_and_wrong_membership() {
        let bytes = b"sentinel";
        let reference =
            json!({"byte_count":bytes.len(),"digest":blake3::hash(bytes).to_hex().to_string()});
        verify_bytes(&reference, bytes).unwrap();
        assert!(verify_bytes(&reference, b"changed").is_err());
        let dataset = uuid::Uuid::nil().to_string();
        let source = json!({"id":uuid::Uuid::nil(),"task_type":"classification","input":{},"expected":null,"tags":[],"metadata":{}});
        let normalized = json!({"task_type":"classification","case_group":null,"input":{},"expected_output":null,"context":null,"tags":[],"metadata":{}});
        let record = json!({"case_ordinal":0,"case_hash":blake3::hash(&serde_json::to_vec(&normalized).unwrap()).to_hex().to_string(),"source":source});
        let encoded=format!("{}\n{}\n",json!({"schema":"vigilo/case-batch/v1","dataset_id":dataset,"ordinal_start":0,"ordinal_end":1}),record).into_bytes();
        assert_eq!(parse_batch(&encoded, &dataset, 0, 1).unwrap().len(), 1);
        assert!(parse_batch(&encoded[..encoded.len() - 1], &dataset, 0, 1).is_err());
        assert!(parse_batch(&encoded, "other", 0, 1).is_err());
        assert!(parse_batch(&encoded, &dataset, 0, 2).is_err());
    }
}
