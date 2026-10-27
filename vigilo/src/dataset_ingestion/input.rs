//! Bounded input preparation and external duplicate-ID validation.
//!
//! JSONL admits one bounded source record at a time. Conventional JSON/YAML
//! documents have an explicit 8 MiB input cap; the versioned JSONL path supports
//! large datasets without retaining all cases. Canonical records and UUIDs are
//! spooled privately, with one process-wide preparation permit owned by callers.
//! External sorting uses 1 MiB runs and 32-way merges, never a global ID set.
use std::{
    fs::File,
    io::{
        BufRead,
        BufReader,
        BufWriter,
        Read,
        Write,
    },
    path::Path,
    sync::Arc,
};

use anyhow::{
    Context,
    Result,
    ensure,
};
use serde::{
    Deserialize,
    de::{
        DeserializeSeed,
        Error,
        MapAccess,
        SeqAccess,
        Visitor,
    },
};
use serde_json::Value;
use tokio::sync::{
    OwnedSemaphorePermit,
    Semaphore,
};
use tokio_util::sync::CancellationToken;

use crate::contracts::{
    case_storage::{
        identity::{
            DatasetIdentity,
            DatasetIdentityBuilder,
            canonical_json,
            case_hash,
        },
        *,
    },
    run::{
        DatasetCase,
        RunDataset,
    },
};

static PREPARATION: std::sync::OnceLock<Arc<Semaphore>> = std::sync::OnceLock::new();
const MIB: u64 = 1024 * 1024;
const SPOOL_CAP: u64 = 8 * 1024 * MIB;
const SOURCE_CAP: u64 = 8 * 1024 * MIB;
const DOCUMENT_CAP: u64 = 8 * MIB;

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub(crate) enum InputFormat {
    Json,
    Yaml,
    Jsonl,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    schema: String,
    fields: Value,
}

pub(crate) struct Prepared {
    pub records: tempfile::NamedTempFile,
    pub fields: DatasetSourceFields,
    pub identity: DatasetIdentity,
    pub limits: FormatLimits,
    _reservation: OwnedSemaphorePermit,
}
/// Preparation is explicitly admitted once per process, including files retained
/// during upload. Cancellation also interrupts the blocking parser/sorter.
pub(crate) async fn prepare(
    path: std::path::PathBuf,
    format: InputFormat,
    limits: FormatLimits,
    cancel: &CancellationToken,
) -> Result<Prepared> {
    limits.validate()?;
    let pool = PREPARATION
        .get_or_init(|| Arc::new(Semaphore::new(1)))
        .clone();
    let reservation = tokio::select! {biased;_=cancel.cancelled()=>anyhow::bail!("dataset preparation cancelled"),permit=pool.acquire_owned()=>permit?};
    let token = cancel.child_token();
    let _cancel_on_drop = token.clone().drop_guard();
    let (output, reservation) = tokio::task::spawn_blocking(move || {
        prepare_sync(&path, format, limits, &token).map(|o| (o, reservation))
    })
    .await??;
    Ok(Prepared {
        records: output.0,
        fields: output.1,
        identity: output.2,
        limits: output.3,
        _reservation: reservation,
    })
}

fn cancelled(token: &CancellationToken) -> Result<()> {
    ensure!(!token.is_cancelled(), "dataset preparation cancelled");
    Ok(())
}
struct Quota {
    used: u64,
}
impl Quota {
    fn add(&mut self, n: usize) -> Result<()> {
        self.used = self
            .used
            .checked_add(n as u64)
            .context("spool size overflow")?;
        ensure!(
            self.used <= SPOOL_CAP,
            "dataset validation spool exceeds 8 GiB"
        );
        Ok(())
    }
}

type Material = (
    tempfile::NamedTempFile,
    DatasetSourceFields,
    DatasetIdentity,
    FormatLimits,
);
fn prepare_sync(
    path: &Path,
    format: InputFormat,
    limits: FormatLimits,
    cancel: &CancellationToken,
) -> Result<Material> {
    cancelled(cancel)?;
    let file = File::open(path).context("opening dataset input")?;
    ensure!(
        file.metadata()?.is_file(),
        "dataset input must be a regular file"
    );
    ensure!(
        file.metadata()?.len() <= SOURCE_CAP,
        "dataset source exceeds 8 GiB"
    );
    let mut reader = BufReader::new(file);
    let mut header_bytes = 0;
    let (fields, mut conventional) = match format {
        InputFormat::Jsonl => {
            let line = line(&mut reader, limits.source_bytes)?;
            header_bytes = line.len() as u64;
            let h: Header = serde_json::from_value(strict_json(&line)?)?;
            ensure!(
                h.schema == "vigilo/dataset-upload/v1",
                "unknown dataset upload schema"
            );
            (h.fields, None)
        }
        InputFormat::Json | InputFormat::Yaml => {
            let mut bytes = Vec::new();
            (&mut reader)
                .take(DOCUMENT_CAP + 1)
                .read_to_end(&mut bytes)?;
            ensure!(
                bytes.len() as u64 <= DOCUMENT_CAP,
                "JSON/YAML dataset exceeds 8 MiB; use the versioned JSONL upload format"
            );
            let mut value = match format {
                InputFormat::Json => strict_json(&bytes)?,
                _ => yaml(&bytes)?,
            };
            let object = value
                .as_object_mut()
                .context("dataset input must be an object")?;
            let cases = object
                .remove("cases")
                .context("dataset cases are missing")?;
            let cases = match cases {
                Value::Array(cases) => cases,
                _ => anyhow::bail!("dataset cases must be an array"),
            };
            (value, Some(cases.into_iter()))
        }
    };
    let fields = DatasetSourceFields {
        schema: StorageFormat::DatasetSourceFields,
        cases_present: true,
        fields,
    };
    encode(&fields, &limits)?;
    let dataset: RunDataset = serde_json::from_value(fields.fields.clone())?;
    ensure!(
        dataset
            .dataset_version
            .as_ref()
            .is_none_or(|v| v.len() <= 1024),
        "dataset version exceeds 1024 bytes"
    );
    let mut identity = DatasetIdentityBuilder::new(&fields, &limits)?;
    let records = tempfile::NamedTempFile::new()?;
    let mut output = BufWriter::new(records.reopen()?);
    let ids = tempfile::tempdir()?;
    let mut idfile = BufWriter::new(File::create(ids.path().join("ids"))?);
    let mut quota = Quota { used: 0 };
    let mut count = 0_i32;
    let mut source_bytes = header_bytes;
    loop {
        cancelled(cancel)?;
        let source = if let Some(cases) = conventional.as_mut() {
            match cases.next() {
                Some(v) => v,
                None => break,
            }
        } else {
            let raw = line(&mut reader, limits.batch.max_bytes)?;
            if raw.is_empty() {
                break;
            }
            source_bytes = source_bytes
                .checked_add(raw.len() as u64)
                .context("source byte overflow")?;
            ensure!(source_bytes <= SOURCE_CAP, "dataset source exceeds 8 GiB");
            strict_json(&raw)?
        };
        let case: DatasetCase = serde_json::from_value(source.clone())?;
        let record = CaseRecord {
            case_ordinal: count,
            case_hash: case_hash(&case)?,
            source,
        };
        identity.push(&record)?;
        // The batch builder also checks envelope/header fit later. Bound each
        // canonical record here before committing bytes to the validation spool.
        let encoded = serde_json::to_vec(&canonical_json(&serde_json::to_value(&record)?))?;
        ensure!(
            encoded.len() < limits.batch.max_bytes,
            "case record exceeds batch byte limit"
        );
        quota.add(encoded.len() + 1 + 16)?;
        output.write_all(&encoded)?;
        output.write_all(b"\n")?;
        idfile.write_all(case.id.as_bytes())?;
        count = count.checked_add(1).context("case count exceeds i32")?;
    }
    output.flush()?;
    idfile.flush()?;
    drop(output);
    drop(idfile);
    unique_ids(ids.path(), count as usize, &mut quota, cancel)?;
    let (dataset_hash, dataset_version_id, case_count) = identity.finish()?;
    Ok((
        records,
        fields,
        DatasetIdentity {
            dataset_id: dataset.dataset_id,
            dataset_version: dataset.dataset_version,
            dataset_version_id,
            dataset_hash,
            case_count,
        },
        limits,
    ))
}

/// Read at most cap+1 bytes, requiring LF framing. Empty EOF is distinct from an
/// empty line; a malformed/oversized suffix is never silently ignored.
pub(super) fn line(reader: &mut impl BufRead, cap: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader.take(cap as u64 + 1).read_until(b'\n', &mut bytes)?;
    ensure!(bytes.len() <= cap, "dataset record exceeds byte limit");
    ensure!(
        bytes.is_empty() || bytes.last() == Some(&b'\n'),
        "dataset records require final LF"
    );
    ensure!(bytes.len() != 1, "empty dataset record");
    Ok(bytes)
}

fn read_id(reader: &mut impl Read) -> Result<Option<[u8; 16]>> {
    let mut id = [0_u8; 16];
    let n = reader.read(&mut id[..1])?;
    if n == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut id[1..])?;
    Ok(Some(id))
}
fn unique_ids(
    dir: &Path,
    count: usize,
    quota: &mut Quota,
    cancel: &CancellationToken,
) -> Result<()> {
    let source = dir.join("ids");
    let mut input = BufReader::new(File::open(&source)?);
    let mut runs = 0;
    loop {
        cancelled(cancel)?;
        let mut chunk = Vec::with_capacity(65536);
        while chunk.len() < 65536 {
            match read_id(&mut input)? {
                Some(id) => chunk.push(id),
                None => break,
            }
        }
        if chunk.is_empty() {
            break;
        }
        chunk.sort_unstable();
        ensure!(!chunk.windows(2).any(|w| w[0] == w[1]), "duplicate case ID");
        let mut file = BufWriter::new(File::create(dir.join(format!("0-{runs}")))?);
        for id in chunk {
            quota.add(16)?;
            file.write_all(&id)?;
        }
        file.flush()?;
        runs += 1;
    }
    drop(input);
    std::fs::remove_file(source)?;
    quota.used -= count as u64 * 16;
    let mut level = 0;
    while runs > 1 {
        let mut next = 0;
        for start in (0..runs).step_by(32) {
            cancelled(cancel)?;
            let end = (start + 32).min(runs);
            let mut readers = Vec::new();
            let mut sizes = Vec::new();
            let mut heap = std::collections::BinaryHeap::new();
            for index in start..end {
                let path = dir.join(format!("{level}-{index}"));
                sizes.push(std::fs::metadata(&path)?.len());
                let mut r = BufReader::new(File::open(path)?);
                if let Some(id) = read_id(&mut r)? {
                    heap.push(std::cmp::Reverse((id, readers.len())));
                }
                readers.push(r);
            }
            let mut output =
                BufWriter::new(File::create(dir.join(format!("{}-{next}", level + 1)))?);
            let mut previous = None;
            let mut steps = 0;
            while let Some(std::cmp::Reverse((id, index))) = heap.pop() {
                if steps % 4096 == 0 {
                    cancelled(cancel)?;
                }
                steps += 1;
                ensure!(previous != Some(id), "duplicate case ID");
                previous = Some(id);
                quota.add(16)?;
                output.write_all(&id)?;
                if let Some(id) = read_id(&mut readers[index])? {
                    heap.push(std::cmp::Reverse((id, index)));
                }
            }
            output.flush()?;
            drop(output);
            drop(readers);
            for (index, size) in (start..end).zip(sizes) {
                std::fs::remove_file(dir.join(format!("{level}-{index}")))?;
                quota.used -= size;
            }
            next += 1;
        }
        runs = next;
        level += 1;
    }
    Ok(())
}

// Bound expanded YAML values as well as file bytes: aliases must not amplify a
// small admitted source into an unbounded in-memory tree. Duplicate keys fail.
struct Limited<'a> {
    remaining: &'a mut usize,
    depth: usize,
}
impl<'de> DeserializeSeed<'de> for Limited<'_> {
    type Value = Value;
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        de: D,
    ) -> std::result::Result<Value, D::Error> {
        if self.depth >= 128 || *self.remaining < 64 {
            return Err(D::Error::custom(
                "expanded YAML exceeds memory/depth budget",
            ));
        }
        *self.remaining -= 64;
        de.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Limited<'_> {
    type Value = Value;
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("bounded JSON-compatible YAML")
    }
    fn visit_unit<E: Error>(self) -> std::result::Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_bool<E: Error>(self, v: bool) -> std::result::Result<Value, E> {
        Ok(v.into())
    }
    fn visit_i64<E: Error>(self, v: i64) -> std::result::Result<Value, E> {
        Ok(v.into())
    }
    fn visit_u64<E: Error>(self, v: u64) -> std::result::Result<Value, E> {
        Ok(v.into())
    }
    fn visit_f64<E: Error>(self, v: f64) -> std::result::Result<Value, E> {
        serde_json::Number::from_f64(v)
            .map(Value::Number)
            .ok_or_else(|| E::custom("nonfinite YAML number"))
    }
    fn visit_str<E: Error>(self, v: &str) -> std::result::Result<Value, E> {
        if v.len() > *self.remaining {
            return Err(E::custom("expanded YAML exceeds memory budget"));
        }
        *self.remaining -= v.len();
        Ok(v.into())
    }
    fn visit_string<E: Error>(self, v: String) -> std::result::Result<Value, E> {
        self.visit_str(&v)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<Value, A::Error> {
        let mut values = Vec::new();
        while let Some(v) = seq.next_element_seed(Limited {
            remaining: self.remaining,
            depth: self.depth + 1,
        })? {
            values.push(v);
        }
        Ok(Value::Array(values))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<Value, A::Error> {
        let mut values = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if values.contains_key(&key) || key.len() > *self.remaining {
                return Err(A::Error::custom(
                    "duplicate YAML key or expanded memory budget exceeded",
                ));
            }
            *self.remaining -= key.len();
            let value = map.next_value_seed(Limited {
                remaining: self.remaining,
                depth: self.depth + 1,
            })?;
            values.insert(key, value);
        }
        Ok(Value::Object(values))
    }
}
/// Parse JSON-compatible YAML with duplicate-key, expanded-value and depth bounds.
/// Callers must bound source bytes before parsing; this also admits JSON profiles.
pub(crate) fn yaml(bytes: &[u8]) -> Result<Value> {
    let mut remaining = 32 * 1024 * 1024;
    Ok(Limited {
        remaining: &mut remaining,
        depth: 0,
    }
    .deserialize(serde_yaml::Deserializer::from_slice(bytes))?)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    fn prepare_text(bytes: &[u8], format: InputFormat) -> Result<Material> {
        let mut f = tempfile::NamedTempFile::new()?;
        f.write_all(bytes)?;
        prepare_sync(
            f.path(),
            format,
            FormatLimits::default(),
            &CancellationToken::new(),
        )
    }
    fn jsonl(mut value: Value) -> Vec<u8> {
        let cases = value.as_object_mut().unwrap().remove("cases").unwrap();
        let mut bytes =
            serde_json::to_vec(&json!({"schema":"vigilo/dataset-upload/v1","fields":value}))
                .unwrap();
        bytes.push(b'\n');
        for case in cases.as_array().unwrap() {
            bytes.extend(serde_json::to_vec(case).unwrap());
            bytes.push(b'\n');
        }
        bytes
    }
    #[test]
    fn json_yaml_and_streaming_input_preserve_the_independent_identity_oracle() {
        let expected: Value = serde_json::from_str(include_str!(
            "../cli/commands/run/fixtures/dataset-identity.expected.json"
        ))
        .unwrap();
        let json = include_bytes!("../cli/commands/run/fixtures/dataset-identity.json");
        let yaml = include_bytes!("../cli/commands/run/fixtures/dataset-identity.yaml");
        let streaming = jsonl(serde_json::from_slice(json).unwrap());
        for (bytes, format) in [
            (json.as_slice(), InputFormat::Json),
            (yaml.as_slice(), InputFormat::Yaml),
            (streaming.as_slice(), InputFormat::Jsonl),
        ] {
            let (_, _, identity, _) = prepare_text(bytes, format).unwrap();
            assert_eq!(
                identity.dataset_hash,
                expected["dataset_hash"].as_str().unwrap()
            );
            assert_eq!(
                identity.dataset_version_id.to_string(),
                expected["dataset_version_id"].as_str().unwrap()
            );
            assert_eq!(identity.case_count, 3);
        }
    }
    #[test]
    fn invalid_suffix_duplicate_ids_and_fields_fail_before_upload() {
        let value: Value = serde_json::from_slice(include_bytes!(
            "../cli/commands/run/fixtures/dataset-identity.json"
        ))
        .unwrap();
        let valid = jsonl(value.clone());
        let mut truncated = valid.clone();
        truncated.pop();
        assert!(prepare_text(&truncated, InputFormat::Jsonl).is_err());
        let mut suffix = valid.clone();
        suffix.extend(b"malformed\n");
        assert!(prepare_text(&suffix, InputFormat::Jsonl).is_err());
        let mut duplicate = value.clone();
        duplicate["cases"][2]["id"] = duplicate["cases"][0]["id"].clone();
        assert!(prepare_text(&jsonl(duplicate), InputFormat::Jsonl).is_err());
        let mut empty = value;
        empty["cases"] = json!([]);
        assert!(prepare_text(&jsonl(empty), InputFormat::Jsonl).is_err());
        assert!(
            prepare_text(
                b"{\"schema\":\"unknown\",\"fields\":{}}\n",
                InputFormat::Jsonl
            )
            .is_err()
        );
        assert!(
            prepare_text(
                b"{\"dataset_id\":1,\"dataset_id\":2,\"cases\":[]}",
                InputFormat::Json
            )
            .is_err()
        );
    }
    #[test]
    fn bounded_line_and_yaml_expansion_are_fail_closed() {
        for input in [b"123456789\n".as_slice(), b"123", b"\n"] {
            assert!(line(&mut std::io::Cursor::new(input), 8).is_err());
        }
        assert_eq!(
            line(&mut std::io::Cursor::new(b"123\n"), 4).unwrap(),
            b"123\n"
        );
        assert!(yaml(b"x: 1\nx: 2\n").is_err());
        assert!(yaml(b"x: .nan\n").is_err());
        let text = format!(
            "a: &a '{}'\nb: &b [*a,*a,*a,*a,*a,*a,*a,*a,*a,*a]\nc: &c [*b,*b,*b,*b,*b,*b,*b,*b,*b,*b]\nd: [*c,*c,*c,*c,*c,*c,*c,*c,*c,*c]\n",
            "x".repeat(100_000)
        );
        assert!(yaml(text.as_bytes()).is_err());
        let mut q = Quota { used: SPOOL_CAP };
        assert!(q.add(1).is_err());
    }
    #[test]
    fn external_sort_detects_duplicates_across_runs_and_cleans_scratch() {
        for duplicate in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let count = 65537;
            let mut file = BufWriter::new(File::create(dir.path().join("ids")).unwrap());
            for i in 0..count {
                let n = if duplicate && i == count - 1 { 0 } else { i };
                file.write_all(&(n as u128).to_be_bytes()).unwrap();
            }
            file.flush().unwrap();
            drop(file);
            let result = unique_ids(
                dir.path(),
                count,
                &mut Quota {
                    used: count as u64 * 16,
                },
                &CancellationToken::new(),
            );
            assert_eq!(result.is_err(), duplicate);
        }
    }
    #[tokio::test]
    async fn cancellation_releases_preparation_admission() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(include_bytes!(
            "../cli/commands/run/fixtures/dataset-identity.json"
        ))
        .unwrap();
        let first = prepare(
            file.path().to_owned(),
            InputFormat::Json,
            FormatLimits::default(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(
            prepare(
                file.path().to_owned(),
                InputFormat::Json,
                FormatLimits::default(),
                &cancel
            )
            .await
            .is_err()
        );
        drop(first);
        prepare(
            file.path().to_owned(),
            InputFormat::Json,
            FormatLimits::default(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    }
}
