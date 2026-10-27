//! Bounded canonical JSON and sequential JSONL codecs.
//!
//! Reference integrity precedes parsing. Strict JSON parsing rejects duplicate
//! keys recursively, including user data, instead of silently discarding fields.
//! Both decoders require a VerifiedObject byte-integrity proof. Metadata decoding
//! parses field types; callers explicitly validate semantics before use. Batch
//! decoding returns a header awaiting validation, then a reader parses and
//! validates one source case at a time. Encoding uses a capped writer before
//! canonicalization. Successful full consumption is required before
//! publication, so no valid prefix can authorize a truncated object.

use std::{
    collections::BTreeSet,
    fmt,
    io::{
        self,
        Write,
    },
};

use anyhow::{
    Context,
    Result,
    ensure,
};
use serde::{
    Deserialize,
    Deserializer,
    Serialize,
    de::{
        self,
        MapAccess,
        SeqAccess,
        Visitor,
    },
};
use serde_json::Value;
use uuid::Uuid;

use super::{
    CaseBatchHeader,
    CaseRecord,
    FormatLimits,
    StorageDocument,
    StorageFormat,
    VerifiedObject,
    formats::range,
    identity::canonical_json,
};

struct CappedWriter {
    bytes: Vec<u8>,
    limit: usize,
}

impl Write for CappedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("encoded object exceeds byte limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Limits serialized bytes before allocating a recursively sorted JSON value.
pub(super) fn compact<T: Serialize>(value: &T, limit: usize) -> Result<Vec<u8>> {
    let mut writer = CappedWriter {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut writer, value)
        .context("encoded object exceeds byte limit or cannot be serialized")?;
    let parsed = strict_json(&writer.bytes)?;
    let bytes = serde_json::to_vec(&canonical_json(&parsed))?;
    ensure!(bytes.len() <= limit, "canonical object exceeds byte limit");
    Ok(bytes)
}

struct StrictValue(Value);

impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct StrictVisitor;
        impl<'de> Visitor<'de> for StrictVisitor {
            type Value = StrictValue;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("JSON without duplicate keys")
            }
            fn visit_bool<E: de::Error>(self, value: bool) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Bool(value)))
            }
            fn visit_i64<E: de::Error>(self, value: i64) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(value.into()))
            }
            fn visit_u64<E: de::Error>(self, value: u64) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(value.into()))
            }
            fn visit_f64<E: de::Error>(self, value: f64) -> std::result::Result<Self::Value, E> {
                serde_json::Number::from_f64(value)
                    .map(|n| StrictValue(Value::Number(n)))
                    .ok_or_else(|| E::custom("nonfinite JSON number"))
            }
            fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::String(value.to_owned())))
            }
            fn visit_string<E: de::Error>(
                self,
                value: String,
            ) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::String(value)))
            }
            fn visit_unit<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(StrictValue(value)) = seq.next_element()? {
                    values.push(value);
                }
                Ok(StrictValue(Value::Array(values)))
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = serde_json::Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(de::Error::custom(format!("duplicate JSON key {key}")));
                    }
                    let StrictValue(value) = map.next_value()?;
                    values.insert(key, value);
                }
                Ok(StrictValue(Value::Object(values)))
            }
        }
        deserializer.deserialize_any(StrictVisitor)
    }
}

/// Parse admitted JSON bytes without silently discarding duplicate object keys.
/// Callers bound input size before parsing; storage decoding additionally requires
/// byte verification and canonical stored encodings through the typed decoders.
pub(crate) fn strict_json(bytes: &[u8]) -> Result<Value> {
    let StrictValue(value) = serde_json::from_slice(bytes).context("invalid object JSON")?;
    Ok(value)
}

fn parse<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T> {
    let value = strict_json(bytes)?;
    ensure!(
        serde_json::to_vec(&canonical_json(&value))? == bytes,
        "object JSON is not compact canonical UTF-8"
    );
    serde_json::from_value(value).context("invalid stored fields")
}

/// Encodes validated metadata within its schema's byte budget, without I/O.
pub(crate) fn encode<T: StorageDocument>(document: &T, limits: &FormatLimits) -> Result<Vec<u8>> {
    limits.validate()?;
    // Bound encoding before local validation can clone arbitrary source values.
    let bytes = compact(document, T::SCHEMA.byte_limit(limits))?;
    document.validate(limits)?;
    Ok(bytes)
}

/// Parses verified metadata with strict JSON, canonical bytes, and field types.
/// Semantic validation is explicit: call StorageDocument::validate before use.
pub(crate) fn decode<T: StorageDocument>(object: &VerifiedObject<'_>) -> Result<T> {
    ensure!(
        object.reference().schema == T::SCHEMA,
        "reference schema does not match requested document"
    );
    parse(object.bytes())
}

/// Encodes an already bounded slice; ingestion uses CaseBatchBuilder for streaming splits.
pub(crate) fn encode_batch(
    header: &CaseBatchHeader,
    records: &[CaseRecord],
    limits: &FormatLimits,
) -> Result<Vec<u8>> {
    let mut builder = CaseBatchBuilder::new(header.dataset_id, header.ordinal_start, limits)?;
    ensure!(
        header.schema == StorageFormat::CaseBatch,
        "invalid batch schema"
    );
    for record in records {
        ensure!(
            builder.try_push(record)?,
            "batch exceeds configured case or byte limit"
        );
    }
    ensure!(
        builder.end == header.ordinal_end,
        "batch header/count mismatch"
    );
    builder.finish()
}

/// Bounded encoded staging for one case batch. A full batch returns false without
/// consuming the candidate; callers flush it and retry the same record next.
#[derive(Debug)]
pub(crate) struct CaseBatchBuilder {
    dataset_id: Uuid,
    start: i32,
    end: i32,
    limits: FormatLimits,
    records: Vec<u8>,
    ids: BTreeSet<Uuid>,
}

impl CaseBatchBuilder {
    pub(crate) fn new(dataset_id: Uuid, ordinal_start: i32, limits: &FormatLimits) -> Result<Self> {
        limits.validate()?;
        ensure!(
            (0..i32::MAX).contains(&ordinal_start),
            "invalid batch start"
        );
        Ok(Self {
            dataset_id,
            start: ordinal_start,
            end: ordinal_start,
            limits: limits.clone(),
            records: Vec::new(),
            ids: BTreeSet::new(),
        })
    }

    fn header(&self, end: i32) -> CaseBatchHeader {
        CaseBatchHeader {
            schema: StorageFormat::CaseBatch,
            dataset_id: self.dataset_id,
            ordinal_start: self.start,
            ordinal_end: end,
        }
    }

    /// Accounts for the updated header and both LF terminators before accepting.
    /// Oversized single records are errors; they never create an empty object.
    pub(crate) fn try_push(&mut self, record: &CaseRecord) -> Result<bool> {
        ensure!(
            record.case_ordinal == self.end,
            "batch ordinal gap or overlap"
        );
        let end = self
            .end
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("batch end exceeds i32"))?;
        let encoded = compact(record, self.limits.batch.max_bytes)?;
        let case = record.validate()?;
        ensure!(!self.ids.contains(&case.id), "duplicate case ID in batch");
        let header = compact(&self.header(end), self.limits.batch.max_bytes)?;
        let new_bytes = header
            .len()
            .checked_add(1)
            .and_then(|n| n.checked_add(self.records.len()))
            .and_then(|n| n.checked_add(encoded.len()))
            .and_then(|n| n.checked_add(1))
            .ok_or_else(|| anyhow::anyhow!("batch byte count overflow"))?;
        if self.ids.len() == self.limits.batch.max_cases || new_bytes > self.limits.batch.max_bytes
        {
            ensure!(
                !self.ids.is_empty(),
                "single case plus header exceeds batch byte limit"
            );
            return Ok(false);
        }
        self.records.extend_from_slice(&encoded);
        self.records.push(b'\n');
        self.ids.insert(case.id);
        self.end = end;
        Ok(true)
    }

    pub(crate) fn finish(self) -> Result<Vec<u8>> {
        ensure!(!self.ids.is_empty(), "empty case batch");
        let mut bytes = compact(&self.header(self.end), self.limits.batch.max_bytes)?;
        bytes.push(b'\n');
        bytes.extend_from_slice(&self.records);
        ensure!(
            bytes.len() <= self.limits.batch.max_bytes,
            "batch exceeds byte limit"
        );
        Ok(bytes)
    }
}

/// A verified byte slice decoded one case at a time. Dropping it early does not
/// validate the suffix; full-file validation requires exhaustion or
/// validate_remaining(), as used when validating ingestion or test inputs.
pub(crate) struct CaseBatchReader<'a> {
    header: CaseBatchHeader,
    lines: std::slice::Split<'a, u8, fn(&u8) -> bool>,
    ordinal: i32,
    ids: BTreeSet<Uuid>,
    terminal: bool,
    failed: bool,
}

/// A decoded batch header and verified bytes awaiting semantic validation.
/// Records stay encoded until the validated reader consumes them, so decoding
/// cannot materialize an entire dataset or silently authorize a valid prefix.
pub(crate) struct DecodedCaseBatch<'a> {
    header: CaseBatchHeader,
    bytes: &'a [u8],
}

/// Parses the JSONL framing and header of an integrity-verified case batch.
/// Performs no storage I/O; case records stay encoded until the reader uses them.
/// Call validate() to check header semantics and obtain a streaming case reader.
pub(crate) fn decode_case_batch<'a>(object: &VerifiedObject<'a>) -> Result<DecodedCaseBatch<'a>> {
    ensure!(
        object.reference().schema == StorageFormat::CaseBatch,
        "reference is not a case batch"
    );
    let bytes = object.bytes();
    ensure!(bytes.last() == Some(&b'\n'), "batch must end with LF");
    let header_end = bytes
        .iter()
        .position(|byte| *byte == b'\n')
        .ok_or_else(|| anyhow::anyhow!("missing batch header"))?;
    let header = parse(&bytes[..header_end])?;
    Ok(DecodedCaseBatch { header, bytes })
}

impl<'a> DecodedCaseBatch<'a> {
    /// Validates header policy before yielding a reader that parses and validates
    /// one case at a time. Full-file validation requires successful exhaustion or
    /// validate_remaining() before initial publication.
    pub(crate) fn validate(self, limits: &FormatLimits) -> Result<CaseBatchReader<'a>> {
        limits.validate()?;
        ensure!(
            self.bytes.len() <= limits.batch.max_bytes,
            "batch exceeds byte limit"
        );
        ensure!(
            self.header.schema == StorageFormat::CaseBatch,
            "invalid batch schema"
        );
        range(self.header.ordinal_start, self.header.ordinal_end)?;
        ensure!(
            usize::try_from(self.header.ordinal_end - self.header.ordinal_start)?
                <= limits.batch.max_cases,
            "batch case count exceeds limit"
        );
        let mut lines =
            self.bytes[..self.bytes.len() - 1].split((|b: &u8| *b == b'\n') as fn(&u8) -> bool);
        lines.next();
        Ok(CaseBatchReader {
            ordinal: self.header.ordinal_start,
            header: self.header,
            lines,
            ids: BTreeSet::new(),
            terminal: false,
            failed: false,
        })
    }
}

impl Iterator for CaseBatchReader<'_> {
    type Item = Result<CaseRecord>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.terminal {
            return None;
        }
        let result = (|| {
            let Some(line) = self.lines.next() else {
                self.terminal = true;
                ensure!(
                    self.ordinal == self.header.ordinal_end,
                    "truncated batch record count"
                );
                return Ok(None);
            };
            ensure!(
                self.ordinal < self.header.ordinal_end,
                "too many batch records"
            );
            let record: CaseRecord = parse(line)?;
            ensure!(
                record.case_ordinal == self.ordinal,
                "batch ordinal gap or overlap"
            );
            let case = record.validate()?;
            ensure!(self.ids.insert(case.id), "duplicate case ID in batch");
            self.ordinal += 1;
            Ok(Some(record))
        })();
        match result {
            Ok(record) => record.map(Ok),
            Err(error) => {
                self.terminal = true;
                self.failed = true;
                Some(Err(error))
            }
        }
    }
}

impl CaseBatchReader<'_> {
    /// Returns the immutable header used to validate record membership.
    pub(crate) fn header(&self) -> &CaseBatchHeader {
        &self.header
    }

    /// Consumes remaining records and checks their content and final record count.
    /// Use this to complete full-file validation after a partial read. Existing
    /// validation errors remain failures. This does not execute cases or require
    /// workers to revalidate unassigned records in published immutable files.
    pub(crate) fn validate_remaining(self) -> Result<()> {
        ensure!(!self.failed, "batch reader already failed");
        for record in self {
            record?;
        }
        Ok(())
    }
}
