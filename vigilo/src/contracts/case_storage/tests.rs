//! Service-free contract tests use frozen identities and downloadable case-storage bytes.
//! Negative tests re-sign malformed bytes so integrity cannot mask decoder defects.

use serde_json::{
    Value,
    json,
};
use uuid::Uuid;

use super::{
    identity::{
        DatasetIdentityBuilder,
        canonical_json,
        case_hash,
    },
    *,
};

const BATCH: &[u8] = include_bytes!("../../../../web/static/examples/case-storage/cases.jsonl");
const SOURCE: &[u8] =
    include_bytes!("../../../../web/static/examples/case-storage/source-fields.json");
const INDEX: &[u8] =
    include_bytes!("../../../../web/static/examples/case-storage/batch-index.json");
const MANIFEST: &[u8] =
    include_bytes!("../../../../web/static/examples/case-storage/dataset-manifest.json");
const REFERENCE: &[u8] =
    include_bytes!("../../../../web/static/examples/case-storage/dataset-reference.json");

fn reference(schema: StorageFormat, bytes: &[u8]) -> ObjectRef {
    ObjectRef::for_bytes(
        "development".into(),
        "test/object".into(),
        schema,
        bytes,
        &FormatLimits::default(),
    )
    .unwrap()
}

/// Existing acceptance tests exercise the complete explicit read pipeline.
fn read_document<T: StorageDocument>(
    reference: &ObjectRef,
    bytes: &[u8],
    limits: &FormatLimits,
) -> anyhow::Result<T> {
    let verified = reference.verify(bytes, limits)?;
    let document: T = decode(&verified)?;
    document.validate(limits)?;
    Ok(document)
}

fn read_batch<'a>(
    reference: &ObjectRef,
    bytes: &'a [u8],
    limits: &FormatLimits,
) -> anyhow::Result<codec::CaseBatchReader<'a>> {
    let verified = reference.verify(bytes, limits)?;
    decode_case_batch(&verified)?.validate(limits)
}

fn records() -> Vec<CaseRecord> {
    read_batch(
        &reference(StorageFormat::CaseBatch, BATCH),
        BATCH,
        &FormatLimits::default(),
    )
    .unwrap()
    .collect::<anyhow::Result<_>>()
    .unwrap()
}

fn source() -> DatasetSourceFields {
    read_document(
        &reference(StorageFormat::DatasetSourceFields, SOURCE),
        SOURCE,
        &FormatLimits::default(),
    )
    .unwrap()
}
fn index() -> IndexPage {
    read_document(
        &reference(StorageFormat::OrdinalIndex, INDEX),
        INDEX,
        &FormatLimits::default(),
    )
    .unwrap()
}
fn manifest() -> DatasetManifest {
    read_document(
        &reference(StorageFormat::DatasetManifest, MANIFEST),
        MANIFEST,
        &FormatLimits::default(),
    )
    .unwrap()
}
fn bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(&canonical_json(value)).unwrap()
}

fn rejects<T: StorageDocument>(value: Value) {
    let bytes = bytes(&value);
    assert!(
        read_document::<T>(
            &reference(T::SCHEMA, &bytes),
            &bytes,
            &FormatLimits::default()
        )
        .is_err(),
        "accepted {value}"
    );
}

#[test]
fn downloadable_dataset_objects_and_identities_are_exact() {
    let limits = FormatLimits::default();
    let root: ObjectRef = serde_json::from_slice(REFERENCE).unwrap();
    let manifest: DatasetManifest = read_document(&root, MANIFEST, &limits).unwrap();
    let fields: DatasetSourceFields =
        read_document(&manifest.source_fields, SOURCE, &limits).unwrap();
    let page: IndexPage = read_document(&manifest.batches.object, INDEX, &limits).unwrap();
    manifest
        .batches
        .validate_page(&page, IndexKind::DatasetBatches, &limits)
        .unwrap();
    let reader = read_batch(&page.entries[0].object, BATCH, &limits).unwrap();
    let header = reader.header().clone();
    assert_eq!(header.dataset_id, manifest.dataset_id);
    let mut identity = DatasetIdentityBuilder::new(&fields, &limits).unwrap();
    let mut cases = Vec::new();
    for record in reader {
        let record = record.unwrap();
        identity.push(&record).unwrap();
        cases.push(record);
    }
    let (hash, version, count) = identity.finish().unwrap();
    assert_eq!(hash, manifest.dataset_hash);
    assert_eq!(version, manifest.dataset_version_id);
    assert_eq!(count, manifest.case_count);
    assert_eq!(encode(&manifest, &limits).unwrap(), MANIFEST);
    assert_eq!(encode(&fields, &limits).unwrap(), SOURCE);
    assert_eq!(encode(&page, &limits).unwrap(), INDEX);
    assert_eq!(encode_batch(&header, &cases, &limits).unwrap(), BATCH);
    assert!(!cases[1].source.as_object().unwrap().contains_key("tags"));
    assert_eq!(cases[0].source["tags"], json!(["zeta", "alpha", "alpha"]));
    assert_eq!(cases[1].case_hash, cases[2].case_hash);
}

#[test]
fn metadata_rejects_unknown_missing_duplicate_and_noncanonical_fields() {
    let original: Value = serde_json::from_slice(SOURCE).unwrap();
    for field in ["schema", "cases_present", "fields"] {
        let mut value = original.clone();
        value.as_object_mut().unwrap().remove(field);
        rejects::<DatasetSourceFields>(value);
    }
    let mut value = original.clone();
    value["unexpected"] = json!(true);
    rejects::<DatasetSourceFields>(value);
    let mut value = original.clone();
    value["schema"] = json!("vigilo/dataset-source-fields/v99");
    rejects::<DatasetSourceFields>(value);
    let mut value = original.clone();
    value["schema"] = json!("vigilo/case-batch/v1");
    rejects::<DatasetSourceFields>(value);
    let mut value = original.clone();
    value["fields"]["cases"] = json!([]);
    rejects::<DatasetSourceFields>(value);
    let mut value = original.clone();
    value["fields"]["dataset_id"] = json!("invalid");
    rejects::<DatasetSourceFields>(value);
    for invalid in [
        br#"{"cases_present":true,"fields":{"dataset_id":"x","dataset_id":"y"},"schema":"vigilo/dataset-source-fields/v1"}"#.as_slice(),
        br#"{"cases_present":true,"cases_present":false,"fields":{},"schema":"vigilo/dataset-source-fields/v1"}"#,
        b"\xef\xbb\xbf{}", b"{\"x\":1}\n", b"{ \"x\":1}", b"{\"x\":\xff}", b"{}{}",
    ] {
        assert!(read_document::<DatasetSourceFields>(&reference(StorageFormat::DatasetSourceFields, invalid), invalid, &FormatLimits::default()).is_err());
    }
}

#[test]
fn references_require_explicit_nulls_and_fail_closed_on_location_and_identity() {
    let original = reference(StorageFormat::CaseBatch, BATCH);
    for key in [
        "",
        "/absolute",
        "https://host/object",
        "a/../b",
        "a/./b",
        "a//b",
        "a?secret",
        "a#fragment",
        "a\\b",
        "a\nb",
    ] {
        let mut value = original.clone();
        value.key = key.into();
        assert!(value.validate(&FormatLimits::default()).is_err(), "{key}");
    }
    let mut value = original.clone();
    value.key = "é".repeat(512);
    value.validate(&FormatLimits::default()).unwrap();
    value.key.push('a');
    assert!(value.validate(&FormatLimits::default()).is_err());
    for alias in ["", "https://host", "a b", "a\nb"] {
        let mut value = original.clone();
        value.storage_alias = alias.into();
        assert!(value.validate(&FormatLimits::default()).is_err());
    }
    for digest in ["x".repeat(64), "A".repeat(64), "0".repeat(63)] {
        let mut value = original.clone();
        value.digest = digest;
        assert!(value.verify(BATCH, &FormatLimits::default()).is_err());
    }
    let mut value = original.clone();
    value.byte_count += 1;
    assert!(value.verify(BATCH, &FormatLimits::default()).is_err());
    let mut value = original.clone();
    value.digest = "0".repeat(64);
    assert!(value.verify(BATCH, &FormatLimits::default()).is_err());
    let mut value = original.clone();
    value.revision = Some(String::new());
    assert!(value.validate(&FormatLimits::default()).is_err());
    let mut value = serde_json::to_value(&original).unwrap();
    value.as_object_mut().unwrap().remove("revision");
    assert!(serde_json::from_value::<ObjectRef>(value).is_err());
    let mut value = serde_json::to_value(&original).unwrap();
    value["hash_scheme"] = json!("etag");
    assert!(serde_json::from_value::<ObjectRef>(value).is_err());
    assert!(
        read_document::<DatasetSourceFields>(&original, BATCH, &FormatLimits::default()).is_err()
    );
}

#[test]
fn batch_limits_count_header_lf_and_utf8_and_split_without_consuming() {
    let records = records();
    let dataset_id = manifest().dataset_id;
    let mut limits = FormatLimits::default();
    limits.batch.max_cases = 1;
    let mut builder = CaseBatchBuilder::new(dataset_id, 0, &limits).unwrap();
    assert!(builder.try_push(&records[0]).unwrap());
    assert!(!builder.try_push(&records[1]).unwrap());
    let first = builder.finish().unwrap();
    let mut builder = CaseBatchBuilder::new(dataset_id, 1, &limits).unwrap();
    assert!(builder.try_push(&records[1]).unwrap());
    let second = builder.finish().unwrap();
    assert!(first.ends_with(b"\n") && second.ends_with(b"\n"));
    let mut limits = FormatLimits::default();
    limits.batch.max_bytes = first.len();
    let mut builder = CaseBatchBuilder::new(dataset_id, 0, &limits).unwrap();
    assert!(builder.try_push(&records[0]).unwrap());
    assert!(!builder.try_push(&records[1]).unwrap());
    assert_eq!(builder.finish().unwrap(), first);
    limits.batch.max_bytes -= 1;
    let mut builder = CaseBatchBuilder::new(dataset_id, 0, &limits).unwrap();
    assert!(builder.try_push(&records[0]).is_err());
    assert!(builder.finish().is_err());
    assert!(CaseBatchBuilder::new(dataset_id, i32::MAX, &limits).is_err());
}

fn batch_rejects(bytes: &[u8]) {
    let result = read_batch(
        &reference(StorageFormat::CaseBatch, bytes),
        bytes,
        &FormatLimits::default(),
    )
    .and_then(|reader| reader.validate_remaining());
    assert!(result.is_err(), "accepted invalid batch");
}

#[test]
fn batch_rejects_truncation_extra_lines_duplicates_corruption_and_bad_ranges() {
    batch_rejects(&BATCH[..BATCH.len() - 1]);
    batch_rejects(&[BATCH, b"\n"].concat());
    batch_rejects(&[BATCH, b"{}\n"].concat());
    batch_rejects(
        &BATCH
            .iter()
            .copied()
            .flat_map(|b| {
                if b == b'\n' {
                    vec![b'\r', b'\n']
                } else {
                    vec![b]
                }
            })
            .collect::<Vec<_>>(),
    );
    let lines: Vec<_> = BATCH
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .collect();
    let truncated = [lines[0], b"\n", lines[1], b"\n"].concat();
    batch_rejects(&truncated);
    for (line_index, field, value) in [
        (0, "ordinal_start", json!(-1)),
        (0, "ordinal_end", json!(0)),
        (0, "ordinal_end", json!(2147483648_u64)),
        (0, "schema", json!("unknown")),
        (1, "case_ordinal", json!(1)),
        (2, "case_ordinal", json!(0)),
        (1, "case_hash", json!("0".repeat(64))),
        (1, "extra", json!(true)),
    ] {
        let mut changed: Vec<Vec<u8>> = lines.iter().map(|line| line.to_vec()).collect();
        let mut value_line: Value = serde_json::from_slice(lines[line_index]).unwrap();
        value_line[field] = value;
        changed[line_index] = bytes(&value_line);
        let mut encoded = changed.join(&b'\n');
        encoded.push(b'\n');
        batch_rejects(&encoded);
    }
    let mut duplicate = records();
    duplicate[2].source["id"] = duplicate[1].source["id"].clone();
    let header = CaseBatchHeader {
        schema: StorageFormat::CaseBatch,
        dataset_id: manifest().dataset_id,
        ordinal_start: 0,
        ordinal_end: 3,
    };
    assert!(encode_batch(&header, &duplicate, &FormatLimits::default()).is_err());
    let mut encoded = bytes(&serde_json::to_value(header).unwrap());
    encoded.push(b'\n');
    for record in &duplicate {
        encoded.extend(bytes(&serde_json::to_value(record).unwrap()));
        encoded.push(b'\n');
    }
    batch_rejects(&encoded);
    let mutated = String::from_utf8(BATCH.to_vec()).unwrap().replace(
        "\"prompt\":\"line\\nbreak\"",
        "\"prompt\":\"line\\nbreak\",\"prompt\":\"other\"",
    );
    batch_rejects(mutated.as_bytes());
}

#[test]
fn batch_writer_failure_is_atomic_and_header_width_is_accounted() {
    let mut record = records()[0].clone();
    record.case_ordinal = 9;
    let mut builder =
        CaseBatchBuilder::new(manifest().dataset_id, 9, &FormatLimits::default()).unwrap();
    let mut wrong = record.clone();
    wrong.case_hash = "0".repeat(64);
    assert!(builder.try_push(&wrong).is_err());
    assert!(builder.try_push(&record).unwrap());
    let encoded = builder.finish().unwrap();
    let reader = read_batch(
        &reference(StorageFormat::CaseBatch, &encoded),
        &encoded,
        &FormatLimits::default(),
    )
    .unwrap();
    assert_eq!(reader.header().ordinal_end, 10);
    reader.validate_remaining().unwrap();
}

#[test]
fn metadata_admits_exact_bytes_and_rejects_one_byte_over_and_entry_overflow() {
    let mut page = index();
    let encoded = encode(&page, &FormatLimits::default()).unwrap();
    let mut limits = FormatLimits {
        page_bytes: encoded.len(),
        ..FormatLimits::default()
    };
    assert_eq!(encode(&page, &limits).unwrap(), encoded);
    limits.page_bytes -= 1;
    assert!(encode(&page, &limits).is_err());
    assert!(
        read_document::<IndexPage>(
            &reference(StorageFormat::OrdinalIndex, &encoded),
            &encoded,
            &limits
        )
        .is_err()
    );
    page.entries.push(page.entries[0].clone());
    assert!(
        encode(
            &page,
            &FormatLimits {
                page_entries: 1,
                ..FormatLimits::default()
            }
        )
        .is_err()
    );
}

#[test]
fn finite_limits_and_memory_spool_relationships_fail_before_work() {
    FormatLimits::default().validate().unwrap();
    let original = serde_json::to_value(FormatLimits::default()).unwrap();
    for (pointer, invalid) in [
        ("/batch/max_cases", json!(0)),
        ("/batch/max_cases", json!(65537)),
        ("/batch/max_bytes", json!(0)),
        ("/batch/max_bytes", json!(67108865)),
        ("/page_bytes", json!(0)),
        ("/page_bytes", json!(8388609)),
        ("/page_entries", json!(0)),
        ("/page_entries", json!(4097)),
        ("/root_bytes", json!(0)),
        ("/root_bytes", json!(1048577)),
        ("/source_bytes", json!(0)),
        ("/source_bytes", json!(67108865)),
        ("/index_height", json!(0)),
        ("/index_height", json!(9)),
        ("/resident_batches", json!(0)),
        ("/resident_batches", json!(17)),
        ("/resident_batch_bytes", json!(0)),
        ("/resident_batch_bytes", json!(1073741825_u64)),
        ("/staging_spool_bytes", json!(0)),
        ("/staging_spool_bytes", json!(4294967297_u64)),
    ] {
        let mut value = original.clone();
        *value.pointer_mut(pointer).unwrap() = invalid;
        assert!(
            serde_json::from_value::<FormatLimits>(value)
                .map(|limits| limits.validate().is_err())
                .unwrap_or(true),
            "{pointer}"
        );
    }
    let mut limits = FormatLimits::default();
    limits.batch.max_bytes = 64 * 1_048_576;
    assert!(limits.validate().is_err());
    limits.resident_batch_bytes = 256 * 1_048_576;
    limits.validate().unwrap();
}

#[test]
fn index_edges_reject_ranges_depth_schema_kind_and_level_disagreement() {
    let page = index();
    for field in ["range_start", "range_end"] {
        let mut value = serde_json::to_value(&page).unwrap();
        value[field] = json!(-1);
        rejects::<IndexPage>(value);
    }
    let mut invalid = page.clone();
    invalid.entries.clear();
    assert!(invalid.validate(&FormatLimits::default()).is_err());
    let mut invalid = page.clone();
    invalid.entries[0].range_start = 1;
    assert!(invalid.validate(&FormatLimits::default()).is_err());
    let mut invalid = page.clone();
    invalid.entries[0].range_end = 4;
    assert!(invalid.validate(&FormatLimits::default()).is_err());
    let mut invalid = page.clone();
    invalid.level = 8;
    assert!(invalid.validate(&FormatLimits::default()).is_err());
    let mut invalid = page.clone();
    invalid.kind = IndexKind::RunChunks;
    assert!(invalid.validate(&FormatLimits::default()).is_err());
    let descriptor = manifest().batches;
    assert!(
        descriptor
            .validate_page(&page, IndexKind::RunChunks, &FormatLimits::default())
            .is_err()
    );
    let mut child = page.clone();
    child.entries[0].object.storage_alias = "other".into();
    assert!(
        descriptor
            .validate_page(&child, IndexKind::DatasetBatches, &FormatLimits::default())
            .is_err()
    );
    let mut parent = page.clone();
    parent.level = 1;
    parent.entries[0].object = descriptor.object;
    parent
        .validate_child(0, &page, &FormatLimits::default())
        .unwrap();
    assert!(
        parent
            .validate_child(1, &page, &FormatLimits::default())
            .is_err()
    );
    assert!(
        page.validate_child(0, &page, &FormatLimits::default())
            .is_err()
    );
    let mut child = page.clone();
    child.level = 1;
    child.entries[0].object = parent.entries[0].object.clone();
    assert!(
        parent
            .validate_child(0, &child, &FormatLimits::default())
            .is_err()
    );
}

#[test]
fn dataset_manifest_records_admitted_policy_and_rejects_invalid_membership_roots() {
    let original = serde_json::to_value(manifest()).unwrap();
    for (pointer, invalid) in [
        ("/case_count", json!(0)),
        ("/case_count", json!(2147483648_u64)),
        ("/batches/range_start", json!(1)),
        ("/batches/range_end", json!(4)),
        ("/batch_limits/max_cases", json!(0)),
        ("/batch_limits/max_cases", json!(101)),
        ("/identity_scheme", json!("other")),
        ("/dataset_hash", json!("0")),
        ("/source_fields/storage_alias", json!("other")),
    ] {
        let mut value = original.clone();
        *value.pointer_mut(pointer).unwrap() = invalid;
        rejects::<DatasetManifest>(value);
    }
    for field in ["batch_limits", "dataset_version"] {
        let mut value = original.clone();
        value.as_object_mut().unwrap().remove(field);
        rejects::<DatasetManifest>(value);
    }
    let mut defaults = FormatLimits::default();
    defaults.batch.max_cases = 200;
    assert_eq!(
        read_document::<DatasetManifest>(
            &reference(StorageFormat::DatasetManifest, MANIFEST),
            MANIFEST,
            &defaults
        )
        .unwrap()
        .batch_limits
        .max_cases,
        100
    );
}

#[test]
fn incremental_identity_preserves_unknown_fields_nulls_and_sorted_top_level_position() {
    let original_records = records();
    for fields in [
        json!({"dataset_id": manifest().dataset_id, "dataset_version": null}),
        json!({"aaa": {"z": true, "a": null}, "dataset_id": manifest().dataset_id, "zzz": [3, 1]}),
        json!({"cases-before": null, "dataset_id": manifest().dataset_id}),
    ] {
        let source = DatasetSourceFields {
            schema: StorageFormat::DatasetSourceFields,
            cases_present: true,
            fields: fields.clone(),
        };
        let mut full = fields;
        full["cases"] = Value::Array(
            original_records
                .iter()
                .map(|record| record.source.clone())
                .collect(),
        );
        let mut identity = DatasetIdentityBuilder::new(&source, &FormatLimits::default()).unwrap();
        let mut wrong = original_records[0].clone();
        wrong.case_ordinal = 1;
        assert!(identity.push(&wrong).is_err());
        for record in &original_records {
            identity.push(record).unwrap();
        }
        assert_eq!(
            identity.finish().unwrap().0,
            blake3::hash(&bytes(&full)).to_hex().to_string()
        );
    }
    let mut source = source();
    source.cases_present = false;
    assert!(DatasetIdentityBuilder::new(&source, &FormatLimits::default()).is_err());
    assert!(
        DatasetIdentityBuilder::new(&self::source(), &FormatLimits::default())
            .unwrap()
            .finish()
            .is_err()
    );
    let mut invalid = original_records[0].clone();
    invalid.source["task_type"] = json!("");
    let case = serde_json::from_value(invalid.source.clone()).unwrap();
    invalid.case_hash = case_hash(&case).unwrap();
    assert!(invalid.validate().is_err());
}

fn gate_count_page(ids: &[&str], next: Option<ObjectRef>) -> GateCountPage {
    GateCountPage {
        schema: StorageFormat::GateCounts,
        run_shard: 0,
        entries: ids
            .iter()
            .map(|id| GateCount {
                gate_id: (*id).into(),
                planned_count: 2,
            })
            .collect(),
        next,
    }
}

#[test]
fn gate_count_chains_enforce_order_counts_denominators_termination_and_owner() {
    let limits = FormatLimits::default();
    let tail = gate_count_page(&["b", "c"], None);
    let tail_bytes = encode(&tail, &limits).unwrap();
    let tail_ref = reference(StorageFormat::GateCounts, &tail_bytes);
    let head = gate_count_page(&["a"], Some(tail_ref.clone()));
    let head_bytes = encode(&head, &limits).unwrap();
    let head_ref = reference(StorageFormat::GateCounts, &head_bytes);
    let descriptor = GateCountDescriptor {
        first: Some(head_ref.clone()),
        page_count: 2,
        entry_count: 3,
    };
    let mut chain = GateCountChainValidator::new(&descriptor, 0, 3, &limits).unwrap();
    assert_eq!(chain.next(), Some(&head_ref));
    let mut wrong = head.clone();
    wrong.run_shard = 1;
    assert!(chain.accept(&head_ref, &wrong, &limits).is_err());
    let mut wrong = head.clone();
    wrong.entries[0].planned_count = 4;
    assert!(chain.accept(&head_ref, &wrong, &limits).is_err());
    let mut wrong = head.clone();
    wrong.next = Some(head_ref.clone());
    assert!(chain.accept(&head_ref, &wrong, &limits).is_err());
    let mut wrong = head.clone();
    wrong.next.as_mut().unwrap().storage_alias = "other".into();
    assert!(chain.accept(&head_ref, &wrong, &limits).is_err());
    chain.accept(&head_ref, &head, &limits).unwrap();
    let wrong = gate_count_page(&["a", "b"], None);
    assert!(chain.accept(&tail_ref, &wrong, &limits).is_err());
    chain.accept(&tail_ref, &tail, &limits).unwrap();
    chain.finish().unwrap();
    let mut chain = GateCountChainValidator::new(&descriptor, 0, 3, &limits).unwrap();
    let mut wrong = head.clone();
    wrong.next = None;
    assert!(chain.accept(&head_ref, &wrong, &limits).is_err());
    assert!(chain.finish().is_err());
    let empty = GateCountDescriptor {
        first: None,
        page_count: 0,
        entry_count: 0,
    };
    GateCountChainValidator::new(&empty, 0, 1, &limits)
        .unwrap()
        .finish()
        .unwrap();
    let invalid = GateCountDescriptor {
        first: None,
        page_count: 1,
        entry_count: 1,
    };
    assert!(GateCountChainValidator::new(&invalid, 0, 1, &limits).is_err());
    let invalid = GateCountDescriptor {
        first: Some(head_ref),
        page_count: 2,
        entry_count: 1,
    };
    assert!(GateCountChainValidator::new(&invalid, 0, 1, &limits).is_err());
    for page in [
        gate_count_page(&[], None),
        gate_count_page(&["b", "a"], None),
        gate_count_page(&["a", "a"], None),
    ] {
        assert!(encode(&page, &limits).is_err());
    }
    let mut page = gate_count_page(&["a"], None);
    page.entries[0].planned_count = -1;
    assert!(encode(&page, &limits).is_err());
}

fn chunk_page() -> RunChunkPage {
    RunChunkPage {
        schema: StorageFormat::RunChunks,
        run_id: Uuid::from_u128(1),
        chunk_index_start: 0,
        chunk_index_end: 2,
        chunks: (0..2)
            .map(|index| RunChunk {
                chunk_index: index,
                chunk_id: Uuid::from_u128(index as u128 + 10),
                run_shard: index as i16,
                ordinal_start: index * 100,
                ordinal_end: (index + 1) * 100,
                profile_group_id: "default".into(),
                case_projection_hash: "0".repeat(64),
            })
            .collect(),
    }
}

#[test]
fn run_chunk_pages_validate_indexes_case_ranges_routes_and_identities() {
    let page = chunk_page();
    let encoded = encode(&page, &FormatLimits::default()).unwrap();
    assert_eq!(
        read_document::<RunChunkPage>(
            &reference(StorageFormat::RunChunks, &encoded),
            &encoded,
            &FormatLimits::default()
        )
        .unwrap(),
        page
    );
    let original = serde_json::to_value(page).unwrap();
    for (pointer, invalid) in [
        ("/chunk_index_end", json!(3)),
        ("/chunks/1/chunk_index", json!(0)),
        (
            "/chunks/1/chunk_id",
            original["chunks"][0]["chunk_id"].clone(),
        ),
        ("/chunks/1/run_shard", json!(128)),
        ("/chunks/1/run_shard", json!(-1)),
        ("/chunks/0/ordinal_end", json!(101)),
        ("/chunks/1/ordinal_start", json!(99)),
        ("/chunks/1/profile_group_id", json!("")),
        ("/chunks/0/case_projection_hash", json!("invalid")),
    ] {
        let mut value = original.clone();
        *value.pointer_mut(pointer).unwrap() = invalid;
        rejects::<RunChunkPage>(value);
    }
}

fn plan() -> RunPlanManifest {
    RunPlanManifest {
        schema: StorageFormat::RunPlanManifest,
        run_id: Uuid::from_u128(1),
        dataset: reference(StorageFormat::DatasetManifest, MANIFEST),
        profile_hash: "0".repeat(64),
        aggregation_policy_hash: "0".repeat(64),
        execution_plan_hash: "0".repeat(64),
        configuration: reference(StorageFormat::RunConfiguration, b"{}"),
        chunk_count: 2,
        chunks: IndexDescriptor {
            level: 0,
            range_start: 0,
            range_end: 2,
            object: reference(StorageFormat::OrdinalIndex, INDEX),
        },
        shards: (0..2)
            .map(|run_shard| RunPlanShard {
                run_shard,
                case_count: 100,
                chunk_count: 1,
                planned_chunk_hash: "0".repeat(64),
                gate_counts: GateCountDescriptor {
                    first: None,
                    page_count: 0,
                    entry_count: 0,
                },
            })
            .collect(),
    }
}

#[test]
fn run_roots_validate_shard_totals_and_checked_integer_bounds() {
    let plan = plan();
    let limits = FormatLimits::default();
    let encoded = encode(&plan, &limits).unwrap();
    assert_eq!(
        read_document::<RunPlanManifest>(
            &reference(StorageFormat::RunPlanManifest, &encoded),
            &encoded,
            &limits
        )
        .unwrap(),
        plan
    );
    let original = serde_json::to_value(plan).unwrap();
    for (pointer, invalid) in [
        ("/shards/1/run_shard", json!(0)),
        ("/shards/1/run_shard", json!(128)),
        ("/shards/0/case_count", json!(0)),
        ("/shards/0/case_count", json!(101)),
        ("/shards/0/chunk_count", json!(2)),
        ("/chunk_count", json!(3)),
        ("/shards", json!([])),
        ("/configuration/storage_alias", json!("other")),
        ("/profile_hash", json!("x")),
    ] {
        let mut value = original.clone();
        *value.pointer_mut(pointer).unwrap() = invalid;
        rejects::<RunPlanManifest>(value);
    }
    let mut large = self::plan();
    large.chunk_count = 40_000_000;
    large.chunks.range_end = large.chunk_count;
    for shard in &mut large.shards {
        shard.case_count = 2_000_000_000;
        shard.chunk_count = 20_000_000;
    }
    assert!(encode(&large, &limits).is_err());
}

#[test]
fn run_configuration_is_bounded_and_keeps_cases_outside_snapshot() {
    let profile: Value =
        serde_yaml::from_str(include_str!("../../../../example/profile.yaml")).unwrap();
    let plan = crate::contracts::evaluator_abi::EvaluatorExecutionPlan::new("0".repeat(64), vec![]);
    let configuration = RunConfiguration {
        schema: StorageFormat::RunConfiguration,
        snapshot: json!({
            "profile": profile, "agent": profile["agent"], "dataset_ref": {"dataset_id": manifest().dataset_id, "dataset_version": null, "dataset_version_id": manifest().dataset_version_id, "dataset_hash": manifest().dataset_hash, "case_count": 3}, "dataset_version_id": manifest().dataset_version_id, "profile_version_id": "example/1",
            "profile_hash": identity::hash_serialized_json(&canonical_json(&profile)).unwrap(), "dataset_hash": manifest().dataset_hash, "aggregation_policy_hash": "0".repeat(64), "execution_plan_hash": plan.hash().unwrap(), "execution_plan": plan, "chunk_size": 100, "executability": {"expected_execution_count":3,"expected_evaluator_execution_count":3,"unique_evaluator_ref_count":1,"runnable_evaluator_ref_count":1},
        }),
    };
    let encoded = encode(&configuration, &FormatLimits::default()).unwrap();
    assert_eq!(
        read_document::<RunConfiguration>(
            &reference(StorageFormat::RunConfiguration, &encoded),
            &encoded,
            &FormatLimits::default()
        )
        .unwrap(),
        configuration
    );
    let mut invalid = configuration.clone();
    invalid.snapshot["cases"] = json!([]);
    assert!(encode(&invalid, &FormatLimits::default()).is_err());
    let mut invalid = configuration.clone();
    invalid.snapshot["dataset_ref"]["cases"] = json!([]);
    assert!(encode(&invalid, &FormatLimits::default()).is_err());
    let mut invalid = configuration.clone();
    invalid.snapshot["chunk_size"] = json!(101);
    assert!(encode(&invalid, &FormatLimits::default()).is_err());
    let mut invalid = configuration;
    invalid.snapshot["profile_hash"] = json!(null);
    assert!(encode(&invalid, &FormatLimits::default()).is_err());
}

#[test]
fn verified_leaf_links_bind_ranges_and_owning_dataset_or_run() {
    let limits = FormatLimits::default();
    let page = index();
    let mut header = read_batch(&reference(StorageFormat::CaseBatch, BATCH), BATCH, &limits)
        .unwrap()
        .header()
        .clone();
    let dataset = header.dataset_id;
    page.validate_batch(0, &header, dataset, &limits).unwrap();
    assert!(
        page.validate_batch(0, &header, Uuid::nil(), &limits)
            .is_err()
    );
    header.ordinal_end = 4;
    assert!(page.validate_batch(0, &header, dataset, &limits).is_err());
    assert!(page.validate_batch(1, &header, dataset, &limits).is_err());
    let chunks = chunk_page();
    let encoded = encode(&chunks, &limits).unwrap();
    let mut run_index = page;
    run_index.kind = IndexKind::RunChunks;
    run_index.range_end = 2;
    run_index.entries[0].range_end = 2;
    run_index.entries[0].object = reference(StorageFormat::RunChunks, &encoded);
    run_index
        .validate_chunks(0, &chunks, chunks.run_id, &limits)
        .unwrap();
    assert!(
        run_index
            .validate_chunks(0, &chunks, Uuid::nil(), &limits)
            .is_err()
    );
    run_index.range_end = 3;
    run_index.entries[0].range_end = 3;
    assert!(
        run_index
            .validate_chunks(0, &chunks, chunks.run_id, &limits)
            .is_err()
    );
}

#[test]
fn manifest_identity_verification_rejects_forged_roots_and_preserves_user_fields() {
    let limits = FormatLimits::default();
    let compute = || {
        let mut identity = DatasetIdentityBuilder::new(&source(), &limits).unwrap();
        for record in records() {
            identity.push(&record).unwrap();
        }
        identity
    };
    compute().verify(&manifest(), &limits).unwrap();
    let mut altered = manifest();
    altered.dataset_id = Uuid::nil();
    assert!(compute().verify(&altered, &limits).is_err());
    let mut altered = manifest();
    altered.dataset_version = None;
    assert!(compute().verify(&altered, &limits).is_err());
    let mut altered = manifest();
    altered.dataset_hash = "0".repeat(64);
    assert!(compute().verify(&altered, &limits).is_err());
    let mut altered = manifest();
    altered.dataset_version_id = Uuid::nil();
    assert!(compute().verify(&altered, &limits).is_err());
    let mut record = records()[0].clone();
    record.source["custom"] = json!({"b": null, "a": "preserved"});
    // Unknown user fields affect submitted identity, not normalized case content.
    record.validate().unwrap();
    let header = CaseBatchHeader {
        schema: StorageFormat::CaseBatch,
        dataset_id: manifest().dataset_id,
        ordinal_start: 0,
        ordinal_end: 1,
    };
    let encoded = encode_batch(&header, &[record.clone()], &limits).unwrap();
    let decoded = read_batch(
        &reference(StorageFormat::CaseBatch, &encoded),
        &encoded,
        &limits,
    )
    .unwrap()
    .next()
    .unwrap()
    .unwrap();
    assert_eq!(decoded.source, record.source);
}

#[test]
fn failed_batch_reader_cannot_validate_remaining_after_error_is_observed() {
    let malformed = [BATCH, b"{}\n"].concat();
    let object = reference(StorageFormat::CaseBatch, &malformed);
    let mut reader = read_batch(&object, &malformed, &FormatLimits::default()).unwrap();
    for _ in 0..3 {
        reader.next().unwrap().unwrap();
    }
    assert!(reader.next().unwrap().is_err());
    assert!(reader.next().is_none());
    assert!(reader.validate_remaining().is_err());
}

#[test]
fn strict_json_rejects_excessive_nesting_and_source_wrong_types() {
    let deep = [b"[".repeat(140), b"0".to_vec(), b"]".repeat(140)].concat();
    assert!(
        read_document::<DatasetSourceFields>(
            &reference(StorageFormat::DatasetSourceFields, &deep),
            &deep,
            &FormatLimits::default()
        )
        .is_err()
    );
    for (field, value) in [
        ("id", json!("bad")),
        ("input", json!(null)),
        ("tags", json!([1])),
        ("metadata", json!([])),
    ] {
        let mut record = records()[0].clone();
        if field == "input" {
            record.source.as_object_mut().unwrap().remove(field);
        } else {
            record.source[field] = value;
        }
        assert!(record.validate().is_err(), "{field}");
    }
}

#[test]
fn index_ranges_use_kind_specific_units_and_round_trip_explicit_bounds() {
    let limits = FormatLimits::default();
    for (kind, child_schema) in [
        (IndexKind::DatasetBatches, StorageFormat::CaseBatch),
        (IndexKind::RunChunks, StorageFormat::RunChunks),
    ] {
        let content = json!({
            "schema": "vigilo/ordinal-index/v1",
            "kind": kind,
            "level": 0,
            "range_start": 4,
            "range_end": 7,
            "entries": [{
                "range_start": 4,
                "range_end": 7,
                "object": reference(child_schema, b"{}"),
            }],
        });
        let encoded = bytes(&content);
        let page: IndexPage = read_document(
            &reference(StorageFormat::OrdinalIndex, &encoded),
            &encoded,
            &limits,
        )
        .unwrap();
        assert_eq!(encode(&page, &limits).unwrap(), encoded);
        let descriptor: IndexDescriptor = serde_json::from_value(json!({
            "level": 0, "range_start": 4, "range_end": 7,
            "object": reference(StorageFormat::OrdinalIndex, &encoded),
        }))
        .unwrap();
        descriptor.validate_page(&page, kind, &limits).unwrap();
        for field in ["range_start", "range_end"] {
            let mut missing = content.clone();
            missing.as_object_mut().unwrap().remove(field);
            rejects::<IndexPage>(missing);
            let mut missing = content.clone();
            missing["entries"][0].as_object_mut().unwrap().remove(field);
            rejects::<IndexPage>(missing);
        }
    }
    for (root, field, end) in [
        (serde_json::to_value(manifest()).unwrap(), "batches", 3),
        (serde_json::to_value(plan()).unwrap(), "chunks", 2),
    ] {
        assert_eq!(root[field]["range_start"], json!(0));
        assert_eq!(root[field]["range_end"], json!(end));
    }
}

#[test]
fn metadata_decoding_leaves_semantic_checks_to_validation() {
    let mut content: Value = serde_json::from_slice(INDEX).unwrap();
    content["range_start"] = json!(-1);
    let encoded = bytes(&content);
    let object = reference(StorageFormat::OrdinalIndex, &encoded);
    let verified = object.verify(&encoded, &FormatLimits::default()).unwrap();
    let decoded: IndexPage =
        decode(&verified).expect("well-formed fields must decode before semantic validation");
    assert!(decoded.validate(&FormatLimits::default()).is_err());
}

#[test]
fn verification_checks_integrity_and_admission_without_parsing() {
    let limits = FormatLimits::default();
    let malformed = b"{broken-json";
    let object = reference(StorageFormat::OrdinalIndex, malformed);
    let verified = object.verify(malformed, &limits).unwrap();
    assert!(decode::<IndexPage>(&verified).is_err());
    let mut corrupted = malformed.to_vec();
    corrupted[1] = b'x';
    assert!(
        object
            .verify(&corrupted, &limits)
            .unwrap_err()
            .to_string()
            .contains("digest mismatch")
    );
    assert!(
        object
            .verify(&malformed[..malformed.len() - 1], &limits)
            .unwrap_err()
            .to_string()
            .contains("byte_count mismatch")
    );
    let mut invalid = object.clone();
    invalid.byte_count = 0;
    assert!(invalid.verify(malformed, &limits).is_err());
    let mut invalid = limits.clone();
    invalid.batch.max_cases = 0;
    assert!(object.verify(malformed, &invalid).is_err());
    let oversized = vec![b' '; 257];
    let object = reference(StorageFormat::OrdinalIndex, &oversized);
    let narrow = FormatLimits {
        page_bytes: 256,
        ..limits
    };
    assert!(object.verify(&oversized, &narrow).is_err());
}

#[test]
fn verified_metadata_decoding_checks_shape_and_requested_format() {
    let limits = FormatLimits::default();
    let original: Value = serde_json::from_slice(INDEX).unwrap();
    for altered in [
        {
            let mut value = original.clone();
            value["unknown"] = json!(true);
            value
        },
        {
            let mut value = original.clone();
            value.as_object_mut().unwrap().remove("range_end");
            value
        },
        {
            let mut value = original.clone();
            value["range_end"] = json!("3");
            value
        },
        {
            let mut value = original;
            value["schema"] = json!("unsupported");
            value
        },
    ] {
        let encoded = bytes(&altered);
        let verified = reference(StorageFormat::OrdinalIndex, &encoded)
            .verify(&encoded, &limits)
            .unwrap();
        assert!(decode::<IndexPage>(&verified).is_err());
    }
    let duplicate = br#"{"x":1,"x":2}"#;
    let verified = reference(StorageFormat::OrdinalIndex, duplicate)
        .verify(duplicate, &limits)
        .unwrap();
    assert!(
        format!("{:#}", decode::<IndexPage>(&verified).unwrap_err()).contains("duplicate JSON key")
    );
    let verified = reference(StorageFormat::OrdinalIndex, INDEX)
        .verify(INDEX, &limits)
        .unwrap();
    assert!(decode::<DatasetSourceFields>(&verified).is_err());
    assert!(decode_case_batch(&verified).is_err());
    let page: IndexPage = decode(&verified).unwrap();
    page.validate(&limits).unwrap();
    assert_eq!(encode(&page, &limits).unwrap(), INDEX);
}

#[test]
fn verified_object_keeps_reference_snapshot_borrows_bytes_and_redacts_payload() {
    let limits = FormatLimits::default();
    let mut original = reference(StorageFormat::OrdinalIndex, INDEX);
    let verified = original.verify(INDEX, &limits).unwrap();
    assert_eq!(verified.reference(), &original);
    assert!(std::ptr::eq(verified.bytes().as_ptr(), INDEX.as_ptr()));
    original.schema = StorageFormat::CaseBatch;
    original.digest = "0".repeat(64);
    assert_eq!(verified.reference().schema, StorageFormat::OrdinalIndex);
    let page: IndexPage = decode(&verified).unwrap();
    page.validate(&limits).unwrap();
    let secret = b"payload-must-not-appear-in-diagnostics";
    let verified = reference(StorageFormat::OrdinalIndex, secret)
        .verify(secret, &limits)
        .unwrap();
    let debug = format!("{verified:?}");
    assert!(!debug.contains(std::str::from_utf8(secret).unwrap()));
    assert!(debug.contains("byte_count"));
}

#[test]
fn batch_decoding_defers_header_semantics_and_rechecks_validation_budgets() {
    let limits = FormatLimits::default();
    let header_end = BATCH.iter().position(|byte| *byte == b'\n').unwrap();
    let mut header: Value = serde_json::from_slice(&BATCH[..header_end]).unwrap();
    header["ordinal_start"] = json!(-1);
    let mut encoded = bytes(&header);
    encoded.extend_from_slice(&BATCH[header_end..]);
    let verified = reference(StorageFormat::CaseBatch, &encoded)
        .verify(&encoded, &limits)
        .unwrap();
    let decoded = decode_case_batch(&verified)
        .expect("valid header fields decode before semantic validation");
    assert!(decoded.validate(&limits).is_err());
    let verified = reference(StorageFormat::CaseBatch, BATCH)
        .verify(BATCH, &limits)
        .unwrap();
    let mut narrow = limits.clone();
    narrow.batch.max_cases = 2;
    assert!(
        decode_case_batch(&verified)
            .unwrap()
            .validate(&narrow)
            .is_err()
    );
    narrow = limits.clone();
    narrow.batch.max_bytes = BATCH.len() - 1;
    assert!(
        decode_case_batch(&verified)
            .unwrap()
            .validate(&narrow)
            .is_err()
    );
    narrow = limits.clone();
    narrow.index_height = 0;
    assert!(
        decode_case_batch(&verified)
            .unwrap()
            .validate(&narrow)
            .is_err()
    );
    decode_case_batch(&verified)
        .unwrap()
        .validate(&limits)
        .unwrap()
        .validate_remaining()
        .unwrap();
    let no_lf = &BATCH[..BATCH.len() - 1];
    let verified = reference(StorageFormat::CaseBatch, no_lf)
        .verify(no_lf, &limits)
        .unwrap();
    assert!(decode_case_batch(&verified).is_err());
    let malformed = b"{broken-header}\n";
    let verified = reference(StorageFormat::CaseBatch, malformed)
        .verify(malformed, &limits)
        .unwrap();
    assert!(decode_case_batch(&verified).is_err());
}

#[test]
fn streaming_validation_rejects_bad_records_and_incomplete_suffixes() {
    let limits = FormatLimits::default();
    let lines: Vec<_> = BATCH
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .collect();
    let mut first: Value = serde_json::from_slice(lines[1]).unwrap();
    first["case_hash"] = json!("0".repeat(64));
    let mut encoded = lines[0].to_vec();
    encoded.push(b'\n');
    encoded.extend(bytes(&first));
    encoded.push(b'\n');
    let verified = reference(StorageFormat::CaseBatch, &encoded)
        .verify(&encoded, &limits)
        .unwrap();
    let mut reader = decode_case_batch(&verified)
        .unwrap()
        .validate(&limits)
        .unwrap();
    assert!(
        reader
            .next()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("case hash mismatch")
    );
    assert!(reader.validate_remaining().is_err());
    let truncated = [lines[0], b"\n", lines[1], b"\n"].concat();
    let verified = reference(StorageFormat::CaseBatch, &truncated)
        .verify(&truncated, &limits)
        .unwrap();
    let mut reader = decode_case_batch(&verified)
        .unwrap()
        .validate(&limits)
        .unwrap();
    reader.next().unwrap().unwrap();
    assert!(reader.validate_remaining().is_err());
}
