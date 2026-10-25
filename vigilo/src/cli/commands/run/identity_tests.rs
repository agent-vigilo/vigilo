//! Golden compatibility checks for identities that blob ingestion must preserve.
//!
//! Expected bytes and BLAKE3 digests were calculated independently of the CLI
//! implementation. These tests exercise the existing input boundary without
//! databases or services; they do not prescribe a future blob encoding.

use serde_json::{
    Value,
    json,
};
use uuid::Uuid;

use super::{
    build_case_plans,
    build_chunks,
    canonical_json,
    compute_dataset_version_id,
    hash_json,
    parse_structured_payload,
};
use crate::contracts::run::RunDataset;

const JSON: &str = include_str!("fixtures/dataset-identity.json");
const YAML: &str = include_str!("fixtures/dataset-identity.yaml");
const EXPECTED: &str = include_str!("fixtures/dataset-identity.expected.json");

fn fixture() -> Value {
    serde_json::from_str(JSON).unwrap()
}

fn expected() -> Value {
    serde_json::from_str(EXPECTED).unwrap()
}

fn case_hashes(payload: &Value) -> Vec<String> {
    let dataset: RunDataset = serde_json::from_value(payload.clone()).unwrap();
    build_case_plans(&dataset)
        .unwrap()
        .0
        .into_iter()
        .map(|blob| blob.case_hash)
        .collect()
}

fn version_id(payload: &Value) -> Uuid {
    let dataset: RunDataset = serde_json::from_value(payload.clone()).unwrap();
    let (_, membership) = build_case_plans(&dataset).unwrap();
    compute_dataset_version_id(&dataset, &membership).unwrap()
}

#[test]
fn json_and_yaml_preserve_exact_dataset_bytes_and_hash() {
    let golden = expected();
    for source in [JSON, YAML] {
        let parsed = parse_structured_payload(source, "dataset").unwrap();
        let canonical = canonical_json(&parsed);
        assert_eq!(
            serde_json::to_string(&canonical).unwrap(),
            golden["dataset_canonical"]
        );
        assert_eq!(hash_json(&canonical).unwrap(), golden["dataset_hash"]);
    }
}

#[test]
fn normalized_case_bytes_hashes_and_ordinals_match_golden() {
    let golden = expected();
    for source in [JSON, YAML] {
        let dataset: RunDataset = serde_yaml::from_str(source).unwrap();
        let (blobs, membership) = build_case_plans(&dataset).unwrap();
        assert_eq!(blobs.len(), 3);
        assert_eq!(membership.len(), 3);
        for (index, blob) in blobs.iter().enumerate() {
            let payload = json!({
                "task_type": blob.task_type,
                "case_group": blob.case_group,
                "input": blob.input_payload,
                "expected_output": blob.expected_output,
                "context": blob.context_payload,
                "tags": blob.tags,
                "metadata": blob.metadata,
            });
            assert_eq!(
                serde_json::to_string(&payload).unwrap(),
                golden["blobs"][index]["canonical"]
            );
            assert_eq!(blob.case_hash, golden["blobs"][index]["hash"]);
            assert_eq!(membership[index].case_hash, blob.case_hash);
        }
        assert_eq!(
            serde_json::to_value(&membership).unwrap(),
            golden["membership"]
        );
        assert_eq!(blobs[1].case_hash, blobs[2].case_hash);
        assert_ne!(membership[1].case_id, membership[2].case_id);
        assert_eq!(blobs[0].tags, json!(["alpha", "alpha", "zeta"]));
    }
}

#[test]
fn ordered_membership_preserves_exact_version_uuid() {
    let golden = expected();
    let payload = fixture();
    let preimage = json!({
        "dataset_id": payload["dataset_id"],
        "dataset_version": payload["dataset_version"],
        "membership": golden["membership"],
    });
    assert_eq!(
        serde_json::to_string(&preimage).unwrap(),
        golden["version_preimage"]
    );
    assert_eq!(
        version_id(&payload).to_string(),
        golden["dataset_version_id"]
    );
    assert_eq!(version_id(&payload).get_version_num(), 8);
}

#[test]
fn omitted_and_explicit_nulls_have_same_case_identity_but_distinct_source_hash() {
    let original = fixture();
    let mut explicit = original.clone();
    explicit["cases"][1]["expected"] = Value::Null;
    explicit["cases"][1]["context"] = Value::Null;
    explicit["cases"][1]["case_group"] = Value::Null;
    explicit["cases"][1]["tags"] = json!([]);
    explicit["cases"][1]["metadata"] = json!({});
    assert_eq!(case_hashes(&original), case_hashes(&explicit));
    assert_eq!(version_id(&original), version_id(&explicit));
    assert_ne!(
        hash_json(&canonical_json(&original)).unwrap(),
        hash_json(&canonical_json(&explicit)).unwrap()
    );
}

#[test]
fn tag_order_is_normalized_but_duplicate_tags_are_preserved() {
    let original = fixture();
    let mut reordered = original.clone();
    reordered["cases"][0]["tags"] = json!(["alpha", "zeta", "alpha"]);
    assert_eq!(case_hashes(&original), case_hashes(&reordered));
    assert_eq!(version_id(&original), version_id(&reordered));
    assert_ne!(
        hash_json(&original).unwrap(),
        hash_json(&reordered).unwrap()
    );
    reordered["cases"][0]["tags"] = json!(["alpha", "zeta"]);
    assert_ne!(case_hashes(&original)[0], case_hashes(&reordered)[0]);
    assert_ne!(version_id(&original), version_id(&reordered));
}

#[test]
fn every_case_content_field_contributes_to_identity() {
    let original = fixture();
    let hashes = case_hashes(&original);
    for (field, replacement) in [
        ("task_type", json!("other")),
        ("case_group", Value::Null),
        ("input", json!({"different": true})),
        ("expected", Value::Null),
        ("context", Value::Null),
        ("tags", json!([])),
        ("metadata", json!({})),
    ] {
        let mut changed = original.clone();
        changed["cases"][0][field] = replacement;
        let changed_hashes = case_hashes(&changed);
        assert_ne!(hashes[0], changed_hashes[0], "{field}");
        assert_eq!(hashes[1..], changed_hashes[1..], "{field}");
        assert_ne!(version_id(&original), version_id(&changed), "{field}");
    }
}

#[test]
fn case_id_and_dataset_identity_change_membership_identity_without_changing_content() {
    let original = fixture();
    for (path, replacement) in [
        ("/cases/0/id", json!("018f1111-1111-7111-8111-111111111199")),
        ("/dataset_id", json!("018f1111-1111-7111-8111-111111111199")),
        ("/dataset_version", json!("another-version")),
        ("/dataset_version", Value::Null),
    ] {
        let mut changed = original.clone();
        *changed.pointer_mut(path).unwrap() = replacement;
        assert_eq!(case_hashes(&original), case_hashes(&changed), "{path}");
        assert_ne!(version_id(&original), version_id(&changed), "{path}");
    }
}

#[test]
fn case_order_changes_ordinals_and_version_identity() {
    let original = fixture();
    let mut reversed = original.clone();
    reversed["cases"].as_array_mut().unwrap().reverse();
    let mut hashes = case_hashes(&original);
    hashes.reverse();
    assert_eq!(hashes, case_hashes(&reversed));
    assert_ne!(version_id(&original), version_id(&reversed));
    assert_ne!(hash_json(&original).unwrap(), hash_json(&reversed).unwrap());
    let dataset: RunDataset = serde_json::from_value(reversed).unwrap();
    let (_, members) = build_case_plans(&dataset).unwrap();
    for (ordinal, member) in members.iter().enumerate() {
        assert_eq!(member.case_ordinal, i32::try_from(ordinal).unwrap());
        assert_eq!(member.case_id, dataset.cases[ordinal].id);
    }
}

#[test]
fn chunk_ranges_cover_membership_once_including_partial_final_batch() {
    for (count, size, ranges) in [
        (0, 2, vec![]),
        (1, 2, vec![(0, 1)]),
        (3, 2, vec![(0, 2), (2, 3)]),
        (3, 3, vec![(0, 3)]),
        (3, 4, vec![(0, 3)]),
    ] {
        let chunks = build_chunks(count, size);
        let actual: Vec<_> = chunks
            .iter()
            .map(|c| (c.ordinal_start, c.ordinal_end))
            .collect();
        assert_eq!(actual, ranges);
        let covered: Vec<_> = chunks
            .iter()
            .flat_map(|c| c.ordinal_start..c.ordinal_end)
            .collect();
        assert_eq!(
            covered,
            (0..i32::try_from(count).unwrap()).collect::<Vec<_>>()
        );
    }
}
