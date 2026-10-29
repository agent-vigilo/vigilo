//! Portable package format and integrity checks, independent of external services.
use super::*;

fn input() -> PackageInput {
    PackageInput {
        name: "example".into(),
        version: "1.2.3".into(),
        description: Some("test".into()),
        tags: serde_json::json!(["test"]),
        metadata: serde_json::json!({"owner":"test"}),
        abi: resolve_declaration("vigilo:evaluator", "evaluator-world", "evaluator", "1.0.0")
            .unwrap()
            .identity()
            .clone(),
        wasm: b"fixture bytes; host compilation is a separate boundary".to_vec(),
    }
}

#[test]
fn archives_are_deterministic_round_trip_exactly_and_never_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("first.tar.gz");
    let second = dir.path().join("second.tar.gz");
    write(&first, input()).unwrap();
    write(&second, input()).unwrap();
    let before = fs::read(&first).unwrap();
    assert_eq!(before, fs::read(&second).unwrap());
    let loaded = load(&first).unwrap().unwrap();
    assert_eq!(loaded.wasm, input().wasm);
    assert_eq!(loaded.name, "example");
    assert_eq!(loaded.version, "1.2.3");
    assert_eq!(loaded.tags, input().tags);
    assert_eq!(loaded.metadata, input().metadata);
    assert_eq!(loaded.abi, input().abi);
    assert!(write(&first, input()).is_err());
    assert_eq!(fs::read(&first).unwrap(), before);
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 2);
}

#[test]
fn unpacked_packages_validate_hashes_identity_and_format_without_source_files() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("package.tar.gz");
    write(&path, input()).unwrap();
    let (manifest, wasm) = read_archive(&path).unwrap();
    fs::write(dir.path().join(MANIFEST), &manifest).unwrap();
    fs::write(dir.path().join(WASM), &wasm).unwrap();
    assert_eq!(load(dir.path()).unwrap().unwrap().wasm, wasm);
    let original = String::from_utf8(manifest).unwrap();
    for (from, to) in [
        (FORMAT, "unknown"),
        ("evaluator.wasm", "../outside.wasm"),
        ("name = \"example\"", "name = \"bad/name\""),
        ("1.2.3", "not-semver"),
        ("evaluator-world", "unknown-world"),
        (input().abi.contract_hash.as_str(), "wrong"),
        (blake3::hash(&wasm).to_hex().as_str(), "wrong"),
    ] {
        fs::write(dir.path().join(MANIFEST), original.replace(from, to)).unwrap();
        assert!(load(dir.path()).is_err(), "accepted mutation {from}");
    }
    fs::write(
        dir.path().join(MANIFEST),
        format!("unknown = 1\n{original}"),
    )
    .unwrap();
    assert!(load(dir.path()).is_err());
    fs::write(dir.path().join(MANIFEST), original).unwrap();
    fs::write(dir.path().join(WASM), b"tampered").unwrap();
    assert!(load(dir.path()).is_err());
}

#[test]
fn source_directories_remain_distinct_and_reads_are_bounded() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join(MANIFEST),
        "[package]\nmanifest='Cargo.toml'\n",
    )
    .unwrap();
    assert!(load(dir.path()).unwrap().is_none());
    assert_eq!(read_bounded(&b"abcd"[..], 4).unwrap(), b"abcd");
    assert!(read_bounded(&b"abcde"[..], 4).is_err());
    assert!(read_bounded(&b"a"[..], 0).is_err());
    fs::write(
        dir.path().join(MANIFEST),
        vec![b' '; MAX_MANIFEST as usize + 1],
    )
    .unwrap();
    assert!(load(dir.path()).is_err());
}

fn archive(entries: &[(&str, &[u8], u8)], declared_size: Option<u64>) -> Vec<u8> {
    let mut tar = Vec::new();
    for (name, bytes, kind) in entries {
        let mut header = tar::Header::new_ustar();
        header.as_mut_bytes()[..name.len()].copy_from_slice(name.as_bytes());
        header.set_entry_type(tar::EntryType::new(*kind));
        header.set_size(declared_size.unwrap_or(bytes.len() as u64));
        header.set_cksum();
        tar.extend_from_slice(header.as_bytes());
        tar.extend_from_slice(bytes);
        tar.resize(tar.len().div_ceil(512) * 512, 0);
    }
    tar.resize(tar.len() + 1024, 0);
    let mut encoder = GzBuilder::new()
        .mtime(0)
        .write(Vec::new(), Compression::default());
    encoder.write_all(&tar).unwrap();
    encoder.finish().unwrap()
}

#[test]
fn archives_reject_unsafe_paths_types_duplicates_missing_and_oversized_members() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.tar.gz");
    let cases = [
        vec![("../outside", b"".as_slice(), b'0')],
        vec![("/evaluator.wasm", b"".as_slice(), b'0')],
        vec![("nested/evaluator.wasm", b"".as_slice(), b'0')],
        vec![("Vigilo.toml", b"".as_slice(), b'2')],
        vec![("Vigilo.toml", b"".as_slice(), b'1')],
        vec![("Vigilo.toml", b"".as_slice(), b'5')],
        vec![
            ("Vigilo.toml", b"".as_slice(), b'0'),
            ("Vigilo.toml", b"".as_slice(), b'0'),
        ],
        vec![("Vigilo.toml", b"".as_slice(), b'0')],
    ];
    for entries in cases {
        fs::write(&path, archive(&entries, None)).unwrap();
        assert!(read_archive(&path).is_err(), "accepted {entries:?}");
    }
    fs::write(&path, archive(&[(WASM, b"", b'0')], Some(MAX_WASM + 1))).unwrap();
    assert!(
        read_archive(&path)
            .unwrap_err()
            .to_string()
            .contains("byte limit")
    );
    assert!(!dir.path().join("outside").exists());
}

#[test]
fn truncated_corrupt_and_trailing_archives_fail_without_outputs() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("package.tar.gz");
    write(&path, input()).unwrap();
    let original = fs::read(&path).unwrap();
    for end in [0, 4, original.len() / 2, original.len() - 1] {
        fs::write(&path, &original[..end]).unwrap();
        assert!(load(&path).is_err());
    }
    let mut corrupted = original.clone();
    let last = corrupted.len() - 1;
    corrupted[last] ^= 1;
    fs::write(&path, corrupted).unwrap();
    assert!(load(&path).is_err());
    let mut trailing = original;
    trailing.extend(archive(&[("extra", b"x", b'0')], None));
    fs::write(&path, trailing).unwrap();
    assert!(load(&path).is_err());
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
}

#[test]
fn directory_members_must_be_regular_files_and_invalid_writes_leave_no_artifact() {
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir(dir.path().join(MANIFEST)).unwrap();
    assert!(load(dir.path()).is_err());
    let mut bad = input();
    bad.name = "invalid/name".into();
    let output = dir.path().join("bad.tar.gz");
    assert!(write(&output, bad).is_err());
    assert!(!output.exists());
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
}
