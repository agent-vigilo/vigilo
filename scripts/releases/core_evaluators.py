#!/usr/bin/env python3
"""Build and release an immutable core evaluator collection on GitHub.

The CLI packages and validates each evaluator. This script indexes those packages,
compares the published collection, and publishes a draft only after change checks.
Use --prepare-only to build and inspect assets without any GitHub mutation.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
import tomllib

REPO = "agent-vigilo/vigilo"
FORMAT = "vigilo/core-evaluators/v1"
MAX_ARCHIVE = 66 * 1024 * 1024


def command(*args):
    return subprocess.check_output(args, text=True).strip()


def collection_key(value):
    match = re.fullmatch(r"(\d{4}\.\d{2}\.\d{2})\.([1-9][0-9]*)", value)
    if not match or int(match[2]) > 4294967295:
        raise ValueError(f"invalid collection: {value}")
    return datetime.datetime.strptime(match[1], "%Y.%m.%d").date(), int(match[2])


def describe(path):
    """Read only the bounded manifest; the Vigilo packager validates Wasm/ABI."""
    size = path.stat().st_size
    if not 0 < size <= MAX_ARCHIVE:
        raise ValueError("archive size outside contract")
    with tarfile.open(path, "r:gz") as archive:
        members = archive.getmembers()
        if {m.name for m in members} != {"Vigilo.toml", "evaluator.wasm"} or len(members) != 2:
            raise ValueError("unexpected archive members")
        if any(not m.isfile() for m in members):
            raise ValueError("archive members must be regular files")
        member = archive.getmember("Vigilo.toml")
        if member.size > 1024 * 1024:
            raise ValueError("manifest exceeds byte limit")
        manifest = tomllib.loads(archive.extractfile(member).read().decode())
    if manifest["format"] != "vigilo/evaluator-package/v1":
        raise ValueError("unknown package format")
    package = manifest["package"]
    asset = f'{package["name"]}-{package["version"]}.tar.gz'
    if path.name != asset:
        raise ValueError("archive filename disagrees with manifest")
    with path.open("rb") as file:
        checksum = hashlib.file_digest(file, "sha256").hexdigest()
    return dict(name=package["name"], version=package["version"], asset=asset,
                size_bytes=size, sha256=checksum,
                artifact_blake3=manifest["artifact"]["blake3"], wit=manifest["wit"])


def compare(entries, previous):
    """Reject changed bytes under an existing version; detect empty releases."""
    old = {e["name"]: e for e in previous}
    if len({e["name"] for e in entries}) != len(entries):
        raise ValueError("duplicate evaluator name")
    for entry in entries:
        prior = old.get(entry["name"])
        def stable_version(version):
            if not re.fullmatch(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", version):
                raise ValueError("core releases require stable semantic evaluator versions")
            return tuple(map(int, version.split(".")))
        version = stable_version(entry["version"])
        if prior and version < stable_version(prior["version"]):
            raise ValueError(f'{entry["name"]}: evaluator version cannot decrease')
        if prior and prior["version"] == entry["version"] and prior != entry:
            raise ValueError(f'{entry["name"]}: changed package requires a version bump')
    return sorted(entries, key=lambda e: e["name"]) != sorted(previous, key=lambda e: e["name"])


def releases():
    result = []
    for page in range(1, 11):
        batch = json.loads(command("gh", "api", f"repos/{REPO}/releases?per_page=100&page={page}"))
        result.extend(batch)
        if len(batch) < 100:
            return result
    raise ValueError("release discovery limit reached (1000 releases)")


def build(root, output, binary):
    entries = []
    sources = sorted((root / "evaluators").glob("*/Vigilo.toml"))
    if not 1 <= len(sources) <= 1000:
        raise ValueError("expected 1..=1000 core evaluator packages")
    for source in sources:
        package = tomllib.loads((source.parent / "Cargo.toml").read_text())["package"]
        command("cargo", "test", "--locked", "-p", package["name"], "--lib")
        command("cargo", "build", "--locked", "-p", package["name"], "--target", "wasm32-wasip2", "--release")
        archive = output / f'{package["name"]}-{package["version"]}.tar.gz'
        command(str(binary), "evaluator", "package", str(source.parent), "--release", "--output", str(archive))
        entries.append(describe(archive))
    return entries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--output", type=Path, required=True, help="new, nonexistent output directory")
    parser.add_argument("--binary", type=Path, default=Path("target/release/vigilo"))
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    os.chdir(root)
    args.output.mkdir(parents=True, exist_ok=False)
    entries = build(root, args.output, args.binary.resolve())
    (args.output / "LICENSE").write_bytes((root / "LICENSE").read_bytes())
    if args.prepare_only:
        catalog = dict(format=FORMAT, collection="1970.01.01.1", evaluators=entries)
        (args.output / "catalog.json").write_text(json.dumps(catalog, indent=2) + "\n")
        print("Prepared assets only; catalog uses a placeholder collection. Nothing published.")
        return
    all_releases = releases()
    published = sorted((r for r in all_releases if not r["draft"] and not r["prerelease"]
                        and r["tag_name"].startswith("evaluators/")),
                       key=lambda r: collection_key(r["tag_name"].removeprefix("evaluators/")), reverse=True)
    previous = []
    if published:
        with tempfile.TemporaryDirectory() as directory:
            command("gh", "release", "download", published[0]["tag_name"], "--repo", REPO,
                    "--pattern", "catalog.json", "--dir", directory)
            path = Path(directory) / "catalog.json"
            if path.stat().st_size > 4 * 1024 * 1024:
                raise ValueError("catalog exceeds byte limit")
            catalog = json.loads(path.read_text())
            if catalog["format"] != FORMAT or catalog["collection"] != published[0]["tag_name"].removeprefix("evaluators/"):
                raise ValueError("published catalog identity mismatch")
            previous = catalog["evaluators"]
    if not compare(entries, previous):
        print("No evaluator changes; no tag or release created.")
        return
    # Include drafts when reserving the next sequence: never overwrite a failed release.
    today = datetime.datetime.now(datetime.timezone.utc).date()
    sequence = 1
    for release in all_releases:
        if release["tag_name"].startswith("evaluators/"):
            date, number = collection_key(release["tag_name"].removeprefix("evaluators/"))
            if date == today:
                sequence = max(sequence, number + 1)
    collection = f'{today:%Y.%m.%d}.{sequence}'
    collection_key(collection)
    catalog = dict(format=FORMAT, collection=collection, evaluators=entries)
    (args.output / "catalog.json").write_text(json.dumps(catalog, indent=2) + "\n")
    checksums = "".join(f'{e["sha256"]}  {e["asset"]}\n' for e in entries)
    (args.output / "SHA256SUMS").write_text(checksums)
    tag = f"evaluators/{collection}"
    sha = command("git", "rev-parse", "HEAD")
    command("gh", "release", "create", tag, "--repo", REPO, "--target", sha, "--draft",
            "--latest=false", "--title", f"Core Evaluators {collection}",
            "--notes", "Official core evaluator collection. See catalog.json for exact versions and ABI identities.")
    # Upload bounded groups; a failure leaves a hidden draft for operator inspection.
    for asset in sorted(args.output.iterdir()):
        command("gh", "release", "upload", tag, str(asset), "--repo", REPO)
    command("gh", "release", "edit", tag, "--repo", REPO, "--draft=false", "--latest=false")
    print(f"Published {tag} at {sha}")


if __name__ == "__main__":
    main()
