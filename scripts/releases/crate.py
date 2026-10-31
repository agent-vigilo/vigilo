#!/usr/bin/env python3
"""Validate the Vigilo release tag against the checked-out crate manifest.

This command performs no network access or publication. The workflow checks out
GitHub's release commit, validates its identity here, then publishes that package.
"""
import argparse
from pathlib import Path
import tomllib


def validate_tag(package, tag):
    """Return the exact crate version, rejecting other packages or release families."""
    version = package.get("version")
    if package.get("name") != "vigilo" or not isinstance(version, str) or not version:
        raise ValueError("expected the vigilo crate with an explicit version")
    expected = f"vigilo/v{version}"
    if tag != expected:
        raise ValueError(f"release tag {tag!r} does not match crate tag {expected!r}")
    return version


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    with (root / "vigilo/Cargo.toml").open("rb") as manifest:
        package = tomllib.load(manifest)["package"]
    print(validate_tag(package, args.tag))


if __name__ == "__main__":
    main()
