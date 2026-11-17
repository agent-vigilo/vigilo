#!/usr/bin/env python3
"""Verify the distributed crate and its tests without repository-relative assets.

Dependencies are fetched separately. Cargo packages and verifies the locked crate
without network access, then its library/binary tests and operation doctests run
from an extracted temporary directory outside the checkout. No publication occurs.
"""
import argparse
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import tomllib


def verify(root, allow_dirty=False):
    """Use the package itself as the boundary; always remove extracted sources."""
    target = Path(os.environ.get("CARGO_TARGET_DIR", root / "target"))
    if not target.is_absolute():
        target = root / target
    target = target.resolve()
    package = tomllib.loads((root / "vigilo/Cargo.toml").read_text())["package"]
    identity = f'{package["name"]}-{package["version"]}'
    command = ["cargo", "package", "-p", "vigilo", "--locked", "--offline",
               "--target-dir", str(target)]
    if allow_dirty:
        command.append("--allow-dirty")
    subprocess.run(command, cwd=root, check=True)
    with tempfile.TemporaryDirectory(prefix="vigilo-package-") as temporary:
        with tarfile.open(target / "package" / f"{identity}.crate", "r:gz") as archive:
            archive.extractall(temporary, filter="data")
        source = Path(temporary) / identity
        arguments = ["cargo", "test", "--locked", "--offline", "--target-dir", str(target)]
        subprocess.run([*arguments, "--lib", "--bins"], cwd=source, check=True)
        subprocess.run([*arguments, "--doc", "operations"], cwd=source, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-dirty", action="store_true",
                        help="permit packaging local uncommitted changes")
    arguments = parser.parse_args()
    verify(Path(__file__).resolve().parents[2], arguments.allow_dirty)


if __name__ == "__main__":
    main()
