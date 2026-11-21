"""Service-free checks for the crate release identity boundary."""
import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from crate import validate_tag
from verify_package import main as verify_package


class CrateReleaseTests(unittest.TestCase):
    def test_exact_release_and_prerelease_versions(self):
        for version in ["0.1.0", "1.2.3", "0.2.0-beta.1"]:
            self.assertEqual(validate_tag(dict(name="vigilo", version=version),
                                          f"vigilo/v{version}"), version)

    def test_wrong_version_and_other_release_families_fail(self):
        package = dict(name="vigilo", version="0.1.0")
        for tag in ["vigilo/v0.2.0", "v0.1.0", "vigilo/0.1.0",
                    "evaluators/2026.10.29.1", "vigilo/v0.1.0\n", ""]:
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                validate_tag(package, tag)

    def test_only_vigilo_with_an_explicit_version_can_be_published(self):
        for package in [dict(name="xtask", version="0.1.0"),
                        dict(name="sentiment-basic-en", version="0.1.0"),
                        dict(name="vigilo"), dict(name="vigilo", version=""),
                        dict(name="vigilo", version={"workspace": True})]:
            with self.subTest(package=package), self.assertRaises(ValueError):
                validate_tag(package, "vigilo/v0.1.0")

    def test_release_verifier_writes_the_verified_archive_checksum(self):
        with TemporaryDirectory() as temporary:
            artifact = Path(temporary) / "vigilo-0.1.0.crate"
            contents = b"verified archive bytes"
            artifact.write_bytes(contents)
            checksum = Path(temporary) / "checksum"

            with patch("verify_package.verify", return_value=artifact), patch(
                "sys.argv", ["verify_package.py", "--checksum-output", str(checksum)]
            ):
                verify_package()

            self.assertEqual(checksum.read_text().strip(), hashlib.sha256(contents).hexdigest())


if __name__ == "__main__":
    unittest.main()
