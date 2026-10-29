"""Service-free release policy tests; no builds, network, tags, or publication."""
import unittest
from core_evaluators import collection_key, compare


class ReleasePolicyTests(unittest.TestCase):
    def test_numeric_sequence(self):
        self.assertGreater(collection_key("2026.10.28.10"), collection_key("2026.10.28.2"))
        for invalid in ["latest", "2026.02.29.1", "2026.1.01.1", "2026.10.28.0", "2026.10.28.01"]:
            with self.assertRaises(ValueError):
                collection_key(invalid)

    def test_unchanged_is_not_a_release_and_changed_bytes_require_version(self):
        entry = dict(name="sentiment", version="0.1.0", sha256="a")
        self.assertFalse(compare([entry], [dict(entry)]))
        self.assertTrue(compare([entry], []))
        self.assertTrue(compare([], [entry]))
        with self.assertRaisesRegex(ValueError, "version bump"):
            compare([dict(entry, sha256="b")], [entry])
        self.assertTrue(compare([dict(entry, version="0.2.0", sha256="b")], [entry]))
        with self.assertRaisesRegex(ValueError, "cannot decrease"):
            compare([dict(entry, version="0.0.9")], [entry])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            compare([entry, entry], [])


class ArchiveCatalogTests(unittest.TestCase):
    def test_catalog_describes_exact_archive_without_extracting(self):
        import hashlib
        import io
        from pathlib import Path
        import tarfile
        import tempfile
        from core_evaluators import describe
        manifest = b'''format = "vigilo/evaluator-package/v1"
[package]
name = "test"
version = "0.1.0"
[artifact]
blake3 = "content"
[wit]
package = "vigilo:evaluator"
'''
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test-0.1.0.tar.gz"
            with tarfile.open(path, "w:gz") as archive:
                for name, data in [("Vigilo.toml", manifest), ("evaluator.wasm", b"wasm")]:
                    header = tarfile.TarInfo(name)
                    header.size = len(data)
                    archive.addfile(header, io.BytesIO(data))
            entry = describe(path)
            self.assertEqual(entry["size_bytes"], path.stat().st_size)
            self.assertEqual(entry["sha256"], hashlib.sha256(path.read_bytes()).hexdigest())
            self.assertEqual(entry["name"], "test")
            self.assertEqual(list(Path(directory).iterdir()), [path])
            renamed = path.with_name("wrong.tar.gz")
            path.rename(renamed)
            with self.assertRaisesRegex(ValueError, "filename"):
                describe(renamed)


class LicensePackagingTests(unittest.TestCase):
    def test_crate_license_copies_match_authoritative_workspace_license(self):
        from pathlib import Path
        import tomllib
        root = Path(__file__).resolve().parents[2]
        license_text = (root / "LICENSE").read_bytes()
        workspace = tomllib.loads((root / "Cargo.toml").read_text())
        for member in workspace["workspace"]["members"]:
            with self.subTest(crate=member):
                crate = root / member
                self.assertEqual((crate / "LICENSE").read_bytes(), license_text)
                package = tomllib.loads((crate / "Cargo.toml").read_text())["package"]
                self.assertEqual(package["license"], "Apache-2.0")
                self.assertNotIn("license-file", package)


if __name__ == "__main__":
    unittest.main()
