"""Service-free checks for crate release retries and publication visibility."""

from io import BytesIO
import json
from unittest import TestCase
from unittest.mock import patch
from urllib.error import HTTPError

from registry import check_package


CHECKSUM = "a" * 64
VERSION = "0.1.0"


def registry_response(**fields):
    version = {"crate": "vigilo", "num": VERSION, "checksum": CHECKSUM, "yanked": False}
    return BytesIO(json.dumps({"version": version | fields}).encode())


def not_found():
    return HTTPError("https://crates.io/", 404, "missing", {}, None)


class RegistryReleaseTests(TestCase):
    @patch("registry.urlopen")
    def test_existing_package_must_match_exact_checksum(self, request):
        request.return_value = registry_response()
        self.assertTrue(check_package(VERSION, CHECKSUM))

        for fields in [{"checksum": "b" * 64}, {"num": "0.2.0"},
                       {"crate": "unrelated"}, {"yanked": True}]:
            with self.subTest(fields=fields), self.assertRaisesRegex(ValueError, "published crate"):
                request.return_value = registry_response(**fields)
                check_package(VERSION, CHECKSUM)

    @patch("registry.urlopen")
    def test_absence_allows_first_publication(self, request):
        request.side_effect = not_found()
        self.assertFalse(check_package(VERSION, CHECKSUM))

    @patch("registry.urlopen")
    def test_registry_access_error_fails_closed(self, request):
        request.side_effect = HTTPError("https://crates.io/", 403, "forbidden", {}, None)
        with self.assertRaises(HTTPError):
            check_package(VERSION, CHECKSUM)

    @patch("registry.sleep")
    @patch("registry.urlopen")
    def test_published_package_can_take_time_to_appear(self, request, delay):
        request.side_effect = [not_found(), registry_response()]
        self.assertTrue(check_package(VERSION, CHECKSUM, wait=True))
        delay.assert_called_once()

    @patch("registry.sleep")
    @patch("registry.urlopen")
    def test_waiting_for_publication_is_bounded(self, request, delay):
        request.side_effect = not_found()
        with self.assertRaisesRegex(ValueError, "did not appear"):
            check_package(VERSION, CHECKSUM, wait=True)
        self.assertEqual(request.call_count, delay.call_count + 1)
        self.assertLessEqual(request.call_count, 6)
