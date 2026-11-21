#!/usr/bin/env python3
"""Check crates.io version availability and the verified package checksum."""

import argparse
import json
import re
from time import sleep
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


def check_package(version, checksum, *, wait=False):
    """Return whether this exact package is public, failing on a conflicting version."""
    if not re.fullmatch(r"[0-9a-f]{64}", checksum):
        raise ValueError("expected a SHA-256 checksum for the verified crate")
    request = Request(
        f"https://crates.io/api/v1/crates/vigilo/{quote(version, safe='')}",
        headers={"User-Agent": "vigilo-release-workflow"},
    )
    attempts = 6 if wait else 3
    for attempt in range(attempts):
        try:
            with urlopen(request, timeout=15) as response:
                document = json.load(response)
        except HTTPError as error:
            if error.code == 404:
                if not wait:
                    return False
                if attempt == attempts - 1:
                    raise ValueError(f"vigilo {version} did not appear on crates.io") from error
            elif (error.code != 429 and error.code < 500) or attempt == attempts - 1:
                raise
        except (URLError, TimeoutError):
            if attempt == attempts - 1:
                raise
        else:
            entry = document.get("version") if isinstance(document, dict) else None
            if (
                not isinstance(entry, dict)
                or (entry.get("crate"), entry.get("num"), entry.get("checksum"))
                != ("vigilo", version, checksum)
                or entry.get("yanked") is not False
            ):
                raise ValueError(
                    f"published crate vigilo {version} is unavailable or differs from the verified package"
                )
            return True
        sleep(3)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--checksum", required=True)
    parser.add_argument("--wait", action="store_true")
    args = parser.parse_args()
    try:
        published = check_package(args.version, args.checksum, wait=args.wait)
    except (OSError, ValueError) as error:
        parser.exit(1, f"{error}\n")
    print("true" if published else "false")


if __name__ == "__main__":
    main()
