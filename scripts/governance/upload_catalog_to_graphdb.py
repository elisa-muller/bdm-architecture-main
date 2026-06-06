#!/usr/bin/env python3
"""Upload the generated Turtle catalog to a GraphDB repository.

Defaults:
- GraphDB URL: http://localhost:7200
- repository: bdm-graph
- TTL file: BDM_P2/metadata/data_product_catalog.ttl

We need to create the repository in the GraphDB Workbench first, and then run this script.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def upload_ttl(graphdb_url: str, repository: str, ttl_path: Path, clear_first: bool) -> None:
    base_url = graphdb_url.rstrip("/")
    statements_url = f"{base_url}/repositories/{repository}/statements"

    if clear_first:
        request = Request(statements_url, method="DELETE")
        try:
            with urlopen(request, timeout=30) as response:
                print(f"Cleared repository statements: HTTP {response.status}")
        except HTTPError as exc:
            raise SystemExit(f"Failed to clear repository {repository}: HTTP {exc.code} {exc.reason}") from exc
        except URLError as exc:
            raise SystemExit(f"Could not reach GraphDB at {base_url}: {exc.reason}") from exc

    data = ttl_path.read_bytes()
    request = Request(
        statements_url,
        data=data,
        method="POST",
        headers={"Content-Type": "text/turtle"},
    )
    try:
        with urlopen(request, timeout=60) as response:
            print(f"Uploaded {ttl_path} to {repository}: HTTP {response.status}")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(
            f"Failed to upload TTL to {repository}: HTTP {exc.code} {exc.reason}\n{detail}"
        ) from exc
    except URLError as exc:
        raise SystemExit(f"Could not reach GraphDB at {base_url}: {exc.reason}") from exc


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload BDM DCAT catalog TTL to GraphDB.")
    parser.add_argument("--graphdb-url", default="http://localhost:7200", help="GraphDB base URL.")
    parser.add_argument("--repository", default="bdm-graph", help="GraphDB repository id.")
    parser.add_argument("--ttl", default="BDM_P2/metadata/data_product_catalog.ttl", help="Turtle file to upload.")
    parser.add_argument("--clear-first", action="store_true", help="Delete existing statements before upload.")
    args = parser.parse_args()

    ttl_path = Path(args.ttl)
    if not ttl_path.exists():
        raise SystemExit(f"TTL file not found: {ttl_path}. Run build_dcat_catalog.py first.")

    upload_ttl(args.graphdb_url, args.repository, ttl_path, args.clear_first)


if __name__ == "__main__":
    main()
