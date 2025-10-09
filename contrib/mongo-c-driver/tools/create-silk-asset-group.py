#!/usr/bin/env python

"""
Reusable utility script for creating an asset group in Silk. Invoke this script
with "--help" for details
"""

from __future__ import annotations

import argparse
import http
import http.client
import json
import os
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Protocol, Sequence, cast

JSON_MIME = "application/json"
JSON_HEADERS = {"Accept": JSON_MIME, "Content-Type": JSON_MIME}


class CommandParams(Protocol):
    silk_endpoint: str
    silk_client_id: None | str
    silk_client_secret: None | str
    exist_ok: bool
    project: str
    code_repo_url: str
    branch: str
    asset_id: str | None
    sbom_lite_path: str


def parse_argv(argv: Sequence[str]) -> CommandParams:
    parser = argparse.ArgumentParser(
        description="""
        This script creates an asset group in Silk using the Silk HTTP API. This
        script requires credentials to access Silk.
        """,
        epilog="""
        The --silk-client-id and --silk-client-secret can be passed via their
        associated environment variables (recommended). If credentials are
        not provided, then this script will immediately fail with an error.
        """,
        allow_abbrev=False,
    )
    parser.add_argument(
        "--project",
        required=True,
        metavar="<name>",
        help="The name of the project that owns the asset group",
    )
    parser.add_argument(
        "--branch",
        metavar="<branch>",
        required=True,
        help="The name of the project's branch",
    )
    parser.add_argument(
        "--code-repo-url",
        required=True,
        metavar="<url>",
        help="The repository URL where the project can be found",
    )
    parser.add_argument(
        "--asset-id",
        metavar="<name>",
        help='The asset ID to be created (Default is "<project>-<branch>")',
    )
    parser.add_argument(
        "--sbom-lite-path",
        required=True,
        metavar="<path>",
        help="Relative file path within the project respository on the specified branch where the CycloneDX SBOM-Lite can be found",
    )
    parser.add_argument(
        "--exist-ok",
        action="store_true",
        help="If specified, do not generate an error if the asset group already exists",
    )
    parser.add_argument(
        "--silk-client-id",
        default=os.environ.get("SILK_CLIENT_ID"),
        metavar="<id>",
        help="The Silk API client ID (env: SILK_CLIENT_ID)",
    )
    parser.add_argument(
        "--silk-client-secret",
        default=os.environ.get("SILK_CLIENT_SECRET"),
        help="The Silk API secret access token (env: SILK_CLIENT_SECRET)",
        metavar="<secret>",
    )
    parser.add_argument(
        "--silk-endpoint",
        metavar="<url>",
        help="The base API URL for Silk access",
        default="https://silkapi.us1.app.silk.security/api/v1",
    )
    return cast(CommandParams, parser.parse_args(argv))


@dataclass
class SimpleSilkClient:
    endpoint: str
    """The base API endpoint for Silk access"""
    auth_token: str
    """The Authorization token that can be used to act on the Silk server"""

    @classmethod
    def open(cls, api_ep: str, client_id: str, secret: str) -> SimpleSilkClient:
        """
        Open a new authenticated client against the Silk server.
        """
        auth_ep = f"{api_ep}/authenticate"
        req = urllib.request.Request(
            auth_ep,
            method="POST",
            headers=JSON_HEADERS,
            data=json.dumps(
                {
                    "client_id": client_id,
                    "client_secret": secret,
                }
            ).encode(),
        )
        resp: http.client.HTTPResponse
        try:
            with urllib.request.urlopen(req) as resp:
                resp_json = json.loads(resp.read())
        except urllib.error.HTTPError as err:
            raise RuntimeError(
                f"Attempting to authenticate with Silk failed (see context)"
            ) from err
        match resp_json:
            case {"token": str(tk)}:
                return SimpleSilkClient(api_ep, tk)
            case _:
                assert False, f"Abnormal authentication response from Silk {resp_json=}"

    def create_asset_group(
        self,
        *,
        branch: str,
        asset_id: str,
        sbom_lite_path: str,
        code_repo_url: str,
        project: str,
        exist_ok: bool,
    ) -> None:
        """
        Create a new Silk asset group with the given parameters.
        """
        req = urllib.request.Request(
            f"{self.endpoint}/raw/asset_group",
            data=json.dumps(
                {
                    "active": True,
                    "name": project,
                    "code_repo_url": code_repo_url,
                    "branch": branch,
                    "file_path": [],
                    "metadata": {
                        "sbom_lite_path": sbom_lite_path,
                    },
                    "asset_id": asset_id,
                }
            ).encode(),
            headers=JSON_HEADERS | {"Authorization": self.auth_token},
        )
        try:
            with urllib.request.urlopen(req) as resp:
                json.loads(resp.read())  # No-op, but validates that JSON was returned
        except urllib.error.HTTPError as err:
            if err.status == 409 and exist_ok:
                # 409: Conflict
                print(f"No-op: Asset group {asset_id!r} already exists")
                return
            raise RuntimeError(
                f"Creating Silk asset group {asset_id!r} failed (see context)"
            ) from err
        else:
            print(f"Asset group {asset_id!r} was created for {project}#{branch}")


def main(argv: Sequence[str]) -> int:
    args = parse_argv(argv)
    # Enforce that we have a client ID and secret
    cid = args.silk_client_id
    if cid is None:
        raise ValueError("A --silk-client-id/$SILK_CLIENT_ID is required")
    secret = args.silk_client_secret
    if secret is None:
        raise ValueError("A --silk-client-secret/$SILK_CLIENT_SECRET is required")

    cl = SimpleSilkClient.open(args.silk_endpoint, cid, secret)
    cl.create_asset_group(
        branch=args.branch,
        asset_id=args.asset_id or f"{args.project}-{args.branch}",
        sbom_lite_path=args.sbom_lite_path,
        code_repo_url=args.code_repo_url,
        project=args.project,
        exist_ok=args.exist_ok,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
