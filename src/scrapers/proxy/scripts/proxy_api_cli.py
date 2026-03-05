#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

import requests


DEFAULT_API_URL = os.getenv("JOBSEARCH_PROXY_API_URL", "http://localhost:8090")
DEFAULT_TIMEOUT_SECONDS = float(os.getenv("JOBSEARCH_PROXY_API_TIMEOUT_SECONDS", "10"))


def _request(
    *,
    method: str,
    base_url: str,
    path: str,
    timeout_seconds: float,
    payload: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> Any:
    url = f"{base_url.rstrip('/')}{path}"
    response = requests.request(
        method=method,
        url=url,
        json=payload,
        params=params,
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    return response.json()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CLI for internal proxy-api endpoints")
    parser.add_argument("--api-url", default=DEFAULT_API_URL, help="Base URL for proxy-api")
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="HTTP timeout for requests",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("health", help="GET /health")
    sizes_parser = subparsers.add_parser("sizes", help="GET /sizes")
    sizes_parser.add_argument("--scope", required=True, help="Queue scope/domain")
    lease_parser = subparsers.add_parser("lease", help="POST /lease")
    lease_parser.add_argument("--scope", required=True, help="Queue scope/domain")

    release_parser = subparsers.add_parser("release", help="POST /release")
    release_parser.add_argument("--resource", required=True)
    release_parser.add_argument("--token", required=True)
    release_parser.add_argument("--scope", required=True, help="Queue scope/domain")

    block_parser = subparsers.add_parser("block", help="POST /block")
    block_parser.add_argument("--resource", required=True)
    block_parser.add_argument("--token", required=True)
    block_parser.add_argument("--scope", required=True, help="Queue scope/domain")

    try_enqueue_parser = subparsers.add_parser("try-enqueue", help="POST /try-enqueue")
    try_enqueue_parser.add_argument("--resource", required=True)
    try_enqueue_parser.add_argument("--capacity", required=True, type=int)
    try_enqueue_parser.add_argument("--scope", required=True, help="Queue scope/domain")

    state_parser = subparsers.add_parser("state", help="GET /state?resource=...")
    state_parser.add_argument("--resource", required=True)
    state_parser.add_argument("--scope", required=True, help="Queue scope/domain")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        result: Any
        if args.command == "health":
            result = _request(
                method="GET",
                base_url=args.api_url,
                path="/health",
                timeout_seconds=args.timeout_seconds,
            )
        elif args.command == "sizes":
            params = {"scope": args.scope}
            result = _request(
                method="GET",
                base_url=args.api_url,
                path="/sizes",
                timeout_seconds=args.timeout_seconds,
                params=params,
            )
        elif args.command == "lease":
            payload = {"scope": args.scope}
            result = _request(
                method="POST",
                base_url=args.api_url,
                path="/lease",
                timeout_seconds=args.timeout_seconds,
                payload=payload,
            )
        elif args.command == "release":
            payload: dict[str, Any] = {"resource": args.resource, "token": args.token, "scope": args.scope}
            result = _request(
                method="POST",
                base_url=args.api_url,
                path="/release",
                timeout_seconds=args.timeout_seconds,
                payload=payload,
            )
        elif args.command == "block":
            payload = {"resource": args.resource, "token": args.token, "scope": args.scope}
            result = _request(
                method="POST",
                base_url=args.api_url,
                path="/block",
                timeout_seconds=args.timeout_seconds,
                payload=payload,
            )
        elif args.command == "try-enqueue":
            payload = {"resource": args.resource, "capacity": args.capacity, "scope": args.scope}
            result = _request(
                method="POST",
                base_url=args.api_url,
                path="/try-enqueue",
                timeout_seconds=args.timeout_seconds,
                payload=payload,
            )
        elif args.command == "state":
            params = {"resource": args.resource, "scope": args.scope}
            result = _request(
                method="GET",
                base_url=args.api_url,
                path="/state",
                timeout_seconds=args.timeout_seconds,
                params=params,
            )
        else:
            parser.error(f"Unsupported command: {args.command}")
            return 2

        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "unknown"
        body = exc.response.text if exc.response is not None else str(exc)
        print(f"proxy api request failed: status={status} body={body}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"proxy api request failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
