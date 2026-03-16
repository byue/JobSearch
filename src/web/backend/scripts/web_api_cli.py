#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

import requests


DEFAULT_API_URL = os.getenv("JOBSEARCH_WEB_API_URL", "http://localhost:8000")
DEFAULT_TIMEOUT_SECONDS = float(os.getenv("JOBSEARCH_WEB_API_TIMEOUT_SECONDS", "10"))


def _request(
    *,
    method: str,
    base_url: str,
    path: str,
    timeout_seconds: float,
    payload: dict[str, Any] | None = None,
) -> Any:
    url = f"{base_url.rstrip('/')}{path}"
    response = requests.request(
        method=method,
        url=url,
        json=payload,
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    return response.json()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CLI for web backend job endpoints")
    parser.add_argument("--api-url", default=DEFAULT_API_URL, help="Base URL for web backend")
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="HTTP timeout for requests",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("get-companies", help="GET /get_companies")

    get_jobs_parser = subparsers.add_parser("get-jobs", help="POST /get_jobs")
    get_jobs_parser.add_argument("--company", required=True)
    get_jobs_parser.add_argument("--page", type=int, default=1, help="Pagination index (>=1)")
    get_jobs_parser.add_argument("--query", default=None, help="Optional search query")
    get_jobs_parser.add_argument("--posted-within", default=None, help="Optional recency window, e.g. 24h, 7d, 30d")
    get_jobs_parser.add_argument("--job-type", default=None, help="Optional job type filter")
    get_jobs_parser.add_argument("--country", default=None, help="Optional country filter")
    get_jobs_parser.add_argument("--region", default=None, help="Optional region filter")
    get_jobs_parser.add_argument("--city", default=None, help="Optional city filter")

    get_location_filters_parser = subparsers.add_parser("get-location-filters", help="GET /get_location_filters")
    get_location_filters_parser.add_argument("--company", default=None)
    get_location_filters_parser.add_argument("--posted-within", default=None)
    get_location_filters_parser.add_argument("--job-type", default=None)
    get_location_filters_parser.add_argument("--country", default=None)
    get_location_filters_parser.add_argument("--region", default=None)

    get_details_parser = subparsers.add_parser("get-job-details", help="POST /get_job_details")
    get_details_parser.add_argument("--company", required=True)
    get_details_parser.add_argument("--job-id", required=True)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        result: Any
        if args.command == "get-companies":
            result = _request(
                method="GET",
                base_url=args.api_url,
                path="/get_companies",
                timeout_seconds=args.timeout_seconds,
            )
        elif args.command == "get-jobs":
            page = max(1, int(args.page))
            payload = {
                "company": args.company,
                "query": args.query,
                "posted_within": args.posted_within,
                "job_type": args.job_type,
                "country": args.country,
                "region": args.region,
                "city": args.city,
                "pagination_index": page,
            }
            result = _request(
                method="POST",
                base_url=args.api_url,
                path="/get_jobs",
                timeout_seconds=args.timeout_seconds,
                payload=payload,
            )
        elif args.command == "get-location-filters":
            query_params: list[tuple[str, str]] = []
            for key, value in (
                ("company", args.company),
                ("posted_within", args.posted_within),
                ("job_type", args.job_type),
                ("country", args.country),
                ("region", args.region),
            ):
                if value is not None:
                    query_params.append((key, value))
            path = "/get_location_filters"
            if query_params:
                from urllib.parse import urlencode

                path = f"{path}?{urlencode(query_params)}"
            result = _request(
                method="GET",
                base_url=args.api_url,
                path=path,
                timeout_seconds=args.timeout_seconds,
            )
        elif args.command == "get-job-details":
            payload = {
                "company": args.company,
                "job_id": args.job_id,
            }
            result = _request(
                method="POST",
                base_url=args.api_url,
                path="/get_job_details",
                timeout_seconds=args.timeout_seconds,
                payload=payload,
            )
        else:  # pragma: no cover - argparse enforces valid subcommands
            parser.error(f"Unsupported command: {args.command}")
            return 2

        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "unknown"
        body = exc.response.text if exc.response is not None else str(exc)
        print(f"web api request failed: status={status} body={body}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"web api request failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
