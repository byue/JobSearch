#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import requests

ROOT_SRC = Path(__file__).resolve().parents[2]
if str(ROOT_SRC) not in sys.path:
    sys.path.insert(0, str(ROOT_SRC))


DEFAULT_API_URL = os.getenv("JOBSEARCH_FEATURES_API_URL", "http://localhost:8010")
DEFAULT_TIMEOUT_SECONDS = float(os.getenv("JOBSEARCH_FEATURES_API_TIMEOUT_SECONDS", "10"))
DEFAULT_CONNECT_TIMEOUT_SECONDS = float(os.getenv("JOBSEARCH_FEATURES_API_CONNECT_TIMEOUT_SECONDS", "2"))
DEFAULT_MAX_RETRIES = int(os.getenv("JOBSEARCH_FEATURES_API_MAX_RETRIES", "1"))
DEFAULT_BACKOFF_FACTOR = float(os.getenv("JOBSEARCH_FEATURES_API_BACKOFF_FACTOR", "0.5"))
DEFAULT_MAX_BACKOFF_SECONDS = float(os.getenv("JOBSEARCH_FEATURES_API_MAX_BACKOFF_SECONDS", "6"))
DEFAULT_JITTER = os.getenv("JOBSEARCH_FEATURES_API_JITTER", "false").strip().lower() in {"1", "true", "yes", "on"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CLI for the features service")
    parser.add_argument("--api-url", default=DEFAULT_API_URL, help="Base URL for features service")
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="HTTP read timeout",
    )
    parser.add_argument(
        "--connect-timeout-seconds",
        type=float,
        default=DEFAULT_CONNECT_TIMEOUT_SECONDS,
        help="HTTP connect timeout",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help="Maximum request attempts",
    )
    parser.add_argument(
        "--backoff-factor",
        type=float,
        default=DEFAULT_BACKOFF_FACTOR,
        help="Exponential backoff factor",
    )
    parser.add_argument(
        "--max-backoff-seconds",
        type=float,
        default=DEFAULT_MAX_BACKOFF_SECONDS,
        help="Maximum exponential backoff delay",
    )
    parser.add_argument(
        "--jitter",
        action="store_true",
        default=DEFAULT_JITTER,
        help="Enable backoff jitter",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    get_job_skills_parser = subparsers.add_parser("get-job-skills", help="POST /job_skills")
    get_job_skills_parser.add_argument("--text", required=True, help="Input text to extract skills from")

    normalize_locations_parser = subparsers.add_parser("normalize-locations", help="POST /normalize_locations")
    normalize_locations_parser.add_argument(
        "--location",
        dest="locations",
        action="append",
        required=True,
        help="Raw location string. Pass multiple times for multiple inputs.",
    )

    return parser


def main() -> int:
    from common.request_policy import RequestPolicy
    from features.client import FeaturesClient

    parser = build_parser()
    args = parser.parse_args()

    client = FeaturesClient(
        base_url=args.api_url,
        request_policy=RequestPolicy(
            timeout_seconds=float(args.timeout_seconds),
            connect_timeout_seconds=float(args.connect_timeout_seconds),
            max_retries=max(1, int(args.max_retries)),
            backoff_factor=float(args.backoff_factor),
            max_backoff_seconds=float(args.max_backoff_seconds),
            jitter=bool(args.jitter),
        ),
    )

    try:
        if args.command == "get-job-skills":
            result = client.get_job_skills(text=args.text)
        elif args.command == "normalize-locations":
            result = client.normalize_locations(locations=list(args.locations))
        else:  # pragma: no cover - argparse enforces valid subcommands
            parser.error(f"Unsupported command: {args.command}")
            return 2

        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "unknown"
        body = exc.response.text if exc.response is not None else str(exc)
        print(f"features api request failed: status={status} body={body}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"features api request failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
