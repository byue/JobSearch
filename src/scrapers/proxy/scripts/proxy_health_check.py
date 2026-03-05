#!/usr/bin/env python3
from __future__ import annotations

import os
import sys

from redis import Redis

REDIS_URL = os.getenv("JOBSEARCH_PROXY_REDIS_URL", "redis://localhost:6379/0")
QUEUE_KEY = "lease:available"
INUSE_PREFIX = "lease:inuse*"
BLOCKED_PREFIX = "lease:blocked*"


def _count_keys(client: Redis, pattern: str) -> int:
    total = 0
    cursor = 0
    while True:
        cursor, keys = client.scan(cursor=cursor, match=pattern, count=500)
        total += len(keys)
        if cursor == 0:
            break
    return total


def main() -> int:
    try:
        client = Redis.from_url(REDIS_URL, decode_responses=True)
        available = int(client.llen(QUEUE_KEY) or 0)
        inuse = _count_keys(client, INUSE_PREFIX)
        blocked = _count_keys(client, BLOCKED_PREFIX)
        samples = client.lrange(QUEUE_KEY, 0, 4)

        print(f"redis_url={REDIS_URL}")
        print(f"available={available}")
        print(f"inuse={inuse}")
        print(f"blocked={blocked}")
        print("sample_available=")
        for value in samples:
            print(f"- {value}")
        return 0
    except Exception as exc:
        print(f"proxy health check failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
