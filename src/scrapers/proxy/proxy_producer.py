"""Continuously refill Redis proxy queue from in-repo proxy source."""

from __future__ import annotations

import logging
import os
import signal
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from redis import Redis

from scrapers.common.company_scopes import resolve_companies as resolve_companies_from_env
from scrapers.common.company_scopes import resolve_scopes as resolve_proxy_scopes_for_companies
from scrapers.common.env import env_float, env_int, require_env, require_env_int
from scrapers.proxy.lease_manager import LeaseManager
from scrapers.proxy.proxy_generator_client import ProxyGeneratorClient

LOGGER = logging.getLogger("jobsearch.proxy_producer")
logging.basicConfig(level=os.getenv("JOBSEARCH_PROXY_PRODUCER_LOG_LEVEL", "INFO"))

_STOP = False


@dataclass
class ScopeCounters:
    accepted: int = 0
    rejected_total: int = 0
    rejected_capacity: int = 0
    rejected_blocked: int = 0
    rejected_inuse: int = 0
    rejected_duplicate: int = 0
    rejected_invalid_capacity: int = 0
    rejected_unknown: int = 0


def _new_scope_counters(scopes: list[str]) -> dict[str, ScopeCounters]:
    return {scope: ScopeCounters() for scope in scopes}


def _log_scope_heartbeat(*, scope: str, snapshot: dict[str, int], counters: ScopeCounters) -> None:
    LOGGER.info(
        (
            "proxy_producer_scope_heartbeat scope=%s queue_size=%s inuse_count=%s blocked_count=%s "
            "accepted=%s enqueue_rejected=%s rejected_capacity=%s rejected_blocked=%s rejected_inuse=%s "
            "rejected_duplicate=%s rejected_invalid_capacity=%s rejected_unknown=%s"
        ),
        scope,
        snapshot["available"],
        snapshot["inuse"],
        snapshot["blocked"],
        counters.accepted,
        counters.rejected_total,
        counters.rejected_capacity,
        counters.rejected_blocked,
        counters.rejected_inuse,
        counters.rejected_duplicate,
        counters.rejected_invalid_capacity,
        counters.rejected_unknown,
    )


def _handle_signal(_signum: int, _frame: object) -> None:
    global _STOP
    _STOP = True


def main() -> int:
    redis_url = require_env("JOBSEARCH_PROXY_REDIS_URL")
    max_queue_size = env_int("JOBSEARCH_PROXY_QUEUE_MAX_SIZE", default=128, minimum=1)
    validate_timeout_seconds = env_float("JOBSEARCH_PROXY_VALIDATE_TIMEOUT_SECONDS", default=2.5, minimum=0.5)
    list_fetch_timeout_seconds = env_float("JOBSEARCH_PROXY_LIST_FETCH_TIMEOUT_SECONDS", default=15.0, minimum=0.5)
    validate_workers = env_int("JOBSEARCH_PROXY_VALIDATE_WORKERS", default=32, minimum=1)
    loop_sleep_seconds = env_float("JOBSEARCH_PROXY_PRODUCER_SLEEP_SECONDS", default=1.0, minimum=0.1)
    heartbeat_interval_seconds = env_float("JOBSEARCH_PROXY_PRODUCER_HEARTBEAT_SECONDS", default=15.0, minimum=1.0)
    lease_ttl_seconds = require_env_int("JOBSEARCH_PROXY_LEASE_TTL_SECONDS", minimum=1)
    lease_max_attempts = require_env_int("JOBSEARCH_PROXY_LEASE_MAX_ATTEMPTS", minimum=1)
    blocked_cooldown_seconds = require_env_int("JOBSEARCH_PROXY_BLOCKED_COOLDOWN_SECONDS", minimum=1)
    raw_scope_override = os.getenv("JOBSEARCH_PROXY_SCOPES")
    if raw_scope_override:
        scopes = [LeaseManager.normalize_scope(item) for item in raw_scope_override.split(",") if item.strip()]
    else:
        companies = resolve_companies_from_env(require_env("JOBSEARCH_AIRFLOW_COMPANIES"))
        scopes = [LeaseManager.normalize_scope(item) for item in resolve_proxy_scopes_for_companies(companies)]
    if not scopes:
        raise ValueError("resolved proxy scopes must contain at least one non-empty scope")
    scopes = list(dict.fromkeys(scopes))
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    redis_client = Redis.from_url(redis_url, decode_responses=False)
    lease_manager = LeaseManager(
        redis_client,
        lease_ttl_seconds=lease_ttl_seconds,
        blocked_ttl_seconds=blocked_cooldown_seconds,
        max_attempts=lease_max_attempts,
    )
    proxy_generator_client = ProxyGeneratorClient(
        validate_timeout_seconds=validate_timeout_seconds,
        list_fetch_timeout_seconds=list_fetch_timeout_seconds,
    )
    last_heartbeat = 0.0
    loops_since_heartbeat = 0
    fetched_since_heartbeat = 0
    validation_errors_since_heartbeat = 0
    invalid_since_heartbeat = 0
    enqueue_rejected_since_heartbeat = 0
    enqueue_rejected_capacity_since_heartbeat = 0
    enqueue_rejected_blocked_since_heartbeat = 0
    enqueue_rejected_inuse_since_heartbeat = 0
    enqueue_rejected_duplicate_since_heartbeat = 0
    enqueue_rejected_invalid_capacity_since_heartbeat = 0
    enqueue_rejected_unknown_since_heartbeat = 0
    accepted_since_heartbeat = 0
    per_scope_counters = _new_scope_counters(scopes)

    try:
        while not _STOP:
            try:
                loops_since_heartbeat += 1
                now = time.monotonic()
                if now - last_heartbeat >= heartbeat_interval_seconds:
                    scoped_snapshots = {scope: lease_manager.sizes(scope=scope) for scope in scopes}
                    snapshots = list(scoped_snapshots.values())
                    snapshot = {
                        "available": sum(int(item["available"]) for item in snapshots),
                        "inuse": sum(int(item["inuse"]) for item in snapshots),
                        "blocked": sum(int(item["blocked"]) for item in snapshots),
                    }
                    LOGGER.info(
                        (
                            "proxy_producer_heartbeat queue_size=%s inuse_count=%s blocked_count=%s "
                            "loops=%s fetched=%s accepted=%s invalid=%s "
                            "validation_errors=%s enqueue_rejected=%s "
                            "rejected_capacity=%s rejected_blocked=%s rejected_inuse=%s "
                            "rejected_duplicate=%s rejected_invalid_capacity=%s rejected_unknown=%s"
                        ),
                        snapshot["available"],
                        snapshot["inuse"],
                        snapshot["blocked"],
                        loops_since_heartbeat,
                        fetched_since_heartbeat,
                        accepted_since_heartbeat,
                        invalid_since_heartbeat,
                        validation_errors_since_heartbeat,
                        enqueue_rejected_since_heartbeat,
                        enqueue_rejected_capacity_since_heartbeat,
                        enqueue_rejected_blocked_since_heartbeat,
                        enqueue_rejected_inuse_since_heartbeat,
                        enqueue_rejected_duplicate_since_heartbeat,
                        enqueue_rejected_invalid_capacity_since_heartbeat,
                        enqueue_rejected_unknown_since_heartbeat,
                    )
                    for scope in scopes:
                        _log_scope_heartbeat(
                            scope=scope,
                            snapshot=scoped_snapshots[scope],
                            counters=per_scope_counters[scope],
                        )
                    last_heartbeat = now
                    loops_since_heartbeat = 0
                    fetched_since_heartbeat = 0
                    validation_errors_since_heartbeat = 0
                    invalid_since_heartbeat = 0
                    enqueue_rejected_since_heartbeat = 0
                    accepted_since_heartbeat = 0
                    enqueue_rejected_capacity_since_heartbeat = 0
                    enqueue_rejected_blocked_since_heartbeat = 0
                    enqueue_rejected_inuse_since_heartbeat = 0
                    enqueue_rejected_duplicate_since_heartbeat = 0
                    enqueue_rejected_invalid_capacity_since_heartbeat = 0
                    enqueue_rejected_unknown_since_heartbeat = 0
                    per_scope_counters = _new_scope_counters(scopes)

                candidates = proxy_generator_client.get_proxy_urls()
                fetched_since_heartbeat += len(candidates)
                if candidates:
                    max_workers = min(validate_workers, len(candidates))
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        future_to_proxy = {
                            executor.submit(proxy_generator_client.is_proxy_valid, proxy_url): proxy_url
                            for proxy_url in candidates
                        }

                        for future in as_completed(future_to_proxy):
                            proxy_url = future_to_proxy[future]

                            try:
                                is_valid = bool(future.result())
                            except Exception as exc:
                                validation_errors_since_heartbeat += 1
                                LOGGER.debug(
                                    "proxy_candidate result=rejected reason=validation-error proxy=%s error=%s",
                                    proxy_url,
                                    type(exc).__name__,
                                )
                                continue

                            if not is_valid:
                                invalid_since_heartbeat += 1
                                LOGGER.debug(
                                    "proxy_candidate result=rejected reason=invalid proxy=%s",
                                    proxy_url,
                                )
                                continue

                            for scope in scopes:
                                enqueue_result, enqueue_reason = lease_manager.try_enqueue_with_reason(
                                    proxy_url,
                                    max_queue_size,
                                    scope=scope,
                                )
                                if not enqueue_result:
                                    enqueue_rejected_since_heartbeat += 1
                                    per_scope_counters[scope].rejected_total += 1
                                    if enqueue_reason == LeaseManager.ENQUEUE_REASON_CAPACITY:
                                        enqueue_rejected_capacity_since_heartbeat += 1
                                        per_scope_counters[scope].rejected_capacity += 1
                                    elif enqueue_reason == LeaseManager.ENQUEUE_REASON_BLOCKED:
                                        enqueue_rejected_blocked_since_heartbeat += 1
                                        per_scope_counters[scope].rejected_blocked += 1
                                    elif enqueue_reason == LeaseManager.ENQUEUE_REASON_INUSE:
                                        enqueue_rejected_inuse_since_heartbeat += 1
                                        per_scope_counters[scope].rejected_inuse += 1
                                    elif enqueue_reason == LeaseManager.ENQUEUE_REASON_DUPLICATE:
                                        enqueue_rejected_duplicate_since_heartbeat += 1
                                        per_scope_counters[scope].rejected_duplicate += 1
                                    elif enqueue_reason == LeaseManager.ENQUEUE_REASON_INVALID_CAPACITY:
                                        enqueue_rejected_invalid_capacity_since_heartbeat += 1
                                        per_scope_counters[scope].rejected_invalid_capacity += 1
                                    else:
                                        enqueue_rejected_unknown_since_heartbeat += 1
                                        per_scope_counters[scope].rejected_unknown += 1
                                    LOGGER.debug(
                                        "proxy_candidate result=rejected reason=enqueue-%s proxy=%s scope=%s",
                                        enqueue_reason,
                                        proxy_url,
                                        scope,
                                    )
                                    continue

                                accepted_since_heartbeat += 1
                                per_scope_counters[scope].accepted += 1
                                LOGGER.debug(
                                    "proxy_candidate result=accepted proxy=%s scope=%s",
                                    proxy_url,
                                    scope,
                                )
            except Exception as exc:
                LOGGER.error("producer loop error: %s", exc)
            time.sleep(loop_sleep_seconds)
    finally:
        LOGGER.info("proxy producer exiting")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
