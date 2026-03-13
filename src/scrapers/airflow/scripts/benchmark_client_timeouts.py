"""Benchmark company clients across request/connect timeout grids.

Runs the same client path used by the Airflow DAG:
- builds clients via client_factory
- uses ProxyManagementClient leases for requests
- calls get_jobs(page=1) and get_job_details(first_job_id)

Outputs:
- detailed_attempts.csv: one row per endpoint attempt
- summary.csv: aggregated stats + top error causes
"""

from __future__ import annotations

import argparse
import csv
import os
import statistics
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scrapers.airflow.clients.client_factory import build_client
from common.request_policy import RequestPolicy
from scrapers.common.company_scopes import resolve_companies as resolve_companies_from_env
from scrapers.common.company_scopes import resolve_scopes as resolve_proxy_scopes_for_companies
from scrapers.proxy.proxy_management_client import ProxyManagementClient


@dataclass(frozen=True)
class TimeoutConfig:
    request_timeout_seconds: float
    connect_timeout_seconds: float


@dataclass(frozen=True)
class BenchmarkConfig:
    timeout: TimeoutConfig
    max_retries: int
    backoff_factor: float
    max_backoff_seconds: float
    jitter: bool


def _parse_env_file(path: Path) -> dict[str, str]:
    loaded: dict[str, str] = {}
    if not path.exists():
        return loaded
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("export "):
            stripped = stripped[len("export ") :].strip()
        if "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        if not key:
            continue
        loaded[key] = value.strip()
    return loaded


def _apply_env_defaults(env_from_file: dict[str, str]) -> None:
    for key, value in env_from_file.items():
        os.environ.setdefault(key, value)


def _require(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise RuntimeError(f"Missing required env var: {name}")
    return value.strip()


def _read_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    raise RuntimeError(f"Invalid bool for {name}: {raw!r}")


def _read_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    return float(raw.strip())


def _read_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    return int(raw.strip())


def _parse_csv_floats(raw: str) -> list[float]:
    out: list[float] = []
    for part in raw.split(","):
        value = float(part.strip())
        if value <= 0:
            raise ValueError(f"Timeout values must be > 0, got {value}")
        out.append(value)
    return out


def _exception_chain(exc: BaseException) -> str:
    parts: list[str] = []
    current: BaseException | None = exc
    visited: set[int] = set()
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        rendered = f"{type(current).__name__}: {current}"
        if rendered not in parts:
            parts.append(rendered)
        current = current.__cause__ if current.__cause__ is not None else current.__context__
    return " <- ".join(parts)


def _status_ok(value: Any) -> bool:
    if value is None:
        return True
    try:
        status = int(value)
    except (TypeError, ValueError):
        return False
    return 200 <= status < 300


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    rank = (len(ordered) - 1) * pct
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    weight = rank - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _wait_for_proxy_capacity(
    proxy_client: ProxyManagementClient,
    scopes: list[str],
    *,
    min_per_scope: int,
    timeout_seconds: float,
    poll_seconds: float,
) -> dict[str, int]:
    deadline = time.monotonic() + timeout_seconds
    latest: dict[str, int] = {}
    while time.monotonic() < deadline:
        latest = {scope: int(proxy_client.sizes(scope=scope).get("available", 0)) for scope in scopes}
        if all(available >= min_per_scope for available in latest.values()):
            return latest
        time.sleep(poll_seconds)
    raise RuntimeError(
        "Timed out waiting for proxy capacity; "
        f"required_per_scope={min_per_scope} latest={latest}"
    )


def _build_policy(config: BenchmarkConfig) -> RequestPolicy:
    return RequestPolicy(
        timeout_seconds=config.timeout.request_timeout_seconds,
        connect_timeout_seconds=config.timeout.connect_timeout_seconds,
        max_retries=config.max_retries,
        backoff_factor=config.backoff_factor,
        max_backoff_seconds=config.max_backoff_seconds,
        jitter=config.jitter,
    )


def _run_single_benchmark(
    *,
    company: str,
    policy: RequestPolicy,
    proxy_client: ProxyManagementClient,
    trial_index: int,
    config_id: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    client = build_client(company=company, proxy_management_client=proxy_client, default_request_policy=policy)

    started = time.perf_counter()
    first_job_id: str | None = None
    try:
        jobs_response = client.get_jobs(page=1)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        jobs = list(getattr(jobs_response, "jobs", []) or [])
        first_job = jobs[0] if jobs else None
        raw_id = getattr(first_job, "id", None) if first_job is not None else None
        first_job_id = str(raw_id).strip() if raw_id is not None else None
        if first_job_id == "":
            first_job_id = None
        status = getattr(jobs_response, "status", None)
        error_msg = getattr(jobs_response, "error", None)
        ok = _status_ok(status) and error_msg in (None, "")
        rows.append(
            {
                "config_id": config_id,
                "company": company,
                "trial": trial_index,
                "endpoint": "get_jobs",
                "success": ok,
                "status": status,
                "latency_ms": round(elapsed_ms, 2),
                "job_id": first_job_id,
                "error_cause": "" if ok else str(error_msg or "non-2xx or upstream error payload"),
            }
        )
    except Exception as exc:  # pragma: no cover - runtime diagnostics path
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        rows.append(
            {
                "config_id": config_id,
                "company": company,
                "trial": trial_index,
                "endpoint": "get_jobs",
                "success": False,
                "status": "",
                "latency_ms": round(elapsed_ms, 2),
                "job_id": "",
                "error_cause": _exception_chain(exc),
            }
        )

    if not first_job_id:
        rows.append(
            {
                "config_id": config_id,
                "company": company,
                "trial": trial_index,
                "endpoint": "get_job_details",
                "success": False,
                "status": "",
                "latency_ms": 0.0,
                "job_id": "",
                "error_cause": "Skipped: no job id from get_jobs(page=1)",
            }
        )
        return rows

    started = time.perf_counter()
    try:
        details_response = client.get_job_details(job_id=first_job_id)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        status = getattr(details_response, "status", None)
        error_msg = getattr(details_response, "error", None)
        ok = _status_ok(status) and error_msg in (None, "")
        rows.append(
            {
                "config_id": config_id,
                "company": company,
                "trial": trial_index,
                "endpoint": "get_job_details",
                "success": ok,
                "status": status,
                "latency_ms": round(elapsed_ms, 2),
                "job_id": first_job_id,
                "error_cause": "" if ok else str(error_msg or "non-2xx or upstream error payload"),
            }
        )
    except Exception as exc:  # pragma: no cover - runtime diagnostics path
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        rows.append(
            {
                "config_id": config_id,
                "company": company,
                "trial": trial_index,
                "endpoint": "get_job_details",
                "success": False,
                "status": "",
                "latency_ms": round(elapsed_ms, 2),
                "job_id": first_job_id,
                "error_cause": _exception_chain(exc),
            }
        )
    return rows


def _write_detailed_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "config_id",
        "company",
        "trial",
        "endpoint",
        "success",
        "status",
        "latency_ms",
        "job_id",
        "error_cause",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_summary_csv(path: Path, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["config_id"]), str(row["company"]), str(row["endpoint"]))].append(row)

    summary_rows: list[dict[str, Any]] = []
    for (config_id, company, endpoint), items in sorted(grouped.items()):
        total = len(items)
        successes = [item for item in items if bool(item["success"])]
        success_count = len(successes)
        success_rate = (success_count / total) * 100.0 if total else 0.0
        latencies_all = [float(item["latency_ms"]) for item in items]
        latencies_ok = [float(item["latency_ms"]) for item in successes]
        failures = [item for item in items if not bool(item["success"])]
        causes = Counter(str(item["error_cause"]).strip() for item in failures if str(item["error_cause"]).strip())
        top_causes = " | ".join(f"{msg} (x{count})" for msg, count in causes.most_common(3))

        summary_rows.append(
            {
                "config_id": config_id,
                "company": company,
                "endpoint": endpoint,
                "attempts": total,
                "successes": success_count,
                "failures": total - success_count,
                "success_rate_pct": round(success_rate, 2),
                "mean_latency_ms_all": round(statistics.fmean(latencies_all), 2) if latencies_all else "",
                "mean_latency_ms_success": round(statistics.fmean(latencies_ok), 2) if latencies_ok else "",
                "p50_latency_ms_success": round(_percentile(latencies_ok, 0.5) or 0.0, 2) if latencies_ok else "",
                "p95_latency_ms_success": round(_percentile(latencies_ok, 0.95) or 0.0, 2) if latencies_ok else "",
                "top_failure_causes": top_causes,
            }
        )

    fieldnames = [
        "config_id",
        "company",
        "endpoint",
        "attempts",
        "successes",
        "failures",
        "success_rate_pct",
        "mean_latency_ms_all",
        "mean_latency_ms_success",
        "p50_latency_ms_success",
        "p95_latency_ms_success",
        "top_failure_causes",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summary_rows)
    return summary_rows


def _rank_configs(summary_rows: list[dict[str, Any]]) -> list[tuple[str, float, float]]:
    by_config: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in summary_rows:
        by_config[str(row["config_id"])].append(row)

    ranked: list[tuple[str, float, float]] = []
    for config_id, items in by_config.items():
        success_rates = [float(item["success_rate_pct"]) for item in items]
        p95s = [float(item["p95_latency_ms_success"]) for item in items if item["p95_latency_ms_success"] != ""]
        avg_success = statistics.fmean(success_rates) if success_rates else 0.0
        avg_p95 = statistics.fmean(p95s) if p95s else float("inf")
        ranked.append((config_id, avg_success, avg_p95))

    ranked.sort(key=lambda item: (-item[1], item[2]))
    return ranked


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark timeout grid for company scraper clients.")
    parser.add_argument("--env-file", default="src/scrapers/airflow/docker.env")
    parser.add_argument("--output-dir", default="artifacts/airflow-timeout-benchmark")
    parser.add_argument("--companies", default=os.getenv("JOBSEARCH_AIRFLOW_COMPANIES", ""))
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--request-timeouts", default="10,15,20")
    parser.add_argument("--connect-timeouts", default="2,3,5")
    parser.add_argument("--max-retries", type=int, default=_read_int("JOBSEARCH_AIRFLOW_CLIENT_MAX_RETRIES", 3))
    parser.add_argument("--backoff-factor", type=float, default=_read_float("JOBSEARCH_AIRFLOW_CLIENT_BACKOFF_FACTOR", 0.5))
    parser.add_argument(
        "--max-backoff-seconds",
        type=float,
        default=_read_float("JOBSEARCH_AIRFLOW_CLIENT_MAX_BACKOFF_SECONDS", 6.0),
    )
    parser.add_argument("--jitter", action="store_true", default=_read_bool("JOBSEARCH_AIRFLOW_CLIENT_BACKOFF_JITTER", True))
    parser.add_argument("--no-jitter", action="store_false", dest="jitter")
    parser.add_argument("--proxy-api-url", default=os.getenv("JOBSEARCH_PROXY_API_URL", "http://localhost:8090"))
    parser.add_argument(
        "--proxy-api-timeout-seconds",
        type=float,
        default=_read_float("JOBSEARCH_PROXY_API_TIMEOUT_SECONDS", 10.0),
    )
    parser.add_argument(
        "--proxy-lease-acquire-timeout-seconds",
        type=float,
        default=_read_float("JOBSEARCH_PROXY_LEASE_ACQUIRE_TIMEOUT_SECONDS", 6.0),
    )
    parser.add_argument(
        "--proxy-lease-poll-interval-seconds",
        type=float,
        default=_read_float("JOBSEARCH_PROXY_LEASE_POLL_INTERVAL_SECONDS", 0.1),
    )
    parser.add_argument("--min-proxies-per-scope", type=int, default=5)
    parser.add_argument("--proxy-wait-timeout-seconds", type=float, default=900.0)
    parser.add_argument("--proxy-wait-poll-seconds", type=float, default=10.0)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    env_path = Path(args.env_file)
    _apply_env_defaults(_parse_env_file(env_path))

    companies = resolve_companies_from_env(args.companies or os.getenv("JOBSEARCH_AIRFLOW_COMPANIES"))
    scopes = resolve_proxy_scopes_for_companies(companies)

    if not companies:
        raise RuntimeError("No companies resolved for benchmark")
    if not scopes:
        raise RuntimeError("No proxy scopes resolved for companies")

    request_timeouts = _parse_csv_floats(args.request_timeouts)
    connect_timeouts = _parse_csv_floats(args.connect_timeouts)

    output_root = Path(args.output_dir)
    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = output_root / run_stamp
    run_dir.mkdir(parents=True, exist_ok=False)

    proxy_client = ProxyManagementClient(
        base_url=str(args.proxy_api_url),
        timeout_seconds=float(args.proxy_api_timeout_seconds),
        lease_acquire_timeout_seconds=float(args.proxy_lease_acquire_timeout_seconds),
        lease_poll_interval_seconds=float(args.proxy_lease_poll_interval_seconds),
    )

    available = _wait_for_proxy_capacity(
        proxy_client,
        scopes,
        min_per_scope=int(args.min_proxies_per_scope),
        timeout_seconds=float(args.proxy_wait_timeout_seconds),
        poll_seconds=float(args.proxy_wait_poll_seconds),
    )

    all_rows: list[dict[str, Any]] = []
    for connect_timeout in connect_timeouts:
        for request_timeout in request_timeouts:
            config = BenchmarkConfig(
                timeout=TimeoutConfig(
                    request_timeout_seconds=request_timeout,
                    connect_timeout_seconds=connect_timeout,
                ),
                max_retries=max(1, int(args.max_retries)),
                backoff_factor=float(args.backoff_factor),
                max_backoff_seconds=float(args.max_backoff_seconds),
                jitter=bool(args.jitter),
            )
            config_id = (
                f"connect={config.timeout.connect_timeout_seconds}s;"
                f"request={config.timeout.request_timeout_seconds}s;"
                f"retries={config.max_retries};"
                f"backoff={config.backoff_factor};"
                f"max_backoff={config.max_backoff_seconds};"
                f"jitter={str(config.jitter).lower()}"
            )
            policy = _build_policy(config)
            for company in companies:
                for trial in range(1, int(args.trials) + 1):
                    rows = _run_single_benchmark(
                        company=company,
                        policy=policy,
                        proxy_client=proxy_client,
                        trial_index=trial,
                        config_id=config_id,
                    )
                    all_rows.extend(rows)

    detailed_csv = run_dir / "detailed_attempts.csv"
    summary_csv = run_dir / "summary.csv"
    _write_detailed_csv(detailed_csv, all_rows)
    summary_rows = _write_summary_csv(summary_csv, all_rows)
    ranked = _rank_configs(summary_rows)

    print("Benchmark complete")
    print(f"Run dir: {run_dir}")
    print(f"Proxy availability at start: {available}")
    print(f"Detailed CSV: {detailed_csv}")
    print(f"Summary CSV: {summary_csv}")
    print("Top configs (avg success %, avg p95 success latency ms):")
    for config_id, success_rate, p95 in ranked[:5]:
        p95_text = f"{p95:.2f}" if p95 != float("inf") else "n/a"
        print(f"  {success_rate:.2f}% | p95={p95_text} | {config_id}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
