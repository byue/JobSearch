# Proxy Subsystem

Provides shared proxy queueing/lease management for scraper clients.

## Components
- `proxy_producer.py`: continuously fetches/validates proxy candidates and enqueues them by scope.
- `proxy_api.py`: FastAPI service exposing lease lifecycle endpoints.
- `lease_manager.py`: Redis-backed queue/in-use/blocked state machine.
- `proxy_management_client.py`: client used by scraper requests to lease/release/block proxies.
- `scripts/proxy_api_cli.py`: CLI for proxy API operations.

## Scopes (Core Concept)
- A **scope** is the queue namespace for a target site/domain (for example: `www.amazon.jobs`, `apply.careers.microsoft.com`).
- Proxy state is tracked **per scope** in Redis (`available`, `inuse`, `blocked` are scope-specific).

Why this is needed:
- Blocking is domain-specific: a proxy blocked by one site may still work on another site.
- Isolation: one noisy domain should not drain or poison another domain’s queue.
- Better refill diagnostics: rejection reasons and queue sizes are meaningful only when segmented by target domain.
- Safer retry behavior: lease/release/block decisions are tied to the exact destination site.

## API Endpoints
- `GET /health`
- `GET /sizes?scope=<domain>`
- `POST /lease`
- `POST /release`
- `POST /block`
- `POST /try-enqueue`
- `GET /state?resource=<proxy>&scope=<domain>`

## Key Environment Variables
- `JOBSEARCH_PROXY_REDIS_URL`
- `JOBSEARCH_PROXY_SCOPES`
- `JOBSEARCH_PROXY_QUEUE_MAX_SIZE`
- `JOBSEARCH_PROXY_LEASE_TTL_SECONDS`
- `JOBSEARCH_PROXY_LEASE_MAX_ATTEMPTS`
- `JOBSEARCH_PROXY_BLOCKED_COOLDOWN_SECONDS`
- `JOBSEARCH_PROXY_API_URL`
- `JOBSEARCH_PROXY_API_TIMEOUT_SECONDS`
- `JOBSEARCH_PROXY_PRODUCER_SLEEP_SECONDS`
- `JOBSEARCH_PROXY_PRODUCER_HEARTBEAT_SECONDS`

Primary env file in local stack:
- `src/scrapers/airflow/docker.env`

## Run (Unified Docker Stack)
```bash
make up
make logs SERVICE=proxy-producer
make logs SERVICE=proxy-api
```

## Useful Commands

Proxy queue snapshot by scopes:
```bash
make proxy-state
```

Proxy API CLI examples:
```bash
PYTHONPATH=src .venv/bin/python src/scrapers/proxy/scripts/proxy_api_cli.py health
PYTHONPATH=src .venv/bin/python src/scrapers/proxy/scripts/proxy_api_cli.py sizes --scope www.amazon.jobs
PYTHONPATH=src .venv/bin/python src/scrapers/proxy/scripts/proxy_api_cli.py lease --scope www.amazon.jobs
```

## Notes
- Scopes are required; queues are segmented by scope/domain.
- Blocked proxies are cooled down before becoming leaseable again.
- Producer heartbeat logs include acceptance/rejection reason buckets to diagnose refill behavior.

## Scraping Technique Summary

### IP Rotation
- Enabled via proxy lease lifecycle:
  - scraper requests `lease(scope)`
  - uses leased proxy for request attempt
  - reports result via `release` (success/retryable) or `block` (bad proxy for cooldown)
- Rotation happens across attempts because each retry can lease a different proxy.
- Scope segmentation prevents one domain from consuming or poisoning another domain’s pool.

### TLS Fingerprinting
- Requests use `curl_cffi` in client request paths.
- This gives browser-like TLS/HTTP behavior (better than plain `requests` for bot defense surfaces).
- If client code varies the `impersonate` browser profile across attempts, TLS/browser fingerprints vary accordingly.

### User-Agent Rotation
- With `curl_cffi`, changing `impersonate` profile also changes browser-like headers (including UA-related headers).
- So UA/header variation is profile-driven.
- Rotation policy still belongs in client code: if impersonation profile stays fixed, headers/UA stay effectively fixed.

## Redis Lease Tracking Model

Per scope/domain, the lease manager tracks three logical states:
- `available`: proxy can be leased now.
- `inuse`: proxy currently leased (token + TTL).
- `blocked`: proxy cooled down after block-worthy failure.

Lifecycle:
1. `lease(scope)`:
   - pop one resource from `available`
   - mark resource as `inuse` with lease token and TTL
2. request runs with that proxy
3. terminal action:
   - `release(resource, token, scope)` -> removes `inuse` lock, resource can return to pool
   - `block(resource, token, scope)` -> moves resource to `blocked` with cooldown TTL

Producer interaction:
- Producer calls `try-enqueue(resource, capacity, scope)`.
- Enqueue can be rejected when resource is:
  - already blocked
  - currently in-use
  - duplicate/already tracked
  - over scope capacity

TTL behavior:
- Lease TTL avoids permanent `inuse` leaks.
- Blocked TTL prevents immediate reuse of failing proxies.
- After TTL expiry and re-introduction by producer, resource can become `available` again.

Observed metrics:
- `sizes(scope)` exposes `available`, `inuse`, `blocked`.
- Producer heartbeat logs:
  - throughput: `fetched`, `accepted`, `invalid`
  - enqueue rejects: `enqueue_rejected`
  - reason buckets: `rejected_blocked`, `rejected_inuse`, `rejected_duplicate`, `rejected_capacity`, etc.
