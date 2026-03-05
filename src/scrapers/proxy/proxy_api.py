"""Internal API for lease-manager operations."""

from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from redis import Redis

from scrapers.common.env import require_env, require_env_int
from scrapers.proxy.lease_manager import LeaseManager, LeaseState


class LeaseResponse(BaseModel):
    resource: str
    token: str


class LeaseRequest(BaseModel):
    scope: str


class LeaseResultResponse(BaseModel):
    ok: bool


class TryEnqueueRequest(BaseModel):
    resource: str
    capacity: int
    scope: str


class LeaseActionRequest(BaseModel):
    resource: str
    token: str
    scope: str


class StateResponse(BaseModel):
    state: str


redis_url = require_env("JOBSEARCH_PROXY_REDIS_URL")
lease_ttl_seconds = require_env_int("JOBSEARCH_PROXY_LEASE_TTL_SECONDS", minimum=1)
lease_max_attempts = require_env_int("JOBSEARCH_PROXY_LEASE_MAX_ATTEMPTS", minimum=1)
blocked_cooldown_seconds = require_env_int(
    "JOBSEARCH_PROXY_BLOCKED_COOLDOWN_SECONDS",
    minimum=1,
) if os.getenv("JOBSEARCH_PROXY_BLOCKED_COOLDOWN_SECONDS") else require_env_int(
    "JOBSEARCH_PROXY_DENY_COOLDOWN_SECONDS",
    minimum=1,
)

redis_client = Redis.from_url(redis_url, decode_responses=False)
lease_manager = LeaseManager(
    redis_client,
    lease_ttl_seconds=lease_ttl_seconds,
    blocked_ttl_seconds=blocked_cooldown_seconds,
    max_attempts=lease_max_attempts,
)

app = FastAPI(title="jobsearch-proxy-api", version="1.0.0")


@app.get("/health")
def health() -> dict[str, str]:
    redis_client.ping()
    return {"status": "ok"}


@app.get("/sizes")
def sizes(scope: str) -> dict[str, int]:
    return lease_manager.sizes(scope=scope)


@app.post("/lease", response_model=LeaseResponse | None)
def lease(payload: LeaseRequest) -> LeaseResponse | None:
    res = lease_manager.lease(scope=payload.scope)
    if res is None:
        return None
    return LeaseResponse(resource=res[0], token=res[1])


@app.post("/release", response_model=LeaseResultResponse)
def release(payload: LeaseActionRequest) -> LeaseResultResponse:
    return LeaseResultResponse(ok=lease_manager.release(payload.resource, payload.token, scope=payload.scope))


@app.post("/block", response_model=LeaseResultResponse)
def block(payload: LeaseActionRequest) -> LeaseResultResponse:
    return LeaseResultResponse(ok=lease_manager.block(payload.resource, payload.token, scope=payload.scope))


@app.post("/try-enqueue", response_model=LeaseResultResponse)
def try_enqueue(payload: TryEnqueueRequest) -> LeaseResultResponse:
    return LeaseResultResponse(ok=lease_manager.try_enqueue(payload.resource, payload.capacity, scope=payload.scope))


@app.get("/state", response_model=StateResponse)
def state(resource: str, scope: str) -> StateResponse:
    lease_state = lease_manager.get_state(resource, scope=scope)
    if lease_state == LeaseState.MISSING:
        raise HTTPException(status_code=404, detail="resource not found")
    return StateResponse(state=lease_state.name)
