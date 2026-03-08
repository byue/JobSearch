"""Shared environment parsing utilities."""

from __future__ import annotations

import os


def require_env(name: str) -> str:
    raw = os.getenv(name)
    if raw is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    value = raw.strip()
    if not value:
        raise RuntimeError(f"Environment variable is empty: {name}")
    return value


def require_env_int(name: str, minimum: int = 1) -> int:
    value = require_env(name)
    try:
        parsed = int(value)
    except ValueError as exc:
        raise RuntimeError(f"Environment variable must be an int: {name}={value!r}") from exc
    if parsed < minimum:
        raise RuntimeError(f"Environment variable must be >= {minimum}: {name}={parsed}")
    return parsed


def require_env_float(name: str, minimum: float = 0.1) -> float:
    value = require_env(name)
    try:
        parsed = float(value)
    except ValueError as exc:
        raise RuntimeError(f"Environment variable must be a float: {name}={value!r}") from exc
    if parsed < minimum:
        raise RuntimeError(f"Environment variable must be >= {minimum}: {name}={parsed}")
    return parsed


def require_env_bool(name: str) -> bool:
    value = require_env(name).lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    raise RuntimeError(f"Environment variable must be a boolean: {name}={value!r}")


def env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        parsed = int(raw.strip())
    except ValueError:
        return default
    return max(parsed, minimum)


def env_float(name: str, default: float, minimum: float = 0.1) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        parsed = float(raw.strip())
    except ValueError:
        return default
    return max(parsed, minimum)


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    return default
