from __future__ import annotations

import atexit
import threading

from testcontainers.redis import RedisContainer

_LOCK = threading.Lock()
_CONTAINER: RedisContainer | None = None
_REDIS_URL: str | None = None


def get_shared_redis_url() -> str:
    global _CONTAINER, _REDIS_URL
    with _LOCK:
        if _REDIS_URL is not None:
            return _REDIS_URL

        container = RedisContainer("redis:7.2-alpine")
        container.start()
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        _CONTAINER = container
        _REDIS_URL = f"redis://{host}:{port}/0"
        return _REDIS_URL


def stop_shared_redis_container() -> None:
    global _CONTAINER, _REDIS_URL
    with _LOCK:
        if _CONTAINER is not None:
            _CONTAINER.stop()
        _CONTAINER = None
        _REDIS_URL = None


atexit.register(stop_shared_redis_container)
