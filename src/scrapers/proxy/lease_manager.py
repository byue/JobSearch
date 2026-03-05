import uuid
from enum import Enum
from typing import Optional, Tuple
import redis


class LeaseState(Enum):
    AVAILABLE = "available"
    INUSE = "inuse"
    BLOCKED = "blocked"
    MISSING = "missing"


class LeaseManager:
    """
    Redis-backed lease manager used to coordinate proxy usage across many
    concurrent workers.

    State model
    -----------
    Each resource can be in one of three states:

        available  -> stored in Redis list:      lease:available
        in-use     -> stored as Redis key:       lease:inuse:{resource}
        blocked     -> stored as Redis key:       lease:blocked:{resource}

    - The available list acts as a queue of candidate IPs.
    - The in-use key represents an active lease and has a TTL so that
      leases automatically expire if a worker crashes or fails to release it.
    - The blocked key represents a temporarily blocked resource and also has a TTL
      so the resource can automatically recover after a cooldown period.

    Concurrency techniques
    ----------------------
    The implementation is designed to be safe under high concurrency with
    many workers leasing and returning IPs simultaneously.

    1. Lease ownership tokens
       When a resource is leased, a unique token is stored as the value of
       `lease:inuse:{resource}`. The caller receives this token and must present it
       when returning or blocking the resource.

       This prevents stale workers from accidentally releasing or blocking
       a lease that has already expired and been reassigned to another worker.

    2. Redis TTL-based leases
       The `lease:inuse:{resource}` key has a TTL. If a worker crashes or fails to
       return the resource, the lease automatically expires and the resource becomes
       eligible for reuse.

    3. Atomic state transitions with Lua
       Operations that require a read-modify-write sequence (such as verifying
       the ownership token before releasing a lease) are executed inside Lua
       scripts. Redis executes Lua scripts atomically, preventing race
       conditions between concurrent workers.

    4. SET NX for exclusive leasing
       Leasing uses the Redis pattern:

           SET key value NX EX ttl

       The NX flag ensures that only one worker can successfully create the
       in-use key for a given resource, guaranteeing mutual exclusion.

    Failure handling
    ----------------
    `release()` and `block()` may return False if the lease token no
    longer matches. This occurs when the lease expired or another worker
    has already acquired the resource. Consumers should treat these operations
    as best-effort cleanup and ignore failures.

    Inventory management
    --------------------
    This class manages lease state only. It assumes that another component
    is responsible for populating and maintaining the `lease:available` queue.
    """

    AVAILABLE_KEY = "lease:available"
    AVAILABLE_SET_KEY = "lease:available:members"
    INUSE_PREFIX = "lease:inuse"
    BLOCKED_PREFIX = "lease:blocked"

    ENQUEUE_REASON_ENQUEUED = "enqueued"
    ENQUEUE_REASON_CAPACITY = "capacity"
    ENQUEUE_REASON_BLOCKED = "blocked"
    ENQUEUE_REASON_INUSE = "inuse"
    ENQUEUE_REASON_DUPLICATE = "duplicate"
    ENQUEUE_REASON_INVALID_CAPACITY = "invalid_capacity"
    ENQUEUE_REASON_UNKNOWN = "unknown"

    @classmethod
    def normalize_scope(cls, scope: str) -> str:
        normalized = str(scope).strip().lower()
        if not normalized:
            raise ValueError("scope must be non-empty")
        return normalized

    @classmethod
    def available_key_for_scope(cls, scope: str) -> str:
        normalized = cls.normalize_scope(scope)
        if normalized == "default":
            return cls.AVAILABLE_KEY
        return f"{cls.AVAILABLE_KEY}:{normalized}"

    @classmethod
    def available_set_key_for_scope(cls, scope: str) -> str:
        normalized = cls.normalize_scope(scope)
        if normalized == "default":
            return cls.AVAILABLE_SET_KEY
        return f"{cls.AVAILABLE_SET_KEY}:{normalized}"

    @classmethod
    def inuse_prefix_for_scope(cls, scope: str) -> str:
        normalized = cls.normalize_scope(scope)
        if normalized == "default":
            return f"{cls.INUSE_PREFIX}:"
        return f"{cls.INUSE_PREFIX}:{normalized}:"

    @classmethod
    def blocked_prefix_for_scope(cls, scope: str) -> str:
        normalized = cls.normalize_scope(scope)
        if normalized == "default":
            return f"{cls.BLOCKED_PREFIX}:"
        return f"{cls.BLOCKED_PREFIX}:{normalized}:"

    _LUA_LEASE_RESOURCE = r"""
    -- KEYS[1] = lease:available
    -- KEYS[2] = lease:available:members
    -- ARGV[1] = inuse_prefix ("lease:inuse:")
    -- ARGV[2] = blocked_prefix ("lease:blocked:")
    -- ARGV[3] = token
    -- ARGV[4] = lease_ttl_seconds
    -- ARGV[5] = max_attempts

    local available = KEYS[1]
    local available_set = KEYS[2]
    local inuse_prefix = ARGV[1]
    local blocked_prefix = ARGV[2]
    local token = ARGV[3]
    local ttl = tonumber(ARGV[4])
    local max_attempts = tonumber(ARGV[5])

    for i = 1, max_attempts do
      local resource = redis.call("RPOP", available)
      if not resource then
        return nil
      end
      redis.call("SREM", available_set, resource)

      -- If blocked, drop this queue entry and try another candidate.
      if redis.call("EXISTS", blocked_prefix .. resource) == 1 then
        -- no-op
      else
        -- Try to claim lease
        local ok = redis.call("SET", inuse_prefix .. resource, token, "EX", ttl, "NX")
        if ok then
          return {resource, token}
        end
        -- Someone else already leased it (e.g., duplicate in queue); put it back.
        redis.call("LPUSH", available, resource)
        redis.call("SADD", available_set, resource)
      end
    end

    return nil
    """

    _LUA_RETURN_RESOURCE = r"""
    -- KEYS[1] = lease:available
    -- KEYS[2] = lease:available:members
    -- ARGV[1] = inuse_prefix
    -- ARGV[2] = resource
    -- ARGV[3] = token

    local available = KEYS[1]
    local available_set = KEYS[2]
    local inuse_key = ARGV[1] .. ARGV[2]
    local token = ARGV[3]

    local cur = redis.call("GET", inuse_key)
    if not cur or cur ~= token then
      return 0
    end

    local existing = redis.call("LPOS", available, ARGV[2])
    redis.call("DEL", inuse_key)
    if existing then
      redis.call("SADD", available_set, ARGV[2])
      return 1
    end
    local added = redis.call("SADD", available_set, ARGV[2])
    if added == 1 then
      redis.call("LPUSH", available, ARGV[2])
    end
    return 1
    """

    _LUA_BLOCK_RESOURCE = r"""
    -- KEYS[1] = lease:available
    -- KEYS[2] = lease:available:members
    -- ARGV[1] = inuse_prefix
    -- ARGV[2] = blocked_prefix
    -- ARGV[3] = resource
    -- ARGV[4] = token
    -- ARGV[5] = blocked_ttl_seconds

    local inuse_key = ARGV[1] .. ARGV[3]
    local blocked_key = ARGV[2] .. ARGV[3]
    local available_set = KEYS[2]
    local token = ARGV[4]
    local ttl = tonumber(ARGV[5])

    local cur = redis.call("GET", inuse_key)
    if not cur or cur ~= token then
      return 0
    end

    redis.call("LREM", KEYS[1], 0, ARGV[3])
    redis.call("SREM", available_set, ARGV[3])
    redis.call("DEL", inuse_key)
    redis.call("SET", blocked_key, "1", "EX", ttl)
    return 1
    """

    _LUA_TRY_ENQUEUE_RESOURCE = r"""
    -- KEYS[1] = lease:available
    -- KEYS[2] = lease:available:members
    -- ARGV[1] = inuse_prefix
    -- ARGV[2] = blocked_prefix
    -- ARGV[3] = resource
    -- ARGV[4] = capacity
    --
    -- Return codes:
    -- 1 => enqueued
    -- 0 => rejected: capacity
    -- -1 => rejected: blocked
    -- -2 => rejected: inuse
    -- -3 => rejected: duplicate
    -- -4 => rejected: invalid capacity

    local available = KEYS[1]
    local available_set = KEYS[2]
    local inuse_key = ARGV[1] .. ARGV[3]
    local blocked_key = ARGV[2] .. ARGV[3]
    local capacity = tonumber(ARGV[4])

    if not capacity or capacity <= 0 then
      return -4
    end
    if redis.call("SCARD", available_set) >= capacity then
      return 0
    end

    if redis.call("EXISTS", blocked_key) == 1 then
      return -1
    end
    if redis.call("EXISTS", inuse_key) == 1 then
      return -2
    end

    if redis.call("SISMEMBER", available_set, ARGV[3]) == 1 then
      return -3
    end
    local existing = redis.call("LPOS", available, ARGV[3])
    if existing then
      redis.call("SADD", available_set, ARGV[3])
      return -3
    end

    local added = redis.call("SADD", available_set, ARGV[3])
    if added == 0 then
      return -3
    end

    redis.call("LPUSH", available, ARGV[3])
    return 1
    """

    _LUA_GET_STATE = r"""
    -- KEYS[1] = lease:available
    -- ARGV[1] = inuse_prefix
    -- ARGV[2] = blocked_prefix
    -- ARGV[3] = resource
    --
    -- Return codes:
    -- 2 => INUSE
    -- 1 => BLOCKED
    -- 0 => AVAILABLE
    -- -1 => MISSING

    local inuse_key = ARGV[1] .. ARGV[3]
    if redis.call("EXISTS", inuse_key) == 1 then
      return 2
    end

    local blocked_key = ARGV[2] .. ARGV[3]
    if redis.call("EXISTS", blocked_key) == 1 then
      return 1
    end

    local pos = redis.call("LPOS", KEYS[1], ARGV[3])
    if pos then
      return 0
    end

    return -1
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        lease_ttl_seconds: int,
        blocked_ttl_seconds: int,
        max_attempts: int,
    ):
        self.redis = redis_client
        self.lease_ttl = int(lease_ttl_seconds)
        self.blocked_ttl = int(blocked_ttl_seconds)
        self.max_attempts = int(max_attempts)

        self._lua_lease = self.redis.register_script(self._LUA_LEASE_RESOURCE)
        self._lua_return = self.redis.register_script(self._LUA_RETURN_RESOURCE)
        self._lua_block = self.redis.register_script(self._LUA_BLOCK_RESOURCE)
        self._lua_try_enqueue = self.redis.register_script(self._LUA_TRY_ENQUEUE_RESOURCE)
        self._lua_get_state = self.redis.register_script(self._LUA_GET_STATE)

    def lease(self, scope: str) -> Optional[Tuple[str, str]]:
        """
        available -> in-use

        Returns (resource, token) or None.
        Token is required to later return/block safely.
        """
        normalized_scope = self.normalize_scope(scope)
        token = uuid.uuid4().hex
        res = self._lua_lease(
            keys=[
                self.available_key_for_scope(normalized_scope),
                self.available_set_key_for_scope(normalized_scope),
            ],
            args=[
                self.inuse_prefix_for_scope(normalized_scope),
                self.blocked_prefix_for_scope(normalized_scope),
                token,
                self.lease_ttl,
                self.max_attempts,
            ],
        )
        if not res:
            return None

        resource_b, tok_b = res[0], res[1]
        resource = resource_b.decode() if isinstance(resource_b, (bytes, bytearray)) else str(resource_b)
        tok = tok_b.decode() if isinstance(tok_b, (bytes, bytearray)) else str(tok_b)
        return resource, tok

    def release(self, resource: str, token: str, scope: str) -> bool:
        """
        in-use -> available (only if token matches current lease)
        Returns True if returned, False if not owner / already expired / already changed.
        """
        normalized_scope = self.normalize_scope(scope)
        res = self._lua_return(
            keys=[
                self.available_key_for_scope(normalized_scope),
                self.available_set_key_for_scope(normalized_scope),
            ],
            args=[self.inuse_prefix_for_scope(normalized_scope), resource, token],
        )
        return bool(res)

    def block(self, resource: str, token: str, scope: str) -> bool:
        """
        in-use -> blocked (only if token matches current lease)
        Returns True if blocked, False if not owner / already expired / already changed.
        """
        normalized_scope = self.normalize_scope(scope)
        res = self._lua_block(
            keys=[
                self.available_key_for_scope(normalized_scope),
                self.available_set_key_for_scope(normalized_scope),
            ],
            args=[
                self.inuse_prefix_for_scope(normalized_scope),
                self.blocked_prefix_for_scope(normalized_scope),
                resource,
                token,
                self.blocked_ttl,
            ],
        )
        return bool(res)

    def try_enqueue_with_reason(
        self,
        resource: str,
        capacity: int,
        scope: str,
    ) -> tuple[bool, str]:
        """
        Try to add a candidate resource to the available queue.

        Returns:
            (True, "enqueued") if accepted
            (False, reason) if rejected
        """
        normalized_scope = self.normalize_scope(scope)
        res = self._lua_try_enqueue(
            keys=[
                self.available_key_for_scope(normalized_scope),
                self.available_set_key_for_scope(normalized_scope),
            ],
            args=[
                self.inuse_prefix_for_scope(normalized_scope),
                self.blocked_prefix_for_scope(normalized_scope),
                resource,
                int(capacity),
            ],
        )
        code = int(res or 0)
        if code == 1:
            return True, self.ENQUEUE_REASON_ENQUEUED
        if code == 0:
            return False, self.ENQUEUE_REASON_CAPACITY
        if code == -1:
            return False, self.ENQUEUE_REASON_BLOCKED
        if code == -2:
            return False, self.ENQUEUE_REASON_INUSE
        if code == -3:
            return False, self.ENQUEUE_REASON_DUPLICATE
        if code == -4:
            return False, self.ENQUEUE_REASON_INVALID_CAPACITY
        return False, self.ENQUEUE_REASON_UNKNOWN

    def try_enqueue(self, resource: str, capacity: int, scope: str) -> bool:
        accepted, _reason = self.try_enqueue_with_reason(resource, capacity, scope=scope)
        return accepted

    def get_state(self, resource: str, scope: str) -> LeaseState:
        """Return current lease state for a resource."""
        normalized_scope = self.normalize_scope(scope)
        res = int(
            self._lua_get_state(
                keys=[self.available_key_for_scope(normalized_scope)],
                args=[
                    self.inuse_prefix_for_scope(normalized_scope),
                    self.blocked_prefix_for_scope(normalized_scope),
                    resource,
                ],
            )
            or 0
        )
        if res == 2:
            return LeaseState.INUSE
        if res == 1:
            return LeaseState.BLOCKED
        if res == -1:
            return LeaseState.MISSING
        return LeaseState.AVAILABLE

    def sizes(self, scope: str) -> dict:
        """
        Best-effort (not a snapshot) sizes:
        - available: LLEN(lease:available)
        - inuse: count of keys matching lease:inuse:*
        - blocked: count of keys matching lease:blocked:*

        Uses SCAN (non-blocking). Counts may change during the call under concurrency.
        """
        normalized_scope = self.normalize_scope(scope)
        available = int(self.redis.llen(self.available_key_for_scope(normalized_scope)))

        def _count_keys(match: str, batch: int = 500) -> int:
            total = 0
            cursor = 0
            while True:
                cursor, keys = self.redis.scan(cursor=cursor, match=match, count=batch)
                total += len(keys)
                if cursor == 0:
                    break
            return total

        inuse = _count_keys(self.inuse_prefix_for_scope(normalized_scope) + "*")
        blocked = _count_keys(self.blocked_prefix_for_scope(normalized_scope) + "*")

        return {"available": available, "inuse": inuse, "blocked": blocked}
