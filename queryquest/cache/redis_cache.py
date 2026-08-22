"""Read-through cache over Redis.

The contract: `get_or_load(key, loader)` returns the cached value if it
is there, otherwise calls `loader`, stores the result and returns it.
Everything interesting is in the failure modes around that one line.

  * Fail open. If Redis is unreachable, every call falls through to the
    loader and the application keeps serving. A cache that can take the
    site down is a liability, not an optimisation.

  * Single flight. On a miss, exactly one caller runs the loader; the
    rest wait briefly for it to publish. Without this, a popular key
    expiring at peak sends every concurrent request to the database at
    once -- the classic cache stampede, and the failure mode that
    actually takes systems down.

  * TTL jitter. Keys written together would otherwise expire together
    and re-create the stampede on a schedule. +/-20% spreads them.

  * Negative caching. A miss that resolves to "no such row" is cached
    too, for a short TTL. Otherwise a hot 404 is an uncacheable query
    that hits the database forever.

  * Versioned namespaces. Invalidating a whole entity class is an
    INCR on a version counter, not a KEYS scan -- KEYS is O(n) over the
    keyspace and blocks the server.
"""

from __future__ import annotations

import json
import logging
import random
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable

import redis

from ..config import settings

log = logging.getLogger("queryquest.cache")

_MISS_SENTINEL = "\x00__qq_null__"


def _default(obj: Any) -> Any:
    """Serialise what psycopg hands back: Decimal, datetime, date."""
    if isinstance(obj, Decimal):
        return {"__dec__": str(obj)}
    if isinstance(obj, (datetime, date)):
        return {"__dt__": obj.isoformat()}
    raise TypeError(f"not JSON serialisable: {type(obj).__name__}")


def _object_hook(d: dict) -> Any:
    if "__dec__" in d:
        return Decimal(d["__dec__"])
    if "__dt__" in d:
        return datetime.fromisoformat(d["__dt__"])
    return d


def dumps(value: Any) -> str:
    return json.dumps(value, default=_default, separators=(",", ":"))


def loads(raw: str) -> Any:
    return json.loads(raw, object_hook=_object_hook)


@dataclass
class CacheStats:
    hits: int = 0
    misses: int = 0
    errors: int = 0
    loads: int = 0
    waited: int = 0          # single-flight waits satisfied by another caller
    negative_hits: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def _bump(self, name: str) -> None:
        with self._lock:
            setattr(self, name, getattr(self, name) + 1)

    @property
    def total(self) -> int:
        return self.hits + self.misses

    @property
    def hit_ratio(self) -> float:
        return (self.hits / self.total) if self.total else 0.0

    def snapshot(self) -> dict:
        return {
            "hits": self.hits,
            "misses": self.misses,
            "errors": self.errors,
            "loads": self.loads,
            "waited": self.waited,
            "negative_hits": self.negative_hits,
            "total": self.total,
            "hit_ratio": round(self.hit_ratio, 5),
        }

    def reset(self) -> None:
        with self._lock:
            self.hits = self.misses = self.errors = 0
            self.loads = self.waited = self.negative_hits = 0


class Cache:
    def __init__(
        self,
        url: str | None = None,
        namespace: str | None = None,
        client: redis.Redis | None = None,
        enabled: bool | None = None,
        version_ttl_s: float = 1.0,
    ) -> None:
        self.namespace = namespace or settings.cache_namespace
        self.stats = CacheStats()
        self.version_ttl_s = version_ttl_s
        self._version_cache: dict[str, tuple[int, float]] = {}
        # `enabled=False` turns every call into a straight pass-through to
        # the loader. That is how the load test measures the uncached
        # baseline through the exact same code path.
        self._enabled = settings.cache_enabled if enabled is None else enabled
        if client is not None:
            self.client = client
        else:
            self.client = redis.Redis.from_url(
                url or settings.redis_url,
                decode_responses=True,
                socket_timeout=2,
                socket_connect_timeout=2,
                health_check_interval=30,
            )

    # -- key construction ------------------------------------------------
    def _version(self, entity: str) -> int:
        """Monotonic version per entity class; bumping it orphans every
        key in that class at once, in O(1).

        Memoised in process for `version_ttl_s`. Without that, building a
        key costs a Redis round trip, so every cache hit is two round
        trips instead of one -- which measurably *reduced* throughput in
        the first version of this code. The cost is bounded staleness:
        after an entity-wide invalidation, a process can serve the old
        version for up to version_ttl_s. That is the right trade for
        leaderboards and catalogue pages; anything needing immediate
        invalidation uses `invalidate()` on the exact key, which is not
        subject to this.
        """
        cached = self._version_cache.get(entity)
        now = time.monotonic()
        if cached is not None and now - cached[1] < self.version_ttl_s:
            return cached[0]

        try:
            raw = self.client.get(f"{self.namespace}:ver:{entity}")
            version = int(raw) if raw else 1
        except redis.RedisError:
            self.stats._bump("errors")
            version = cached[0] if cached else 1

        self._version_cache[entity] = (version, now)
        return version

    def key(self, entity: str, ident: Any, suffix: str = "") -> str:
        v = self._version(entity)
        tail = f":{suffix}" if suffix else ""
        return f"{self.namespace}:v{v}:{entity}:{ident}{tail}"

    def _ttl(self, base: int | None = None) -> int:
        base = base or settings.cache_ttl_s
        jitter = settings.cache_ttl_jitter
        if jitter <= 0:
            return base
        return max(1, int(base * (1 + random.uniform(-jitter, jitter))))

    # -- core ------------------------------------------------------------
    def get_or_load(
        self,
        key: str,
        loader: Callable[[], Any],
        ttl: int | None = None,
        negative_ttl: int = 30,
    ) -> Any:
        if not self._enabled:
            self.stats._bump("loads")
            return loader()

        try:
            raw = self.client.get(key)
        except redis.RedisError as exc:
            # Fail open: degraded latency beats an outage.
            self.stats._bump("errors")
            log.warning("cache read failed for %s (%s); falling through", key, exc)
            self.stats._bump("loads")
            return loader()

        if raw is not None:
            if raw == _MISS_SENTINEL:
                self.stats._bump("hits")
                self.stats._bump("negative_hits")
                return None
            self.stats._bump("hits")
            return loads(raw)

        self.stats._bump("misses")
        return self._load_single_flight(key, loader, ttl, negative_ttl)

    def _load_single_flight(
        self, key: str, loader: Callable[[], Any], ttl: int | None, negative_ttl: int
    ) -> Any:
        lock_key = f"{key}:lock"
        try:
            got_lock = bool(
                self.client.set(lock_key, "1", nx=True, ex=settings.cache_lock_timeout_s)
            )
        except redis.RedisError:
            self.stats._bump("errors")
            got_lock = True  # degrade to "everyone loads" rather than stalling

        if not got_lock:
            # Someone else is already loading. Wait briefly for them to
            # publish; if they are slow, load it ourselves rather than
            # queueing indefinitely behind an unknown transaction.
            deadline = time.monotonic() + 0.25
            while time.monotonic() < deadline:
                time.sleep(0.002)
                try:
                    raw = self.client.get(key)
                except redis.RedisError:
                    break
                if raw is not None:
                    self.stats._bump("waited")
                    return None if raw == _MISS_SENTINEL else loads(raw)

        self.stats._bump("loads")
        try:
            value = loader()
        finally:
            try:
                self.client.delete(lock_key)
            except redis.RedisError:
                self.stats._bump("errors")

        try:
            if value is None:
                self.client.set(key, _MISS_SENTINEL, ex=negative_ttl)
            else:
                self.client.set(key, dumps(value), ex=self._ttl(ttl))
        except redis.RedisError as exc:
            self.stats._bump("errors")
            log.warning("cache write failed for %s (%s)", key, exc)

        return value

    # -- invalidation ----------------------------------------------------
    def invalidate(self, key: str) -> None:
        """Delete one key. Used on write-through paths where the exact
        affected key is known."""
        try:
            self.client.delete(key)
        except redis.RedisError:
            self.stats._bump("errors")

    def invalidate_entity(self, entity: str) -> int:
        """Orphan every key of an entity class in O(1).

        Deliberately not `KEYS pattern` + delete: KEYS walks the entire
        keyspace and blocks the single-threaded server while it does.
        Bumping a version prefix leaves the old keys to expire on their
        own TTL and makes them unreachable immediately.
        """
        try:
            version = int(self.client.incr(f"{self.namespace}:ver:{entity}"))
        except redis.RedisError:
            self.stats._bump("errors")
            return 1
        # Drop the local memo immediately: the process that performed the
        # bump must not keep serving the version it just superseded.
        self._version_cache[entity] = (version, time.monotonic())
        return version

    def reset_version_cache(self) -> None:
        self._version_cache.clear()

    def flush_namespace(self) -> int:
        """Remove every key under this namespace. SCAN-based, so it does
        not block the server. For tests and operator use only."""
        removed = 0
        self._version_cache.clear()
        try:
            for k in self.client.scan_iter(match=f"{self.namespace}:*", count=500):
                self.client.delete(k)
                removed += 1
        except redis.RedisError:
            self.stats._bump("errors")
        return removed

    def ping(self) -> bool:
        try:
            return bool(self.client.ping())
        except redis.RedisError:
            return False


_CACHE: Cache | None = None


def get_cache() -> Cache:
    global _CACHE
    if _CACHE is None:
        _CACHE = Cache()
    return _CACHE
