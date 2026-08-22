"""Cache behaviour.

Correctness first (a cache that serves wrong data is worse than no
cache), then the properties that keep it from becoming a liability:
failing open when Redis is down, collapsing concurrent misses into one
load, and invalidating without an O(keyspace) scan.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from decimal import Decimal

import pytest
import redis

from queryquest.cache.redis_cache import Cache, dumps, loads


def test_round_trip_preserves_decimal_and_datetime():
    """psycopg returns Decimal and datetime. JSON has neither, and
    silently degrading money to float is how rounding bugs get into
    revenue reports."""
    value = [{"amount": Decimal("19.99"), "at": datetime(2026, 3, 4, 5, 6, tzinfo=timezone.utc)}]
    back = loads(dumps(value))
    assert back[0]["amount"] == Decimal("19.99")
    assert isinstance(back[0]["amount"], Decimal)
    assert back[0]["at"] == value[0]["at"]


def test_miss_then_hit(cache):
    calls = {"n": 0}

    def load():
        calls["n"] += 1
        return {"value": 42}

    key = cache.key("thing", 1)
    assert cache.get_or_load(key, load) == {"value": 42}
    assert cache.get_or_load(key, load) == {"value": 42}

    assert calls["n"] == 1
    assert cache.stats.hits == 1
    assert cache.stats.misses == 1


def test_none_is_cached_negatively(cache):
    """A hot lookup for a row that does not exist must not query the
    database forever."""
    calls = {"n": 0}

    def load():
        calls["n"] += 1
        return None

    key = cache.key("missing", 999)
    assert cache.get_or_load(key, load, negative_ttl=30) is None
    assert cache.get_or_load(key, load, negative_ttl=30) is None

    assert calls["n"] == 1
    assert cache.stats.negative_hits == 1


def test_invalidate_single_key(cache):
    calls = {"n": 0}

    def load():
        calls["n"] += 1
        return calls["n"]

    key = cache.key("thing", 7)
    assert cache.get_or_load(key, load) == 1
    cache.invalidate(key)
    assert cache.get_or_load(key, load) == 2


def test_entity_invalidation_is_one_operation(cache):
    """Bumping a version prefix orphans every key of a class in O(1).

    The alternative -- KEYS pattern then delete -- walks the whole
    keyspace and blocks Redis, which is single threaded, while it does.
    """
    calls = {"n": 0}

    def load():
        calls["n"] += 1
        return calls["n"]

    keys_before = [cache.key("leaderboard", i) for i in range(5)]
    for k in keys_before:
        cache.get_or_load(k, load)
    assert calls["n"] == 5

    cache.invalidate_entity("leaderboard")

    keys_after = [cache.key("leaderboard", i) for i in range(5)]
    assert keys_after != keys_before, "the version prefix did not change"
    for k in keys_after:
        cache.get_or_load(k, load)
    assert calls["n"] == 10


def test_single_flight_collapses_a_stampede(cache):
    """20 threads miss the same key at once. The loader must run once.

    Without this, a popular key expiring at peak sends the entire
    concurrent request set to the database simultaneously -- the failure
    mode that actually takes systems down.
    """
    loads_started = {"n": 0}
    lock = threading.Lock()

    def slow_load():
        with lock:
            loads_started["n"] += 1
        time.sleep(0.15)
        return {"expensive": True}

    key = cache.key("hot", "stampede")
    start = threading.Barrier(20)

    def call():
        start.wait()
        return cache.get_or_load(key, slow_load)

    with ThreadPoolExecutor(max_workers=20) as pool:
        results = [f.result() for f in as_completed([pool.submit(call) for _ in range(20)])]

    assert all(r == {"expensive": True} for r in results)
    assert loads_started["n"] <= 2, (
        f"{loads_started['n']} concurrent loads ran -- single flight is not holding"
    )


def test_ttl_jitter_spreads_expiry(cache):
    """Identical TTLs make a synchronised herd that re-stampedes on a
    schedule. Jitter is what breaks the synchronisation."""
    ttls = set()
    for i in range(40):
        k = cache.key("jitter", i)
        cache.get_or_load(k, lambda: {"i": i}, ttl=300)
        ttls.add(cache.client.ttl(k))
    assert len(ttls) > 5, f"expiries are not spread: {sorted(ttls)}"


def test_cache_fails_open_when_redis_is_down():
    """Redis being unreachable must degrade latency, never availability."""
    broken = Cache(url="redis://127.0.0.1:1/0", namespace="qq_down")
    calls = {"n": 0}

    def load():
        calls["n"] += 1
        return "from the database"

    assert broken.get_or_load("qq_down:v1:x:1", load) == "from the database"
    assert calls["n"] == 1
    assert broken.stats.errors >= 1


def test_disabled_cache_is_a_pass_through(cache):
    off = Cache(namespace=cache.namespace, enabled=False)
    calls = {"n": 0}

    def load():
        calls["n"] += 1
        return calls["n"]

    key = off.key("thing", 1)
    assert off.get_or_load(key, load) == 1
    assert off.get_or_load(key, load) == 2, "a disabled cache must not serve from Redis"


def test_version_memo_expires(cache):
    """The in-process version memo trades bounded staleness for a saved
    round trip. Bounded means it must actually expire."""
    fast = Cache(namespace=cache.namespace, version_ttl_s=0.05)
    first = fast.key("thing", 1)

    other = Cache(namespace=cache.namespace)
    other.invalidate_entity("thing")

    time.sleep(0.1)
    assert fast.key("thing", 1) != first, "the memo never refreshed"


# ---------------------------------------------------------------------
def test_repository_read_path_uses_the_cache(conn, cache):
    """End to end: the second call must not touch PostgreSQL."""
    from queryquest.cache import repository as repo
    from queryquest.db import fetch_value

    customer_id = fetch_value(conn, "SELECT customer_id FROM qq.customers LIMIT 1")
    if customer_id is None:
        pytest.skip("no customers loaded")

    first = repo.customer_360(int(customer_id), cache=cache)
    assert first is not None
    assert cache.stats.misses == 1

    second = repo.customer_360(int(customer_id), cache=cache)
    assert cache.stats.hits == 1
    assert second["customer_id"] == first["customer_id"]
    assert second["lifetime_value"] == first["lifetime_value"]


def test_repository_invalidation_forces_a_refetch(conn, cache):
    from queryquest.cache import repository as repo
    from queryquest.db import fetch_value

    customer_id = fetch_value(conn, "SELECT customer_id FROM qq.customers LIMIT 1")
    if customer_id is None:
        pytest.skip("no customers loaded")

    repo.customer_360(int(customer_id), cache=cache)
    repo.invalidate_customer(int(customer_id), cache=cache)
    before_misses = cache.stats.misses

    repo.customer_360(int(customer_id), cache=cache)
    assert cache.stats.misses == before_misses + 1
