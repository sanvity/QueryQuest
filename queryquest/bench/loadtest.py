"""Closed-loop load test for the cached read paths.

Drives a realistic mix of profile, catalogue and leaderboard reads from
N worker threads for a fixed duration, then reports achieved throughput,
cache hit ratio and latency percentiles. The same run can be repeated
with `cached=False`, which routes every call through the identical code
path with the cache disabled -- so the comparison isolates the cache and
nothing else.

Key selection is deliberately skewed: 80% of requests target 20% of the
keys. Uniform random access is the one distribution under which caching
looks useless, and it is also the one distribution real traffic never
has.
"""

from __future__ import annotations

import logging
import random
import statistics
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable

import psycopg
from psycopg.rows import dict_row

from ..cache.redis_cache import Cache
from ..cache import repository as repo
from ..config import META_SCHEMA, OPTIMIZED_SCHEMA, settings
from .runner import percentile

log = logging.getLogger("queryquest.loadtest")

# Weighted operation mix, roughly modelled on a storefront's read
# traffic: profile and catalogue dominate, dashboards are rarer.
MIX: tuple[tuple[str, int], ...] = (
    ("customer_360", 35),
    ("recent_orders", 20),
    ("product_detail", 25),
    ("top_sellers", 8),
    ("region_revenue", 5),
    ("inventory_snapshot", 7),
)


@dataclass
class LoadResult:
    run_id: str
    workload: str
    cached: bool
    concurrency: int
    duration_s: float
    queries_total: int
    queries_per_min: float
    latencies_ms: list[float] = field(default_factory=list)
    errors: int = 0
    cache_stats: dict = field(default_factory=dict)

    def summary(self) -> dict:
        return {
            "run_id": self.run_id,
            "workload": self.workload,
            "cached": self.cached,
            "concurrency": self.concurrency,
            "duration_s": round(self.duration_s, 3),
            "queries_total": self.queries_total,
            "queries_per_min": round(self.queries_per_min, 2),
            "p50_ms": round(percentile(self.latencies_ms, 0.50), 3),
            "p95_ms": round(percentile(self.latencies_ms, 0.95), 3),
            "p99_ms": round(percentile(self.latencies_ms, 0.99), 3),
            "mean_ms": round(statistics.fmean(self.latencies_ms), 3) if self.latencies_ms else 0.0,
            "errors": self.errors,
            **{f"cache_{k}": v for k, v in self.cache_stats.items()},
        }


def sample_keys(conn: psycopg.Connection, size: int = 2000) -> dict:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"SELECT customer_id FROM {OPTIMIZED_SCHEMA}.customers LIMIT %s", (size,)
        )
        customers = [r["customer_id"] for r in cur.fetchall()]
        cur.execute(f"SELECT product_id FROM {OPTIMIZED_SCHEMA}.products LIMIT %s", (size,))
        products = [r["product_id"] for r in cur.fetchall()]
        cur.execute(
            f"SELECT variant_id FROM {OPTIMIZED_SCHEMA}.product_variants LIMIT %s", (size,)
        )
        variants = [r["variant_id"] for r in cur.fetchall()]
    return {"customers": customers, "products": products, "variants": variants}


def _skewed(rng: random.Random, pool: list):
    """80/20 access skew -- the shape real traffic actually has."""
    if not pool:
        raise RuntimeError("empty key pool: load data before running the load test")
    hot = max(1, len(pool) // 5)
    if rng.random() < 0.8:
        return pool[rng.randrange(hot)]
    return pool[rng.randrange(len(pool))]


def _make_op(
    rng: random.Random, keys: dict, cache: Cache
) -> tuple[str, Callable[[], object]]:
    names = [n for n, _ in MIX]
    weights = [w for _, w in MIX]
    op = rng.choices(names, weights=weights, k=1)[0]

    if op == "customer_360":
        cid = _skewed(rng, keys["customers"])
        return op, lambda: repo.customer_360(cid, cache=cache)
    if op == "recent_orders":
        cid = _skewed(rng, keys["customers"])
        return op, lambda: repo.recent_orders(cid, cache=cache)
    if op == "product_detail":
        pid = _skewed(rng, keys["products"])
        return op, lambda: repo.product_detail(pid, cache=cache)
    if op == "top_sellers":
        days = rng.choice([7, 30, 90])
        return op, lambda: repo.top_sellers(days, cache=cache)
    if op == "region_revenue":
        days = rng.choice([30, 90])
        return op, lambda: repo.region_revenue(days, cache=cache)
    vid = _skewed(rng, keys["variants"])
    return op, lambda: repo.inventory_snapshot(vid, cache=cache)


def run_loadtest(
    conn: psycopg.Connection,
    *,
    duration_s: float = 30.0,
    concurrency: int = 16,
    cached: bool = True,
    warmup_s: float = 10.0,
    seed: int = 20260822,
    run_id: str | None = None,
    workload: str = "storefront_read_mix",
) -> LoadResult:
    run_id = run_id or uuid.uuid4().hex[:12]
    keys = sample_keys(conn)

    cache = Cache(enabled=cached)
    if cached and not cache.ping():
        raise RuntimeError(f"Redis unreachable at {settings.redis_url}")
    if cached:
        cache.flush_namespace()   # start cold: a warm cache flatters the result

    if concurrency > settings.pool_max:
        log.warning(
            "concurrency %d exceeds pool_max %d -- the uncached run will queue "
            "on connections, which is a real effect but worth naming",
            concurrency, settings.pool_max,
        )

    stop = threading.Event()
    latencies: list[list[float]] = [[] for _ in range(concurrency)]
    counts = [0] * concurrency
    errors = [0] * concurrency
    measuring = threading.Event()

    def worker(idx: int) -> None:
        rng = random.Random(seed + idx)
        local: list[float] = []
        n = 0
        err = 0
        while not stop.is_set():
            _, call = _make_op(rng, keys, cache)
            t0 = time.perf_counter()
            try:
                call()
            except Exception:  # noqa: BLE001 -- a failed request is a data point
                err += 1
                continue
            dt = (time.perf_counter() - t0) * 1000
            if measuring.is_set():
                local.append(dt)
                n += 1
        latencies[idx] = local
        counts[idx] = n
        errors[idx] = err

    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(concurrency)]
    for t in threads:
        t.start()

    # Warm-up runs but is not counted: the first pass populates the cache
    # and the connection pool, and measuring it would report the cost of
    # starting up rather than the cost of serving. It needs to be long
    # enough for the hot 20% of keys to be resident -- time it too short
    # and the "cached" run is really a measurement of cache misses.
    time.sleep(warmup_s)
    if cached:
        cache.stats.reset()
    measuring.set()
    t_start = time.perf_counter()

    time.sleep(duration_s)
    measured = time.perf_counter() - t_start
    stop.set()
    for t in threads:
        t.join(timeout=10)

    all_lat = [x for sub in latencies for x in sub]
    total = sum(counts)

    result = LoadResult(
        run_id=run_id,
        workload=workload,
        cached=cached,
        concurrency=concurrency,
        duration_s=measured,
        queries_total=total,
        queries_per_min=(total / measured) * 60 if measured else 0.0,
        latencies_ms=all_lat,
        errors=sum(errors),
        cache_stats=cache.stats.snapshot() if cached else {},
    )

    persist_load(conn, result)
    log.info(
        "%s cached=%s: %d queries in %.1fs = %.0f q/min, p95=%.2fms, hit_ratio=%.3f",
        workload, cached, total, measured, result.queries_per_min,
        percentile(all_lat, 0.95), cache.stats.hit_ratio if cached else 0.0,
    )
    return result


def persist_load(conn: psycopg.Connection, result: LoadResult) -> None:
    s = result.summary()
    stats = result.cache_stats
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {META_SCHEMA}.cache_stats
                (run_id, workload, duration_s, queries_total, queries_per_min,
                 hits, misses, hit_ratio, p50_ms, p95_ms, p99_ms, concurrency)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                result.run_id,
                f"{result.workload}{'' if result.cached else ' (uncached)'}",
                s["duration_s"], s["queries_total"], s["queries_per_min"],
                stats.get("hits", 0), stats.get("misses", 0), stats.get("hit_ratio", 0.0),
                s["p50_ms"], s["p95_ms"], s["p99_ms"], result.concurrency,
            ),
        )
    conn.commit()
