"""Benchmark harness.

Measures the same seven queries against `qq_baseline` (heap tables,
primary keys only) and `qq` (partitioned, indexed, materialized views)
and writes every timing to qq_meta.query_benchmarks.

Fairness rules, because a benchmark that flatters itself is worthless:

  * Same box, same data, same parameter values for both variants. The
    parameter sequence is generated from a seeded RNG and replayed, so
    variant B answers the identical questions variant A did.

  * Warm-up repetitions before timing, discarded. The first execution of
    any query pays for plan generation and a cold buffer cache; timing
    it measures start-up, not steady state.

  * Variants are interleaved per query rather than run in two blocks, so
    a gradually warming page cache cannot systematically favour whichever
    ran second.

  * p95 is reported alongside the mean because the mean hides exactly
    the tail that users feel.
"""

from __future__ import annotations

import json
import logging
import random
import statistics
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Sequence

import psycopg
from psycopg.rows import dict_row

from ..config import BASELINE_SCHEMA, META_SCHEMA, OPTIMIZED_SCHEMA
from .queries import SUITE, BenchQuery, sample_pools

log = logging.getLogger("queryquest.bench")


def percentile(values: Sequence[float], q: float) -> float:
    """Nearest-rank percentile. No interpolation: with 30 samples,
    interpolating invents a number no run actually produced."""
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, int(round(q * len(ordered) + 0.5)) - 1))
    return ordered[idx]


@dataclass
class QueryTiming:
    query_name: str
    variant: str
    samples: list[float] = field(default_factory=list)
    rows_returned: int = 0
    shared_hit: int = 0
    shared_read: int = 0
    plan: dict | None = None

    def summary(self) -> dict:
        s = self.samples
        return {
            "query_name": self.query_name,
            "variant": self.variant,
            "repetitions": len(s),
            "p50_ms": round(percentile(s, 0.50), 3),
            "p95_ms": round(percentile(s, 0.95), 3),
            "p99_ms": round(percentile(s, 0.99), 3),
            "mean_ms": round(statistics.fmean(s), 3) if s else 0.0,
            "min_ms": round(min(s), 3) if s else 0.0,
            "max_ms": round(max(s), 3) if s else 0.0,
            "rows_returned": self.rows_returned,
            "shared_hit": self.shared_hit,
            "shared_read": self.shared_read,
        }


def _time_once(conn: psycopg.Connection, sql: str, params: dict) -> tuple[float, int]:
    """Execute and fully drain the result, measuring wall clock.

    Draining matters: measuring only `execute` on a cursor times how long
    the server took to *start* returning rows, which for a large result
    set is a fraction of the real cost.
    """
    with conn.cursor(row_factory=dict_row) as cur:
        t0 = time.perf_counter()
        cur.execute(sql, params)
        rows = cur.fetchall()
        elapsed = (time.perf_counter() - t0) * 1000
    return elapsed, len(rows)


def _capture_plan(conn: psycopg.Connection, sql: str, params: dict) -> dict | None:
    """EXPLAIN (ANALYZE, BUFFERS) -- the evidence behind every claim.

    BUFFERS is the part people skip and the part that actually explains
    the win: shared_read is pages fetched from disk, shared_hit is pages
    already in the buffer cache. An optimisation that turns reads into
    hits is real; one that only moves time around is not.
    """
    try:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, FORMAT JSON) " + sql, params
            )
            row = cur.fetchone()
            value = next(iter(row.values()))
            return value[0] if isinstance(value, list) else value
    except psycopg.Error as exc:
        log.warning("could not capture plan: %s", exc)
        conn.rollback()
        return None


def _buffers_from_plan(plan: dict | None) -> tuple[int, int]:
    if not plan:
        return 0, 0
    total_hit = total_read = 0

    def walk(node: dict) -> None:
        nonlocal total_hit, total_read
        total_hit += int(node.get("Shared Hit Blocks", 0) or 0)
        total_read += int(node.get("Shared Read Blocks", 0) or 0)
        for child in node.get("Plans", []) or []:
            walk(child)

    root = plan.get("Plan") if isinstance(plan, dict) else None
    if root:
        walk(root)
    return total_hit, total_read


def run_query(
    conn: psycopg.Connection,
    query: BenchQuery,
    variant: str,
    pools: dict,
    param_seq: list[dict],
    *,
    warmup: int,
) -> QueryTiming:
    schema = OPTIMIZED_SCHEMA if variant == "optimized" else BASELINE_SCHEMA
    use_mart = variant == "optimized"
    sql = query.sql(schema, use_mart)

    timing = QueryTiming(query_name=query.name, variant=variant)

    for i in range(warmup):
        try:
            _time_once(conn, sql, param_seq[i % len(param_seq)])
        except psycopg.Error:
            conn.rollback()
            raise

    for params in param_seq:
        elapsed, nrows = _time_once(conn, sql, params)
        timing.samples.append(elapsed)
        timing.rows_returned = nrows

    timing.plan = _capture_plan(conn, sql, param_seq[0])
    timing.shared_hit, timing.shared_read = _buffers_from_plan(timing.plan)
    return timing


def persist(conn: psycopg.Connection, run_id: str, timing: QueryTiming) -> None:
    s = timing.summary()
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {META_SCHEMA}.query_benchmarks
                (run_id, query_name, variant, repetitions, p50_ms, p95_ms, p99_ms,
                 mean_ms, min_ms, max_ms, rows_returned, shared_hit, shared_read, plan_json)
            VALUES (%(run_id)s, %(query_name)s, %(variant)s, %(repetitions)s,
                    %(p50_ms)s, %(p95_ms)s, %(p99_ms)s, %(mean_ms)s, %(min_ms)s,
                    %(max_ms)s, %(rows_returned)s, %(shared_hit)s, %(shared_read)s,
                    %(plan)s)
            """,
            {**s, "run_id": run_id, "plan": json.dumps(timing.plan) if timing.plan else None},
        )
    conn.commit()


def run_suite(
    conn: psycopg.Connection,
    *,
    reps: int = 30,
    warmup: int = 3,
    seed: int = 20260822,
    only: Sequence[str] | None = None,
    variants: Sequence[str] = ("baseline", "optimized"),
    run_id: str | None = None,
) -> dict[str, Any]:
    run_id = run_id or uuid.uuid4().hex[:12]
    log.info("benchmark run %s: %d reps, %d warmup", run_id, reps, warmup)

    # Pools come from the optimized schema, but both schemas hold the
    # same rows, so the ids are valid against either.
    pools = sample_pools(conn, OPTIMIZED_SCHEMA)
    if not pools.get("customer_ids") or not pools.get("variant_ids"):
        raise RuntimeError("no data loaded -- run the ETL before benchmarking")

    results: dict[str, dict[str, dict]] = {}

    for query in SUITE:
        if only and query.name not in only:
            continue

        # One parameter sequence, replayed for every variant.
        rng = random.Random(f"{seed}:{query.name}")
        random.seed(rng.random())
        param_seq = [query.params(pools) for _ in range(reps)]

        results[query.name] = {}
        for variant in variants:
            try:
                timing = run_query(
                    conn, query, variant, pools, param_seq, warmup=warmup
                )
            except psycopg.Error as exc:
                log.error("%s/%s failed: %s", query.name, variant, exc)
                conn.rollback()
                continue

            persist(conn, run_id, timing)
            results[query.name][variant] = timing.summary()
            log.info(
                "%-28s %-9s p50=%8.2fms p95=%8.2fms",
                query.name, variant,
                results[query.name][variant]["p50_ms"],
                results[query.name][variant]["p95_ms"],
            )

    return {"run_id": run_id, "reps": reps, "results": results}
