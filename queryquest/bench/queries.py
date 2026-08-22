"""The benchmark suite.

Seven queries chosen to cover the access patterns the schema is actually
designed for -- point lookup, windowed aggregate, backlog poll, per-key
time series, threshold scan, profile join. Anything narrower would let a
single index flatter the whole result.

Parameters are drawn from the data that is really loaded, and a fresh
value is used on each repetition. Hammering one customer_id would
measure how warm one buffer is, not how the index performs.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

import psycopg
from psycopg.rows import dict_row

from ..config import QUERY_DIR


@dataclass(frozen=True)
class BenchQuery:
    name: str
    sql_file: str
    description: str
    params: Callable[[dict], dict]
    mart_file: str | None = None   # optimized-variant override, if any

    def sql(self, schema: str, use_mart: bool) -> str:
        path = QUERY_DIR / (self.mart_file if (use_mart and self.mart_file) else self.sql_file)
        text = path.read_text(encoding="utf-8")
        return text.replace("${SCHEMA}", schema)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------
# Parameter pools, sampled once from live data
# ---------------------------------------------------------------------
def sample_pools(conn: psycopg.Connection, schema: str, size: int = 500) -> dict:
    """Draw real ids to parameterise the run.

    TABLESAMPLE SYSTEM is used rather than ORDER BY random(): the latter
    sorts the entire table, which on the fact tables costs more than the
    benchmark itself.
    """
    pools: dict = {}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT customer_id FROM {schema}.customers
            TABLESAMPLE SYSTEM (5) LIMIT %s
            """,
            (size,),
        )
        pools["customer_ids"] = [r["customer_id"] for r in cur.fetchall()]
        if not pools["customer_ids"]:
            cur.execute(f"SELECT customer_id FROM {schema}.customers LIMIT %s", (size,))
            pools["customer_ids"] = [r["customer_id"] for r in cur.fetchall()]

        cur.execute(
            f"""
            SELECT variant_id FROM {schema}.product_variants
            TABLESAMPLE SYSTEM (10) LIMIT %s
            """,
            (size,),
        )
        pools["variant_ids"] = [r["variant_id"] for r in cur.fetchall()]
        if not pools["variant_ids"]:
            cur.execute(f"SELECT variant_id FROM {schema}.product_variants LIMIT %s", (size,))
            pools["variant_ids"] = [r["variant_id"] for r in cur.fetchall()]

        cur.execute(f"SELECT max(placed_at) AS hi, min(placed_at) AS lo FROM {schema}.orders")
        row = cur.fetchone()
        pools["max_placed_at"] = row["hi"] or now_utc()
        pools["min_placed_at"] = row["lo"] or now_utc()

    return pools


def _hi(pools: dict) -> datetime:
    return pools.get("max_placed_at") or now_utc()


SUITE: tuple[BenchQuery, ...] = (
    BenchQuery(
        name="q1_customer_order_history",
        sql_file="q1_customer_order_history.sql",
        description="One customer's last 12 months of orders, newest first",
        params=lambda p: {
            "customer_id": random.choice(p["customer_ids"]),
            "since": _hi(p) - timedelta(days=365),
        },
    ),
    BenchQuery(
        name="q2_revenue_by_region",
        sql_file="q2_revenue_by_region.sql",
        mart_file="q2_revenue_by_region.mart.sql",
        description="Gross and net revenue by region over a 90-day window",
        params=lambda p: {
            "since": _hi(p) - timedelta(days=90),
            "until": _hi(p),
        },
    ),
    BenchQuery(
        name="q3_top_sellers",
        sql_file="q3_top_sellers.sql",
        mart_file="q3_top_sellers.mart.sql",
        description="Top 20 sellers by revenue over 30 days",
        params=lambda p: {"since": _hi(p) - timedelta(days=30)},
    ),
    BenchQuery(
        name="q4_open_order_backlog",
        sql_file="q4_open_order_backlog.sql",
        description="Oldest 200 orders still pending or paid",
        params=lambda p: {"cutoff": _hi(p)},
    ),
    BenchQuery(
        name="q5_sku_velocity",
        sql_file="q5_sku_velocity.sql",
        description="Weekly units and revenue for one SKU over 180 days",
        params=lambda p: {
            "variant_id": random.choice(p["variant_ids"]),
            "since": _hi(p) - timedelta(days=180),
        },
    ),
    BenchQuery(
        name="q6_restock_list",
        sql_file="q6_restock_list.sql",
        description="SKUs at or below reorder point, worst shortfall first",
        params=lambda p: {},
    ),
    BenchQuery(
        name="q7_customer_360",
        sql_file="q7_customer_360.sql",
        mart_file="q7_customer_360.mart.sql",
        description="Customer profile with lifetime totals",
        params=lambda p: {"customer_id": random.choice(p["customer_ids"])},
    ),
)

BY_NAME = {q.name: q for q in SUITE}
