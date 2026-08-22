"""Cached read paths.

Each function is a read-through: build a key, hand the cache a loader
that knows how to fetch from PostgreSQL, return the value. The
connection is acquired *inside* the loader, so a cache hit never takes a
pool slot at all -- which is the actual reason the cache raises
throughput, not just latency. Under load the pool stops being the
bottleneck because most requests never reach it.

Write paths call the matching `invalidate_*` so a stale read is bounded
by the write, not by the TTL.
"""

from __future__ import annotations

import logging
from typing import Any

from psycopg.rows import dict_row

from ..config import MART_SCHEMA, OPTIMIZED_SCHEMA
from ..db import get_pool
from .redis_cache import Cache, get_cache

log = logging.getLogger("queryquest.cache.repository")

CUSTOMER_TTL = 300
PRODUCT_TTL = 600
LEADERBOARD_TTL = 120
REVENUE_TTL = 300


def _query(sql: str, params: dict | tuple, one: bool = False) -> Any:
    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params)
            return cur.fetchone() if one else cur.fetchall()


# ---------------------------------------------------------------------
# Customer 360
# ---------------------------------------------------------------------
CUSTOMER_360_SQL = f"""
SELECT c.customer_id,
       c.customer_uid,
       c.full_name,
       c.email,
       c.status,
       c.signup_at,
       r.region_code,
       r.region_name,
       coalesce(l.lifetime_orders, 0)     AS lifetime_orders,
       coalesce(l.lifetime_value,  0)     AS lifetime_value,
       l.last_order_at
FROM {OPTIMIZED_SCHEMA}.customers c
JOIN {OPTIMIZED_SCHEMA}.regions   r ON r.region_id = c.region_id
LEFT JOIN {MART_SCHEMA}.customer_ltv l ON l.customer_id = c.customer_id
WHERE c.customer_id = %(customer_id)s
"""


def customer_360(customer_id: int, cache: Cache | None = None) -> dict | None:
    cache = cache or get_cache()
    key = cache.key("customer", customer_id, "360")
    return cache.get_or_load(
        key,
        lambda: _query(CUSTOMER_360_SQL, {"customer_id": customer_id}, one=True),
        ttl=CUSTOMER_TTL,
    )


def invalidate_customer(customer_id: int, cache: Cache | None = None) -> None:
    cache = cache or get_cache()
    cache.invalidate(cache.key("customer", customer_id, "360"))


# ---------------------------------------------------------------------
# Recent orders for a customer
# ---------------------------------------------------------------------
RECENT_ORDERS_SQL = f"""
SELECT o.order_id, o.order_uid, o.placed_at, o.status,
       o.total_amount, o.item_count
FROM {OPTIMIZED_SCHEMA}.orders o
WHERE o.customer_id = %(customer_id)s
ORDER BY o.placed_at DESC
LIMIT %(limit)s
"""


def recent_orders(customer_id: int, limit: int = 20, cache: Cache | None = None) -> list[dict]:
    cache = cache or get_cache()
    key = cache.key("customer", customer_id, f"orders:{limit}")
    return cache.get_or_load(
        key,
        lambda: _query(RECENT_ORDERS_SQL, {"customer_id": customer_id, "limit": limit}),
        ttl=CUSTOMER_TTL,
    )


# ---------------------------------------------------------------------
# Product detail (catalogue page)
# ---------------------------------------------------------------------
PRODUCT_DETAIL_SQL = f"""
SELECT p.product_id, p.product_uid, p.title, p.base_price, p.is_active,
       s.seller_uid, s.display_name AS seller_name, s.rating AS seller_rating,
       cat.category_slug, cat.category_name,
       v.variant_id, v.sku, v.variant_label, v.price_delta,
       i.qty_available
FROM {OPTIMIZED_SCHEMA}.products         p
JOIN {OPTIMIZED_SCHEMA}.sellers          s   ON s.seller_id   = p.seller_id
JOIN {OPTIMIZED_SCHEMA}.categories       cat ON cat.category_id = p.category_id
JOIN {OPTIMIZED_SCHEMA}.product_variants v   ON v.product_id  = p.product_id
LEFT JOIN {OPTIMIZED_SCHEMA}.inventory   i   ON i.variant_id  = v.variant_id
WHERE p.product_id = %(product_id)s AND v.is_active
ORDER BY v.variant_id
"""


def product_detail(product_id: int, cache: Cache | None = None) -> list[dict]:
    cache = cache or get_cache()
    key = cache.key("product", product_id, "detail")
    return cache.get_or_load(
        key,
        lambda: _query(PRODUCT_DETAIL_SQL, {"product_id": product_id}),
        ttl=PRODUCT_TTL,
    )


def invalidate_product(product_id: int, cache: Cache | None = None) -> None:
    cache = cache or get_cache()
    cache.invalidate(cache.key("product", product_id, "detail"))


# ---------------------------------------------------------------------
# Leaderboards, served off the mart
# ---------------------------------------------------------------------
TOP_SELLERS_SQL = f"""
SELECT sd.seller_id, s.display_name,
       sum(sd.revenue)    AS revenue,
       sum(sd.units_sold) AS units_sold
FROM {MART_SCHEMA}.seller_daily_sales sd
JOIN {OPTIMIZED_SCHEMA}.sellers s ON s.seller_id = sd.seller_id
WHERE sd.day >= current_date - %(days)s::integer
GROUP BY sd.seller_id, s.display_name
ORDER BY revenue DESC
LIMIT %(limit)s
"""


def top_sellers(days: int = 30, limit: int = 20, cache: Cache | None = None) -> list[dict]:
    cache = cache or get_cache()
    key = cache.key("leaderboard", f"sellers:{days}", str(limit))
    return cache.get_or_load(
        key,
        lambda: _query(TOP_SELLERS_SQL, {"days": days, "limit": limit}),
        ttl=LEADERBOARD_TTL,
    )


REGION_REVENUE_SQL = f"""
SELECT d.region_id, r.region_code, r.region_name,
       sum(d.order_count)   AS order_count,
       sum(d.gross_revenue) AS gross_revenue,
       sum(d.net_revenue)   AS net_revenue
FROM {MART_SCHEMA}.daily_region_revenue d
JOIN {OPTIMIZED_SCHEMA}.regions r ON r.region_id = d.region_id
WHERE d.day >= current_date - %(days)s::integer
GROUP BY d.region_id, r.region_code, r.region_name
ORDER BY gross_revenue DESC
"""


def region_revenue(days: int = 90, cache: Cache | None = None) -> list[dict]:
    cache = cache or get_cache()
    key = cache.key("leaderboard", f"regions:{days}")
    return cache.get_or_load(
        key,
        lambda: _query(REGION_REVENUE_SQL, {"days": days}),
        ttl=REVENUE_TTL,
    )


def invalidate_leaderboards(cache: Cache | None = None) -> int:
    """Called after a mart refresh -- one INCR orphans every leaderboard."""
    cache = cache or get_cache()
    return cache.invalidate_entity("leaderboard")


# ---------------------------------------------------------------------
# Inventory: read-through, but invalidated on every reservation.
# ---------------------------------------------------------------------
INVENTORY_SQL = f"""
SELECT i.variant_id, v.sku, i.qty_on_hand, i.qty_reserved,
       i.qty_available, i.reorder_point, i.row_version
FROM {OPTIMIZED_SCHEMA}.inventory i
JOIN {OPTIMIZED_SCHEMA}.product_variants v ON v.variant_id = i.variant_id
WHERE i.variant_id = %(variant_id)s
"""


def inventory_snapshot(variant_id: int, cache: Cache | None = None) -> dict | None:
    """Cached with a short TTL on purpose.

    Stock is the one value where a stale read has a business cost, so
    this is advisory only: the reservation path in
    concurrency/inventory.py always re-reads under a row lock and never
    trusts this number.
    """
    cache = cache or get_cache()
    key = cache.key("inventory", variant_id)
    return cache.get_or_load(
        key,
        lambda: _query(INVENTORY_SQL, {"variant_id": variant_id}, one=True),
        ttl=30,
    )


def invalidate_inventory(variant_id: int, cache: Cache | None = None) -> None:
    cache = cache or get_cache()
    cache.invalidate(cache.key("inventory", variant_id))


READ_PATHS = {
    "customer_360": customer_360,
    "recent_orders": recent_orders,
    "product_detail": product_detail,
    "top_sellers": top_sellers,
    "region_revenue": region_revenue,
    "inventory_snapshot": inventory_snapshot,
}
