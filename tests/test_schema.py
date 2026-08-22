"""Schema-level guarantees.

These assert the things the schema promises and the application then
relies on. If a constraint is silently dropped in a future migration,
one of these fails rather than a data corruption bug surfacing months
later in a revenue report.
"""

from __future__ import annotations

import pytest
from psycopg import errors as pg_errors

from queryquest.db import fetch_all, fetch_one, fetch_value

DOMAIN_TABLES = [
    "regions", "customers", "customer_addresses", "sellers", "categories",
    "products", "product_variants", "inventory", "orders", "order_items",
    "payments", "shipments",
]


def test_twelve_domain_tables(conn):
    rows = fetch_all(
        conn,
        """
        SELECT c.relname AS name
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'qq' AND c.relkind IN ('r', 'p')
          AND c.relispartition = false
        ORDER BY 1
        """,
    )
    names = {r["name"] for r in rows}
    assert set(DOMAIN_TABLES) <= names
    # Exactly twelve base tables -- partitions are excluded above.
    assert len([n for n in names if n in DOMAIN_TABLES]) == 12


def test_baseline_mirrors_optimized_columns(conn):
    """The two schemas must stay column-identical or the benchmark is a lie."""
    for table in DOMAIN_TABLES:
        cols = fetch_all(
            conn,
            """
            SELECT table_schema, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema IN ('qq', 'qq_baseline') AND table_name = %s
            ORDER BY column_name
            """,
            (table,),
        )
        opt = [(c["column_name"], c["data_type"]) for c in cols if c["table_schema"] == "qq"]
        base = [
            (c["column_name"], c["data_type"]) for c in cols if c["table_schema"] == "qq_baseline"
        ]
        assert opt == base, f"{table}: schemas have drifted apart"


def test_orders_is_partitioned_and_baseline_is_not(conn):
    assert fetch_value(
        conn,
        "SELECT relkind = 'p' FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'qq' AND c.relname = 'orders'",
    ) is True
    assert fetch_value(
        conn,
        "SELECT relkind = 'p' FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'qq_baseline' AND c.relname = 'orders'",
    ) is False


def test_monthly_partitions_exist(conn):
    n = fetch_value(
        conn,
        """
        SELECT count(*) FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'qq' AND c.relname LIKE 'orders_p2%'
        """,
    )
    assert n >= 12, f"expected at least a year of monthly partitions, found {n}"


def test_default_partition_is_a_backstop_not_a_dumping_ground(conn):
    """A non-empty default partition means the calendar outran the
    partitions that exist, and pruning is silently degraded for it."""
    n = fetch_value(conn, "SELECT count(*) FROM qq.orders_pdefault")
    assert n == 0, f"{n} orders landed in the default partition -- extend the calendar"


def test_inventory_cannot_be_oversold_by_constraint(conn, isolated_variant):
    """The last line of defence. Even with the locking bypassed, the
    database must refuse to record more reserved than on hand."""
    with pytest.raises(pg_errors.CheckViolation):
        conn.execute(
            "UPDATE qq.inventory SET qty_reserved = qty_on_hand + 1 WHERE variant_id = %s",
            (isolated_variant,),
        )
    conn.rollback()


def test_qty_available_is_generated(conn, isolated_variant):
    row = fetch_one(
        conn,
        "SELECT qty_on_hand, qty_reserved, qty_available FROM qq.inventory WHERE variant_id = %s",
        (isolated_variant,),
    )
    assert row["qty_available"] == row["qty_on_hand"] - row["qty_reserved"]


def test_order_totals_are_constrained(conn):
    with pytest.raises((pg_errors.CheckViolation, pg_errors.ForeignKeyViolation)):
        conn.execute(
            """
            INSERT INTO qq.orders (order_uid, placed_at, customer_id, region_id,
                                   currency_code, subtotal, total_amount, item_count, source_hash)
            VALUES ('BAD-1', now(), 1, 1, 'USD', 10, -5, 1, sha256('x'::bytea))
            """
        )
    conn.rollback()


def test_expected_indexes_exist(conn):
    names = {
        r["indexname"]
        for r in fetch_all(conn, "SELECT indexname FROM pg_indexes WHERE schemaname = 'qq'")
    }
    for expected in (
        "orders_customer_placed_idx",
        "orders_open_backlog_idx",
        "order_items_variant_placed_idx",
        "order_items_placed_brin_idx",
        "inventory_restock_idx",
        "customers_email_lower_uq",
    ):
        assert expected in names, f"missing index {expected}"


def test_baseline_has_no_secondary_indexes(conn):
    """If the baseline picked up indexes, the comparison stops being one."""
    rows = fetch_all(
        conn,
        """
        SELECT indexname FROM pg_indexes
        WHERE schemaname = 'qq_baseline'
          AND indexname NOT LIKE '%_pk'
          AND indexname NOT LIKE '%_uq'
          AND indexname NOT LIKE '%_pkey'
          AND indexname NOT LIKE '%_key'
        """,
    )
    assert rows == [], f"baseline has extra indexes: {rows}"


def test_materialized_views_exist(conn):
    names = {
        r["matviewname"]
        for r in fetch_all(conn, "SELECT matviewname FROM pg_matviews WHERE schemaname = 'qq_mart'")
    }
    assert {
        "daily_region_revenue", "seller_daily_sales",
        "category_month_sales", "customer_ltv",
    } <= names


def test_matviews_have_unique_indexes_for_concurrent_refresh(conn):
    """REFRESH ... CONCURRENTLY requires one. Without it the refresh takes
    an ACCESS EXCLUSIVE lock and the dashboard blocks for its duration."""
    for mv in ("daily_region_revenue", "seller_daily_sales",
               "category_month_sales", "customer_ltv"):
        n = fetch_value(
            conn,
            """
            SELECT count(*) FROM pg_index i
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace ns ON ns.oid = c.relnamespace
            WHERE ns.nspname = 'qq_mart' AND c.relname = %s AND i.indisunique
            """,
            (mv,),
        )
        assert n >= 1, f"{mv} has no unique index"
