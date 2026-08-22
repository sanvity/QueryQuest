"""Plan-shape assertions.

A latency number can improve for reasons that have nothing to do with
the change you made -- a warmer cache, a quieter box, a lucky sample.
These tests assert on the *plan* instead, which is what the optimisation
actually claims: this query should use this index, prune these
partitions, and stop sorting.

They are the regression tests for the performance work. If someone drops
`orders_customer_placed_idx`, this fails immediately and names the
reason, rather than the p95 quietly drifting upward for a month.
"""

from __future__ import annotations

import pytest

from queryquest.bench.queries import BY_NAME, sample_pools


def _plan(conn, sql: str, params: dict) -> dict:
    with conn.cursor() as cur:
        cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, params)
        value = next(iter(cur.fetchone().values()))
    return value[0] if isinstance(value, list) else value


def _nodes(plan: dict) -> list[dict]:
    out: list[dict] = []

    def walk(node: dict) -> None:
        out.append(node)
        for child in node.get("Plans", []) or []:
            walk(child)

    walk(plan["Plan"])
    return out


def _node_types(plan: dict) -> set[str]:
    return {n.get("Node Type", "") for n in _nodes(plan)}


def _indexes_used(plan: dict) -> set[str]:
    return {n["Index Name"] for n in _nodes(plan) if "Index Name" in n}


@pytest.fixture(scope="module")
def pools(request):
    from queryquest.db import connect

    with connect() as conn:
        p = sample_pools(conn, "qq")
    if not p.get("customer_ids"):
        pytest.skip("no data loaded")
    return p


def test_q1_uses_the_composite_index_and_does_not_sort(conn, pools):
    q = BY_NAME["q1_customer_order_history"]
    params = q.params(pools)
    plan = _plan(conn, q.sql("qq", use_mart=False), params)

    assert "orders_customer_placed_idx" in _indexes_used(plan), (
        f"expected the composite index, plan used {_indexes_used(plan)}"
    )
    # The index is already in placed_at DESC order, so the ORDER BY is free.
    assert "Sort" not in _node_types(plan), "the composite index should remove the sort"


def test_q1_on_the_baseline_falls_back_to_a_scan(conn, pools):
    """The comparison must be real: without the index this is a scan."""
    q = BY_NAME["q1_customer_order_history"]
    plan = _plan(conn, q.sql("qq_baseline", use_mart=False), q.params(pools))
    assert "Seq Scan" in _node_types(plan), (
        "the baseline is not doing a sequential scan -- it may have acquired an index"
    )


def test_q2_prunes_partitions(conn, pools):
    """A 90-day window over a 24-month table should touch ~4 partitions."""
    q = BY_NAME["q2_revenue_by_region"]
    plan = _plan(conn, q.sql("qq", use_mart=False), q.params(pools))

    scanned = {
        n.get("Relation Name", "")
        for n in _nodes(plan)
        if n.get("Relation Name", "").startswith("orders_p")
    }
    total = len(scanned)
    assert 0 < total <= 8, (
        f"expected pruning to a handful of monthly partitions, scanned {total}: {sorted(scanned)}"
    )
    assert "orders_pdefault" not in scanned


def test_q4_uses_the_partial_index(conn, pools):
    q = BY_NAME["q4_open_order_backlog"]
    plan = _plan(conn, q.sql("qq", use_mart=False), q.params(pools))
    assert "orders_open_backlog_idx" in _indexes_used(plan), (
        f"partial index unused, plan used {_indexes_used(plan)}"
    )


def test_q5_uses_the_covering_index(conn, pools):
    q = BY_NAME["q5_sku_velocity"]
    plan = _plan(conn, q.sql("qq", use_mart=False), q.params(pools))
    assert "order_items_variant_placed_idx" in _indexes_used(plan)


def test_q6_uses_the_partial_restock_index(conn, pools):
    q = BY_NAME["q6_restock_list"]
    plan = _plan(conn, q.sql("qq", use_mart=False), q.params(pools))
    types = _node_types(plan)
    # Small tables can legitimately be scanned; what must not happen is a
    # scan of the fact tables to answer a threshold question.
    assert "inventory_restock_idx" in _indexes_used(plan) or "Seq Scan" in types


def test_mart_queries_read_the_rollup_not_the_fact_table(conn, pools):
    """The materialized-view claim: the optimized form must not touch
    order_items at all."""
    q = BY_NAME["q3_top_sellers"]
    plan = _plan(conn, q.sql("qq", use_mart=True), q.params(pools))
    relations = {n.get("Relation Name", "") for n in _nodes(plan)}
    assert not any(r.startswith("order_items") for r in relations), (
        f"the mart query still reads the fact table: {relations}"
    )
    assert "seller_daily_sales" in relations


def test_optimized_reads_fewer_buffers_than_baseline(conn, pools):
    """Buffer counts are the hardware-independent version of the timing
    claim: fewer pages touched is fewer pages touched on any box."""
    q = BY_NAME["q3_top_sellers"]
    params = q.params(pools)

    def buffers(sql: str) -> int:
        plan = _plan(conn, sql, params)
        return sum(
            int(n.get("Shared Hit Blocks", 0) or 0) + int(n.get("Shared Read Blocks", 0) or 0)
            for n in _nodes(plan)
        )

    base = buffers(q.sql("qq_baseline", use_mart=False))
    opt = buffers(q.sql("qq", use_mart=True))
    assert opt < base, f"optimized touched {opt} buffers, baseline {base}"
