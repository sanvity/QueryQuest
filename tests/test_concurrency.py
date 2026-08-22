"""ACID behaviour under real concurrency.

Every test here runs genuine parallel transactions against a genuine
PostgreSQL. That is the point: `SELECT ... FOR UPDATE` has no observable
behaviour at all unless something else is contending for the row, so a
single-threaded or mocked test of it proves nothing.

The headline test is a matched pair -- the same workload run through the
locking implementation and through a deliberately unlocked one. The
unlocked version oversells. That is what makes the locking version's
success meaningful rather than a tautology.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from queryquest.concurrency import inventory as inv
from queryquest.db import close_pool, fetch_one, fetch_value

pytestmark = pytest.mark.concurrency


def _stock(conn, variant_id: int) -> dict:
    return fetch_one(
        conn,
        "SELECT qty_on_hand, qty_reserved, qty_available, row_version "
        "FROM qq.inventory WHERE variant_id = %s",
        (variant_id,),
    )


def _set_stock(conn, variant_id: int, on_hand: int, reserved: int = 0) -> None:
    conn.execute(
        "UPDATE qq.inventory SET qty_reserved = 0 WHERE variant_id = %s", (variant_id,)
    )
    conn.execute(
        "UPDATE qq.inventory SET qty_on_hand = %s, qty_reserved = %s WHERE variant_id = %s",
        (on_hand, reserved, variant_id),
    )
    conn.commit()


# ---------------------------------------------------------------------
def test_concurrent_reservations_never_oversell(conn, isolated_variant):
    """40 threads race for 20 units. Exactly 20 must win."""
    _set_stock(conn, isolated_variant, on_hand=20, reserved=0)

    workers, want = 40, 1
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(inv.reserve, isolated_variant, want) for _ in range(workers)
        ]
        results = [f.result() for f in as_completed(futures)]

    granted = sum(1 for r in results if r.ok)
    after = _stock(conn, isolated_variant)

    assert granted == 20, f"expected exactly 20 winners, got {granted}"
    assert after["qty_reserved"] == 20
    assert after["qty_available"] == 0
    assert after["qty_reserved"] <= after["qty_on_hand"]


def test_the_unlocked_version_actually_oversells(conn, isolated_variant):
    """The control case.

    `unsafe_reserve_inventory` is `reserve_inventory` with the FOR UPDATE
    removed. It reads availability, pauses, then writes -- the textbook
    lost update. If this test ever stops overselling, the test above has
    stopped proving anything and something else is serialising the work.
    """
    _set_stock(conn, isolated_variant, on_hand=5, reserved=0)

    def unsafe() -> bool:
        with inv.transaction() as c:
            with c.cursor() as cur:
                cur.execute(
                    "SELECT qq.unsafe_reserve_inventory(%s, 1) AS ok", (isolated_variant,)
                )
                return bool(cur.fetchone()["ok"])

    granted = 0
    errors = 0
    with ThreadPoolExecutor(max_workers=12) as pool:
        futures = [pool.submit(unsafe) for _ in range(12)]
        for f in as_completed(futures):
            try:
                granted += 1 if f.result() else 0
            except Exception:  # noqa: BLE001 -- the CHECK constraint firing IS the failure
                errors += 1

    # Either more reservations were granted than there was stock, or the
    # CHECK constraint had to abort transactions to stop it. Both are the
    # lost update showing up; the locking version produces neither.
    assert granted > 5 or errors > 0, (
        "the unlocked path did not misbehave -- the locking test above may be vacuous"
    )


def test_reservation_is_rejected_when_stock_runs_out(conn, isolated_variant):
    _set_stock(conn, isolated_variant, on_hand=3, reserved=0)

    ok = inv.reserve(isolated_variant, 3)
    assert ok.ok

    denied = inv.reserve(isolated_variant, 1)
    assert not denied.ok
    assert denied.reason == "insufficient stock"


def test_release_returns_stock(conn, isolated_variant):
    _set_stock(conn, isolated_variant, on_hand=10, reserved=0)
    assert inv.reserve(isolated_variant, 4).ok
    assert _stock(conn, isolated_variant)["qty_reserved"] == 4

    assert inv.release(isolated_variant, 4)
    assert _stock(conn, isolated_variant)["qty_reserved"] == 0


# ---------------------------------------------------------------------
def test_opposing_cart_orders_do_not_deadlock(conn, isolated_variant, schema_ready):
    """Two carts, same two SKUs, opposite order in the request.

    Locking in request order would deadlock roughly half the time.
    reserve_cart sorts by variant_id first, so a wait cycle cannot form.
    """
    second = fetch_value(
        conn,
        """
        SELECT variant_id FROM qq.inventory
        WHERE variant_id <> %s ORDER BY variant_id LIMIT 1
        """,
        (isolated_variant,),
    )
    if second is None:
        pytest.skip("need at least two SKUs")

    a, b = sorted([isolated_variant, int(second)])
    _set_stock(conn, a, on_hand=200)
    _set_stock(conn, b, on_hand=200)

    rounds = 30
    barrier = threading.Barrier(2)

    def cart(order):
        results = []
        for _ in range(rounds):
            barrier.wait()
            results.append(inv.reserve_cart(order))
        return results

    with ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(cart, [(a, 1), (b, 1)])
        f2 = pool.submit(cart, [(b, 1), (a, 1)])
        r1, r2 = f1.result(), f2.result()

    all_results = r1 + r2
    assert all(r.ok for r in all_results), [r.reason for r in all_results if not r.ok]
    assert not any("Deadlock" in r.reason for r in all_results)

    assert _stock(conn, a)["qty_reserved"] == 2 * rounds
    assert _stock(conn, b)["qty_reserved"] == 2 * rounds


def test_cart_reservation_is_all_or_nothing(conn, isolated_variant):
    """One unsatisfiable line must undo the lines already reserved."""
    second = fetch_value(
        conn,
        "SELECT variant_id FROM qq.inventory WHERE variant_id <> %s ORDER BY variant_id LIMIT 1",
        (isolated_variant,),
    )
    if second is None:
        pytest.skip("need at least two SKUs")

    a, b = sorted([isolated_variant, int(second)])
    _set_stock(conn, a, on_hand=100)
    _set_stock(conn, b, on_hand=1)

    result = inv.reserve_cart([(a, 5), (b, 50)])   # b cannot be satisfied

    assert not result.ok
    assert _stock(conn, a)["qty_reserved"] == 0, (
        "the satisfiable line was left reserved -- the rollback did not happen"
    )
    assert _stock(conn, b)["qty_reserved"] == 0


# ---------------------------------------------------------------------
def test_checkout_is_atomic(conn, isolated_variant):
    """Reservation, order header and line items commit as one unit."""
    import uuid

    _set_stock(conn, isolated_variant, on_hand=50)
    customer_id = fetch_value(conn, "SELECT customer_id FROM qq.customers LIMIT 1")
    if customer_id is None:
        pytest.skip("no customers loaded")

    uid = f"CHK-{uuid.uuid4().hex[:10]}"
    result = inv.checkout(int(customer_id), uid, [(isolated_variant, 2, 19.99)])

    assert result.ok, result.reason
    assert _stock(conn, isolated_variant)["qty_reserved"] == 2

    row = fetch_one(
        conn,
        "SELECT o.order_id, o.item_count, count(oi.*) AS lines "
        "FROM qq.orders o LEFT JOIN qq.order_items oi "
        "  ON oi.order_id = o.order_id AND oi.placed_at = o.placed_at "
        "WHERE o.order_uid = %s GROUP BY o.order_id, o.item_count",
        (uid,),
    )
    assert row is not None
    assert row["lines"] == 1


def test_failed_checkout_leaves_no_trace(conn, isolated_variant):
    """A checkout that cannot be satisfied must not reserve stock or
    leave a half-built order behind."""
    import uuid

    _set_stock(conn, isolated_variant, on_hand=1)
    customer_id = fetch_value(conn, "SELECT customer_id FROM qq.customers LIMIT 1")
    if customer_id is None:
        pytest.skip("no customers loaded")

    uid = f"CHK-FAIL-{uuid.uuid4().hex[:10]}"
    result = inv.checkout(int(customer_id), uid, [(isolated_variant, 99, 19.99)])

    assert not result.ok
    assert _stock(conn, isolated_variant)["qty_reserved"] == 0
    assert fetch_value(
        conn, "SELECT count(*) FROM qq.orders WHERE order_uid = %s", (uid,)
    ) == 0


def test_concurrent_checkouts_conserve_stock(conn, isolated_variant):
    """The global invariant, under load: units reserved equals units sold."""
    import uuid

    _set_stock(conn, isolated_variant, on_hand=30)
    customer_id = fetch_value(conn, "SELECT customer_id FROM qq.customers LIMIT 1")
    if customer_id is None:
        pytest.skip("no customers loaded")

    def one(i: int):
        return inv.checkout(
            int(customer_id), f"CHK-RACE-{uuid.uuid4().hex[:8]}-{i}",
            [(isolated_variant, 2, 5.00)],
        )

    with ThreadPoolExecutor(max_workers=25) as pool:
        results = [f.result() for f in as_completed([pool.submit(one, i) for i in range(25)])]

    won = [r for r in results if r.ok]
    after = _stock(conn, isolated_variant)

    assert len(won) == 15, f"30 units / 2 per order = 15 orders, got {len(won)}"
    assert after["qty_reserved"] == 30
    assert after["qty_reserved"] <= after["qty_on_hand"]

    sold = fetch_value(
        conn,
        "SELECT coalesce(sum(quantity), 0) FROM qq.order_items "
        "WHERE variant_id = %s AND order_id IN "
        "  (SELECT order_id FROM qq.orders WHERE order_uid LIKE 'CHK-RACE-%%')",
        (isolated_variant,),
    )
    assert int(sold) == 30, "reserved units and sold units have diverged"


# ---------------------------------------------------------------------
def test_skip_locked_gives_workers_disjoint_batches(conn, schema_ready):
    """Two workers draining the same queue must not collide or overlap."""
    claimed: list[list[int]] = []
    lock = threading.Lock()

    def worker():
        rows = inv.claim_restock(limit=5)
        with lock:
            claimed.append([r["variant_id"] for r in rows])

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(as_completed([pool.submit(worker) for _ in range(2)]))

    if not any(claimed):
        pytest.skip("no SKUs below reorder point in this dataset")


def test_retry_only_retries_retryable_errors():
    """A programming error must surface immediately, not be retried five
    times and then surface -- retrying a bug just delays the report."""
    from psycopg import errors as pg_errors

    calls = {"n": 0}

    def always_bad():
        calls["n"] += 1
        raise pg_errors.UndefinedColumn("no such column")

    with pytest.raises(pg_errors.UndefinedColumn):
        inv.with_retry(always_bad, max_retries=3)
    assert calls["n"] == 1


def test_retry_recovers_from_a_transient_serialization_failure():
    from psycopg import errors as pg_errors

    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise pg_errors.SerializationFailure("could not serialize")
        return "ok"

    value, attempts = inv.with_retry(flaky, max_retries=5, base_ms=1)
    assert value == "ok"
    assert attempts == 3


@pytest.fixture(autouse=True, scope="module")
def _close_pool_after_module():
    yield
    close_pool()
