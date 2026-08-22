"""Transactional inventory operations.

This is the application half of the concurrency story; the SQL half is
in sql/040_functions.sql.

What it defends against, concretely:

  lost update   two carts read qty_available = 1 and both reserve it.
                Prevented by SELECT ... FOR UPDATE inside the function,
                which serialises writers on the contended row.

  deadlock      cart A holds SKU 7 and wants SKU 3 while cart B holds 3
                and wants 7. Prevented by sorting every multi-SKU
                reservation into a single global lock order, so a cycle
                cannot form.

  torn writes   an order row committed without its line items, or stock
                reserved for an order that then failed to insert.
                Prevented by doing the whole checkout in one
                transaction: it commits as a unit or not at all.

  thundering    a hot SKU under contention produces serialization
  retries       failures and lock timeouts. Handled with bounded
                exponential backoff plus jitter -- unjittered retries
                re-collide in lockstep.
"""

from __future__ import annotations

import logging
import random
import time
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Iterable, Iterator, Sequence, TypeVar

import psycopg
from psycopg import errors as pg_errors
from psycopg.rows import dict_row

from ..config import OPTIMIZED_SCHEMA, settings
from ..db import get_pool

log = logging.getLogger("queryquest.concurrency")

T = TypeVar("T")

# SQLSTATEs that mean "the database asked you to try again", not
# "your request was wrong". Retrying these is correct; retrying anything
# else just repeats a real error.
RETRYABLE = (
    pg_errors.SerializationFailure,   # 40001
    pg_errors.DeadlockDetected,       # 40P01
    pg_errors.LockNotAvailable,       # 55P03
)


class LockPolicy(str, Enum):
    WAIT = "wait"              # FOR UPDATE      -- queue behind the holder
    NOWAIT = "nowait"          # FOR UPDATE NOWAIT -- fail fast, retry
    SKIP_LOCKED = "skip"       # FOR UPDATE SKIP LOCKED -- worker queues


@dataclass
class ReservationResult:
    ok: bool
    variant_id: int | None = None
    quantity: int = 0
    attempts: int = 1
    reason: str = ""
    elapsed_ms: float = 0.0


# ---------------------------------------------------------------------
# Retry
# ---------------------------------------------------------------------
def with_retry(
    fn: Callable[[], T],
    *,
    max_retries: int | None = None,
    base_ms: int | None = None,
    on_retry: Callable[[int, Exception], None] | None = None,
) -> tuple[T, int]:
    """Run `fn`, retrying only on retryable SQLSTATEs. Returns (value, attempts).

    Backoff is exponential with full jitter. Fixed backoff is worse than
    useless under contention: every loser waits the same interval and
    collides again at exactly the same moment.
    """
    max_retries = settings.max_retries if max_retries is None else max_retries
    base_ms = settings.retry_base_ms if base_ms is None else base_ms

    attempt = 0
    while True:
        attempt += 1
        try:
            return fn(), attempt
        except RETRYABLE as exc:
            if attempt > max_retries:
                log.warning("giving up after %d attempts: %s", attempt, exc.__class__.__name__)
                raise
            delay = random.uniform(0, (base_ms / 1000) * (2 ** (attempt - 1)))
            if on_retry:
                on_retry(attempt, exc)
            log.debug("retry %d after %s, sleeping %.3fs", attempt, exc.__class__.__name__, delay)
            time.sleep(delay)


@contextmanager
def transaction(
    isolation: str | None = None, readonly: bool = False
) -> Iterator[psycopg.Connection]:
    """A pooled connection wrapped in one explicit transaction.

    psycopg commits on clean exit of the `with conn.transaction()` block
    and rolls back on any exception, so partial work cannot escape.
    """
    pool = get_pool()
    with pool.connection() as conn:
        if isolation:
            conn.isolation_level = getattr(psycopg.IsolationLevel, isolation.upper())
        if readonly:
            conn.read_only = True
        with conn.transaction():
            yield conn


# ---------------------------------------------------------------------
# Single-SKU reservation
# ---------------------------------------------------------------------
def reserve(
    variant_id: int,
    quantity: int,
    *,
    policy: LockPolicy = LockPolicy.WAIT,
    schema: str = OPTIMIZED_SCHEMA,
) -> ReservationResult:
    """Reserve stock for one SKU.

    Returns ok=False for "not enough stock" -- an expected business
    answer. Genuine faults still raise.
    """
    fn = "reserve_inventory_nowait" if policy is LockPolicy.NOWAIT else "reserve_inventory"
    t0 = time.perf_counter()

    def _once() -> bool:
        with transaction() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"SELECT {schema}.{fn}(%(vid)s, %(qty)s) AS ok",
                    {"vid": variant_id, "qty": quantity},
                )
                return bool(cur.fetchone()["ok"])

    try:
        ok, attempts = with_retry(_once)
    except RETRYABLE as exc:
        return ReservationResult(
            ok=False,
            variant_id=variant_id,
            quantity=quantity,
            attempts=settings.max_retries + 1,
            reason=f"contention: {exc.__class__.__name__}",
            elapsed_ms=(time.perf_counter() - t0) * 1000,
        )

    return ReservationResult(
        ok=ok,
        variant_id=variant_id,
        quantity=quantity,
        attempts=attempts,
        reason="" if ok else "insufficient stock",
        elapsed_ms=(time.perf_counter() - t0) * 1000,
    )


def release(variant_id: int, quantity: int, schema: str = OPTIMIZED_SCHEMA) -> bool:
    with transaction() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"SELECT {schema}.release_inventory(%(vid)s, %(qty)s) AS ok",
                {"vid": variant_id, "qty": quantity},
            )
            return bool(cur.fetchone()["ok"])


# ---------------------------------------------------------------------
# Multi-SKU reservation
# ---------------------------------------------------------------------
def reserve_cart(
    items: Sequence[tuple[int, int]], schema: str = OPTIMIZED_SCHEMA
) -> ReservationResult:
    """Reserve every line of a cart, or none of them.

    Lines are merged by SKU and sorted ascending before locking. That
    ordering is the deadlock prevention: two carts holding the same pair
    of SKUs can no longer wait on each other in a cycle, because they
    both take the lower id first.
    """
    if not items:
        return ReservationResult(ok=True, quantity=0)

    merged: dict[int, int] = {}
    for vid, qty in items:
        if qty <= 0:
            return ReservationResult(ok=False, variant_id=vid, reason="non-positive quantity")
        merged[vid] = merged.get(vid, 0) + qty

    ordered = sorted(merged.items())
    vids = [v for v, _ in ordered]
    qtys = [q for _, q in ordered]
    t0 = time.perf_counter()

    def _once() -> bool:
        with transaction() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"SELECT {schema}.reserve_many(%(vids)s, %(qtys)s) AS ok",
                    {"vids": vids, "qtys": qtys},
                )
                ok = bool(cur.fetchone()["ok"])
                if not ok:
                    # Undo every lock taken so far. Raising inside the
                    # transaction block is what guarantees this; returning
                    # False would commit the partial reservations.
                    raise _InsufficientStock()
                return True

    try:
        ok, attempts = with_retry(_once)
    except _InsufficientStock:
        return ReservationResult(
            ok=False,
            quantity=sum(qtys),
            reason="insufficient stock on at least one line",
            elapsed_ms=(time.perf_counter() - t0) * 1000,
        )
    except RETRYABLE as exc:
        return ReservationResult(
            ok=False,
            quantity=sum(qtys),
            reason=f"contention: {exc.__class__.__name__}",
            elapsed_ms=(time.perf_counter() - t0) * 1000,
        )

    return ReservationResult(
        ok=ok,
        quantity=sum(qtys),
        attempts=attempts,
        elapsed_ms=(time.perf_counter() - t0) * 1000,
    )


class _InsufficientStock(Exception):
    """Internal: unwinds the transaction so nothing partial commits."""


# ---------------------------------------------------------------------
# Checkout -- the full ACID demonstration
# ---------------------------------------------------------------------
CHECKOUT_ORDER_SQL = """
INSERT INTO {schema}.orders
    (order_uid, placed_at, customer_id, region_id, status, currency_code,
     subtotal, shipping_fee, tax_amount, total_amount, item_count, source_hash)
SELECT %(order_uid)s, %(placed_at)s, c.customer_id, c.region_id, 'pending',
       r.currency_code, %(subtotal)s, %(shipping)s, %(tax)s, %(total)s,
       %(item_count)s, sha256(%(order_uid)s::bytea)
FROM {schema}.customers c
JOIN {schema}.regions   r ON r.region_id = c.region_id
WHERE c.customer_id = %(customer_id)s
RETURNING order_id, placed_at
"""

CHECKOUT_ITEM_SQL = """
INSERT INTO {schema}.order_items
    (order_id, placed_at, line_no, variant_id, seller_id,
     quantity, unit_price, discount, line_total)
SELECT %(order_id)s, %(placed_at)s, %(line_no)s, v.variant_id, p.seller_id,
       %(qty)s, %(unit_price)s, 0, %(qty)s * %(unit_price)s
FROM {schema}.product_variants v
JOIN {schema}.products p ON p.product_id = v.product_id
WHERE v.variant_id = %(variant_id)s
"""


@dataclass
class CheckoutResult:
    ok: bool
    order_id: int | None = None
    order_uid: str = ""
    total: float = 0.0
    attempts: int = 1
    reason: str = ""


def checkout(
    customer_id: int,
    order_uid: str,
    items: Sequence[tuple[int, int, float]],
    *,
    placed_at=None,
    schema: str = OPTIMIZED_SCHEMA,
) -> CheckoutResult:
    """Reserve stock, create the order and its lines -- atomically.

    Every statement runs in ONE transaction. If the last line item
    violates a constraint, the stock reservation from the first
    statement is rolled back with it. Splitting this into "reserve, then
    create order" across two transactions is the bug that produces
    phantom reservations against orders that never existed.

    items: (variant_id, quantity, unit_price)
    """
    if not items:
        return CheckoutResult(ok=False, reason="empty cart")

    merged: dict[int, tuple[int, float]] = {}
    for vid, qty, price in items:
        q, p = merged.get(vid, (0, price))
        merged[vid] = (q + qty, p)
    ordered = sorted(merged.items())          # global lock order, again

    subtotal = round(sum(q * p for _, (q, p) in ordered), 2)
    shipping = 4.99 if subtotal < 50 else 0.0
    tax = round(subtotal * 0.08, 2)
    total = round(subtotal + shipping + tax, 2)
    item_count = sum(q for _, (q, _) in ordered)

    def _once() -> CheckoutResult:
        with transaction() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                # 1. Reserve, in ascending variant_id order.
                for vid, (qty, _price) in ordered:
                    cur.execute(
                        f"SELECT {schema}.reserve_inventory(%s, %s) AS ok", (vid, qty)
                    )
                    if not cur.fetchone()["ok"]:
                        raise _InsufficientStock(f"variant {vid}")

                # 2. Create the order header.
                cur.execute(
                    CHECKOUT_ORDER_SQL.format(schema=schema),
                    {
                        "order_uid": order_uid,
                        "placed_at": placed_at,
                        "customer_id": customer_id,
                        "subtotal": subtotal,
                        "shipping": shipping,
                        "tax": tax,
                        "total": total,
                        "item_count": item_count,
                    },
                )
                header = cur.fetchone()
                if header is None:
                    raise _InsufficientStock(f"unknown customer {customer_id}")

                # 3. Create the lines.
                for line_no, (vid, (qty, price)) in enumerate(ordered, start=1):
                    cur.execute(
                        CHECKOUT_ITEM_SQL.format(schema=schema),
                        {
                            "order_id": header["order_id"],
                            "placed_at": header["placed_at"],
                            "line_no": line_no,
                            "variant_id": vid,
                            "qty": qty,
                            "unit_price": price,
                        },
                    )

                return CheckoutResult(
                    ok=True,
                    order_id=header["order_id"],
                    order_uid=order_uid,
                    total=total,
                )

    try:
        result, attempts = with_retry(_once)
        result.attempts = attempts
        return result
    except _InsufficientStock as exc:
        return CheckoutResult(ok=False, order_uid=order_uid, reason=f"insufficient stock: {exc}")
    except RETRYABLE as exc:
        return CheckoutResult(
            ok=False, order_uid=order_uid, reason=f"contention: {exc.__class__.__name__}"
        )


# ---------------------------------------------------------------------
# Worker queue
# ---------------------------------------------------------------------
def claim_restock(limit: int = 50, schema: str = OPTIMIZED_SCHEMA) -> list[dict]:
    """Claim a slice of the restock queue with SKIP LOCKED.

    N workers can call this at once: each gets a disjoint set of rows,
    none of them blocks, and no row is processed twice.
    """
    with transaction() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f"SELECT * FROM {schema}.claim_restock_batch(%s)", (limit,))
            return cur.fetchall()


def inventory_totals(schema: str = OPTIMIZED_SCHEMA) -> dict:
    """Aggregate invariant check, used by the tests as a global assertion."""
    with transaction(readonly=True) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT count(*)                                   AS skus,
                       coalesce(sum(qty_on_hand), 0)              AS on_hand,
                       coalesce(sum(qty_reserved), 0)             AS reserved,
                       count(*) FILTER (WHERE qty_reserved > qty_on_hand) AS oversold
                FROM {schema}.inventory
                """
            )
            return cur.fetchone()
