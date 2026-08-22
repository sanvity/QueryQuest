# Concurrency and ACID

One row per SKU in `qq.inventory`, and many writers wanting it at once.
That is the whole problem, and it is enough to demonstrate every failure
mode worth knowing about.

## The invariant

```
qty_reserved <= qty_on_hand,  always, under any interleaving
```

It is defended in three independent layers, on purpose:

1. `SELECT ... FOR UPDATE` serialises writers on the contended row.
2. The `UPDATE` re-checks availability inside the same transaction.
3. `CHECK (qty_reserved <= qty_on_hand)` aborts the transaction if the
   first two are ever wrong.

Layer 3 is not redundant with layers 1 and 2. It is what catches the
code path someone adds next year that forgets to take the lock.

## Lost update, demonstrated rather than asserted

`sql/040_functions.sql` ships two versions of the same function. The
second one is a bug, kept deliberately:

```sql
-- reserve_inventory: correct
SELECT qty_available INTO v_available
FROM qq.inventory WHERE variant_id = p_variant_id
FOR UPDATE;                                   -- <-- serialises writers

-- unsafe_reserve_inventory: the textbook race
SELECT qty_available INTO v_available
FROM qq.inventory WHERE variant_id = p_variant_id;   -- no lock
PERFORM pg_sleep(0.01);                              -- widen the window
```

`tests/test_concurrency.py` runs both:

- `test_concurrent_reservations_never_oversell` — 40 threads race for
  20 units. Exactly 20 win, every time.
- `test_the_unlocked_version_actually_oversells` — the same workload
  through the unlocked function either grants more reservations than
  there was stock, or forces the CHECK constraint to abort transactions
  to stop it.

The second test is what makes the first one meaningful. Without a
control case, "no overselling occurred" could just mean the test never
created real contention.

## Deadlock prevention by lock ordering

Cart A holds SKU 7 and wants SKU 3. Cart B holds SKU 3 and wants SKU 7.
Locking in cart order deadlocks roughly half the time.

`reserve_many` locks in a single global order:

```sql
SELECT i.variant_id, i.qty_available
FROM qq.inventory i
WHERE i.variant_id = ANY (p_variant_ids)
ORDER BY i.variant_id            -- <-- the entire trick
FOR UPDATE;
```

A wait cycle cannot form if every transaction acquires locks in the same
order. This makes deadlock *impossible* rather than merely unlikely —
`test_opposing_cart_orders_do_not_deadlock` runs two carts with the same
two SKUs in opposite order, 30 synchronised rounds, and expects zero
deadlocks.

The application layer sorts too, before it ever calls the function,
because a cart that arrives as `[(7, 1), (3, 2)]` must not be allowed to
set the order.

## Three lock policies, three use cases

| Policy | SQL | When |
|:---|:---|:---|
| Wait | `FOR UPDATE` | default — correctness matters more than latency |
| Fail fast | `FOR UPDATE NOWAIT` | latency-sensitive callers that prefer retrying to blocking; raises SQLSTATE `55P03` |
| Step over | `FOR UPDATE SKIP LOCKED` | worker queues |

`SKIP LOCKED` is the opposite policy to the other two: instead of
queueing behind a busy row, ignore it. N restock workers drain the same
queue concurrently with zero contention and zero double-processing.
`claim_restock_batch` uses it.

## Retry policy

```python
RETRYABLE = (
    pg_errors.SerializationFailure,   # 40001
    pg_errors.DeadlockDetected,       # 40P01
    pg_errors.LockNotAvailable,       # 55P03
)
```

Only these. They mean "the database asked you to try again", not "your
request was wrong". `test_retry_only_retries_retryable_errors` asserts
that an `UndefinedColumn` surfaces on the first attempt — retrying a
programming error just delays the bug report by five attempts.

Backoff is exponential with **full jitter**:

```python
delay = random.uniform(0, (base_ms / 1000) * (2 ** (attempt - 1)))
```

Fixed backoff is worse than useless under contention: every loser waits
the same interval and collides again at exactly the same moment.

## Atomic checkout

`checkout()` does everything in one transaction:

1. Reserve every line, in ascending `variant_id` order.
2. Insert the order header.
3. Insert the line items.

If step 3 fails, step 1 is rolled back with it. Splitting this into
"reserve, then create the order" across two transactions is the bug that
produces phantom reservations against orders that never existed — stock
that is unavailable to customers and attached to nothing.

Two tests cover it:

- `test_failed_checkout_leaves_no_trace` — a checkout that cannot be
  satisfied reserves nothing and leaves no partial order.
- `test_concurrent_checkouts_conserve_stock` — 25 concurrent checkouts
  against 30 units at 2 units each. Exactly 15 succeed, `qty_reserved`
  lands on 30, and the sum of `order_items.quantity` equals it. Reserved
  units and sold units cannot diverge.

## Session guards

Every connection sets:

```sql
SET statement_timeout = 120000;
SET lock_timeout = 5000;
```

`lock_timeout` is the one that matters here. Without it a transaction
waiting on a row lock waits forever, and a single stuck writer becomes a
pile of parked threads. With it, contention surfaces as a fast,
retryable `LockNotAvailable` instead.

The pool's `configure` hook commits after those `SET`s. An
idle-in-transaction connection sitting in a pool pins the oldest
snapshot and blocks vacuum — psycopg's pool rejects such connections,
correctly, and finding out why was worth the comment now in the code.

## What is not covered

- **No distributed transactions.** Single database, single node. Adding
  a second datastore to the checkout path would require an outbox and a
  different set of tests.
- **Reservations do not expire.** A production system needs a sweeper
  for abandoned carts; `release_inventory` is the primitive it would
  call, but the scheduler is not built.
- **`SERIALIZABLE` is available but not the default.** `transaction()`
  accepts an isolation level; the reservation path uses `READ COMMITTED`
  with explicit row locks, which is cheaper than serialisable snapshot
  isolation for a workload whose contention is this well understood.
