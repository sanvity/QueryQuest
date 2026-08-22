# How the query work was done

The method matters more than the numbers, because the numbers depend on
the box. What follows is the process, and how to re-run it.

## The setup

Two schemas, identical rows:

| | `qq_baseline` | `qq` |
|:---|:---|:---|
| Tables | plain heaps | `orders`, `order_items` RANGE partitioned monthly |
| Indexes | primary and unique keys only | composite, partial, covering, BRIN |
| Rollups | none | four materialized views in `qq_mart` |
| Statistics | default | extended stats on correlated columns |

Both are rendered from the same DDL file with a `${SCHEMA}` placeholder,
so they cannot drift apart —
`test_baseline_mirrors_optimized_columns` fails if they do.

The baseline keeps its keys, deliberately. It is meant to be the schema
someone writes before thinking about access patterns, not a straw man
with no primary key.

## Rules the harness follows

A benchmark that flatters itself is worthless, so:

- **Same parameter values for both variants.** The parameter sequence is
  generated once from a seeded RNG and replayed, so the optimized run
  answers the identical questions the baseline did.
- **Fresh parameters per repetition.** Hammering one `customer_id`
  measures how warm one buffer is, not how the index performs.
- **Warm-up repetitions, discarded.** The first execution of any query
  pays for plan generation and a cold cache. Timing it measures start-up.
- **Variants interleaved per query**, not run in two blocks, so a
  gradually warming page cache cannot systematically favour whichever
  ran second.
- **The result set is fully drained.** Timing only `execute` measures how
  long the server took to *start* returning rows.
- **p95 reported next to the mean**, because the mean hides exactly the
  tail users feel.
- **`EXPLAIN (ANALYZE, BUFFERS)` captured for every query**, and the
  buffer counts published alongside the timings.

Every measurement is written to `qq_meta.query_benchmarks`, and
`docs/BENCHMARKS.md` is generated from that table. No number in that
file is typed by hand.

## Why BUFFERS is the honest metric

`shared_read` is pages fetched from disk; `shared_hit` is pages already
in the buffer cache. Wall-clock time moves for all sorts of reasons —
a noisy neighbour, a warm cache, a lucky sample. Buffer counts do not.
An optimisation that turns reads into hits, or removes pages from the
scan entirely, is doing real work on any hardware.

This is also why the test suite asserts on *plan shape* rather than on
latency. `tests/test_query_plans.py` checks that Q1 uses
`orders_customer_placed_idx` and performs no sort, that a 90-day query
prunes to a handful of monthly partitions, and that the mart queries do
not touch `order_items` at all. If someone drops an index, that fails
immediately and names the reason, instead of the p95 quietly drifting
upward for a month.

## The techniques, and what each one is for

### Composite indexes: equality, then range, then payload

```sql
CREATE INDEX orders_customer_placed_idx
    ON qq.orders (customer_id, placed_at DESC)
    INCLUDE (status, total_amount, item_count);
```

Column order is not stylistic. `WHERE customer_id = ? AND placed_at >= ?
ORDER BY placed_at DESC` matches equality-then-range exactly, so the
index delivers rows already sorted and the `Sort` node disappears from
the plan.

`INCLUDE` carries the projected columns in the leaf pages, making the
scan index-only: no heap fetch per row. The payload is not part of the
key, so it costs nothing in comparisons.

### Partial indexes: index the rows you query

```sql
CREATE INDEX orders_open_backlog_idx
    ON qq.orders (placed_at)
    INCLUDE (order_id, customer_id, total_amount)
    WHERE status IN ('pending', 'paid');
```

Only a small fraction of orders are ever open. A full index on `status`
would be an order of magnitude larger and consist mostly of rows this
query can never want. The partial index is small enough to stay resident
in `shared_buffers`, which is the entire point — the ops queue is polled
continuously, and it should cost buffer hits, not a scan.

`inventory_restock_idx` does the same thing over a *cross-column*
predicate, `WHERE qty_available <= reorder_point`.

### BRIN where physical order tracks logical order

```sql
CREATE INDEX order_items_placed_brin_idx
    ON qq.order_items USING brin (placed_at) WITH (pages_per_range = 128);
```

`order_items` is loaded in `placed_at` order and never updated — the
ideal BRIN case. It costs a few hundred KB where a btree would cost
hundreds of MB. It cannot replace the btrees for selective lookups; it
exists to cheaply skip whole segments on wide scans.

### Partition pruning

A 90-day query against a 24-month table reads four partitions and never
opens the other twenty. That is not an index optimisation — it is the
planner declining to look at 85% of the table. Combined with the shared
partition key on `order_items`, it also enables partition-wise joins.

### Materialized views: not a faster scan, no scan

```sql
CREATE MATERIALIZED VIEW qq_mart.seller_daily_sales AS
SELECT date_trunc('day', placed_at)::date AS day, seller_id,
       sum(quantity) AS units_sold, sum(line_total) AS revenue, ...
FROM qq.order_items GROUP BY 1, 2;
```

Aggregated at **day** grain so any window — 7d, 30d, quarter-to-date —
is a cheap re-sum rather than a re-scan. The dashboard reads a few
thousand pre-aggregated rows instead of tens of millions of order lines.

Every mart view carries a `UNIQUE` index, which is not optional:
`REFRESH MATERIALIZED VIEW CONCURRENTLY` requires one. Without it the
refresh takes an `ACCESS EXCLUSIVE` lock and the dashboard goes dark for
the duration of the rebuild.

`refresh_all()` checks `pg_class.relispopulated` and falls back to a
blocking refresh on first run, because `CONCURRENTLY` needs a populated
view to diff against — otherwise a cold deploy fails.

### Extended statistics

```sql
CREATE STATISTICS orders_cust_region_stx (dependencies, ndistinct)
    ON customer_id, region_id, status FROM qq.orders;
```

`customer_id` and `region_id` are correlated — a customer sits in one
region. Without being told, the planner multiplies their selectivities,
badly underestimates the row count, and picks a nested loop where a hash
join is correct. This is the failure mode where every index is right and
the query is still slow.

### ANALYZE after bulk loading

Skipping it is the single most common reason a correctly-indexed schema
still produces terrible plans: the planner is working from an
empty-table estimate and chooses a nested loop over ten million rows.
`queryquest.cli refresh` runs it as part of the refresh cycle.

## What this costs

Worth stating plainly, because indexes are not free:

- Loading `order_items` into `qq` takes roughly **4x longer** than into
  `qq_baseline` — five secondary indexes plus partition routing on every
  insert. This shows up directly in the ETL timings in
  `docs/BENCHMARKS.md`.
- Materialized views are stale between refreshes. That is acceptable for
  dashboards and wrong for anything transactional, which is why the
  checkout path never reads them.
- Partitioning forces composite primary keys and composite foreign keys
  throughout, and constrains what a unique constraint can even mean.

## Reproducing it

```bash
docker compose up -d
python -m queryquest.cli init
python scripts/generate_data.py --scale full   # ~10M rows
python -m queryquest.cli load
python -m queryquest.cli refresh
python -m queryquest.cli bench --reps 20
python -m queryquest.cli report                # regenerates docs/BENCHMARKS.md
```

`--scale demo` runs the same thing in a couple of minutes at ~350k rows.
The absolute numbers will differ on your hardware; the plan shapes and
buffer ratios should not.
