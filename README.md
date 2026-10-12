# QueryQuest

A normalized PostgreSQL marketplace warehouse, an idempotent ETL
pipeline that loads it, and a performance story that is **measured
rather than asserted**.

[![CI](https://github.com/sanvity/QueryQuest/actions/workflows/ci.yml/badge.svg)](https://github.com/sanvity/QueryQuest/actions/workflows/ci.yml)

---

## The idea

Most database projects show you a fast query. This one ships the slow
one too.

The same DDL is rendered into two schemas holding **identical rows**:

- **`qq_baseline`** — plain heap tables, primary and unique keys only.
  The schema you write before thinking about access patterns.
- **`qq`** — RANGE partitioned facts, composite / partial / covering /
  BRIN indexes, four materialized views, extended statistics.

Every performance number in this repository is a measured A/B between
those two, on one box, with the same parameter values and warm-up
repetitions discarded. `docs/BENCHMARKS.md` is **generated** from the
`qq_meta.query_benchmarks` table the harness writes to — nothing in it
is typed by hand, so it cannot drift from what was actually observed.

<!--RESULTS-->

---

## Quickstart

```bash
git clone https://github.com/sanvity/QueryQuest.git
cd QueryQuest
pip install -r requirements-dev.txt

docker compose up -d
export QQ_DSN=postgresql://queryquest:queryquest@localhost:5432/queryquest

make demo        # init -> generate -> load -> refresh -> bench -> loadtest -> report
```

`make demo` takes a couple of minutes at ~350k rows. `make full`
generates ~14.7M records and takes the better part of an hour.

**No Docker?** Both servers can run in user space, from pip wheels, with
no root and no container runtime:

```bash
python scripts/devserver.py serve      # leave running
eval "$(python scripts/devserver.py env)"
make demo
```

(`pgserver` and `redislite` ship real PostgreSQL and Redis binaries
inside a wheel. POSIX only — on Windows, use Docker or run it in WSL.)

---

## What is in here

```
sql/
  000_meta.sql          pipeline control plane: batch ledger, rejects, DQ verdicts
  001_schema.sql        the 12 domain tables, rendered twice via ${SCHEMA}
  002_partitions.sql    monthly RANGE partitions + ensure_partitions()
  003_keys.sql          primary / natural / foreign keys
  020_indexes.sql       every index, with the query it serves in a comment
  030_matviews.sql      the reporting mart + concurrent refresh
  040_functions.sql     transactional inventory primitives (FOR UPDATE, SKIP LOCKED)
  queries/              the benchmark suite, one file per query

queryquest/
  db.py                 connections, pool, schema bootstrap, ${SCHEMA} rendering
  etl/specs.py          declarative TableSpec per table -- the whole config surface
  etl/loader.py         COPY -> dedup -> DQ -> key resolution -> UPSERT
  etl/pipeline.py       batch ledger, idempotency, chunked commits, quarantine
  cache/redis_cache.py  read-through cache: single flight, jitter, versioned keys
  cache/repository.py   the cached read paths
  concurrency/          row locking, lock ordering, retry policy, atomic checkout
  bench/                benchmark harness, cache load test, report generator
  cli.py                one entry point for all of it

tests/                  runs against real PostgreSQL and real Redis
docs/                   SCHEMA / ETL / OPTIMIZATION / CONCURRENCY / BENCHMARKS
```

---

## The four pieces

### 1. A 12-table normalized schema → [`docs/SCHEMA.md`](docs/SCHEMA.md)

Third normal form, with two denormalisations that each have a stated
reason. `orders.region_id` is duplicated from the customer because a
customer who relocates must not retroactively move last year's revenue.
`order_items.placed_at` is copied down from the parent because it is
what makes the child partitionable on the same key.

Invariants live in the schema, not in hope:

```sql
qty_available integer GENERATED ALWAYS AS (qty_on_hand - qty_reserved) STORED,
CONSTRAINT inventory_oversell_ck CHECK (qty_reserved <= qty_on_hand)
```

### 2. An idempotent ETL pipeline → [`docs/ETL.md`](docs/ETL.md)

`COPY → dedup → data quality → key resolution → UPSERT`, all set-based,
all driven from a declarative `TableSpec`. Adding a table is a
declaration, not new code.

Idempotency here means something stronger than "re-running does not
error":

```
customers    15029 read / 0 inserted / 0 updated / 14975 unchanged
orders       60000 read / 0 inserted / 0 updated / 59626 unchanged
order_items 155664 read / 0 inserted / 0 updated / 154412 unchanged
```

That is a **forced** replay with the batch ledger bypassed. Zero writes,
because `ON CONFLICT DO UPDATE ... WHERE source_hash IS DISTINCT FROM
EXCLUDED.source_hash` skips rows that did not actually change. Without
that guard a nightly re-run rewrites every row: same values, new tuple
versions, doubled dead tuples, autovacuum storm.

Bad rows are **quarantined, not fatal**. A malformed email or a total
that does not reconcile goes to `qq_meta.etl_rejects` with the rule that
caught it and its full payload; the batch keeps going. Past a
configurable reject ratio the chunk is rolled back instead — partial
garbage is worse than no data.

### 3. Query optimization → [`docs/OPTIMIZATION.md`](docs/OPTIMIZATION.md)

Every index in `sql/020_indexes.sql` names the query it serves and the
plan node it removes. Indexes without a named consumer are cost, not
benefit.

The techniques, and what each is actually for:

- **Composite, equality-then-range-then-payload.**
  `(customer_id, placed_at DESC) INCLUDE (status, total_amount)` makes
  the `ORDER BY` free and the scan index-only.
- **Partial indexes.** The open-order backlog indexes only rows that are
  still open — small enough to stay in `shared_buffers`, which is the
  point when a queue is polled continuously.
- **BRIN** on `order_items(placed_at)`, where physical order tracks
  logical order: hundreds of KB instead of hundreds of MB.
- **Partition pruning.** A 90-day query reads four monthly partitions
  and never opens the other twenty.
- **Materialized views** at day grain, so any window is a re-sum rather
  than a re-scan. Each carries the `UNIQUE` index that
  `REFRESH ... CONCURRENTLY` requires.
- **Extended statistics** on correlated columns, for the failure mode
  where every index is right and the plan is still a nested loop.

The test suite asserts on **plan shape**, not latency — that Q1 uses the
composite index and performs no sort, that a 90-day query prunes to a
handful of partitions, that the mart queries never touch `order_items`.
Drop an index and it fails immediately with a reason, instead of the p95
drifting upward for a month.

### 4. Caching and concurrency → [`docs/CONCURRENCY.md`](docs/CONCURRENCY.md)

**Redis read-through cache** where the interesting parts are the failure
modes: it fails open when Redis is down (a cache that can take the site
down is a liability), collapses concurrent misses into a single load
(the stampede is the failure mode that actually kills systems), jitters
TTLs so keys written together do not expire together, caches negative
lookups, and invalidates whole entity classes with an `INCR` on a
version prefix rather than a `KEYS` scan that blocks the server.

One finding worth keeping: the first version memoised nothing and looked
up the entity version on every call, so each cache hit cost **two**
Redis round trips — and measured *slower* than no cache at all. The fix
is a one-second in-process memo, and the trade it makes is documented in
the code.

**Row-level locking** with the bug preserved next to the fix.
`sql/040_functions.sql` ships `reserve_inventory` (with `FOR UPDATE`)
and `unsafe_reserve_inventory` (without). The test suite runs both: 40
threads race for 20 units, exactly 20 win through the locking path, and
the unlocked path oversells. The control case is what makes the first
result mean anything.

Multi-SKU carts sort into a single global lock order, which makes
deadlock impossible rather than unlikely. Retries cover only
`SerializationFailure`, `DeadlockDetected` and `LockNotAvailable`, with
exponential backoff and full jitter — retrying a programming error just
delays the bug report.

---

## Tests

```bash
make test
```

The suite runs against a **real** PostgreSQL and a **real** Redis, in CI
too. That is deliberate: `FOR UPDATE`, `ON CONFLICT`, partition pruning,
`SKIP LOCKED` and Redis expiry are precisely the behaviours a mock
cannot have an opinion about. A mocked row lock proves nothing about
whether stock can be oversold.

| File | Covers |
|:---|:---|
| `test_schema.py` | 12 tables, partition layout, constraints, index presence, baseline purity |
| `test_etl_idempotency.py` | batch short-circuit, forced-replay inertness, dedup last-write-wins |
| `test_dq.py` | quarantine routing, referential rejects, reject-ratio ceiling |
| `test_query_plans.py` | index usage, sort elimination, partition pruning, buffer counts |
| `test_cache.py` | hit/miss, negative caching, single flight, jitter, fail-open |
| `test_concurrency.py` | no oversell, lost update demonstrated, deadlock avoidance, atomic checkout |

---

## CLI

```
python -m queryquest.cli init       create schemas, partitions, indexes, marts
                        load       run the ETL into both schemas
                        refresh     refresh materialized views + ANALYZE
                        bench       baseline vs optimized query latency
                        loadtest    cached vs uncached read throughput
                        report      regenerate docs/BENCHMARKS.md
                        status      row counts, batches, quarantine summary
                        pipeline    all of the above, in order
                        reset       drop every schema (destructive, needs --yes)
```

---

## Honest limitations

- **Synthetic data.** Distributions are power-law skewed on purpose
  (uniform data makes every index look equally good), but it is still
  generated, not production traffic.
- **Single node.** No replication, no distributed transactions, no
  outbox. Adding a second datastore to the checkout path would need all
  three.
- **Reservations never expire.** A production system needs a sweeper for
  abandoned carts. `release_inventory` is the primitive it would call;
  the scheduler is not built.
- **Numbers are hardware-dependent.** The measurements in
  `docs/BENCHMARKS.md` come from the machine that ran them. The plan
  shapes and buffer ratios should reproduce anywhere; the milliseconds
  will not.
- **The mart is stale between refreshes.** Fine for dashboards, wrong
  for anything transactional — which is why the checkout path never
  reads it.

---

## License

MIT — see [LICENSE](LICENSE).
