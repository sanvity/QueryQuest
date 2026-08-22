# The load pipeline

One chunk of source rows goes through six steps inside a single
transaction:

```
COPY  ->  dedup  ->  data quality  ->  key resolution  ->  UPSERT  ->  post-fix
```

The engine (`queryquest/etl/loader.py`) is generic. It builds every
statement from a `TableSpec` (`queryquest/etl/specs.py`), so adding a
table is a declaration, not new code:

```python
CUSTOMERS = TableSpec(
    table="customers",
    stage_columns=(("customer_uid", "text"), ("email", "text"), ...),
    stage_dedup_key=("customer_uid",),
    conflict_key=("customer_uid",),
    hash_columns=("email", "full_name", "region_code", "status", ...),
    lookups=(Lookup("region_code", "regions", "region_code", "region_id", "region_id"),),
    rules=(not_blank("customer_uid"), Rule("email_shape", ...), ...),
)
```

---

## 1. COPY into staging

`COPY` into an UNLOGGED temp table, then a set-based UPSERT out of it.

Row-by-row `INSERT ... ON CONFLICT` over ten million records is roughly
two orders of magnitude slower: every row pays a network round trip and
a WAL flush. Temp tables are implicitly unlogged, so staging writes
generate no WAL at all.

The CSV is streamed in chunks rather than read whole — a ten-million-row
feed is several GB of Python objects if you materialise it. Empty
strings become `NULL` on the way in, because a missing value in a CSV is
absence, not the empty string, and letting `''` through defeats every
not-null rule downstream.

## 2. Deduplication

```sql
CREATE TEMP TABLE stg_dedup AS
SELECT DISTINCT ON (customer_uid) *
FROM stg_raw
ORDER BY customer_uid, source_updated_at DESC NULLS LAST;
```

Feeds redeliver. A single file routinely contains the same natural key
more than once — an update that happened after the extract started, or a
retried upstream publish.

The explicit `ORDER BY` is what makes the winner *deterministic*.
`DISTINCT ON` without it returns whichever duplicate the heap felt like
that day, which turns a data question into a coin flip.

## 3. Data quality

Rules are boolean SQL expressions evaluated against the staging alias.
A row that fails one is moved to `qq_meta.etl_rejects` — with the rule
that caught it, a human-readable reason, its natural key and its full
payload as `jsonb` — and deleted from staging. The batch continues.

```python
Rule(
    name="total_reconciles",
    predicate="abs(s.total_amount - (s.subtotal + s.shipping_fee + s.tax_amount)) <= 0.01",
    description="total_amount must equal subtotal + shipping + tax (1c tolerance)",
)
```

Three properties of that design matter more than the rule list:

**NULL counts as failure.** A predicate that cannot be evaluated has
not passed. The alternative silently admits every row with a missing
value.

**Quarantine, not crash.** Losing a million good rows because a hundred
are malformed is the wrong trade every time. The payload is preserved so
an operator can replay the quarantine once upstream is fixed.

**A ceiling on the quarantine.** Past `QQ_MAX_REJECT_RATIO` (2% by
default) the chunk is rolled back and the batch is marked
`quarantined`. Half-loading garbage is worse than loading nothing, and a
batch that is mostly broken is a signal, not a data-cleaning exercise.

Rules deliberately duplicate some CHECK constraints — `inventory` has
both a `reserved_within_on_hand` rule and a matching CHECK. The
constraint would abort the whole transaction; the rule turns that into
one quarantined row and a load that still succeeds. The constraint stays
as the backstop it should be.

## 4. Key resolution

The feeds are flat and speak business keys: `customer_uid`,
`region_code`, `sku`, `order_uid`. The schema is normalized and speaks
surrogates. Resolution happens in the same statement as the insert, via
`LEFT JOIN`:

```sql
INSERT INTO qq.orders (order_uid, placed_at, customer_id, region_id, ...)
SELECT s.order_uid, s.placed_at, r_customer_id.customer_id, r_region_id.region_id, ...
FROM stg_dedup s
LEFT JOIN qq.customers AS r_customer_id ON r_customer_id.customer_uid = s.customer_uid
LEFT JOIN qq.regions   AS r_region_id   ON r_region_id.region_code   = s.region_code
```

Rows whose lookups do not resolve are rejected *before* the insert
rather than left to the foreign key. This is not belt-and-braces: an FK
violation aborts the entire transaction and takes a million good rows
down with one orphan. Catching it first turns that into a quarantined
row and a load that completes.

Feeds are walked in dependency order for the same reason —
`order_items` cannot resolve an `order_uid` until `orders` has landed.

## 5. The UPSERT

```sql
INSERT INTO qq.customers (...)
SELECT ... FROM stg_dedup s LEFT JOIN ...
ON CONFLICT (customer_uid) DO UPDATE
   SET email = EXCLUDED.email, ..., updated_at = now()
 WHERE customers.source_hash IS DISTINCT FROM EXCLUDED.source_hash
RETURNING 1
```

The `WHERE` on `DO UPDATE` is the whole idempotency story. Without it, a
nightly re-run rewrites every row: same values, brand new tuple
versions, doubled dead tuples, bloated indexes, and an autovacuum storm
at 3am. With it, an unchanged replay performs **zero writes**.

Change detection has two implementations, chosen per table:

- **Content hash** for dimensions. `sha256` over the business columns,
  computed in SQL. Each column is `coalesce(col::text, '~NULL~')` before
  concatenation — otherwise `('a', NULL, 'b')` and `('a', 'b', NULL)`
  hash identically and a real change reads as no change.
- **Column comparison** for `order_items`. At tens of millions of rows a
  32-byte digest per row is real storage for no extra signal, so the
  guard compares the value columns directly.

### Counting inserts vs updates

The usual trick is `RETURNING (xmax = 0)`. PostgreSQL rejects it on
partitioned tables — system columns are not exposed through a
partitioned `RETURNING` clause. So the loader counts how many staged
natural keys already exist *before* the UPSERT and derives the rest:

```
inserted  = surviving - already_present
updated   = written   - inserted
unchanged = surviving - written
```

One extra semi-join per chunk, and the counts are correct on partitioned
and plain tables alike.

## 6. Post-fix

An optional statement in the same transaction. `categories` uses it to
stitch up its self-referencing hierarchy: parents cannot be resolved in
the same `INSERT` that creates them, so a second set-based pass links
children to parents once every node exists.

---

## Idempotency, precisely

`qq_meta.etl_batches` is the ledger. `batch_key` is `sha256` over the
source name, the target schema and the file's bytes, so identical input
produces an identical key and collides with the unique constraint.

Two properties, both asserted in `tests/test_etl_idempotency.py`:

1. **Recognised.** Re-running the same feed reports `skipped_duplicate`
   and reads zero rows.
2. **Inert.** Re-running with `--force` bypasses the ledger, re-reads
   every row, and still performs zero inserts and zero updates —
   because the change guard finds nothing different.

The second is the one that matters. A pipeline that merely refuses to
run twice is not idempotent, it is locked.

Measured on the full dataset:

```
customers   15029 read / 0 inserted / 0 updated / 14975 unchanged
orders      60000 read / 0 inserted / 0 updated / 59626 unchanged
order_items 155664 read / 0 inserted / 0 updated / 154412 unchanged
```

## Chunked commits

Each chunk commits separately. A ten-million-row load inside one
transaction pins a single snapshot for the whole run, blocks vacuum,
grows the WAL without bound, and leaves you with nothing if it dies at
90%.

Chunked commits make the load resumable — and resumability is only safe
*because* the UPSERT is idempotent, so replaying the already-loaded
prefix writes nothing. The two design choices only work together.

## What the ledger records

| Column | Meaning |
|:---|:---|
| `rows_read` | rows pulled from the feed |
| `rows_duplicate` | collapsed by in-batch dedup |
| `rows_rejected` | quarantined by a DQ or referential rule |
| `rows_inserted` | new rows |
| `rows_updated` | existing rows that genuinely changed |
| `rows_unchanged` | existing rows the guard skipped |

`qq_meta.dq_results` holds one row per rule per batch — the audit trail
that answers "was the data good on the 14th?" months later.
