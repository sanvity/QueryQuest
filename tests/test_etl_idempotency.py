"""Idempotency and deduplication.

The load pipeline claims a strong property: running the same feed twice
leaves the database in exactly the same state, and the second run writes
nothing. These tests hold it to that, in both of the ways it can be
true -- short-circuited by the batch ledger, and replayed but inert.
"""

from __future__ import annotations

import pytest

from queryquest.db import fetch_value
from queryquest.etl import pipeline, specs

SCHEMA = "qq"


def _count(conn, table: str) -> int:
    return int(fetch_value(conn, f"SELECT count(*) FROM {SCHEMA}.{table}"))


@pytest.fixture
def loaded(conn, feed_dir):
    results = pipeline.run_all(conn, feed_dir, schema=SCHEMA)
    assert results, "no feeds were loaded"
    for r in results:
        assert r.ok, f"{r.table} finished as {r.state}: {r.error}"
    return results


def test_first_load_inserts(loaded):
    by_table = {r.table: r for r in loaded}
    assert by_table["customers"].stats.rows_inserted > 0
    assert by_table["orders"].stats.rows_inserted > 0
    assert by_table["order_items"].stats.rows_inserted > 0


def test_same_feed_is_recognised_and_skipped(conn, feed_dir, loaded):
    """Property 1: the batch ledger short-circuits identical input."""
    again = pipeline.run_all(conn, feed_dir, schema=SCHEMA)
    assert all(r.state == "skipped_duplicate" for r in again), [
        (r.table, r.state) for r in again
    ]
    assert all(r.stats.rows_read == 0 for r in again)


def test_forced_replay_writes_nothing(conn, feed_dir, loaded):
    """Property 2, the stronger one: even with the ledger bypassed, the
    change guard means a replay performs zero inserts and zero updates.

    This is what stops a nightly re-run from rewriting every row, doubling
    the table's dead tuples and triggering an autovacuum storm.
    """
    before = {t: _count(conn, t) for t in ("customers", "orders", "order_items", "payments")}

    again = pipeline.run_all(conn, feed_dir, schema=SCHEMA, force=True)

    for r in again:
        assert r.stats.rows_inserted == 0, f"{r.table} inserted {r.stats.rows_inserted} on replay"
        assert r.stats.rows_updated == 0, f"{r.table} updated {r.stats.rows_updated} on replay"
        assert r.stats.rows_read > 0, f"{r.table} did not actually re-read the feed"

    after = {t: _count(conn, t) for t in before}
    assert before == after


def test_changed_row_is_updated_not_duplicated(conn, feed_dir, loaded):
    """A genuine change must produce exactly one update and no new row."""
    uid = fetch_value(conn, f"SELECT customer_uid FROM {SCHEMA}.customers ORDER BY customer_id LIMIT 1")
    before_rows = _count(conn, "customers")

    path = feed_dir / "customers.csv"
    lines = path.read_text(encoding="utf-8").splitlines()
    header = lines[0].split(",")
    name_idx = header.index("full_name")

    changed = 0
    for i, line in enumerate(lines[1:], start=1):
        parts = line.split(",")
        if parts[0] == uid:
            parts[name_idx] = "Renamed Person"
            lines[i] = ",".join(parts)
            changed += 1
    assert changed == 1, "test fixture assumption broken"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    # New content -> new fingerprint -> a genuinely new batch.
    result = pipeline.run_file(conn, specs.CUSTOMERS, path, schema=SCHEMA)

    assert result.state == "succeeded"
    assert result.stats.rows_updated == 1
    assert result.stats.rows_inserted == 0
    assert _count(conn, "customers") == before_rows
    assert fetch_value(
        conn, f"SELECT full_name FROM {SCHEMA}.customers WHERE customer_uid = %s", (uid,)
    ) == "Renamed Person"


def test_in_batch_duplicates_are_collapsed_last_write_wins(conn, feed_dir):
    """Deduplication: one natural key, several rows in one file.

    The winner is the highest source_updated_at, chosen by DISTINCT ON
    with an explicit ORDER BY -- so it is deterministic rather than
    whatever the heap happened to return first.
    """
    path = feed_dir / "customers.csv"
    lines = path.read_text(encoding="utf-8").splitlines()
    header = lines[0].split(",")
    uid_i = header.index("customer_uid")
    name_i = header.index("full_name")
    upd_i = header.index("source_updated_at")

    first = lines[1].split(",")
    uid = first[uid_i]

    older = list(first)
    older[name_i] = "Older Version"
    older[upd_i] = "2020-01-01 00:00:00+00"

    newer = list(first)
    newer[name_i] = "Newer Version"
    newer[upd_i] = "2030-01-01 00:00:00+00"

    dupes = path.parent / "customers_dupes.csv"
    dupes.write_text(
        "\n".join([lines[0], ",".join(older), ",".join(newer), ",".join(first)]) + "\n",
        encoding="utf-8",
    )

    result = pipeline.run_file(conn, specs.CUSTOMERS, dupes, schema=SCHEMA)

    assert result.stats.rows_read == 3
    assert result.stats.rows_duplicate == 2, "the two extra rows should have collapsed"
    assert fetch_value(
        conn, f"SELECT full_name FROM {SCHEMA}.customers WHERE customer_uid = %s", (uid,)
    ) == "Newer Version"


def test_batch_ledger_records_the_run(conn, feed_dir, loaded):
    row = fetch_value(
        conn,
        """
        SELECT count(*) FROM qq_meta.etl_batches
        WHERE state = 'succeeded' AND rows_read > 0
        """,
    )
    assert int(row) >= len(specs.LOAD_ORDER) - 2


def test_watermark_advances(conn, feed_dir, loaded):
    n = fetch_value(conn, "SELECT count(*) FROM qq_meta.etl_watermarks")
    assert int(n) > 0
