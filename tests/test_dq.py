"""Data quality: quarantine, not crash.

The design choice under test is that one malformed row must not cost a
million good ones. A defect is routed to `qq_meta.etl_rejects` with the
rule that caught it and its original payload, the batch continues, and
an operator can replay the quarantine after the upstream fix.

The exception is a batch that is *mostly* broken. Half-loading garbage
is worse than loading nothing, so past a configured reject ratio the
whole chunk is rolled back and the batch is marked quarantined.
"""

from __future__ import annotations

from queryquest.db import fetch_all, fetch_value
from queryquest.etl import pipeline, specs

SCHEMA = "qq"


def _write(path, header, rows):
    path.write_text(
        "\n".join([",".join(header)] + [",".join(str(c) for c in r) for r in rows]) + "\n",
        encoding="utf-8",
    )
    return path


def test_malformed_email_is_quarantined_not_fatal(conn, feed_dir, tmp_path):
    pipeline.run_all(conn, feed_dir, schema=SCHEMA, only=["regions"])

    header = [c for c, _ in specs.CUSTOMERS.stage_columns]
    good = ["DQ-OK-1", "fine@example.com", "Fine Person", "na-east", "active",
            "2025-01-01 00:00:00+00", "10.00", "2025-01-01 00:00:00+00"]
    bad = ["DQ-BAD-1", "not-an-email", "Broken Person", "na-east", "active",
           "2025-01-01 00:00:00+00", "10.00", "2025-01-01 00:00:00+00"]

    path = _write(tmp_path / "customers.csv", header, [good, bad])
    result = pipeline.run_file(conn, specs.CUSTOMERS, path, schema=SCHEMA)

    assert result.state == "succeeded", "one bad row must not fail the batch"
    assert result.stats.rows_inserted == 1
    assert result.stats.rows_rejected == 1

    rejects = fetch_all(
        conn,
        """
        SELECT rule_name, natural_key, raw_payload
        FROM qq_meta.etl_rejects WHERE batch_id = %s
        """,
        (result.batch_id,),
    )
    assert len(rejects) == 1
    assert rejects[0]["rule_name"] == "email_shape"
    assert rejects[0]["natural_key"] == "DQ-BAD-1"
    # The original payload is preserved, so the row can be replayed.
    assert rejects[0]["raw_payload"]["email"] == "not-an-email"

    assert fetch_value(
        conn, f"SELECT count(*) FROM {SCHEMA}.customers WHERE customer_uid = 'DQ-BAD-1'"
    ) == 0


def test_unresolvable_reference_is_a_reject_not_a_constraint_violation(
    conn, feed_dir, tmp_path
):
    """A missing parent must be caught before the foreign key sees it.

    If it reached the FK, the violation would abort the whole
    transaction and take every good row in the chunk with it.
    """
    pipeline.run_all(conn, feed_dir, schema=SCHEMA, only=["regions"])

    header = [c for c, _ in specs.CUSTOMERS.stage_columns]
    orphan = ["DQ-ORPHAN-1", "orphan@example.com", "Orphan", "no-such-region",
              "active", "2025-01-01 00:00:00+00", "0.00", "2025-01-01 00:00:00+00"]

    path = _write(tmp_path / "customers.csv", header, [orphan])
    result = pipeline.run_file(conn, specs.CUSTOMERS, path, schema=SCHEMA)

    assert result.state == "succeeded"
    assert result.stats.rows_rejected == 1
    rule = fetch_value(
        conn,
        "SELECT rule_name FROM qq_meta.etl_rejects WHERE batch_id = %s",
        (result.batch_id,),
    )
    assert rule == "fk_region_id_resolves"


def test_totals_that_do_not_reconcile_are_caught(conn, feed_dir, tmp_path):
    """The rule that catches real upstream bugs: money that does not add up."""
    pipeline.run_all(conn, feed_dir, schema=SCHEMA)

    uid = fetch_value(conn, f"SELECT customer_uid FROM {SCHEMA}.customers LIMIT 1")
    header = [c for c, _ in specs.ORDERS.stage_columns]
    # subtotal 100 + shipping 5 + tax 8 = 113, but total says 500.
    bad = ["DQ-ORD-1", uid, "na-east", "2025-06-01 00:00:00+00", "paid", "USD",
           "100.00", "5.00", "8.00", "500.00", "2", "2025-06-01 00:00:00+00"]

    path = _write(tmp_path / "orders.csv", header, [bad])
    result = pipeline.run_file(conn, specs.ORDERS, path, schema=SCHEMA)

    assert result.stats.rows_rejected == 1
    assert fetch_value(
        conn, "SELECT rule_name FROM qq_meta.etl_rejects WHERE batch_id = %s",
        (result.batch_id,),
    ) == "total_reconciles"


def test_check_constraint_violation_is_downgraded_to_a_reject(conn, feed_dir, tmp_path):
    """inventory has CHECK (qty_reserved <= qty_on_hand).

    A violating row would abort the batch at the constraint. The
    matching DQ rule catches it first, so the load succeeds with one row
    quarantined -- the constraint stays as the backstop it should be.
    """
    pipeline.run_all(conn, feed_dir, schema=SCHEMA)

    sku = fetch_value(conn, f"SELECT sku FROM {SCHEMA}.product_variants LIMIT 1")
    header = [c for c, _ in specs.INVENTORY.stage_columns]
    bad = [sku, "10", "999", "5", "2025-06-01 00:00:00+00"]

    path = _write(tmp_path / "inventory.csv", header, [bad])
    result = pipeline.run_file(conn, specs.INVENTORY, path, schema=SCHEMA)

    assert result.state == "succeeded"
    assert result.stats.rows_rejected == 1
    assert fetch_value(
        conn, "SELECT rule_name FROM qq_meta.etl_rejects WHERE batch_id = %s",
        (result.batch_id,),
    ) == "reserved_within_on_hand"


def test_dq_verdicts_are_recorded_for_every_rule(conn, feed_dir, tmp_path):
    pipeline.run_all(conn, feed_dir, schema=SCHEMA, only=["regions"])

    header = [c for c, _ in specs.CUSTOMERS.stage_columns]
    good = ["DQ-AUDIT-1", "audit@example.com", "Audit", "na-east", "active",
            "2025-01-01 00:00:00+00", "1.00", "2025-01-01 00:00:00+00"]
    path = _write(tmp_path / "customers.csv", header, [good])
    result = pipeline.run_file(conn, specs.CUSTOMERS, path, schema=SCHEMA)

    recorded = {
        r["rule_name"]
        for r in fetch_all(
            conn, "SELECT rule_name FROM qq_meta.dq_results WHERE batch_id = %s",
            (result.batch_id,),
        )
    }
    for rule in specs.CUSTOMERS.rules:
        assert rule.name in recorded, f"{rule.name} left no audit trail"
    assert "fk_region_id_resolves" in recorded


def test_batch_over_the_reject_ceiling_is_quarantined_wholesale(
    conn, feed_dir, tmp_path
):
    """Past the ceiling, nothing lands. Partial garbage is worse than none."""
    pipeline.run_all(conn, feed_dir, schema=SCHEMA, only=["regions"])

    header = [c for c, _ in specs.CUSTOMERS.stage_columns]
    rows = []
    for i in range(100):
        email = "fine@example.com" if i >= 50 else "broken"
        rows.append([f"DQ-BULK-{i}", email, f"Person {i}", "na-east", "active",
                     "2025-01-01 00:00:00+00", "0.00", "2025-01-01 00:00:00+00"])

    path = _write(tmp_path / "customers.csv", header, rows)
    result = pipeline.run_file(conn, specs.CUSTOMERS, path, schema=SCHEMA)

    assert result.state == "quarantined"
    assert "exceeds ceiling" in (result.error or "")
    # The good half must NOT have been committed.
    # Single % here, not %%: psycopg only un-escapes doubled percent signs
    # when parameters are passed, and this query has none -- so '%%' would
    # stay literal and the LIKE would match nothing, passing the assertion
    # for entirely the wrong reason.
    assert fetch_value(
        conn,
        f"SELECT count(*) FROM {SCHEMA}.customers WHERE customer_uid LIKE %s",
        ("DQ-BULK-%",),
    ) == 0


def test_a_quarantined_batch_is_not_silently_retried(conn, feed_dir, tmp_path):
    pipeline.run_all(conn, feed_dir, schema=SCHEMA, only=["regions"])

    header = [c for c, _ in specs.CUSTOMERS.stage_columns]
    rows = [[f"DQ-Q-{i}", "broken", f"P{i}", "na-east", "active",
             "2025-01-01 00:00:00+00", "0.00", "2025-01-01 00:00:00+00"]
            for i in range(50)]
    path = _write(tmp_path / "customers.csv", header, rows)

    first = pipeline.run_file(conn, specs.CUSTOMERS, path, schema=SCHEMA)
    assert first.state == "quarantined"

    second = pipeline.run_file(conn, specs.CUSTOMERS, path, schema=SCHEMA)
    assert second.state == "skipped_duplicate", (
        "a quarantined batch must stay quarantined until someone forces it"
    )
