"""Connection handling and schema bootstrap.

The one interesting idea in here is `render_sql`: sql/001_schema.sql and
sql/003_keys.sql are written once with a ${SCHEMA} placeholder and
rendered twice -- into `qq` (partitioned, indexed) and `qq_baseline`
(plain heap, keys only). That guarantees the two schemas cannot drift
apart, which is what makes the benchmark an honest A/B instead of a
comparison between a tuned schema and a straw man someone forgot to
update.
"""

from __future__ import annotations

import logging
import re
import string
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Sequence

import psycopg
from psycopg import sql as pgsql
from psycopg.rows import dict_row

from .config import BASELINE_SCHEMA, OPTIMIZED_SCHEMA, SQL_DIR, settings

log = logging.getLogger("queryquest.db")

_POOL: Any = None


class _SqlTemplate(string.Template):
    """${NAME} only -- bare $ is legal in SQL (dollar-quoted bodies)."""

    pattern = r"""
        \$(?:
            (?P<escaped>\$)             |
            \{(?P<braced>[A-Z_][A-Z0-9_]*)\} |
            (?P<named>(?!x)x)           |
            (?P<invalid>(?!x)x)
        )
    """


def render_sql(text: str, **subs: str) -> str:
    """Substitute ${PLACEHOLDERS}, leaving dollar-quoted bodies alone."""
    return _SqlTemplate(text).substitute(**subs)


def _schema_subs(schema: str) -> dict[str, str]:
    partitioned = schema == OPTIMIZED_SCHEMA
    return {
        "SCHEMA": schema,
        "PARTITION_ORDERS": "PARTITION BY RANGE (placed_at)" if partitioned else "",
        "PARTITION_ITEMS": "PARTITION BY RANGE (placed_at)" if partitioned else "",
    }


# ---------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------
def _apply_session_guards(conn: psycopg.Connection) -> None:
    """Never let a single statement hold the box hostage.

    statement_timeout bounds runaway queries; lock_timeout bounds waiting
    for a row lock, so a stuck writer surfaces as a fast, retryable error
    instead of a thread parked forever.
    """
    with conn.cursor() as cur:
        cur.execute(f"SET statement_timeout = {int(settings.statement_timeout_ms)}")
        cur.execute(f"SET lock_timeout = {int(settings.lock_timeout_ms)}")
        cur.execute("SET application_name = 'queryquest'")
    # Close the implicit transaction those SETs opened. The pool hands
    # this function connections to configure and rejects any it gets back
    # still INTRANS -- correctly, since an idle-in-transaction connection
    # sitting in a pool pins the oldest snapshot and blocks vacuum.
    if not conn.autocommit:
        conn.commit()


@contextmanager
def connect(autocommit: bool = False, dsn: str | None = None) -> Iterator[psycopg.Connection]:
    """A single short-lived connection. Commits on clean exit, rolls back otherwise."""
    conn = psycopg.connect(dsn or settings.dsn, autocommit=autocommit, row_factory=dict_row)
    try:
        _apply_session_guards(conn)
        yield conn
        if not autocommit:
            conn.commit()
    except BaseException:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


def get_pool():
    """Lazily-built connection pool, for the concurrent workloads."""
    global _POOL
    if _POOL is None:
        from psycopg_pool import ConnectionPool

        _POOL = ConnectionPool(
            conninfo=settings.dsn,
            min_size=settings.pool_min,
            max_size=settings.pool_max,
            kwargs={"row_factory": dict_row},
            configure=_apply_session_guards,
            open=True,
        )
    return _POOL


def close_pool() -> None:
    global _POOL
    if _POOL is not None:
        _POOL.close()
        _POOL = None


# ---------------------------------------------------------------------
# Small query helpers
# ---------------------------------------------------------------------
def fetch_all(conn: psycopg.Connection, query: str, params: Sequence | dict | None = None) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()


def fetch_one(conn: psycopg.Connection, query: str, params: Sequence | dict | None = None) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchone()


def fetch_value(conn: psycopg.Connection, query: str, params: Sequence | dict | None = None) -> Any:
    row = fetch_one(conn, query, params)
    if row is None:
        return None
    return next(iter(row.values()))


def execute(conn: psycopg.Connection, query: str, params: Sequence | dict | None = None) -> int:
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.rowcount


# ---------------------------------------------------------------------
# Schema bootstrap
# ---------------------------------------------------------------------
def run_sql_file(conn: psycopg.Connection, path: Path, **subs: str) -> None:
    text = path.read_text(encoding="utf-8")
    if subs:
        text = render_sql(text, **subs)
    log.debug("executing %s", path.name)
    with conn.cursor() as cur:
        cur.execute(text)


def bootstrap(conn: psycopg.Connection, *, with_baseline: bool = True) -> None:
    """Create every schema object. Idempotent -- safe to run repeatedly."""
    run_sql_file(conn, SQL_DIR / "000_meta.sql")

    # Optimized schema: tables -> partitions -> keys -> indexes -> mart.
    # Order matters: partitions must exist before the PK is added, and
    # indexes must come after the keys so nothing is created twice.
    run_sql_file(conn, SQL_DIR / "001_schema.sql", **_schema_subs(OPTIMIZED_SCHEMA))
    run_sql_file(conn, SQL_DIR / "002_partitions.sql")
    run_sql_file(conn, SQL_DIR / "003_keys.sql", **_schema_subs(OPTIMIZED_SCHEMA))
    run_sql_file(conn, SQL_DIR / "020_indexes.sql")
    run_sql_file(conn, SQL_DIR / "040_functions.sql")
    run_sql_file(conn, SQL_DIR / "030_matviews.sql")

    if with_baseline:
        # Same DDL, no partitioning, no secondary indexes, no mart.
        run_sql_file(conn, SQL_DIR / "001_schema.sql", **_schema_subs(BASELINE_SCHEMA))
        run_sql_file(conn, SQL_DIR / "003_keys.sql", **_schema_subs(BASELINE_SCHEMA))

    conn.commit()
    log.info("schema bootstrap complete")


def ensure_partitions(conn: psycopg.Connection, start, end) -> int:
    """Materialise monthly partitions covering [start, end)."""
    created = fetch_value(conn, "SELECT qq.ensure_partitions(%s, %s)", (start, end))
    conn.commit()
    return int(created or 0)


def table_counts(conn: psycopg.Connection, schema: str = OPTIMIZED_SCHEMA) -> dict[str, int]:
    """Exact row counts for the 12 domain tables. Slow by design -- this
    is used for reporting and assertions, not for hot paths."""
    tables = [
        "regions", "customers", "customer_addresses", "sellers", "categories",
        "products", "product_variants", "inventory", "orders", "order_items",
        "payments", "shipments",
    ]
    counts: dict[str, int] = {}
    for t in tables:
        stmt = pgsql.SQL("SELECT count(*) AS n FROM {}.{}").format(
            pgsql.Identifier(schema), pgsql.Identifier(t)
        )
        with conn.cursor() as cur:
            cur.execute(stmt)
            counts[t] = int(cur.fetchone()["n"])
    return counts


def analyze(conn: psycopg.Connection, schema: str = OPTIMIZED_SCHEMA) -> None:
    """Refresh planner statistics.

    Skipping this after a bulk load is the single most common reason a
    correctly-indexed schema still produces terrible plans: the planner
    is working from an empty-table estimate and picks a nested loop over
    ten million rows.
    """
    # ANALYZE has to run outside a transaction block, and psycopg refuses
    # to flip autocommit while one is open -- which it always is here,
    # because the session guards in _apply_session_guards issue SETs.
    conn.commit()
    prev = conn.autocommit
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT tablename FROM pg_tables WHERE schemaname = %s
                """,
                (schema,),
            )
            names = [r["tablename"] for r in cur.fetchall()]
            for name in names:
                cur.execute(
                    pgsql.SQL("ANALYZE {}.{}").format(
                        pgsql.Identifier(schema), pgsql.Identifier(name)
                    )
                )
    finally:
        conn.autocommit = prev


def server_version(conn: psycopg.Connection) -> str:
    raw = fetch_value(conn, "SELECT version()")
    m = re.search(r"PostgreSQL (\S+)", raw or "")
    return m.group(1) if m else (raw or "unknown")
