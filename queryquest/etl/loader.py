"""The generic load engine.

One chunk of source rows goes through six steps, all inside a single
transaction so the chunk is atomic:

    COPY -> dedup -> data quality -> key resolution -> UPSERT -> post-fix

Design notes worth defending in review:

* COPY into an UNLOGGED temp table, then a set-based UPSERT out of it.
  Row-by-row INSERT ... ON CONFLICT over ten million records is roughly
  two orders of magnitude slower, because every row pays a round trip
  and a WAL flush.

* The UPSERT's DO UPDATE carries a `WHERE ... IS DISTINCT FROM` guard.
  Without it, re-running a feed rewrites every row: same values, brand
  new tuple versions, table bloat, index bloat, and an autovacuum
  storm. With it, an unchanged re-run does no writes at all -- which is
  what "idempotent" has to mean in practice, not just "does not error".

* Every column expression is cast to the destination column's exact
  type, read from the catalogue. That is what lets the staging table be
  plain text/numeric while the target uses enums and char(n).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Iterable, Sequence

import psycopg
from psycopg import sql as S

from ..config import META_SCHEMA, OPTIMIZED_SCHEMA
from .specs import TableSpec

log = logging.getLogger("queryquest.etl.loader")

STAGE_RAW = "stg_raw"
STAGE_DEDUP = "stg_dedup"

_TYPE_CACHE: dict[tuple[str, str], dict[str, str]] = {}


@dataclass
class LoadStats:
    table: str
    rows_read: int = 0
    rows_duplicate: int = 0
    rows_rejected: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_unchanged: int = 0
    duration_ms: int = 0
    dq: list[dict] = field(default_factory=list)

    def merge(self, other: "LoadStats") -> None:
        self.rows_read += other.rows_read
        self.rows_duplicate += other.rows_duplicate
        self.rows_rejected += other.rows_rejected
        self.rows_inserted += other.rows_inserted
        self.rows_updated += other.rows_updated
        self.rows_unchanged += other.rows_unchanged
        self.duration_ms += other.duration_ms
        self.dq.extend(other.dq)

    @property
    def rows_written(self) -> int:
        return self.rows_inserted + self.rows_updated


# ---------------------------------------------------------------------
# Catalogue helpers
# ---------------------------------------------------------------------
def column_types(conn: psycopg.Connection, schema: str, table: str) -> dict[str, str]:
    key = (schema, table)
    if key not in _TYPE_CACHE:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.attname AS name,
                       format_type(a.atttypid, a.atttypmod) AS coltype
                FROM pg_attribute a
                JOIN pg_class c     ON c.oid = a.attrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s
                  AND a.attnum > 0 AND NOT a.attisdropped
                """,
                (schema, table),
            )
            _TYPE_CACHE[key] = {r["name"]: r["coltype"] for r in cur.fetchall()}
    return _TYPE_CACHE[key]


def _ident(*parts: str) -> S.Composed:
    return S.SQL(".").join(S.Identifier(p) for p in parts)


# ---------------------------------------------------------------------
# Step 1 -- staging + COPY
# ---------------------------------------------------------------------
def create_staging(conn: psycopg.Connection, spec: TableSpec) -> None:
    cols = S.SQL(", ").join(
        S.SQL("{} {}").format(S.Identifier(name), S.SQL(coltype))
        for name, coltype in spec.stage_columns
    )
    with conn.cursor() as cur:
        cur.execute(S.SQL("DROP TABLE IF EXISTS {}").format(S.Identifier(STAGE_RAW)))
        cur.execute(S.SQL("DROP TABLE IF EXISTS {}").format(S.Identifier(STAGE_DEDUP)))
        # UNLOGGED is implicit for temp tables, so staging writes generate
        # no WAL at all -- the single biggest win in the whole load path.
        cur.execute(
            S.SQL("CREATE TEMP TABLE {} ({}) ON COMMIT DROP").format(
                S.Identifier(STAGE_RAW), cols
            )
        )


def copy_into_staging(
    conn: psycopg.Connection, spec: TableSpec, rows: Iterable[Sequence]
) -> int:
    stmt = S.SQL("COPY {} ({}) FROM STDIN").format(
        S.Identifier(STAGE_RAW),
        S.SQL(", ").join(S.Identifier(c) for c in spec.stage_column_names),
    )
    n = 0
    with conn.cursor() as cur:
        with cur.copy(stmt) as cp:
            for row in rows:
                cp.write_row(row)
                n += 1
    return n


# ---------------------------------------------------------------------
# Step 2 -- deduplication
# ---------------------------------------------------------------------
def deduplicate(conn: psycopg.Connection, spec: TableSpec) -> int:
    """Collapse in-batch duplicates, last write wins.

    Feeds redeliver. A single file routinely contains the same natural
    key more than once -- an update that happened after the extract
    started, or a retried upstream publish. DISTINCT ON with an explicit
    ORDER BY makes the winner deterministic; without the ORDER BY,
    "which duplicate survived" is whatever the heap felt like that day.
    """
    keys = S.SQL(", ").join(S.Identifier(k) for k in spec.stage_dedup_key)
    with conn.cursor() as cur:
        cur.execute(
            S.SQL(
                "CREATE TEMP TABLE {dedup} ON COMMIT DROP AS "
                "SELECT DISTINCT ON ({keys}) * FROM {raw} "
                "ORDER BY {keys}, {order}"
            ).format(
                dedup=S.Identifier(STAGE_DEDUP),
                keys=keys,
                raw=S.Identifier(STAGE_RAW),
                order=S.SQL(spec.dedup_order),
            )
        )
        cur.execute(S.SQL("SELECT count(*) AS n FROM {}").format(S.Identifier(STAGE_DEDUP)))
        return int(cur.fetchone()["n"])


# ---------------------------------------------------------------------
# Step 3 -- data quality
# ---------------------------------------------------------------------
def _natural_key_expr(spec: TableSpec) -> S.Composed:
    return S.SQL("concat_ws('|', {})").format(
        S.SQL(", ").join(
            S.SQL("s.{}::text").format(S.Identifier(k)) for k in spec.stage_dedup_key
        )
    )


def run_quality_rules(
    conn: psycopg.Connection, spec: TableSpec, batch_id: int, staged: int
) -> tuple[int, list[dict]]:
    """Quarantine bad rows; return (rejected_count, per-rule verdicts).

    A failing row is moved to qq_meta.etl_rejects with the rule that
    caught it and its full payload as jsonb, then deleted from staging.
    The batch keeps going. Losing a million good rows because a hundred
    are malformed is the wrong trade every time -- and an operator can
    replay the quarantine once upstream is fixed.
    """
    rejected = 0
    verdicts: list[dict] = []

    for rule in spec.rules:
        failing = S.SQL("(NOT ({p}) OR ({p}) IS NULL)").format(p=S.SQL(rule.predicate))

        with conn.cursor() as cur:
            if rule.severity == "error":
                cur.execute(
                    S.SQL(
                        "WITH bad AS (SELECT s.* FROM {dedup} s WHERE {failing}), "
                        "moved AS ("
                        "  INSERT INTO {rej} (batch_id, target_table, rule_name, reason,"
                        "                     natural_key, raw_payload) "
                        "  SELECT %(batch)s, %(table)s, %(rule)s, %(reason)s, {nk}, to_jsonb(s) "
                        "  FROM bad s RETURNING 1) "
                        "SELECT count(*) AS n FROM moved"
                    ).format(
                        dedup=S.Identifier(STAGE_DEDUP),
                        failing=failing,
                        rej=_ident(META_SCHEMA, "etl_rejects"),
                        nk=_natural_key_expr(spec),
                    ),
                    {
                        "batch": batch_id,
                        "table": spec.table,
                        "rule": rule.name,
                        "reason": rule.description,
                    },
                )
                failed = int(cur.fetchone()["n"])

                if failed:
                    cur.execute(
                        S.SQL("DELETE FROM {dedup} s WHERE {failing}").format(
                            dedup=S.Identifier(STAGE_DEDUP), failing=failing
                        )
                    )
                    rejected += failed
            else:
                cur.execute(
                    S.SQL("SELECT count(*) AS n FROM {dedup} s WHERE {failing}").format(
                        dedup=S.Identifier(STAGE_DEDUP), failing=failing
                    )
                )
                failed = int(cur.fetchone()["n"])

        ratio = (failed / staged) if staged else 0.0
        verdicts.append(
            {
                "rule_name": rule.name,
                "severity": rule.severity,
                "rows_checked": staged,
                "rows_failed": failed,
                "fail_ratio": round(ratio, 5),
                "passed": failed == 0,
                "detail": rule.description,
            }
        )

    return rejected, verdicts


def enforce_referential_integrity(
    conn: psycopg.Connection, spec: TableSpec, schema: str, batch_id: int, staged: int
) -> tuple[int, list[dict]]:
    """Reject rows whose business keys do not resolve to a parent row.

    Deliberately checked here rather than left to the foreign key. A FK
    violation aborts the entire transaction and takes a million good
    rows down with one orphan; catching it first turns that into a
    quarantined row and a load that still completes.
    """
    rejected = 0
    verdicts: list[dict] = []

    for lk in spec.lookups:
        if not lk.required:
            continue

        extra = S.SQL(lk.extra_on.format(ref="t")) if lk.extra_on else S.SQL("")
        missing = S.SQL(
            "NOT EXISTS (SELECT 1 FROM {ref} t WHERE t.{key} = s.{col} {extra})"
        ).format(
            ref=_ident(schema, lk.ref_table),
            key=S.Identifier(lk.ref_key),
            col=S.Identifier(lk.stage_col),
            extra=extra,
        )
        rule_name = f"fk_{lk.target_col}_resolves"
        reason = f"{lk.stage_col} does not resolve to an existing {lk.ref_table} row"

        with conn.cursor() as cur:
            cur.execute(
                S.SQL(
                    "WITH bad AS (SELECT s.* FROM {dedup} s WHERE {missing}), "
                    "moved AS ("
                    "  INSERT INTO {rej} (batch_id, target_table, rule_name, reason,"
                    "                     natural_key, raw_payload) "
                    "  SELECT %(batch)s, %(table)s, %(rule)s, %(reason)s, {nk}, to_jsonb(s) "
                    "  FROM bad s RETURNING 1) "
                    "SELECT count(*) AS n FROM moved"
                ).format(
                    dedup=S.Identifier(STAGE_DEDUP),
                    missing=missing,
                    rej=_ident(META_SCHEMA, "etl_rejects"),
                    nk=_natural_key_expr(spec),
                ),
                {
                    "batch": batch_id,
                    "table": spec.table,
                    "rule": rule_name,
                    "reason": reason,
                },
            )
            failed = int(cur.fetchone()["n"])

            if failed:
                cur.execute(
                    S.SQL("DELETE FROM {dedup} s WHERE {missing}").format(
                        dedup=S.Identifier(STAGE_DEDUP), missing=missing
                    )
                )
                rejected += failed

        verdicts.append(
            {
                "rule_name": rule_name,
                "severity": "error",
                "rows_checked": staged,
                "rows_failed": failed,
                "fail_ratio": round((failed / staged) if staged else 0.0, 5),
                "passed": failed == 0,
                "detail": reason,
            }
        )

    return rejected, verdicts


# ---------------------------------------------------------------------
# Steps 4 + 5 -- key resolution and UPSERT
# ---------------------------------------------------------------------
def _hash_expr(spec: TableSpec) -> S.Composed:
    """sha256 over the business columns.

    coalesce to a sentinel rather than letting concat_ws drop NULLs:
    without it ('a', NULL, 'b') and ('a', 'b', NULL) hash identically,
    and a real change would be mistaken for no change.
    """
    parts = S.SQL(", ").join(
        S.SQL("coalesce(s.{}::text, '~NULL~')").format(S.Identifier(c))
        for c in spec.hash_columns
    )
    return S.SQL("sha256(concat_ws('|', {})::bytea)").format(parts)


def _select_expr(spec: TableSpec, col: str, types: dict[str, str]) -> S.Composed:
    lookup_by_target = {lk.target_col: lk for lk in spec.lookups}
    coltype = types.get(col, "text")

    if col == "source_hash":
        base: S.Composed | S.SQL = _hash_expr(spec)
    elif col == "source_batch":
        base = S.SQL("%(batch_id)s")
    elif col in lookup_by_target:
        lk = lookup_by_target[col]
        base = S.SQL("{}.{}").format(S.Identifier(lk.alias), S.Identifier(lk.ref_id))
    elif col in spec.stage_column_names:
        base = S.SQL("s.{}").format(S.Identifier(col))
    else:
        raise KeyError(f"{spec.table}: no source for insert column {col!r}")

    return S.SQL("({})::{}").format(base, S.SQL(coltype))


def _joins(spec: TableSpec, schema: str) -> S.Composed:
    parts: list[S.Composed] = []
    for lk in spec.lookups:
        extra = S.SQL(lk.extra_on.format(ref=lk.alias)) if lk.extra_on else S.SQL("")
        parts.append(
            S.SQL("LEFT JOIN {ref} AS {alias} ON {alias}.{key} = s.{col} {extra}").format(
                ref=_ident(schema, lk.ref_table),
                alias=S.Identifier(lk.alias),
                key=S.Identifier(lk.ref_key),
                col=S.Identifier(lk.stage_col),
                extra=extra,
            )
        )
    return S.SQL(" ").join(parts) if parts else S.SQL("")


def _change_guard(spec: TableSpec) -> S.Composed:
    """The predicate that makes a replay free."""
    if spec.hash_columns:
        return S.SQL("{tbl}.source_hash IS DISTINCT FROM EXCLUDED.source_hash").format(
            tbl=S.Identifier(spec.table)
        )
    left = S.SQL(", ").join(
        S.SQL("{}.{}").format(S.Identifier(spec.table), S.Identifier(c))
        for c in spec.compare_columns
    )
    right = S.SQL(", ").join(
        S.SQL("EXCLUDED.{}").format(S.Identifier(c)) for c in spec.compare_columns
    )
    return S.SQL("({}) IS DISTINCT FROM ({})").format(left, right)


def count_already_present(
    conn: psycopg.Connection, spec: TableSpec, schema: str
) -> int:
    """How many staged rows already exist in the target, by natural key.

    Needed because the usual `RETURNING (xmax = 0)` trick for telling
    inserts from updates is rejected on partitioned tables -- PostgreSQL
    will not expose a system column through a partitioned RETURNING
    clause. One extra semi-join is a cheap price for counts that are
    correct on partitioned and plain tables alike.
    """
    types = column_types(conn, schema, spec.table)
    key_exprs = S.SQL(", ").join(
        S.SQL("{} AS {}").format(_select_expr(spec, c, types), S.Identifier(c))
        for c in spec.conflict_key
    )
    on_clause = S.SQL(" AND ").join(
        S.SQL("t.{c} = src.{c}").format(c=S.Identifier(c)) for c in spec.conflict_key
    )

    stmt = S.SQL(
        "WITH src AS (SELECT {keys} FROM {dedup} s {joins}) "
        "SELECT count(*) AS n FROM src JOIN {target} t ON {on}"
    ).format(
        keys=key_exprs,
        dedup=S.Identifier(STAGE_DEDUP),
        joins=_joins(spec, schema),
        target=_ident(schema, spec.table),
        on=on_clause,
    )
    with conn.cursor() as cur:
        cur.execute(stmt, {"batch_id": 0})
        return int(cur.fetchone()["n"])


def upsert(
    conn: psycopg.Connection, spec: TableSpec, schema: str, batch_id: int
) -> int:
    """Set-based UPSERT out of staging. Returns the number of rows written.

    Rows skipped by the change guard are not returned, so `written` is
    exactly "rows that really differed".
    """
    types = column_types(conn, schema, spec.table)

    stmt = S.SQL(
        "WITH upserted AS ("
        "  INSERT INTO {target} ({cols}) "
        "  SELECT {exprs} FROM {dedup} s {joins} "
        "  ON CONFLICT ({conflict}) DO UPDATE SET {sets} "
        "  WHERE {guard} "
        "  RETURNING 1 AS touched"
        ") "
        "SELECT count(*) AS written FROM upserted"
    ).format(
        target=_ident(schema, spec.table),
        cols=S.SQL(", ").join(S.Identifier(c) for c in spec.insert_columns),
        exprs=S.SQL(", ").join(_select_expr(spec, c, types) for c in spec.insert_columns),
        dedup=S.Identifier(STAGE_DEDUP),
        joins=_joins(spec, schema),
        conflict=S.SQL(", ").join(S.Identifier(c) for c in spec.conflict_key),
        sets=S.SQL(", ").join(
            [
                S.SQL("{c} = EXCLUDED.{c}").format(c=S.Identifier(c))
                for c in spec.update_columns
            ]
            + ([S.SQL("updated_at = now()")] if spec.touch_updated_at else [])
        ),
        guard=_change_guard(spec),
    )

    with conn.cursor() as cur:
        cur.execute(stmt, {"batch_id": batch_id})
        return int(cur.fetchone()["written"] or 0)


def run_post_sql(conn: psycopg.Connection, spec: TableSpec, schema: str) -> None:
    if not spec.post_sql:
        return
    with conn.cursor() as cur:
        cur.execute(spec.post_sql.format(schema=schema))


# ---------------------------------------------------------------------
# The whole chunk
# ---------------------------------------------------------------------
def load_chunk(
    conn: psycopg.Connection,
    spec: TableSpec,
    rows: Iterable[Sequence],
    *,
    batch_id: int,
    schema: str = OPTIMIZED_SCHEMA,
) -> LoadStats:
    """COPY -> dedup -> DQ -> resolve -> UPSERT, atomically.

    The caller owns the transaction boundary: everything here either
    lands together or not at all.
    """
    t0 = time.perf_counter()
    stats = LoadStats(table=spec.table)

    create_staging(conn, spec)
    stats.rows_read = copy_into_staging(conn, spec, rows)
    if stats.rows_read == 0:
        return stats

    staged = deduplicate(conn, spec)
    stats.rows_duplicate = stats.rows_read - staged

    rejected_dq, verdicts = run_quality_rules(conn, spec, batch_id, staged)
    rejected_fk, fk_verdicts = enforce_referential_integrity(
        conn, spec, schema, batch_id, staged
    )
    stats.rows_rejected = rejected_dq + rejected_fk
    stats.dq = verdicts + fk_verdicts

    surviving = staged - stats.rows_rejected
    if surviving > 0:
        # Order matters: count what already exists *before* the UPSERT
        # changes the answer.
        already = count_already_present(conn, spec, schema)
        written = upsert(conn, spec, schema, batch_id)
        run_post_sql(conn, spec, schema)

        stats.rows_inserted = max(0, surviving - already)
        stats.rows_updated = max(0, written - stats.rows_inserted)

    stats.rows_unchanged = max(0, surviving - stats.rows_written)
    stats.duration_ms = int((time.perf_counter() - t0) * 1000)
    return stats
