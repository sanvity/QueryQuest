"""Batch orchestration and idempotency.

Idempotency here means something stronger than "re-running does not
error". It means re-running a feed:

  1. is recognised as the same batch and short-circuits entirely, or
  2. if forced, replays through the same UPSERTs and writes zero rows,
     because the change guard finds nothing different.

Both properties are asserted in tests/test_etl_idempotency.py.

The batch ledger (qq_meta.etl_batches) is the mechanism. `batch_key` is
sha256 over the source name plus the file's bytes, so identical input
produces an identical key and collides with the unique constraint.
"""

from __future__ import annotations

import csv
import hashlib
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Sequence

import psycopg

from ..config import META_SCHEMA, OPTIMIZED_SCHEMA, settings
from . import loader
from .specs import LOAD_ORDER, TableSpec

log = logging.getLogger("queryquest.etl.pipeline")


@dataclass
class BatchResult:
    table: str
    batch_id: int | None
    state: str
    stats: loader.LoadStats
    duration_ms: int = 0
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.state in {"succeeded", "skipped_duplicate"}


# ---------------------------------------------------------------------
# Source reading
# ---------------------------------------------------------------------
def file_fingerprint(path: Path, source_name: str) -> str:
    """Content-addressed batch key. Same bytes -> same key -> skipped."""
    h = hashlib.sha256()
    h.update(source_name.encode())
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def iter_chunks(
    path: Path, columns: Sequence[str], size: int
) -> Iterator[list[tuple]]:
    """Stream a CSV in fixed-size chunks.

    Streamed rather than read whole: a ten-million-row feed is a few GB
    of Python objects if you materialise it, and there is no reason to.
    Empty strings become NULL -- a missing value in a CSV is absence,
    not the empty string, and letting '' through would defeat every
    not-null rule downstream.
    """
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        missing = [c for c in columns if c not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"{path.name}: missing columns {missing}")

        chunk: list[tuple] = []
        for row in reader:
            chunk.append(tuple((row[c] if row[c] != "" else None) for c in columns))
            if len(chunk) >= size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk


# ---------------------------------------------------------------------
# Batch ledger
# ---------------------------------------------------------------------
def _open_batch(
    conn: psycopg.Connection, spec: TableSpec, batch_key: str, source_name: str
) -> tuple[int | None, str | None]:
    """Claim the batch. Returns (batch_id, existing_state)."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT batch_id, state FROM {META_SCHEMA}.etl_batches
            WHERE batch_key = %s
            """,
            (batch_key,),
        )
        found = cur.fetchone()
        if found and found["state"] in ("succeeded", "quarantined"):
            return found["batch_id"], found["state"]

        if found:
            # A previous attempt died mid-flight. Reclaim the row rather
            # than orphaning it -- the load is idempotent, so replaying
            # is safe and re-using the id keeps the audit trail linear.
            cur.execute(
                f"""
                UPDATE {META_SCHEMA}.etl_batches
                   SET state = 'running', started_at = now(),
                       finished_at = NULL, error_text = NULL
                 WHERE batch_id = %s
                """,
                (found["batch_id"],),
            )
            return found["batch_id"], None

        cur.execute(
            f"""
            INSERT INTO {META_SCHEMA}.etl_batches
                (batch_key, source_name, target_table, state)
            VALUES (%s, %s, %s, 'running')
            RETURNING batch_id
            """,
            (batch_key, source_name, spec.table),
        )
        return cur.fetchone()["batch_id"], None


def _close_batch(
    conn: psycopg.Connection,
    batch_id: int,
    state: str,
    stats: loader.LoadStats,
    duration_ms: int,
    error: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {META_SCHEMA}.etl_batches
               SET state = %s, rows_read = %s, rows_duplicate = %s,
                   rows_rejected = %s, rows_inserted = %s, rows_updated = %s,
                   rows_unchanged = %s, finished_at = now(), duration_ms = %s,
                   error_text = %s
             WHERE batch_id = %s
            """,
            (
                state, stats.rows_read, stats.rows_duplicate, stats.rows_rejected,
                stats.rows_inserted, stats.rows_updated, stats.rows_unchanged,
                duration_ms, error, batch_id,
            ),
        )


def _record_dq(conn: psycopg.Connection, batch_id: int, verdicts: list[dict]) -> None:
    """Collapse per-chunk verdicts into one row per rule for the batch."""
    if not verdicts:
        return

    rolled: dict[str, dict] = {}
    for v in verdicts:
        agg = rolled.setdefault(
            v["rule_name"],
            {
                "severity": v["severity"],
                "rows_checked": 0,
                "rows_failed": 0,
                "detail": v["detail"],
            },
        )
        agg["rows_checked"] += v["rows_checked"]
        agg["rows_failed"] += v["rows_failed"]

    with conn.cursor() as cur:
        for name, agg in rolled.items():
            checked = agg["rows_checked"]
            ratio = (agg["rows_failed"] / checked) if checked else 0.0
            cur.execute(
                f"""
                INSERT INTO {META_SCHEMA}.dq_results
                    (batch_id, rule_name, severity, rows_checked, rows_failed,
                     fail_ratio, passed, detail)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    batch_id, name, agg["severity"], checked, agg["rows_failed"],
                    round(ratio, 5), agg["rows_failed"] == 0, agg["detail"],
                ),
            )


def _bump_watermark(
    conn: psycopg.Connection, source_name: str, batch_id: int
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {META_SCHEMA}.etl_watermarks (source_name, watermark_ts, last_batch_id)
            VALUES (%s, now(), %s)
            ON CONFLICT (source_name) DO UPDATE
               SET watermark_ts = EXCLUDED.watermark_ts,
                   last_batch_id = EXCLUDED.last_batch_id,
                   updated_at = now()
            """,
            (source_name, batch_id),
        )


# ---------------------------------------------------------------------
# Running a feed
# ---------------------------------------------------------------------
class QuarantinedBatch(RuntimeError):
    """Raised when a chunk's reject ratio exceeds the configured ceiling."""


def run_file(
    conn: psycopg.Connection,
    spec: TableSpec,
    path: Path,
    *,
    schema: str = OPTIMIZED_SCHEMA,
    batch_size: int | None = None,
    force: bool = False,
    progress_every: int = 500_000,
) -> BatchResult:
    """Load one source file into one table.

    Commits per chunk on purpose. A ten-million-row load inside a single
    transaction pins one snapshot for the whole run, blocks vacuum, and
    grows the WAL without bound; if it dies at 90% you have nothing.
    Chunked commits make the load *resumable* -- and resumability is
    only safe because the UPSERT is idempotent, so a replay of the
    already-loaded prefix writes zero rows.
    """
    batch_size = batch_size or settings.batch_rows
    source_name = path.name
    # The target schema is part of the key: the same feed loaded into
    # `qq` and into `qq_baseline` are two different batches, and without
    # this the second one would be short-circuited as a duplicate.
    batch_key = file_fingerprint(path, f"{source_name}@{schema}")

    batch_id, existing = _open_batch(conn, spec, batch_key, source_name)
    conn.commit()

    if existing and not force:
        log.info("%s: batch already %s, skipping", spec.table, existing)
        return BatchResult(spec.table, batch_id, "skipped_duplicate", loader.LoadStats(spec.table))

    assert batch_id is not None
    t0 = time.perf_counter()
    total = loader.LoadStats(table=spec.table)

    try:
        for chunk in iter_chunks(path, spec.stage_column_names, batch_size):
            stats = loader.load_chunk(conn, spec, chunk, batch_id=batch_id, schema=schema)

            considered = stats.rows_read - stats.rows_duplicate
            ratio = (stats.rows_rejected / considered) if considered else 0.0
            if ratio > settings.max_reject_ratio:
                conn.rollback()
                raise QuarantinedBatch(
                    f"{spec.table}: chunk reject ratio {ratio:.2%} exceeds ceiling "
                    f"{settings.max_reject_ratio:.2%} -- "
                    f"{stats.rows_rejected}/{considered} rows failed validation"
                )

            conn.commit()
            total.merge(stats)

            if progress_every and total.rows_read % progress_every < batch_size:
                log.info(
                    "%s: %s rows read (%s inserted, %s updated, %s rejected)",
                    spec.table,
                    f"{total.rows_read:,}",
                    f"{total.rows_inserted:,}",
                    f"{total.rows_updated:,}",
                    f"{total.rows_rejected:,}",
                )

        duration = int((time.perf_counter() - t0) * 1000)
        _record_dq(conn, batch_id, total.dq)
        _close_batch(conn, batch_id, "succeeded", total, duration)
        _bump_watermark(conn, f"{source_name}@{schema}", batch_id)
        conn.commit()

        log.info(
            "%s: %s read / %s inserted / %s updated / %s unchanged / %s dup / %s rejected in %sms",
            spec.table, total.rows_read, total.rows_inserted, total.rows_updated,
            total.rows_unchanged, total.rows_duplicate, total.rows_rejected, duration,
        )
        return BatchResult(spec.table, batch_id, "succeeded", total, duration)

    except QuarantinedBatch as exc:
        duration = int((time.perf_counter() - t0) * 1000)
        _record_dq(conn, batch_id, total.dq)
        _close_batch(conn, batch_id, "quarantined", total, duration, str(exc))
        conn.commit()
        log.error("%s: quarantined -- %s", spec.table, exc)
        return BatchResult(spec.table, batch_id, "quarantined", total, duration, str(exc))

    except Exception as exc:  # noqa: BLE001 -- recorded, then re-raised
        conn.rollback()
        duration = int((time.perf_counter() - t0) * 1000)
        _close_batch(conn, batch_id, "failed", total, duration, repr(exc))
        conn.commit()
        log.exception("%s: batch failed", spec.table)
        raise


def run_all(
    conn: psycopg.Connection,
    data_dir: Path,
    *,
    schema: str = OPTIMIZED_SCHEMA,
    batch_size: int | None = None,
    force: bool = False,
    only: Sequence[str] | None = None,
) -> list[BatchResult]:
    """Walk every feed in dependency order.

    Order is not cosmetic: order_items cannot resolve an order_uid until
    orders has landed, so a shuffled run would quarantine most of the
    fact rows as unresolved references.
    """
    results: list[BatchResult] = []
    for spec in LOAD_ORDER:
        if only and spec.table not in only:
            continue
        path = data_dir / spec.source_file
        if not path.exists():
            log.warning("%s: no feed at %s, skipping", spec.table, path)
            continue
        results.append(
            run_file(conn, spec, path, schema=schema, batch_size=batch_size, force=force)
        )
    return results
