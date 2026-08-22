"""QueryQuest command line.

    python -m queryquest.cli init                 create schemas + partitions
    python -m queryquest.cli load                 run the ETL into both schemas
    python -m queryquest.cli refresh              refresh marts + ANALYZE
    python -m queryquest.cli bench                benchmark baseline vs optimized
    python -m queryquest.cli loadtest             cached read throughput
    python -m queryquest.cli report               regenerate docs/BENCHMARKS.md
    python -m queryquest.cli status               row counts and recent batches
    python -m queryquest.cli pipeline             everything, in order
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

from .config import BASELINE_SCHEMA, MART_SCHEMA, META_SCHEMA, OPTIMIZED_SCHEMA, settings
from .db import analyze, bootstrap, connect, fetch_all, fetch_one, table_counts


def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)-28s %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("psycopg").setLevel(logging.WARNING)


# ---------------------------------------------------------------------
def cmd_init(args: argparse.Namespace) -> int:
    with connect() as conn:
        bootstrap(conn, with_baseline=not args.no_baseline)

        # Partitions must cover the data's whole date range plus headroom.
        # Loading a row with no matching partition would land it in the
        # DEFAULT partition, which quietly undoes pruning for that month.
        start = date.today() - timedelta(days=args.months_back * 31)
        end = date.today() + timedelta(days=args.months_ahead * 31)
        created = fetch_one(conn, "SELECT qq.ensure_partitions(%s, %s) AS n", (start, end))
        conn.commit()
        print(f"schemas ready; {created['n']} partitions created "
              f"covering {start} .. {end}")
    return 0


def cmd_load(args: argparse.Namespace) -> int:
    from .etl import pipeline

    data_dir = Path(args.data_dir) if args.data_dir else settings.data_dir
    targets = [OPTIMIZED_SCHEMA] if args.schema == "qq" else (
        [BASELINE_SCHEMA] if args.schema == "qq_baseline"
        else [OPTIMIZED_SCHEMA, BASELINE_SCHEMA]
    )

    overall_ok = True
    for schema in targets:
        print(f"\n=== loading into {schema} ===")
        with connect() as conn:
            results = pipeline.run_all(
                conn,
                data_dir,
                schema=schema,
                batch_size=args.batch_size,
                force=args.force,
                only=args.only,
            )
        for r in results:
            s = r.stats
            print(
                f"  {r.table:<20} {r.state:<18} "
                f"read={s.rows_read:>9,} dup={s.rows_duplicate:>7,} "
                f"rej={s.rows_rejected:>7,} ins={s.rows_inserted:>9,} "
                f"upd={s.rows_updated:>8,} same={s.rows_unchanged:>9,} "
                f"{r.duration_ms:>7,}ms"
            )
            overall_ok = overall_ok and r.ok
    return 0 if overall_ok else 1


def cmd_refresh(args: argparse.Namespace) -> int:
    with connect(autocommit=True) as conn:
        rows = fetch_all(conn, f"SELECT * FROM {MART_SCHEMA}.refresh_all(%s)", (not args.blocking,))
        for r in rows:
            mode = "CONCURRENTLY" if r["refreshed_concurrently"] else "blocking"
            print(f"  {r['view_name']:<24} {mode:<14} {r['duration_ms']:>8,}ms")

    with connect() as conn:
        for schema in (OPTIMIZED_SCHEMA, BASELINE_SCHEMA):
            analyze(conn, schema)
            print(f"  ANALYZE {schema}: done")
        # Invalidating the leaderboards is part of the refresh, not an
        # afterthought: the mart just moved, so every cached rollup built
        # from it is stale by definition.
    try:
        from .cache.repository import invalidate_leaderboards

        version = invalidate_leaderboards()
        print(f"  leaderboard cache version -> {version}")
    except Exception as exc:  # noqa: BLE001 -- Redis is optional here
        print(f"  cache invalidation skipped: {exc}")
    return 0


def cmd_bench(args: argparse.Namespace) -> int:
    from .bench.runner import run_suite

    with connect() as conn:
        out = run_suite(
            conn,
            reps=args.reps,
            warmup=args.warmup,
            only=args.only,
            variants=tuple(args.variants),
        )
    print(json.dumps(out, indent=2, default=str))
    return 0


def cmd_loadtest(args: argparse.Namespace) -> int:
    from .bench.loadtest import run_loadtest

    with connect() as conn:
        results = []
        if not args.cached_only:
            results.append(
                run_loadtest(
                    conn,
                    duration_s=args.duration,
                    concurrency=args.concurrency,
                    warmup_s=args.lt_warmup,
                    cached=False,
                )
            )
        results.append(
            run_loadtest(
                conn,
                duration_s=args.duration,
                concurrency=args.concurrency,
                warmup_s=args.lt_warmup,
                cached=True,
            )
        )

    for r in results:
        s = r.summary()
        label = "cached" if r.cached else "uncached"
        print(
            f"  {label:<9} {s['queries_per_min']:>12,.0f} q/min  "
            f"p50={s['p50_ms']:>8.2f}ms  p95={s['p95_ms']:>8.2f}ms  "
            f"hit={s.get('cache_hit_ratio', 0):.1%}  errors={s['errors']}"
        )
    if len(results) == 2 and results[0].queries_per_min:
        print(f"  throughput gain: {results[1].queries_per_min / results[0].queries_per_min:.1f}x")
    return 0


def cmd_report(args: argparse.Namespace) -> int:
    from .bench.report import write_report

    with connect() as conn:
        path = write_report(conn, args.run_id)
    print(f"wrote {path}")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    with connect() as conn:
        for schema in (OPTIMIZED_SCHEMA, BASELINE_SCHEMA):
            try:
                counts = table_counts(conn, schema)
            except Exception:  # noqa: BLE001 -- schema may not exist yet
                conn.rollback()
                continue
            total = sum(counts.values())
            print(f"\n{schema}: {total:,} rows across {len(counts)} tables")
            for t, n in counts.items():
                print(f"  {t:<22} {n:>12,}")

        print("\nrecent batches:")
        for b in fetch_all(
            conn,
            f"""SELECT target_table, state, rows_read, rows_inserted, rows_updated,
                       rows_rejected, duration_ms
                FROM {META_SCHEMA}.etl_batches ORDER BY batch_id DESC LIMIT 15""",
        ):
            print(
                f"  {b['target_table']:<20} {b['state']:<18} "
                f"read={b['rows_read']:>9,} ins={b['rows_inserted']:>9,} "
                f"upd={b['rows_updated']:>8,} rej={b['rows_rejected']:>6,} "
                f"{(b['duration_ms'] or 0):>7,}ms"
            )

        rejects = fetch_all(
            conn,
            f"""SELECT rule_name, count(*) AS n FROM {META_SCHEMA}.etl_rejects
                GROUP BY rule_name ORDER BY n DESC LIMIT 12""",
        )
        if rejects:
            print("\nquarantined rows by rule:")
            for r in rejects:
                print(f"  {r['rule_name']:<34} {r['n']:>10,}")
    return 0


def cmd_reset(args: argparse.Namespace) -> int:
    """Drop every QueryQuest schema. Destructive, and gated on --yes."""
    if not args.yes:
        print("refusing to drop schemas without --yes")
        return 2
    with connect(autocommit=True) as conn:
        for schema in (MART_SCHEMA, OPTIMIZED_SCHEMA, BASELINE_SCHEMA, META_SCHEMA):
            conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            print(f"  dropped {schema}")
    return 0


def cmd_pipeline(args: argparse.Namespace) -> int:
    """init -> load -> refresh -> bench -> loadtest -> report."""
    steps = [
        ("init", cmd_init),
        ("load", cmd_load),
        ("refresh", cmd_refresh),
        ("bench", cmd_bench),
    ]
    if not args.skip_loadtest:
        steps.append(("loadtest", cmd_loadtest))
    steps.append(("report", cmd_report))

    for name, fn in steps:
        print(f"\n{'=' * 62}\n== {name}\n{'=' * 62}")
        rc = fn(args)
        if rc != 0:
            print(f"step {name} failed with {rc}")
            return rc
    return 0


# ---------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="queryquest",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="command", required=True)

    def common_init(sp):
        sp.add_argument("--no-baseline", action="store_true",
                        help="skip the comparison schema (halves load time)")
        sp.add_argument("--months-back", type=int, default=30)
        sp.add_argument("--months-ahead", type=int, default=3)

    def common_load(sp):
        sp.add_argument("--data-dir", default=None)
        sp.add_argument("--schema", choices=["qq", "qq_baseline", "both"], default="both")
        sp.add_argument("--batch-size", type=int, default=None)
        sp.add_argument("--force", action="store_true",
                        help="reload even if the batch key was already seen")
        sp.add_argument("--only", nargs="*", default=None, help="limit to these tables")

    def common_bench(sp):
        sp.add_argument("--reps", type=int, default=30)
        sp.add_argument("--warmup", type=int, default=3)
        sp.add_argument("--variants", nargs="*", default=["baseline", "optimized"])

    def common_loadtest(sp):
        sp.add_argument("--duration", type=float, default=30.0)
        sp.add_argument("--concurrency", type=int, default=16)
        sp.add_argument("--cached-only", action="store_true")
        # Named distinctly from bench's --warmup (a repetition count) so
        # the combined `pipeline` parser can carry both.
        sp.add_argument("--load-warmup", type=float, default=10.0, dest="lt_warmup",
                        help="seconds of unmeasured traffic before timing starts")

    sp = sub.add_parser("init", help="create schemas, partitions, indexes, marts")
    common_init(sp)
    sp.set_defaults(func=cmd_init)

    sp = sub.add_parser("load", help="run the ETL")
    common_load(sp)
    sp.set_defaults(func=cmd_load)

    sp = sub.add_parser("refresh", help="refresh materialized views and ANALYZE")
    sp.add_argument("--blocking", action="store_true",
                    help="plain REFRESH instead of CONCURRENTLY")
    sp.set_defaults(func=cmd_refresh)

    sp = sub.add_parser("bench", help="baseline vs optimized query latency")
    common_bench(sp)
    sp.add_argument("--only", nargs="*", default=None)
    sp.set_defaults(func=cmd_bench)

    sp = sub.add_parser("loadtest", help="cached read throughput")
    common_loadtest(sp)
    sp.set_defaults(func=cmd_loadtest)

    sp = sub.add_parser("report", help="regenerate docs/BENCHMARKS.md")
    sp.add_argument("--run-id", default=None)
    sp.set_defaults(func=cmd_report)

    sp = sub.add_parser("status", help="row counts, batches, quarantine")
    sp.set_defaults(func=cmd_status)

    sp = sub.add_parser("reset", help="drop every QueryQuest schema (destructive)")
    sp.add_argument("--yes", action="store_true", help="confirm the drop")
    sp.set_defaults(func=cmd_reset)

    sp = sub.add_parser("pipeline", help="run every step end to end")
    common_init(sp)
    common_load(sp)
    common_bench(sp)
    common_loadtest(sp)   # --only comes from common_load; bench reuses it
    sp.add_argument("--run-id", default=None)
    sp.add_argument("--blocking", action="store_true")
    sp.add_argument("--skip-loadtest", action="store_true")
    sp.set_defaults(func=cmd_pipeline)

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    setup_logging(args.verbose)
    try:
        return args.func(args)
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
