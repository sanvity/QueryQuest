#!/usr/bin/env python3
"""Zero-infrastructure PostgreSQL + Redis for local runs.

Docker is the primary path (`docker compose up -d`). This is the
fallback for machines without it: `pgserver` ships real PostgreSQL
binaries in a wheel and `redislite` ships a real redis-server, so both
run in user space with no daemon, no root and no container runtime.

    python scripts/devserver.py serve       # foreground, holds both up
    python scripts/devserver.py env         # print the exports
    python scripts/devserver.py stop

While `serve` is running, connection details are written to
`.devserver.json`; the test suite and CLI pick them up automatically.

Linux and macOS only -- both wheels are POSIX. On Windows, use Docker or
run this inside WSL.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
STATE = REPO / ".devserver.json"
PGDATA = Path(os.getenv("QQ_PGDATA", REPO / ".devdata" / "pgdata"))
REDIS_DB = Path(os.getenv("QQ_REDISDATA", REPO / ".devdata" / "redis.db"))
DBNAME = "queryquest"


def _require(mod: str):
    try:
        return __import__(mod)
    except ImportError:
        sys.exit(
            f"{mod} is not installed. Either use Docker (docker compose up -d) "
            f"or install the no-Docker extras:\n"
            f"    pip install -r requirements-dev.txt"
        )


def start_postgres() -> str:
    pgserver = _require("pgserver")
    PGDATA.mkdir(parents=True, exist_ok=True)
    # cleanup_mode=None leaves the server up after this process exits, so
    # a crashed CLI does not take the database with it.
    server = pgserver.get_server(PGDATA, cleanup_mode=None)

    # pgserver hands back a URI for its default database; QueryQuest gets
    # its own, so a `DROP SCHEMA` never risks collateral damage.
    # CREATE DATABASE cannot run inside a transaction or be guarded with
    # IF NOT EXISTS, so the existence check comes first.
    exists = server.psql(
        f"SELECT 1 FROM pg_database WHERE datname = '{DBNAME}'"
    ).strip()
    if "1" not in exists:
        server.psql(f"CREATE DATABASE {DBNAME}")

    return _swap_db(server.get_uri())


def _swap_db(uri: str) -> str:
    """Point a DSN at our database, leaving everything else intact.

    Parsed rather than string-sliced: pgserver connects over a Unix
    socket, so the DSN carries `?host=/some/path` and naive slicing on
    the last '/' rewrites the socket directory instead of the database.
    """
    from urllib.parse import urlsplit, urlunsplit

    parts = urlsplit(uri)
    return urlunsplit((parts.scheme, parts.netloc, f"/{DBNAME}", parts.query, parts.fragment))


def start_redis():
    redislite = _require("redislite")
    REDIS_DB.parent.mkdir(parents=True, exist_ok=True)
    client = redislite.Redis(str(REDIS_DB))
    client.ping()
    return client, f"unix://{client.socket_file}"


def cmd_serve(args: argparse.Namespace) -> int:
    dsn = start_postgres()
    client, redis_url = start_redis()

    state = {"QQ_DSN": dsn, "QQ_REDIS_URL": redis_url, "pid": os.getpid()}
    STATE.write_text(json.dumps(state, indent=2), encoding="utf-8")

    print("QueryQuest dev servers up")
    print(f"  QQ_DSN       = {dsn}")
    print(f"  QQ_REDIS_URL = {redis_url}")
    print(f"  state written to {STATE}")
    print("\nIn another shell:")
    print(f"  export QQ_DSN='{dsn}'")
    print(f"  export QQ_REDIS_URL='{redis_url}'")
    print("\nCtrl-C to stop Redis (PostgreSQL keeps running; use `stop` to halt it).")

    stopping = {"now": False}

    def handle(_sig, _frm):
        stopping["now"] = True

    signal.signal(signal.SIGINT, handle)
    signal.signal(signal.SIGTERM, handle)
    while not stopping["now"]:
        time.sleep(0.5)

    print("\nshutting down redis")
    try:
        client.shutdown()
    except Exception:  # noqa: BLE001 -- best effort on the way out
        pass
    STATE.unlink(missing_ok=True)
    return 0


def cmd_env(args: argparse.Namespace) -> int:
    if not STATE.exists():
        sys.exit("no dev server running -- start one with: python scripts/devserver.py serve")
    state = json.loads(STATE.read_text(encoding="utf-8"))
    print(f"export QQ_DSN='{state['QQ_DSN']}'")
    print(f"export QQ_REDIS_URL='{state['QQ_REDIS_URL']}'")
    return 0


def cmd_stop(args: argparse.Namespace) -> int:
    pgserver = _require("pgserver")
    try:
        server = pgserver.get_server(PGDATA, cleanup_mode=None)
        server.cleanup()
        print("postgres stopped")
    except Exception as exc:  # noqa: BLE001
        print(f"postgres already down ({exc})")
    STATE.unlink(missing_ok=True)
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("serve").set_defaults(func=cmd_serve)
    sub.add_parser("env").set_defaults(func=cmd_env)
    sub.add_parser("stop").set_defaults(func=cmd_stop)
    args = p.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
