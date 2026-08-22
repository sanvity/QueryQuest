"""Shared fixtures.

The suite runs against a real PostgreSQL and a real Redis. That is a
deliberate choice: the things worth testing here -- FOR UPDATE
semantics, ON CONFLICT behaviour, partition pruning, SKIP LOCKED, Redis
expiry -- are precisely the things a mock cannot have an opinion about.
A mocked `SELECT ... FOR UPDATE` proves nothing about whether stock can
be oversold.

If QQ_DSN is unset and no dev server is running, the suite skips rather
than failing, so `pytest` on a fresh checkout is not a wall of red.
"""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
STATE = REPO / ".devserver.json"


def _discover() -> None:
    """Pick up a running `scripts/devserver.py serve` if env is unset."""
    if STATE.exists():
        state = json.loads(STATE.read_text(encoding="utf-8"))
        os.environ.setdefault("QQ_DSN", state["QQ_DSN"])
        os.environ.setdefault("QQ_REDIS_URL", state["QQ_REDIS_URL"])


_discover()


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "slow: takes more than a few seconds")
    config.addinivalue_line("markers", "concurrency: spawns worker threads")


@pytest.fixture(scope="session")
def dsn() -> str:
    from queryquest.config import settings

    import psycopg

    try:
        with psycopg.connect(settings.dsn, connect_timeout=3) as conn:
            conn.execute("SELECT 1")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"PostgreSQL unavailable at {settings.dsn}: {exc}")
    return settings.dsn


@pytest.fixture(scope="session")
def schema_ready(dsn: str) -> str:
    """Bootstrap the schema once for the whole session."""
    from datetime import date, timedelta

    from queryquest.db import bootstrap, connect, fetch_one

    with connect() as conn:
        bootstrap(conn, with_baseline=True)
        start = date.today() - timedelta(days=31 * 30)
        end = date.today() + timedelta(days=31 * 3)
        fetch_one(conn, "SELECT qq.ensure_partitions(%s, %s) AS n", (start, end))
        conn.commit()
    return "qq"


@pytest.fixture
def conn(schema_ready):
    from queryquest.db import connect

    with connect() as c:
        yield c


@pytest.fixture(scope="session")
def redis_url() -> str:
    from queryquest.cache.redis_cache import Cache

    probe = Cache(namespace="qq_probe")
    if not probe.ping():
        pytest.skip("Redis unavailable")
    return os.environ.get("QQ_REDIS_URL", "")


@pytest.fixture
def cache(redis_url):
    """A cache in its own namespace, emptied before and after."""
    from queryquest.cache.redis_cache import Cache

    c = Cache(namespace=f"qqtest:{uuid.uuid4().hex[:8]}")
    c.flush_namespace()
    yield c
    c.flush_namespace()


@pytest.fixture
def feed_dir(tmp_path: Path) -> Path:
    """A tiny, deterministic set of source feeds with no injected defects."""
    import sys

    sys.path.insert(0, str(REPO))
    from scripts.generate_data import Generator

    out = tmp_path / "feeds"
    Generator("tiny", out, seed=1234, dirty_ratio=0.0, months=6).run()
    return out


@pytest.fixture
def dirty_feed_dir(tmp_path: Path) -> Path:
    """Feeds with a heavy dose of deliberate defects, for the DQ tests."""
    import sys

    sys.path.insert(0, str(REPO))
    from scripts.generate_data import Generator

    out = tmp_path / "dirty"
    Generator("tiny", out, seed=99, dirty_ratio=0.05, months=6).run()
    return out


@pytest.fixture
def isolated_variant(conn):
    """A SKU nothing else in the suite touches, with known stock.

    Concurrency tests need a row they exclusively own; sharing one across
    tests turns an interference bug into a flaky failure that looks like
    a locking bug.
    """
    from queryquest.db import fetch_one

    uid = uuid.uuid4().hex[:10]
    row = fetch_one(
        conn,
        """
        WITH reg AS (
            INSERT INTO qq.regions (region_code, region_name, country_code, currency_code)
            VALUES (%(code)s, 'Test Region', 'US', 'USD')
            ON CONFLICT (region_code) DO UPDATE SET region_name = EXCLUDED.region_name
            RETURNING region_id
        ), sel AS (
            INSERT INTO qq.sellers (seller_uid, display_name, region_id, rating,
                                    onboarded_at, source_hash)
            SELECT %(code)s, 'Test Seller', reg.region_id, 4.5, now(), sha256(convert_to(%(code)s, 'UTF8'))
            FROM reg RETURNING seller_id
        ), cat AS (
            INSERT INTO qq.categories (category_slug, category_name, depth)
            VALUES (%(code)s, 'Test Category', 0)
            ON CONFLICT (category_slug) DO UPDATE SET category_name = EXCLUDED.category_name
            RETURNING category_id
        ), prod AS (
            INSERT INTO qq.products (product_uid, title, seller_id, category_id,
                                     base_price, launched_at, source_hash)
            SELECT %(code)s, 'Test Product', sel.seller_id, cat.category_id, 10.00,
                   now(), sha256(convert_to(%(code)s, 'UTF8'))
            FROM sel, cat RETURNING product_id
        ), var AS (
            INSERT INTO qq.product_variants (sku, product_id, variant_label, source_hash)
            SELECT %(code)s, prod.product_id, 'default', sha256(convert_to(%(code)s, 'UTF8'))
            FROM prod RETURNING variant_id
        )
        INSERT INTO qq.inventory (variant_id, qty_on_hand, qty_reserved, reorder_point)
        SELECT var.variant_id, %(stock)s, 0, 5 FROM var
        RETURNING variant_id
        """,
        {"code": f"TEST-{uid}", "stock": 100},
    )
    conn.commit()
    return row["variant_id"]
