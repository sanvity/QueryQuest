"""Runtime configuration, resolved from the environment.

Everything the pipeline needs is read once, here, so that no module
reaches for os.environ at call time -- that pattern makes tests
untestable and behaviour depend on import order.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

try:  # optional convenience, not a hard dependency
    from dotenv import load_dotenv

    load_dotenv(override=False)
except ImportError:  # pragma: no cover
    pass


REPO_ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = REPO_ROOT / "sql"
QUERY_DIR = SQL_DIR / "queries"
DATA_DIR = Path(os.getenv("QQ_DATA_DIR", REPO_ROOT / "data"))
REPORT_DIR = Path(os.getenv("QQ_REPORT_DIR", REPO_ROOT / "reports"))

OPTIMIZED_SCHEMA = "qq"
BASELINE_SCHEMA = "qq_baseline"
META_SCHEMA = "qq_meta"
MART_SCHEMA = "qq_mart"


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    # --- PostgreSQL -----------------------------------------------------
    dsn: str = field(
        default_factory=lambda: os.getenv(
            "QQ_DSN", "postgresql://postgres:postgres@localhost:5432/queryquest"
        )
    )
    pool_min: int = field(default_factory=lambda: _int("QQ_POOL_MIN", 2))
    pool_max: int = field(default_factory=lambda: _int("QQ_POOL_MAX", 16))
    statement_timeout_ms: int = field(
        default_factory=lambda: _int("QQ_STATEMENT_TIMEOUT_MS", 120_000)
    )
    lock_timeout_ms: int = field(default_factory=lambda: _int("QQ_LOCK_TIMEOUT_MS", 5_000))

    # --- Redis ----------------------------------------------------------
    redis_url: str = field(
        default_factory=lambda: os.getenv("QQ_REDIS_URL", "redis://localhost:6379/0")
    )
    cache_ttl_s: int = field(default_factory=lambda: _int("QQ_CACHE_TTL_S", 300))
    # Jitter stops a synchronised herd of keys from expiring in the same
    # second and stampeding the database. +/- 20% by default.
    cache_ttl_jitter: float = field(default_factory=lambda: _float("QQ_CACHE_TTL_JITTER", 0.2))
    cache_lock_timeout_s: int = field(default_factory=lambda: _int("QQ_CACHE_LOCK_TIMEOUT_S", 10))
    cache_enabled: bool = field(default_factory=lambda: _bool("QQ_CACHE_ENABLED", True))
    cache_namespace: str = field(default_factory=lambda: os.getenv("QQ_CACHE_NS", "qq"))

    # --- ETL ------------------------------------------------------------
    batch_rows: int = field(default_factory=lambda: _int("QQ_BATCH_ROWS", 50_000))
    copy_buffer_rows: int = field(default_factory=lambda: _int("QQ_COPY_BUFFER_ROWS", 10_000))
    # A batch whose reject ratio exceeds this is quarantined wholesale
    # rather than half-loaded: partial garbage is worse than no data.
    max_reject_ratio: float = field(default_factory=lambda: _float("QQ_MAX_REJECT_RATIO", 0.02))

    # --- Concurrency ----------------------------------------------------
    max_retries: int = field(default_factory=lambda: _int("QQ_MAX_RETRIES", 5))
    retry_base_ms: int = field(default_factory=lambda: _int("QQ_RETRY_BASE_MS", 20))

    @property
    def report_dir(self) -> Path:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        return REPORT_DIR

    @property
    def data_dir(self) -> Path:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        return DATA_DIR


settings = Settings()
