"""Idempotent ETL: COPY -> dedup -> data quality -> key resolution -> UPSERT."""

from .pipeline import BatchResult, QuarantinedBatch, run_all, run_file  # noqa: F401
from .specs import LOAD_ORDER, BY_NAME, TableSpec  # noqa: F401
