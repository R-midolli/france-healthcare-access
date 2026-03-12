"""
db.py — Shared database utilities.

Provides the SQLAlchemy engine (from POSTGRES_URL env var) and
helpers to create the raw / staging / mart schemas on first run.
"""

import os

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


DEFAULT_POSTGRES_URL = "postgresql://healthcare:healthcare@localhost:5433/healthcare"


def get_engine() -> Engine:
    """Return a SQLAlchemy engine using POSTGRES_URL (with sensible default)."""
    url = os.environ.get("POSTGRES_URL", DEFAULT_POSTGRES_URL)
    return create_engine(url, pool_pre_ping=True)


def ensure_schemas(engine: Engine) -> None:
    """Create raw / staging / mart schemas if they do not exist."""
    with engine.begin() as conn:
        for schema in ("raw", "staging", "mart"):
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))


def get_last_refresh(engine: Engine) -> str | None:
    """Return the timestamp of the last successful pipeline run, or None."""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT completed_at FROM mart.pipeline_runs WHERE status = 'success' ORDER BY completed_at DESC LIMIT 1")
            ).fetchone()
        return result[0] if result else None
    except Exception:
        return None
