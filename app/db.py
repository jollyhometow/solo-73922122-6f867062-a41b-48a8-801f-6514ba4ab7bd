"""PostgreSQL connection pool and schema bootstrap."""
from __future__ import annotations

from pathlib import Path

from psycopg_pool import ConnectionPool

from .config import settings

pool: ConnectionPool | None = None
_SCHEMA_PATH = Path(__file__).with_name("schema.sql")


def init_pool() -> ConnectionPool:
    global pool
    if pool is None:
        pool = ConnectionPool(
            settings.database_url,
            min_size=settings.pool_min_size,
            max_size=settings.pool_max_size,
            kwargs={"autocommit": False},
            open=True,
        )
    if settings.init_schema:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # CREATE EXTENSION / IF NOT EXISTS statements are safe to repeat.
                cur.execute(_SCHEMA_PATH.read_text())
            conn.commit()
    return pool


def close_pool() -> None:
    global pool
    if pool is not None:
        pool.close()
        pool = None
