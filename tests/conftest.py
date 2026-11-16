"""Shared fixtures: schema bootstrap and FastAPI TestClient.

DATABASE_URL points at the local PostgreSQL cluster (scripts/dev_pg.sh) or the
docker-compose database. Each test gets a dedicated, randomly-named schema so
tests never see each other's rows even though they share one database.

Schema drop is best-effort with a short lock_timeout: a leftover backend from a
crashed earlier process may hold locks on an unrelated schema, and blocking on
DROP would hang the whole suite. Random schema names mean undeferred drops are
harmless and can be reaped later.
"""
from __future__ import annotations

import os
import pathlib
import uuid

import psycopg
import pytest

os.environ.setdefault(
    "DATABASE_URL", "postgresql://node:node@localhost:5439/meterdb"
)

_BASE_URL = os.environ["DATABASE_URL"]
SCHEMA_SQL = pathlib.Path(__file__).resolve().parents[1] / "app" / "schema.sql"


@pytest.fixture(scope="session")
def admin_conn():
    with psycopg.connect(_BASE_URL, autocommit=True) as conn:
        # Never hang indefinitely on a lock left by a crashed process.
        conn.execute("SET lock_timeout = '2s'")
        yield conn


@pytest.fixture()
def test_schema(admin_conn):
    schema = f"t_{uuid.uuid4().hex[:16]}"
    with admin_conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema}")
        # Create tables directly in the new schema so tests that only use
        # standalone connections (concurrency tests) have the DDL even though
        # they never start the API/pool.
        cur.execute(f"SET search_path TO {schema}")
        cur.execute(SCHEMA_SQL.read_text())
    sep = "&" if "?" in _BASE_URL else "?"
    # search_path via libpq option keeps every pooled connection in the schema.
    os.environ["DATABASE_URL"] = (
        f"{_BASE_URL}{sep}options=-csearch_path%3D{schema}"
    )
    os.environ["TEST_SCHEMA"] = schema
    # Rebuild the pool so its connections use the new search_path.
    from app import db as app_db
    app_db.pool = None
    try:
        yield schema
    finally:
        # Best-effort cleanup; a lock timeout here must not fail the test.
        try:
            with admin_conn.cursor() as cur:
                cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        except psycopg.Error:
            admin_conn.rollback()
        os.environ["DATABASE_URL"] = _BASE_URL
        os.environ.pop("TEST_SCHEMA", None)
        try:
            if app_db.pool is not None:
                app_db.pool.close()
        except psycopg.Error:
            pass
        app_db.pool = None


@pytest.fixture()
def client(test_schema):
    from app.main import app
    from fastapi.testclient import TestClient

    with TestClient(app) as c:
        yield c


@pytest.fixture()
def db_conn(test_schema):
    """Direct connection into the test schema for assertions."""
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        conn.execute("SET lock_timeout = '5s'")
        yield conn
