"""Concurrency tests simulating two API instances sharing one database.

Every worker thread opens its OWN psycopg connection (a connection is not
thread-safe), exactly like two uvicorn processes. Correctness relies on row
locks (SELECT ... FOR UPDATE), the GiST exclusion constraint and the
optimistic meter version, all enforced inside PostgreSQL.
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

import pytest

from app import services

UTC = timezone.utc
T0 = datetime(2026, 6, 1, tzinfo=UTC)


def _current_version(conn, meter):
    with conn.cursor() as cur:
        cur.execute("SELECT version FROM meter WHERE meter_no = %s", (meter,))
        return cur.fetchone()[0]


def test_concurrent_corrections_one_succeeds_one_conflicts(connection_factory):
    setup = connection_factory()
    meter = "MC-1"
    services.register_ownership(
        setup, meter, T0, T0 + timedelta(hours=24), "LINE-A")
    version = _current_version(setup, meter)
    setup.close()

    barrier = threading.Barrier(2)
    results: list[tuple[str, object]] = []

    def correct(line, tag):
        conn = connection_factory()  # thread-private connection
        try:
            barrier.wait()
            v = services.correct_ownership(
                conn, meter, T0 + timedelta(hours=2),
                T0 + timedelta(hours=4), line, base_version=version,
            )
            results.append((tag, ("ok", v)))
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            results.append((tag, ("err", type(exc).__name__, str(exc))))
        finally:
            conn.close()

    t1 = threading.Thread(target=correct, args=("LINE-B", "i1"))
    t2 = threading.Thread(target=correct, args=("LINE-C", "i2"))
    t1.start(); t2.start(); t1.join(); t2.join()

    statuses = sorted(r[1][0] for r in results)
    assert statuses == ["err", "ok"], results
    ok = next(r for r in results if r[1][0] == "ok")
    assert ok[1][1] == version + 1
    err = next(r for r in results if r[1][0] == "err")
    assert err[1][1] == "ConflictError"

    check = connection_factory()
    with check.cursor() as cur:
        cur.execute("SELECT line, valid_from, valid_to FROM ownership "
                    "WHERE meter_no = %s ORDER BY valid_from", (meter,))
        segs = cur.fetchall()
    check.close()
    assert len(segs) == 3
    # the loser's line must never have been written
    lines = {s[0] for s in segs}
    assert lines <= {"LINE-A", "LINE-B"} or lines <= {"LINE-A", "LINE-C"}


def test_concurrent_ingest_and_correction_never_orphans(connection_factory):
    setup = connection_factory()
    meter = "MC-2"
    services.register_ownership(
        setup, meter, T0, T0 + timedelta(hours=24), "LINE-A")
    base_version = _current_version(setup, meter)
    setup.close()

    errors: list[Exception] = []

    def ingest(idx):
        conn = connection_factory()
        try:
            services.ingest_readings(conn, [{
                "reading_uid": f"CC-{idx}",
                "meter_no": meter,
                "ts": T0 + timedelta(hours=5),
                "wh": idx,
            }])
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            errors.append(exc)
        finally:
            conn.close()

    def correct():
        conn = connection_factory()
        try:
            services.correct_ownership(
                conn, meter, T0 + timedelta(hours=4),
                T0 + timedelta(hours=6), "LINE-B",
                base_version=base_version,
            )
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            errors.append(exc)
        finally:
            conn.close()

    threads = [
        threading.Thread(target=ingest, args=(1,)),
        threading.Thread(target=correct),
        threading.Thread(target=ingest, args=(2,)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    check = connection_factory()
    with check.cursor() as cur:
        cur.execute(
            "SELECT r.reading_uid, o.line, o.valid_from, o.valid_to, r.ts "
            "FROM reading r JOIN ownership o ON o.id = r.ownership_id "
            "WHERE r.meter_no = %s ORDER BY r.reading_uid", (meter,))
        rows = cur.fetchall()
    check.close()
    assert len(rows) == 2, errors
    for uid, line, vf, vt, ts in rows:
        assert vf <= ts < vt
        assert line in {"LINE-A", "LINE-B"}


def test_concurrent_overlapping_registrations_one_rejected(connection_factory):
    meter = "MC-3"
    barrier = threading.Barrier(2)
    outcomes: list[tuple[str, str]] = []

    def register(line, tag, shift):
        conn = connection_factory()
        try:
            barrier.wait()
            services.register_ownership(
                conn, meter,
                T0 + timedelta(hours=shift),
                T0 + timedelta(hours=10 + shift), line,
            )
            outcomes.append((tag, "ok"))
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            outcomes.append((tag, type(exc).__name__))
        finally:
            conn.close()

    t1 = threading.Thread(target=register, args=("LINE-A", "i1", 0))
    t2 = threading.Thread(target=register, args=("LINE-B", "i2", 5))
    t1.start(); t2.start(); t1.join(); t2.join()

    kinds = sorted(o[1] for o in outcomes)
    assert kinds == ["ConflictError", "ok"], outcomes
    check = connection_factory()
    with check.cursor() as cur:
        cur.execute("SELECT count(*) FROM ownership WHERE meter_no = %s",
                    (meter,))
        assert cur.fetchone()[0] == 1
    check.close()


def test_failed_correction_rolls_back_everything(connection_factory):
    c1 = connection_factory()
    c2 = connection_factory()
    meter = "MC-4"
    services.register_ownership(
        c1, meter, T0, T0 + timedelta(hours=24), "LINE-A")
    services.ingest_readings(c1, [{
        "reading_uid": "ROLL-1", "meter_no": meter,
        "ts": T0 + timedelta(hours=3), "wh": 9}])
    version_before = _current_version(c1, meter)

    with pytest.raises(services.ConflictError):
        services.correct_ownership(
            c2, meter, T0 + timedelta(hours=1), T0 + timedelta(hours=2),
            "LINE-X", base_version=version_before + 100,  # stale
        )

    with c2.cursor() as cur:
        cur.execute("SELECT version FROM meter WHERE meter_no = %s", (meter,))
        assert cur.fetchone()[0] == version_before
        cur.execute("SELECT count(*) FROM ownership WHERE meter_no = %s",
                    (meter,))
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT line FROM ownership WHERE meter_no = %s", (meter,))
        assert cur.fetchone()[0] == "LINE-A"
        cur.execute("SELECT count(*) FROM ownership_history WHERE meter_no = %s",
                    (meter,))
        assert cur.fetchone()[0] == 1  # only the registration INSERT
        cur.execute(
            "SELECT r.reading_uid, o.line FROM reading r "
            "JOIN ownership o ON o.id = r.ownership_id WHERE r.meter_no = %s",
            (meter,))
        assert cur.fetchall() == [("ROLL-1", "LINE-A")]
    c1.close(); c2.close()
