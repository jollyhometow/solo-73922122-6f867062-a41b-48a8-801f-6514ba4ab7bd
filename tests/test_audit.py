"""Functional tests for GET /audit/coverage/{line}.

Covers grid generation anchored at the window start, clipping to ownership
segments, missing-interval merging, duplicate flagging, input validation and
the single-snapshot guarantee against concurrent corrections/ingestions.
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

from app import services

UTC = timezone.utc
T0 = datetime(2026, 3, 1, 0, 0, tzinfo=UTC)
STEP = 900  # 15 minutes


def iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def grid(frm: datetime, to: datetime, step: int = STEP) -> list[datetime]:
    out, t = [], frm
    while t < to:
        out.append(t)
        t += timedelta(seconds=step)
    return out


def register(client, meter, frm, to, line):
    r = client.post("/ownership/register", json={
        "meter_no": meter, "valid_from": iso(frm),
        "valid_to": iso(to), "line": line})
    assert r.status_code == 201, r.text
    return r.json()["version"]


def ingest(client, meter, timestamps, uid_prefix):
    r = client.post("/readings/ingest", json={"readings": [
        {"reading_uid": f"{uid_prefix}-{i}", "meter_no": meter,
         "ts": iso(ts), "wh": 1}
        for i, ts in enumerate(timestamps)]})
    assert r.status_code == 201, r.text


def audit(client, line, frm, to, step=STEP, expected_status=200):
    r = client.get(f"/audit/coverage/{line}", params={
        "start": iso(frm), "end": iso(to), "interval_seconds": step})
    assert r.status_code == expected_status, r.text
    return r.json() if r.status_code == 200 else r


# ---------------------------------------------------------------------------
# happy path
# ---------------------------------------------------------------------------

def test_audit_complete_coverage(client):
    meter = "M-AUD-OK"
    register(client, meter, T0, T0 + timedelta(hours=4), "LINE-A")
    ingest(client, meter, grid(T0, T0 + timedelta(hours=4)), "OK")

    res = audit(client, "LINE-A", T0, T0 + timedelta(hours=4))
    assert res["line"] == "LINE-A"
    assert res["interval_seconds"] == STEP
    assert res["expected_total"] == 16
    assert res["missing_total"] == 0
    assert res["meters"] == [
        {"meter_no": meter, "expected": 16, "present": 16, "missing": 0}]
    assert res["missing_intervals"] == []
    assert res["duplicates"] == []


def test_audit_missing_intervals_are_merged_per_run(client):
    meter = "M-AUD-GAP"
    register(client, meter, T0, T0 + timedelta(hours=4), "LINE-A")
    present = [t for t in grid(T0, T0 + timedelta(hours=4))
               if t not in {
                   # run of three consecutive misses
                   T0 + timedelta(hours=1),
                   T0 + timedelta(hours=1, minutes=15),
                   T0 + timedelta(hours=1, minutes=30),
                   # isolated miss
                   T0 + timedelta(hours=3)}]
    ingest(client, meter, present, "GAP")

    res = audit(client, "LINE-A", T0, T0 + timedelta(hours=4))
    assert res["expected_total"] == 16
    assert res["missing_total"] == 4
    assert res["missing_intervals"] == [
        {"meter_no": meter,
         "interval_from": iso(T0 + timedelta(hours=1)),
         # right-open: last missing instant 01:30 + 15 min
         "interval_to": iso(T0 + timedelta(hours=1, minutes=45)),
         "missing_count": 3},
        {"meter_no": meter,
         "interval_from": iso(T0 + timedelta(hours=3)),
         "interval_to": iso(T0 + timedelta(hours=3, minutes=15)),
         "missing_count": 1},
    ]
    assert res["meters"][0]["missing"] == 4


def test_audit_clips_expectation_to_ownership_segments(client):
    meter = "M-AUD-PART"
    # meter belongs to LINE-A only for [01:00, 03:00)
    register(client, meter, T0 + timedelta(hours=1),
             T0 + timedelta(hours=3), "LINE-A")

    res = audit(client, "LINE-A", T0, T0 + timedelta(hours=4))
    # only the 8 instants inside the segment are expected
    assert res["expected_total"] == 8
    assert res["missing_total"] == 8
    assert res["missing_intervals"] == [{
        "meter_no": meter,
        "interval_from": iso(T0 + timedelta(hours=1)),
        "interval_to": iso(T0 + timedelta(hours=3)),
        "missing_count": 8}]


def test_audit_segments_with_ownership_gap(client):
    meter = "M-AUD-2SEG"
    register(client, meter, T0, T0 + timedelta(hours=1), "LINE-A")
    register(client, meter, T0 + timedelta(hours=2),
             T0 + timedelta(hours=4), "LINE-A")

    res = audit(client, "LINE-A", T0, T0 + timedelta(hours=4))
    # 4 instants in [00:00,01:00) + 8 in [02:00,04:00); the gap is not audited
    assert res["expected_total"] == 12
    assert res["missing_total"] == 12
    assert [(i["interval_from"], i["interval_to"], i["missing_count"])
            for i in res["missing_intervals"]] == [
        (iso(T0), iso(T0 + timedelta(hours=1)), 4),
        (iso(T0 + timedelta(hours=2)), iso(T0 + timedelta(hours=4)), 8),
    ]


def test_audit_grid_is_anchored_at_window_start(client):
    meter = "M-AUD-ANCHOR"
    register(client, meter, T0, T0 + timedelta(hours=2), "LINE-A")
    frm = T0 + timedelta(minutes=7)  # unaligned window start
    to = T0 + timedelta(hours=1, minutes=7)
    ingest(client, meter, grid(frm, to), "ANCHOR")

    res = audit(client, "LINE-A", frm, to)
    # instants are 00:07, 00:22, 00:37, 00:52 -- not wall-clock quarters
    assert res["expected_total"] == 4
    assert res["missing_total"] == 0


def test_audit_offgrid_reading_does_not_fill_a_slot(client):
    meter = "M-AUD-OFFGRID"
    register(client, meter, T0, T0 + timedelta(hours=1), "LINE-A")
    ingest(client, meter, [T0 + timedelta(minutes=7)], "OFFGRID")

    res = audit(client, "LINE-A", T0, T0 + timedelta(hours=1))
    assert res["expected_total"] == 4
    assert res["missing_total"] == 4
    # the off-grid reading is counted as present but covers no instant
    assert res["meters"] == [
        {"meter_no": meter, "expected": 4, "present": 1, "missing": 4}]


def test_audit_duplicates_flagged_and_still_present(client):
    meter = "M-AUD-DUP"
    other = "M-AUD-DUP-OTHER"
    register(client, meter, T0, T0 + timedelta(hours=2), "LINE-A")
    register(client, other, T0, T0 + timedelta(hours=2), "LINE-B")
    ts = grid(T0, T0 + timedelta(hours=2))
    ingest(client, meter, ts, "DUP")
    # second reading at the same instant (different reading_uid)
    dup_ts = T0 + timedelta(minutes=30)
    ingest(client, meter, [dup_ts], "DUP-AGAIN")
    # a duplicate on another line must not leak into this audit
    ingest(client, other, ts, "OTHER")
    ingest(client, other, [dup_ts], "OTHER-AGAIN")

    res = audit(client, "LINE-A", T0, T0 + timedelta(hours=2))
    assert res["missing_total"] == 0
    assert res["duplicates"] == [
        {"meter_no": meter, "ts": iso(dup_ts), "count": 2}]
    # both copies count towards "present"
    assert res["meters"] == [
        {"meter_no": meter, "expected": 8, "present": 9, "missing": 0}]


def test_audit_multiple_meters_reported_separately(client):
    m1, m2 = "M-AUD-MM-1", "M-AUD-MM-2"
    register(client, m1, T0, T0 + timedelta(hours=1), "LINE-A")
    register(client, m2, T0, T0 + timedelta(hours=1), "LINE-A")
    # m1 complete, m2 misses 00:15
    ingest(client, m1, grid(T0, T0 + timedelta(hours=1)), "MM1")
    ingest(client, m2, [T0, T0 + timedelta(minutes=30),
                        T0 + timedelta(minutes=45)], "MM2")

    res = audit(client, "LINE-A", T0, T0 + timedelta(hours=1))
    assert res["expected_total"] == 8
    assert res["missing_total"] == 1
    assert {m["meter_no"]: m for m in res["meters"]} == {
        m1: {"meter_no": m1, "expected": 4, "present": 4, "missing": 0},
        m2: {"meter_no": m2, "expected": 4, "present": 3, "missing": 1},
    }
    assert res["missing_intervals"] == [{
        "meter_no": m2,
        "interval_from": iso(T0 + timedelta(minutes=15)),
        "interval_to": iso(T0 + timedelta(minutes=30)),
        "missing_count": 1}]


def test_audit_scoped_to_audited_line(client):
    register(client, "M-AUD-SC-A", T0, T0 + timedelta(hours=1), "LINE-A")
    register(client, "M-AUD-SC-B", T0, T0 + timedelta(hours=1), "LINE-B")
    ingest(client, "M-AUD-SC-B", grid(T0, T0 + timedelta(hours=1)), "SCB")

    res_a = audit(client, "LINE-A", T0, T0 + timedelta(hours=1))
    assert [m["meter_no"] for m in res_a["meters"]] == ["M-AUD-SC-A"]
    assert res_a["missing_total"] == 4
    res_b = audit(client, "LINE-B", T0, T0 + timedelta(hours=1))
    assert res_b["missing_total"] == 0


def test_audit_reflects_correction_as_one_complete_state(client):
    meter = "M-AUD-COR"
    register(client, meter, T0, T0 + timedelta(hours=4), "LINE-A")
    ingest(client, meter, grid(T0, T0 + timedelta(hours=4)), "COR")
    version = client.get(f"/meters/{meter}").json()["version"]
    r = client.post("/ownership/correct", json={
        "meter_no": meter,
        "valid_from": iso(T0 + timedelta(hours=1)),
        "valid_to": iso(T0 + timedelta(hours=2)),
        "new_line": "LINE-B", "base_version": version})
    assert r.status_code == 200, r.text

    res_a = audit(client, "LINE-A", T0, T0 + timedelta(hours=4))
    # the corrected window is no longer expected on LINE-A ...
    assert res_a["expected_total"] == 12
    assert res_a["missing_total"] == 0
    # ... and appears on LINE-B with its readings fully re-linked
    res_b = audit(client, "LINE-B", T0, T0 + timedelta(hours=4))
    assert res_b["expected_total"] == 4
    assert res_b["missing_total"] == 0
    assert res_b["meters"] == [
        {"meter_no": meter, "expected": 4, "present": 4, "missing": 0}]


def test_audit_unknown_line_returns_empty_report(client):
    res = audit(client, "LINE-GHOST", T0, T0 + timedelta(hours=1))
    assert res["expected_total"] == 0
    assert res["missing_total"] == 0
    assert res["meters"] == []
    assert res["missing_intervals"] == []
    assert res["duplicates"] == []


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------

def test_audit_rejects_inverted_window(client):
    audit(client, "LINE-A", T0 + timedelta(hours=1), T0, expected_status=422)
    audit(client, "LINE-A", T0, T0, expected_status=422)


def test_audit_rejects_non_positive_interval(client):
    for bad in (0, -300):
        r = client.get("/audit/coverage/LINE-A", params={
            "start": iso(T0), "end": iso(T0 + timedelta(hours=1)),
            "interval_seconds": bad})
        assert r.status_code == 422


def test_audit_rejects_naive_datetimes(client):
    r = client.get("/audit/coverage/LINE-A", params={
        "start": "2026-03-01T00:00:00",  # no timezone
        "end": iso(T0 + timedelta(hours=1)), "interval_seconds": STEP})
    assert r.status_code == 422
    assert "timezone" in r.json()["detail"]


def test_audit_normalizes_offset_timezones(client):
    meter = "M-AUD-TZ"
    register(client, meter, T0, T0 + timedelta(hours=4), "LINE-A")
    ingest(client, meter, grid(T0, T0 + timedelta(hours=4)), "TZ")
    plus2 = timezone(timedelta(hours=2))
    # 02:00+02:00 == 00:00Z, 03:00+02:00 == 01:00Z
    r = client.get("/audit/coverage/LINE-A", params={
        "start": T0.astimezone(plus2).isoformat(),
        "end": (T0 + timedelta(hours=1)).astimezone(plus2).isoformat(),
        "interval_seconds": STEP})
    assert r.status_code == 200, r.text
    assert r.json()["expected_total"] == 4
    assert r.json()["missing_total"] == 0
    # response echoes the window normalized to UTC
    assert r.json()["window_start"] == iso(T0)


def test_audit_grid_size_guard(client):
    # 30 days at 1-second resolution exceeds the default 1M-point guard
    r = client.get("/audit/coverage/LINE-A", params={
        "start": iso(T0), "end": iso(T0 + timedelta(days=30)),
        "interval_seconds": 1})
    assert r.status_code == 422
    assert "scheduled instants" in r.json()["detail"]


# ---------------------------------------------------------------------------
# snapshot isolation
# ---------------------------------------------------------------------------

def test_audit_statements_share_one_snapshot(connection_factory):
    """A correction committing *between* the audit's statements must not leak
    into the report: every statement reads the snapshot taken at the first
    statement. Uses the very SQL the service runs."""
    setup = connection_factory()
    meter = "M-AUD-SNAP"
    services.register_ownership(
        setup, meter, T0, T0 + timedelta(hours=24), "LINE-A")
    services.ingest_readings(setup, [
        {"reading_uid": f"SNAP-{i}", "meter_no": meter, "ts": ts, "wh": 1}
        for i, ts in enumerate(grid(T0, T0 + timedelta(hours=24)))])
    version = services.get_meter(setup, meter)["version"]
    setup.close()

    params = {"line": "LINE-A", "start": T0,
              "end": T0 + timedelta(hours=24), "step": STEP}
    audit_conn = connection_factory()
    with audit_conn.transaction():
        cur = audit_conn.cursor()
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        cur.execute(services._SQL_AUDIT_SUMMARY, params)
        summary = cur.fetchall()

        # a full correction commits between the audit's statements
        writer = connection_factory()
        services.correct_ownership(
            writer, meter, T0 + timedelta(hours=10),
            T0 + timedelta(hours=14), "LINE-B", base_version=version)
        writer.close()

        cur.execute(services._SQL_AUDIT_MISSING, params)
        missing = cur.fetchall()
        cur.execute(services._SQL_AUDIT_DUPLICATES, params)
        duplicates = cur.fetchall()

    # everything still reflects the pre-correction snapshot
    assert summary == [(meter, 96, 96, 0)]
    assert missing == []
    assert duplicates == []

    # a fresh audit sees the committed correction as one complete state
    check = connection_factory()
    res_a = services.audit_coverage(
        check, "LINE-A", T0, T0 + timedelta(hours=24), STEP)
    assert res_a["expected_total"] == 96 - 16
    assert res_a["missing_total"] == 0
    res_b = services.audit_coverage(
        check, "LINE-B", T0, T0 + timedelta(hours=24), STEP)
    assert res_b["expected_total"] == 16
    assert res_b["missing_total"] == 0
    check.close()


def test_audit_reports_one_committed_state_under_concurrency(
        connection_factory):
    """Stress the full service call against concurrent corrections: every
    response must be internally consistent (per-meter missing counts always
    match the missing-interval list), which only holds for single-snapshot
    reads."""
    setup = connection_factory()
    meter = "M-AUD-RACE"
    services.register_ownership(
        setup, meter, T0, T0 + timedelta(hours=24), "LINE-A")
    services.ingest_readings(setup, [
        {"reading_uid": f"RACE-{i}", "meter_no": meter, "ts": ts, "wh": 1}
        for i, ts in enumerate(grid(T0, T0 + timedelta(hours=24)))])
    setup.close()

    stop = threading.Event()
    errors: list[Exception] = []

    def writer():
        flip = False
        while not stop.is_set():
            conn = connection_factory()
            try:
                version = services.get_meter(conn, meter)["version"]
                services.correct_ownership(
                    conn, meter, T0 + timedelta(hours=10),
                    T0 + timedelta(hours=14),
                    "LINE-B" if flip else "LINE-A",
                    base_version=version)
                flip = not flip
            except services.ConflictError:
                conn.rollback()  # a concurrent correction won; retry
            except Exception as exc:  # noqa: BLE001
                conn.rollback()
                errors.append(exc)
                stop.set()
            finally:
                conn.close()

    t = threading.Thread(target=writer)
    t.start()
    try:
        for _ in range(30):
            conn = connection_factory()
            try:
                res = services.audit_coverage(
                    conn, "LINE-A", T0, T0 + timedelta(hours=24), STEP)
            finally:
                conn.close()
            by_meter = {i["meter_no"]: 0 for i in res["missing_intervals"]}
            for i in res["missing_intervals"]:
                by_meter[i["meter_no"]] += i["missing_count"]
            for m in res["meters"]:
                assert m["missing"] == by_meter.get(m["meter_no"], 0), res
            assert res["missing_total"] == sum(by_meter.values()), res
            # readings are re-pointed, never dropped: whatever the state,
            # every expected instant of this meter has its reading
            assert res["missing_total"] == 0, res
    finally:
        stop.set()
        t.join()
    assert not errors
