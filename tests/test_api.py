"""End-to-end functional tests through the FastAPI TestClient."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

UTC = timezone.utc
T0 = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)


def iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def register(client, meter, frm, to, line, expected=201):
    r = client.post(
        "/ownership/register",
        json={
            "meter_no": meter,
            "valid_from": iso(frm),
            "valid_to": iso(to),
            "line": line,
        },
    )
    assert r.status_code == expected, r.text
    return r.json() if r.status_code < 300 else r


def test_health(client):
    assert client.get("/health").json()["status"] == "ok"


def test_register_non_overlapping_segments(client):
    meter = "M-REG-1"
    v1 = register(client, meter, T0, T0 + timedelta(hours=12), "LINE-A")
    assert v1["version"] == 1
    v2 = register(client, meter, T0 + timedelta(hours=12),
                  T0 + timedelta(hours=24), "LINE-B")
    assert v2["version"] == 2

    segments = client.get(f"/ownership/{meter}").json()
    assert [s["line"] for s in segments] == ["LINE-A", "LINE-B"]
    # touching segments are allowed (left-closed/right-open, no overlap)
    assert segments[0]["valid_to"] == segments[1]["valid_from"]


def test_overlapping_registration_rejected_and_keeps_history(client):
    meter = "M-REG-2"
    register(client, meter, T0, T0 + timedelta(hours=10), "LINE-A")
    version_after_success = client.get(f"/meters/{meter}").json()["version"]
    before = client.get(f"/ownership/{meter}").json()

    r = register(client, meter, T0 + timedelta(hours=5),
                 T0 + timedelta(hours=15), "LINE-B", expected=409)
    assert "overlap" in r.json()["detail"]

    # failed transaction leaves ownership and meter version untouched:
    # the version is still the one the successful registration produced.
    after = client.get(f"/ownership/{meter}").json()
    assert before == after
    assert (client.get(f"/meters/{meter}").json()["version"]
            == version_after_success)


def test_invalid_segment_range_rejected(client):
    r = register(client, "M-REG-3", T0 + timedelta(hours=1), T0, "LINE-A",
                 expected=422)
    # pydantic-level or service-level validation, both acceptable as 422
    assert r.status_code == 422


def test_ingest_binds_to_covering_ownership(client):
    meter = "M-ING-1"
    register(client, meter, T0, T0 + timedelta(hours=12), "LINE-A")
    register(client, meter, T0 + timedelta(hours=12),
             T0 + timedelta(hours=24), "LINE-B")

    r = client.post("/readings/ingest", json={"readings": [
        {"reading_uid": "U-1", "meter_no": meter,
         "ts": iso(T0 + timedelta(hours=11, minutes=59)), "wh": 100},
        {"reading_uid": "U-2", "meter_no": meter,
         "ts": iso(T0 + timedelta(hours=12)), "wh": 200},
    ]})
    assert r.status_code == 201, r.text
    rows = {x["reading_uid"]: x for x in r.json()["readings"]}
    # right-open boundary: 12:00 belongs to the second segment
    assert rows["U-1"]["line"] == "LINE-A"
    assert rows["U-2"]["line"] == "LINE-B"
    assert rows["U-1"]["ownership_id"] != rows["U-2"]["ownership_id"]


def test_ingest_without_ownership_is_rejected(client):
    meter = "M-ING-2"
    register(client, meter, T0, T0 + timedelta(hours=12), "LINE-A")
    r = client.post("/readings/ingest", json={"readings": [
        {"reading_uid": "U-10", "meter_no": meter,
         "ts": iso(T0 + timedelta(hours=13)), "wh": 50},
    ]})
    assert r.status_code == 422
    assert "no ownership" in r.json()["detail"]

    # whole batch is atomic: the covered reading in the same batch is absent
    r2 = client.post("/readings/ingest", json={"readings": [
        {"reading_uid": "U-11", "meter_no": meter,
         "ts": iso(T0 + timedelta(hours=1)), "wh": 10},
        {"reading_uid": "U-12", "meter_no": meter,
         "ts": iso(T0 + timedelta(hours=13)), "wh": 20},
    ]})
    assert r2.status_code == 422
    rows = client.get(f"/readings/by-line/LINE-A",
                      params={"start": iso(T0), "end": iso(T0 + timedelta(days=2))}).json()
    assert all(x["reading_uid"] != "U-11" for x in rows)


def test_duplicate_reading_uid_conflict(client):
    meter = "M-ING-3"
    register(client, meter, T0, T0 + timedelta(hours=12), "LINE-A")
    payload = {"readings": [
        {"reading_uid": "DUP-1", "meter_no": meter,
         "ts": iso(T0 + timedelta(hours=1)), "wh": 1}]}
    assert client.post("/readings/ingest", json=payload).status_code == 201
    r = client.post("/readings/ingest", json=payload)
    assert r.status_code == 409


def test_unregistered_meter_ingest_rejected(client):
    r = client.post("/readings/ingest", json={"readings": [
        {"reading_uid": "U-99", "meter_no": "GHOST",
         "ts": iso(T0), "wh": 1}]})
    assert r.status_code == 404


def test_correction_replaces_window_and_preserves_sides(client, db_conn):
    meter = "M-COR-1"
    register(client, meter, T0, T0 + timedelta(hours=24), "LINE-A")
    # readings inside and outside the window
    client.post("/readings/ingest", json={"readings": [
        {"reading_uid": "C-1", "meter_no": meter,
         "ts": iso(T0 + timedelta(hours=2)), "wh": 10},
        {"reading_uid": "C-2", "meter_no": meter,
         "ts": iso(T0 + timedelta(hours=12)), "wh": 20},
        {"reading_uid": "C-3", "meter_no": meter,
         "ts": iso(T0 + timedelta(hours=22)), "wh": 30},
    ]})

    version = client.get(f"/meters/{meter}").json()["version"]
    r = client.post("/ownership/correct", json={
        "meter_no": meter,
        "valid_from": iso(T0 + timedelta(hours=6)),
        "valid_to": iso(T0 + timedelta(hours=18)),
        "new_line": "LINE-B",
        "base_version": version,
    })
    assert r.status_code == 200, r.text
    assert r.json()["version"] == version + 1

    segments = client.get(f"/ownership/{meter}").json()
    got = [(s["line"], s["valid_from"], s["valid_to"]) for s in segments]
    assert got == [
        ("LINE-A", iso(T0), iso(T0 + timedelta(hours=6))),
        ("LINE-B", iso(T0 + timedelta(hours=6)),
         iso(T0 + timedelta(hours=18))),
        ("LINE-A", iso(T0 + timedelta(hours=18)),
         iso(T0 + timedelta(hours=24))),
    ]

    # warehoused readings were re-linked (filter to this meter's uids because
    # the whole test session shares one schema)
    rows = {x["reading_uid"]: x for x in client.get(
        f"/readings/by-line/LINE-B",
        params={"start": iso(T0), "end": iso(T0 + timedelta(days=2))}).json()
        if x["meter_no"] == meter}
    assert set(rows) == {"C-2"}
    rows_a = {x["reading_uid"] for x in client.get(
        f"/readings/by-line/LINE-A",
        params={"start": iso(T0), "end": iso(T0 + timedelta(days=2))}).json()
        if x["meter_no"] == meter}
    assert rows_a == {"C-1", "C-3"}

    # every reading still has a non-null, covering FK
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM reading r JOIN ownership o ON o.id = r.ownership_id "
            "WHERE r.meter_no = %s AND (r.ts < o.valid_from OR r.ts >= o.valid_to)",
            (meter,),
        )
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT count(*) FROM reading WHERE meter_no = %s", (meter,))
        assert cur.fetchone()[0] == 3


def test_stale_correction_conflicts_and_changes_nothing(client, db_conn):
    meter = "M-COR-2"
    register(client, meter, T0, T0 + timedelta(hours=24), "LINE-A")
    client.post("/readings/ingest", json={"readings": [
        {"reading_uid": "S-1", "meter_no": meter,
         "ts": iso(T0 + timedelta(hours=1)), "wh": 5}]})

    stale = client.get(f"/meters/{meter}").json()["version"]
    # someone else corrects first -> version bumps
    client.post("/ownership/correct", json={
        "meter_no": meter, "valid_from": iso(T0),
        "valid_to": iso(T0 + timedelta(hours=2)),
        "new_line": "LINE-X", "base_version": stale})

    r = client.post("/ownership/correct", json={
        "meter_no": meter,
        "valid_from": iso(T0 + timedelta(hours=3)),
        "valid_to": iso(T0 + timedelta(hours=4)),
        "new_line": "LINE-Y", "base_version": stale})
    assert r.status_code == 409
    assert "stale" in r.json()["detail"]

    with db_conn.cursor() as cur:
        cur.execute("SELECT line FROM ownership WHERE meter_no = %s "
                    "ORDER BY valid_from", (meter,))
        lines = [x[0] for x in cur.fetchall()]
        assert "LINE-Y" not in lines
        cur.execute("SELECT count(*) FROM ownership_history WHERE meter_no = %s",
                    (meter,))
        # one INSERT (registration) + one REPLACE; failed correction added none
        assert cur.fetchone()[0] == 2


def test_correction_crossing_segment_boundaries(client):
    meter = "M-COR-3"
    register(client, meter, T0, T0 + timedelta(hours=12), "LINE-A")
    register(client, meter, T0 + timedelta(hours=12),
             T0 + timedelta(hours=24), "LINE-B")
    client.post("/readings/ingest", json={"readings": [
        {"reading_uid": "X-1", "meter_no": meter,
         "ts": iso(T0 + timedelta(hours=8)), "wh": 1},
        {"reading_uid": "X-2", "meter_no": meter,
         "ts": iso(T0 + timedelta(hours=16)), "wh": 2}]})

    version = client.get(f"/meters/{meter}").json()["version"]
    r = client.post("/ownership/correct", json={
        "meter_no": meter,
        "valid_from": iso(T0 + timedelta(hours=6)),
        "valid_to": iso(T0 + timedelta(hours=18)),
        "new_line": "LINE-C", "base_version": version})
    assert r.status_code == 200, r.text

    segments = client.get(f"/ownership/{meter}").json()
    assert [s["line"] for s in segments] == ["LINE-A", "LINE-C", "LINE-B"]
    rows = {x["reading_uid"]: x["line"] for x in client.get(
        f"/readings/by-line/LINE-C",
        params={"start": iso(T0), "end": iso(T0 + timedelta(days=2))}).json()}
    assert rows == {"X-1": "LINE-C", "X-2": "LINE-C"}


def test_correction_on_gap_rejected(client):
    meter = "M-COR-4"
    register(client, meter, T0, T0 + timedelta(hours=12), "LINE-A")
    version = client.get(f"/meters/{meter}").json()["version"]
    # window includes the uncovered 12:00-24:00 gap
    r = client.post("/ownership/correct", json={
        "meter_no": meter,
        "valid_from": iso(T0 + timedelta(hours=6)),
        "valid_to": iso(T0 + timedelta(hours=18)),
        "new_line": "LINE-Z", "base_version": version})
    assert r.status_code == 422
    assert "gap" in r.json()["detail"]


def test_by_line_only_sees_committed_data(client):
    meter = "M-Q-1"
    register(client, meter, T0, T0 + timedelta(hours=12), "LINE-Q")
    client.post("/readings/ingest", json={"readings": [
        {"reading_uid": "Q-1", "meter_no": meter,
         "ts": iso(T0 + timedelta(hours=1)), "wh": 7}]})
    rows = client.get("/readings/by-line/LINE-Q").json()
    assert len(rows) == 1 and rows[0]["wh"] == 7
    assert client.get("/readings/by-line/NOPE").json() == []


def test_correction_records_audit_history(client, db_conn):
    meter = "M-HIST-1"
    register(client, meter, T0, T0 + timedelta(hours=24), "LINE-A")
    version = client.get(f"/meters/{meter}").json()["version"]
    client.post("/ownership/correct", json={
        "meter_no": meter,
        "valid_from": iso(T0 + timedelta(hours=6)),
        "valid_to": iso(T0 + timedelta(hours=18)),
        "new_line": "LINE-B", "base_version": version})

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT action, old_line, new_line, meter_version "
            "FROM ownership_history WHERE meter_no = %s ORDER BY id",
            (meter,))
        history = cur.fetchall()
    # one registration INSERT, then a SPLIT (the window is strictly inside)
    assert history[0][0] == "INSERT" and history[0][2] == "LINE-A"
    assert history[1][0] == "SPLIT"
    assert history[1][1] == "LINE-A" and history[1][2] == "LINE-B"
    assert history[1][3] == version + 1


def test_integer_wh_rejects_floats_and_accepts_negative(client):
    meter = "M-WH-1"
    register(client, meter, T0, T0 + timedelta(hours=12), "LINE-A")
    # floats are not valid integer Wh -> 422 from pydantic
    r = client.post("/readings/ingest", json={"readings": [
        {"reading_uid": "WH-FLOAT", "meter_no": meter,
         "ts": iso(T0 + timedelta(hours=1)), "wh": 1.5}]})
    assert r.status_code == 422
    # negative integers are allowed (e.g. reversal/correction readings)
    r2 = client.post("/readings/ingest", json={"readings": [
        {"reading_uid": "WH-NEG", "meter_no": meter,
         "ts": iso(T0 + timedelta(hours=2)), "wh": -50}]})
    assert r2.status_code == 201, r2.text
    assert r2.json()["readings"][0]["wh"] == -50


def test_utc_normalization_of_offset_input(client):
    meter = "M-UTC-1"
    # 02:00+02:00 == 00:00Z; the API must normalize to UTC
    plus2 = timezone(timedelta(hours=2))
    frm = datetime(2026, 1, 1, 2, 0, tzinfo=plus2)
    to = datetime(2026, 1, 1, 14, 0, tzinfo=plus2)
    register(client, meter, frm, to, "LINE-A")
    seg = client.get(f"/ownership/{meter}").json()[0]
    # +02:00 input is normalized to UTC (serialized with the Z suffix)
    assert seg["valid_from"] == "2026-01-01T00:00:00Z"
    assert seg["valid_to"] == "2026-01-01T12:00:00Z"
