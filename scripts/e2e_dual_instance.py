#!/usr/bin/env python3
"""Two API instances + one database: concurrent correction race over real HTTP.

Fires two corrections for the same meter, based on the same observed version,
at two different uvicorn ports concurrently. Exactly one must commit (200);
the other must receive a 409 stale-version conflict, and the warehoused
reading must end up re-linked to the winning line with full coverage.
"""
from __future__ import annotations

import os
import sys
import threading
import uuid

import httpx

A = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8011"
B = sys.argv[2] if len(sys.argv) > 2 else "http://127.0.0.1:8012"
METER = "M-DUAL-" + uuid.uuid4().hex[:10]
WIN = ("2026-02-01T02:00:00Z", "2026-02-01T08:00:00Z")


def main() -> int:
    with httpx.Client(timeout=20) as http:
        r = http.post(f"{A}/ownership/register", json={
            "meter_no": METER,
            "valid_from": "2026-02-01T00:00:00Z",
            "valid_to": "2026-02-02T00:00:00Z",
            "line": "LINE-A",
        })
        r.raise_for_status()
        http.post(f"{A}/readings/ingest", json={"readings": [{
            "reading_uid": "DUAL-" + uuid.uuid4().hex[:10],
            "meter_no": METER,
            "ts": "2026-02-01T05:00:00Z", "wh": 77}]}).raise_for_status()

        version = http.get(f"{A}/meters/{METER}").json()["version"]
        print("base version observed by both instances:", version)

        results: list[tuple[str, int, dict]] = []
        barrier = threading.Barrier(2)

        def race(base: str, line: str, tag: str) -> None:
            barrier.wait()
            resp = http.post(f"{base}/ownership/correct", json={
                "meter_no": METER,
                "valid_from": WIN[0], "valid_to": WIN[1],
                "new_line": line, "base_version": version,
            })
            results.append((tag, resp.status_code, resp.json()))

        t1 = threading.Thread(target=race, args=(A, "LINE-B", "api1"))
        t2 = threading.Thread(target=race, args=(B, "LINE-C", "api2"))
        t1.start(); t2.start(); t1.join(); t2.join()

    for tag, code, body in sorted(results):
        print(f"{tag}: HTTP {code} {body}")
    codes = sorted(code for _, code, _ in results)
    assert codes == [200, 409], f"expected one 200 and one 409, got {codes}"
    winner = next(body for _, code, body in results if code == 200)
    print("committed new version:", winner["version"])

    with httpx.Client(timeout=20) as http:
        segs = http.get(f"{A}/ownership/{METER}").json()
        print("final segments:")
        for s in segs:
            print("  ", s["line"], s["valid_from"], "->", s["valid_to"])
        lines = {s["line"] for s in segs}
        assert lines <= {"LINE-A", "LINE-B"} or lines <= {"LINE-A", "LINE-C"}

        found = None
        for line in ("LINE-B", "LINE-C"):
            rows = [r for r in http.get(
                f"{A}/readings/by-line/{line}",
                params={"start": "2026-02-01T00:00:00Z",
                        "end": "2026-02-03T00:00:00Z"}).json()
                if r["meter_no"] == METER]
            for r in rows:
                found = r
        assert found is not None, "reading disappeared after the race"
        assert found["valid_from"] <= found["ts"] < found["valid_to"]
        print("reading re-linked to winning line:", found["line"],
              "segment", found["ownership_id"])

    print("DUAL-INSTANCE CONCURRENCY OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
