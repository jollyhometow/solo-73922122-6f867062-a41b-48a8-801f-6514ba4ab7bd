"""Core transactional services.

Every public function takes a psycopg connection and manages the transaction
itself: commit on success, rollback on any error. Row-level locks on the meter
row serialise concurrent changes of the same meter across both API instances,
because both instances talk to the same PostgreSQL database.
"""
from __future__ import annotations

import math
from datetime import datetime

from psycopg import Connection
from psycopg.errors import CheckViolation, ExclusionViolation, UniqueViolation

from .config import settings
from .errors import ConflictError, NotFoundError, UnprocessableError


def _lock_meter(cur, meter_no: str) -> int:
    """Lock the meter row and return its current version.

    Concurrent registrations/corrections for the same meter queue here, so the
    exclusion constraint only ever sees a consistent segment timeline.
    """
    cur.execute(
        "SELECT version FROM meter WHERE meter_no = %s FOR UPDATE",
        (meter_no,),
    )
    row = cur.fetchone()
    if row is None:
        raise NotFoundError(f"meter '{meter_no}' is not registered")
    return row[0]


def _ensure_meter(cur, meter_no: str) -> None:
    cur.execute(
        "INSERT INTO meter (meter_no) VALUES (%s) ON CONFLICT DO NOTHING",
        (meter_no,),
    )


def register_ownership(
    conn: Connection,
    meter_no: str,
    valid_from: datetime,
    valid_to: datetime,
    line: str,
) -> int:
    """Register one non-overlapping attribution segment for a meter."""
    if valid_from >= valid_to:
        raise UnprocessableError("valid_from must be strictly before valid_to")
    try:
        with conn.transaction():
            cur = conn.cursor()
            _ensure_meter(cur, meter_no)
            version = _lock_meter(cur, meter_no)
            cur.execute(
                """
                INSERT INTO ownership
                    (meter_no, valid_from, valid_to, line, version)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id, version
                """,
                (meter_no, valid_from, valid_to, line, version),
            )
            ownership_id, _ = cur.fetchone()
            cur.execute(
                """
                INSERT INTO ownership_history
                    (meter_no, ownership_id, valid_from, valid_to,
                     old_line, new_line, action, meter_version)
                VALUES (%s, %s, %s, %s, NULL, %s, 'INSERT', %s)
                """,
                (meter_no, ownership_id, valid_from, valid_to, line, version),
            )
            cur.execute(
                "UPDATE meter SET version = version + 1, updated_at = now() "
                "WHERE meter_no = %s",
                (meter_no,),
            )
        return version
    except ExclusionViolation:
        # Roll back the whole transaction explicitly so the pooled connection
        # is returned clean and the meter version is untouched.
        conn.rollback()
        raise ConflictError(
            f"segment [{valid_from.isoformat()}, "
            f"{valid_to.isoformat()}) overlaps an existing "
            "attribution segment for this meter"
        )
    except CheckViolation:
        conn.rollback()
        raise UnprocessableError("valid_from must be strictly before valid_to")


def ingest_readings(conn: Connection, readings: list[dict]) -> list[dict]:
    """Bind each reading to the current ownership covering its timestamp.

    Rejects the whole batch if any reading lacks coverage or uses a duplicated
    reading_uid, so a partially ingested batch can never be observed.
    """
    try:
        with conn.transaction():
            cur = conn.cursor()
            out: list[dict] = []
            # Lock touched meters so a correction cannot rewrite a segment
            # between the coverage lookup and the insert. Corrections take the
            # same FOR UPDATE lock, so ingestion and correction serialize.
            meter_nos = sorted({r["meter_no"] for r in readings})
            cur.execute(
                "SELECT meter_no FROM meter WHERE meter_no = ANY(%s) FOR UPDATE",
                (meter_nos,),
            )
            found = {r[0] for r in cur.fetchall()}
            missing = sorted(set(meter_nos) - found)
            if missing:
                raise NotFoundError(
                    "unregistered meter(s): " + ", ".join(missing)
                )
            for r in readings:
                cur.execute(
                    """
                    SELECT o.id, o.line, o.valid_from, o.valid_to
                    FROM ownership o
                    WHERE o.meter_no = %s
                      AND o.valid_from <= %s
                      AND o.valid_to   >  %s
                    """,
                    (r["meter_no"], r["ts"], r["ts"]),
                )
                seg = cur.fetchone()
                if seg is None:
                    raise UnprocessableError(
                        f"reading '{r['reading_uid']}' at {r['ts'].isoformat()} "
                        "has no ownership segment covering its timestamp; rejected"
                    )
                ownership_id, line, seg_from, seg_to = seg
                try:
                    cur.execute(
                        """
                        INSERT INTO reading
                            (reading_uid, meter_no, ts, wh, ownership_id)
                        VALUES (%s, %s, %s, %s, %s)
                        RETURNING id
                        """,
                        (
                            r["reading_uid"],
                            r["meter_no"],
                            r["ts"],
                            r["wh"],
                            ownership_id,
                        ),
                    )
                except UniqueViolation:
                    raise ConflictError(
                        "reading_uid must be unique; duplicate rejected"
                    )
                out.append(
                    {
                        "reading_uid": r["reading_uid"],
                        "meter_no": r["meter_no"],
                        "ts": r["ts"],
                        "wh": r["wh"],
                        "ownership_id": ownership_id,
                        "line": line,
                        "valid_from": seg_from,
                        "valid_to": seg_to,
                    }
                )
        return out
    except (ConflictError, UnprocessableError, NotFoundError):
        raise


def correct_ownership(
    conn: Connection,
    meter_no: str,
    valid_from: datetime,
    valid_to: datetime,
    new_line: str,
    base_version: int,
) -> int:
    """Replace the attribution line on [valid_from, valid_to) only.

    * Optimistic concurrency: fails with 409 when base_version is stale.
    * Only that window is replaced; segments (and segment parts) outside it
      keep their identity and history.
    * Readings already in the warehouse are re-pointed to the replacement
      segment(s) in the same transaction, so they stay covered.
    """
    if valid_from >= valid_to:
        raise UnprocessableError("valid_from must be strictly before valid_to")
    try:
        with conn.transaction():
            cur = conn.cursor()
            # Defer the reading -> ownership FK for this transaction so an old
            # segment can be deleted before its replacement is inserted.
            cur.execute("SET CONSTRAINTS ALL DEFERRED")
            version = _lock_meter(cur, meter_no)
            if version != base_version:
                raise ConflictError(
                    f"stale correction: meter version is {version}, "
                    f"request was based on {base_version}"
                )

            # All segments overlapping the correction window.
            cur.execute(
                """
                SELECT id, valid_from, valid_to, line
                FROM ownership
                WHERE meter_no = %s
                  AND valid_from < %s
                  AND valid_to   > %s
                ORDER BY valid_from
                FOR UPDATE
                """,
                (meter_no, valid_to, valid_from),
            )
            overlapping = cur.fetchall()
            if not overlapping:
                raise UnprocessableError(
                    "correction window does not overlap any existing "
                    "attribution segment; nothing to correct"
                )

            # Verify the window is covered by a continuous, gap-free union of
            # segments (left-closed/right-open). Walk the overlapping segments
            # in time order and merge them; the merged span must equal the
            # requested window. A mere SUM of durations cannot detect an
            # internal gap, hence the explicit merge walk.
            cursor_pos = valid_from
            for _seg_id, seg_from, seg_to, _line in overlapping:
                if seg_from > cursor_pos:
                    raise UnprocessableError(
                        "correction window contains time without any ownership "
                        "segment; cannot replace a gap"
                    )
                if seg_to > cursor_pos:
                    cursor_pos = seg_to
                if cursor_pos >= valid_to:
                    break
            if cursor_pos < valid_to:
                raise UnprocessableError(
                    "correction window contains time without any ownership "
                    "segment; cannot replace a gap"
                )

            new_version = version + 1

            # Phase 1: retire every overlapping old segment. The deferred FK
            # lets readings keep pointing at the (now deleted) ids transiently.
            for seg_id, seg_from, seg_to, old_line in overlapping:
                cur.execute(
                    "DELETE FROM ownership WHERE id = %s", (seg_id,)
                )
                action = "REPLACE"
                if seg_from < valid_from or seg_to > valid_to:
                    action = "SPLIT"
                cur.execute(
                    """
                    INSERT INTO ownership_history
                        (meter_no, ownership_id, valid_from, valid_to,
                         old_line, new_line, action, meter_version)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (meter_no, seg_id, seg_from, seg_to, old_line, new_line,
                     action, new_version),
                )

            # Phase 2a: re-create the parts of each old segment that fall
            # outside the correction window (left and right side history).
            for _seg_id, seg_from, seg_to, old_line in overlapping:
                left = max(seg_from, valid_from)
                right = min(seg_to, valid_to)
                if seg_from < left:
                    cur.execute(
                        """
                        INSERT INTO ownership
                            (meter_no, valid_from, valid_to, line, version)
                        VALUES (%s, %s, %s, %s, %s) RETURNING id
                        """,
                        (meter_no, seg_from, left, old_line, new_version),
                    )
                    keep_left = cur.fetchone()[0]
                    cur.execute(
                        "UPDATE reading SET ownership_id = %s "
                        "WHERE meter_no = %s AND ts >= %s AND ts < %s",
                        (keep_left, meter_no, seg_from, left),
                    )
                if right < seg_to:
                    cur.execute(
                        """
                        INSERT INTO ownership
                            (meter_no, valid_from, valid_to, line, version)
                        VALUES (%s, %s, %s, %s, %s) RETURNING id
                        """,
                        (meter_no, right, seg_to, old_line, new_version),
                    )
                    keep_right = cur.fetchone()[0]
                    cur.execute(
                        "UPDATE reading SET ownership_id = %s "
                        "WHERE meter_no = %s AND ts >= %s AND ts < %s",
                        (keep_right, meter_no, right, seg_to),
                    )

            # Phase 2b: one replacement segment covering the whole corrected
            # window. The gap-free check above guarantees the window was fully
            # owned before, so this single segment plus the kept sides form a
            # continuous, non-overlapping timeline.
            cur.execute(
                """
                INSERT INTO ownership
                    (meter_no, valid_from, valid_to, line, version)
                VALUES (%s, %s, %s, %s, %s) RETURNING id
                """,
                (meter_no, valid_from, valid_to, new_line, new_version),
            )
            replacement_id = cur.fetchone()[0]

            # Phase 3: every reading whose timestamp falls in the corrected
            # window is re-pointed to the single replacement segment. Readings
            # on the kept left/right sides were already re-pointed in phase 2a.
            cur.execute(
                """
                UPDATE reading SET ownership_id = %s
                WHERE meter_no = %s AND ts >= %s AND ts < %s
                """,
                (replacement_id, meter_no, valid_from, valid_to),
            )

            # Safety net: no reading may be left pointing at a deleted segment.
            cur.execute(
                """
                SELECT count(*) FROM reading r
                WHERE r.meter_no = %s
                  AND NOT EXISTS (SELECT 1 FROM ownership o
                                  WHERE o.id = r.ownership_id)
                """,
                (meter_no,),
            )
            if cur.fetchone()[0] != 0:
                # Defensive: should never happen; force a rollback rather than
                # commit orphaned facts.
                raise ConflictError(
                    "correction would orphan readings; rolled back"
                )

            cur.execute(
                "UPDATE meter SET version = version + 1, updated_at = now() "
                "WHERE meter_no = %s",
                (meter_no,),
            )
        return new_version
    except ExclusionViolation:
        # The meter lock normally prevents this; keep a clear error anyway.
        conn.rollback()
        raise ConflictError("concurrent attribution change detected; retry")
    except CheckViolation:
        conn.rollback()
        raise UnprocessableError("valid_from must be strictly before valid_to")


def list_ownership(conn: Connection, meter_no: str) -> list[dict]:
    with conn.transaction():
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM meter WHERE meter_no = %s", (meter_no,)
        )
        if cur.fetchone() is None:
            raise NotFoundError(f"meter '{meter_no}' is not registered")
        cur.execute(
            """
            SELECT id, meter_no, valid_from, valid_to, line, version
            FROM ownership WHERE meter_no = %s ORDER BY valid_from
            """,
            (meter_no,),
        )
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_meter(conn: Connection, meter_no: str) -> dict:
    with conn.transaction():
        cur = conn.cursor()
        cur.execute(
            "SELECT meter_no, version, updated_at FROM meter WHERE meter_no = %s",
            (meter_no,),
        )
        row = cur.fetchone()
        if row is None:
            raise NotFoundError(f"meter '{meter_no}' is not registered")
        return {"meter_no": row[0], "version": row[1], "updated_at": row[2]}


def query_readings_by_line(
    conn: Connection,
    line: str,
    start: datetime | None,
    end: datetime | None,
    limit: int,
) -> list[dict]:
    """Readings currently attributed to a line.

    Only committed rows are visible (READ COMMITTED); a failed correction or
    ingestion rolls back entirely, so partial states can never be observed.
    """
    with conn.transaction():
        cur = conn.cursor()
        sql = [
            "SELECT r.reading_uid, r.meter_no, r.ts, r.wh, r.ownership_id,",
            "       o.line, o.valid_from, o.valid_to",
            "FROM reading r JOIN ownership o ON o.id = r.ownership_id",
            "WHERE o.line = %s",
        ]
        params: list = [line]
        if start is not None:
            sql.append("AND r.ts >= %s")
            params.append(start)
        if end is not None:
            sql.append("AND r.ts <  %s")
            params.append(end)
        sql.append("ORDER BY r.ts, r.id LIMIT %s")
        params.append(limit)
        cur.execute(" ".join(sql), params)
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# Coverage audit
# ---------------------------------------------------------------------------
#
# The audit answers: "did every meter of this line produce a reading at every
# scheduled instant?" Expected instants are generated from the window start
# (start, start+step, ... strictly before end) and clipped to each meter's
# current ownership segments on the audited line: a meter is only expected to
# sample while it belongs to that line.
#
# All audit statements share one CTE stack and run inside a single
# REPEATABLE READ transaction, so every row they read comes from the same
# database snapshot. A correction or batch ingestion committing concurrently
# is either fully visible or fully invisible to the whole audit -- never a
# mix of pre- and post-commit state. (A read-only REPEATABLE READ transaction
# cannot hit serialization failures, so no retry loop is needed.)

_AUDIT_CTES = """
WITH grid AS (
    -- Scheduled instants anchored at the window start, right-open window.
    SELECT gs AS ts
    FROM generate_series(
        %(start)s::timestamptz,
        %(end)s::timestamptz,
        %(step)s * interval '1 second'
    ) AS gs
    WHERE gs < %(end)s::timestamptz
),
expected AS (
    -- Scheduled instants falling inside a current ownership segment of the
    -- audited line. Live segments of one meter never overlap (GiST exclusion
    -- constraint), so each instant matches at most one segment.
    SELECT o.meter_no, g.ts
    FROM ownership o
    JOIN grid g
      ON g.ts >= o.valid_from AND g.ts < o.valid_to
    WHERE o.line = %(line)s
      AND o.valid_from < %(end)s
      AND o.valid_to >  %(start)s
),
actual AS (
    -- Readings currently attributed to the audited line, grouped per
    -- (meter, ts) so duplicate readings at the same instant stay visible.
    SELECT r.meter_no, r.ts, count(*) AS n
    FROM reading r
    JOIN ownership o ON o.id = r.ownership_id
    WHERE o.line = %(line)s
      AND r.ts >= %(start)s
      AND r.ts <  %(end)s
    GROUP BY r.meter_no, r.ts
)
"""

_SQL_AUDIT_SUMMARY = _AUDIT_CTES + """
, exp_agg AS (
    SELECT meter_no, count(*) AS expected FROM expected GROUP BY meter_no
),
act_agg AS (
    SELECT meter_no, sum(n) AS present FROM actual GROUP BY meter_no
),
miss_agg AS (
    SELECT e.meter_no, count(*) AS missing
    FROM expected e
    LEFT JOIN actual a ON a.meter_no = e.meter_no AND a.ts = e.ts
    WHERE a.ts IS NULL
    GROUP BY e.meter_no
)
SELECT m.meter_no,
       coalesce(ea.expected, 0) AS expected,
       coalesce(aa.present,  0) AS present,
       coalesce(ma.missing,  0) AS missing
FROM (SELECT meter_no FROM exp_agg
      UNION
      SELECT meter_no FROM act_agg) m
LEFT JOIN exp_agg  ea USING (meter_no)
LEFT JOIN act_agg  aa USING (meter_no)
LEFT JOIN miss_agg ma USING (meter_no)
ORDER BY m.meter_no
"""

_SQL_AUDIT_MISSING = _AUDIT_CTES + """
, missing AS (
    SELECT e.meter_no, e.ts
    FROM expected e
    LEFT JOIN actual a ON a.meter_no = e.meter_no AND a.ts = e.ts
    WHERE a.ts IS NULL
),
islands AS (
    -- Gaps-and-islands: instants on a uniform grid share
    -- ts - row_number()*step within each run of consecutive misses.
    SELECT meter_no, ts,
           ts - row_number() OVER (PARTITION BY meter_no ORDER BY ts)
                  * (%(step)s * interval '1 second') AS grp
    FROM missing
)
SELECT meter_no,
       min(ts)                                        AS interval_from,
       max(ts) + %(step)s * interval '1 second'       AS interval_to,
       count(*)                                       AS missing_count
FROM islands
GROUP BY meter_no, grp
ORDER BY meter_no, interval_from
"""

_SQL_AUDIT_DUPLICATES = _AUDIT_CTES + """
SELECT meter_no, ts, n AS cnt
FROM actual
WHERE n > 1
ORDER BY meter_no, ts
"""


def audit_coverage(
    conn: Connection,
    line: str,
    start: datetime,
    end: datetime,
    interval_seconds: int,
) -> dict:
    """Audit scheduled-sampling coverage for one production line.

    Returns per-meter expected/present/missing counts, the missing instants
    merged into [from, to) intervals, and duplicate (meter, ts) readings.
    Everything is read from one REPEATABLE READ snapshot (see module notes
    above), so the report always reflects exactly one committed state of the
    warehouse even while corrections and ingestions commit concurrently.
    """
    if start >= end:
        raise UnprocessableError("start must be strictly before end")
    if interval_seconds <= 0:
        raise UnprocessableError("interval_seconds must be a positive integer")
    grid_points = math.ceil(
        (end - start).total_seconds() / interval_seconds
    )
    if grid_points > settings.audit_max_grid_points:
        raise UnprocessableError(
            f"audit would generate {grid_points} scheduled instants per "
            f"meter; the limit is {settings.audit_max_grid_points} - "
            "narrow the window or increase interval_seconds"
        )
    params = {"line": line, "start": start, "end": end,
              "step": interval_seconds}
    with conn.transaction():
        cur = conn.cursor()
        # Must be the first statement of the transaction: pins every
        # subsequent statement to one consistent snapshot.
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")

        cur.execute(_SQL_AUDIT_SUMMARY, params)
        meters = [
            {"meter_no": r[0], "expected": r[1], "present": r[2],
             "missing": r[3]}
            for r in cur.fetchall()
        ]

        cur.execute(_SQL_AUDIT_MISSING, params)
        missing_intervals = [
            {"meter_no": r[0], "interval_from": r[1], "interval_to": r[2],
             "missing_count": r[3]}
            for r in cur.fetchall()
        ]

        cur.execute(_SQL_AUDIT_DUPLICATES, params)
        duplicates = [
            {"meter_no": r[0], "ts": r[1], "count": r[2]}
            for r in cur.fetchall()
        ]

    return {
        "line": line,
        "window_start": start,
        "window_end": end,
        "interval_seconds": interval_seconds,
        "expected_total": sum(m["expected"] for m in meters),
        "missing_total": sum(m["missing"] for m in meters),
        "meters": meters,
        "missing_intervals": missing_intervals,
        "duplicates": duplicates,
    }
