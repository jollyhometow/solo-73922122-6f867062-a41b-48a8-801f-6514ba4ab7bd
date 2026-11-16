-- Meter-to-line attribution schema.
-- All timestamps are TIMESTAMPTZ; the API only accepts timezone-aware (UTC) values.
-- Conventions: intervals are left-closed / right-open [valid_from, valid_to).

CREATE EXTENSION IF NOT EXISTS btree_gist;

-- One row per physical meter. `version` is the optimistic-concurrency token
-- for the whole attribution timeline of that meter. Every successful
-- registration or correction bumps it.
CREATE TABLE IF NOT EXISTS meter (
    meter_no   TEXT PRIMARY KEY,
    version    BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Current (live) attribution segments only. Superseded segments are moved to
-- ownership_history, so the GiST exclusion constraint can enforce that live
-- segments of one meter never overlap.
CREATE TABLE IF NOT EXISTS ownership (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    meter_no   TEXT NOT NULL REFERENCES meter(meter_no) ON DELETE CASCADE,
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to   TIMESTAMPTZ NOT NULL,
    line       TEXT NOT NULL,
    version    BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ownership_range_ok CHECK (valid_from < valid_to),
    CONSTRAINT ownership_no_overlap EXCLUDE USING gist (
        meter_no WITH =,
        tstzrange(valid_from, valid_to, '[)') WITH &&
    )
);

CREATE INDEX IF NOT EXISTS idx_ownership_meter_time
    ON ownership (meter_no, valid_from, valid_to);

-- Immutable audit trail of every attribution change.
CREATE TABLE IF NOT EXISTS ownership_history (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    meter_no     TEXT NOT NULL,
    ownership_id BIGINT,
    valid_from   TIMESTAMPTZ NOT NULL,
    valid_to     TIMESTAMPTZ NOT NULL,
    old_line     TEXT,
    new_line     TEXT,
    action       TEXT NOT NULL CHECK (action IN ('INSERT', 'REPLACE', 'SPLIT', 'DELETE')),
    meter_version BIGINT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ownership_history_meter
    ON ownership_history (meter_no, created_at);

-- Fact table. Every reading is hard-bound to the ownership segment that
-- covered its timestamp at ingestion time (FK, NOT NULL). The FK is
-- DEFERRABLE INITIALLY DEFERRED so a correction can delete a superseded
-- segment and insert its replacement(s) inside one transaction; the
-- reading -> ownership link is re-pointed before commit, when the check
-- actually runs. Corrections re-point ownership_id in the same transaction.
CREATE TABLE IF NOT EXISTS reading (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    reading_uid  TEXT NOT NULL UNIQUE,
    meter_no     TEXT NOT NULL,
    ts           TIMESTAMPTZ NOT NULL,
    wh           INTEGER NOT NULL,
    ownership_id BIGINT NOT NULL
        REFERENCES ownership(id) DEFERRABLE INITIALLY DEFERRED,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_reading_meter_ts ON reading (meter_no, ts);
CREATE INDEX IF NOT EXISTS idx_reading_ownership ON reading (ownership_id);
