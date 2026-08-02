-- whatsontheriver v2 cloud-side schema (TimescaleDB / Postgres)
-- Matches the spec sent to Reed 2026-08-02: one database for both
-- telemetry and relational lock data, hypertables for time-series growth,
-- retention + continuous aggregates instead of hand-rolled cleanup jobs.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Registry of every physical/logical ingestion source (antenna, LPMS
-- scraper, future receiver sites). Adding a new receiver location is a
-- row here plus a new collector deploy — no backend code change.
CREATE TABLE sources (
    source_id   TEXT PRIMARY KEY,
    type        TEXT NOT NULL,       -- 'ais_receiver' | 'lpms_scraper' | ...
    location    TEXT,
    owner       TEXT,
    status      TEXT NOT NULL DEFAULT 'active',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Raw immutable event log. Every ingested payload lands here first,
-- unmodified, before any normalization — this is the forensic/replay
-- layer and the thing retention policies trim, not the app's read path.
CREATE TABLE telemetry_raw (
    received_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    observed_at   TIMESTAMPTZ NOT NULL,
    source_id     TEXT NOT NULL REFERENCES sources(source_id),
    event_type    TEXT NOT NULL,     -- 'ais_position' | 'lock_status' | ...
    schema_version SMALLINT NOT NULL DEFAULT 1,
    dedupe_key    TEXT NOT NULL,
    raw_payload   JSONB NOT NULL
);
SELECT create_hypertable('telemetry_raw', 'received_at');
CREATE UNIQUE INDEX telemetry_raw_dedupe_idx ON telemetry_raw (source_id, dedupe_key, received_at);

-- Normalized vessel positions — what the map/replay/ETA logic actually reads.
CREATE TABLE vessel_position (
    time        TIMESTAMPTZ NOT NULL,
    source_id   TEXT NOT NULL REFERENCES sources(source_id),
    mmsi        BIGINT,
    name        TEXT,
    lat         DOUBLE PRECISION NOT NULL,
    lon         DOUBLE PRECISION NOT NULL,
    sog_mph     DOUBLE PRECISION,
    cog_deg     DOUBLE PRECISION,
    heading_deg DOUBLE PRECISION,
    nav_status  TEXT,
    is_commercial BOOLEAN,
    quality     TEXT NOT NULL DEFAULT 'live'  -- 'live' | 'estimated' | 'projected'
);
SELECT create_hypertable('vessel_position', 'time');
CREATE INDEX vessel_position_mmsi_time_idx ON vessel_position (mmsi, time DESC);

-- Normalized lock clearance/status observations (from LPMS scraping).
CREATE TABLE lock_status_observation (
    time        TIMESTAMPTZ NOT NULL,
    source_id   TEXT NOT NULL REFERENCES sources(source_id),
    lock_id     TEXT NOT NULL,
    vessel_name TEXT,
    direction   TEXT,               -- 'U' | 'D'
    barges      INTEGER,
    sol_at      TIMESTAMPTZ,        -- start of lockage
    eol_at      TIMESTAMPTZ,        -- end of lockage
    raw_payload JSONB
);
SELECT create_hypertable('lock_status_observation', 'time');
CREATE INDEX lock_status_lock_time_idx ON lock_status_observation (lock_id, time DESC);

-- Tiny, continuously-updated table — "where is every vessel right now."
-- This is what the live map actually queries; never scans the hypertables.
CREATE TABLE vessel_current_state (
    mmsi            BIGINT PRIMARY KEY,
    name            TEXT,
    last_seen       TIMESTAMPTZ NOT NULL,
    last_lat        DOUBLE PRECISION,
    last_lon        DOUBLE PRECISION,
    last_source_id  TEXT REFERENCES sources(source_id),
    quality         TEXT NOT NULL
);

-- Derived ETA predictions, versioned by model so we can compare
-- (e.g. against the empirical-leg-time model discussed 2026-07-31).
CREATE TABLE derived_eta (
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    mmsi            BIGINT,
    target_lock_id  TEXT NOT NULL,
    eta             TIMESTAMPTZ,
    confidence      TEXT,
    model_version   TEXT NOT NULL
);
SELECT create_hypertable('derived_eta', 'computed_at');

-- Real constraint found applying this schema 2026-08-02: Neon's
-- TimescaleDB extension is the Apache-2.0 build only -- hypertables
-- (create_hypertable, above) work fine, but retention POLICIES,
-- native COMPRESSION, and CONTINUOUS aggregates are all gated behind
-- the commercial "timescale" license (Timescale Cloud only), not
-- available here. Confirmed by testing directly against this database,
-- not assumed from docs. At our real current volume (~300-400k
-- events/day per Reed's measurement) this isn't a functional blocker --
-- it just means retention/rollups are a manual/cron job instead of a
-- built-in policy for now. Revisit if we ever move to Timescale Cloud
-- itself, or if raw-table size becomes a real problem before then.

-- Retention: no automatic policy available -- run a scheduled DELETE
-- instead (cron hitting a small maintenance endpoint, or pg_cron if
-- Neon ever supports it). Not wired up yet; raw data will just
-- accumulate until this is added -- fine short-term at this volume,
-- revisit before it isn't.

-- Heatmap: plain (non-materialized) view instead of a continuous
-- aggregate. At current volume this computes fast enough to just query
-- live -- swap to a materialized view + manual REFRESH on a schedule
-- if lock_status_observation grows large enough that this gets slow.
CREATE VIEW heatmap_1h AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    lock_id,
    count(*) AS lockage_count,
    avg(barges) AS avg_barges
FROM lock_status_observation
GROUP BY bucket, lock_id;
