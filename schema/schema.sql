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

-- Retention: raw stays 90 days (forensic window), typed positions
-- compressed after 30 days, kept 1 year. Aggregates (below) live forever.
SELECT add_retention_policy('telemetry_raw', INTERVAL '90 days');
SELECT add_retention_policy('vessel_position', INTERVAL '365 days');
ALTER TABLE vessel_position SET (timescaledb.compress, timescaledb.compress_segmentby = 'mmsi');
SELECT add_compression_policy('vessel_position', INTERVAL '30 days');

-- Continuous aggregate powering the traffic-heatmap endpoint — replaces
-- the daily cron job that builds this today.
CREATE MATERIALIZED VIEW heatmap_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    lock_id,
    count(*) AS lockage_count,
    avg(barges) AS avg_barges
FROM lock_status_observation
GROUP BY bucket, lock_id;

SELECT add_continuous_aggregate_policy('heatmap_1h',
    start_offset => INTERVAL '3 days',
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
