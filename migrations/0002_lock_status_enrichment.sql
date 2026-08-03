=== migrations/0002_lock_status_enrichment.sql ===
-- Incremental migration for the already-live Neon DB -- additive only,
-- safe to run against the real data currently flowing (IF NOT EXISTS /
-- ADD COLUMN IF NOT EXISTS everywhere, nothing here drops or rewrites
-- existing rows). Matches the schema.sql changes made 2026-08-02 for the
-- /live-positions + /lock-status field-parity port.

ALTER TABLE vessel_current_state ADD COLUMN IF NOT EXISTS sog_mph DOUBLE PRECISION;
ALTER TABLE vessel_current_state ADD COLUMN IF NOT EXISTS cog_deg DOUBLE PRECISION;
ALTER TABLE vessel_current_state ADD COLUMN IF NOT EXISTS heading_deg DOUBLE PRECISION;
ALTER TABLE vessel_current_state ADD COLUMN IF NOT EXISTS nav_status TEXT;
ALTER TABLE vessel_current_state ADD COLUMN IF NOT EXISTS shiptype INTEGER;
ALTER TABLE vessel_current_state ADD COLUMN IF NOT EXISTS is_commercial BOOLEAN;
ALTER TABLE vessel_current_state ADD COLUMN IF NOT EXISTS callsign TEXT;
ALTER TABLE vessel_current_state ADD COLUMN IF NOT EXISTS destination TEXT;
ALTER TABLE vessel_current_state ADD COLUMN IF NOT EXISTS draught DOUBLE PRECISION;

CREATE TABLE IF NOT EXISTS lock_live_state (
    lock_id         TEXT PRIMARY KEY,
    pending         INTEGER NOT NULL DEFAULT 0,
    locking         INTEGER NOT NULL DEFAULT 0,
    delay24h_min    INTEGER NOT NULL DEFAULT 0,
    throughput_24h  INTEGER NOT NULL DEFAULT 0,
    last_seen       TIMESTAMPTZ NOT NULL
);
