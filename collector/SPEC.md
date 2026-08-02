# Cabin collector — contract, not code

This is Reed's piece (touches the existing AIS-catcher process and LPMS
scraper directly) — this file is just the contract it needs to satisfy
to talk to `ingest/main.py`.

## What it does

1. Reads from the existing sources exactly as today: AIS-catcher (HTTP,
   already has a timeout — per Reed, this was never the actual bug) and
   the LPMS scraper.
2. Normalizes each observation into a `TelemetryEvent`:
   ```json
   {
     "observed_at": "2026-08-02T18:32:00Z",
     "event_type": "ais_position",
     "schema_version": 1,
     "dedupe_key": "<mmsi>-<observed_at>",
     "payload": { "mmsi": ..., "lat": ..., "lon": ..., "sog_mph": ..., ... }
   }
   ```
3. Batches events (every few seconds, not one HTTP call per event) and
   POSTs to `ingest/main.py`'s `/ingest` with:
   - Body: `{"source_id": "...", "sent_at": <unix ts>, "events": [...]}`
   - Header `X-Signature`: HMAC-SHA256 of the raw body, using the shared
     secret for that `source_id`.
4. **Local spool on failure.** If `/ingest` is unreachable (cloud outage,
   WAN blip), write the batch to a local SQLite queue and retry with
   backoff. This is what makes the new architecture actually resilient
   to cabin-side network issues — the collector is disposable/restartable,
   never blocks on the cloud being up.
5. Heartbeat: send a small `event_type: "collector_heartbeat"` event
   periodically so the read API can honestly report "source X hasn't
   reported in N minutes" instead of silently going stale.

## What it explicitly does NOT do

- Never serves HTTP to the public. No inbound listener needed at all
  beyond whatever AIS-catcher itself already runs locally.
- Never blocks the local AIS-catcher poll loop on the cloud upload —
  those are two separate concerns; a slow/down cloud endpoint should
  never back up local ingestion (this is the exact class of coupling
  bug that caused the original incident, don't reintroduce it here).

## Open question for Reed

Given his actual measured event rate (asked, not yet answered as of
2026-08-02), batch size/interval should be tuned rather than guessed —
noted in the main README.
