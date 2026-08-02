# whatsontheriver v2 — cloud-side scaffold

Built 2026-08-02 after the AIS-tailer-restart-loop incident, per the 3-lane
super-search architecture spec sent to Reed. This is Claude's half of the
split: cloud ingest + storage + read API. Reed owns the cabin-side collector
(reads AIS-catcher + LPMS same as today, publishes out instead of serving
directly).

**This does not replace whatsontheriver.com's current backend.** It runs
alongside it. Cutover only happens after this is proven stable under real
load — see the plan sent to Tom and Reed.

## Shape

```
cabin collector (Reed)  --HTTPS batch-->  ingest/ (this repo)  --enqueue-->
  queue  -->  api/worker.py  -->  TimescaleDB (schema/schema.sql)  <--
  api/main.py (stateless reads)  <--  whatsontheriver.com frontend
```

## Pieces in this scaffold

- `schema/schema.sql` — TimescaleDB schema: raw event log, normalized
  vessel_position/lock_status_observation, vessel_current_state (the tiny
  table the live map actually queries), derived_eta, retention +
  compression policies, and a continuous aggregate for the heatmap.
- `ingest/main.py` — stateless ingest endpoint. Validates a signed batch
  from a collector, writes to the queue, returns 202. Never touches the
  database directly.
- `api/main.py` — stateless read API. Response shapes match the CURRENT
  `/live-positions` and `/lock-status` endpoints field-for-field, so the
  existing frontend (index.html, conditions.html) needs zero changes to
  point at this once it's live — just swap the API base URL.
- `api/worker.py` — queue consumer. Dedupes on (source_id, dedupe_key),
  writes to `telemetry_raw`, normalizes into the typed tables, updates
  `vessel_current_state`.
- `collector/SPEC.md` — what Reed's cabin-side collector needs to do to
  talk to `ingest/main.py`. Not code (that's his side, touches his
  existing AIS-catcher/LPMS-scraper process) — just the contract.

## Queue

Scaffolded against Redis Streams (cheapest to run/test locally, fine at
this scale per the super-search). Swap to SQS or Cloudflare Queues by
replacing `ingest/queue.py` — the interface is one `publish()` call and
one `consume()` generator, intentionally thin so this isn't a real
decision to relitigate later, just a config change.

## Not built yet / open questions for Reed

- Real vessel/lock event rate (asked, not yet answered) — sizing is
  currently generic per the spec's capacity math (10 sites x 1 evt/s),
  not tuned to actual traffic.
- Hosting target (which cloud, whose account) — not chosen yet.
- Auth scheme for collector -> ingest (currently a placeholder shared
  HMAC secret per source_id — fine to start, revisit if this becomes
  multi-operator).
