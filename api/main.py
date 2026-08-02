"""whatsontheriver v2 cloud API — read endpoints + ingest, one deployed
service. Response shapes for /live-positions and /lock-status match the
CURRENT next-vessel-server field-for-field where the data is available
from this schema, so the existing frontend needs zero changes to point
here once cutover happens.

Real pivot 2026-08-02, deploying this for real: the spec called for
ingest -> SQS -> separate writer worker -> DB, but that needs a second
cloud account (AWS) before Reed's collector has anywhere real to POST
to. At our actual measured volume (Reed: ~300-400k events/day, a few
events/sec) a queue is protecting against a problem we don't have yet.
So for now: ingest writes directly to Postgres, synchronously, in the
same request. The queue abstraction (ingest/queue.py) still exists and
this can grow into it the moment durability/backpressure actually
matters -- boring and working now beats correct-on-paper and blocked.

HONEST GAP, not silently faked: the current production API enriches
lock-projected vessels with river-mile lookups, distance-from-receiver,
and channel-snapped path_from_lock polylines -- that's real projection
business logic living in Reed's existing next-vessel-server, not just
stored data. This returns the fields this schema can genuinely answer
and leaves the rest as explicit TODOs rather than fabricating
plausible-looking numbers. Port that logic here before this is a real
drop-in replacement, don't ship it silently missing.
"""
import hashlib
import hmac
import json
import os
import time
from datetime import datetime, timezone

import asyncpg
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr

app = FastAPI(title="whatsontheriver API")

# whatsontheriver.com calls this from the browser (photo submissions,
# eventually the live read endpoints too) -- needs real CORS, not the
# default same-origin-only behavior.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://whatsontheriver.com", "https://www.whatsontheriver.com"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

LOCK_META = {
    # TODO: move to a `locks` reference table once Track C is proven;
    # hardcoded here only to get a working skeleton without another
    # round-trip to Reed for the reference data.
    "01": {"name": "St Paul", "river_mile": 847.6},
    "02": {"name": "Hastings", "river_mile": 815.2},
    "03": {"name": "Red Wing (Welch)", "river_mile": 796.9},
    "04": {"name": "Alma", "river_mile": 752.8},
    "05": {"name": "Minneiska", "river_mile": 738.1},
    "5A": {"name": "Winona", "river_mile": 728.5},
}

FRESH_CUTOFF_S = 300
MAX_CLOCK_SKEW_S = 300  # reject ingest batches signed more than 5 min off

# source_id -> shared secret. Small enough to be an env-var map for now;
# move to a real secrets store once there's more than a couple of sources.
SOURCE_SECRETS: dict[str, str] = {
    k[len("SOURCE_SECRET_"):]: v
    for k, v in os.environ.items()
    if k.startswith("SOURCE_SECRET_")
}


@app.on_event("startup")
async def startup():
    app.state.pool = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=2, max_size=10)


@app.get("/live-positions")
async def live_positions():
    now = datetime.now(timezone.utc)
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT mmsi, name, last_seen, last_lat, last_lon, last_source_id, quality
            FROM vessel_current_state
            ORDER BY last_seen DESC
            """
        )

    vessels = []
    for r in rows:
        age_s = (now - r["last_seen"]).total_seconds()
        vessels.append({
            "mmsi": r["mmsi"],
            "name": r["name"],
            "lat": r["last_lat"],
            "lon": r["last_lon"],
            "last_signal_s": age_s,
            "is_live": age_s < FRESH_CUTOFF_S,
            # TODO (not yet ported from next-vessel-server): speed_mph,
            # cog_deg, heading_deg, shiptype, is_commercial, distance/
            # bearing_from_receiver, river_mile, direction, callsign,
            # destination, draught_m, last_lock/path_from_lock projection.
        })

    return {
        "generated_at": now.isoformat(),
        "fresh_cutoff_s": FRESH_CUTOFF_S,
        "vessels": vessels,
    }


@app.get("/lock-status")
async def lock_status():
    now = datetime.now(timezone.utc)
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT ON (lock_id) lock_id, vessel_name, barges, direction, time
            FROM lock_status_observation
            ORDER BY lock_id, time DESC
            """
        )
    last_cleared_by_lock = {r["lock_id"]: r for r in rows}

    locks = {}
    for lock_id, meta in LOCK_META.items():
        last = last_cleared_by_lock.get(lock_id)
        locks[lock_id] = {
            "name": meta["name"],
            "river_mile": meta["river_mile"],
            "last_cleared": {
                "vessel": last["vessel_name"],
                "barges": last["barges"],
                "min_ago": round((now - last["time"]).total_seconds() / 60),
                "direction": last["direction"],
            } if last else None,
            # TODO: flag/map_badge/conditions_line/live_state/active_lockage
            # -- these are derived presentation logic in the current API,
            # not raw stored fields. Port before cutover.
        }

    return {"generated_at": now.isoformat(), "locks": locks}


# --- Ingest ------------------------------------------------------------

class TelemetryEvent(BaseModel):
    observed_at: str          # ISO8601, from the collector's clock
    event_type: str           # 'ais_position' | 'lock_status'
    schema_version: int = 1
    dedupe_key: str
    payload: dict


class Batch(BaseModel):
    source_id: str
    sent_at: float            # unix epoch, for clock-skew rejection
    events: list[TelemetryEvent]


def verify_signature(source_id: str, body: bytes, signature: str) -> bool:
    secret = SOURCE_SECRETS.get(source_id)
    if not secret:
        return False
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


def _parse_dt(s: str | None) -> datetime | None:
    # Real bug found testing this live: asyncpg's binary protocol needs
    # an actual datetime object for timestamptz columns, not a raw ISO
    # string -- passing the string through unconverted threw a 500 with
    # no useful message client-side. Handles 'Z' suffix (Python 3.11+).
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


async def write_event(conn: asyncpg.Connection, source_id: str, event: TelemetryEvent):
    observed_at = _parse_dt(event.observed_at)
    await conn.execute(
        """
        INSERT INTO telemetry_raw (observed_at, source_id, event_type,
                                    schema_version, dedupe_key, raw_payload)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT DO NOTHING
        """,
        observed_at, source_id, event.event_type,
        event.schema_version, event.dedupe_key, json.dumps(event.payload),
    )

    if event.event_type == "ais_position":
        p = event.payload
        await conn.execute(
            """
            INSERT INTO vessel_position (time, source_id, mmsi, name, lat, lon,
                                          sog_mph, cog_deg, heading_deg, nav_status,
                                          is_commercial, quality)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            """,
            observed_at, source_id, p.get("mmsi"), p.get("name"),
            p["lat"], p["lon"], p.get("sog_mph"), p.get("cog_deg"), p.get("heading_deg"),
            p.get("nav_status"), p.get("is_commercial"), p.get("quality", "live"),
        )
        if p.get("mmsi"):
            await conn.execute(
                """
                INSERT INTO vessel_current_state (mmsi, name, last_seen, last_lat, last_lon,
                                                    last_source_id, quality)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (mmsi) DO UPDATE SET
                    name = EXCLUDED.name, last_seen = EXCLUDED.last_seen,
                    last_lat = EXCLUDED.last_lat, last_lon = EXCLUDED.last_lon,
                    last_source_id = EXCLUDED.last_source_id, quality = EXCLUDED.quality
                WHERE EXCLUDED.last_seen > vessel_current_state.last_seen
                """,
                p["mmsi"], p.get("name"), observed_at, p["lat"], p["lon"],
                source_id, p.get("quality", "live"),
            )

    elif event.event_type == "lock_status":
        p = event.payload
        await conn.execute(
            """
            INSERT INTO lock_status_observation (time, source_id, lock_id, vessel_name,
                                                   direction, barges, sol_at, eol_at, raw_payload)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
            observed_at, source_id, p["lock_id"], p.get("vessel_name"),
            p.get("direction"), p.get("barges"), _parse_dt(p.get("sol_at")),
            _parse_dt(p.get("eol_at")), json.dumps(p),
        )


@app.post("/ingest")
async def ingest(request: Request, x_signature: str = Header(...)):
    body = await request.body()
    batch = Batch.model_validate_json(body)

    if not verify_signature(batch.source_id, body, x_signature):
        raise HTTPException(401, "bad signature")
    if abs(time.time() - batch.sent_at) > MAX_CLOCK_SKEW_S:
        raise HTTPException(400, "clock skew too large -- check collector's clock")

    # ensure the source is registered before writing events that
    # foreign-key against it -- upsert is deliberate, not a bug: a new
    # receiver location should Just Work the first time it POSTs, not
    # need a manual DB row first.
    async with app.state.pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO sources (source_id, type) VALUES ($1, 'unknown') ON CONFLICT DO NOTHING",
            batch.source_id,
        )
        async with conn.transaction():
            for event in batch.events:
                await write_event(conn, batch.source_id, event)

    return {"accepted": len(batch.events)}


# --- Photo submissions ---------------------------------------------------
# Real note on the storage choice: photos are stored as base64 in Postgres,
# not object storage (R2/S3). This is an honest interim call, not an
# oversight -- we don't have R2 access provisioned yet, and at low
# submission volume this is genuinely fine (Neon free tier is 500MB).
# Revisit if/when this becomes a real bottleneck, not before.
MAX_PHOTO_BYTES = 6_000_000  # ~6MB raw, keeps base64-inflated size sane
ALLOWED_MIME = {"image/jpeg", "image/png", "image/webp"}


class PhotoSubmission(BaseModel):
    vessel_name: str
    photographer_name: str
    credit_name: str
    contact_email: EmailStr
    message: str | None = None
    photo_data_b64: str
    photo_mime: str
    agreed: bool


@app.post("/photo-submissions")
async def submit_photo(sub: PhotoSubmission):
    if not sub.agreed:
        raise HTTPException(400, "must agree to the terms to submit")
    if sub.photo_mime not in ALLOWED_MIME:
        raise HTTPException(400, f"photo must be one of {sorted(ALLOWED_MIME)}")
    # base64 is ~4/3 the size of the raw bytes -- check the encoded length
    # directly rather than decoding first, cheaper and just as accurate
    # for a size gate.
    if len(sub.photo_data_b64) > MAX_PHOTO_BYTES * 4 // 3:
        raise HTTPException(400, "photo too large -- please keep it under 6MB")
    if not sub.vessel_name.strip() or not sub.photographer_name.strip() or not sub.credit_name.strip():
        raise HTTPException(400, "vessel name, your name, and credit name are all required")

    async with app.state.pool.acquire() as conn:
        row_id = await conn.fetchval(
            """
            INSERT INTO photo_submissions (vessel_name, photographer_name, credit_name,
                                            contact_email, message, photo_data_b64, photo_mime, agreed_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, now())
            RETURNING id
            """,
            sub.vessel_name.strip(), sub.photographer_name.strip(), sub.credit_name.strip(),
            sub.contact_email, sub.message, sub.photo_data_b64, sub.photo_mime,
        )

    return {"submitted": True, "id": row_id}


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
