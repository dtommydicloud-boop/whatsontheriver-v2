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

GAP CLOSED 2026-08-07: /live-positions now includes the dead-reckoned and
lock-projected tiers too (see projections.py), ported from Reed's
live-positions.py against this schema instead of his JSONL/LPMS-disk-cache
data layer. Real edge cases preserved: reception-radius capping, grace-
window overdue flagging, empirical-vs-physics leg-timing fallback,
barge-class-segmented transit medians. This was a dual-run/shadow-compared
port, not a guess -- see the comparator instrument used to verify it against
the original before this replaced the live-only stub.
"""
import hashlib
import hmac
import json
import math
import os
import time
import traceback
from datetime import datetime, timedelta, timezone

import asyncpg
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr

from api import projections
from api import replay as replay_mod
from api import vessel_profile as vessel_profile_mod
from api import vessel_track as vessel_track_mod

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
    # Exact match to lock-status.py's LOCKS/COVERED (Reed's original system) --
    # ported straight across 2026-08-02, ping-tested against the same real
    # USACE lockNumber/lockMile values, not re-guessed.
    "01": {"name": "St Paul", "river_mile": 847.6},
    "02": {"name": "Hastings", "river_mile": 815.2},
    "03": {"name": "Red Wing (Welch)", "river_mile": 796.9},
    "04": {"name": "Alma", "river_mile": 752.8},
    "05": {"name": "Minneiska", "river_mile": 738.1},
    "5A": {"name": "Winona", "river_mile": 728.5},
    "06": {"name": "Trempealeau", "river_mile": 714.1},
    "07": {"name": "La Crescent (Dresbach)", "river_mile": 702.5},
    "08": {"name": "Genoa", "river_mile": 679.2},
    "09": {"name": "Lynxville", "river_mile": 647.9},
    "10": {"name": "Guttenberg", "river_mile": 615.1},
    "11": {"name": "Dubuque", "river_mile": 583.0},
}
COVERED_LOCKS = list(LOCK_META.keys())

# Ported straight from Reed's live-positions.py AIS_SOURCES -- receiver
# coords used for distance/bearing-from-receiver, keyed by source_id so a
# vessel's distance is measured from whichever antenna actually heard it.
RECEIVERS = {
    "lake_mac": {"lat": 44.489, "lon": -92.311, "name": "Lake Mac (Lake City MN)"},
    "red_wing": {"lat": 44.56515, "lon": -92.54908, "name": "Ole Miss Marina (Red Wing MN)"},
}

# Ported from next-vessel.py's 4-anchor CENTERLINE + lat_lon_to_rm -- the
# SIMPLE river-mile system used for the live/real-position tier (distinct
# from Reed's channel-geojson-snapped system used for dead-reckoned/
# lock-projected tiers, which isn't ported here yet -- see /live-positions
# docstring below for the honest scope line on that).
CENTERLINE = [
    (44.7482, -92.8635, 815.2),  # Hastings L2
    (44.6122, -92.6110, 796.9),  # Red Wing L3
    (44.489, -92.311, 774.3),    # Lake Mac cabin (gate anchor)
    (44.319, -92.056, 752.8),    # Alma L4
]

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
    # Real bug found 2026-08-03: this container runs for hours between
    # restarts (that's the point of sleepAfter), but Neon's serverless
    # Postgres suspends its compute after a few minutes of no activity
    # ("scale to zero"). asyncpg does NOT ping-check a pooled connection
    # before handing it to a query (unlike some other Postgres clients) --
    # so after any real gap in traffic, the next request(s) get a
    # connection that looks fine but is actually dead, and the query hangs
    # on it for a long time (client sees 15-20s timeouts, HTTP 000) instead
    # of failing fast. Two real fixes, not a workaround: (1)
    # max_inactive_connection_lifetime proactively recycles idle
    # connections well before they'd go stale from a Neon suspend: (2)
    # command_timeout means even a genuinely stuck query fails fast and
    # frees its pool slot instead of holding it (and eventually the whole
    # pool) hostage forever.
    app.state.pool = await asyncpg.create_pool(
        os.environ["DATABASE_URL"],
        min_size=2,
        max_size=10,
        max_inactive_connection_lifetime=60,
        command_timeout=10,
    )
    async with app.state.pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS email_signups (
                id         BIGSERIAL PRIMARY KEY,
                email      TEXT NOT NULL UNIQUE,
                site       TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        # migrations/0002_lock_status_enrichment.sql, applied idempotently
        # on boot (ADD COLUMN IF NOT EXISTS / CREATE TABLE IF NOT EXISTS
        # throughout) rather than run out-of-band, so a deploy is never
        # blocked on separately reaching the live Neon DB with a migration
        # tool.
        await conn.execute(
            """
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
            """
        )


# --- Geo helpers, ported verbatim from Reed's live-positions.py/next-vessel.py ---

def _hav_mi(lat1, lon1, lat2, lon2):
    r_mi = 3958.7613
    p1, p2 = math.radians(lat1), math.radians(lat2)
    d = (math.sin(math.radians(lat2 - lat1) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2)
    return 2 * r_mi * math.asin(math.sqrt(d))


def _hav_km(a_lat, a_lon, b_lat, b_lon):
    r = 6371.0
    p1, p2 = math.radians(a_lat), math.radians(b_lat)
    x = math.sin(math.radians(b_lat - a_lat) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(b_lon - a_lon) / 2) ** 2
    return 2 * r * math.asin(math.sqrt(x))


def _bearing_deg(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dlon = math.radians(lon2 - lon1)
    x = math.sin(dlon) * math.cos(p2)
    y = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def _dir_from_cog(cog):
    """Half-circle split, not +-45deg buckets -- see live-positions.py's
    _dir_from_cog for the real bug (Neil N. Diehl) this fixed upstream."""
    if cog is None or not isinstance(cog, (int, float)):
        return None
    c = cog % 360
    if c > 270 or c < 90:
        return "upbound"
    if 90 <= c <= 270:
        return "downbound"
    return None


def _lat_lon_to_rm(lat, lon):
    """4-anchor nearest-two-point interpolation -- the SIMPLE river-mile
    system next-vessel.py uses for real (live-tier) positions. NOT the
    channel-geojson-snapped system used for dead-reckoned/lock-projected
    tiers (that one needs the channel polyline data, not ported yet)."""
    scored = sorted(CENTERLINE, key=lambda c: _hav_km(lat, lon, c[0], c[1]))
    a, b = scored[0], scored[1]
    da, db = _hav_km(lat, lon, a[0], a[1]), _hav_km(lat, lon, b[0], b[1])
    total = da + db
    if total == 0:
        return a[2]
    return (a[2] * db + b[2] * da) / total


def _clean_name(n):
    n = (n or "").strip()
    parts = n.rsplit(" ", 1)
    if len(parts) == 2 and parts[1].isdigit():
        n = parts[0]
    return n.title()


@app.get("/live-positions")
async def live_positions():
    """Field-parity pass 2026-08-02: real/live-tier vessels carry the full
    enrichment set the old next-vessel-server returns (speed_mph, cog_deg,
    heading_deg, shiptype, is_commercial, distance/bearing_from_receiver,
    river_mile, direction, callsign, destination, draught_m).

    Dead-reckoned + lock-projected tiers ported 2026-08-07 (see
    projections.py for the real algorithm and its provenance) -- this
    endpoint now shows the same three tiers the original server does.
    """
    now = datetime.now(timezone.utc)
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT mmsi, name, last_seen, last_lat, last_lon, last_source_id, quality,
                   sog_mph, cog_deg, heading_deg, nav_status, shiptype, is_commercial,
                   callsign, destination, draught
            FROM vessel_current_state
            WHERE last_seen > now() - interval '300 seconds'
            ORDER BY last_seen DESC
            """
        )

        vessels = []
        for r in rows:
            age_s = (now - r["last_seen"]).total_seconds()
            lat, lon = r["last_lat"], r["last_lon"]
            src = RECEIVERS.get(r["last_source_id"], RECEIVERS["lake_mac"])
            direction = _dir_from_cog(r["cog_deg"])
            river_mile = _lat_lon_to_rm(lat, lon) if lat is not None and lon is not None else None

            vessels.append({
                "mmsi": r["mmsi"],
                "name": _clean_name(r["name"]),
                "lat": round(lat, 6) if lat is not None else None,
                "lon": round(lon, 6) if lon is not None else None,
                "speed_mph": r["sog_mph"],
                "cog_deg": round(r["cog_deg"], 1) if r["cog_deg"] is not None else None,
                "heading_deg": r["heading_deg"],
                "shiptype": r["shiptype"],
                "is_commercial": r["is_commercial"],
                "last_signal_s": age_s,
                "is_live": True,
                "heard_by": r["last_source_id"],
                "distance_mi_from_receiver": round(_hav_mi(src["lat"], src["lon"], lat, lon), 2) if lat is not None else None,
                "bearing_deg_from_receiver": round(_bearing_deg(src["lat"], src["lon"], lat, lon), 0) if lat is not None else None,
                "river_mile": round(river_mile, 2) if river_mile is not None else None,
                "direction": direction,
                "callsign": r["callsign"] or None,
                "destination": r["destination"] or None,
                "draught_m": r["draught"],
            })

        try:
            dead_reckoned, lock_projected = await projections.compute_projected_tiers(
                conn, LOCK_META, RECEIVERS, _clean_name, _bearing_deg,
            )
            vessels.extend(dead_reckoned)
            vessels.extend(lock_projected)
        except Exception as e:
            # Projection tiers are additive -- a bug in them should never take
            # down the live tier, which is the one users actually depend on
            # most. Real failure here is a real bug to go fix, not something
            # to silently retry into looking fine.
            app.state.last_projection_error = f"{type(e).__name__}: {e!r}\n{traceback.format_exc()}"

    return {
        "generated_at": now.isoformat(),
        "fresh_cutoff_s": FRESH_CUTOFF_S,
        "vessels": vessels,
    }


# --- Lock-status 4-tier flag logic, ported verbatim from lock-status.py ---
# (thresholds/wording unchanged -- this is presentation logic Tom actually
# sees, so it's a direct port, not a rebuild-from-guesses.)

def _dir_word(raw):
    return {"U": "upbound", "D": "downbound"}.get(raw)


def _delay_range_for_barges(barges):
    if barges is None or barges == 0:
        return ("15-30 min", 22)
    if barges <= 3:
        return ("30-45 min", 37)
    if barges <= 8:
        return ("45-60 min", 57)
    if barges <= 12:
        return ("60-90 min", 75)
    if barges <= 15:
        return ("90-120 min", 105)
    return ("2+ hours", 135)


def _is_recreational(vessel_name, raw_payload):
    if "RECREATION" in (vessel_name or "").upper():
        return True
    try:
        payload = json.loads(raw_payload) if isinstance(raw_payload, str) else (raw_payload or {})
    except (TypeError, ValueError):
        payload = {}
    # Real bug found live 2026-08-07: at least one historical row has a
    # double-JSON-encoded raw_payload (a string that itself decodes to a
    # string, not a dict), which crashed every /lock-status request with
    # 'str' object has no attribute 'get'. The isinstance guard above only
    # catches raw_payload arriving as a string; it doesn't catch the
    # PARSED result also being a non-dict. Not worth silently swallowing
    # bad data forever, but not worth 500ing the whole endpoint over one
    # malformed historical row either -- treat a non-dict payload as "not
    # recreational" rather than crash.
    if not isinstance(payload, dict):
        return False
    return payload.get("vessel_no") == "9999999"


def _find_current_lockage(rows, now):
    """Vessel with sol_at <= now and (no eol_at OR end within last 30 min)
    -- the one currently in the chamber."""
    best = None
    for r in rows:
        if _is_recreational(r["vessel_name"], r["raw_payload"]):
            continue
        sol, end = r["sol_at"], r["eol_at"]
        if sol is None:
            continue
        in_progress = sol <= now if end is None else (sol <= now <= end + timedelta(minutes=15))
        if in_progress:
            candidate = {
                "vessel": _clean_name(r["vessel_name"]),
                "barges": r["barges"] or 0,
                "direction": r["direction"],
                "started_at": sol,
                "started_min_ago": int((now - sol).total_seconds() / 60),
            }
            if best is None or candidate["started_at"] > best["started_at"]:
                best = candidate
    return best


def _find_last_cleared(rows, now):
    """Most recent commercial lockage that has already ended."""
    best = None
    for r in rows:
        if _is_recreational(r["vessel_name"], r["raw_payload"]):
            continue
        end = r["eol_at"]
        if not end or end > now:
            continue
        if best is None or end > best["end"]:
            best = {
                "vessel": _clean_name(r["vessel_name"]),
                "barges": r["barges"] or 0,
                "direction": r["direction"],
                "end": end,
                "min_ago": int((now - end).total_seconds() / 60),
            }
    return best


def _compute_flag(live_state, active, last_cleared, now):
    """4-tier logic, exact thresholds from lock-status.py:
    normal  — pending=0 AND (locking=0 OR active has 0-3 barges)
    watch   — pending=1 OR active 4-8 barges OR delay24h_min >= 20
    delay   — (pending>=1 AND locking=1) OR (4-8 barges AND delay24h_min >= 30)
              OR pending>=2 with any active work
    jam     — active has 9+ barges (double-cut) OR active lockage 90+ min old
              OR pending>=3
    """
    pending = live_state.get("pending", 0) or 0
    locking = live_state.get("locking", 0) or 0
    delay24h = live_state.get("delay24h_min", 0) or 0
    active_barges = (active or {}).get("barges") or 0
    active_age = (active or {}).get("started_min_ago") or 0

    if active_barges >= 9:
        return "jam", active_barges, "big-tow-double-cut"
    if active and active_age >= 90:
        return "jam", None, "long-lockage"
    if pending >= 3:
        return "jam", None, "queue-backup"
    if pending >= 1 and locking >= 1:
        return "delay", active_barges, "queue-and-active"
    if active_barges >= 4 and delay24h >= 30:
        return "delay", active_barges, "medium-tow-with-recent-delays"
    if pending >= 2:
        return "delay", None, "queue-pending"
    if pending >= 1:
        return "watch", None, "one-pending"
    if active_barges >= 4:
        return "watch", active_barges, "medium-tow"
    if delay24h >= 20:
        return "watch", None, "recent-delays"
    return "normal", active_barges, "clear"


def _build_badge_and_line(name, live_state, flag, active_barges, driver, active, last_cleared):
    pending = live_state.get("pending", 0) or 0
    delay24h = live_state.get("delay24h_min", 0) or 0

    if flag == "jam":
        if driver == "big-tow-double-cut":
            range_copy, _ = _delay_range_for_barges(active_barges)
            badge = f"~{range_copy} delay"
            v = active["vessel"]
            dir_word = _dir_word(active["direction"]) or ""
            line = (f"{name} — big tow ({v}, {active_barges}-barge double-cut) locking through "
                    f"{dir_word}. Expect ~{range_copy} delay.")
        elif driver == "long-lockage":
            badge = "Extended lockage"
            v = (active or {}).get("vessel", "tow")
            line = f"{name} — {v} has been locking {active['started_min_ago']}+ min. Expect ongoing delay."
        else:  # queue-backup
            badge = f"{pending} tows pending"
            line = f"{name} — jam: {pending} tows pending, plan for 2+ hr wait."
    elif flag == "delay":
        badge = "~1 hr delay"
        if active_barges:
            range_copy, _ = _delay_range_for_barges(active_barges)
            line = f"{name} — congested. {active_barges}-barge tow locking, expect ~{range_copy} delay."
        else:
            line = f"{name} — congested with {pending} pending. Expect ~1 hr wait."
    elif flag == "watch":
        badge = "Medium tow — plan ~45 min"
        if active_barges:
            line = f"{name} — medium tow ({active_barges} barges) locking. Plan for ~45 min."
        elif pending:
            line = f"{name} — 1 tow pending, watch for delay."
        else:
            line = f"{name} — recent 24h delays averaging {delay24h} min. Plan for potential wait."
    else:  # normal
        badge = "Clear"
        if last_cleared:
            v = last_cleared["vessel"]
            line = f"{name} — clear. Last tow cleared {last_cleared['min_ago']} min ago ({v}, {last_cleared['barges']} barges)."
        else:
            line = f"{name} — clear. No recent commercial activity."

    return badge, line


@app.get("/replay")
async def replay(hours: int = None):
    """Compressed vessel-track playback -- ported 2026-08-07 from Reed's
    replay.py, see replay.py's module docstring for the real port notes.
    """
    try:
        async with app.state.pool.acquire() as conn:
            return await replay_mod.build(conn, RECEIVERS, window_hours=hours)
    except Exception as e:
        app.state.last_replay_error = f"{type(e).__name__}: {e!r}\n{traceback.format_exc()}"
        raise HTTPException(500, "replay failed")


@app.get("/admin/last-replay-error")
async def admin_last_replay_error(x_admin_key: str = Header(default="")):
    if not x_admin_key or x_admin_key != os.environ.get("ADMIN_RESTART_KEY", "__unset__"):
        raise HTTPException(404)
    return {"last_replay_error": getattr(app.state, "last_replay_error", None)}


@app.get("/vessel-profile")
async def vessel_profile(vessel: str):
    """Per-vessel lockage-history profile -- ported 2026-08-07 from Reed's
    vessel-profile.py, see vessel_profile.py's module docstring."""
    try:
        async with app.state.pool.acquire() as conn:
            return await vessel_profile_mod.build(conn, vessel)
    except Exception as e:
        app.state.last_vessel_profile_error = f"{type(e).__name__}: {e!r}\n{traceback.format_exc()}"
        raise HTTPException(500, "vessel-profile failed")


@app.get("/admin/last-vessel-profile-error")
async def admin_last_vessel_profile_error(x_admin_key: str = Header(default="")):
    if not x_admin_key or x_admin_key != os.environ.get("ADMIN_RESTART_KEY", "__unset__"):
        raise HTTPException(404)
    return {"last_vessel_profile_error": getattr(app.state, "last_vessel_profile_error", None)}


@app.get("/vessel-track")
async def vessel_track(vessel: str, hours: float = None, project_hours: float = None):
    """Real breadcrumb trail + channel-snapped projection for one vessel --
    ported 2026-08-07 from Reed's vessel-track.py, see vessel_track.py's
    module docstring."""
    try:
        async with app.state.pool.acquire() as conn:
            return await vessel_track_mod.build(conn, vessel, hours_back=hours, project_hours=project_hours)
    except Exception as e:
        app.state.last_vessel_track_error = f"{type(e).__name__}: {e!r}\n{traceback.format_exc()}"
        raise HTTPException(500, "vessel-track failed")


@app.get("/admin/last-vessel-track-error")
async def admin_last_vessel_track_error(x_admin_key: str = Header(default="")):
    if not x_admin_key or x_admin_key != os.environ.get("ADMIN_RESTART_KEY", "__unset__"):
        raise HTTPException(404)
    return {"last_vessel_track_error": getattr(app.state, "last_vessel_track_error", None)}


@app.get("/lock-status")
async def lock_status():
    """Field-parity pass 2026-08-02: 4-tier flag/badge/conditions_line
    logic ported verbatim from Reed's lock-status.py (see functions
    above). Reads per-transit rows from lock_status_observation (the
    queue-row equivalent) plus the live pending/locking/delay/throughput
    snapshot from lock_live_state -- both now sent by Reed's collector
    (added 2026-08-02 specifically for this port)."""
    now = datetime.now(timezone.utc)
    try:
        async with app.state.pool.acquire() as conn:
            # Real timeout found live 2026-08-06: this table is ~509K rows
            # now and the (lock_id, time DESC) ORDER BY doesn't line up with
            # either index cleanly (one covers the WHERE, the other the
            # ORDER BY), so the sort + Neon's occasional scale-to-zero wake
            # latency together were blowing past the pool's 10s
            # command_timeout. Same per-query override already used for
            # /admin/export -- doesn't touch the pool-wide default.
            obs_rows = await conn.fetch(
                """
                SELECT lock_id, vessel_name, barges, direction, sol_at, eol_at, raw_payload
                FROM lock_status_observation
                WHERE time > now() - interval '30 days'
                ORDER BY lock_id, time DESC
                """,
                timeout=30,
            )
            live_rows = await conn.fetch(
                "SELECT lock_id, pending, locking, delay24h_min, throughput_24h FROM lock_live_state",
                timeout=15,
            )
    except Exception as e:
        # TEMP 2026-08-06: /lock-status started 500ing/hanging on live prod
        # mid-migration-work -- same stash-and-read pattern as /ingest's
        # debug endpoint, remove once root cause is found and fixed.
        app.state.last_lock_status_error = f"{type(e).__name__}: {e!r}\n{traceback.format_exc()}"
        raise HTTPException(500, "lock-status failed")

    rows_by_lock: dict[str, list] = {}
    for r in obs_rows:
        rows_by_lock.setdefault(r["lock_id"], []).append(r)
    live_by_lock = {r["lock_id"]: r for r in live_rows}

    locks = {}
    for lock_id, meta in LOCK_META.items():
        rows = rows_by_lock.get(lock_id, [])
        live = live_by_lock.get(lock_id)
        live_state = {
            "pending": live["pending"] if live else 0,
            "locking": live["locking"] if live else 0,
            "delay24h_min": live["delay24h_min"] if live else 0,
            "throughput_24h": live["throughput_24h"] if live else 0,
        }
        active = _find_current_lockage(rows, now)
        last_cleared = _find_last_cleared(rows, now)
        flag, active_barges, driver = _compute_flag(live_state, active, last_cleared, now)
        badge, line = _build_badge_and_line(meta["name"], live_state, flag, active_barges, driver, active, last_cleared)

        locks[lock_id] = {
            "name": meta["name"],
            "river_mile": meta["river_mile"],
            "flag": flag,
            "map_badge": badge,
            "conditions_line": line,
            "live_state": live_state,
            "active_lockage": ({
                "vessel": active["vessel"],
                "barges": active["barges"],
                "direction": active["direction"],
                "started_min_ago": active["started_min_ago"],
            } if active else None),
            "last_cleared": ({
                "vessel": last_cleared["vessel"],
                "barges": last_cleared["barges"],
                "min_ago": last_cleared["min_ago"],
                "direction": _dir_word(last_cleared["direction"]),
            } if last_cleared else None),
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
                                                    last_source_id, quality, sog_mph, cog_deg,
                                                    heading_deg, nav_status, shiptype,
                                                    is_commercial, callsign, destination, draught)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                ON CONFLICT (mmsi) DO UPDATE SET
                    name = EXCLUDED.name, last_seen = EXCLUDED.last_seen,
                    last_lat = EXCLUDED.last_lat, last_lon = EXCLUDED.last_lon,
                    last_source_id = EXCLUDED.last_source_id, quality = EXCLUDED.quality,
                    sog_mph = EXCLUDED.sog_mph, cog_deg = EXCLUDED.cog_deg,
                    heading_deg = EXCLUDED.heading_deg, nav_status = EXCLUDED.nav_status,
                    shiptype = EXCLUDED.shiptype, is_commercial = EXCLUDED.is_commercial,
                    callsign = EXCLUDED.callsign, destination = EXCLUDED.destination,
                    draught = EXCLUDED.draught
                WHERE EXCLUDED.last_seen > vessel_current_state.last_seen
                """,
                p["mmsi"], p.get("name"), observed_at, p["lat"], p["lon"],
                source_id, p.get("quality", "live"), p.get("sog_mph"), p.get("cog_deg"),
                p.get("heading_deg"), p.get("nav_status"), p.get("shiptype"),
                p.get("is_commercial"), p.get("callsign"), p.get("destination"), p.get("draught"),
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

    elif event.event_type == "lock_live_state":
        p = event.payload
        await conn.execute(
            """
            INSERT INTO lock_live_state (lock_id, pending, locking, delay24h_min,
                                          throughput_24h, last_seen)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (lock_id) DO UPDATE SET
                pending = EXCLUDED.pending, locking = EXCLUDED.locking,
                delay24h_min = EXCLUDED.delay24h_min, throughput_24h = EXCLUDED.throughput_24h,
                last_seen = EXCLUDED.last_seen
            WHERE EXCLUDED.last_seen >= lock_live_state.last_seen
            """,
            p["lock_id"], p.get("pending", 0), p.get("locking", 0),
            p.get("delay24h_min", 0), p.get("throughput_24h", 0), observed_at,
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
    try:
        async with app.state.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO sources (source_id, type) VALUES ($1, 'unknown') ON CONFLICT DO NOTHING",
                batch.source_id,
            )
            async with conn.transaction():
                for event in batch.events:
                    await write_event(conn, batch.source_id, event)
    except Exception as e:
        # TEMP 2026-08-06: ingest has been silently 500ing since 2026-08-03
        # with nothing in the collector logs or wrangler tail to explain
        # why. wrangler tail can't see inside the container process, so
        # stash it in-memory for a gated debug read instead -- remove both
        # once the root cause is found, this isn't meant to linger.
        app.state.last_ingest_error = f"{type(e).__name__}: {e!r}\n{traceback.format_exc()}"
        raise HTTPException(500, "ingest failed")

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


# --- Email signup ---------------------------------------------------------
# This route existed on the OLD worker deployed at this same name before the
# Track C rebuild -- deploying a container-based worker over it silently
# dropped it once already (2026-08-01), causing real 404s on the site's
# landing-page signup form for ~2 days before it was caught. Keep this
# section intact on every future merge/port.
class SignupRequest(BaseModel):
    email: EmailStr
    site: str | None = None


@app.post("/signup")
async def signup(req: SignupRequest):
    async with app.state.pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO email_signups (email, site)
            VALUES ($1, $2)
            ON CONFLICT (email) DO NOTHING
            """,
            req.email, req.site,
        )
    return {"signed_up": True}


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


# --- Temporary operational escape hatch -----------------------------------
# Cloudflare Containers' sleepAfter idle-restart never fires while ANY
# traffic keeps hitting the container -- including a collector's retry loop
# hammering a route that 401s, which is exactly what happened rolling out
# SOURCE_SECRET_lpms 2026-08-03: the old process kept getting kept alive by
# the very retries that needed the new envVars it didn't have. No
# `wrangler containers restart` exists (only the destructive `delete`), so
# this lets the running process force its own supervised restart -- picks
# up fresh envVars from the DO constructor without touching the app
# definition. Gated on ADMIN_RESTART_KEY so it's not a public kill switch.
@app.get("/admin/last-ingest-error")
async def admin_last_ingest_error(x_admin_key: str = Header(default="")):
    # TEMP 2026-08-06, pairs with the in-memory stash in /ingest -- remove
    # both once the silent-write-failure root cause is found and fixed.
    if not x_admin_key or x_admin_key != os.environ.get("ADMIN_RESTART_KEY", "__unset__"):
        raise HTTPException(404)
    return {"last_ingest_error": getattr(app.state, "last_ingest_error", None)}


@app.get("/admin/last-lock-status-error")
async def admin_last_lock_status_error(x_admin_key: str = Header(default="")):
    if not x_admin_key or x_admin_key != os.environ.get("ADMIN_RESTART_KEY", "__unset__"):
        raise HTTPException(404)
    return {"last_lock_status_error": getattr(app.state, "last_lock_status_error", None)}


@app.get("/admin/last-projection-error")
async def admin_last_projection_error(x_admin_key: str = Header(default="")):
    if not x_admin_key or x_admin_key != os.environ.get("ADMIN_RESTART_KEY", "__unset__"):
        raise HTTPException(404)
    return {"last_projection_error": getattr(app.state, "last_projection_error", None)}


@app.post("/admin/restart")
async def admin_restart(x_admin_key: str = Header(default="")):
    if not x_admin_key or x_admin_key != os.environ.get("ADMIN_RESTART_KEY", "__unset__"):
        raise HTTPException(404)
    os._exit(1)


# --- Temporary migration escape hatch --------------------------------------
# 2026-08-06: migrating this DB to Railway, but the raw Neon connection
# string was never saved locally (deliberately, for security) -- only this
# running app has it, via DATABASE_URL. Rather than go hunting for Neon
# console credentials, export through the app's own DB pool, gated the same
# way as /admin/restart. Small operational tables export in full; the large
# hypertables (telemetry_raw/vessel_position/lock_status_observation/
# derived_eta) are bounded to a recent window via ?days= so this stays a
# reasonable HTTP payload instead of trying to ship the entire forensic
# archive over JSON. Remove this route once the migration is done -- it's
# a real, if gated, data-export surface and shouldn't linger.
import decimal


def _json_safe(row):
    out = {}
    for k, v in dict(row).items():
        if isinstance(v, (datetime, decimal.Decimal)):
            out[k] = str(v)
        else:
            out[k] = v
    return out


@app.get("/admin/export")
async def admin_export(days: int = 45, x_admin_key: str = Header(default="")):
    if not x_admin_key or x_admin_key != os.environ.get("ADMIN_RESTART_KEY", "__unset__"):
        raise HTTPException(404)

    out = {}
    async with app.state.pool.acquire() as conn:
        for table in ("sources", "vessel_current_state", "lock_live_state",
                      "email_signups", "photo_submissions"):
            try:
                rows = await conn.fetch(f"SELECT * FROM {table}")
                out[table] = [_json_safe(r) for r in rows]
            except Exception as e:
                out[table] = {"error": str(e)}

    return out


_EXPORT_HYPERTABLES = {
    "telemetry_raw": "received_at",
    "vessel_position": "time",
    "lock_status_observation": "time",
    "derived_eta": "computed_at",
}


@app.get("/admin/export-table")
async def admin_export_table(
    table: str,
    since_days_ago: float = 1,
    until_days_ago: float = 0,
    x_admin_key: str = Header(default=""),
):
    # Whole-archive-in-one-response (the old /admin/export hypertable loop)
    # blew past both the container's response-size/time budget on the real
    # 45-day pull -- HTTP 500 with a tiny body, no useful error surfaced.
    # This exports ONE table over a bounded day-window per call so a driver
    # script can walk the full range in safe slices instead.
    if not x_admin_key or x_admin_key != os.environ.get("ADMIN_RESTART_KEY", "__unset__"):
        raise HTTPException(404)
    if table not in _EXPORT_HYPERTABLES:
        raise HTTPException(400, f"table must be one of {list(_EXPORT_HYPERTABLES)}")
    if since_days_ago <= until_days_ago:
        raise HTTPException(400, "since_days_ago must be > until_days_ago")

    time_col = _EXPORT_HYPERTABLES[table]
    async with app.state.pool.acquire() as conn:
        try:
            rows = await conn.fetch(
                f"SELECT * FROM {table} "
                f"WHERE {time_col} > now() - make_interval(days => $1) "
                f"AND {time_col} <= now() - make_interval(days => $2) "
                f"ORDER BY {time_col}",
                since_days_ago,
                until_days_ago,
                timeout=100,
            )
            return {"table": table, "rows": [_json_safe(r) for r in rows]}
        except Exception as e:
            raise HTTPException(500, repr(e))


@app.post("/admin/prune-telemetry-raw")
async def admin_prune_telemetry_raw(older_than_days: int = 20, x_admin_key: str = Header(default="")):
    # TEMP EMERGENCY 2026-08-06: Neon project hit its 512MB cap on 2026-08-03,
    # which has been silently failing every /ingest write since. telemetry_raw
    # is the raw forensic archive -- not read by the live map or lock-status
    # logic (those read vessel_position / lock_status_observation directly) --
    # so it's the safe thing to prune to get writes flowing again right now,
    # while the real fix (Railway migration, no 512MB cap) finishes.
    if not x_admin_key or x_admin_key != os.environ.get("ADMIN_RESTART_KEY", "__unset__"):
        raise HTTPException(404)
    if older_than_days < 0:
        raise HTTPException(400, "older_than_days must be >= 0")
    async with app.state.pool.acquire() as conn:
        deleted = await conn.execute(
            "DELETE FROM telemetry_raw WHERE received_at < now() - make_interval(days => $1)",
            older_than_days,
            timeout=100,
        )
        remaining = await conn.fetchval("SELECT count(*) FROM telemetry_raw")
        # Plain DELETE only marks tuples dead -- doesn't shrink the file or
        # free space against Neon's project-size cap. VACUUM FULL rewrites
        # the table to actually reclaim it. Can't run inside conn.transaction()
        # (VACUUM disallowed in a tx block); this execute is unwrapped so it's fine.
        await conn.execute("VACUUM FULL telemetry_raw", timeout=100)
    return {"deleted": deleted, "remaining_telemetry_raw_rows": remaining, "vacuumed": True}


@app.get("/admin/live-schema")
async def admin_live_schema(x_admin_key: str = Header(default="")):
    # Railway-migration helper: schema.sql is known stale (photo_submissions
    # was never added to it, created ad hoc directly on Neon) so pull the
    # real live schema straight from information_schema instead of
    # reconstructing it by hand from scattered SQL files.
    if not x_admin_key or x_admin_key != os.environ.get("ADMIN_RESTART_KEY", "__unset__"):
        raise HTTPException(404)
    async with app.state.pool.acquire() as conn:
        cols = await conn.fetch(
            """
            SELECT table_name, column_name, data_type, udt_name, is_nullable,
                   column_default, character_maximum_length, numeric_precision
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position
            """
        )
        pks = await conn.fetch(
            """
            SELECT tc.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = 'public'
            """
        )
        uniques = await conn.fetch(
            """
            SELECT tc.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            WHERE tc.constraint_type = 'UNIQUE' AND tc.table_schema = 'public'
            """
        )
        indexes = await conn.fetch(
            "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'public'"
        )
    return {
        "columns": [_json_safe(r) for r in cols],
        "primary_keys": [_json_safe(r) for r in pks],
        "unique_constraints": [_json_safe(r) for r in uniques],
        "indexes": [_json_safe(r) for r in indexes],
    }


@app.get("/admin/table-counts")
async def admin_table_counts(x_admin_key: str = Header(default="")):
    if not x_admin_key or x_admin_key != os.environ.get("ADMIN_RESTART_KEY", "__unset__"):
        raise HTTPException(404)
    tables = ["sources", "telemetry_raw", "vessel_position", "lock_status_observation",
              "vessel_current_state", "derived_eta", "email_signups", "photo_submissions",
              "lock_live_state"]
    out = {}
    async with app.state.pool.acquire() as conn:
        for t in tables:
            try:
                out[t] = await conn.fetchval(f"SELECT count(*) FROM {t}")
            except Exception as e:
                out[t] = f"error: {e}"
    return out


@app.get("/admin/export-table-range")
async def admin_export_table_range(table: str, x_admin_key: str = Header(default="")):
    if not x_admin_key or x_admin_key != os.environ.get("ADMIN_RESTART_KEY", "__unset__"):
        raise HTTPException(404)
    if table not in _EXPORT_HYPERTABLES:
        raise HTTPException(400, f"table must be one of {list(_EXPORT_HYPERTABLES)}")
    time_col = _EXPORT_HYPERTABLES[table]
    async with app.state.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT min({time_col}) AS oldest, max({time_col}) AS newest, now() AS db_now FROM {table}"
        )
        return _json_safe(row)
