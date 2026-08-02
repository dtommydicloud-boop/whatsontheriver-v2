"""Stateless public read API. Response shapes match the CURRENT
next-vessel-server /live-positions and /lock-status field-for-field
where the data is available from this schema, so the existing frontend
(index.html, conditions.html) can point here with zero changes.

Never touches the ingest path, the queue, or AIS/LPMS directly -- only
ever reads from Postgres. If the writer is behind or the queue backs up,
this endpoint still serves whatever's in vessel_current_state /
lock_status_observation, just possibly a bit stale (labeled honestly).

HONEST GAP, not silently faked: the current production API enriches
lock-projected vessels with river-mile lookups, distance-from-receiver,
and channel-snapped path_from_lock polylines -- that's real projection
business logic living in Reed's existing next-vessel-server, not just
stored data. This skeleton returns the fields this schema can genuinely
answer and leaves the rest as explicit TODOs rather than fabricating
plausible-looking numbers. Port that logic here before this is a real
drop-in replacement, don't ship it silently missing.
"""
import os
from datetime import datetime, timezone

import asyncpg
from fastapi import FastAPI

app = FastAPI(title="whatsontheriver read API")

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


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
