"""vessel_track.py -- real antenna-heard breadcrumb trail for ONE vessel,
ported from Reed's vessel-track.py (next-vessel-server, lake-mac) against
v2's Postgres schema.

Tom's ask (per the source's own docstring): "I'd like to see a track, the
whole track that we have that AIS information from Reads Landing all the
way over to there, and when we lose it, and then I'd like to see the
projected one in another hour when it's going up towards Red Wing."
Automatic, <5s load.

Data access rebuilt: the source tails daily JSONL AIS snapshot files and
has to filter out ais-catcher's repeated-stale-position noise via a
last_signal freshness check. v2's vessel_position table only ever gets a
new row on a genuinely new ingest event (same fact replay.py's port
already relies on) -- there is no repeated-snapshot noise to filter here,
so that specific check is dropped. Table is small (~120K rows total as of
this port, full history, not just today) so a straight time+name filter on
the existing time index is fast without needing a dedicated name index --
confirmed via EXPLAIN ANALYZE before writing this.

Algorithm (direction-from-trail-delta, channel-snapped projection using
her own fresh speed or a typical directional band, the separate "exactly
1h out" point) ported verbatim from the source.
"""
from datetime import datetime, timedelta, timezone

from api import projections

CADENCE_S = 45
HOURS_BACK_DEFAULT = 6
HOURS_BACK_MAX = 24
PROJECT_HOURS_DEFAULT = 1.0
PROJECT_HOURS_MAX = 3.0
PROJECT_SAMPLE_MIN = 10
LIVE_SPEED_TRUST_AGE_S = 600

KN_TO_MPH = 1.15078


def _clean_ais_name(nm):
    nm = (nm or "").upper().strip()
    nm = "".join(ch for ch in nm if ch.isalnum() or ch.isspace())
    parts = nm.split()
    if parts and parts[-1].isdigit():
        parts = parts[:-1]
    return " ".join(parts)


async def _load_real_trail(conn, vessel, cutoff):
    target = _clean_ais_name(vessel)
    rows = await conn.fetch(
        """
        SELECT time, name, lat, lon, sog_mph, cog_deg
        FROM vessel_position
        WHERE time > $1
          AND upper(trim(name)) = upper(trim($2))
        ORDER BY time
        """,
        cutoff, vessel,
        timeout=15,
    )
    pts = []
    last_t = None
    for r in rows:
        if _clean_ais_name(r["name"]) != target:
            continue
        if r["lat"] is None or r["lon"] is None:
            continue
        t = r["time"]
        t_ts = int(t.timestamp())
        if last_t is not None and (t_ts - last_t) < CADENCE_S:
            continue
        last_t = t_ts
        pts.append({
            "t": t_ts,
            "at": t.astimezone(timezone.utc).isoformat(timespec="seconds"),
            "lat": round(r["lat"], 6), "lon": round(r["lon"], 6),
            "speed_mph": round(r["sog_mph"], 1) if isinstance(r["sog_mph"], (int, float)) else None,
            "cog_deg": round(r["cog_deg"], 1) if isinstance(r["cog_deg"], (int, float)) else None,
        })
    return pts


async def build(conn, vessel, hours_back=None, project_hours=None):
    now = datetime.now(timezone.utc)
    now_ts = int(now.timestamp())

    if hours_back is None:
        hours_back = HOURS_BACK_DEFAULT
    try:
        hours_back = float(hours_back)
    except (TypeError, ValueError):
        hours_back = HOURS_BACK_DEFAULT
    hours_back = max(1.0, min(hours_back, HOURS_BACK_MAX))

    if project_hours is None:
        project_hours = PROJECT_HOURS_DEFAULT
    try:
        project_hours = float(project_hours)
    except (TypeError, ValueError):
        project_hours = PROJECT_HOURS_DEFAULT
    project_hours = max(0.0, min(project_hours, PROJECT_HOURS_MAX))

    cutoff = now - timedelta(hours=hours_back)
    real_trail = await _load_real_trail(conn, vessel, cutoff)

    if not real_trail:
        return {
            "vessel": vessel, "real_track": [], "last_real_fix": None,
            "projected_track": [], "projected_point_1h": None,
            "generated_at": now.isoformat(timespec="seconds"),
            "error": f"no antenna-heard positions for this vessel in the last {hours_back:g}h -- "
                     "falls back to whatever lock-projected marker /live-positions already shows.",
        }

    last = real_trail[-1]
    age_s = max(0, now_ts - last["t"])

    # Both ends stay in projections.py's channel-polyline RM system -- mixing
    # RM systems mid-projection produced a real seam bug in the source
    # (2026-07-27, Robin B. Ingram); there's only one RM system here so the
    # seam can't happen, but keeping the same single-system discipline.
    last_rm = projections.latlon_to_rm(last["lat"], last["lon"])
    first_rm = projections.latlon_to_rm(real_trail[0]["lat"], real_trail[0]["lon"])

    direction = None
    if last_rm is not None and first_rm is not None and abs(last_rm - first_rm) >= 0.2:
        direction = "upbound" if last_rm > first_rm else "downbound"
    if direction is None:
        direction = projections._dir_from_cog(last.get("cog_deg"))

    projected_track = []
    projected_point_1h = None
    projection_note = None
    if direction and project_hours > 0 and last_rm is not None:
        SPEED_HINT = {"upbound": (4.0, 5.5), "downbound": (5.5, 7.5)}
        lo_mph, hi_mph = SPEED_HINT.get(direction, (4.5, 6.0))
        if age_s <= LIVE_SPEED_TRUST_AGE_S and isinstance(last.get("speed_mph"), (int, float)) and last["speed_mph"] >= 1.0:
            proj_mph = last["speed_mph"]
            projection_note = f"projected at her own last-seen speed ({proj_mph} mph, {age_s}s ago)"
        else:
            proj_mph = (lo_mph + hi_mph) / 2.0
            projection_note = (f"her last real speed is {int(age_s/60)} min stale, too old to trust -- "
                               f"projected at typical {direction} speed ({proj_mph} mph)")
        dir_sign = 1 if direction == "upbound" else -1
        n_samples = max(1, int((project_hours * 60) / PROJECT_SAMPLE_MIN))
        for i in range(1, n_samples + 1):
            mins = i * PROJECT_SAMPLE_MIN
            rm_i = last_rm + dir_sign * proj_mph * (mins / 60.0)
            lat_i, lon_i = projections.rm_to_latlon(rm_i)
            if lat_i is None:
                continue
            projected_track.append({
                "minutes_ahead": mins,
                "at": (now + timedelta(minutes=mins)).isoformat(timespec="seconds"),
                "lat": round(lat_i, 6), "lon": round(lon_i, 6),
                "river_mile": round(rm_i, 2),
            })
        rm_1h = last_rm + dir_sign * proj_mph * 1.0
        lat_1h, lon_1h = projections.rm_to_latlon(rm_1h)
        if lat_1h is not None:
            projected_point_1h = {
                "at": (now + timedelta(hours=1)).isoformat(timespec="seconds"),
                "lat": round(lat_1h, 6), "lon": round(lon_1h, 6),
                "river_mile": round(rm_1h, 2),
            }

    return {
        "vessel": vessel,
        "direction": direction,
        "real_track": real_trail,
        "last_real_fix": {**last, "age_s": age_s, "river_mile": round(last_rm, 2) if last_rm is not None else None},
        "projected_track": projected_track,
        "projected_point_1h": projected_point_1h,
        "projection_note": projection_note,
        "generated_at": now.isoformat(timespec="seconds"),
        "honest_caveats": [
            "real_track is every real antenna decode we have for this vessel in the lookback "
            "window, downsampled to ~1 point per 45s -- gaps mean she was out of range, not "
            "that she stopped.",
            "projected_track and projected_point_1h start exactly where the real track ends "
            "('last_real_fix') and are snapped to the real river channel, not a straight line -- "
            "but they are projections, not real observations. Treat them as dashed/estimated in "
            "the UI, distinct from real_track.",
            "Projection uses her own last real speed if it is fresh (<10 min old), otherwise "
            "falls back to the typical directional speed band -- see projection_note for which "
            "one applied.",
        ],
    }
