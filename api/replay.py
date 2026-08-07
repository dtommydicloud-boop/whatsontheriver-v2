"""replay.py -- compressed vessel-track playback, ported from Reed's
replay.py (next-vessel-server, lake-mac) against v2's Postgres schema.

Same port philosophy as projections.py: the algorithm (downsampling
cadence, plausible-range filtering, channel-snapped simulated lock-to-lock
legs) is ported close to verbatim; the data-access layer is rebuilt against
this schema's tables instead of the source's daily JSONL snapshot files and
LPMS disk cache.

Real difference worth being explicit about: the source reads periodic FULL
antenna snapshots (every ship visible right now, repeated each poll) and
has to filter out repeats of the same stale position via a last_signal
freshness cutoff. v2's vessel_position table only ever gets a new row when
a genuinely new ais_position ingest event arrives (see main.py's
write_event) -- there's no repeated-snapshot noise to filter, so that
specific freshness cutoff isn't needed here. Everything else (cadence
downsampling, per-vessel point cap, plausible-range filtering, simulated
lock-to-lock legs) is preserved.
"""
from datetime import datetime, timedelta, timezone

from api import projections

WINDOW_HOURS_DEFAULT = 5
WINDOW_HOURS_MAX = 48
CADENCE_S = 60
MAX_PTS_PER_VESSEL = 400
MAX_VESSELS = 80

MAX_PLAUSIBLE_RANGE_KM = 50.0

# Same 3 locks as the source -- commercial-tow lock-to-lock legs, only
# where we have real, non-anonymized LPMS vessel-name matching.
SIM_LOCKS = [("02", "Hastings", 815.2), ("03", "Red Wing (Welch)", 796.9), ("04", "Alma", 752.8)]
SIM_LOCK_RM = {lk: rm for lk, _, rm in SIM_LOCKS}
SIM_LOCK_NAME = {lk: nm for lk, nm, rm in SIM_LOCKS}
SIM_DEDUP_HOURS = 3.0
SIM_MAX_LEG_HOURS = 24.0


def _hav_km(a_lat, a_lon, b_lat, b_lon):
    import math
    r = 6371.0
    p1, p2 = math.radians(a_lat), math.radians(b_lat)
    x = (math.sin(math.radians(b_lat - a_lat) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(math.radians(b_lon - a_lon) / 2) ** 2)
    return 2 * r * math.asin(math.sqrt(x))


def _clean_name(n):
    n = (n or "").strip()
    if not n:
        return None
    parts = n.rsplit(" ", 1)
    if len(parts) == 2 and parts[1].isdigit():
        n = parts[0]
    return n.title()


def _name_key(n):
    return (n or "").strip().upper()


def _channel_slice_with_rm(from_rm, to_rm):
    """Same as projections.channel_slice, but also returns river-mile
    (chainage) per vertex, so the frontend can interpolate time->river_mile
    (monotone) instead of time->lat/lon and avoid cutting corners across
    river bends -- ported straight from the source's rationale."""
    coords, rms = projections._load_channel()
    if not coords:
        return []
    lo_rm, hi_rm = min(from_rm, to_rm), max(from_rm, to_rm)
    reverse = from_rm > to_rm
    start_lat, start_lon = projections.rm_to_latlon(lo_rm)
    end_lat, end_lon = projections.rm_to_latlon(hi_rm)
    if start_lat is None or end_lat is None:
        return []
    slice_pts = [(round(start_lon, 6), round(start_lat, 6), round(lo_rm, 3))]
    for i, rm in enumerate(rms):
        if lo_rm < rm < hi_rm:
            slice_pts.append((round(coords[i][0], 6), round(coords[i][1], 6), round(rm, 3)))
    slice_pts.append((round(end_lon, 6), round(end_lat, 6), round(hi_rm, 3)))
    dedup = []
    for p in slice_pts:
        if not dedup or dedup[-1][:2] != p[:2]:
            dedup.append(p)
    if reverse:
        dedup.reverse()
    return dedup


async def _load_lock_clearances(conn, window_start, now):
    """{name_key: [{lock_id, t, rm}, ...]} sorted by time, for the 3 SIM_LOCKS,
    built from real lock_status_observation rows instead of the source's LPMS
    disk cache. Same multi-cut collapse (same lock within SIM_DEDUP_HOURS).

    Real bug found 2026-08-07 via a second-opinion code review: this query
    had no lower time bound at all -- eol_at <= now with nothing else --
    so every /replay call scanned the ENTIRE history of lock_status_observation
    for the 3 sim locks regardless of the requested window (5h/24h/48h).
    Bounded to window_start minus SIM_MAX_LEG_HOURS margin: a clearance pair
    can straddle the window boundary (the first clearance just before
    window_start, the segment still visible inside it), and SIM_MAX_LEG_HOURS
    is the real cap _build_simulated_segments already applies to how long a
    single leg can span, so that's the correct amount of lookback margin.
    """
    rows = await conn.fetch(
        """
        SELECT lock_id, vessel_name, eol_at
        FROM lock_status_observation
        WHERE lock_id = ANY($1::text[])
          AND eol_at IS NOT NULL
          AND eol_at <= $2
          AND eol_at >= $3
        ORDER BY vessel_name, eol_at
        """,
        [lk for lk, _, _ in SIM_LOCKS], now, window_start - timedelta(hours=SIM_MAX_LEG_HOURS),
        timeout=15,
    )
    by_vessel = {}
    for r in rows:
        if r["vessel_name"] and "RECREATION" in r["vessel_name"].upper():
            continue
        key = _name_key(r["vessel_name"])
        if not key:
            continue
        by_vessel.setdefault(key, []).append({
            "lock_id": r["lock_id"], "t": int(r["eol_at"].timestamp()), "rm": SIM_LOCK_RM[r["lock_id"]],
        })

    out = {}
    for key, events in by_vessel.items():
        events.sort(key=lambda e: e["t"])
        collapsed = []
        for e in events:
            if (collapsed and collapsed[-1]["lock_id"] == e["lock_id"]
                    and (e["t"] - collapsed[-1]["t"]) <= SIM_DEDUP_HOURS * 3600):
                collapsed[-1]["t"] = e["t"]
            else:
                collapsed.append(dict(e))
        out[key] = collapsed
    return out


def _build_simulated_segments(name, window_start_ts, window_end_ts, clearances_by_vessel):
    key = _name_key(name)
    events = clearances_by_vessel.get(key)
    if not events or len(events) < 2:
        return []
    segments = []
    for a, b in zip(events, events[1:]):
        if a["lock_id"] == b["lock_id"]:
            continue
        if b["t"] < window_start_ts or a["t"] > window_end_ts:
            continue
        gap_h = (b["t"] - a["t"]) / 3600.0
        if gap_h <= 0 or gap_h > SIM_MAX_LEG_HOURS:
            continue
        path_pts = _channel_slice_with_rm(a["rm"], b["rm"])
        if not path_pts:
            continue
        segments.append({
            "lock_from": SIM_LOCK_NAME[a["lock_id"]],
            "lock_to": SIM_LOCK_NAME[b["lock_id"]],
            "cleared_from_at": datetime.fromtimestamp(a["t"], tz=timezone.utc).isoformat(timespec="seconds"),
            "cleared_to_at": datetime.fromtimestamp(b["t"], tz=timezone.utc).isoformat(timespec="seconds"),
            "path": [[lon, lat] for lon, lat, _rm in path_pts],
            "path_rm": [rm for _lon, _lat, rm in path_pts],
        })
    return segments


async def build(conn, receivers, window_hours=None):
    if window_hours is None:
        window_hours = WINDOW_HOURS_DEFAULT
    try:
        window_hours = int(window_hours)
    except (TypeError, ValueError):
        window_hours = WINDOW_HOURS_DEFAULT
    window_hours = max(1, min(window_hours, WINDOW_HOURS_MAX))

    now = datetime.now(timezone.utc)
    now_ts = int(now.timestamp())
    window_start = now - timedelta(hours=window_hours)
    window_start_ts = int(window_start.timestamp())

    rows = await conn.fetch(
        """
        SELECT time, source_id, mmsi, name, lat, lon, sog_mph, cog_deg, is_commercial
        FROM vessel_position
        WHERE time > $1 AND time <= $2
        ORDER BY mmsi, time
        """,
        window_start, now,
        timeout=30,
    )

    tracks = {}
    for r in rows:
        mmsi = r["mmsi"]
        lat, lon = r["lat"], r["lon"]
        if not mmsi or lat is None or lon is None:
            continue
        src = receivers.get(r["source_id"], receivers["lake_mac"])
        if _hav_km(src["lat"], src["lon"], lat, lon) > MAX_PLAUSIBLE_RANGE_KM:
            continue  # corrupted decode -- geographically impossible for this receiver
        t = int(r["time"].timestamp())
        # vessel_position has no shiptype column (only vessel_current_state
        # does) -- is_commercial is already computed at ingest time
        # (main.py's write_event checks COMMERCIAL_SHIPTYPES), so use that
        # directly instead of re-deriving from a shiptype this table doesn't
        # carry per-row.
        entry = tracks.setdefault(mmsi, {
            "mmsi": mmsi, "name": _clean_name(r["name"]),
            "is_commercial": bool(r["is_commercial"]),
            "points": [], "_last_t": None,
        })
        if r["is_commercial"]:
            entry["is_commercial"] = True
        if entry["_last_t"] is not None and (t - entry["_last_t"]) < CADENCE_S:
            continue
        entry["_last_t"] = t
        pt = {"t": t, "lat": round(lat, 6), "lon": round(lon, 6)}
        if isinstance(r["sog_mph"], (int, float)):
            pt["speed"] = round(r["sog_mph"], 1)
        if isinstance(r["cog_deg"], (int, float)):
            pt["cog"] = round(r["cog_deg"], 1)
        entry["points"].append(pt)
        if entry["name"] is None:
            entry["name"] = _clean_name(r["name"])

    clearances_by_vessel = await _load_lock_clearances(conn, window_start, now)

    finalized = []
    for entry in tracks.values():
        if not entry["points"]:
            continue
        entry.pop("_last_t", None)
        if len(entry["points"]) > MAX_PTS_PER_VESSEL:
            step = len(entry["points"]) / MAX_PTS_PER_VESSEL
            entry["points"] = [entry["points"][int(i * step)] for i in range(MAX_PTS_PER_VESSEL)]
        for pt in entry["points"]:
            rm = projections.latlon_to_rm(pt["lat"], pt["lon"])
            if rm is not None:
                pt["river_mile"] = round(rm, 3)
        entry["point_count"] = len(entry["points"])
        entry["first_t"] = entry["points"][0]["t"]
        entry["last_t"] = entry["points"][-1]["t"]
        entry["simulated_segments"] = (
            _build_simulated_segments(entry["name"], window_start_ts, now_ts, clearances_by_vessel)
            if entry["is_commercial"] and entry["name"] else []
        )
        finalized.append(entry)

    finalized.sort(key=lambda e: (not e["is_commercial"], -e["point_count"]))
    if len(finalized) > MAX_VESSELS:
        finalized = finalized[:MAX_VESSELS]

    return {
        "generated_at": now.isoformat(timespec="seconds"),
        "window_hours": window_hours,
        "window_start_ts": window_start_ts,
        "window_end_ts": now_ts,
        "cadence_s": CADENCE_S,
        "vessel_count": len(finalized),
        "point_count_total": sum(e["point_count"] for e in finalized),
        "simulated_segment_count_total": sum(len(e["simulated_segments"]) for e in finalized),
        "tracks": finalized,
        "honest_caveats": [
            "Points are only where an AIS receiver heard the vessel; gaps occur "
            "where a boat drifts out of range -- do not interpolate straight "
            "lines across gaps as truth.",
            f"Downsampled to ~1 point per {CADENCE_S}s per vessel to keep payload small.",
            "tracks[].points[].river_mile and tracks[].simulated_segments[].path_rm give "
            "distance-along-channel (chainage) for every point, real and simulated alike -- "
            "interpolate time->river_mile (monotone), not time->lat/lon, to avoid cutting "
            "corners across river bends.",
            "tracks[].simulated_segments: one entry per real LPMS lock-to-lock leg "
            "(Hastings/Red Wing/Alma) that overlaps this window, channel-snapped between "
            "the two real clearance timestamps. Commercial tows only (recreational "
            "lockages are anonymized in LPMS). A leg longer than 24h is dropped as an "
            "implausible single transit.",
        ],
    }
