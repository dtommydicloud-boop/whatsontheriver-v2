"""next_vessel.py -- for a user lat/lon on the Lake Pepin reach, predict the
next commercial tow expected from upstream and downstream, ported from
Reed's next-vessel.py (next-vessel-server, lake-mac) against v2's Postgres
schema.

This is the most bespoke endpoint in the whole port: it's a cabin-anchored
ETA model calibrated against 138 real historical transits past Tom's own
cabin (RM 774.3, own 4-anchor CENTERLINE RM system -- deliberately NOT the
same RM system as projections.py's channel-polyline one, same reason
vessel_track.py's port keeps its own single system: mixing two different
RM systems mid-calculation produced a real seam bug in the source). The
calibration constants (TRANSIT_MODELS) and lock/anchor geometry are ported
verbatim, unchanged data.

Data access rebuilt, same philosophy as every other port here: the source
tails an LPMS disk cache + daily AIS JSONL snapshot files; this queries
lock_status_observation and vessel_position directly.

Real simplification found doing this port: the source has ~6 separate
functions for "find a vessel's live/recent AIS position" (latest_ais_ships,
_live_position_for, _last_real_position_today, _find_last_observation_nv,
_find_last_observation_nv_cached, _collect_all_last_observations) --
that multiplicity exists ONLY because the source reads periodic snapshot
files bucketed by calendar day. vessel_position only ever gets a new row
on a genuine ingest event (same fact replay.py/vessel_track.py's ports
already rely on), so "the vessel's most recent known position, however
stale" is always just one query: ORDER BY time DESC LIMIT 1 (well, LIMIT 5
with a Python identity check -- see _latest_position). All six of the
source's functions collapse into that one helper here.

Real cross-source gap the source's _clean_ais_name exists to solve, and
which THIS port has to solve too since it queries vessel_position (AIS
names) using vessel names that came from lock_status_observation (LPMS
names): AIS carries registered punctuation ("W. Red Harris", "CGC
Wyaconda") while LPMS strips it ("W RED HARRIS", "CG WYACONDA"). A plain
upper(trim(name))=upper(trim($1)) SQL match (fast, and correct for the
common case) misses these -- _latest_position falls back to a slower
punctuation-stripped scan only when the fast path finds nothing.
vessel_position is small (~120K rows as of this port) so that fallback
scan is still well under a second; if the table grows a lot, this is the
first place to add a functional index (same fix already applied to
lock_status_observation for vessel_profile.py).

Honest gap, not silently faked: the source's per-vessel Red Wing
terminal-stop-rate proxy (red-wing-stop-lpms-proxy.json, nightly-rebuilt
on lake-mac disk) isn't ported -- it lives only on lake-mac and isn't
bundled data like pepin-channel.geojson (it's live-refreshed, a bundled
snapshot would just go stale). Effect: the terminal-stop caveat always
shows for a route that touches the Red Wing terminal zone, instead of
being suppressed for vessels with a proven near-zero stop rate. Safe
default (over-cautious, not under), just less refined than the source.
"""
import math
from datetime import datetime, timedelta, timezone

# --- Static calibration + geometry (ported verbatim, unchanged data) -----

LOCKS = [
    ("01", "St Paul", 847.6),
    ("02", "Hastings", 815.2),
    ("03", "Red Wing", 796.9),
    ("04", "Alma", 752.8),
    ("05", "Minneiska", 738.1),
    ("5A", "Winona", 728.5),
    ("06", "Trempealeau", 714.1),
]

CENTERLINE = [
    (44.7482, -92.8635, 815.2),  # Hastings L2
    (44.6122, -92.6110, 796.9),  # Red Wing L3
    (44.489,  -92.311,  774.3),  # Lake Mac cabin (gate anchor)
    (44.319,  -92.056,  752.8),  # Alma L4
]

CABIN_RM = 774.3
CAL_MIN_RM = 730.0
CAL_MAX_RM = 820.0
MAX_OFF_CORRIDOR_KM = 20.0

TRANSIT_MODELS = {
    ("04", "up_to_cabin", "small"):   {"p10": 121.3, "med": 248.4, "p90": 498.6, "n": 5},
    ("04", "up_to_cabin", "big"):     {"p10": 220.3, "med": 251.8, "p90": 316.1, "n": 17},
    ("03", "down_to_cabin", "small"): {"p10": 152.8, "med": 181.0, "p90": 605.8, "n": 5},
    ("03", "down_to_cabin", "big"):   {"p10": 163.0, "med": 242.9, "p90": 641.6, "n": 18},
}

RED_WING_TERMINAL_RM = (789.0, 791.3)
SHUTTLE_ROUNDTRIP_MAX_H = 3.0
SPEED_HINT_NV = {"downbound": (5.5, 7.5), "upbound": (4.0, 5.5)}   # mph per direction

MAX_WALK_HOPS = 2
MAX_BOATS_PER_DIRECTION = 2


def hav_km(a_lat, a_lon, b_lat, b_lon):
    r = 6371.0
    p1, p2 = math.radians(a_lat), math.radians(b_lat)
    x = (math.sin(math.radians(b_lat - a_lat) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(math.radians(b_lon - a_lon) / 2) ** 2)
    return 2 * r * math.asin(math.sqrt(x))


def lat_lon_to_rm(lat, lon):
    """Own 4-anchor RM system -- NOT projections.py's channel-polyline one.
    Returns (rm, distance_to_nearest_anchor_km)."""
    scored = sorted(CENTERLINE, key=lambda c: hav_km(lat, lon, c[0], c[1]))
    a, b = scored[0], scored[1]
    da = hav_km(lat, lon, a[0], a[1])
    db = hav_km(lat, lon, b[0], b[1])
    total = da + db
    if total == 0:
        return a[2], 0.0
    rm = (a[2] * db + b[2] * da) / total
    return rm, da


def clean(n):
    n = (n or "").strip()
    p = n.rsplit(" ", 1)
    if len(p) == 2 and p[1].isdigit():
        n = p[0]
    return n.title()


def _clean_ais_name(nm):
    nm = (nm or "").upper().strip()
    nm = "".join(ch for ch in nm if ch.isalnum() or ch.isspace())
    parts = nm.split()
    if parts and parts[-1].isdigit():
        parts = parts[:-1]
    if parts and parts[0] == "CGC":
        parts[0] = "CG"
    return " ".join(parts)


def barge_class(n):
    n = n or 0
    return "big" if n >= 4 else "small"


def collapse_multicut(rows):
    """rows: dicts with 'vessel','barges','end' (datetime), 'direction'.
    Groups by (vessel, upper-cased) and merges rows within 3h, keeping
    max(barges) and the latest end -- same LPMS multi-cut dedup as every
    other port here."""
    buckets = {}
    for r in rows:
        key = (r["vessel"] or "").upper().strip()
        buckets.setdefault(key, []).append(r)
    collapsed = []
    for key, group in buckets.items():
        group.sort(key=lambda x: x["end"])
        merged = [group[0]]
        for nxt in group[1:]:
            gap_h = (nxt["end"] - merged[-1]["end"]).total_seconds() / 3600.0
            if gap_h <= 3.0:
                merged[-1] = {
                    **merged[-1],
                    "barges": max(merged[-1]["barges"] or 0, nxt["barges"] or 0),
                    "end": nxt["end"],
                    "elapsed_h": nxt["elapsed_h"],
                    "multicut": True,
                }
            else:
                merged.append(nxt)
        collapsed.extend(merged)
    return collapsed


def scale_transit(base, cabin_leg_rm, user_leg_rm):
    if cabin_leg_rm == 0:
        return None
    ratio = user_leg_rm / cabin_leg_rm
    return {k: v * ratio for k, v in base.items() if k in ("p10", "med", "p90")}


def _is_local_shuttle(vessel_name, end_time, cand_all_directions, exclude_row=None, this_direction=None):
    for r in cand_all_directions:
        if r is exclude_row:
            continue
        if clean(r.get("vessel")) != vessel_name:
            continue
        other_dir = r.get("direction")
        if this_direction is not None and other_dir == this_direction:
            continue  # same-direction double-cut, not a shuttle
        other_end = r.get("end")
        if not other_end:
            continue
        gap_h = abs((other_end - end_time).total_seconds()) / 3600.0
        if gap_h <= SHUTTLE_ROUNDTRIP_MAX_H:
            return True
    return False


def _rw_terminal_touch(source_rm, user_rm):
    route_min, route_max = min(source_rm, user_rm), max(source_rm, user_rm)
    if RED_WING_TERMINAL_RM[0] <= user_rm <= RED_WING_TERMINAL_RM[1]:
        return True, "Destination is Red Wing terminal (Enstructure / Red Wing Grain / CD Terminal). Time-to-arrival, not time-through."
    if route_min < RED_WING_TERMINAL_RM[1] and route_max > RED_WING_TERMINAL_RM[0]:
        return True, "Route passes Red Wing terminal zone; vessel may pause there."
    return False, None


def _rw_stop_rate_for(vessel_name):
    # See module docstring "Honest gap" -- per-vessel proxy data lives only
    # on lake-mac disk and isn't ported. Terminal caveat still fires (safe
    # default); this refinement (suppressing it for proven near-zero-stop
    # vessels) is future work.
    return None


def _is_recreational(vessel_name):
    return "RECREATION" in (vessel_name or "").upper()


# --- Postgres-backed data access (rebuilt; see module docstring) ---------

async def _pull_lock(conn, lock_id, now):
    """All commercial lockage rows at this lock, any direction, wide enough
    window to cover the 24h elapsed-time cutoff below plus shuttle-window
    context. Returns list of dicts: vessel, barges, end, direction."""
    rows = await conn.fetch(
        """
        SELECT vessel_name, direction, barges, eol_at
        FROM lock_status_observation
        WHERE lock_id = $1
          AND eol_at IS NOT NULL
          AND eol_at > $2
        ORDER BY eol_at
        """,
        lock_id, now - timedelta(hours=30),
        timeout=15,
    )
    out = []
    for r in rows:
        if _is_recreational(r["vessel_name"]):
            continue
        out.append({
            "vessel_raw": r["vessel_name"], "vessel": clean(r["vessel_name"]),
            "direction": r["direction"], "barges": r["barges"] or 0, "end": r["eol_at"],
        })
    return out


async def _latest_position(conn, vessel_name, min_time=None):
    """Most recent vessel_position row for vessel_name, however stale (no
    age cutoff here -- callers apply their own). See module docstring for
    the two-tier exact/fallback match rationale."""
    target = _clean_ais_name(vessel_name)
    if not target:
        return None
    time_clause = "AND time > $2" if min_time is not None else ""
    args = [vessel_name] + ([min_time] if min_time is not None else [])
    rows = await conn.fetch(
        f"""
        SELECT time, lat, lon, sog_mph, cog_deg, name
        FROM vessel_position
        WHERE upper(trim(name)) = upper(trim($1)) {time_clause}
        ORDER BY time DESC LIMIT 5
        """,
        *args, timeout=10,
    )
    for r in rows:
        if _clean_ais_name(r["name"]) == target:
            return r
    rows = await conn.fetch(
        f"""
        SELECT time, lat, lon, sog_mph, cog_deg, name
        FROM vessel_position
        WHERE upper(regexp_replace(name, '[^A-Za-z0-9 ]', '', 'g'))
              LIKE '%' || upper(regexp_replace($1, '[^A-Za-z0-9 ]', '', 'g')) || '%'
        {time_clause}
        ORDER BY time DESC LIMIT 20
        """,
        *args, timeout=10,
    )
    for r in rows:
        if _clean_ais_name(r["name"]) == target:
            return r
    return None


async def _vessel_already_passed(conn, vessel_name, direction_word, user_rm, now, tolerance_mi=1.0):
    pos = await _latest_position(conn, vessel_name)
    if pos is None:
        return False
    rm, _ = lat_lon_to_rm(pos["lat"], pos["lon"])
    if rm is None:
        return False
    if direction_word == "downbound" and rm < user_rm - tolerance_mi:
        return True
    if direction_word == "upbound" and rm > user_rm + tolerance_mi:
        return True
    cog = pos["cog_deg"]
    if isinstance(cog, (int, float)):
        moving_upbound = (cog >= 315 or cog <= 45)
        moving_downbound = (135 <= cog <= 225)
        if direction_word == "upbound" and moving_downbound:
            return True
        if direction_word == "downbound" and moving_upbound:
            return True
    return False


async def _live_priority_rm(conn, vessel_name, direction_word, user_rm, now):
    """If we're hearing this vessel fresh (<120s) right now and she hasn't
    reached the user's point yet in her direction of travel, she must win
    the slot regardless of what the statistical model says (see source's
    Jennie K bug writeup, ported into this module's docstring history)."""
    pos = await _latest_position(conn, vessel_name)
    if pos is None:
        return None
    age_s = (now - pos["time"]).total_seconds()
    if age_s > 120:
        return None
    rm, _ = lat_lon_to_rm(pos["lat"], pos["lon"])
    if rm is None:
        return None
    if direction_word == "upbound" and rm > user_rm + 2.0:
        return None
    if direction_word == "downbound" and rm < user_rm - 2.0:
        return None
    return rm


async def build_live_estimate(conn, vessel_name, user_lat, user_lon, direction_word, now, user_rm=None):
    if not vessel_name:
        return None
    pos = await _latest_position(conn, vessel_name)
    if pos is None:
        return None
    age_s = (now - pos["time"]).total_seconds()
    if age_s > 120:
        return None
    lat, lon = pos["lat"], pos["lon"]
    speed_mph = pos["sog_mph"]
    cog = pos["cog_deg"]
    if not isinstance(speed_mph, (int, float)) or speed_mph < 1.0:
        return None
    if user_rm is not None:
        vessel_rm, _ = lat_lon_to_rm(lat, lon)
        if vessel_rm is not None:
            if direction_word == "downbound" and vessel_rm < user_rm - 1.0:
                return None
            if direction_word == "upbound" and vessel_rm > user_rm + 1.0:
                return None
    dist_mi = hav_km(lat, lon, user_lat, user_lon) * 0.621371
    dir_ok = True
    if isinstance(cog, (int, float)):
        if direction_word == "upbound":
            dir_ok = (cog >= 315 or cog <= 45)
        elif direction_word == "downbound":
            dir_ok = (135 <= cog <= 225)
    if not dir_ok:
        return None
    if dist_mi < 0.3:
        return {"eta": now.isoformat(timespec="seconds"), "confidence": "high",
                "note": f"{vessel_name} is right at your point right now",
                "distance_mi": round(dist_mi, 2), "current_speed_mph": round(speed_mph, 1),
                "last_signal_s": int(age_s)}
    if dist_mi > 15.0:
        return None
    hours = dist_mi / speed_mph
    eta_dt = now + timedelta(hours=hours)
    confidence = "high" if (age_s < 60 and speed_mph >= 3.0) else "medium"
    return {
        "eta": eta_dt.isoformat(timespec="seconds"), "confidence": confidence,
        "distance_mi": round(dist_mi, 2), "current_speed_mph": round(speed_mph, 1),
        "current_heading_deg": round(cog, 1) if isinstance(cog, (int, float)) else None,
        "last_signal_s": int(age_s), "source": "live-ais",
        "note": ("Educated guess from live position + speed. Not exact; boat may "
                 "slow, fleet barges, or stop en route. See eta.p10/med/p90 for the "
                 "honest 80% window."),
    }


async def compute_antenna_verified_bands(conn, vessel_name, direction_word, user_rm, now):
    if direction_word not in SPEED_HINT_NV:
        return None
    lo_mph, hi_mph = SPEED_HINT_NV[direction_word]
    pos = await _latest_position(conn, vessel_name)
    if pos is None:
        return None
    age_s = (now - pos["time"]).total_seconds()
    speed_mph = pos["sog_mph"]

    # Tier 1: LIVE_UNDERWAY (fresh <300s, moving >=3mph)
    if (age_s < 300 and isinstance(speed_mph, (int, float)) and speed_mph >= 3.0
            and pos["lat"] is not None and pos["lon"] is not None):
        vessel_rm, _ = lat_lon_to_rm(pos["lat"], pos["lon"])
        already_passed = (
            (direction_word == "downbound" and vessel_rm <= user_rm) or
            (direction_word == "upbound" and vessel_rm >= user_rm)
        )
        leg_mi = abs(user_rm - vessel_rm)
        if not already_passed and 0.1 < leg_mi < 20.0:
            p10_min = (leg_mi / hi_mph) * 60
            p90_min = (leg_mi / lo_mph) * 60
            med_min = (leg_mi / speed_mph) * 60
            if p10_min > med_min:
                p10_min = med_min * 0.9
            if p90_min < med_min:
                p90_min = med_min * 1.1
            return {
                "tier": "LIVE_UNDERWAY",
                "model": (f"LIVE_UNDERWAY (distance/speed band, speed_now={speed_mph:.1f} mph, "
                          f"directional range [{lo_mph}, {hi_mph}] mph, last_signal {int(age_s)}s ago)"),
                "confidence": "high",
                "eta_p10": now + timedelta(minutes=p10_min),
                "eta_med": now + timedelta(minutes=med_min),
                "eta_p90": now + timedelta(minutes=p90_min),
                "leg_mi_remaining": round(leg_mi, 2),
                "current_speed_mph": round(speed_mph, 1),
                "last_signal_s": int(age_s),
            }

    # Tier 2: DEAD_RECKONED_FROM_LIVE (5-120min stale, was moving)
    if age_s < 300 or age_s > 7200:
        return None
    if not isinstance(speed_mph, (int, float)) or speed_mph < 3.0:
        return None
    vessel_rm_at_obs, _ = lat_lon_to_rm(pos["lat"], pos["lon"])
    age_h = age_s / 3600.0
    dir_sign = -1 if direction_word == "downbound" else 1
    projected_rm_now = vessel_rm_at_obs + dir_sign * speed_mph * age_h
    if direction_word == "downbound" and projected_rm_now <= user_rm:
        return None
    if direction_word == "upbound" and projected_rm_now >= user_rm:
        return None
    leg_mi_remaining = abs(user_rm - projected_rm_now)
    if leg_mi_remaining < 0.1 or leg_mi_remaining > 25.0:
        return None
    widen = 1.0 + age_h * 0.5
    p10_min = (leg_mi_remaining / hi_mph) * 60
    p90_min = (leg_mi_remaining / lo_mph) * 60
    med_min = (leg_mi_remaining / speed_mph) * 60
    p10_min = med_min - (med_min - p10_min) * widen
    p90_min = med_min + (p90_min - med_min) * widen
    if p10_min < 0:
        p10_min = 0
    if p90_min < med_min * 1.1:
        p90_min = med_min * 1.1
    return {
        "tier": "DEAD_RECKONED_FROM_LIVE",
        "model": (f"DEAD_RECKONED_FROM_LIVE (last real observation {round(age_s/60, 1)} min ago "
                  f"at {round(speed_mph, 1)} mph, band widened x{round(widen, 2)}; "
                  f"projected forward on the assumption vessel kept moving)"),
        "confidence": "medium-high",
        "eta_p10": now + timedelta(minutes=p10_min),
        "eta_med": now + timedelta(minutes=med_min),
        "eta_p90": now + timedelta(minutes=p90_min),
        "leg_mi_remaining": round(leg_mi_remaining, 2),
        "dead_reckoned_from": {
            "last_seen_at": pos["time"].isoformat(timespec="seconds"),
            "last_seen_lat": pos["lat"], "last_seen_lon": pos["lon"],
            "last_speed_mph": round(speed_mph, 1), "age_min": round(age_s / 60, 1),
            "widening_factor": round(widen, 2), "projected_river_mile_now": round(projected_rm_now, 2),
        },
    }


async def find_best_at_lock(conn, lk_id, lk_nm, lk_rm, direction, user_rm, cabin_leg_rm,
                             model_lock_id, model_dir_key, now, walk_hop=0):
    cand = await _pull_lock(conn, lk_id, now)
    direction_word = "upbound" if direction == "U" else "downbound"
    commercial = []
    for r in cand:
        if r["direction"] != direction:
            continue
        elapsed_h = (now - r["end"]).total_seconds() / 3600.0
        if elapsed_h < 0 or elapsed_h > 24:
            continue
        vname = r["vessel"]
        if _is_local_shuttle(vname, r["end"], cand, exclude_row=r, this_direction=r["direction"]):
            continue
        if await _vessel_already_passed(conn, vname, direction_word, user_rm, now):
            continue
        commercial.append({"vessel": vname, "barges": r["barges"], "end": r["end"],
                            "elapsed_h": elapsed_h, "direction": r["direction"]})
    commercial = collapse_multicut(commercial)
    if not commercial:
        return None, f"no {'downbound' if direction=='D' else 'upbound'} commercial vessel departed {lk_nm} in the last 24h", []

    leg_rm = abs(lk_rm - user_rm)
    best = None
    all_candidates = []
    for tow in commercial:
        bc = barge_class(tow["barges"])
        base = TRANSIT_MODELS[(model_lock_id, model_dir_key, bc)]
        scaled = scale_transit(base, cabin_leg_rm=cabin_leg_rm, user_leg_rm=leg_rm)
        if walk_hop > 0:
            widen = 1.0 + 0.15 * walk_hop
            median_val = scaled["med"]
            for k in ("p10", "p90"):
                scaled[k] = median_val + (scaled[k] - median_val) * widen
        elapsed_min = tow["elapsed_h"] * 60.0
        has_live_signal = (await _latest_position(conn, tow["vessel"])) is not None
        outer_bound_min = min(scaled["p90"] * 2, 24 * 60) if has_live_signal else min(scaled["p90"] + 180, 24 * 60)
        if elapsed_min > outer_bound_min:
            continue
        med_remaining = scaled["med"] - elapsed_min
        running_late = elapsed_min > scaled["med"]
        running_very_late = elapsed_min > scaled["p90"]
        sort_key = med_remaining if med_remaining > 0 else (10_000 + elapsed_min - scaled["med"])
        live_confirmed = False
        live_rm = await _live_priority_rm(conn, tow["vessel"], direction_word, user_rm, now)
        if live_rm is not None:
            live_confirmed = True
            sort_key = -100_000 + abs(user_rm - live_rm)
        cand_entry = {**tow, "scaled": scaled, "elapsed_min": elapsed_min,
                      "med_remaining_min": med_remaining, "source_lock": lk_nm,
                      "leg_rm": leg_rm, "walk_hop": walk_hop,
                      "running_late": running_late, "running_very_late": running_very_late,
                      "live_confirmed": live_confirmed, "live_river_mile": live_rm,
                      "_sort_key": sort_key}
        all_candidates.append(cand_entry)
        if best is None or sort_key < best["_sort_key"]:
            best = cand_entry
    if best is None:
        return None, f"vessels present at {lk_nm} but all past their outer bound (2x p90 or 24h)", []
    all_candidates.sort(key=lambda c: c["_sort_key"])
    return best, None, all_candidates


async def build_eta_block(conn, best, direction_word, walk_hop, in_calibrated_range,
                           user_lat=None, user_lon=None, now=None, user_rm=None, source_rm=None):
    end = best["end"]
    eta_p10 = end + timedelta(minutes=best["scaled"]["p10"])
    eta_med = end + timedelta(minutes=best["scaled"]["med"])
    eta_p90 = end + timedelta(minutes=best["scaled"]["p90"])
    confidence = "medium"
    detail_parts = [f"Scaled from cabin-correlation.json; leg = {best['leg_rm']:.1f} river-miles from {best['source_lock']}"]
    if walk_hop > 0:
        confidence = "low"
        detail_parts.append(f"Walked {walk_hop} lock(s) further than nearest bracket; widened bands.")
    if not in_calibrated_range:
        confidence = "low"
    status = "on-track"
    if best["running_very_late"]:
        status = "very-late"
        detail_parts.append("Elapsed past 80% band — likely stopped, fleeting, or waiting on a queue.")
    elif best["running_late"]:
        status = "late"
        detail_parts.append("Elapsed past median — running slower than usual, expect soon.")

    block = {
        "vessel": best["vessel"], "barges": best["barges"], "direction": direction_word,
        "source_lock": best["source_lock"], "departed_lock_at": end.isoformat(timespec="seconds"),
        "eta": {"p10": eta_p10.isoformat(timespec="seconds"), "med": eta_med.isoformat(timespec="seconds"),
                "p90": eta_p90.isoformat(timespec="seconds")},
        "source": "lock-feed", "confidence": confidence, "status": status, "walk_hop": walk_hop,
        "beyond_calibrated_range": walk_hop > 0 or not in_calibrated_range,
        "detail": " ".join(detail_parts),
    }
    if user_lat is not None and user_lon is not None and now is not None:
        live = await build_live_estimate(conn, best["vessel"], user_lat, user_lon, direction_word, now, user_rm=user_rm)
        if live is not None:
            block["live_estimate"] = live

    NEAR_ZERO_STOP_RATE = 0.05
    MIN_SAMPLES_TO_TRUST_NEAR_ZERO = 3
    if user_rm is not None and source_rm is not None:
        may_stop, note = _rw_terminal_touch(source_rm, user_rm)
        if may_stop:
            rate_info = _rw_stop_rate_for(best["vessel"])
            is_confident_near_zero = (
                rate_info is not None
                and rate_info.get("source", "").startswith("per-vessel")
                and (rate_info.get("n_total_events") or 0) >= MIN_SAMPLES_TO_TRUST_NEAR_ZERO
                and (rate_info.get("rate") or 0) <= NEAR_ZERO_STOP_RATE
            )
            if not is_confident_near_zero:
                block["may_stop_at_terminal"] = True
                block["terminal_stop_note"] = note
                block["red_wing_stop_rate"] = rate_info

    if user_rm is not None and now is not None:
        av = await compute_antenna_verified_bands(conn, best["vessel"], direction_word, user_rm, now)
        if av is not None:
            block["eta"] = {"p10": av["eta_p10"].isoformat(timespec="seconds"),
                             "med": av["eta_med"].isoformat(timespec="seconds"),
                             "p90": av["eta_p90"].isoformat(timespec="seconds")}
            block["model"] = av["model"]
            block["confidence"] = av["confidence"]
            block["source"] = "antenna-verified"
            if "dead_reckoned_from" in av:
                block["dead_reckoned_from"] = av["dead_reckoned_from"]
            block["leg_mi_remaining"] = av.get("leg_mi_remaining")
    return block


async def build(conn, lat, lon):
    now = datetime.now(timezone.utc)
    user_rm, dist_to_anchor_km = lat_lon_to_rm(lat, lon)
    in_range = (CAL_MIN_RM <= user_rm <= CAL_MAX_RM) and (dist_to_anchor_km <= MAX_OFF_CORRIDOR_KM)

    result = {
        "user_point": {"lat": lat, "lon": lon, "river_mile": round(user_rm, 2),
                        "distance_to_nearest_anchor_km": round(dist_to_anchor_km, 2),
                        "in_calibrated_range": in_range},
        "generated_at": now.isoformat(timespec="seconds"),
        "next_from_upstream": None, "next_from_downstream": None,
        "next_2_from_upstream": [], "next_2_from_downstream": [],
        "quiet_flags": {},
        "honest_caveats": [
            "Windows are 80% confidence bands from real historical transits past the cabin anchor point.",
            "SB predictions have longer tails due to Red Wing terminal fleeting.",
            "When a `live_estimate` block is present, that's a TIGHT educated guess "
            "based on live AIS position + speed + heading (only when the boat is "
            "actually within our reception right now). Wide statistical window still "
            "shown as the honest fallback -- boats slow, fleet barges, stop en route.",
        ],
    }

    if not in_range:
        result["honest_caveats"].append(
            f"User point at RM {user_rm:.1f} is OUTSIDE our calibrated range "
            f"(RM {CAL_MIN_RM}-{CAL_MAX_RM}). Predictions suppressed. "
            "Coming soon as we grow the corridor dataset."
        )
        return result

    upstream_chain = sorted([(lk, nm, rm) for lk, nm, rm in LOCKS if rm > user_rm], key=lambda x: x[2])
    downstream_chain = sorted([(lk, nm, rm) for lk, nm, rm in LOCKS if rm < user_rm], key=lambda x: -x[2])

    upstream_notes = []
    for hop, (lk_id, lk_nm, lk_rm) in enumerate(upstream_chain[:1 + MAX_WALK_HOPS]):
        best, reason, all_cands = await find_best_at_lock(
            conn, lk_id, lk_nm, lk_rm, direction="D", user_rm=user_rm, cabin_leg_rm=22.6,
            model_lock_id="03", model_dir_key="down_to_cabin", now=now, walk_hop=hop,
        )
        if best:
            blocks = [await build_eta_block(conn, c, "downbound", hop, in_range, user_lat=lat, user_lon=lon,
                                             now=now, user_rm=user_rm, source_rm=lk_rm)
                      for c in all_cands[:MAX_BOATS_PER_DIRECTION]]
            result["next_from_upstream"] = blocks[0]
            result["next_2_from_upstream"] = blocks
            break
        upstream_notes.append(f"{lk_nm}: {reason}")
    else:
        result["quiet_flags"]["upstream"] = "; ".join(upstream_notes) if upstream_notes else "no upstream lock"

    downstream_notes = []
    for hop, (lk_id, lk_nm, lk_rm) in enumerate(downstream_chain[:1 + MAX_WALK_HOPS]):
        best, reason, all_cands = await find_best_at_lock(
            conn, lk_id, lk_nm, lk_rm, direction="U", user_rm=user_rm, cabin_leg_rm=22.5,
            model_lock_id="04", model_dir_key="up_to_cabin", now=now, walk_hop=hop,
        )
        if best:
            blocks = [await build_eta_block(conn, c, "upbound", hop, in_range, user_lat=lat, user_lon=lon,
                                             now=now, user_rm=user_rm, source_rm=lk_rm)
                      for c in all_cands[:MAX_BOATS_PER_DIRECTION]]
            result["next_from_downstream"] = blocks[0]
            result["next_2_from_downstream"] = blocks
            break
        downstream_notes.append(f"{lk_nm}: {reason}")
    else:
        result["quiet_flags"]["downstream"] = "; ".join(downstream_notes) if downstream_notes else "no downstream lock"

    return result
