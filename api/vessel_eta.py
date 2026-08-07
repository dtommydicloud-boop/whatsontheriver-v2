"""vessel_eta.py -- given a specific vessel (name + current lat/lon [+ COG]),
project its ETA to the NEXT waypoint in its direction of travel, ported from
Reed's vessel-eta.py (next-vessel-server, lake-mac) against v2's Postgres
schema.

Different from /next-vessel:
  - next-vessel: for an arbitrary map point, when's the next boat arrive?
  - vessel-eta:  for THIS specific vessel we just saw, when does IT arrive
                 at the next town/lock?

Same cabin-anchored statistical model as next_vessel.py -- this module
imports that one directly (lat_lon_to_rm, _clean_ais_name, barge_class,
scale_transit, TRANSIT_MODELS(_DIRECT), _latest_position, _rw_stop_rate_for)
rather than duplicating them, mirroring the source's own
`nv = SourceFileLoader(...next-vessel.py...)` import.

Data access rebuilt against lock_status_observation (most recent lockage
across the 3 covered locks, for barge count + fallback direction) and
vessel_position (live/dead-reckoned anchor tiers) -- see next_vessel.py's
module docstring for the fuller rationale (same table, same simplifications
apply here).

Honest gap, not silently faked: same as next_vessel.py, the per-vessel Red
Wing stop-rate proxy isn't ported (lives only on lake-mac disk, nightly-
refreshed, not static bundleable data) -- red_wing_stop_rate stays None,
may_stop_at_terminal still fires as the safe default.
"""
from datetime import datetime, timedelta, timezone

from api import next_vessel as nv

WAYPOINTS = [
    ("St Paul",       847.6, "lock"),
    ("Hastings",      815.2, "lock"),
    ("Prescott",      811.4, "town"),
    ("Welch Lock",    796.9, "lock"),
    ("Red Wing",      791.0, "town"),
    ("Frontenac",     782.0, "town"),
    ("Lake City",     773.3, "town"),
    ("Reads Landing", 767.5, "landing"),
    ("Wabasha",       760.0, "town"),
    ("Alma",          752.8, "lock"),
    ("Minneiska",     738.1, "lock"),
]

RED_WING_TERMINAL_RM = (789.0, 791.3)
RED_WING_TERMINAL_NAMES = ("Red Wing", "Welch Lock")
RED_WING_LOCK_RM = 796.9
RED_WING_BRIDGE_RM = 791.0

SPEED_HINT = {"downbound": (5.5, 7.5), "upbound": (4.0, 5.5)}

DEAD_RECKON_MIN_AGE_S = 300
DEAD_RECKON_MAX_AGE_S = 120 * 60
DEAD_RECKON_WIDEN_PER_HOUR = 0.5

ETA_LOCKS = [("02", "Hastings"), ("03", "Red Wing"), ("04", "Alma")]


def _dir_from_cog(cog):
    if cog is None or not isinstance(cog, (int, float)):
        return None
    c = cog % 360
    if c >= 315 or c <= 45:
        return "upbound"
    if 135 <= c <= 225:
        return "downbound"
    return None


def _next_waypoint(current_rm, direction):
    if direction == "upbound":
        candidates = [w for w in WAYPOINTS if w[1] > current_rm]
        return min(candidates, key=lambda w: w[1]) if candidates else None
    if direction == "downbound":
        candidates = [w for w in WAYPOINTS if w[1] < current_rm]
        return max(candidates, key=lambda w: w[1]) if candidates else None
    return None


def _model_key_for_direction(direction):
    if direction == "upbound":
        return ("04", "up_to_cabin", 22.5)
    if direction == "downbound":
        return ("03", "down_to_cabin", 22.6)
    return None


async def _find_last_lockage(conn, vessel_name):
    """Most recent lockage for this vessel across the 3 covered locks --
    same two-tier exact/fallback name match as next_vessel._latest_position,
    since the caller-supplied name may be AIS-punctuated while
    lock_status_observation carries LPMS-stripped names."""
    target = nv._clean_ais_name(vessel_name)
    if not target:
        return None
    lock_ids = [lk for lk, _ in ETA_LOCKS]
    lock_name_by_id = {lk: nm for lk, nm in ETA_LOCKS}
    rows = await conn.fetch(
        """
        SELECT lock_id, vessel_name, direction, barges, eol_at
        FROM lock_status_observation
        WHERE lock_id = ANY($1::text[])
          AND eol_at IS NOT NULL
          AND upper(trim(vessel_name)) = upper(trim($2))
        ORDER BY eol_at DESC LIMIT 5
        """,
        lock_ids, vessel_name, timeout=10,
    )
    for r in rows:
        if nv._clean_ais_name(r["vessel_name"]) == target:
            return {"lock": lock_name_by_id[r["lock_id"]], "lock_id": r["lock_id"],
                    "end_of_lockage": r["eol_at"], "direction": r["direction"],
                    "barges": r["barges"] or 0}
    rows = await conn.fetch(
        """
        SELECT lock_id, vessel_name, direction, barges, eol_at
        FROM lock_status_observation
        WHERE lock_id = ANY($1::text[])
          AND eol_at IS NOT NULL
          AND upper(regexp_replace(vessel_name, '[^A-Za-z0-9 ]', '', 'g'))
              LIKE '%' || upper(regexp_replace($2, '[^A-Za-z0-9 ]', '', 'g')) || '%'
        ORDER BY eol_at DESC LIMIT 20
        """,
        lock_ids, vessel_name, timeout=10,
    )
    for r in rows:
        if nv._clean_ais_name(r["vessel_name"]) == target:
            return {"lock": lock_name_by_id[r["lock_id"]], "lock_id": r["lock_id"],
                    "end_of_lockage": r["eol_at"], "direction": r["direction"],
                    "barges": r["barges"] or 0}
    return None


async def _is_commercial_tow(conn, vessel_name):
    """Real ask (Tom, source's own note): the Red Wing terminal-stop
    uncertainty only makes sense for actual cargo tows that might divert
    into the grain/Enstructure terminals -- a Coast Guard cutter has zero
    reason to stop there. Default True (commercial, cautious) if she's not
    currently in vessel_position at all."""
    pos = await nv._latest_position(conn, vessel_name)
    if pos is None:
        return True
    return bool(pos["is_commercial"])


async def _try_live_anchor(conn, vessel, direction, wp_rm, leg_rm, now):
    if not vessel or direction not in SPEED_HINT:
        return None
    pos = await nv._latest_position(conn, vessel)
    if pos is None:
        return None
    age_s = (now - pos["time"]).total_seconds()
    if age_s > 300:
        return None
    speed_mph = pos["sog_mph"]
    if not isinstance(speed_mph, (int, float)) or speed_mph < 3.0:
        return None
    lo_mph, hi_mph = SPEED_HINT[direction]
    p10_min = (leg_rm / hi_mph) * 60
    p90_min = (leg_rm / lo_mph) * 60
    med_min = (leg_rm / speed_mph) * 60
    if p10_min > med_min:
        p10_min = med_min * 0.9
    if p90_min < med_min:
        p90_min = med_min * 1.1
    return {"scaled": {"p10": p10_min, "med": med_min, "p90": p90_min},
            "speed_now_mph": round(speed_mph, 1), "speed_range_mph": [lo_mph, hi_mph],
            "last_signal_s": int(age_s)}


async def _try_dead_reckoned(conn, vessel, direction, leg_rm, now):
    if not vessel or direction not in SPEED_HINT:
        return None
    pos = await nv._latest_position(conn, vessel)
    if pos is None:
        return None
    age_s = (now - pos["time"]).total_seconds()
    if age_s < DEAD_RECKON_MIN_AGE_S or age_s > DEAD_RECKON_MAX_AGE_S:
        return None
    speed_mph = pos["sog_mph"]
    if not isinstance(speed_mph, (int, float)) or speed_mph < 3.0:
        return None
    lo_mph, hi_mph = SPEED_HINT[direction]
    age_h = age_s / 3600.0
    widen = 1.0 + age_h * DEAD_RECKON_WIDEN_PER_HOUR
    p10_min = (leg_rm / hi_mph) * 60
    p90_min = (leg_rm / lo_mph) * 60
    med_min = (leg_rm / speed_mph) * 60
    p10_min = med_min - (med_min - p10_min) * widen
    p90_min = med_min + (p90_min - med_min) * widen
    if p10_min < 0:
        p10_min = 0
    if p90_min < med_min * 1.1:
        p90_min = med_min * 1.1
    return {"scaled": {"p10": p10_min, "med": med_min, "p90": p90_min},
            "last_seen_at": pos["time"].isoformat(timespec="seconds"),
            "last_seen_at_lat": pos["lat"], "last_seen_at_lon": pos["lon"],
            "last_speed_mph": round(speed_mph, 1), "age_min_since_last_signal": round(age_s / 60, 1),
            "widening_factor": round(widen, 2)}


async def _project_leg(conn, vessel, direction, user_rm, wp_name, wp_rm, wp_type,
                        lock_id, model_dir_key, cabin_leg_rm, bc, now):
    leg_rm = abs(wp_rm - user_rm)

    may_stop_at_terminal = False
    stop_reason = None
    if await _is_commercial_tow(conn, vessel):
        if wp_name in RED_WING_TERMINAL_NAMES:
            may_stop_at_terminal = True
            stop_reason = ("Destination is Red Wing terminal (Enstructure / Red Wing Grain / "
                           "CD Terminal). Time-to-arrival, not time-through.")
        else:
            route_min, route_max = min(user_rm, wp_rm), max(user_rm, wp_rm)
            if route_min < RED_WING_TERMINAL_RM[1] and route_max > RED_WING_TERMINAL_RM[0]:
                may_stop_at_terminal = True
                stop_reason = "Route passes Red Wing terminal zone; vessel may pause there."

    red_wing_stop_rate = nv._rw_stop_rate_for(vessel) if may_stop_at_terminal else None

    live_anchor = await _try_live_anchor(conn, vessel, direction, wp_rm, leg_rm, now)
    dead_reckoned = None
    tier_extra = {}
    if live_anchor is not None and not may_stop_at_terminal:
        scaled = live_anchor["scaled"]
        model_source = ("LIVE_UNDERWAY (distance/speed band, "
                        f"speed_now={live_anchor['speed_now_mph']:.1f} mph, "
                        f"directional range {live_anchor['speed_range_mph']} mph, "
                        f"last_signal {live_anchor['last_signal_s']}s ago)")
    else:
        if not may_stop_at_terminal:
            dead_reckoned = await _try_dead_reckoned(conn, vessel, direction, leg_rm, now)
        if dead_reckoned is not None:
            scaled = dead_reckoned["scaled"]
            model_source = (
                f"DEAD_RECKONED_FROM_LIVE (last real observation {dead_reckoned['age_min_since_last_signal']} min ago "
                f"at {dead_reckoned['last_speed_mph']} mph, band widened x{dead_reckoned['widening_factor']}; "
                f"projected forward on the assumption vessel kept moving)"
            )
            tier_extra["dead_reckoned_from"] = {
                "last_seen_at": dead_reckoned["last_seen_at"],
                "last_seen_lat": dead_reckoned["last_seen_at_lat"],
                "last_seen_lon": dead_reckoned["last_seen_at_lon"],
                "last_speed_mph": dead_reckoned["last_speed_mph"],
                "age_min": dead_reckoned["age_min_since_last_signal"],
                "widening_factor": dead_reckoned["widening_factor"],
            }
        else:
            model_source = "TRANSIT_MODELS_DIRECT (fleeters stripped, non-terminal leg)"
            base = nv.TRANSIT_MODELS_DIRECT.get((lock_id, model_dir_key, bc)) if not may_stop_at_terminal else None
            if base is None:
                base = nv.TRANSIT_MODELS.get((lock_id, model_dir_key, bc))
                model_source = "TRANSIT_MODELS (full sample, includes fleeting tail)"
            if not base:
                return None
            scaled = nv.scale_transit(base, cabin_leg_rm=cabin_leg_rm, user_leg_rm=leg_rm)

    eta_p10 = now + timedelta(minutes=scaled["p10"])
    eta_med = now + timedelta(minutes=scaled["med"])
    eta_p90 = now + timedelta(minutes=scaled["p90"])

    return {
        "name": wp_name, "type": wp_type, "river_mile": wp_rm, "leg_river_miles": round(leg_rm, 2),
        "eta": {"p10": eta_p10.isoformat(timespec="seconds"), "med": eta_med.isoformat(timespec="seconds"),
                "p90": eta_p90.isoformat(timespec="seconds")},
        "model": model_source,
        "confidence": ("high" if model_source.startswith("LIVE_UNDERWAY")
                       else "medium-high" if model_source.startswith("DEAD_RECKONED_FROM_LIVE")
                       else "medium"),
        "may_stop_at_terminal": may_stop_at_terminal,
        "terminal_stop_note": stop_reason,
        "red_wing_stop_rate": red_wing_stop_rate,
        **tier_extra,
        "detail": (f"{vessel} projected to reach {wp_name} ({wp_type}, RM {wp_rm}) "
                   f"in {leg_rm:.1f} river-miles. Bands scaled from cabin-anchored "
                   f"transit distribution ({lock_id}/{model_dir_key}/{bc}, "
                   f"cabin_leg={cabin_leg_rm} mi)."),
    }


async def build(conn, vessel, lat, lon, cog=None):
    now = datetime.now(timezone.utc)
    user_rm, dist_to_anchor_km = nv.lat_lon_to_rm(lat, lon)

    lockage = await _find_last_lockage(conn, vessel)
    barges = (lockage or {}).get("barges", 0)
    lockage_dir = (lockage or {}).get("direction")
    lockage_dir_word = {"U": "upbound", "D": "downbound"}.get(lockage_dir)

    direction = _dir_from_cog(cog) or lockage_dir_word
    if direction is None:
        return {
            "vessel": vessel, "current": {"lat": lat, "lon": lon, "river_mile": round(user_rm, 2)},
            "error": "cannot infer direction: no valid COG and no recent lockage for this vessel",
            "generated_at": now.isoformat(timespec="seconds"),
        }

    wp = _next_waypoint(user_rm, direction)
    if wp is None:
        return {
            "vessel": vessel, "current": {"lat": lat, "lon": lon, "river_mile": round(user_rm, 2)},
            "direction": direction, "error": "no next waypoint in that direction within our covered reach",
            "generated_at": now.isoformat(timespec="seconds"),
        }
    wp_name, wp_rm, wp_type = wp

    m = _model_key_for_direction(direction)
    if not m:
        return {"vessel": vessel, "error": "no model for direction", "generated_at": now.isoformat(timespec="seconds")}
    lock_id, model_dir_key, cabin_leg_rm = m
    bc = nv.barge_class(barges)

    leg = await _project_leg(conn, vessel, direction, user_rm, wp_name, wp_rm, wp_type,
                              lock_id, model_dir_key, cabin_leg_rm, bc, now)
    if leg is None:
        return {"vessel": vessel, "error": f"no distribution for ({lock_id},{model_dir_key},{bc})",
                "generated_at": now.isoformat(timespec="seconds")}

    red_wing_checkpoints = {}
    ahead = (lambda rm: rm > user_rm) if direction == "upbound" else (lambda rm: rm < user_rm)
    if ahead(RED_WING_BRIDGE_RM):
        bridge_leg = await _project_leg(conn, vessel, direction, user_rm, "Red Wing", RED_WING_BRIDGE_RM, "town",
                                         lock_id, model_dir_key, cabin_leg_rm, bc, now)
        if bridge_leg:
            red_wing_checkpoints["bridge"] = bridge_leg
    if ahead(RED_WING_LOCK_RM):
        lock_leg = await _project_leg(conn, vessel, direction, user_rm, "Welch Lock", RED_WING_LOCK_RM, "lock",
                                       lock_id, model_dir_key, cabin_leg_rm, bc, now)
        if lock_leg:
            red_wing_checkpoints["lock"] = lock_leg

    return {
        "vessel": vessel,
        "current": {"lat": lat, "lon": lon, "river_mile": round(user_rm, 2), "cog_deg": cog},
        "direction": direction, "barges": barges, "barge_class": bc,
        "last_lock_seen": (
            {"lock": lockage["lock"], "endOfLockage": lockage["end_of_lockage"].isoformat(timespec="seconds")}
            if lockage else None
        ),
        "next_waypoint": {"name": leg["name"], "type": leg["type"], "river_mile": leg["river_mile"],
                           "leg_river_miles": leg["leg_river_miles"]},
        "eta": leg["eta"],
        "generated_at": now.isoformat(timespec="seconds"),
        "source": "next-vessel statistical model scaled to vessel's remaining leg",
        "model": leg["model"], "confidence": leg["confidence"],
        "may_stop_at_terminal": leg["may_stop_at_terminal"],
        "terminal_stop_note": leg["terminal_stop_note"],
        "red_wing_stop_rate": leg["red_wing_stop_rate"],
        "dead_reckoned_from": leg.get("dead_reckoned_from"),
        "detail": leg["detail"],
        "red_wing_checkpoints": red_wing_checkpoints or None,
        "honest_caveats": [
            "ETA is a statistical projection using the same transit model as /next-vessel. "
            "Actual arrival depends on speed, wind, current, and unplanned stops.",
            "Fleeting stops (Red Wing terminal complex especially) are the largest source "
            "of positive error; when `may_stop_at_terminal=true`, treat the eta.med as "
            "'best case if they don't stop' rather than a hard arrival prediction.",
            "'Red Wing' (town/bridge, RM 791) and 'Welch Lock' (RM 796.9) are two distinct "
            "checkpoints ~5-6 river-mi apart -- see red_wing_checkpoints for both ETAs "
            "separately when relevant, don't treat them as the same arrival time.",
        ],
    }
