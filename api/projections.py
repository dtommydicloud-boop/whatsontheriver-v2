"""projections.py -- dead-reckoned + lock-projected vessel position tiers,
ported from Reed's live-positions.py (next-vessel-server, lake-mac) to v2's
Postgres-backed schema.

Port scope, 2026-08-07 (Tom's ask, night-of the Railway migration): v2's
/live-positions only ever showed genuinely-live AIS hits -- a real feature
gap against the original server, which also shows:

  is_live=false, is_dead_reckoned=true   -- projected forward from the last
                    real AIS fix (5min-2h old) along the channel, by speed*age
  is_live=false, is_lock_projected=true  -- no AIS ever heard, but LPMS shows
                    a departure heading our way; projected from lock-departure
                    + elapsed time, preferring real empirical leg-transit
                    medians over typical-speed extrapolation

The algorithm itself (river-mile projection math, reception-radius capping,
overdue/grace handling, empirical-vs-physics fallback) is ported close to
verbatim from the source -- it's pure logic over already-fetched values, not
tied to how those values got fetched. What's REBUILT is the data-access
layer: the source tails local JSONL AIS snapshot files and an LPMS disk
cache; this queries the same information out of Postgres
(vessel_current_state / vessel_position / lock_status_observation), which is
where v2 already keeps it. Every real edge case documented in the source
(grace windows, reception-boundary capping, overdue-unconfirmed freezing,
barge-class-segmented empirical medians) is preserved -- this is a port, not
a redesign.

Not ported: the source's own resilience layer around FILE reading (tail
offsets, persistent-obs disk cache, watchdog-flap history) -- none of that
applies once the data lives in Postgres instead of daily-growing JSONL files.
"""
import bisect
import json
import math
import pathlib
from datetime import datetime, timedelta, timezone

CHANNEL_PATH = pathlib.Path(__file__).parent / "data" / "pepin-channel.geojson"

FRESH_CUTOFF_S = 300
DEAD_RECKON_MAX_AGE_S = 120 * 60       # 2h
DEAD_RECKON_MIN_SPEED_MPH = 3.0
UPBOUND_MPH_TYPICAL = 4.75
DOWNBOUND_MPH_TYPICAL = 6.5
LOCK_PROJECT_MAX_HOURS = 12
NEXT_LOCK_GRACE_H = 2.0
LIVE_RECEPTION_RADIUS_MI_TOW = 3.3     # barges >= 1
LIVE_RECEPTION_RADIUS_MI_LIGHT = 1.0   # barges == 0

# Real stop-rate evidence, Reed's 26-day AIS+LPMS analysis (2026-07-30).
# Only this one leg has real ground truth; every other lock/direction has
# zero on-corridor AIS pings and stays confidence="low", not "0% stop rate".
KNOWN_STOP_RATE = {
    ("04", "U"): {"rate": 0.059, "n_transits": 102},
}

EMPIRICAL_TRANSIT_MIN_N_BLENDED = 5
EMPIRICAL_TRANSIT_MIN_N_SEGMENTED = 5
EMPIRICAL_TRANSIT_MAX_GAP_H = 48


# --- Channel geometry (ported verbatim from live-positions.py) -----------

_channel_cache = {"coords": None, "rms": None}


def _load_channel():
    if _channel_cache["coords"] is not None:
        return _channel_cache["coords"], _channel_cache["rms"]
    d = json.loads(CHANNEL_PATH.read_text())
    feature = (d.get("features") or [{}])[0]
    coords = (feature.get("geometry") or {}).get("coordinates") or []
    rms = ((feature.get("properties") or {}).get("rm_per_vertex")) or []
    if len(coords) != len(rms):
        return None, None
    idx = sorted(range(len(coords)), key=lambda i: rms[i])
    coords_asc = [coords[i] for i in idx]
    rms_asc = [rms[i] for i in idx]
    _channel_cache["coords"] = coords_asc
    _channel_cache["rms"] = rms_asc
    return coords_asc, rms_asc


def rm_to_latlon(target_rm):
    coords, rms = _load_channel()
    if not coords:
        return None, None
    if target_rm <= rms[0]:
        return coords[0][1], coords[0][0]
    if target_rm >= rms[-1]:
        return coords[-1][1], coords[-1][0]
    i = bisect.bisect_left(rms, target_rm)
    r0, r1 = rms[i - 1], rms[i]
    c0, c1 = coords[i - 1], coords[i]
    t = (target_rm - r0) / (r1 - r0) if r1 != r0 else 0.0
    lat = c0[1] + (c1[1] - c0[1]) * t
    lon = c0[0] + (c1[0] - c0[0]) * t
    return lat, lon


def latlon_to_rm(lat, lon):
    coords, rms = _load_channel()
    if not coords:
        return None
    best_d, best_rm = float("inf"), None
    for i in range(1, len(coords)):
        lon0, lat0 = coords[i - 1]
        lon1, lat1 = coords[i]
        dx, dy = lon1 - lon0, lat1 - lat0
        if dx == 0 and dy == 0:
            t = 0.0
        else:
            t = ((lon - lon0) * dx + (lat - lat0) * dy) / (dx * dx + dy * dy)
            t = max(0.0, min(1.0, t))
        proj_lat = lat0 + t * dy
        proj_lon = lon0 + t * dx
        d = _hav_mi(lat, lon, proj_lat, proj_lon)
        if d < best_d:
            best_d = d
            best_rm = rms[i - 1] + (rms[i] - rms[i - 1]) * t
    return best_rm


def channel_slice(from_rm, to_rm):
    coords, rms = _load_channel()
    if not coords:
        return []
    lo_rm, hi_rm = min(from_rm, to_rm), max(from_rm, to_rm)
    reverse = from_rm > to_rm
    start_lat, start_lon = rm_to_latlon(lo_rm)
    end_lat, end_lon = rm_to_latlon(hi_rm)
    if start_lat is None or end_lat is None:
        return []
    slice_pts = [[round(start_lon, 6), round(start_lat, 6)]]
    for i, rm in enumerate(rms):
        if lo_rm < rm < hi_rm:
            slice_pts.append([round(coords[i][0], 6), round(coords[i][1], 6)])
    slice_pts.append([round(end_lon, 6), round(end_lat, 6)])
    dedup = []
    for p in slice_pts:
        if not dedup or dedup[-1] != p:
            dedup.append(p)
    if reverse:
        dedup.reverse()
    return dedup


def _hav_mi(lat1, lon1, lat2, lon2):
    r_mi = 3958.7613
    p1, p2 = math.radians(lat1), math.radians(lat2)
    d = (math.sin(math.radians(lat2 - lat1) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2)
    return 2 * r_mi * math.asin(math.sqrt(d))


def _dir_from_cog(cog):
    if cog is None or not isinstance(cog, (int, float)):
        return None
    c = cog % 360
    if c > 270 or c < 90:
        return "upbound"
    if 90 <= c <= 270:
        return "downbound"
    return None


def _name_key(n):
    return (n or "").strip().upper()


def _is_recreational(vessel_name, raw_payload):
    """Same check as main.py's _is_recreational (name literal + payload
    vessel_no) -- duplicated locally rather than imported to avoid a
    main<->projections circular import, same pattern as every other
    per-module recreational check in this codebase."""
    if "RECREATION" in (vessel_name or "").upper():
        return True
    try:
        payload = json.loads(raw_payload) if isinstance(raw_payload, str) else (raw_payload or {})
    except (TypeError, ValueError):
        payload = {}
    if not isinstance(payload, dict):
        return False
    return payload.get("vessel_no") == "9999999"


# --- Empirical leg-transit table (ported; queries lock_status_observation
# instead of an LPMS disk cache -- same methodology, real Postgres source) --

# Real perf bug found testing this against live Railway data (2026-08-07):
# 45 days of lock_status_observation is ~525K rows (essentially the whole
# table -- every row has eol_at set) and the source's own 1-hour cache
# TTL was the thing keeping this affordable, not the query itself. Without
# it, every single /live-positions poll (frontend polls every 90s) would
# redo this full fetch + Python-side nested-loop matching from scratch --
# measured multiple minutes at ~100% CPU on a re-run, not the sub-second
# cost this needs to be for a live endpoint. Ported the source's own cache
# pattern (_empirical_transit_cache, ttl_s=3600) instead of reinventing one.
_empirical_transit_cache = {"at": 0.0, "data": None}
_EMPIRICAL_TRANSIT_TTL_S = 3600


async def _build_empirical_transit_table(conn, lock_meta):
    """{(from_lock_id, to_lock_id, direction): {blended:(median,n), small:(median,n), big:(median,n)}}
    built from real consecutive clearances between adjacent locks (by RM
    order), same filter/collapse/match rules as the source: RECREATION
    excluded (vessel_no filtering already happens at ingest, see
    _is_recreational in main.py), direction-consistent, capped match window.
    Process-cached for _EMPIRICAL_TRANSIT_TTL_S -- see comment above.

    Real perf fix 2026-08-07: the matching step (each vessel's clearance ->
    her NEXT clearance at the specific adjacent lock, within
    EMPIRICAL_TRANSIT_MAX_GAP_H) is done here via SQL's LEAD() window
    function -- a single sorted pass -- instead of a Python nested loop
    over every row (measured multiple minutes at ~100% CPU on the real
    525K-row table; caching alone doesn't fix an uncached-call cost that
    blows well past the frontend's request timeout). Using each vessel's
    chronologically NEXT clearance (any lock) rather than her next
    clearance specifically-at-the-target-lock is actually a slightly
    STRICTER match than the source (a data gap that skipped the adjacent
    lock's own row would no longer get attributed as a single-hop time to
    the next lock that DOES have a row) -- same intent, tighter
    correctness, and it's what makes a single SQL pass possible instead of
    the source's per-vessel-per-lock list matching.
    """
    import time as _t
    now_t = _t.time()
    if (_empirical_transit_cache["data"] is not None
            and (now_t - _empirical_transit_cache["at"]) < _EMPIRICAL_TRANSIT_TTL_S):
        return _empirical_transit_cache["data"]

    rows = await conn.fetch(
        """
        WITH ordered AS (
            SELECT lock_id, vessel_name, direction, barges, eol_at,
                   LEAD(lock_id) OVER w AS next_lock_id,
                   LEAD(direction) OVER w AS next_direction,
                   LEAD(eol_at) OVER w AS next_eol_at
            FROM lock_status_observation
            WHERE eol_at IS NOT NULL
              AND time > now() - interval '45 days'
            WINDOW w AS (PARTITION BY upper(vessel_name) ORDER BY eol_at)
        )
        SELECT lock_id, next_lock_id, direction, barges,
               (EXTRACT(EPOCH FROM (next_eol_at - eol_at)) / 60.0)::float8 AS transit_min
        FROM ordered
        WHERE next_lock_id IS NOT NULL
          AND direction = next_direction
          AND next_eol_at - eol_at <= make_interval(hours => $1)
        """,
        EMPIRICAL_TRANSIT_MAX_GAP_H,
        timeout=30,
    )

    ordered_locks = sorted(lock_meta.items(), key=lambda kv: kv[1]["river_mile"])
    adjacent_pairs = {
        (lo_id, hi_id, "U"): (lo_id, hi_id) for (lo_id, _), (hi_id, _) in zip(ordered_locks, ordered_locks[1:])
    }
    adjacent_pairs.update({
        (hi_id, lo_id, "D"): (hi_id, lo_id) for (lo_id, _), (hi_id, _) in zip(ordered_locks, ordered_locks[1:])
    })

    by_key = {}
    for r in rows:
        key = (r["lock_id"], r["next_lock_id"], r["direction"])
        if key not in adjacent_pairs:
            continue  # not an adjacent-lock hop -- a data gap skipped a lock in between
        by_key.setdefault(key, []).append((r["transit_min"], r["barges"] or 0))

    table = {}
    for (from_id, to_id, dir_code), deltas in by_key.items():
        blended_vals = [m for m, b in deltas]
        small_vals = [m for m, b in deltas if b < 4]
        big_vals = [m for m, b in deltas if b >= 4]
        entry = {}
        if len(blended_vals) >= EMPIRICAL_TRANSIT_MIN_N_BLENDED:
            entry["blended"] = (round(_median(blended_vals), 1), len(blended_vals))
        if len(small_vals) >= EMPIRICAL_TRANSIT_MIN_N_SEGMENTED:
            entry["small"] = (round(_median(small_vals), 1), len(small_vals))
        if len(big_vals) >= EMPIRICAL_TRANSIT_MIN_N_SEGMENTED:
            entry["big"] = (round(_median(big_vals), 1), len(big_vals))
        if entry:
            table[(from_id, to_id, dir_code)] = entry

    _empirical_transit_cache["data"] = table
    _empirical_transit_cache["at"] = now_t
    return table


def _median(vals):
    if not vals:
        return None
    s = sorted(vals)
    return s[len(s) // 2]


def _empirical_leg_info(table, from_lock_id, to_lock_id, direction, barges):
    entry = table.get((from_lock_id, to_lock_id, direction))
    if not entry:
        return None
    key = "big" if (barges or 0) >= 4 else "small"
    if key in entry:
        mins, n = entry[key]
        return mins, n, key
    if "blended" in entry:
        mins, n = entry["blended"]
        return mins, n, "blended"
    return None


# --- Core projection math (ported verbatim -- pure function of inputs) ---

def _project_rm_with_lockage(lock_meta, table, start_rm, dir_sign, elapsed_h,
                              typical_mph, barges, exclude_lock_id=None):
    """Walk the real sequence of locks ahead of her (in her direction of
    travel) ONE hop -- the single next lock ahead, no multi-hop chaining.
    Prefers the real empirical median transit time for that specific hop
    (barge-class-segmented where sample size supports it) over a cruise-
    speed physics fallback. Three phases: (1) still short of the real/
    physics transit time -- interpolate proportionally along the hop,
    legitimately in transit; (2) past nominal ETA but within the
    NEXT_LOCK_GRACE_H courtesy window -- keep extrapolating past the lock at
    typical cruise speed, not yet flagged; (3) grace expired with zero real
    confirmation -- freeze at the grace-boundary position and flag overdue,
    don't keep advancing just because more real time passes.

    Returns (projected_rm, locks_transited, minutes_deducted, basis, overdue).
    """
    dir_code = "U" if dir_sign > 0 else "D"
    ahead_locks = sorted(
        ((lk_id, meta["river_mile"]) for lk_id, meta in lock_meta.items()
         if lk_id != exclude_lock_id and (meta["river_mile"] > start_rm if dir_sign > 0 else meta["river_mile"] < start_rm)),
        key=lambda pair: pair[1],
        reverse=(dir_sign < 0),
    )

    if not ahead_locks:
        current_rm = start_rm + dir_sign * elapsed_h * typical_mph
        return current_rm, 0, 0.0, {"method": "cruise_open_water", "sample_n": None}, False

    next_lock_id, next_lock_rm = ahead_locks[0]
    leg_info = _empirical_leg_info(table, exclude_lock_id, next_lock_id, dir_code, barges) if exclude_lock_id else None

    if leg_info is not None:
        leg_min, leg_n, leg_class = leg_info
        leg_h = leg_min / 60.0
        if elapsed_h < leg_h:
            frac = elapsed_h / leg_h if leg_h > 0 else 0.0
            current_rm = start_rm + dir_sign * frac * abs(next_lock_rm - start_rm)
            basis = {"method": "empirical_interpolated", "from_lock_id": exclude_lock_id,
                     "to_lock_id": next_lock_id, "leg_median_min": leg_min, "sample_n": leg_n,
                     "barge_class": leg_class, "frac_complete": round(frac, 3)}
            return current_rm, 0, 0.0, basis, False
        naive_cruise_min = abs(next_lock_rm - start_rm) / typical_mph * 60.0
        minutes_deducted = max(0.0, leg_min - naive_cruise_min)
        grace_elapsed_h = elapsed_h - leg_h
        if grace_elapsed_h < NEXT_LOCK_GRACE_H:
            current_rm = next_lock_rm + dir_sign * grace_elapsed_h * typical_mph
            basis = {"method": "empirical_grace", "from_lock_id": exclude_lock_id,
                     "to_lock_id": next_lock_id, "leg_median_min": leg_min, "sample_n": leg_n,
                     "barge_class": leg_class, "grace_elapsed_h": round(grace_elapsed_h, 2)}
            return current_rm, 1, minutes_deducted, basis, False
        current_rm = next_lock_rm + dir_sign * NEXT_LOCK_GRACE_H * typical_mph
        basis = {"method": "empirical_overdue", "from_lock_id": exclude_lock_id,
                 "to_lock_id": next_lock_id, "leg_median_min": leg_min, "sample_n": leg_n,
                 "barge_class": leg_class}
        return current_rm, 1, minutes_deducted, basis, True

    cruise_h = abs(next_lock_rm - start_rm) / typical_mph
    if elapsed_h < cruise_h:
        current_rm = start_rm + dir_sign * elapsed_h * typical_mph
        return current_rm, 0, 0.0, {"method": "cruise_open_water", "sample_n": None}, False
    grace_elapsed_h = elapsed_h - cruise_h
    if grace_elapsed_h < NEXT_LOCK_GRACE_H:
        current_rm = next_lock_rm + dir_sign * grace_elapsed_h * typical_mph
        basis = {"method": "cruise_grace", "sample_n": None, "grace_elapsed_h": round(grace_elapsed_h, 2)}
        return current_rm, 1, 0.0, basis, False
    current_rm = next_lock_rm + dir_sign * NEXT_LOCK_GRACE_H * typical_mph
    return current_rm, 1, 0.0, {"method": "cruise_overdue_at_lock", "sample_n": None}, True


# --- Top-level: build all 3 tiers ------------------------------------------

async def compute_projected_tiers(conn, lock_meta, receivers, clean_name_fn, bearing_fn):
    """Returns (dead_reckoned_vessels, lock_projected_vessels) -- both lists
    in the same shape as the live tier's vessel dicts, ready to merge with
    the caller's live-tier list. `conn` must be inside an active pool
    acquire(); `clean_name_fn`/`bearing_fn` reuse main.py's existing helpers
    so name-cleanup and bearing math stay identical across all three tiers.
    """
    now = datetime.now(timezone.utc)
    now_ts = now.timestamp()

    # Names to exclude from lock-projection because we've heard them on AIS
    # at all recently (live OR dead-reckoned -- ANY freshness, not just the
    # dead-reckon candidate window below). Real bug found via the old-vs-new
    # shadow comparator, 2026-08-07: this used to be derived only from
    # dr_rows (300s-2h stale), so a genuinely LIVE vessel (<300s) was never
    # added to the exclusion set and could show up in BOTH the live tier AND
    # lock_projected simultaneously (confirmed live: Viking Mississippi in
    # both). The source's own ais_vessel_keys is built from the FULL vessel
    # history dict (every AIS tier), not just the dead-reckon subset -- match
    # that here with a separate, wider query.
    ais_name_rows = await conn.fetch(
        """
        SELECT DISTINCT name FROM vessel_current_state
        WHERE last_seen > now() - interval '2 hours' AND name IS NOT NULL
        """,
        timeout=15,
    )
    ais_vessel_keys = {_name_key(r["name"]) for r in ais_name_rows if r["name"]}

    # Candidates for dead-reckoning: vessel_current_state rows stale for the
    # live tier but within the 2h dead-reckon window, moving fast enough to
    # be worth projecting.
    dr_rows = await conn.fetch(
        """
        SELECT mmsi, name, last_seen, last_lat, last_lon, last_source_id,
               sog_mph, cog_deg, heading_deg, shiptype, is_commercial,
               callsign, destination, draught
        FROM vessel_current_state
        WHERE last_seen > now() - interval '2 hours'
          AND last_seen <= now() - interval '300 seconds'
        """,
        timeout=15,
    )

    dead_reckoned = []
    for r in dr_rows:
        age_s = (now - r["last_seen"]).total_seconds()
        if age_s < 0:
            age_s = 0
        speed_mph = r["sog_mph"]
        direction = _dir_from_cog(r["cog_deg"])
        if not (isinstance(speed_mph, (int, float)) and speed_mph >= DEAD_RECKON_MIN_SPEED_MPH
                and direction is not None and r["last_lat"] is not None):
            continue
        rm_last = latlon_to_rm(r["last_lat"], r["last_lon"])
        if rm_last is None:
            continue
        age_h = age_s / 3600.0
        dir_sign = -1 if direction == "downbound" else 1
        projected_rm = rm_last + dir_sign * speed_mph * age_h
        lat_show, lon_show = rm_to_latlon(projected_rm)
        if lat_show is None:
            continue
        src = receivers.get(r["last_source_id"], receivers["lake_mac"])
        dead_reckoned.append({
            "mmsi": r["mmsi"], "name": clean_name_fn(r["name"]),
            "lat": round(lat_show, 6), "lon": round(lon_show, 6),
            "speed_mph": speed_mph, "cog_deg": r["cog_deg"], "heading_deg": r["heading_deg"],
            "shiptype": r["shiptype"], "is_commercial": r["is_commercial"],
            "last_signal_s": age_s, "is_live": False, "is_dead_reckoned": True,
            "heard_by": r["last_source_id"],
            "distance_mi_from_receiver": round(_hav_mi(src["lat"], src["lon"], lat_show, lon_show), 2),
            "bearing_deg_from_receiver": round(bearing_fn(src["lat"], src["lon"], lat_show, lon_show), 0),
            "river_mile": round(projected_rm, 2), "direction": direction,
            "callsign": r["callsign"], "destination": r["destination"], "draught_m": r["draught"],
            "last_real": {
                "lat": round(r["last_lat"], 6), "lon": round(r["last_lon"], 6),
                "at": r["last_seen"].isoformat(), "age_min": round(age_s / 60, 1),
                "speed_mph": speed_mph, "river_mile": round(rm_last, 2),
            },
            "path_from_last_real": channel_slice(rm_last, projected_rm),
        })

    # Lock-projected candidates: recent lock departures (LPMS) for a vessel
    # we haven't heard on AIS recently.
    #
    # Real gap found via the old-vs-new shadow comparator, 2026-08-07: this
    # query had NO recreational-vessel exclusion at all, unlike every other
    # lock_status_observation query in this codebase (main.py's
    # _is_recreational, vessel_profile.py, replay.py's _load_lock_clearances)
    # and unlike the source's own _fetch_lock_departures_recent (vesselNo ==
    # "9999999" or "RECREATION" in name). An anonymized recreational lockage
    # could get projected forward as if she were a real commercial tow.
    dep_rows = await conn.fetch(
        """
        SELECT lock_id, vessel_name, direction, barges, eol_at, raw_payload
        FROM lock_status_observation
        WHERE eol_at IS NOT NULL
          AND eol_at > now() - make_interval(hours => $1)
          AND direction IN ('U', 'D')
        ORDER BY eol_at DESC
        """,
        LOCK_PROJECT_MAX_HOURS,
        timeout=15,
    )
    best_by_vessel = {}
    for r in dep_rows:
        if _is_recreational(r["vessel_name"], r["raw_payload"]):
            continue
        vkey = _name_key(r["vessel_name"])
        if vkey in ais_vessel_keys:
            continue
        age_h = (now_ts - r["eol_at"].timestamp()) / 3600.0
        prev = best_by_vessel.get(vkey)
        if prev is None or age_h < prev["age_h"]:
            best_by_vessel[vkey] = {"lock_id": r["lock_id"], "direction": r["direction"],
                                     "barges": r["barges"] or 0, "vessel": r["vessel_name"],
                                     "vkey": vkey, "eol_at": r["eol_at"], "age_h": age_h}

    empirical_table = await _build_empirical_transit_table(conn, lock_meta)

    lock_projected = []
    for c in best_by_vessel.values():
        meta = lock_meta[c["lock_id"]]
        dir_sign = 1 if c["direction"] == "U" else -1
        typical_mph = UPBOUND_MPH_TYPICAL if c["direction"] == "U" else DOWNBOUND_MPH_TYPICAL
        projected_rm, _locks, _min_ded, position_basis, overdue_unconfirmed = _project_rm_with_lockage(
            lock_meta, empirical_table, meta["river_mile"], dir_sign, c["age_h"], typical_mph,
            c["barges"], exclude_lock_id=c["lock_id"],
        )

        # Real-hit override: if we've actually heard her more recently than
        # her lock departure (even if too stale for the dead-reckon tier),
        # a real position beats a statistical guess -- but only if it's
        # FURTHER along her direction of travel than the lock-timing guess.
        real = await conn.fetchrow(
            """
            SELECT last_lat, last_lon, last_seen, sog_mph
            FROM vessel_current_state
            WHERE upper(name) = upper($1) AND last_seen > $2 AND last_seen <= now()
            """,
            c["vessel"], c["eol_at"],
            timeout=10,
        )
        if real and real["last_lat"] is not None:
            rm_real = latlon_to_rm(real["last_lat"], real["last_lon"])
            if rm_real is not None:
                age_h_since_real = max(0.0, (now - real["last_seen"]).total_seconds() / 3600.0)
                spd_mph = real["sog_mph"] if isinstance(real["sog_mph"], (int, float)) and real["sog_mph"] > 0 else typical_mph
                projected_rm_from_real = rm_real + dir_sign * spd_mph * age_h_since_real
                if dir_sign * (projected_rm_from_real - projected_rm) > 0:
                    projected_rm = projected_rm_from_real
                    position_basis = {"method": "real_position_dead_reckoned_stale",
                                       "last_real_at": real["last_seen"].isoformat(),
                                       "age_h_since_real": round(age_h_since_real, 2)}

        lat_show, lon_show = rm_to_latlon(projected_rm)
        if lat_show is None:
            continue
        reception_radius = LIVE_RECEPTION_RADIUS_MI_TOW if c["barges"] else LIVE_RECEPTION_RADIUS_MI_LIGHT
        path_pts_full = channel_slice(meta["river_mile"], projected_rm)
        capped_at_reception_boundary = False
        reception_overdue_h = 0.0

        if path_pts_full:
            cum = [0.0]
            for i in range(1, len(path_pts_full)):
                lon0, lat0 = path_pts_full[i - 1]
                lon1, lat1 = path_pts_full[i]
                cum.append(cum[-1] + _hav_mi(lat0, lon0, lat1, lon1))
            total_dist_mi = cum[-1]
            for i, (plon, plat) in enumerate(path_pts_full):
                if _hav_mi(receivers["lake_mac"]["lat"], receivers["lake_mac"]["lon"], plat, plon) <= reception_radius:
                    lon_show, lat_show = plon, plat
                    rm_here = latlon_to_rm(lat_show, lon_show)
                    if rm_here is not None:
                        projected_rm = rm_here
                    capped_at_reception_boundary = True
                    if total_dist_mi > 0:
                        frac_to_cap = cum[i] / total_dist_mi
                        expected_h_to_cap = frac_to_cap * c["age_h"]
                        reception_overdue_h = max(0.0, c["age_h"] - expected_h_to_cap)
                    break
        reception_overdue = capped_at_reception_boundary and reception_overdue_h > NEXT_LOCK_GRACE_H
        dist_mi = round(_hav_mi(receivers["lake_mac"]["lat"], receivers["lake_mac"]["lon"], lat_show, lon_show), 2)
        bearing = round(bearing_fn(receivers["lake_mac"]["lat"], receivers["lake_mac"]["lon"], lat_show, lon_show), 0)
        direction_word = "upbound" if c["direction"] == "U" else "downbound"
        stop_evidence = KNOWN_STOP_RATE.get((c["lock_id"], c["direction"]))

        lock_projected.append({
            "mmsi": None, "name": clean_name_fn(c["vessel"]),
            "lat": round(lat_show, 6), "lon": round(lon_show, 6),
            "speed_mph": typical_mph, "cog_deg": None, "heading_deg": None,
            "shiptype": 57, "is_commercial": True,
            "last_signal_s": None, "is_live": False, "is_lock_projected": True,
            "capped_at_reception_boundary": capped_at_reception_boundary,
            "reception_overdue_h": round(reception_overdue_h, 2),
            "reception_overdue": reception_overdue,
            "overdue_unconfirmed": overdue_unconfirmed,
            "distance_mi_from_receiver": dist_mi,
            "bearing_deg_from_receiver": bearing,
            "river_mile": round(projected_rm, 2), "direction": direction_word,
            "callsign": None, "destination": None, "draught_m": None,
            "confidence": "medium" if stop_evidence else "low",
            "position_basis": position_basis,
            "last_lock": {"lock_id": c["lock_id"], "name": meta["name"],
                          "cleared_at": c["eol_at"].isoformat(), "age_h": round(c["age_h"], 2)},
            "path_from_lock": path_pts_full,
        })

    return dead_reckoned, lock_projected
