"""vessel_speed.py -- per-vessel real speed database, Tom's ask 2026-08-07:
"once we see that boat again show up as a projection coming from Alma, we
should have a better idea as to what speed it's probably gonna be driving
at... we should have some sort of a database... give it its own projection
speed based on that, not that alone of course, it's just another tool."

Built on a real analysis first, not assumption: pulled real AIS cruising
speed (vessel_position, when a vessel is actually in antenna range) against
real lock-to-lock effective speed (lock_status_observation clearance
timestamps, same adjacent-lock-pair method as projections.py's empirical
transit table) for every vessel with enough of both. Found a genuine
correlation, r=0.79 across 24 vessels -- a boat's real cruising speed does
predict her real lock-to-lock pace, with a fairly consistent ratio (real
lock-to-lock speed is real cruising speed times roughly 0.6-0.7, the gap
being real time spent actually locking through, not signal noise).

This module computes and stores, per (vessel, direction):
  - her own median AIS cruising speed (real antenna observations)
  - her own lock-to-lock/AIS ratio, IF she has enough of her own lock-to-
    lock legs to compute it (own_leg_sample_n >= MIN_OWN_LEG_SAMPLES) --
    otherwise falls back to the fleet-wide median ratio (recomputed fresh
    every refresh, not hardcoded, so it improves as more data accumulates)
  - typical_speed_mph = her AIS median * whichever ratio applies -- this is
    what projections.py uses instead of the flat UPBOUND_MPH_TYPICAL /
    DOWNBOUND_MPH_TYPICAL constant, when we have enough history on her.

Refreshed on a TTL (not every request -- a full-table scan takes a couple
seconds, same cost class as the empirical transit table) so profiles get
sharper day over day as real AIS history accumulates, without ever hitting
the request path with that cost. photo_url column exists in the table but
is unused by this module -- reserved for Tom's "we should have a picture
in there" ask, a separate feature.
"""
import statistics
from datetime import datetime, timezone

REFRESH_TTL_S = 24 * 3600

MIN_AIS_SAMPLES = 5          # minimum real AIS speed readings to trust a per-vessel median at all
MIN_OWN_LEG_SAMPLES = 3      # minimum of HER OWN lock-to-lock legs to trust HER OWN ratio over the fleet fallback
PLAUSIBLE_SPEED_MPH = (1.5, 9.0)   # clamp -- a noisy small sample shouldn't hand back an absurd typical speed

_cache = {"at": 0.0, "data": None}


def _clean_key(nm):
    nm = (nm or "").upper().strip()
    nm = "".join(ch for ch in nm if ch.isalnum() or ch.isspace())
    parts = nm.split()
    if parts and parts[-1].isdigit():
        parts = parts[:-1]
    return " ".join(parts)


def _dir_from_cog(cog):
    if cog is None or not isinstance(cog, (int, float)):
        return None
    c = cog % 360
    if c > 270 or c < 90:
        return "upbound"
    if 90 <= c <= 270:
        return "downbound"
    return None


async def refresh(conn, lock_meta):
    """Recompute the whole vessel_speed_profile table from real data.
    Returns the number of (vessel, direction) rows upserted."""
    ordered_locks = sorted(lock_meta.items(), key=lambda kv: kv[1]["river_mile"])
    adjacent_dist = {}
    for (lo_id, lo_meta), (hi_id, hi_meta) in zip(ordered_locks, ordered_locks[1:]):
        dist = hi_meta["river_mile"] - lo_meta["river_mile"]
        adjacent_dist[(lo_id, hi_id, "U")] = dist
        adjacent_dist[(hi_id, lo_id, "D")] = dist

    # Real lock-to-lock legs, per vessel -- same LEAD()-window method as
    # projections.py's empirical transit table, kept per-vessel here instead
    # of blended into fleet medians.
    leg_rows = await conn.fetch(
        """
        WITH ordered AS (
            SELECT lock_id, vessel_name, direction, eol_at,
                   LEAD(lock_id) OVER w AS next_lock_id,
                   LEAD(direction) OVER w AS next_direction,
                   LEAD(eol_at) OVER w AS next_eol_at
            FROM lock_status_observation
            WHERE eol_at IS NOT NULL
              AND time > now() - interval '45 days'
            WINDOW w AS (PARTITION BY upper(vessel_name) ORDER BY eol_at)
        )
        SELECT lock_id, next_lock_id, direction, vessel_name,
               (EXTRACT(EPOCH FROM (next_eol_at - eol_at)) / 60.0)::float8 AS transit_min
        FROM ordered
        WHERE next_lock_id IS NOT NULL
          AND direction = next_direction
          AND next_eol_at - eol_at <= make_interval(hours => 48)
        """,
        timeout=30,
    )
    legs_by_key = {}
    for r in leg_rows:
        key = (r["lock_id"], r["next_lock_id"], r["direction"])
        if key not in adjacent_dist:
            continue
        if "RECREATION" in (r["vessel_name"] or "").upper():
            continue
        transit_h = r["transit_min"] / 60.0
        if transit_h <= 0:
            continue
        eff_mph = adjacent_dist[key] / transit_h
        if not (1.0 <= eff_mph <= 12.0):
            continue
        dword = "upbound" if r["direction"] == "U" else "downbound"
        legs_by_key.setdefault((_clean_key(r["vessel_name"]), dword), []).append(eff_mph)

    # Real AIS cruising speeds, per vessel -- moving, plausible range.
    ais_rows = await conn.fetch(
        """
        SELECT name, sog_mph, cog_deg
        FROM vessel_position
        WHERE sog_mph IS NOT NULL AND sog_mph BETWEEN 2 AND 12
          AND cog_deg IS NOT NULL
          AND name IS NOT NULL AND trim(name) != ''
          AND time > now() - interval '45 days'
        """,
        timeout=30,
    )
    ais_by_key = {}
    display_name = {}
    for r in ais_rows:
        d = _dir_from_cog(r["cog_deg"])
        if d is None:
            continue
        key = (_clean_key(r["name"]), d)
        ais_by_key.setdefault(key, []).append(r["sog_mph"])
        display_name[_clean_key(r["name"])] = r["name"].strip()

    # Fleet-wide fallback ratio -- recomputed fresh every refresh (not
    # hardcoded) from whichever vessels have BOTH real AIS speed and real
    # lock-to-lock legs this cycle, so it improves as more data accumulates.
    fleet_ratios = []
    for key, ais_speeds in ais_by_key.items():
        if len(ais_speeds) < MIN_AIS_SAMPLES:
            continue
        leg_speeds = legs_by_key.get(key)
        if leg_speeds and len(leg_speeds) >= MIN_OWN_LEG_SAMPLES:
            fleet_ratios.append(statistics.median(leg_speeds) / statistics.median(ais_speeds))
    fleet_fallback_ratio = statistics.median(fleet_ratios) if fleet_ratios else 0.67

    now = datetime.now(timezone.utc)
    n_written = 0
    async with conn.transaction():
        for (vkey, dword), ais_speeds in ais_by_key.items():
            if len(ais_speeds) < MIN_AIS_SAMPLES:
                continue
            ais_median = statistics.median(ais_speeds)
            leg_speeds = legs_by_key.get((vkey, dword))
            own_ratio = None
            own_n = 0
            if leg_speeds and len(leg_speeds) >= MIN_OWN_LEG_SAMPLES:
                own_n = len(leg_speeds)
                own_ratio = statistics.median(leg_speeds) / ais_median
            ratio = own_ratio if own_ratio is not None else fleet_fallback_ratio
            typical = ais_median * ratio
            lo, hi = PLAUSIBLE_SPEED_MPH
            typical = max(lo, min(hi, typical))
            await conn.execute(
                """
                INSERT INTO vessel_speed_profile
                    (vessel_key, direction, display_name, typical_speed_mph,
                     ais_median_mph, ais_sample_n, own_ratio, own_leg_sample_n, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (vessel_key, direction) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    typical_speed_mph = EXCLUDED.typical_speed_mph,
                    ais_median_mph = EXCLUDED.ais_median_mph,
                    ais_sample_n = EXCLUDED.ais_sample_n,
                    own_ratio = EXCLUDED.own_ratio,
                    own_leg_sample_n = EXCLUDED.own_leg_sample_n,
                    updated_at = EXCLUDED.updated_at
                """,
                vkey, dword, display_name.get(vkey), typical,
                ais_median, len(ais_speeds), own_ratio, own_n, now,
            )
            n_written += 1
    return n_written


async def ensure_fresh(conn, lock_meta):
    """TTL-gated refresh -- call before using profiles in the request path.
    Cheap no-op most of the time; only pays the full-table-scan cost once
    per REFRESH_TTL_S across the whole process."""
    import time as _t
    now_t = _t.time()
    if (_cache["data"] is not None) and (now_t - _cache["at"]) < REFRESH_TTL_S:
        return
    n = await refresh(conn, lock_meta)
    _cache["data"] = {"n": n}
    _cache["at"] = now_t


async def lookup(conn, vessel_name, direction_word):
    """Her own typical speed for this direction, or None if we don't have
    enough real history on her yet (caller falls back to the fleet
    constant)."""
    vkey = _clean_key(vessel_name)
    row = await conn.fetchrow(
        "SELECT typical_speed_mph, ais_sample_n FROM vessel_speed_profile WHERE vessel_key = $1 AND direction = $2",
        vkey, direction_word,
    )
    if row is None or row["ais_sample_n"] < MIN_AIS_SAMPLES:
        return None
    return row["typical_speed_mph"]
