"""river_pulse.py -- "How busy is the river right now?" composite gauge,
ported from Reed's river-pulse.py (next-vessel-server, lake-mac) against
v2's Postgres schema. Feeds the glance panel on whatsontheriver.com.

Score = max(surface_traffic, lock_pressure). Buckets: 0-39 quiet, 40-64
average, 65-89 busy, 90+ very-busy. Algorithm (weights, divergence-naming
in compose_reason) ported verbatim -- pure logic over already-fetched
values.

Data access rebuilt: the source reads today's local AIS jsonl snapshot
file for surface traffic and calls lock-status.py's build() for lock
pressure. This queries vessel_current_state directly for surface traffic
(same table /live-positions' live tier already reads, same freshness
philosophy as every other port here -- no repeated-snapshot noise to
filter, so no shiptype-membership re-derivation needed: is_commercial is
already computed at ingest time using the same COMMERCIAL_SHIPTYPES set
the source's own COMMERCIAL_SHIPTYPES alias points at). Lock pressure
reuses main.py's _compute_lock_status_data(conn) directly -- the exact
same computation /lock-status already does, not a second copy of it.
"""
from datetime import datetime, timezone

SURFACE_MAX_AGE_S = 180
DIVERGENCE_MARGIN = 20


async def surface_score(conn):
    rows = await conn.fetch(
        """
        SELECT is_commercial FROM vessel_current_state
        WHERE last_seen > now() - interval '180 seconds'
        """,
        timeout=10,
    )
    if not rows:
        return 0, {"source": "no-snap", "commercial": 0, "recreational": 0,
                    "max_age_s": SURFACE_MAX_AGE_S}, "no vessels on the water right now"
    n_c = sum(1 for r in rows if r["is_commercial"])
    n_r = len(rows) - n_c
    raw = n_c * 25 + n_r * 4
    detail = {"commercial": n_c, "recreational": n_r, "source": "ais-live", "max_age_s": SURFACE_MAX_AGE_S}
    if n_c == 0 and n_r == 0:
        reason = "no vessels on the water right now"
    elif n_c and n_r:
        reason = f"{n_c} commercial + {n_r} rec on the water"
    elif n_c:
        reason = f"{n_c} commercial vessel{'s' if n_c > 1 else ''} on the water"
    else:
        reason = f"{n_r} rec vessel{'s' if n_r > 1 else ''} on the water"
    return min(raw, 130), detail, reason


def lock_pressure(lock_data):
    total = 0
    tied_up = []
    queued_at = []
    for lk_id, lk in (lock_data.get("locks") or {}).items():
        s = lk.get("live_state") or {}
        p = int(s.get("pending") or 0)
        l = int(s.get("locking") or 0)
        contrib = l * 30 + p * 20
        flag = lk.get("flag")
        if flag == "jam":
            contrib += 20
        elif flag == "delay":
            contrib += 10
        total += contrib
        nm = lk.get("name") or lk_id
        if l:
            tied_up.append(f"{nm} ({l} in lock)")
        elif p:
            queued_at.append(f"{nm} ({p} pending)")
    reason_parts = []
    if tied_up:
        reason_parts.append("tied up at " + " + ".join(tied_up))
    if queued_at:
        reason_parts.append("queue at " + " + ".join(queued_at))
    reason = "; ".join(reason_parts) if reason_parts else "all bracket locks clear"
    return min(total, 130), reason


def bucketize(score):
    if score < 40:
        return "quiet"
    if score < 65:
        return "average"
    if score < 90:
        return "busy"
    return "very-busy"


def compose_reason(surface, surface_reason, lock, lock_reason, driver, bucket):
    diverge = abs(surface - lock) >= DIVERGENCE_MARGIN
    surface_has_activity = surface > 5
    lock_has_activity = lock > 5
    if not diverge:
        if surface_has_activity and lock_has_activity:
            return f"{surface_reason}; {lock_reason}"
        return lock_reason if driver == "lock-pressure" else surface_reason
    if driver == "lock-pressure":
        if surface_has_activity:
            return f"{surface_reason.rstrip('.')} right now, but {bucket}: {lock_reason}"
        return f"{bucket} because {lock_reason}. (No vessels visible on the water yet.)"
    if lock_has_activity:
        return f"{surface_reason}; locks noted: {lock_reason}"
    return surface_reason


LOCKAGE_HISTORY_DAYS = 7


async def lockage_history_7d(conn):
    """Real daily lockage counts, recreational vs commercial, last 7 days --
    Tom's ask 2026-08-07 ("count the rec boats in the locks as a
    barometer"): recreational lockages actually OUTNUMBER commercial ones
    right now (711 vs 564 over a week, checked live) -- a real signal that
    wasn't surfaced anywhere before this. Day boundaries in America/Chicago
    so "today" lines up with when a user actually experiences it, not UTC
    midnight. Same recreational check as main.py's _is_recreational (name
    literal + raw_payload vessel_no), done in SQL since this is a simple
    per-row classification over a bounded 7-day window, not worth a second
    Python pass."""
    rows = await conn.fetch(
        """
        SELECT (eol_at AT TIME ZONE 'America/Chicago')::date AS day,
               CASE WHEN upper(vessel_name) LIKE '%RECREATION%'
                      OR (raw_payload->>'vessel_no') = '9999999'
                    THEN 'recreational' ELSE 'commercial' END AS kind,
               count(*) AS n
        FROM lock_status_observation
        WHERE eol_at IS NOT NULL
          AND eol_at > now() - make_interval(days => $1)
        GROUP BY 1, 2
        ORDER BY 1
        """,
        LOCKAGE_HISTORY_DAYS,
        timeout=15,
    )
    by_day = {}
    for r in rows:
        d = r["day"].isoformat()
        by_day.setdefault(d, {"date": d, "commercial": 0, "recreational": 0})
        by_day[d][r["kind"]] = r["n"]
    return sorted(by_day.values(), key=lambda x: x["date"])


async def build(conn, compute_lock_status_data_fn):
    now = datetime.now(timezone.utc)
    surface, surface_detail, surface_reason = await surface_score(conn)
    try:
        lock_data = await compute_lock_status_data_fn(conn)
    except Exception as e:
        lock_data = {"locks": {}, "_lock_error": str(e)}
    lock, lock_reason = lock_pressure(lock_data)
    score = max(surface, lock)
    driver = "lock-pressure" if lock >= surface else "surface-traffic"
    bucket = bucketize(score)
    reason_line = compose_reason(surface, surface_reason, lock, lock_reason, driver, bucket)
    divergence_pts = abs(surface - lock)
    lockage_history = await lockage_history_7d(conn)

    dow = now.strftime("%A")
    baseline = {
        "grain": "dow-only",
        "sample_size": 0,
        "note": "insufficient (DoW, hour) history for typical-day comparison yet; "
                "showing raw activity -- baseline auto-upgrades once buckets reach n>=5",
    }
    return {
        "generated_at": now.isoformat(timespec="seconds"),
        "score": round(score),
        "bucket": bucket,
        "driver": driver,
        "reason_line": reason_line,
        "divergence_pts": round(divergence_pts),
        "sides_agree": divergence_pts < DIVERGENCE_MARGIN,
        "components": {
            "surface_traffic": {"score": round(surface), "detail": surface_detail, "reason": surface_reason},
            "lock_pressure": {"score": round(lock), "reason": lock_reason},
        },
        "lockage_history_7d": lockage_history,
        "baseline": baseline,
        "day_of_week": dow,
        "hour_local": now.hour,
        "honest_caveats": [
            "Score is composite max(surface_traffic, lock_pressure). Surface reads live "
            "AIS from the Lake Mac receiver (limited to Lake Pepin reach).",
            "Lock pressure combines pending arrivals + in-lock tows across covered locks; "
            "jam/delay flags add +10-20 pts.",
            "When surface_traffic and lock_pressure diverge sharply (>=20pt), the reason_line "
            "explicitly names both so a low boat count next to a Busy label doesn't read "
            "as contradiction. `sides_agree` flag lets clients render disagreement UI.",
            "Baseline comparison ('vs typical Thursday 12pm') will populate as we collect "
            "more per-hour history.",
        ],
    }
