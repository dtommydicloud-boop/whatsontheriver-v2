"""vessel_profile.py -- per-vessel lockage-history profile, ported from
Reed's vessel-profile.py against v2's Postgres schema.

Data access rebuilt: the source pulls each lock's LPMS queue live/cached
per-request; this queries lock_status_observation directly (the same table
/lock-status and projections.py already read from), scoped to one vessel's
own rows -- naturally small and fast, no perf concern like the empirical
transit table had.

Algorithm ported close to verbatim: local-shuttle detection (same lock,
opposite direction, within SHUTTLE_ROUNDTRIP_MAX_H), and reach-through-%
for her two most common transit legs (does she keep going past the next
lock within 24h, or tend to stop/turn around here).
"""
import bisect
from collections import defaultdict
from datetime import datetime, timezone

SHUTTLE_ROUNDTRIP_MAX_H = 3.0

# Same 6-lock scope as the source (LOCKS minus Trempealeau, "not reliably
# in our cache" per the source's own comment).
LOCK_ORDER = [
    ("01", "St Paul", 847.6),
    ("02", "Hastings", 815.2),
    ("03", "Red Wing (Welch)", 796.9),
    ("04", "Alma", 752.8),
    ("05", "Minneiska", 738.1),
    ("5A", "Winona", 728.5),
]


def _clean_ais_name(nm):
    nm = (nm or "").upper().strip()
    nm = "".join(ch for ch in nm if ch.isalnum() or ch.isspace())
    parts = nm.split()
    if parts and parts[-1].isdigit():
        parts = parts[:-1]
    return " ".join(parts)


def _mark_shuttles(her_rows):
    """Same-lock, opposite-direction, within 3h -- matching the source's real
    rule (a shuttle is a round trip at ONE lock, not a same-vessel event at a
    DIFFERENT lock nearby in time). Rewritten from an O(n^2) full pairwise
    scan to a sorted two-pointer window.

    Real bug found 2026-08-07 via a second-opinion code review: the original
    version of this rewrite dropped the same-lock check entirely -- the
    docstring claimed "same lock" but the code only checked direction+time
    across ALL of her rows regardless of which lock, spanning all 6 locks in
    her_rows. In practice this rarely fired wrong (a single real transit
    clears every lock in the same direction), but it's a real correctness
    gap against the stated rule, not just a doc/code mismatch.

    Real perf bug found testing this live: a common vessel name can match
    ~10K rows across the 6 locks, and the original nested loop compared every
    row against every other row (~100M comparisons) -- measured 13s total
    request time in production despite the SQL itself running in ~9ms
    (confirmed via EXPLAIN ANALYZE). her_rows arrives already sorted by `end`
    (the query's ORDER BY eol_at, preserved through the Python filter above),
    so for each row only a small window of rows within +/-3h can possibly
    match -- no need to scan the whole list.
    """
    n = len(her_rows)
    left = 0
    right = 0
    window_s = SHUTTLE_ROUNDTRIP_MAX_H * 3600.0
    for i in range(n):
        end_i = her_rows[i]["end"]
        while left < n and (end_i - her_rows[left]["end"]).total_seconds() > window_s:
            left += 1
        if right < left:
            right = left
        while right < n and (her_rows[right]["end"] - end_i).total_seconds() <= window_s:
            right += 1
        this_direction = her_rows[i]["direction"]
        this_lock = her_rows[i]["lock_id"]
        is_shuttle = False
        for j in range(left, right):
            if j == i:
                continue
            if her_rows[j]["lock_id"] != this_lock:
                continue  # different lock -- not a same-lock round trip
            if this_direction is not None and her_rows[j]["direction"] == this_direction:
                continue  # same-direction double-cut, not a shuttle
            is_shuttle = True
            break
        her_rows[i]["is_shuttle"] = is_shuttle


async def build(conn, vessel_name):
    now = datetime.now(timezone.utc)
    target = _clean_ais_name(vessel_name)

    # Real perf bug found testing this against live data: a regexp_replace()
    # filter can't use an index, so it forced a sequential scan + per-row
    # regex over the whole table (~500K rows) -- measured 9.7s for a single
    # vessel lookup. Fixed properly: added a functional index on
    # upper(vessel_name) and match on that directly (fast, indexed) --
    # _clean_ais_name below still does the real punctuation-normalized
    # identity check on the resulting small row set, so correctness is
    # unchanged, only the SQL-side pre-filter got cheaper.
    rows = await conn.fetch(
        """
        SELECT lock_id, vessel_name, direction, barges, eol_at
        FROM lock_status_observation
        WHERE lock_id = ANY($1::text[])
          AND eol_at IS NOT NULL
          AND upper(trim(vessel_name)) = upper(trim($2))
        ORDER BY eol_at
        """,
        [lk for lk, _, _ in LOCK_ORDER], vessel_name,
        timeout=15,
    )

    lock_name_by_id = {lk: nm for lk, nm, _ in LOCK_ORDER}
    rm_by_id = {lk: rm for lk, _, rm in LOCK_ORDER}

    # Filter to real exact-normalized-name matches (the SQL LIKE above is a
    # cheap pre-filter so we don't pull the whole table; _clean_ais_name is
    # the real identity check, same as the source).
    her_rows = []
    for r in rows:
        if _clean_ais_name(r["vessel_name"]) != target:
            continue
        if "RECREATION" in (r["vessel_name"] or "").upper():
            continue
        her_rows.append({
            "lock_id": r["lock_id"], "lock": lock_name_by_id[r["lock_id"]],
            "lock_rm": rm_by_id[r["lock_id"]],
            "direction": "upbound" if r["direction"] == "U" else "downbound",
            "end": r["eol_at"], "barges": r["barges"] or 0,
        })

    # her_rows is already time-sorted (query ORDER BY eol_at, preserved
    # through the filter loop above) -- _mark_shuttles relies on that order.
    _mark_shuttles(her_rows)

    if not her_rows:
        return {
            "vessel": vessel_name, "generated_at": now.isoformat(timespec="seconds"),
            "total_lockages": 0,
            "error": "no lockage history found for this vessel across the 6 covered locks",
        }

    total = len(her_rows)
    shuttle_events = [r for r in her_rows if r["is_shuttle"]]
    transit_events = [r for r in her_rows if not r["is_shuttle"]]
    pct_shuttle = round(100 * len(shuttle_events) / total, 1)

    barges_seen = [r["barges"] for r in transit_events if r["barges"]]
    typical_barges = round(sum(barges_seen) / len(barges_seen), 1) if barges_seen else 0
    max_barges = max((r["barges"] for r in her_rows), default=0)

    by_lock = {}
    for r in her_rows:
        entry = by_lock.setdefault(r["lock"], {"count": 0, "upbound": 0, "downbound": 0})
        entry["count"] += 1
        entry[r["direction"]] += 1

    # Pre-grouped, time-sorted end-times per (lock, direction) -- transit_events
    # preserves her_rows' time order, so each group is already sorted and a
    # 24h-window "does she reach the next lock" check can be a bisect lookup
    # instead of an any(...) scan over every transit event (same O(n^2) class
    # of bug as the old shuttle matcher, same fix shape).
    ends_by_lock_dir = defaultdict(list)
    for r in transit_events:
        ends_by_lock_dir[(r["lock"], r["direction"])].append(r["end"])

    reach = {}
    rm_by_lock_name = {nm: rm for _, nm, rm in LOCK_ORDER}
    for lk_id, lk_nm, lk_rm in LOCK_ORDER:
        leg_events = [r for r in transit_events if r["lock"] == lk_nm]
        if len(leg_events) < 2:
            continue
        for direction_word in ("upbound", "downbound"):
            dir_events = [r for r in leg_events if r["direction"] == direction_word]
            if len(dir_events) < 2:
                continue
            candidates = [nm for _, nm, rm in LOCK_ORDER
                          if (rm > lk_rm if direction_word == "upbound" else rm < lk_rm)]
            if not candidates:
                continue
            next_lock = min(candidates, key=lambda nm: abs(rm_by_lock_name[nm] - lk_rm))
            next_ends = ends_by_lock_dir.get((next_lock, direction_word), [])
            hits = 0
            for ev in dir_events:
                idx = bisect.bisect_left(next_ends, ev["end"])
                matched = idx < len(next_ends) and (next_ends[idx] - ev["end"]).total_seconds() <= 24 * 3600.0
                if matched:
                    hits += 1
            reach[f"{lk_nm}_{direction_word}"] = {
                "from_lock": lk_nm, "direction": direction_word, "to_lock": next_lock,
                "n_samples": len(dir_events),
                "continues_past_pct": round(100 * hits / len(dir_events), 1),
            }

    return {
        "vessel": vessel_name,
        "generated_at": now.isoformat(timespec="seconds"),
        "total_lockages": total,
        "first_seen": her_rows[0]["end"].isoformat(timespec="seconds"),
        "last_seen": her_rows[-1]["end"].isoformat(timespec="seconds"),
        "pct_local_shuttle": pct_shuttle,
        "pct_real_transit": round(100 - pct_shuttle, 1),
        "typical_barge_count": typical_barges,
        "max_barge_count_seen": max_barges,
        "by_lock": by_lock,
        "continues_past": reach,
        "summary_line": (
            f"{vessel_name}: {total} lockages seen. "
            + (f"{pct_shuttle}% look like local shuttle round-trips (not headed anywhere far); "
               if pct_shuttle > 0 else "")
            + f"{round(100-pct_shuttle,1)}% are real transits, typically {typical_barges or 'unknown'} barges."
        ),
        "honest_caveats": [
            "Cargo type is NOT reported -- no data source we have (AIS or USACE LPMS) carries a cargo "
            "manifest. Barge count is the only real proxy we have.",
            "continues_past percentages are historical base rates from the lockages we've actually "
            "captured for this specific vessel -- small sample sizes (see n_samples) mean real "
            "uncertainty, not a guarantee for any one trip.",
            "History window is whatever's currently retained in the database, not this vessel's "
            "full real-world history.",
        ],
    }
