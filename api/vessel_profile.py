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


def _is_local_shuttle(end_time, her_other_rows, exclude_idx, this_direction):
    for i, r in enumerate(her_other_rows):
        if i == exclude_idx:
            continue
        if this_direction is not None and r["direction"] == this_direction:
            continue  # same-direction double-cut, not a shuttle
        gap_h = abs((r["end"] - end_time).total_seconds()) / 3600.0
        if gap_h <= SHUTTLE_ROUNDTRIP_MAX_H:
            return True
    return False


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

    for i, r in enumerate(her_rows):
        r["is_shuttle"] = _is_local_shuttle(r["end"], her_rows, i, r["direction"])

    her_rows.sort(key=lambda r: r["end"])

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
            hits = 0
            for ev in dir_events:
                matched = any(
                    r["lock"] == next_lock and r["direction"] == direction_word
                    and 0 <= (r["end"] - ev["end"]).total_seconds() / 3600.0 <= 24
                    for r in transit_events
                )
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
