"""Queue consumer -- the only process that writes to the database.
Reads events off the queue, writes the raw immutable copy, normalizes
into the typed tables, and updates vessel_current_state (the tiny table
the read API actually queries for live boat positions).

Runs as its own process/service, separate from api/main.py. If this
process is slow or crashes, the read API keeps serving whatever's
already in vessel_current_state -- reads never wait on ingestion.
"""
import asyncio
import os

import asyncpg

import sys
sys.path.insert(0, "../ingest")
from queue import Queue  # noqa: E402


async def write_event(conn: asyncpg.Connection, source_id: str, event: dict):
    # Raw layer first, always -- this must succeed even if normalization
    # below has a bug, since it's the forensic/replay source of truth.
    await conn.execute(
        """
        INSERT INTO telemetry_raw (received_at, observed_at, source_id, event_type,
                                    schema_version, dedupe_key, raw_payload)
        VALUES (to_timestamp($1), $2, $3, $4, $5, $6, $7)
        ON CONFLICT DO NOTHING
        """,
        event["received_at"], event["observed_at"], source_id,
        event["event_type"], event["schema_version"], event["dedupe_key"], event["payload"],
    )

    if event["event_type"] == "ais_position":
        p = event["payload"]
        await conn.execute(
            """
            INSERT INTO vessel_position (time, source_id, mmsi, name, lat, lon,
                                          sog_mph, cog_deg, heading_deg, nav_status,
                                          is_commercial, quality)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            """,
            event["observed_at"], source_id, p.get("mmsi"), p.get("name"),
            p["lat"], p["lon"], p.get("sog_mph"), p.get("cog_deg"), p.get("heading_deg"),
            p.get("nav_status"), p.get("is_commercial"), p.get("quality", "live"),
        )
        if p.get("mmsi"):
            await conn.execute(
                """
                INSERT INTO vessel_current_state (mmsi, name, last_seen, last_lat, last_lon,
                                                    last_source_id, quality)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (mmsi) DO UPDATE SET
                    name = EXCLUDED.name, last_seen = EXCLUDED.last_seen,
                    last_lat = EXCLUDED.last_lat, last_lon = EXCLUDED.last_lon,
                    last_source_id = EXCLUDED.last_source_id, quality = EXCLUDED.quality
                WHERE EXCLUDED.last_seen > vessel_current_state.last_seen
                """,
                p["mmsi"], p.get("name"), event["observed_at"], p["lat"], p["lon"],
                source_id, p.get("quality", "live"),
            )

    elif event["event_type"] == "lock_status":
        p = event["payload"]
        await conn.execute(
            """
            INSERT INTO lock_status_observation (time, source_id, lock_id, vessel_name,
                                                   direction, barges, sol_at, eol_at, raw_payload)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
            event["observed_at"], source_id, p["lock_id"], p.get("vessel_name"),
            p.get("direction"), p.get("barges"), p.get("sol_at"), p.get("eol_at"), p,
        )


async def main():
    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=2, max_size=10)
    q = Queue()

    async for msg_id, event in q.consume(consumer_name=os.environ.get("HOSTNAME", "worker-1")):
        try:
            async with pool.acquire() as conn:
                await write_event(conn, event["source_id"], event)
            await q.ack(msg_id)
        except Exception as e:
            # Deliberately do NOT ack on failure -- message stays in the
            # stream for redelivery. A pending-count alert on the consumer
            # group catches a stuck writer instead of silently dropping data.
            print(f"write failed for {msg_id}: {e}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
