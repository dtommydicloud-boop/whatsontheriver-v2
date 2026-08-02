"""Stateless ingest endpoint. Validates a signed batch from a cabin
collector and enqueues it. Deliberately does nothing else -- no DB
writes, no AIS/LPMS knowledge, no blocking I/O beyond the queue publish.
This is the piece that stays up even if the writer/DB is briefly behind.
"""
import hashlib
import hmac
import os
import time

from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel

from queue import Queue

app = FastAPI(title="whatsontheriver ingest")
q = Queue()

# source_id -> shared secret. Small enough to be an env-var map for now;
# move to a real secrets store once there's more than a couple of sources.
SOURCE_SECRETS: dict[str, str] = {
    k[len("SOURCE_SECRET_"):]: v
    for k, v in os.environ.items()
    if k.startswith("SOURCE_SECRET_")
}

MAX_CLOCK_SKEW_S = 300  # reject batches signed more than 5 min off


class TelemetryEvent(BaseModel):
    observed_at: str          # ISO8601, from the collector's clock
    event_type: str           # 'ais_position' | 'lock_status'
    schema_version: int = 1
    dedupe_key: str
    payload: dict


class Batch(BaseModel):
    source_id: str
    sent_at: float            # unix epoch, for clock-skew rejection
    events: list[TelemetryEvent]


def verify_signature(source_id: str, body: bytes, signature: str) -> bool:
    secret = SOURCE_SECRETS.get(source_id)
    if not secret:
        return False
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


@app.post("/ingest")
async def ingest(request: Request, x_signature: str = Header(...)):
    body = await request.body()
    batch = Batch.model_validate_json(body)

    if not verify_signature(batch.source_id, body, x_signature):
        raise HTTPException(401, "bad signature")
    if abs(time.time() - batch.sent_at) > MAX_CLOCK_SKEW_S:
        raise HTTPException(400, "clock skew too large -- check collector's clock")

    for event in batch.events:
        await q.publish({
            "source_id": batch.source_id,
            "received_at": time.time(),
            **event.model_dump(),
        })

    return {"accepted": len(batch.events)}


@app.get("/healthz")
async def healthz():
    # Deliberately trivial -- this endpoint must never depend on the DB
    # or the queue being healthy, or we've rebuilt the exact bug we're
    # fixing (a health check that itself can hang).
    return {"status": "ok"}
