"""Queue abstraction — SQS Standard, per Reed's pick 2026-08-02 (pennies
at our real measured volume ~300-400k events/day, no new platform
dependency for a two-person team since neither of us runs Cloudflare
Queues/Workers elsewhere). Kept as a thin wrapper, not because the
choice is likely to change, but so ingest/worker code never talks to
boto3 directly -- easier to unit-test and easier to swap if SQS ever
stops being the right call.
"""
import json
import os
from typing import AsyncIterator

import aioboto3

QUEUE_URL_ENV = "SQS_QUEUE_URL"
DLQ_URL_ENV = "SQS_DLQ_URL"  # dead-letter queue, configured on the SQS side


class Queue:
    def __init__(self, queue_url: str | None = None):
        self._queue_url = queue_url or os.environ[QUEUE_URL_ENV]
        self._session = aioboto3.Session()

    async def publish(self, event: dict) -> None:
        async with self._session.client("sqs") as sqs:
            await sqs.send_message(
                QueueUrl=self._queue_url,
                MessageBody=json.dumps(event),
            )

    async def consume(self, consumer_name: str) -> AsyncIterator[tuple[str, dict]]:
        """Yields (receipt_handle, event). Caller must call ack() after processing.
        consumer_name is unused for SQS (no consumer groups) but kept in the
        interface so swapping back to Redis Streams later doesn't touch callers.
        """
        async with self._session.client("sqs") as sqs:
            while True:
                resp = await sqs.receive_message(
                    QueueUrl=self._queue_url,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=20,  # long-poll, cheaper than tight-loop polling
                    VisibilityTimeout=60,
                )
                for msg in resp.get("Messages", []):
                    yield msg["ReceiptHandle"], json.loads(msg["Body"])

    async def ack(self, receipt_handle: str) -> None:
        async with self._session.client("sqs") as sqs:
            await sqs.delete_message(QueueUrl=self._queue_url, ReceiptHandle=receipt_handle)
