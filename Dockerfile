FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api/ ./api/

# ingest/ (queue.py + worker.py) is the SQS-based design from the
# original spec, kept for when actual durability/backpressure needs it
# -- not currently in the deployed path. api.main includes both read
# and ingest endpoints, writing directly to Postgres. See api/main.py's
# header comment for why.
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8080"]
