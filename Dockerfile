FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingest/ ./ingest/
COPY api/ ./api/

# Which process this container runs is picked at deploy time via CMD
# override in wrangler.toml's [[containers]] block -- same image serves
# the ingest endpoint, the read API, and the worker, so there's one
# image to build/push, not three.
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8080"]
