FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install dependencies first for better layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# SQLite database lives on the mounted volume.
ENV DATABASE_PATH=/data/ledger.db \
    HOST=0.0.0.0 \
    PORT=8000

RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8000

# Single uvicorn worker: admission control uses an in-process asyncio.Lock
# plus BEGIN IMMEDIATE, so one event loop serialises writers cleanly.
# Horizontal scaling would still be safe (SQLite write lock + busy_timeout),
# but is unnecessary for this workload.
CMD ["sh", "-c", "uvicorn app.main:app --host ${HOST} --port ${PORT}"]
