"""
history-service

Consumes the same `train:eta` stream (its own consumer group, so it can fall
behind or restart independently of the gateway) and persists every event
into SQLite. Exposes a small REST API for historical queries -- the kind of
thing a real ops dashboard or an analytics job would hit.

Using a distinct consumer group per downstream service is the point worth
calling out in an interview: Redis Streams let many independent services
each get every message, at their own pace, with their own replay position --
unlike a plain pub/sub channel where a slow/offline consumer just misses data.
"""
import asyncio
import json
import logging
import os
import sqlite3
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import FastAPI

logging.basicConfig(level=logging.INFO, format="%(asctime)s [history-service] %(message)s")
log = logging.getLogger("history-service")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
ETA_STREAM = "train:eta"
GROUP = "history-workers"
CONSUMER = os.getenv("HOSTNAME", "history-1")
DB_PATH = os.getenv("DB_PATH", "/data/history.db")

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
db_lock = asyncio.Lock()


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS train_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            train_id TEXT NOT NULL,
            route_id TEXT NOT NULL,
            route_name TEXT NOT NULL,
            lat REAL, lon REAL,
            status TEXT, last_stop TEXT, next_stop TEXT,
            delay_min REAL, eta_min REAL, delay_status TEXT,
            ts TEXT NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_train_id ON train_events(train_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ts ON train_events(ts)")
    conn.commit()
    conn.close()


async def ensure_group(client: redis.Redis):
    try:
        await client.xgroup_create(ETA_STREAM, GROUP, id="$", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


async def insert_event(event: dict):
    async with db_lock:
        conn = get_conn()
        conn.execute(
            """INSERT INTO train_events
               (train_id, route_id, route_name, lat, lon, status, last_stop, next_stop,
                delay_min, eta_min, delay_status, ts)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                event.get("train_id"), event.get("route_id"), event.get("route_name"),
                event.get("lat"), event.get("lon"), event.get("status"),
                event.get("last_stop"), event.get("next_stop"),
                event.get("delay_min"), event.get("eta_min"), event.get("delay_status"),
                event.get("ts"),
            ),
        )
        conn.commit()
        conn.close()


async def stream_consumer():
    client = redis.from_url(REDIS_URL, decode_responses=True)
    await client.ping()
    await ensure_group(client)
    log.info("history-service consuming %s as %s/%s", ETA_STREAM, GROUP, CONSUMER)

    while True:
        try:
            resp = await client.xreadgroup(GROUP, CONSUMER, {ETA_STREAM: ">"}, count=50, block=5000)
        except Exception:
            log.exception("redis read failed, retrying in 2s")
            await asyncio.sleep(2)
            continue

        if not resp:
            continue

        for _stream, entries in resp:
            for entry_id, fields in entries:
                try:
                    event = json.loads(fields["data"])
                    await insert_event(event)
                    await client.xack(ETA_STREAM, GROUP, entry_id)
                except Exception:
                    log.exception("failed to persist entry %s", entry_id)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    task = asyncio.create_task(stream_consumer())
    log.info("history-service started, db at %s", DB_PATH)
    yield
    task.cancel()


app = FastAPI(title="Train History Service", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/history/{train_id}")
async def train_history(train_id: str, limit: int = 100):
    async with db_lock:
        conn = get_conn()
        rows = conn.execute(
            "SELECT * FROM train_events WHERE train_id = ? ORDER BY id DESC LIMIT ?",
            (train_id, limit),
        ).fetchall()
        conn.close()
    return {"train_id": train_id, "events": [dict(r) for r in rows]}


@app.get("/stats/delays")
async def delay_stats():
    async with db_lock:
        conn = get_conn()
        rows = conn.execute(
            """SELECT route_id, delay_status, COUNT(*) as count
               FROM train_events GROUP BY route_id, delay_status"""
        ).fetchall()
        conn.close()
    return {"stats": [dict(r) for r in rows]}
