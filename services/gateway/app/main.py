"""
gateway (a.k.a. notification-service)

The only service exposed to the outside world. Responsibilities:
  1. Consume the enriched `train:eta` Redis Stream in the background.
  2. Keep an in-memory snapshot of the latest known state per train.
  3. Broadcast every new event to all connected WebSocket clients in real time.
  4. Serve a small REST API (snapshot + health) and the static map frontend.

This is the "API gateway" of the system: it's the single ingress point, it
fans data out to many clients, and it's the piece you'd put behind a load
balancer / add auth to first in a real deployment.
"""
import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

logging.basicConfig(level=logging.INFO, format="%(asctime)s [gateway] %(message)s")
log = logging.getLogger("gateway")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
ETA_STREAM = "train:eta"
GROUP = "gateway-workers"
CONSUMER = os.getenv("HOSTNAME", "gateway-1")

# latest known state per train_id, used to hydrate clients that connect
# mid-stream and to serve the REST snapshot endpoint
latest_state: dict[str, dict] = {}


class ConnectionManager:
    def __init__(self):
        self.active: set[WebSocket] = set()
        self.lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self.lock:
            self.active.add(ws)

    async def disconnect(self, ws: WebSocket):
        async with self.lock:
            self.active.discard(ws)

    async def broadcast(self, message: dict):
        dead = []
        async with self.lock:
            targets = list(self.active)
        payload = json.dumps(message)
        for ws in targets:
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(ws)
        if dead:
            async with self.lock:
                for ws in dead:
                    self.active.discard(ws)


manager = ConnectionManager()


async def ensure_group(client: redis.Redis):
    try:
        await client.xgroup_create(ETA_STREAM, GROUP, id="$", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


async def stream_consumer():
    client = redis.from_url(REDIS_URL, decode_responses=True)
    await client.ping()
    await ensure_group(client)
    log.info("gateway consuming %s as %s/%s", ETA_STREAM, GROUP, CONSUMER)

    while True:
        try:
            resp = await client.xreadgroup(GROUP, CONSUMER, {ETA_STREAM: ">"}, count=20, block=5000)
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
                    latest_state[event["train_id"]] = event
                    await manager.broadcast({"type": "train_update", "data": event})
                    await client.xack(ETA_STREAM, GROUP, entry_id)
                except Exception:
                    log.exception("failed to process entry %s", entry_id)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(stream_consumer())
    log.info("gateway started")
    yield
    task.cancel()


app = FastAPI(title="Train Tracker Gateway", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "connected_clients": len(manager.active), "known_trains": len(latest_state)}


@app.get("/api/trains")
async def trains_snapshot():
    return {"trains": list(latest_state.values())}


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # hydrate the newly connected client with current known state
        await websocket.send_text(json.dumps({"type": "snapshot", "data": list(latest_state.values())}))
        while True:
            # we don't expect client -> server messages, but need to keep the
            # receive loop alive to detect disconnects
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await manager.disconnect(websocket)


static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/")
async def index():
    return FileResponse(os.path.join(static_dir, "index.html"))
