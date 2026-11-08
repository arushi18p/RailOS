"""
eta-service

Consumes raw position events from the `train:positions` Redis Stream using a
consumer group (so this service can be horizontally scaled -- run N replicas
and Redis will load-balance stream entries across them, each acking its own
work). For each event it estimates time-to-next-stop from current speed and
remaining distance, classifies delay severity, and republishes an enriched
event onto `train:eta`.

This is the "business logic" service in the pipeline -- it's intentionally
decoupled from both where positions come from (simulator today, a real GPS
feed tomorrow) and who consumes the enriched output (gateway, history-service,
anything else that subscribes later).
"""
import asyncio
import json
import logging
import math
import os

import redis.asyncio as redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s [eta-service] %(message)s")
log = logging.getLogger("eta-service")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
POSITIONS_STREAM = "train:positions"
ETA_STREAM = "train:eta"
GROUP = "eta-workers"
CONSUMER = os.getenv("HOSTNAME", "eta-worker-1")
ETA_MAXLEN = int(os.getenv("STREAM_MAXLEN", "5000"))

# rough remaining-distance-per-leg approximation: we don't have full route
# geometry here (that lives in the simulator), so we estimate remaining time
# on the *current leg* from leg_progress + speed. Good enough for a live ETA
# display; a production version would look up full route distance via a
# routes API/service.
AVG_LEG_KM = 120  # placeholder average leg length used for the estimate


def classify_delay(delay_min: float) -> str:
    if delay_min < 2:
        return "ON_TIME"
    if delay_min < 10:
        return "MINOR_DELAY"
    return "SEVERE_DELAY"


def compute_eta_minutes(event: dict) -> float:
    speed_kmh = max(event.get("speed_kmh", 60), 1)
    remaining_fraction = max(1 - event.get("leg_progress", 0), 0)
    remaining_km = AVG_LEG_KM * remaining_fraction
    return round((remaining_km / speed_kmh) * 60, 1)


async def ensure_group(client: redis.Redis):
    try:
        await client.xgroup_create(POSITIONS_STREAM, GROUP, id="$", mkstream=True)
        log.info("created consumer group %s on %s", GROUP, POSITIONS_STREAM)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
        log.info("consumer group %s already exists", GROUP)


async def run():
    client = redis.from_url(REDIS_URL, decode_responses=True)
    await client.ping()
    await ensure_group(client)
    log.info("consuming %s as %s/%s", POSITIONS_STREAM, GROUP, CONSUMER)

    while True:
        resp = await client.xreadgroup(GROUP, CONSUMER, {POSITIONS_STREAM: ">"}, count=20, block=5000)
        if not resp:
            continue

        for _stream, entries in resp:
            for entry_id, fields in entries:
                try:
                    event = json.loads(fields["data"])
                    eta_min = compute_eta_minutes(event)
                    enriched = {
                        **event,
                        "eta_min": eta_min,
                        "delay_status": classify_delay(event.get("delay_min", 0)),
                    }
                    await client.xadd(ETA_STREAM, {"data": json.dumps(enriched)}, maxlen=ETA_MAXLEN, approximate=True)
                    await client.xack(POSITIONS_STREAM, GROUP, entry_id)
                except Exception:
                    log.exception("failed to process entry %s -- leaving unacked for retry", entry_id)


if __name__ == "__main__":
    asyncio.run(run())
