"""
ingestion-service (a.k.a. "simulator")

Simulates GPS pings for a fleet of trains moving along predefined routes and
publishes each position update onto a Redis Stream (`train:positions`).

This stands in for what would, in a real system, be a feed adapter for
GTFS-realtime / an ACSES-style position broadcast. Downstream services never
need to know positions are simulated -- they just consume the stream.
"""
import asyncio
import json
import logging
import math
import os
import random
import time
from datetime import datetime, timezone

import redis.asyncio as redis

from routes import ROUTES

logging.basicConfig(level=logging.INFO, format="%(asctime)s [simulator] %(message)s")
log = logging.getLogger("simulator")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
POSITIONS_STREAM = "train:positions"
TICK_SECONDS = float(os.getenv("TICK_SECONDS", "1.5"))
STREAM_MAXLEN = int(os.getenv("STREAM_MAXLEN", "5000"))


def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def interpolate(a, b, t):
    return a + (b - a) * t


class Train:
    """A single simulated train progressing along a route's stop sequence."""

    def __init__(self, train_id: str, route_id: str, speed_kmh: float):
        self.train_id = train_id
        self.route_id = route_id
        self.stops = ROUTES[route_id]["stops"]
        self.leg_index = 0  # index of the stop we're currently departing from
        self.leg_progress = random.uniform(0, 0.15)  # 0..1 along the current leg
        self.base_speed_kmh = speed_kmh
        self.delay_min = 0.0
        self.status = "EN_ROUTE"

    def current_and_next_stop(self):
        a = self.stops[self.leg_index]
        b = self.stops[min(self.leg_index + 1, len(self.stops) - 1)]
        return a, b

    def position(self):
        a, b = self.current_and_next_stop()
        lat = interpolate(a["lat"], b["lat"], self.leg_progress)
        lon = interpolate(a["lon"], b["lon"], self.leg_progress)
        return lat, lon

    def tick(self, dt_seconds: float):
        if self.leg_index >= len(self.stops) - 1:
            self.status = "TERMINATED"
            return

        a, b = self.current_and_next_stop()
        leg_km = max(haversine_km(a["lat"], a["lon"], b["lat"], b["lon"]), 0.001)

        # small random speed jitter + occasional slow-down to simulate real delay
        speed = self.base_speed_kmh * random.uniform(0.85, 1.05)
        if random.random() < 0.03:
            speed *= 0.2  # simulate a signal hold / slow zone
            self.delay_min += dt_seconds / 60 * 0.8

        km_moved = speed * (dt_seconds / 3600)
        self.leg_progress += km_moved / leg_km

        if self.leg_progress >= 1.0:
            self.leg_progress = 0.0
            self.leg_index += 1
            self.status = "AT_STATION" if self.leg_index < len(self.stops) - 1 else "TERMINATED"
        else:
            self.status = "EN_ROUTE"

    def to_event(self):
        lat, lon = self.position()
        a, b = self.current_and_next_stop()
        return {
            "train_id": self.train_id,
            "route_id": self.route_id,
            "route_name": ROUTES[self.route_id]["name"],
            "lat": round(lat, 5),
            "lon": round(lon, 5),
            "status": self.status,
            "last_stop": a["name"],
            "next_stop": b["name"],
            "leg_progress": round(self.leg_progress, 4),
            "delay_min": round(self.delay_min, 1),
            "speed_kmh": round(self.base_speed_kmh, 1),
            "ts": datetime.now(timezone.utc).isoformat(),
        }


def build_fleet():
    fleet = []
    specs = [
        ("101A", "NEC-101", 110),
        ("101B", "NEC-101", 105),
        ("220A", "CAP-220", 95),
        ("501A", "PSW-501", 90),
        ("501B", "PSW-501", 88),
    ]
    for train_id, route_id, speed in specs:
        fleet.append(Train(train_id, route_id, speed))
    return fleet


async def run():
    client = redis.from_url(REDIS_URL, decode_responses=True)
    await client.ping()
    log.info("connected to redis at %s", REDIS_URL)

    fleet = build_fleet()
    log.info("simulating %d trains across %d routes", len(fleet), len(ROUTES))

    last = time.monotonic()
    while True:
        now = time.monotonic()
        dt = now - last
        last = now

        for train in fleet:
            train.tick(dt)
            if train.status == "TERMINATED":
                continue
            event = train.to_event()
            await client.xadd(POSITIONS_STREAM, {"data": json.dumps(event)}, maxlen=STREAM_MAXLEN, approximate=True)

        # respawn any terminated trains after a short pause so the demo runs forever
        for i, train in enumerate(fleet):
            if train.status == "TERMINATED" and random.random() < 0.02:
                fleet[i] = Train(train.train_id, train.route_id, train.base_speed_kmh)

        await asyncio.sleep(TICK_SECONDS)


if __name__ == "__main__":
    asyncio.run(run())
