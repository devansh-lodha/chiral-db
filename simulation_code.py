# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Simulation Server code provided by TA."""

import asyncio
import json
import random
import secrets
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from typing import Any

from faker import Faker
from fastapi import FastAPI
from sse_starlette.sse import EventSourceResponse

# Use SystemRandom for secure random number generation to satisfy security linters
secure_random = random.SystemRandom()

app = FastAPI()
faker = Faker()

# Constants
NESTED_METADATA_PROBABILITY = 0.4
SPARSE_METADATA_PROBABILITY = 0.5
MIN_FIELD_WEIGHT = 0.05
MAX_FIELD_WEIGHT = 0.95
USER_POOL_SIZE = 1000
STREAM_DELAY = 0.01

# 1. Unique Field Constraint: Persistent Pool of 1,000 users (The Glue)
USER_POOL = [faker.user_name() for _ in range(USER_POOL_SIZE)]

# Original pool of 50 realistic fields preserved
FIELD_POOL = {
    "name": lambda: faker.name(),
    "age": lambda: secure_random.randint(18, 70),
    "email": lambda: faker.email(),
    "phone": lambda: faker.phone_number(),
    "ip_address": lambda: faker.ipv4(),
    "device_id": lambda: faker.uuid4(),
    "device_model": lambda: secrets.choice(["iPhone 14", "Pixel 8", "Samsung S23", "OnePlus 12"]),
    "os": lambda: secrets.choice(["Android", "iOS", "Windows", "Linux", "MacOS"]),
    "app_version": lambda: f"v{secure_random.randint(1, 5)}."
    f"{secure_random.randint(0, 9)}.{secure_random.randint(0, 9)}",
    "battery": lambda: secure_random.randint(1, 100),
    "charging": lambda: secrets.choice([True, False]),
    "network": lambda: secrets.choice(["WiFi", "4G", "5G", "Ethernet", "Offline"]),
    "gps_lat": lambda: float(faker.latitude()),
    "gps_lon": lambda: float(faker.longitude()),
    "altitude": lambda: round(secure_random.uniform(1, 3000), 2),
    "speed": lambda: round(secure_random.uniform(0, 120), 2),
    "direction": lambda: secrets.choice(["N", "S", "E", "W"]),
    "city": lambda: faker.city(),
    "country": lambda: faker.country(),
    "postal_code": lambda: faker.postcode(),
    "timestamp": lambda: datetime.now(tz=UTC).isoformat(),
    "session_id": lambda: faker.uuid4(),
    "steps": lambda: secure_random.randint(0, 12000),
    "heart_rate": lambda: secure_random.randint(60, 180),
    "spo2": lambda: secure_random.randint(90, 100),
    "sleep_hours": lambda: round(secure_random.uniform(3, 9), 1),
    "stress_level": lambda: secrets.choice(["low", "medium", "high"]),
    "mood": lambda: secrets.choice(["happy", "sad", "neutral", "angry", "excited"]),
    "weather": lambda: secrets.choice(["sunny", "rainy", "cloudy", "stormy", "snow"]),
    "temperature_c": lambda: round(secure_random.uniform(-10, 45), 1),
    "humidity": lambda: secure_random.randint(10, 100),
    "air_quality": lambda: secrets.choice(["good", "moderate", "bad", "hazardous"]),
    "action": lambda: secrets.choice(["login", "logout", "view", "click", "purchase"]),
    "purchase_value": lambda: round(secure_random.uniform(5, 500), 2),
    "item": lambda: secrets.choice(["book", "phone", "shoes", "bag", "laptop", None]),
    "payment_status": lambda: secrets.choice(["success", "failed", "pending"]),
    "subscription": lambda: secrets.choice(["free", "trial", "basic", "premium"]),
    "language": lambda: faker.language_name(),
    "timezone": lambda: faker.timezone(),
    "cpu_usage": lambda: secure_random.randint(1, 100),
    "ram_usage": lambda: secure_random.randint(1, 100),
    "disk_usage": lambda: secure_random.randint(1, 100),
    "signal_strength": lambda: secure_random.randint(1, 5),
    "error_code": lambda: secrets.choice([None, 100, 200, 500, 404, 403]),
    "retry_count": lambda: secure_random.randint(0, 5),
    "is_active": lambda: secrets.choice([True, False]),
    "is_background": lambda: secrets.choice([True, False]),
    "comment": lambda: faker.sentence(),
    "avatar_url": lambda: faker.image_url(),
    "last_seen": lambda: (datetime.now(tz=UTC) - timedelta(minutes=secure_random.randint(1, 300))).isoformat(),
    "friends_count": lambda: secure_random.randint(0, 5000),
}

# --- NEW: BIAS LOGIC (Randomness inside Randomness) ---
# Each field gets a permanent "Appearance Probability" for this server session.
# Some will be > 0.8 (Common/SQL candidates), some < 0.2 (Rare/Mongo candidates).
FIELD_WEIGHTS = {key: secure_random.uniform(MIN_FIELD_WEIGHT, MAX_FIELD_WEIGHT) for key in FIELD_POOL}


def get_nested_metadata() -> dict[str, Any] | None:
    """Generate consistent nested keys but randomly omits keys AND values."""
    # We define the full potential structure
    full_meta = {
        "sensor_data": {
            "version": "2.1",
            "calibrated": secrets.choice([True, False]),
            "readings": [secure_random.randint(1, 10) for _ in range(3)],
        },
        "tags": [faker.word() for _ in range(secure_random.randint(1, 3))],
        "is_bot": secrets.choice([True, False]),
        "internal_id": faker.bothify(text="ID-####-??"),
    }

    # Heuristic: Randomly drop keys within the nested object (50% chance to drop each key)
    sparse_meta = {k: v for k, v in full_meta.items() if secure_random.random() > SPARSE_METADATA_PROBABILITY}

    # If it's empty, we return None so the field doesn't even appear
    return sparse_meta if sparse_meta else None


def generate_record() -> dict[str, Any]:
    """Generate a single random data record."""
    # Start with the mandatory Username (100% frequency)
    record: dict[str, Any] = {"username": secrets.choice(USER_POOL)}

    # 1. Flat fields: instead of fixed count, we use the pre-defined WEIGHTS
    for key, weight in FIELD_WEIGHTS.items():
        if secure_random.random() < weight:
            record[key] = FIELD_POOL[key]()

    # 2. Nesting: Consistent structure name, but internal data is sparse
    if secure_random.random() > NESTED_METADATA_PROBABILITY:  # 60% chance to include the metadata block
        meta_content = get_nested_metadata()
        if meta_content:
            record["metadata"] = meta_content

    return record


@app.get("/")
async def single_record() -> dict[str, Any]:
    """Return a single generated record."""
    return generate_record()


@app.get("/record/{count}")
async def stream_records(count: int) -> EventSourceResponse:
    """Stream a specified number of generated records using Server-Sent Events."""

    async def event_generator() -> AsyncGenerator[dict[str, Any]]:
        for _ in range(count):
            await asyncio.sleep(STREAM_DELAY)  # Reduced sleep for high-volume 100k tests
            yield {"event": "record", "data": json.dumps(generate_record())}

    return EventSourceResponse(event_generator())
