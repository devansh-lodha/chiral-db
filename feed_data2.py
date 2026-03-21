# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Nested data feeder to validate decomposition table creation and join queries."""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from datetime import UTC, datetime

import httpx

CHIRAL_API_URL = "http://127.0.0.1:8000/ingest"
SESSION_ID = "session_assignment_1"
FLUSH_API_URL = f"http://127.0.0.1:8000/flush/{SESSION_ID}"
TOTAL_RECORDS = 1000
LOG_INTERVAL = 100
SUCCESS_STATUS = 200

logging.basicConfig(level=logging.INFO, format="%(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def _build_comment(comment_id: int) -> dict[str, object]:
    return {
        "comment_id": comment_id,
        "text": f"comment-{comment_id}",
        "score": round(random.uniform(0.0, 1.0), 3),
        "meta": {
            "lang": random.choice(["en", "fr", "de"]),
            "sentiment": random.choice(["positive", "neutral", "negative"]),
        },
    }


def _build_event(event_id: int) -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_type": random.choice(["view", "click", "purchase"]),
        "amount": round(random.uniform(5, 500), 2),
        "extra": {
            "campaign": random.choice(["summer", "winter", "flash"]),
            "region": random.choice(["us", "eu", "apac"]),
        },
    }


def _generate_record(index: int) -> dict[str, object]:
    now = time.time()
    comment_count = random.randint(1, 4)
    event_count = random.randint(1, 3)

    return {
        "session_id": SESSION_ID,
        "username": f"user_{index % 50}",
        "sys_ingested_at": now,
        "t_stamp": now,
        "city": random.choice(["Paris", "Berlin", "Tokyo", "Delhi"]),
        "temperature": random.randint(15, 40),
        "device": random.choice(["android", "ios", "web"]),
        "metadata": {
            "source": "feed_data2",
            "generated_at": datetime.now(tz=UTC).isoformat(),
            "version": "2.0",
        },
        "comments": [_build_comment(index * 10 + offset) for offset in range(comment_count)],
        "events": [_build_event(index * 10 + offset) for offset in range(event_count)],
    }


async def feed() -> None:
    timeout = httpx.Timeout(30.0, read=None)
    async with httpx.AsyncClient(timeout=timeout) as client:
        logger.info("[Feeder2] Sending %d nested records to %s", TOTAL_RECORDS, CHIRAL_API_URL)

        sent = 0
        for index in range(TOTAL_RECORDS):
            payload = {"data": _generate_record(index)}
            response = await client.post(CHIRAL_API_URL, json=payload)
            if response.status_code == SUCCESS_STATUS:
                sent += 1
                if sent % LOG_INTERVAL == 0:
                    logger.info("[Feeder2] Ingested %d records...", sent)
            else:
                logger.error("[Feeder2] Error %d: %s", response.status_code, response.text)

        logger.info("[Feeder2] Finished. Total records sent: %d", sent)
        logger.info("[Feeder2] Sending flush signal...")
        response = await client.post(FLUSH_API_URL)
        if response.status_code == SUCCESS_STATUS:
            logger.info("[Feeder2] Flush successful: %s", json.dumps(response.json()))
        else:
            logger.error("[Feeder2] Flush failed: %s", response.text)


if __name__ == "__main__":
    asyncio.run(feed())
