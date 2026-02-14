# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Data ingestion script for Chiral DB Assignment 1."""

import asyncio
import json
import logging

import httpx

# TA's Simulation URL (Port 8001)
SIMULATION_URL = "http://127.0.0.1:8001/record/1000"
CHIRAL_API_URL = "http://127.0.0.1:8000/ingest"
SESSION_ID = "session_assignment_1"
SUCCESS_STATUS = 200
LOG_INTERVAL = 100

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


async def feed() -> None:
    """Stream data from the simulation server and feed it to the Chiral API."""
    timeout = httpx.Timeout(30.0, read=None)
    async with httpx.AsyncClient(timeout=timeout) as client:
        logger.info("[Feeder] Connecting to Simulation at %s...", SIMULATION_URL)
        count = 0
        async with client.stream("GET", SIMULATION_URL) as response:
            async for line in response.aiter_lines():
                stripped_line = line.strip()
                if stripped_line.startswith("data:"):
                    json_str = stripped_line[5:].strip()
                    try:
                        record = json.loads(json_str)
                        record["session_id"] = SESSION_ID
                        payload = {"data": record}

                        resp = await client.post(CHIRAL_API_URL, json=payload)
                        if resp.status_code == SUCCESS_STATUS:
                            count += 1
                            if count % LOG_INTERVAL == 0:
                                logger.info("[Feeder] Ingested %d records...", count)
                        else:
                            logger.error("[Feeder] Error %d: %s", resp.status_code, resp.text)

                    except (json.JSONDecodeError, httpx.RequestError):
                        logger.exception("[Feeder] Exception processing line")

        logger.info("[Feeder] Finished. Total records sent: %d", count)


if __name__ == "__main__":
    asyncio.run(feed())
