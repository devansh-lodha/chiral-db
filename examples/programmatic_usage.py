# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Programmatic Usage Example for ChiralDB."""

# examples/programmatic_usage.py
import asyncio
import json

from chiral.client import ChiralClient


async def main() -> None:
    # 1. Connect to your database using the ChiralClient
    print("Connecting to ChiralDB...")
    async with ChiralClient("postgresql+asyncpg://chiral:chiral@localhost:5432/chiral_db") as db:
        session_id = "demo_library_session"

        # 2. Autonomous Ingestion (Notice how nested data is mixed with flat data!)
        print("\n[1/3] Ingesting complex schema-less data...")
        record = {
            "username": "devansh_lodha",
            "age": 21,
            "university": "IIT Gandhinagar",
            "courses": [{"name": "CS 432 - Databases", "grade": "A"}, {"name": "CS 301 - Algorithms", "grade": "A"}],
        }
        await db.ingest(session_id=session_id, data=record)

        # Force flush so the background worker immediately analyzes and creates the SQL tables
        await db.flush(session_id)

        # 3. View the autonomously inferred logical schema
        print("\n[2/3] Fetching the inferred Logical Schema...")
        schema = await db.get_logical_schema(session_id)
        print("Logical Fields:", json.dumps(schema, indent=2))

        # 4. Execute a logical query using the library
        print("\n[3/3] Querying the data via the logical interface...")
        query_request = {
            "operation": "read",
            "session_id": session_id,
            "select": ["username", "courses.name"],
            "filters": [{"field": "age", "op": "gte", "value": 20}],
        }

        # Notice that ChiralDB automatically figures out the SQL LEFT JOIN for the courses array!
        result = await db.query(query_request)
        print(f"Rows returned: {result['row_count']}")
        print(json.dumps(result["rows"], indent=2))


if __name__ == "__main__":
    asyncio.run(main())
