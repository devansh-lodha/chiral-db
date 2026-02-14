# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Worker Analysis Logic."""

from typing import Any

from motor.motor_asyncio import AsyncIOMotorDatabase
from sqlalchemy.ext.asyncio import AsyncSession

from chiral.db.sessions import session
from chiral.utils.heuristics import calculate_entropy


@session
async def analyze_staging(
    mongo_db: AsyncIOMotorDatabase,
    sql_session: AsyncSession,  # noqa: ARG001
) -> dict[str, Any]:
    """Analyze the first 100 documents in the staging collection to determine the schema.

    Args:
        mongo_db: Injected MongoDB database.
        sql_session: Injected SQL session (unused but required by decorator).

    Returns:
        Dictionary containing column metadata and placement decisions.
        {
            "column_name": {
                "unique": bool,
                "entropy": float,
                "target": "sql" | "mongo",
                "python_type": "int" | "str" | "float" | ...
            },
            ...
        }

    """
    collection_name = "staging"
    collection = mongo_db[collection_name]

    # 1. Fetch 100 documents (excluding _id)
    # We strip _id because it's Mongo internal
    docs = await collection.find({}, projection={"_id": 0}).to_list(length=100)

    if not docs:
        return {}

    # 2. Pivot data to organize by column (attribute)
    # Different documents might have different keys, we need the union of all keys.
    columns: dict[str, list[Any]] = {}

    for doc in docs:
        for key, value in doc.items():
            if key not in columns:
                columns[key] = []
            columns[key].append(value)

    # Normalize lengths (handle missing values if schematic differences exist)
    # Although the assignment implies checking these 100 creates the baseline.
    total_docs = len(docs)

    analysis_result = {}

    for col_name, values in columns.items():
        # Skip system columns: bi-temporal timestamps and traceability field
        # These are handled explicitly in migrator (no hardcoding of field mappings)
        if col_name in ["sys_ingested_at", "t_stamp", "username"]:
            continue

        # Fill missing values with None for accurate analysis if needed,
        # or just analyze present values. Let's analyze present values.

        # 3. Uniqueness Check
        # "check if all row is unique for that attribute"

        try:
            is_unique = len(set(values)) == len(values) and len(values) == total_docs
        except TypeError:
            is_unique = False

        # 4. Entropy Calculation
        entropy = calculate_entropy(values)

        # 5. Type Inference (Basic)
        # We look at the first non-None value or try to cast all
        # This is needed for the "Mixed Data Handling" later, but here we just describe.
        inferred_type = infer_type(values)

        # 6. Placement Decision (Based on Type Entropy)
        # Type Entropy Rules:
        # - H = 0: Perfect type stability (all same type) → SQL
        # - H > 0: Type drift (mixed types) → MongoDB
        # - dict/list types always → MongoDB (nested structures)

        if inferred_type in {"dict", "list"}:
            target = "mongo"  # Nested structures always go to Mongo
        elif entropy > 0:
            target = "mongo"  # Type drift detected → MongoDB
        else:
            target = "sql"  # Perfect type stability → SQL

        analysis_result[col_name] = {
            "unique": is_unique,
            "entropy": entropy,
            "target": target,
            "type": inferred_type,
        }

    return analysis_result


def infer_type(values: list[Any]) -> str:
    """Infer the dominant Python type from a list of values."""
    if not values:
        return "str"

    # Check first non-none
    valid_values = [v for v in values if v is not None]
    if not valid_values:
        return "str"

    # If any value mismatches the first type, it's mixed (treat as str or object)
    first_type = type(valid_values[0])
    if any(type(x) is not first_type for x in valid_values):
        return "str"

    # Map type to string representation (bool before int since bool is int subclass)
    first_value = valid_values[0]
    type_map = {
        bool: "bool",
        int: "int",
        float: "float",
        dict: "dict",
        list: "list",
    }
    for type_cls, type_name in type_map.items():
        if isinstance(first_value, type_cls):
            return type_name
    return "str"
