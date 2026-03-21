# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Helpers for metadata snapshot loading and schema evolution tracking."""

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


@dataclass(frozen=True)
class MetadataSnapshot:
    """Deterministic session metadata snapshot used by migrators."""

    schema: dict[str, Any]
    schema_version: int
    drift_events: list[dict[str, Any]]
    safety_events: list[dict[str, Any]]
    migration_metrics: list[dict[str, Any]]


def _coerce_json(value: Any, default: Any) -> Any:
    """Coerce DB JSON/text payload to python objects with a default fallback."""
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return value


def build_drift_event(column_name: str, previous_type: str | None) -> dict[str, Any]:
    """Build a normalized drift event record."""
    return {
        "event": "column_migrated_to_jsonb",
        "column": column_name,
        "previous_type": previous_type,
        "target": "jsonb",
        "timestamp": datetime.now(tz=UTC).isoformat(),
    }


def apply_drift_to_metadata(
    schema: dict[str, Any],
    drift_events: list[dict[str, Any]],
    column_name: str,
) -> tuple[dict[str, Any], list[dict[str, Any]], int]:
    """Apply a drift event and return updated schema/events/version increment.

    Returns:
        (updated_schema, updated_drift_events, schema_version_increment)

    """
    updated_schema = dict(schema)
    previous_type = None

    if column_name in updated_schema:
        previous_type = updated_schema[column_name].get("type")
        updated_schema[column_name]["target"] = "jsonb"
        updated_schema[column_name]["routing_reason"] = "type_drift"

    updated_events = [*drift_events, build_drift_event(column_name, previous_type)]
    return updated_schema, updated_events, 1


async def load_metadata_snapshot(sql_session: AsyncSession, session_id: str) -> MetadataSnapshot | None:
    """Load and normalize metadata snapshot for deterministic incremental behavior."""
    result = await sql_session.execute(
        text(
            "SELECT schema_json, schema_version, drift_events, safety_events, migration_metrics "
            "FROM session_metadata WHERE session_id = :sid"
        ),
        {"sid": session_id},
    )
    row = result.fetchone()
    if not row:
        return None

    raw_schema, raw_version, raw_drift_events, raw_safety_events, raw_migration_metrics = row
    schema = _coerce_json(raw_schema, {})
    drift_events = _coerce_json(raw_drift_events, [])
    safety_events = _coerce_json(raw_safety_events, [])
    migration_metrics = _coerce_json(raw_migration_metrics, [])
    schema_version = int(raw_version or 1)

    if not isinstance(schema, dict):
        schema = {}
    if not isinstance(drift_events, list):
        drift_events = []
    if not isinstance(safety_events, list):
        safety_events = []
    if not isinstance(migration_metrics, list):
        migration_metrics = []

    return MetadataSnapshot(
        schema=schema,
        schema_version=schema_version,
        drift_events=drift_events,
        safety_events=safety_events,
        migration_metrics=migration_metrics,
    )


def bounded_append_events(
    existing: list[dict[str, Any]],
    new_events: list[dict[str, Any]],
    max_events: int,
) -> list[dict[str, Any]]:
    """Append events and keep only the most recent max_events entries."""
    combined = [*existing, *new_events]
    if max_events <= 0:
        return []
    if len(combined) <= max_events:
        return combined
    return combined[-max_events:]
