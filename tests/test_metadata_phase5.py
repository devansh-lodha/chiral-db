# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Phase 5 metadata versioning and drift-event tests."""

from src.chiral.db.metadata_store import apply_drift_to_metadata, build_drift_event


def test_build_drift_event_contains_required_fields() -> None:
    """Drift event payload should include event semantics and JSONB target."""
    event = build_drift_event("temperature", "float")
    assert event["event"] == "column_migrated_to_jsonb"
    assert event["column"] == "temperature"
    assert event["previous_type"] == "float"
    assert event["target"] == "jsonb"
    assert "timestamp" in event


def test_apply_drift_to_metadata_updates_schema_and_increments_version() -> None:
    """Applying drift should route field to JSONB and increment schema version by 1."""
    schema = {
        "temperature": {
            "target": "sql",
            "type": "float",
            "routing_reason": "stable_scalar",
        }
    }
    drift_events: list[dict[str, str]] = []

    updated_schema, updated_events, increment = apply_drift_to_metadata(schema, drift_events, "temperature")

    assert updated_schema["temperature"]["target"] == "jsonb"
    assert updated_schema["temperature"]["routing_reason"] == "type_drift"
    assert len(updated_events) == 1
    assert updated_events[0]["column"] == "temperature"
    assert increment == 1


def test_apply_drift_to_metadata_handles_unknown_column() -> None:
    """Unknown columns should still append drift events without crashing."""
    schema = {"known": {"target": "sql", "type": "int"}}
    updated_schema, updated_events, increment = apply_drift_to_metadata(schema, [], "missing")

    assert "known" in updated_schema
    assert len(updated_events) == 1
    assert updated_events[0]["column"] == "missing"
    assert increment == 1
