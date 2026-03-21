# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Phase 8 observability and guardrail tests."""

from src.chiral.db.metadata_store import bounded_append_events
from src.chiral.db.observability import (
    build_guardrail_event,
    build_migration_metrics,
    compute_json_size_bytes,
    compute_nesting_depth,
    should_guardrail_route_to_jsonb,
)


def test_compute_json_size_bytes_non_zero() -> None:
    """JSON byte-size helper should return non-zero payload sizes."""
    assert compute_json_size_bytes({"a": 1}) > 0


def test_compute_nesting_depth_for_nested_value() -> None:
    """Nesting helper should detect depth for nested dict/list structures."""
    value = {"a": [{"b": {"c": 1}}]}
    assert compute_nesting_depth(value) >= 4


def test_guardrail_routes_large_payload_to_jsonb() -> None:
    """Oversized values should trigger size-based guardrail routing."""
    large_value = "x" * 100
    route, size_bytes, _, reason = should_guardrail_route_to_jsonb(large_value, max_bytes=10, max_depth=8)
    assert route
    assert size_bytes > 10
    assert reason == "field_size_exceeded"


def test_guardrail_routes_deep_payload_to_jsonb() -> None:
    """Overly nested values should trigger depth-based guardrail routing."""
    deep_value = {"a": {"b": {"c": 1}}}
    route, _, depth, reason = should_guardrail_route_to_jsonb(deep_value, max_bytes=10000, max_depth=2)
    assert route
    assert depth > 2
    assert reason == "field_nesting_exceeded"


def test_build_migration_metrics_spill_and_drift_rates() -> None:
    """Migration metrics builder should compute spill ratio and drift rate."""
    metrics = build_migration_metrics(
        phase="incremental",
        rows_processed=10,
        rows_inserted=9,
        rows_per_second=50.0,
        overflow_key_count=5,
        total_key_count=20,
        drift_event_count=2,
        guardrail_event_count=1,
    ).as_dict()

    assert metrics["jsonb_spill_ratio"] == 0.25
    assert metrics["drift_rate"] == 0.2
    assert metrics["guardrail_event_count"] == 1


def test_bounded_append_events_keeps_latest() -> None:
    """Bounded event append should retain only newest events."""
    existing = [{"i": 1}, {"i": 2}]
    new_events = [{"i": 3}, {"i": 4}]
    bounded = bounded_append_events(existing, new_events, max_events=3)
    assert bounded == [{"i": 2}, {"i": 3}, {"i": 4}]


def test_build_guardrail_event_shape() -> None:
    """Guardrail event should expose required audit fields."""
    event = build_guardrail_event("payload", "field_size_exceeded", 200, 1)
    assert event["event"] == "guardrail_route_to_jsonb"
    assert event["column"] == "payload"
    assert event["reason"] == "field_size_exceeded"
    assert event["size_bytes"] == 200
    assert event["nesting_depth"] == 1
    assert "timestamp" in event
