# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Observability and guardrail helpers for migration workflows."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


def compute_json_size_bytes(value: Any) -> int:
    """Compute approximate JSON payload size in bytes."""
    try:
        serialized = json.dumps(value, default=str)
    except (TypeError, ValueError):
        serialized = str(value)
    return len(serialized.encode("utf-8"))


def compute_nesting_depth(value: Any) -> int:
    """Compute nesting depth for dict/list values."""
    if isinstance(value, dict):
        if not value:
            return 1
        return 1 + max(compute_nesting_depth(child) for child in value.values())
    if isinstance(value, list):
        if not value:
            return 1
        return 1 + max(compute_nesting_depth(child) for child in value)
    return 0


def should_guardrail_route_to_jsonb(value: Any, max_bytes: int, max_depth: int) -> tuple[bool, int, int, str | None]:
    """Return guardrail decision and diagnostics for a field value."""
    size_bytes = compute_json_size_bytes(value)
    nesting_depth = compute_nesting_depth(value)

    if size_bytes > max_bytes:
        return True, size_bytes, nesting_depth, "field_size_exceeded"
    if nesting_depth > max_depth:
        return True, size_bytes, nesting_depth, "field_nesting_exceeded"
    return False, size_bytes, nesting_depth, None


def build_guardrail_event(
    column: str,
    reason: str,
    size_bytes: int,
    nesting_depth: int,
) -> dict[str, Any]:
    """Build a structured guardrail event for metadata tracking."""
    return {
        "event": "guardrail_route_to_jsonb",
        "column": column,
        "reason": reason,
        "size_bytes": size_bytes,
        "nesting_depth": nesting_depth,
        "timestamp": datetime.now(tz=UTC).isoformat(),
    }


@dataclass(frozen=True)
class MigrationMetrics:
    """Structured migration metrics payload."""

    phase: str
    rows_processed: int
    rows_inserted: int
    rows_per_second: float
    jsonb_spill_ratio: float
    drift_rate: float
    guardrail_event_count: int

    def as_dict(self) -> dict[str, Any]:
        """Serialize metrics to a metadata-friendly dictionary."""
        return {
            "phase": self.phase,
            "rows_processed": self.rows_processed,
            "rows_inserted": self.rows_inserted,
            "rows_per_second": self.rows_per_second,
            "jsonb_spill_ratio": self.jsonb_spill_ratio,
            "drift_rate": self.drift_rate,
            "guardrail_event_count": self.guardrail_event_count,
            "timestamp": datetime.now(tz=UTC).isoformat(),
        }


def build_migration_metrics(
    phase: str,
    rows_processed: int,
    rows_inserted: int,
    rows_per_second: float,
    overflow_key_count: int,
    total_key_count: int,
    drift_event_count: int,
    guardrail_event_count: int,
) -> MigrationMetrics:
    """Build normalized migration metrics from tracked counters."""
    jsonb_spill_ratio = 0.0
    if total_key_count > 0:
        jsonb_spill_ratio = overflow_key_count / total_key_count

    drift_rate = 0.0
    if rows_processed > 0:
        drift_rate = drift_event_count / rows_processed

    return MigrationMetrics(
        phase=phase,
        rows_processed=rows_processed,
        rows_inserted=rows_inserted,
        rows_per_second=rows_per_second,
        jsonb_spill_ratio=jsonb_spill_ratio,
        drift_rate=drift_rate,
        guardrail_event_count=guardrail_event_count,
    )
