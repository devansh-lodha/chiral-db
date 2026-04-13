# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Tests for benchmark workload builders and performance summaries."""

from scripts.performance_benchmark import (
    _normalize_just_argument,
    build_default_requests,
    build_drift_record,
    build_flat_record,
    build_mixed_record,
    build_nested_record,
)
from src.chiral.db.performance import OperationTiming, calculate_rows_per_second, percentile, summarize_timings


def test_workload_builders_produce_expected_shapes() -> None:
    """Benchmark record builders should cover the intended backend shapes."""
    flat = build_flat_record(1, session_id="session-a")
    nested = build_nested_record(2, session_id="session-a")
    mixed = build_mixed_record(3, session_id="session-a")
    drift_even = build_drift_record(4, session_id="session-a")
    drift_odd = build_drift_record(5, session_id="session-a")

    assert flat["session_id"] == "session-a"
    assert "temperature" in flat
    assert "humidity" in flat
    assert isinstance(nested["profile"], dict)
    assert isinstance(nested["events"], list)
    assert isinstance(mixed["profile"], dict)
    assert isinstance(mixed["tags"], list)
    assert isinstance(drift_even["temperature"], int)
    assert isinstance(drift_odd["temperature"], str)


def test_just_argument_normalizer_strips_named_assignments() -> None:
    """Just-style NAME=value arguments should normalize to raw values."""
    assert _normalize_just_argument("SESSION_ID=session_assignment_2") == "session_assignment_2"
    assert _normalize_just_argument("SIZE=25") == "25"
    assert _normalize_just_argument("WORKLOAD=all") == "all"
    assert _normalize_just_argument("plain-value") == "plain-value"


def test_default_requests_include_create_and_session_id() -> None:
    """Benchmark requests should include a nested create workload and hydrated session ids."""
    requests = build_default_requests("session-a")

    assert requests[0]["operation"] == "create"
    assert requests[0]["session_id"] == "session-a"
    assert requests[0]["payload"]["session_id"] == "session-a"
    assert requests[1]["session_id"] == "session-a"
    assert requests[2]["session_id"] == "session-a"


def test_percentile_interpolates_expected_values() -> None:
    """Percentile helper should interpolate between ordered samples."""
    values = [1.0, 2.0, 3.0, 4.0]
    assert percentile(values, 50) == 2.5
    assert percentile(values, 95) == 3.8499999999999996


def test_summarize_timings_tracks_latency_and_distribution() -> None:
    """Timing summaries should aggregate latency, throughput, and backend mix."""
    timings = [
        OperationTiming(
            operation="ingestion",
            phase="flat",
            latency_seconds=0.2,
            rows_processed=1,
            rows_inserted=1,
            sql_rows=1,
        ),
        OperationTiming(
            operation="ingestion",
            phase="flat",
            latency_seconds=0.4,
            rows_processed=1,
            rows_inserted=1,
            jsonb_rows=1,
        ),
        OperationTiming(
            operation="ingestion",
            phase="flat",
            latency_seconds=0.6,
            rows_processed=1,
            rows_inserted=1,
            child_rows=1,
        ),
    ]

    summary = summarize_timings(timings, operation="ingestion", phase="flat")

    assert summary.runs == 3
    assert abs(summary.average_latency_seconds - 0.4) < 1e-9
    assert abs(summary.p50_latency_seconds - 0.4) < 1e-9
    assert summary.p95_latency_seconds > 0.5
    assert abs(summary.throughput_ops_per_second - calculate_rows_per_second(3, 1.2)) < 1e-9
    assert summary.backend_distribution.sql_rows == 1
    assert summary.backend_distribution.jsonb_rows == 1
    assert summary.backend_distribution.child_rows == 1
    assert summary.backend_distribution.total_rows == 3
