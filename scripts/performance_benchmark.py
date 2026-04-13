# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Benchmark runner for the hybrid database framework.

This module provides reusable workload builders and timing helpers for
measuring ingestion latency, logical query latency, metadata lookup overhead,
and transaction coordination overhead.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

from chiral.core.ingestion import ingest_data
from chiral.core.query_service import execute_json_request, translate_json_request_with_metadata
from chiral.db.performance import OperationTiming, summarize_timings

PROJECT_SRC = Path(__file__).resolve().parent.parent / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))


@dataclass(frozen=True)
class BenchmarkWorkload:
    """Collection of records or requests used in a benchmark run."""

    name: str
    items: list[dict[str, Any]]


def build_flat_record(index: int, *, session_id: str) -> dict[str, Any]:
    return {
        "session_id": session_id,
        "username": f"user_{index}",
        "temperature": 20 + (index % 10),
        "humidity": 40 + (index % 15),
        "t_stamp": float(index),
    }


def build_nested_record(index: int, *, session_id: str) -> dict[str, Any]:
    return {
        "session_id": session_id,
        "username": f"nested_user_{index}",
        "profile": {
            "address": {"city": f"city_{index % 4}", "zip": 10000 + index},
            "flags": ["active", "verified"],
        },
        "events": [
            {"kind": "click", "value": index},
            {"kind": "scroll", "value": index * 2},
        ],
        "t_stamp": float(index),
    }


def build_mixed_record(index: int, *, session_id: str) -> dict[str, Any]:
    return {
        "session_id": session_id,
        "username": f"mixed_user_{index}",
        "temperature": 20 + (index % 10),
        "profile": {"city": f"city_{index % 4}", "score": float(index) / 10},
        "tags": ["a", "b", str(index)],
        "t_stamp": float(index),
    }


def build_drift_record(index: int, *, session_id: str) -> dict[str, Any]:
    value: Any
    value = index if index % 2 == 0 else f"drift_{index}"
    return {
        "session_id": session_id,
        "username": f"drift_user_{index}",
        "temperature": value,
        "t_stamp": float(index),
    }


def build_workload(name: str, *, session_id: str, size: int) -> BenchmarkWorkload:
    builders = {
        "flat": build_flat_record,
        "nested": build_nested_record,
        "mixed": build_mixed_record,
        "drift": build_drift_record,
    }
    if name not in builders:
        msg = f"Unsupported workload shape: {name}"
        raise ValueError(msg)

    builder = builders[name]
    return BenchmarkWorkload(
        name=name,
        items=[builder(index, session_id=session_id) for index in range(size)],
    )


async def _measure_async_operation(
    *,
    operation: str,
    phase: str,
    func: Callable[..., Any],
    rows_processed: int = 0,
    rows_inserted: int = 0,
    sql_rows: int = 0,
    jsonb_rows: int = 0,
    child_rows: int = 0,
    metadata_lookups: int = 0,
) -> tuple[OperationTiming, Any]:
    started = time.perf_counter()
    result = await func()
    elapsed = time.perf_counter() - started
    timing = OperationTiming(
        operation=operation,
        phase=phase,
        latency_seconds=elapsed,
        rows_processed=rows_processed,
        rows_inserted=rows_inserted,
        sql_rows=sql_rows,
        jsonb_rows=jsonb_rows,
        child_rows=child_rows,
        metadata_lookups=metadata_lookups,
    )
    return timing, result


async def benchmark_ingestion(session_id: str, workload: BenchmarkWorkload) -> dict[str, Any]:
    timings: list[OperationTiming] = []
    for record in workload.items:
        timing, _ = await _measure_async_operation(
            operation="ingestion",
            phase=workload.name,
            func=lambda record=record: ingest_data(data=record, session_id=session_id),
            rows_processed=1,
            rows_inserted=1,
            # Ingestion writes into staging_data JSONB.
            jsonb_rows=1,
        )
        timings.append(timing)
    return summarize_timings(timings, operation="ingestion", phase=workload.name).as_dict()


async def benchmark_query_execution(requests: list[dict[str, Any]]) -> dict[str, Any]:
    timings: list[OperationTiming] = []
    for request in requests:
        operation_name = str(request.get("operation", "read"))
        timing, result = await _measure_async_operation(
            operation=operation_name,
            phase="logical_execution",
            func=lambda request=request: execute_json_request(request),
        )
        rows_processed = int(result.get("row_count", result.get("affected_rows", 0)) or 0)
        timing = OperationTiming(
            operation=timing.operation,
            phase=timing.phase,
            latency_seconds=timing.latency_seconds,
            rows_processed=rows_processed,
        )
        timings.append(timing)
    return summarize_timings(timings, operation="logical_execution", phase="query").as_dict()


async def benchmark_metadata_lookup(request: dict[str, Any]) -> dict[str, Any]:
    session_id = str(request.get("session_id", ""))
    timings: list[OperationTiming] = []

    if session_id:
        timing, _ = await _measure_async_operation(
            operation="metadata_lookup",
            phase="hydration",
            func=lambda: translate_json_request_with_metadata(request),
            rows_processed=1,
            metadata_lookups=1,
        )
        timings.append(timing)

    return summarize_timings(timings, operation="metadata_lookup", phase="hydration").as_dict()


async def benchmark_transaction_coordination(requests: list[dict[str, Any]]) -> dict[str, Any]:
    timings: list[OperationTiming] = []
    for request in requests:
        operation_name = str(request.get("operation", "create"))
        timing, result = await _measure_async_operation(
            operation=operation_name,
            phase="coordination",
            func=lambda request=request: execute_json_request(request),
        )
        affected_rows = int(result.get("affected_rows", 0) or 0)
        child_rows = 0
        sql_rows = 0
        jsonb_rows = 0
        rows_inserted = affected_rows

        if operation_name == "create":
            child_counts = result.get("child_insert_counts", {})
            if isinstance(child_counts, dict):
                child_rows = sum(int(value or 0) for value in child_counts.values())
            sql_rows = affected_rows
            rows_inserted = affected_rows + child_rows

            # When sync create falls back, data is queued into staging_data JSONB.
            if str(result.get("mode", "")) == "queued_async":
                jsonb_rows = 1
                rows_inserted = max(rows_inserted, 1)

        timing = OperationTiming(
            operation=timing.operation,
            phase=timing.phase,
            latency_seconds=timing.latency_seconds,
            rows_processed=max(1, affected_rows or 1),
            rows_inserted=rows_inserted,
            sql_rows=sql_rows,
            jsonb_rows=jsonb_rows,
            child_rows=child_rows,
            metadata_lookups=1,
        )
        timings.append(timing)
    return summarize_timings(timings, operation="coordination", phase="logical_write").as_dict()


def build_default_requests(session_id: str) -> list[dict[str, Any]]:
    return [
        {
            "operation": "create",
            "table": "chiral_data",
            "session_id": session_id,
            "payload": {
                "session_id": session_id,
                "username": "benchmark_creator",
                "sys_ingested_at": 1742643301.25,
                "t_stamp": 1742643301.25,
                "comments": [
                    {"comment_id": 1, "text": "hello", "score": 0.5},
                    {"comment_id": 2, "text": "world", "score": 0.8},
                ],
            },
            "decomposition_plan": {
                "version": 1,
                "parent_table": "chiral_data",
                "entities": [
                    {
                        "source_field": "comments",
                        "child_table": "chiral_data_comments",
                        "relationship": "one_to_many",
                        "child_columns": ["comment_id", "text", "score"],
                        "child_column_types": {"comment_id": "int", "text": "str", "score": "float"},
                    }
                ],
            },
        },
        {
            "operation": "read",
            "table": "chiral_data",
            "session_id": session_id,
            "select": ["username", "sys_ingested_at"],
            "filters": [{"field": "session_id", "op": "eq", "value": session_id}],
            "limit": 10,
        },
        {
            "operation": "update",
            "table": "chiral_data",
            "session_id": session_id,
            "updates": {"username": "benchmark_user"},
            "filters": [{"field": "session_id", "op": "eq", "value": session_id}],
        },
        {
            "operation": "delete",
            "table": "chiral_data",
            "session_id": session_id,
            "filters": [{"field": "session_id", "op": "eq", "value": session_id}],
        },
    ]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run hybrid database performance benchmarks.")
    parser.add_argument("--session-id", required=True, help="Session identifier for the benchmark run.")
    parser.add_argument(
        "--workload",
        choices=["flat", "nested", "mixed", "drift", "all"],
        default="all",
        help="Workload shape to benchmark.",
    )
    parser.add_argument("--size", type=int, default=25, help="Number of records per workload.")
    return parser


def _normalize_just_argument(value: str) -> str:
    """Strip Just-style NAME=value wrappers from argument values.

    Just can forward recipe parameters as literal strings like ``SESSION_ID=abc``
    when the recipe invocation uses named arguments. This helper makes the
    benchmark runner tolerant of that shape while still accepting plain values.
    """
    for prefix in ("SESSION_ID=", "SIZE=", "WORKLOAD="):
        if value.startswith(prefix):
            return value[len(prefix) :]
    return value


async def _run_async(args: argparse.Namespace) -> dict[str, Any]:
    workload_names = [args.workload] if args.workload != "all" else ["flat", "nested", "mixed", "drift"]
    results: dict[str, Any] = {}

    for workload_name in workload_names:
        workload = build_workload(workload_name, session_id=args.session_id, size=args.size)
        results[f"ingestion_{workload_name}"] = await benchmark_ingestion(args.session_id, workload)

    requests = build_default_requests(args.session_id)
    results["metadata_lookup"] = await benchmark_metadata_lookup(requests[0])
    results["logical_execution"] = await benchmark_query_execution(requests)
    results["coordination"] = await benchmark_transaction_coordination(requests)
    return results


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    raw_argv = sys.argv[1:] if argv is None else argv
    normalized_argv = [_normalize_just_argument(arg) for arg in raw_argv]
    args = parser.parse_args(normalized_argv)
    results = asyncio.run(_run_async(args))
    print(json.dumps(results, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
