# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Benchmark runner for the hybrid database framework."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

from chiral.client import ChiralClient
from chiral.config import get_settings
from chiral.db.performance import OperationTiming, summarize_timings

PROJECT_SRC = Path(__file__).resolve().parent.parent / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))


@dataclass(frozen=True)
class BenchmarkWorkload:
    """Collection of records or requests used in a benchmark run."""

    name: str
    items: list[dict[str, Any]]


def _build_record_sample(
    *,
    sample_index: int,
    timing: OperationTiming,
    request: dict[str, Any] | None = None,
    record: dict[str, Any] | None = None,
    result: dict[str, Any] | None = None,
    workload: str | None = None,
) -> dict[str, Any]:
    return {
        "sample_index": sample_index,
        "operation": timing.operation,
        "phase": timing.phase,
        "workload": workload or timing.phase,
        "latency_seconds": timing.latency_seconds,
        "rows_processed": timing.rows_processed,
        "rows_inserted": timing.rows_inserted,
        "sql_rows": timing.sql_rows,
        "jsonb_rows": timing.jsonb_rows,
        "child_rows": timing.child_rows,
        "metadata_lookups": timing.metadata_lookups,
        "request": request or {},
        "record": record or {},
        "result": result or {},
    }


def _write_record_artifacts(output_dir: Path, records: list[dict[str, Any]], *, session_id: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    records_json_path = output_dir / f"benchmark_records_{session_id}.json"
    records_json_path.write_text(json.dumps(records, indent=2, sort_keys=True, default=str), encoding="utf-8")


def _write_summary_artifact(output_dir: Path, summary: dict[str, Any], *, session_id: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / f"benchmark_summary_{session_id}.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True, default=str), encoding="utf-8")


def build_flat_record(index: int, *, session_id: str) -> dict[str, Any]:
    return {
        "session_id": session_id,
        "username": f"user_{index}",
        "temperature": 20,
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
    profile_value: Any = (
        {"address": f"addr_{index}", "city": f"city_{index % 4}"} if index % 2 == 0 else f"profile_{index}"
    )
    return {
        "session_id": session_id,
        "username": f"user_{index}",
        "profile": profile_value,
        "tags": ["a", index, {"k": "v"}],
        "attributes": {"score": index / 10, "flags": ["x", "y", index % 3]},
        "t_stamp": float(index),
    }


def build_drift_record(index: int, *, session_id: str) -> dict[str, Any]:
    value: Any = index if index % 2 == 0 else f"drift_{index}"
    if index % 5 == 0:
        value = {"value": index, "unit": "C"}
    return {
        "session_id": session_id,
        "username": f"user_{index}",
        "temperature": value,
        "profile": {"city": f"city_{index % 6}", "zip": 10000 + index} if index % 3 == 0 else f"p_{index}",
        "events": [{"kind": "click", "value": index}, "mixed_event", index],
        "t_stamp": float(index),
    }


def _estimate_routing_counts(record: dict[str, Any]) -> tuple[int, int, int]:
    sql_rows = 0
    jsonb_rows = 0
    child_rows = 0

    nested_values = [
        value for key, value in record.items() if key not in {"session_id", "username", "sys_ingested_at", "t_stamp"}
    ]
    has_nested = any(isinstance(value, (dict, list)) for value in nested_values)
    has_repeating_object_array = any(
        isinstance(value, list) and value and all(isinstance(item, dict) for item in value) for value in nested_values
    )

    if has_repeating_object_array:
        child_rows = 1
        sql_rows = 1
    elif has_nested:
        jsonb_rows = 1
    else:
        sql_rows = 1

    return sql_rows, jsonb_rows, child_rows


def build_workload(name: str, *, session_id: str, size: int) -> BenchmarkWorkload:
    builders = {
        "flat": build_flat_record,
        "nested": build_nested_record,
        "mixed": build_mixed_record,
        "drift": build_drift_record,
    }
    return BenchmarkWorkload(name=name, items=[builders[name](i, session_id=session_id) for i in range(size)])


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


async def benchmark_ingestion(
    client: ChiralClient,
    session_id: str,
    workload: BenchmarkWorkload,
    *,
    record_samples: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    timings: list[OperationTiming] = []
    for index, record in enumerate(workload.items):
        sql_rows, jsonb_rows, child_rows = _estimate_routing_counts(record)
        timing, _ = await _measure_async_operation(
            operation="ingestion",
            phase=workload.name,
            func=lambda record=record: client.ingest(session_id=session_id, data=record),
            rows_processed=1,
            rows_inserted=1,
            sql_rows=sql_rows,
            jsonb_rows=jsonb_rows,
            child_rows=child_rows,
        )
        timings.append(timing)
        if record_samples is not None:
            record_samples.append(
                _build_record_sample(sample_index=index, timing=timing, record=record, workload=workload.name)
            )
    return summarize_timings(timings, operation="ingestion", phase=workload.name).as_dict()


async def benchmark_query_execution(
    client: ChiralClient,
    requests: list[dict[str, Any]],
    *,
    record_samples: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    timings: list[OperationTiming] = []
    for index, request in enumerate(requests):
        operation_name = str(request.get("operation", "read"))
        timing, result = await _measure_async_operation(
            operation=operation_name,
            phase="logical_execution",
            func=lambda request=request: client.query(request),
        )
        rows_processed = int(result.get("row_count", result.get("affected_rows", 0)) or 0)
        timing = OperationTiming(
            operation=timing.operation,
            phase=timing.phase,
            latency_seconds=timing.latency_seconds,
            rows_processed=rows_processed,
        )
        timings.append(timing)
        if record_samples is not None:
            record_samples.append(
                _build_record_sample(
                    sample_index=index,
                    timing=timing,
                    request=request,
                    result=result if isinstance(result, dict) else {"value": result},
                    workload="query",
                )
            )
    return summarize_timings(timings, operation="logical_execution", phase="query").as_dict()


async def benchmark_metadata_lookup(
    client: ChiralClient,
    request: dict[str, Any],
    *,
    record_samples: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    session_id = str(request.get("session_id", ""))
    timings: list[OperationTiming] = []
    if session_id:
        timing, _ = await _measure_async_operation(
            operation="metadata_lookup",
            phase="hydration",
            func=lambda: client.translate_only(request),
            rows_processed=1,
            metadata_lookups=1,
        )
        timings.append(timing)
        if record_samples is not None:
            record_samples.append(
                _build_record_sample(sample_index=0, timing=timing, request=request, workload="metadata_lookup")
            )
    return summarize_timings(timings, operation="metadata_lookup", phase="hydration").as_dict()


async def benchmark_transaction_coordination(
    client: ChiralClient,
    requests: list[dict[str, Any]],
    *,
    record_samples: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    timings: list[OperationTiming] = []
    for index, request in enumerate(requests):
        operation_name = str(request.get("operation", "create"))
        timing, result = await _measure_async_operation(
            operation=operation_name,
            phase="coordination",
            func=lambda request=request: client.query(request),
        )
        affected_rows = int(result.get("affected_rows", 0) or 0)
        child_rows, sql_rows, jsonb_rows = 0, 0, 0
        rows_inserted = affected_rows

        if operation_name == "create":
            child_counts = result.get("child_insert_counts", {})
            if isinstance(child_counts, dict):
                child_rows = sum(int(value or 0) for value in child_counts.values())
            sql_rows = affected_rows
            rows_inserted = affected_rows + child_rows
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
        if record_samples is not None:
            record_samples.append(
                _build_record_sample(
                    sample_index=index,
                    timing=timing,
                    request=request,
                    result=result if isinstance(result, dict) else {"value": result},
                    workload="coordination",
                )
            )
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
                "comments": [{"comment_id": 1, "text": "hello", "score": 0.5}],
            },
            "decomposition_plan": {
                "version": 1,
                "parent_table": "chiral_data",
                "entities": [{"source_field": "comments", "child_table": "chiral_data_comments"}],
            },
        },
        {
            "operation": "create",
            "table": "chiral_data",
            "session_id": session_id,
            "payload": {
                "session_id": session_id,
                "username": "jsonb_drift_creator",
                "temperature": {"value": 31, "unit": "C"},
                "profile": "profile_as_string",
                "attributes": {"flags": ["alpha", 1, {"x": True}], "meta": {"source": "benchmark"}},
                "events": [{"kind": "scroll", "value": 12}, "mixed_event", 5],
                "t_stamp": 1742643302.25,
            },
        },
        {
            "operation": "read",
            "session_id": session_id,
            "select": ["username"],
            "filters": [{"field": "session_id", "op": "eq", "value": session_id}],
        },
        {
            "operation": "read",
            "session_id": session_id,
            "select": ["username", "overflow_data.profile", "overflow_data.attributes", "overflow_data.temperature"],
            "filters": [{"field": "session_id", "op": "eq", "value": session_id}],
        },
        {
            "operation": "update",
            "session_id": session_id,
            "updates": {"username": "u"},
            "filters": [{"field": "session_id", "op": "eq", "value": session_id}],
        },
        {
            "operation": "update",
            "session_id": session_id,
            "updates": {"overflow_data.profile": {"city": "updated_city", "score": 99}},
            "filters": [{"field": "session_id", "op": "eq", "value": session_id}],
        },
        {
            "operation": "delete",
            "session_id": session_id,
            "filters": [{"field": "session_id", "op": "eq", "value": session_id}],
        },
    ]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--workload", choices=["flat", "nested", "mixed", "drift", "all"], default="all")
    parser.add_argument("--size", type=int, default=25)
    parser.add_argument("--output-dir", default="benchmark-results")
    return parser


def _normalize_just_argument(value: str) -> str:
    for prefix in ("SESSION_ID=", "SIZE=", "WORKLOAD=", "OUTPUT_DIR="):
        if value.startswith(prefix):
            return value[len(prefix) :]
    return value


async def _run_async(args: argparse.Namespace) -> dict[str, Any]:
    settings = get_settings()

    async with ChiralClient(settings.database_url) as client:
        workload_names = [args.workload] if args.workload != "all" else ["flat", "nested", "mixed", "drift"]
        results: dict[str, Any] = {}
        record_samples: list[dict[str, Any]] = []

        for workload_name in workload_names:
            workload = build_workload(workload_name, session_id=args.session_id, size=args.size)
            results[f"ingestion_{workload_name}"] = await benchmark_ingestion(
                client, args.session_id, workload, record_samples=record_samples
            )

        requests = build_default_requests(args.session_id)
        results["metadata_lookup"] = await benchmark_metadata_lookup(client, requests[0], record_samples=record_samples)
        results["logical_execution"] = await benchmark_query_execution(client, requests, record_samples=record_samples)
        results["coordination"] = await benchmark_transaction_coordination(
            client, requests, record_samples=record_samples
        )

        output_dir = Path(args.output_dir)
        _write_summary_artifact(output_dir, results, session_id=args.session_id)
        _write_record_artifacts(output_dir, record_samples, session_id=args.session_id)
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
