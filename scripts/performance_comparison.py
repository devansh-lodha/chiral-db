# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Comparative benchmark runner for logical vs direct database access."""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from statistics import fmean
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

import matplotlib.pyplot as plt
from sqlalchemy import text

from chiral.client import ChiralClient
from chiral.config import get_settings
from chiral.db.performance import OperationSummary, OperationTiming, summarize_timings
from chiral.domain.key_policy import build_dynamic_child_key_spec

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

SCENARIO_LABELS: dict[str, str] = {
    "user_read": "flat_user_read",
    "nested_read": "highly_nested_read",
    "multi_entity_update": "multi_entity_update",
    "jsonb_drift_read": "jsonb_drift_read",
}

PROFILE_SCENARIOS: dict[str, list[str]] = {
    "full": ["user_read", "nested_read", "multi_entity_update", "jsonb_drift_read"],
    "domain": ["nested_read", "multi_entity_update", "jsonb_drift_read"],
}


@dataclass(frozen=True)
class ScenarioResult:
    """Summary payload for one logical-vs-direct comparison."""

    scenario: str
    workload_size: int
    logical: OperationSummary
    direct: OperationSummary

    def as_dict(self) -> dict[str, Any]:
        """Return a dictionary representation of the scenario result."""
        logical_latency = self.logical.average_latency_seconds
        direct_latency = self.direct.average_latency_seconds
        latency_overhead_seconds = logical_latency - direct_latency
        latency_overhead_percent = (latency_overhead_seconds / direct_latency) * 100 if direct_latency > 0 else 0.0
        throughput_delta = self.logical.throughput_ops_per_second - self.direct.throughput_ops_per_second

        return {
            "scenario": self.scenario,
            "workload_size": self.workload_size,
            "logical": self.logical.as_dict(),
            "direct": self.direct.as_dict(),
            "comparison": {
                "latency_overhead_seconds": latency_overhead_seconds,
                "latency_overhead_percent": latency_overhead_percent,
                "throughput_delta_ops_per_second": throughput_delta,
            },
        }


def _parse_sizes(value: str) -> list[int]:
    sizes: list[int] = []
    for chunk in value.split(","):
        stripped = chunk.strip()
        if not stripped:
            continue
        size = int(stripped)
        if size <= 0:
            msg = "Workload sizes must be positive"
            raise ValueError(msg)
        sizes.append(size)
    if not sizes:
        msg = "At least one workload size is required"
        raise ValueError(msg)
    return sizes


def _validate_identifier(identifier: str) -> str:
    if not IDENTIFIER_RE.fullmatch(identifier):
        msg = f"Invalid SQL identifier: {identifier}"
        raise ValueError(msg)
    return identifier


def _build_logical_user_read_request(session_id: str, size: int) -> dict[str, Any]:
    return {
        "operation": "read",
        "table": "chiral_data",
        "session_id": session_id,
        "select": ["username", "sys_ingested_at", "t_stamp"],
        "filters": [{"field": "session_id", "op": "eq", "value": session_id}],
        "limit": size,
    }


def _build_logical_nested_read_request(session_id: str, size: int) -> dict[str, Any]:
    return {
        "operation": "read",
        "table": "chiral_data",
        "session_id": session_id,
        "select": ["username", "overflow_data.profile", "overflow_data.events"],
        "filters": [{"field": "session_id", "op": "eq", "value": session_id}],
        "limit": size,
    }


async def _load_nested_entity_plan(client: ChiralClient, session_id: str) -> dict[str, Any] | None:
    async with client.session_factory() as session:
        result = await session.execute(
            text("SELECT schema_json FROM session_metadata WHERE session_id = :sid"),
            {"sid": session_id},
        )
        row = result.fetchone()

    if not row or not row[0]:
        return None

    schema_payload = json.loads(row[0]) if isinstance(row[0], str) else row[0]
    if not isinstance(schema_payload, dict):
        return None

    analysis_metadata = schema_payload.get("__analysis_metadata__", {})
    decomposition_plan = analysis_metadata.get("decomposition_plan", {}) if isinstance(analysis_metadata, dict) else {}
    entities = decomposition_plan.get("entities", []) if isinstance(decomposition_plan, dict) else []

    if not isinstance(entities, list) or not entities:
        return None

    first_entity = entities[0]
    if not isinstance(first_entity, dict):
        return None

    source_field = first_entity.get("source_field")
    child_table = first_entity.get("child_table")
    if not isinstance(source_field, str) or not isinstance(child_table, str):
        return None

    _validate_identifier(source_field)
    _validate_identifier(child_table)
    key_spec = build_dynamic_child_key_spec(parent_table="chiral_data", source_field=source_field)
    parent_fk_column = key_spec.foreign_keys[0]["local_column"]

    return {
        "source_field": source_field,
        "child_table": child_table,
        "parent_fk_column": parent_fk_column,
        "decomposition_plan": decomposition_plan,
    }


def _build_logical_multi_entity_update_request(session_id: str, size: int) -> dict[str, Any]:
    return {
        "operation": "update",
        "table": "chiral_data",
        "session_id": session_id,
        "updates": {
            "username": f"framework_user_{size}",
            "overflow_data.profile": {"city": f"logical_city_{size}", "score": size},
        },
        "filters": [{"field": "session_id", "op": "eq", "value": session_id}],
    }


def _build_logical_jsonb_drift_read_request(session_id: str, size: int) -> dict[str, Any]:
    return {
        "operation": "read",
        "table": "chiral_data",
        "session_id": session_id,
        "select": ["username", "overflow_data.profile", "overflow_data.attributes", "overflow_data.temperature"],
        "filters": [{"field": "session_id", "op": "eq", "value": session_id}],
        "limit": size,
    }


def _build_direct_user_read_sql() -> str:
    return (
        'SELECT "username", "sys_ingested_at", "t_stamp" '
        'FROM "chiral_data" '
        'WHERE "session_id" = :session_id '
        'ORDER BY "id" ASC '
        "LIMIT :limit"
    )


def _build_direct_nested_read_sql() -> str:
    return (
        'SELECT "username", '
        '"overflow_data"->\'profile\' AS "profile", '
        '"overflow_data"->\'events\' AS "events" '
        'FROM "chiral_data" '
        'WHERE "session_id" = :session_id '
        'ORDER BY "id" ASC '
        "LIMIT :limit"
    )


def _build_direct_multi_entity_update_statements(session_id: str, size: int) -> list[tuple[str, dict[str, Any]]]:
    profile_value = json.dumps({"city": f"direct_city_{size}", "score": size})
    return [
        (
            'UPDATE "chiral_data" SET "username" = :username WHERE "session_id" = :session_id',
            {"session_id": session_id, "username": f"direct_user_{size}"},
        ),
        (
            'UPDATE "chiral_data" SET "overflow_data" = jsonb_set(COALESCE("overflow_data", \'{}\'::jsonb), \'{profile}\', CAST(:profile AS jsonb), true) '
            'WHERE "session_id" = :session_id',
            {"session_id": session_id, "profile": profile_value},
        ),
    ]


def _build_direct_jsonb_drift_read_sql() -> str:
    return (
        'SELECT "username", '
        '"overflow_data"->\'profile\' AS "profile", '
        '"overflow_data"->\'attributes\' AS "attributes", '
        '"overflow_data"->>\'temperature\' AS "temperature" '
        'FROM "chiral_data" '
        'WHERE "session_id" = :session_id '
        'ORDER BY "id" ASC '
        "LIMIT :limit"
    )


def _build_drift_heavy_record(index: int, *, session_id: str) -> dict[str, Any]:
    # Purposefully vary nested and scalar types to force JSONB-heavy storage behavior.
    profile_value: Any
    if index % 3 == 0:
        profile_value = {"city": f"city_{index % 5}", "zip": 10000 + index, "active": index % 2 == 0}
    elif index % 3 == 1:
        profile_value = f"profile_as_string_{index}"
    else:
        profile_value = {"city": f"city_{index % 7}", "codes": [index, index + 1, index + 2]}

    temperature_value: Any = index if index % 2 == 0 else f"temp_{index}"
    if index % 5 == 0:
        temperature_value = {"value": index, "unit": "C"}

    return {
        "session_id": session_id,
        "username": f"jsonb_user_{index}",
        "temperature": temperature_value,
        "profile": profile_value,
        "attributes": {
            "tags": ["alpha", "beta", index],
            "flags": {"verified": index % 2 == 0, "score": index / 10},
            "history": [{"step": "ingest", "idx": index}, "raw_event"],
        },
        "events": [{"kind": "click", "value": index}, "mixed_payload", index],
        "t_stamp": float(index),
    }


async def _prepare_session_with_drift_data(client: ChiralClient, session_id: str, size: int) -> None:
    seed_size = max(120, size)
    async with client.session_factory() as session, session.begin():
        await session.execute(text('DELETE FROM "session_metadata" WHERE "session_id" = :sid'), {"sid": session_id})

    for index in range(seed_size):
        await client.ingest(session_id=session_id, data=_build_drift_heavy_record(index, session_id=session_id))

    await client.flush(session_id)


async def _measure_operation(
    *,
    operation: str,
    phase: str,
    func: Any,
    rows_processed: int = 0,
) -> OperationTiming:
    started = time.perf_counter()
    await func()
    elapsed = time.perf_counter() - started
    return OperationTiming(
        operation=operation,
        phase=phase,
        latency_seconds=elapsed,
        rows_processed=rows_processed,
    )


async def _run_logical_read(client: ChiralClient, session_id: str, size: int) -> OperationTiming:
    request = _build_logical_user_read_request(session_id, size)

    async def _execute() -> dict[str, Any]:
        return await client.query(request)

    return await _measure_operation(operation="read", phase="logical", func=_execute, rows_processed=size)


async def _run_direct_read(client: ChiralClient, session_id: str, size: int) -> OperationTiming:
    sql = _build_direct_user_read_sql()

    async def _execute() -> list[dict[str, Any]]:
        async with client.session_factory() as session:
            result = await session.execute(text(sql), {"session_id": session_id, "limit": size})
            return [dict(row) for row in result.mappings().all()]

    return await _measure_operation(operation="read", phase="direct_sql", func=_execute, rows_processed=size)


async def _run_logical_nested_read(client: ChiralClient, session_id: str, size: int) -> OperationTiming:
    plan = await _load_nested_entity_plan(client, session_id)
    if plan:
        request = {
            "operation": "read",
            "table": "chiral_data",
            "session_id": session_id,
            "select": ["username", str(plan["source_field"])],
            "filters": [{"field": "session_id", "op": "eq", "value": session_id}],
            "limit": size,
            "decomposition_plan": plan["decomposition_plan"],
        }
    else:
        request = _build_logical_nested_read_request(session_id, size)

    async def _execute() -> dict[str, Any]:
        return await client.query(request)

    return await _measure_operation(operation="nested_read", phase="logical", func=_execute, rows_processed=size)


async def _run_direct_nested_read(client: ChiralClient, session_id: str, size: int) -> OperationTiming:
    plan = await _load_nested_entity_plan(client, session_id)

    if plan:
        child_table = str(plan["child_table"])
        source_field = str(plan["source_field"])
        parent_fk_column = str(plan["parent_fk_column"])
        sql = (
            f'SELECT p."id", p."username", row_to_json(c) AS "child" '
            f'FROM "chiral_data" AS p '
            f'LEFT JOIN "{child_table}" AS c ON c."{parent_fk_column}" = p."id" '
            f'WHERE p."session_id" = :session_id '
            f'ORDER BY p."id" ASC '
            f"LIMIT :limit"
        )

        async def _execute() -> list[dict[str, Any]]:
            async with client.session_factory() as session:
                result = await session.execute(text(sql), {"session_id": session_id, "limit": size})
                rows = [dict(row) for row in result.mappings().all()]
                merged: dict[int, dict[str, Any]] = {}
                for row in rows:
                    parent_id = int(row.get("id", 0) or 0)
                    if parent_id not in merged:
                        merged[parent_id] = {"id": parent_id, "username": row.get("username"), source_field: []}
                    child_payload = row.get("child")
                    if isinstance(child_payload, dict):
                        merged[parent_id][source_field].append(child_payload)
                return list(merged.values())
    else:
        sql = _build_direct_nested_read_sql()

        async def _execute() -> list[dict[str, Any]]:
            async with client.session_factory() as session:
                result = await session.execute(text(sql), {"session_id": session_id, "limit": size})
                return [dict(row) for row in result.mappings().all()]

    return await _measure_operation(operation="nested_read", phase="direct_jsonb", func=_execute, rows_processed=size)


async def _run_logical_multi_entity_update(client: ChiralClient, session_id: str, size: int) -> OperationTiming:
    request = _build_logical_multi_entity_update_request(session_id, size)

    async def _execute() -> dict[str, Any]:
        return await client.query(request)

    return await _measure_operation(operation="multi_entity_update", phase="logical", func=_execute, rows_processed=1)


async def _run_logical_jsonb_drift_read(client: ChiralClient, session_id: str, size: int) -> OperationTiming:
    request = _build_logical_jsonb_drift_read_request(session_id, size)

    async def _execute() -> dict[str, Any]:
        return await client.query(request)

    return await _measure_operation(operation="jsonb_drift_read", phase="logical", func=_execute, rows_processed=size)


async def _run_direct_multi_entity_update(client: ChiralClient, session_id: str, size: int) -> OperationTiming:
    statements = _build_direct_multi_entity_update_statements(session_id, size)

    async def _execute() -> None:
        async with client.session_factory() as session, session.begin():
            for sql, params in statements:
                await session.execute(text(sql), params)

    return await _measure_operation(
        operation="multi_entity_update", phase="direct_sql_transaction", func=_execute, rows_processed=1
    )


async def _run_direct_jsonb_drift_read(client: ChiralClient, session_id: str, size: int) -> OperationTiming:
    sql = _build_direct_jsonb_drift_read_sql()

    async def _execute() -> list[dict[str, Any]]:
        async with client.session_factory() as session:
            result = await session.execute(text(sql), {"session_id": session_id, "limit": size})
            return [dict(row) for row in result.mappings().all()]

    return await _measure_operation(
        operation="jsonb_drift_read", phase="direct_jsonb", func=_execute, rows_processed=size
    )


def _format_decimal(value: float) -> str:
    return f"{value:.4f}"


def _render_markdown_table(results: list[ScenarioResult]) -> str:
    lines = [
        "| Scenario | Size | Logical avg (s) | Direct avg (s) | Overhead (s) | Overhead (%) |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for result in results:
        comparison = result.as_dict()["comparison"]
        lines.append(
            "| {scenario} | {size} | {logical} | {direct} | {overhead} | {percent} |".format(
                scenario=SCENARIO_LABELS.get(result.scenario, result.scenario),
                size=result.workload_size,
                logical=_format_decimal(result.logical.average_latency_seconds),
                direct=_format_decimal(result.direct.average_latency_seconds),
                overhead=_format_decimal(float(comparison["latency_overhead_seconds"])),
                percent=_format_decimal(float(comparison["latency_overhead_percent"])),
            )
        )
    return "\n".join(lines)


def _render_summary_markdown(results: list[ScenarioResult], *, profile: str) -> str:
    average_logical = fmean(result.logical.average_latency_seconds for result in results) if results else 0.0
    average_direct = fmean(result.direct.average_latency_seconds for result in results) if results else 0.0

    logical_latency_wins = sum(
        1 for result in results if result.logical.average_latency_seconds <= result.direct.average_latency_seconds
    )
    logical_throughput_wins = sum(
        1 for result in results if result.logical.throughput_ops_per_second >= result.direct.throughput_ops_per_second
    )
    focus_text = "Domain-focused profile" if profile == "domain" else "Full profile (includes flat user reads)"

    return "\n".join(
        [
            "# Performance Comparison Summary",
            "",
            f"Profile: {focus_text}",
            f"Logical average latency: {_format_decimal(average_logical)} s",
            f"Direct average latency: {_format_decimal(average_direct)} s",
            f"Logical latency wins: {logical_latency_wins}/{len(results)}",
            f"Logical throughput wins: {logical_throughput_wins}/{len(results)}",
            "",
            _render_markdown_table(results),
        ]
    )


def _render_latency_chart(results: list[ScenarioResult], output_path: Path) -> None:
    scenario_labels = [f"{SCENARIO_LABELS.get(r.scenario, r.scenario)} (n={r.workload_size})" for r in results]
    logical_values = [r.logical.average_latency_seconds for r in results]
    direct_values = [r.direct.average_latency_seconds for r in results]
    x_positions = list(range(len(scenario_labels)))
    width = 0.35

    figure, axis = plt.subplots(figsize=(10, 5))
    axis.bar([position - width / 2 for position in x_positions], logical_values, width=width, label="Logical")
    axis.bar([position + width / 2 for position in x_positions], direct_values, width=width, label="Direct")
    axis.set_xticks(x_positions)
    axis.set_xticklabels(scenario_labels, rotation=20, ha="right")
    axis.set_xlabel("Scenario and workload size")
    axis.set_ylabel("Average latency (s)")
    axis.set_title("Logical vs direct latency")
    axis.legend()
    figure.tight_layout()
    figure.savefig(str(output_path), dpi=160)
    plt.close(figure)


def _render_throughput_chart(results_by_size: dict[int, list[ScenarioResult]], output_path: Path) -> None:
    figure, axis = plt.subplots(figsize=(10, 5))
    if not results_by_size:
        return

    scenarios = sorted({result.scenario for size_results in results_by_size.values() for result in size_results})
    for scenario in scenarios:
        sizes = sorted(results_by_size)
        logical_values, direct_values = [], []
        for size in sizes:
            scenario_result = next(result for result in results_by_size[size] if result.scenario == scenario)
            logical_values.append(scenario_result.logical.throughput_ops_per_second)
            direct_values.append(scenario_result.direct.throughput_ops_per_second)
        scenario_label = SCENARIO_LABELS.get(scenario, scenario)
        axis.plot(sizes, logical_values, marker="o", label=f"{scenario_label} logical")
        axis.plot(sizes, direct_values, marker="o", linestyle="--", label=f"{scenario_label} direct")

    axis.set_xlabel("Workload size")
    axis.set_ylabel("Throughput (ops/sec)")
    axis.set_title("Throughput under increasing workload")
    axis.legend(fontsize="small", ncols=2)
    figure.tight_layout()
    figure.savefig(str(output_path), dpi=160)
    plt.close(figure)


def _get_scenario_runners(profile: str) -> list[tuple[str, Callable[..., Any], Callable[..., Any]]]:
    selected_scenarios = PROFILE_SCENARIOS.get(profile, PROFILE_SCENARIOS["full"])
    all_runners = {
        "user_read": ("user_read", _run_logical_read, _run_direct_read),
        "nested_read": ("nested_read", _run_logical_nested_read, _run_direct_nested_read),
        "multi_entity_update": (
            "multi_entity_update",
            _run_logical_multi_entity_update,
            _run_direct_multi_entity_update,
        ),
        "jsonb_drift_read": (
            "jsonb_drift_read",
            _run_logical_jsonb_drift_read,
            _run_direct_jsonb_drift_read,
        ),
    }
    return [all_runners[scenario] for scenario in selected_scenarios]


async def _run_for_size(session_id: str, size: int, trials: int, *, profile: str) -> list[ScenarioResult]:
    settings = get_settings()
    results: list[ScenarioResult] = []

    async with ChiralClient(settings.database_url) as client:
        await _prepare_session_with_drift_data(client, session_id, size)
        for scenario_name, logical_runner, direct_runner in _get_scenario_runners(profile):
            logical_timings: list[OperationTiming] = []
            direct_timings: list[OperationTiming] = []

            for _ in range(trials):
                logical_timings.append(await logical_runner(client, session_id, size))
                direct_timings.append(await direct_runner(client, session_id, size))

            results.append(
                ScenarioResult(
                    scenario=scenario_name,
                    workload_size=size,
                    logical=summarize_timings(logical_timings, operation=scenario_name, phase="logical"),
                    direct=summarize_timings(direct_timings, operation=scenario_name, phase="direct"),
                )
            )
    return results


async def run_comparison(session_id: str, sizes: list[int], trials: int, *, profile: str) -> list[ScenarioResult]:
    results: list[ScenarioResult] = []
    for size in sizes:
        results.extend(await _run_for_size(session_id, size, trials, profile=profile))
    return results


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run logical vs direct database comparison benchmarks.")
    parser.add_argument("--session-id", required=True, help="Session identifier for the benchmark run.")
    parser.add_argument("--sizes", default="10,25,50", help="Comma-separated workload sizes to benchmark.")
    parser.add_argument("--trials", type=int, default=3, help="Number of repeated trials per scenario and size.")
    parser.add_argument("--profile", choices=["domain", "full"], default="full")
    parser.add_argument("--output-dir", default="benchmark-results")
    return parser


async def _run_and_write(session_id: str, sizes: list[int], trials: int, output_dir: Path, *, profile: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    results = await run_comparison(session_id, sizes, trials, profile=profile)

    comparison_payload = {
        "session_id": session_id,
        "sizes": sizes,
        "trials": trials,
        "profile": profile,
        "results": [result.as_dict() for result in results],
        "summary": {
            "logical_average_latency_seconds": fmean(result.logical.average_latency_seconds for result in results)
            if results
            else 0.0,
            "direct_average_latency_seconds": fmean(result.direct.average_latency_seconds for result in results)
            if results
            else 0.0,
        },
    }

    (output_dir / "comparison_results.json").write_text(
        json.dumps(comparison_payload, indent=2, sort_keys=True), encoding="utf-8"
    )
    (output_dir / "comparison_summary.md").write_text(
        _render_summary_markdown(results, profile=profile), encoding="utf-8"
    )
    _render_latency_chart(results, output_dir / "latency_comparison.png")

    results_by_size: dict[int, list[ScenarioResult]] = {}
    for result in results:
        results_by_size.setdefault(result.workload_size, []).append(result)
    _render_throughput_chart(results_by_size, output_dir / "throughput_comparison.png")


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    sizes = _parse_sizes(args.sizes)
    asyncio.run(
        _run_and_write(args.session_id, sizes, max(1, int(args.trials)), Path(args.output_dir), profile=args.profile)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
