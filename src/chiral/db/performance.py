# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Performance helpers for batching, throughput, and benchmark summaries."""

from __future__ import annotations

from dataclasses import dataclass
from statistics import fmean
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence


@dataclass(frozen=True)
class OperationTiming:
    """Single benchmark sample for one operation."""

    operation: str
    phase: str
    latency_seconds: float
    rows_processed: int = 0
    rows_inserted: int = 0
    sql_rows: int = 0
    jsonb_rows: int = 0
    child_rows: int = 0
    metadata_lookups: int = 0


@dataclass(frozen=True)
class BackendDistribution:
    """Distribution of records across physical backends."""

    sql_rows: int
    jsonb_rows: int
    child_rows: int

    @property
    def total_rows(self) -> int:
        """Return the total number of rows across all backends."""
        return self.sql_rows + self.jsonb_rows + self.child_rows

    @property
    def sql_fraction(self) -> float:
        """Return the fraction of rows that were stored in SQL, if any."""
        return self.sql_rows / self.total_rows if self.total_rows else 0.0

    @property
    def jsonb_fraction(self) -> float:
        """Return the fraction of rows that were stored in JSONB, if any."""
        return self.jsonb_rows / self.total_rows if self.total_rows else 0.0

    @property
    def child_fraction(self) -> float:
        """Return the fraction of rows that were stored in child tables, if any."""
        return self.child_rows / self.total_rows if self.total_rows else 0.0

    def as_dict(self) -> dict[str, float | int]:
        """Convert summary to a dictionary for JSON serialization."""
        return {
            "sql_rows": self.sql_rows,
            "jsonb_rows": self.jsonb_rows,
            "child_rows": self.child_rows,
            "total_rows": self.total_rows,
            "sql_fraction": self.sql_fraction,
            "jsonb_fraction": self.jsonb_fraction,
            "child_fraction": self.child_fraction,
        }


@dataclass(frozen=True)
class OperationSummary:
    """Aggregate benchmark statistics for a workload."""

    operation: str
    phase: str
    runs: int
    average_latency_seconds: float
    p50_latency_seconds: float
    p95_latency_seconds: float
    throughput_ops_per_second: float
    rows_processed: int
    rows_inserted: int
    metadata_lookups: int
    backend_distribution: BackendDistribution

    def as_dict(self) -> dict[str, Any]:
        """Convert summary to a dictionary for JSON serialization."""
        return {
            "operation": self.operation,
            "phase": self.phase,
            "runs": self.runs,
            "average_latency_seconds": self.average_latency_seconds,
            "p50_latency_seconds": self.p50_latency_seconds,
            "p95_latency_seconds": self.p95_latency_seconds,
            "throughput_ops_per_second": self.throughput_ops_per_second,
            "rows_processed": self.rows_processed,
            "rows_inserted": self.rows_inserted,
            "metadata_lookups": self.metadata_lookups,
            "backend_distribution": self.backend_distribution.as_dict(),
        }


def chunked(items: list[Any], size: int) -> Iterator[list[Any]]:
    """Yield items in fixed-size chunks."""
    if size <= 0:
        msg = "Chunk size must be greater than zero"
        raise ValueError(msg)

    for index in range(0, len(items), size):
        yield items[index : index + size]


def calculate_rows_per_second(row_count: int, elapsed_seconds: float) -> float:
    """Calculate throughput (rows/sec) with safe zero guards."""
    if row_count <= 0 or elapsed_seconds <= 0:
        return 0.0
    return row_count / elapsed_seconds


def percentile(values: Sequence[float], quantile: float) -> float:
    """Compute a percentile using linear interpolation between ordered samples."""
    if not values:
        return 0.0
    if quantile <= 0:
        return min(values)
    if quantile >= 100:
        return max(values)

    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]

    position = (len(ordered) - 1) * (quantile / 100)
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    fraction = position - lower_index
    lower_value = ordered[lower_index]
    upper_value = ordered[upper_index]
    return lower_value + ((upper_value - lower_value) * fraction)


def summarize_timings(
    timings: Sequence[OperationTiming],
    *,
    operation: str,
    phase: str,
) -> OperationSummary:
    """Aggregate benchmark samples into a summary payload."""
    latencies = [timing.latency_seconds for timing in timings]
    total_rows_processed = sum(timing.rows_processed for timing in timings)
    total_rows_inserted = sum(timing.rows_inserted for timing in timings)
    total_metadata_lookups = sum(timing.metadata_lookups for timing in timings)
    total_sql_rows = sum(timing.sql_rows for timing in timings)
    total_jsonb_rows = sum(timing.jsonb_rows for timing in timings)
    total_child_rows = sum(timing.child_rows for timing in timings)
    total_elapsed = sum(latencies)

    backend_distribution = BackendDistribution(
        sql_rows=total_sql_rows,
        jsonb_rows=total_jsonb_rows,
        child_rows=total_child_rows,
    )

    return OperationSummary(
        operation=operation,
        phase=phase,
        runs=len(timings),
        average_latency_seconds=fmean(latencies) if latencies else 0.0,
        p50_latency_seconds=percentile(latencies, 50),
        p95_latency_seconds=percentile(latencies, 95),
        throughput_ops_per_second=calculate_rows_per_second(len(timings), total_elapsed),
        rows_processed=total_rows_processed,
        rows_inserted=total_rows_inserted,
        metadata_lookups=total_metadata_lookups,
        backend_distribution=backend_distribution,
    )
