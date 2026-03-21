# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Performance helpers for batching and throughput metrics."""

from collections.abc import Iterator
from typing import Any


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
