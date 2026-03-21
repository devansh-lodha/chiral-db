# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Phase 7 performance utility tests."""

import pytest

from src.chiral.db.performance import calculate_rows_per_second, chunked


def test_chunked_splits_list_by_size() -> None:
    """Chunk helper should split lists into fixed-size batches."""
    items = [1, 2, 3, 4, 5]
    chunks = list(chunked(items, 2))
    assert chunks == [[1, 2], [3, 4], [5]]


def test_chunked_rejects_non_positive_size() -> None:
    """Chunk helper should reject invalid chunk sizes."""
    with pytest.raises(ValueError, match="Chunk size"):
        list(chunked([1, 2], 0))


def test_calculate_rows_per_second_handles_zero_guard() -> None:
    """Throughput helper should return 0.0 for invalid inputs."""
    assert calculate_rows_per_second(0, 10.0) == 0.0
    assert calculate_rows_per_second(10, 0.0) == 0.0


def test_calculate_rows_per_second_valid_values() -> None:
    """Throughput helper should compute rows per second correctly."""
    throughput = calculate_rows_per_second(200, 2.0)
    assert throughput == 100.0
