# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Storage routing targets and helpers."""

from enum import StrEnum


class StorageTarget(StrEnum):
    """Supported storage targets for analyzed fields."""

    SQL = "sql"
    JSONB = "jsonb"


class RoutingReason(StrEnum):
    """Canonical routing reason labels for analyzer decisions."""

    STABLE_SCALAR = "stable_scalar"
    NESTED_STRUCTURE = "nested_structure"
    TYPE_DRIFT = "type_drift"


def normalize_storage_target(target: str) -> str:
    """Normalize historical routing labels to current storage target values."""
    normalized = target.strip().lower()
    if normalized == "mongo":
        return StorageTarget.JSONB.value
    if normalized in {StorageTarget.SQL.value, StorageTarget.JSONB.value}:
        return normalized
    return normalized


def is_sql_target(target: str) -> bool:
    """Return whether a target label maps to SQL routing."""
    return normalize_storage_target(target) == StorageTarget.SQL.value
