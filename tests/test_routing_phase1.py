# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Phase 1 routing compatibility tests."""

from src.chiral.domain.contracts import FIELD_CONTRACTS
from src.chiral.domain.routing import StorageTarget, is_sql_target, normalize_storage_target


def test_normalize_storage_target_backwards_compatibility() -> None:
    """Legacy mongo target labels should normalize to jsonb."""
    assert normalize_storage_target("mongo") == StorageTarget.JSONB.value
    assert normalize_storage_target("jsonb") == StorageTarget.JSONB.value
    assert normalize_storage_target("sql") == StorageTarget.SQL.value


def test_is_sql_target_behavior() -> None:
    """SQL checks should only succeed for SQL-routed labels."""
    assert is_sql_target("sql")
    assert not is_sql_target("jsonb")
    assert not is_sql_target("mongo")


def test_field_contracts_are_available() -> None:
    """Domain field contracts should expose canonical contract labels."""
    assert FIELD_CONTRACTS.stable_scalar == "stable_scalar"
    assert FIELD_CONTRACTS.nested_struct == "nested_struct"
    assert FIELD_CONTRACTS.drift_prone == "drift_prone"
    assert FIELD_CONTRACTS.immutable_required == "immutable_required"
