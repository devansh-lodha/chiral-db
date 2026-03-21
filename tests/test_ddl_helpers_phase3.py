# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""DDL helper naming tests for dynamic schema evolution."""

from src.chiral.db.ddl_helpers import build_fk_constraint_name, build_index_name


def test_build_fk_constraint_name_is_deterministic() -> None:
    """FK constraint naming should be deterministic and normalized."""
    name = build_fk_constraint_name("chiral_data_comments", "chiral_data_id", "chiral_data")
    assert name == "fk_chiral_data_comments_chiral_data_id_chiral_data"


def test_build_constraint_names_obey_postgres_identifier_limit() -> None:
    """Generated names should be truncated to PostgreSQL 63-char identifier limit."""
    long_name = build_fk_constraint_name(
        "very_very_very_very_very_very_very_very_very_very_long_table_name",
        "very_very_very_very_very_long_local_column_name",
        "very_very_very_very_very_long_parent_table_name",
    )
    assert len(long_name) <= 63


def test_build_index_name_is_deterministic() -> None:
    """Index naming should be deterministic and normalized."""
    name = build_index_name("chiral_data_comments", "session_id")
    assert name == "idx_chiral_data_comments_session_id"
