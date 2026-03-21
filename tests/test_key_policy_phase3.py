# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Phase 3 key policy and DDL idempotent tests."""

from src.chiral.domain.key_policy import (
    CHIRAL_DATA_KEY_SPEC,
    SESSION_METADATA_KEY_SPEC,
    STAGING_DATA_KEY_SPEC,
    KeyPolicy,
    build_dynamic_child_key_spec,
    build_dynamic_child_table_name,
    get_key_spec_for_table,
    normalize_identifier,
)


def test_key_policy_unique_confidence_threshold() -> None:
    """UNIQUE constraints should only be enforced above confidence threshold."""
    policy = KeyPolicy(unique_confidence_threshold=1.0)

    assert not policy.should_enforce_unique_constraint(True, 0.99)
    assert policy.should_enforce_unique_constraint(True, 1.0)


def test_key_policy_default_threshold() -> None:
    """Default policy should use strict 100% uniqueness confidence."""
    default_policy = KeyPolicy()
    assert default_policy.unique_confidence_threshold == 1.0
    assert not default_policy.should_enforce_unique_constraint(True, 0.95)


def test_key_policy_relaxed_threshold() -> None:
    """Relaxed policy can enforce constraints at lower confidence levels."""
    relaxed_policy = KeyPolicy(unique_confidence_threshold=0.9)
    assert relaxed_policy.should_enforce_unique_constraint(True, 0.95)
    assert not relaxed_policy.should_enforce_unique_constraint(True, 0.85)


def test_key_specs_are_available() -> None:
    """All table key specs should be defined and retrievable."""
    assert SESSION_METADATA_KEY_SPEC.table_name == "session_metadata"
    assert SESSION_METADATA_KEY_SPEC.primary_key_column == "session_id"
    assert SESSION_METADATA_KEY_SPEC.primary_key_type == "TEXT"

    assert CHIRAL_DATA_KEY_SPEC.table_name == "chiral_data"
    assert CHIRAL_DATA_KEY_SPEC.primary_key_column == "id"
    assert len(CHIRAL_DATA_KEY_SPEC.foreign_keys) == 1

    assert STAGING_DATA_KEY_SPEC.table_name == "staging_data"
    assert len(STAGING_DATA_KEY_SPEC.foreign_keys) == 1


def test_get_key_spec_for_table() -> None:
    """Key spec lookup should work for all known tables."""
    assert get_key_spec_for_table("chiral_data") == CHIRAL_DATA_KEY_SPEC
    assert get_key_spec_for_table("session_metadata") == SESSION_METADATA_KEY_SPEC
    assert get_key_spec_for_table("staging_data") == STAGING_DATA_KEY_SPEC
    assert get_key_spec_for_table("unknown_table") is None


def test_foreign_key_specs_reference_correct_columns() -> None:
    """FK specs should correctly reference session_metadata."""
    chiral_fk = CHIRAL_DATA_KEY_SPEC.foreign_keys[0]
    assert chiral_fk["local_column"] == "session_id"
    assert chiral_fk["referenced_table"] == "session_metadata"
    assert chiral_fk["referenced_column"] == "session_id"
    assert chiral_fk["on_delete"] == "CASCADE"


def test_dynamic_child_table_name_is_deterministic() -> None:
    """Dynamic table names should be normalized and deterministic."""
    assert build_dynamic_child_table_name("chiral_data", "comments") == "chiral_data_comments"
    assert build_dynamic_child_table_name("Chiral Data", "Comment-Items") == "chiral_data_comment_items"


def test_normalize_identifier_handles_edge_cases() -> None:
    """Identifier normalization should avoid invalid starts and empty names."""
    assert normalize_identifier("123") == "e_123"
    assert normalize_identifier("!!!") == "entity"


def test_dynamic_child_key_spec_contains_parent_and_session_fks() -> None:
    """Generated child key spec should include surrogate PK plus parent/session FKs."""
    spec = build_dynamic_child_key_spec(parent_table="chiral_data", source_field="comments")

    assert spec.table_name == "chiral_data_comments"
    assert spec.primary_key_column == "id"
    assert spec.primary_key_type == "SERIAL"
    assert len(spec.foreign_keys) == 2

    parent_fk = spec.foreign_keys[0]
    assert parent_fk["local_column"] == "chiral_data_id"
    assert parent_fk["referenced_table"] == "chiral_data"
    assert parent_fk["referenced_column"] == "id"

    session_fk = spec.foreign_keys[1]
    assert session_fk["local_column"] == "session_id"
    assert session_fk["referenced_table"] == "session_metadata"
    assert session_fk["referenced_column"] == "session_id"


def test_dynamic_child_key_spec_can_skip_session_fk() -> None:
    """Optional session FK should be configurable for future variants."""
    spec = build_dynamic_child_key_spec(
        parent_table="chiral_data",
        source_field="events",
        include_session_fk=False,
    )

    assert spec.table_name == "chiral_data_events"
    assert len(spec.foreign_keys) == 1
    assert spec.foreign_keys[0]["referenced_table"] == "chiral_data"
