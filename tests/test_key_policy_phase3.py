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
    get_key_spec_for_table,
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
