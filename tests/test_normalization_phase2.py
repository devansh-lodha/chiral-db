# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Phase 2 normalization policy tests."""

from src.chiral.domain.normalization import (
    NormalizationPolicy,
    calculate_uniqueness_confidence,
    decide_storage_target,
    detect_repeating_entities,
    infer_dominant_type,
)
from src.chiral.domain.routing import RoutingReason, StorageTarget


def test_infer_dominant_type_picks_clear_majority() -> None:
    """Dominant type should be selected with confidence when majority exists."""
    decision = infer_dominant_type([1, 2, 3, "4"])
    assert decision.inferred_type == "int"
    assert decision.confidence == 0.75
    assert not decision.tie_break_applied


def test_infer_dominant_type_tie_defaults_to_str() -> None:
    """Equal top counts should trigger deterministic tie-break to str."""
    decision = infer_dominant_type([1, "1", 2, "2"])
    assert decision.inferred_type == "str"
    assert decision.tie_break_applied
    assert decision.reason == "type_count_tie_default_to_str"


def test_decide_storage_target_uses_nested_and_thresholds() -> None:
    """Routing should favor JSONB for nested, or drift-prone fields."""
    policy = NormalizationPolicy(type_confidence_threshold=0.8, uniqueness_confidence_threshold=1.0)

    nested_target, nested_reason = decide_storage_target("dict", type_confidence=1.0, policy=policy)
    assert nested_target == StorageTarget.JSONB.value
    assert nested_reason == RoutingReason.NESTED_STRUCTURE.value

    drift_target, drift_reason = decide_storage_target("int", type_confidence=0.5, policy=policy)
    assert drift_target == StorageTarget.JSONB.value
    assert drift_reason == RoutingReason.TYPE_DRIFT.value

    stable_target, stable_reason = decide_storage_target("int", type_confidence=1.0, policy=policy)
    assert stable_target == StorageTarget.SQL.value
    assert stable_reason == RoutingReason.STABLE_SCALAR.value


def test_uniqueness_confidence_ratio() -> None:
    """Uniqueness confidence should be distinct_count / total_docs."""
    ratio = calculate_uniqueness_confidence(["a", "a", "b"], expected_total=3)
    assert ratio == 2 / 3


def test_detect_repeating_entities_from_nested_object_arrays() -> None:
    """Homogeneous arrays of objects should yield one-to-many decomposition candidates."""
    docs = [
        {
            "username": "user1",
            "comments": [{"text": "nice", "time": 123}, {"text": "great", "time": 124}],
        },
        {
            "username": "user2",
            "comments": [{"text": "ok", "time": 130}],
        },
    ]

    entities = detect_repeating_entities(docs, parent_table="chiral_data")
    assert len(entities) == 1
    entity = entities[0]
    assert entity.source_field == "comments"
    assert entity.child_table == "chiral_data_comments"
    assert entity.relationship == "one_to_many"
    assert "text" in entity.child_columns
    assert "time" in entity.child_columns


def test_detect_repeating_entities_ignores_non_object_arrays() -> None:
    """Arrays with scalar/mixed values should not be treated as relational entities."""
    docs = [
        {"username": "user1", "tags": ["a", "b", "c"]},
        {"username": "user2", "tags": ["d", "e"]},
    ]

    entities = detect_repeating_entities(docs, parent_table="chiral_data")
    assert entities == []
