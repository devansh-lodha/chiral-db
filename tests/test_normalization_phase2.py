# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Phase 2 normalization policy tests."""

from src.chiral.domain.normalization import (
    NormalizationPolicy,
    calculate_uniqueness_confidence,
    decide_storage_target,
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
    """Routing should favor JSONB for nested, drift-prone, or high-entropy fields."""
    policy = NormalizationPolicy(
        entropy_threshold=0.1, type_confidence_threshold=0.8, uniqueness_confidence_threshold=1.0
    )

    nested_target, nested_reason = decide_storage_target("dict", entropy=0.0, type_confidence=1.0, policy=policy)
    assert nested_target == StorageTarget.JSONB.value
    assert nested_reason == RoutingReason.NESTED_STRUCTURE.value

    drift_target, drift_reason = decide_storage_target("int", entropy=0.0, type_confidence=0.5, policy=policy)
    assert drift_target == StorageTarget.JSONB.value
    assert drift_reason == RoutingReason.TYPE_DRIFT.value

    stable_target, stable_reason = decide_storage_target("int", entropy=0.0, type_confidence=1.0, policy=policy)
    assert stable_target == StorageTarget.SQL.value
    assert stable_reason == RoutingReason.STABLE_SCALAR.value


def test_uniqueness_confidence_ratio() -> None:
    """Uniqueness confidence should be distinct_count / total_docs."""
    ratio = calculate_uniqueness_confidence(["a", "a", "b"], expected_total=3)
    assert ratio == 2 / 3
