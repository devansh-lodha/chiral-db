# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Phase 4 JSONB strategy rule tests."""

from src.chiral.domain.normalization import (
    NormalizationPolicy,
    # calculate_field_stability_ratio,
    calculate_max_nesting_depth,
    evaluate_jsonb_strategy,
)
from src.chiral.domain.routing import RoutingReason, StorageTarget


def test_calculate_max_nesting_depth_nested_structures() -> None:
    """Nested dict/list values should produce depth > 1."""
    values = [{"a": {"b": [1, 2, {"c": 3}]}}]
    assert calculate_max_nesting_depth(values) >= 3


# def test_calculate_field_stability_ratio_uses_presence_and_type_confidence() -> None:
#     """Field stability ratio should combine non-null ratio and type confidence."""
#     values = [1, 2, None, None]
#     ratio = calculate_field_stability_ratio(values, type_confidence=1.0)
#     assert ratio == 0.5


def test_jsonb_strategy_routes_nested_to_jsonb() -> None:
    """Nested fields should route to JSONB with nested strategy rule."""
    policy = NormalizationPolicy(nesting_depth_threshold=1)
    decision = evaluate_jsonb_strategy(
        inferred_type="dict",
        type_confidence=1.0,
        max_nesting_depth=2,
        # field_stability_ratio=1.0,
        policy=policy,
    )
    assert decision.target == StorageTarget.JSONB.value
    assert decision.routing_reason == RoutingReason.NESTED_STRUCTURE.value
    assert decision.strategy_rule == "nesting_depth_threshold_exceeded"


# def test_jsonb_strategy_routes_low_stability_to_jsonb() -> None:
#     """Low field stability should route scalar fields to JSONB."""
#     policy = NormalizationPolicy(field_stability_ratio_threshold=0.75)
#     decision = evaluate_jsonb_strategy(
#         inferred_type="int",
#         type_confidence=1.0,
#         max_nesting_depth=0,
#         field_stability_ratio=0.5,
#         policy=policy,
#     )
#     assert decision.target == StorageTarget.JSONB.value
#     assert decision.routing_reason == RoutingReason.TYPE_DRIFT.value
#     assert decision.strategy_rule == "low_field_stability_ratio"


def test_jsonb_strategy_routes_any_type_drift_to_jsonb() -> None:
    """Any scalar type mismatch should route the whole field to JSONB."""
    policy = NormalizationPolicy(
        type_confidence_threshold=0.8,
        # field_stability_ratio_threshold=0.5,
    )
    decision = evaluate_jsonb_strategy(
        inferred_type="int",
        type_confidence=0.99,
        max_nesting_depth=0,
        # field_stability_ratio=0.99,
        policy=policy,
    )
    assert decision.target == StorageTarget.JSONB.value
    assert decision.routing_reason == RoutingReason.TYPE_DRIFT.value
    assert decision.strategy_rule == "heterogeneous_scalar_types"


# def test_jsonb_strategy_routes_stable_scalar_to_sql() -> None:
#     """Stable scalar fields should stay in SQL."""
#     policy = NormalizationPolicy(field_stability_ratio_threshold=0.75)
#     decision = evaluate_jsonb_strategy(
#         inferred_type="int",
#         type_confidence=1.0,
#         max_nesting_depth=0,
#         field_stability_ratio=1.0,
#         policy=policy,
#     )
#     assert decision.target == StorageTarget.SQL.value
#     assert decision.routing_reason == RoutingReason.STABLE_SCALAR.value
#     assert decision.strategy_rule == "stable_scalar_field"
