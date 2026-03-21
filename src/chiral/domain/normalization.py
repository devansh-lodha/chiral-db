# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Normalization policy and explainable routing decisions."""

from dataclasses import dataclass
from typing import Any

from .routing import RoutingReason, StorageTarget


@dataclass(frozen=True)
class NormalizationPolicy:
    """Threshold policy for deterministic normalization and routing."""

    entropy_threshold: float = 0.0
    type_confidence_threshold: float = 0.8
    uniqueness_confidence_threshold: float = 1.0
    nesting_depth_threshold: int = 1
    field_stability_ratio_threshold: float = 0.75


@dataclass(frozen=True)
class DominantTypeDecision:
    """Dominant type inference with confidence and tie-break details."""

    inferred_type: str
    confidence: float
    tie_break_applied: bool
    reason: str


@dataclass(frozen=True)
class JsonbStrategyDecision:
    """Detailed JSONB-vs-SQL strategy decision for a field."""

    target: str
    routing_reason: str
    strategy_rule: str


def _value_to_inferred_type(value: Any) -> str:
    """Map Python values to normalized analysis type names."""
    type_map = {
        bool: "bool",
        int: "int",
        float: "float",
        dict: "dict",
        list: "list",
    }
    for type_cls, type_name in type_map.items():
        if isinstance(value, type_cls):
            return type_name
    return "str"


def infer_dominant_type(values: list[Any]) -> DominantTypeDecision:
    """Infer dominant type using deterministic tie-break rules."""
    valid_values = [value for value in values if value is not None]
    if not valid_values:
        return DominantTypeDecision(
            inferred_type="str",
            confidence=1.0,
            tie_break_applied=False,
            reason="all_values_null_default_to_str",
        )

    counts: dict[str, int] = {}
    for value in valid_values:
        type_name = _value_to_inferred_type(value)
        counts[type_name] = counts.get(type_name, 0) + 1

    ordered_counts = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    dominant_type, dominant_count = ordered_counts[0]
    total = len(valid_values)
    confidence = dominant_count / total

    if len(ordered_counts) > 1 and ordered_counts[1][1] == dominant_count:
        return DominantTypeDecision(
            inferred_type="str",
            confidence=confidence,
            tie_break_applied=True,
            reason="type_count_tie_default_to_str",
        )

    return DominantTypeDecision(
        inferred_type=dominant_type,
        confidence=confidence,
        tie_break_applied=False,
        reason="dominant_type_selected",
    )


def calculate_uniqueness_confidence(values: list[Any], expected_total: int) -> float:
    """Calculate uniqueness confidence ratio across sampled documents."""
    if expected_total <= 0:
        return 0.0

    normalized_values = [repr(value) for value in values]
    distinct_count = len(set(normalized_values))
    return distinct_count / expected_total


def calculate_max_nesting_depth(values: list[Any]) -> int:
    """Calculate the maximum nesting depth in observed values."""

    def _depth(value: Any) -> int:
        if isinstance(value, dict):
            if not value:
                return 1
            return 1 + max(_depth(child) for child in value.values())
        if isinstance(value, list):
            if not value:
                return 1
            return 1 + max(_depth(child) for child in value)
        return 0

    if not values:
        return 0

    return max(_depth(value) for value in values)


def calculate_field_stability_ratio(values: list[Any], type_confidence: float) -> float:
    """Calculate field stability ratio from value presence and type confidence.

    Stability ratio is defined as:
    (non_null_ratio) * (dominant_type_confidence)
    """
    if not values:
        return 0.0

    non_null_count = len([value for value in values if value is not None])
    non_null_ratio = non_null_count / len(values)
    return non_null_ratio * type_confidence


def evaluate_jsonb_strategy(
    inferred_type: str,
    entropy: float,
    type_confidence: float,
    max_nesting_depth: int,
    field_stability_ratio: float,
    policy: NormalizationPolicy,
) -> JsonbStrategyDecision:
    """Evaluate explicit strategy rules for SQL vs JSONB field routing."""
    if inferred_type in {"dict", "list"} or max_nesting_depth >= policy.nesting_depth_threshold:
        return JsonbStrategyDecision(
            target=StorageTarget.JSONB.value,
            routing_reason=RoutingReason.NESTED_STRUCTURE.value,
            strategy_rule="nesting_depth_threshold_exceeded",
        )

    if field_stability_ratio < policy.field_stability_ratio_threshold:
        return JsonbStrategyDecision(
            target=StorageTarget.JSONB.value,
            routing_reason=RoutingReason.TYPE_DRIFT.value,
            strategy_rule="low_field_stability_ratio",
        )

    if type_confidence < policy.type_confidence_threshold:
        return JsonbStrategyDecision(
            target=StorageTarget.JSONB.value,
            routing_reason=RoutingReason.TYPE_DRIFT.value,
            strategy_rule="low_type_confidence",
        )

    if entropy > policy.entropy_threshold:
        return JsonbStrategyDecision(
            target=StorageTarget.JSONB.value,
            routing_reason=RoutingReason.TYPE_DRIFT.value,
            strategy_rule="high_type_entropy",
        )

    return JsonbStrategyDecision(
        target=StorageTarget.SQL.value,
        routing_reason=RoutingReason.STABLE_SCALAR.value,
        strategy_rule="stable_scalar_field",
    )


def decide_storage_target(
    inferred_type: str,
    entropy: float,
    type_confidence: float,
    policy: NormalizationPolicy,
    max_nesting_depth: int = 0,
    field_stability_ratio: float = 1.0,
) -> tuple[str, str]:
    """Backwards-compatible target decision helper returning target and reason only."""
    decision = evaluate_jsonb_strategy(
        inferred_type=inferred_type,
        entropy=entropy,
        type_confidence=type_confidence,
        max_nesting_depth=max_nesting_depth,
        field_stability_ratio=field_stability_ratio,
        policy=policy,
    )
    return decision.target, decision.routing_reason
