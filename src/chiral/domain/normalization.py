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


@dataclass(frozen=True)
class RepeatingEntityDecision:
    """Deterministic repeating-entity detection result for normalization planning."""

    source_field: str
    child_table: str
    relationship: str
    occurrence_ratio: float
    homogeneity_ratio: float
    average_cardinality: float
    child_columns: list[str]
    reason: str


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


def _normalize_identifier(raw: str) -> str:
    normalized_chars = []
    for char in raw.lower():
        if char.isalnum() or char == "_":
            normalized_chars.append(char)
        else:
            normalized_chars.append("_")

    normalized = "".join(normalized_chars).strip("_")
    if not normalized:
        return "entity"
    if normalized[0].isdigit():
        return f"e_{normalized}"
    return normalized


def detect_repeating_entities(
    docs: list[dict[str, Any]],
    *,
    parent_table: str = "chiral_data",
    min_occurrence_ratio: float = 0.2,
    min_homogeneity_ratio: float = 0.7,
    min_average_cardinality: float = 1.0,
    stable_key_ratio_threshold: float = 0.6,
) -> list[RepeatingEntityDecision]:
    """Detect one-to-many repeating entities from array-of-object fields."""
    if not docs:
        return []

    total_docs = len(docs)
    stats: dict[str, dict[str, Any]] = {}

    for doc in docs:
        for key, value in doc.items():
            if not isinstance(value, list):
                continue

            field_stats = stats.setdefault(
                key,
                {
                    "occurrence_docs": 0,
                    "homogeneous_docs": 0,
                    "total_items": 0,
                    "key_counts": {},
                },
            )

            if not value:
                continue

            if not all(isinstance(item, dict) for item in value):
                continue

            field_stats["occurrence_docs"] += 1
            field_stats["total_items"] += len(value)

            item_key_sets = [set(item.keys()) for item in value]
            if item_key_sets and all(keys == item_key_sets[0] for keys in item_key_sets):
                field_stats["homogeneous_docs"] += 1

            for item in value:
                for item_key in item:
                    key_counts = field_stats["key_counts"]
                    key_counts[item_key] = key_counts.get(item_key, 0) + 1

    decisions: list[RepeatingEntityDecision] = []
    normalized_parent = _normalize_identifier(parent_table)

    for source_field, field_stats in sorted(stats.items()):
        occurrence_docs = field_stats["occurrence_docs"]
        if occurrence_docs == 0:
            continue

        occurrence_ratio = occurrence_docs / total_docs
        homogeneity_ratio = field_stats["homogeneous_docs"] / occurrence_docs
        average_cardinality = field_stats["total_items"] / occurrence_docs

        if occurrence_ratio < min_occurrence_ratio:
            continue
        if homogeneity_ratio < min_homogeneity_ratio:
            continue
        if average_cardinality < min_average_cardinality:
            continue

        total_item_rows = field_stats["total_items"]
        child_columns = []
        for item_key, seen_count in sorted(field_stats["key_counts"].items()):
            key_ratio = seen_count / max(1, total_item_rows)
            if key_ratio >= stable_key_ratio_threshold:
                child_columns.append(item_key)

        child_table = f"{normalized_parent}_{_normalize_identifier(source_field)}"
        decisions.append(
            RepeatingEntityDecision(
                source_field=source_field,
                child_table=child_table,
                relationship="one_to_many",
                occurrence_ratio=occurrence_ratio,
                homogeneity_ratio=homogeneity_ratio,
                average_cardinality=average_cardinality,
                child_columns=child_columns,
                reason="homogeneous_array_of_objects",
            )
        )

    return decisions
