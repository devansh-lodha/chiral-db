# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Worker Analysis Logic."""

import json
import os
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from chiral.db.sessions import session
from chiral.domain.normalization import (
    NormalizationPolicy,
    calculate_field_stability_ratio,
    calculate_max_nesting_depth,
    calculate_uniqueness_confidence,
    evaluate_jsonb_strategy,
    infer_dominant_type,
)
from chiral.utils.heuristics import calculate_entropy


@session
async def analyze_staging(
    sql_session: AsyncSession,
) -> dict[str, Any]:
    """Analyze the first 100 documents in the staging table to determine the schema.

    Args:
        sql_session: Injected SQL session.

    Returns:
        Dictionary containing column metadata and placement decisions.

    """
    # Fetch 100 documents from staging_data (JSONB) — replaces MongoDB staging collection
    result = await sql_session.execute(text("SELECT data FROM staging_data LIMIT 100"))
    rows = result.fetchall()

    if not rows:
        return {}

    # Parse JSONB data — asyncpg returns dicts directly, but handle str just in case
    docs = []
    for row in rows:
        raw = row[0]
        if isinstance(raw, str):
            docs.append(json.loads(raw))
        else:
            docs.append(raw)

    # 2. Pivot data to organize by column (attribute)
    columns: dict[str, list[Any]] = {}

    for doc in docs:
        for key, value in doc.items():
            if key not in columns:
                columns[key] = []
            columns[key].append(value)

    total_docs = len(docs)
    analysis_result = {}
    policy = _build_normalization_policy()

    for col_name, values in columns.items():
        # Skip system columns
        if col_name in ["sys_ingested_at", "t_stamp", "username"]:
            continue

        uniqueness_confidence = calculate_uniqueness_confidence(values, total_docs)
        is_unique = uniqueness_confidence >= policy.uniqueness_confidence_threshold

        # Entropy Calculation
        entropy = calculate_entropy(values)

        # Type Inference
        type_decision = infer_dominant_type(values)
        inferred_type = type_decision.inferred_type
        max_nesting_depth = calculate_max_nesting_depth(values)
        field_stability_ratio = calculate_field_stability_ratio(values, type_decision.confidence)

        # Placement Decision (Phase 4 explicit JSONB strategy)
        strategy_decision = evaluate_jsonb_strategy(
            inferred_type=inferred_type,
            entropy=entropy,
            type_confidence=type_decision.confidence,
            max_nesting_depth=max_nesting_depth,
            field_stability_ratio=field_stability_ratio,
            policy=policy,
        )

        analysis_result[col_name] = {
            "unique": is_unique,
            "unique_confidence": uniqueness_confidence,
            "entropy": entropy,
            "target": strategy_decision.target,
            "routing_reason": strategy_decision.routing_reason,
            "type": inferred_type,
            "type_confidence": type_decision.confidence,
            "max_nesting_depth": max_nesting_depth,
            "field_stability_ratio": field_stability_ratio,
            "explainability": {
                "type_reason": type_decision.reason,
                "tie_break_applied": type_decision.tie_break_applied,
                "strategy_rule": strategy_decision.strategy_rule,
                "entropy_threshold": policy.entropy_threshold,
                "type_confidence_threshold": policy.type_confidence_threshold,
                "uniqueness_confidence_threshold": policy.uniqueness_confidence_threshold,
                "nesting_depth_threshold": policy.nesting_depth_threshold,
                "field_stability_ratio_threshold": policy.field_stability_ratio_threshold,
            },
        }

    return analysis_result


def infer_type(values: list[Any]) -> str:
    """Infer the dominant type using phase-2 deterministic inference logic."""
    return infer_dominant_type(values).inferred_type


def _build_normalization_policy() -> NormalizationPolicy:
    """Build normalization policy from environment or default phase-4 values."""
    return NormalizationPolicy(
        entropy_threshold=float(os.getenv("ROUTING_ENTROPY_THRESHOLD", "0.0")),
        type_confidence_threshold=float(os.getenv("ROUTING_TYPE_CONFIDENCE_THRESHOLD", "0.8")),
        uniqueness_confidence_threshold=float(os.getenv("ROUTING_STABILITY_THRESHOLD", "1.0")),
        nesting_depth_threshold=int(os.getenv("ROUTING_NESTING_DEPTH_THRESHOLD", "2")),
        field_stability_ratio_threshold=float(os.getenv("ROUTING_FIELD_STABILITY_RATIO_THRESHOLD", "0.75")),
    )
