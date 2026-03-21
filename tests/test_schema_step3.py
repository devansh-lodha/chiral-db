# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Step 3 schema materialization helper tests."""

from src.chiral.db.schema import get_decomposition_plan


def test_get_decomposition_plan_returns_defaults() -> None:
    """Missing decomposition metadata should produce a default empty plan."""
    plan = get_decomposition_plan({"temperature": {"target": "sql"}})
    assert plan["version"] == 1
    assert plan["parent_table"] == "chiral_data"
    assert plan["entities"] == []


def test_get_decomposition_plan_extracts_metadata_shape() -> None:
    """Decomposition plan should be extracted from analysis metadata envelope."""
    analysis = {
        "temperature": {"target": "sql", "type": "float"},
        "__analysis_metadata__": {
            "decomposition_plan": {
                "version": 1,
                "parent_table": "chiral_data",
                "entities": [
                    {
                        "source_field": "comments",
                        "child_table": "chiral_data_comments",
                        "child_columns": ["text", "time"],
                    }
                ],
            }
        },
    }

    plan = get_decomposition_plan(analysis)
    assert plan["version"] == 1
    assert plan["parent_table"] == "chiral_data"
    assert len(plan["entities"]) == 1
    assert plan["entities"][0]["source_field"] == "comments"
