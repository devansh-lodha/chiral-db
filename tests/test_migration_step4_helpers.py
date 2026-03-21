# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Step 4 decomposition helper tests."""

from src.chiral.worker.migrator import _build_child_insert_payload, _extract_decomposed_child_items


def test_extract_decomposed_child_items_splits_parent_and_children() -> None:
    """Entity source fields should be removed from parent and emitted as child docs."""
    doc = {
        "username": "user1",
        "temperature": 12,
        "comments": [{"text": "nice", "time": 123}, {"text": "great", "time": 124}],
    }
    entities = [{"source_field": "comments", "child_columns": ["text", "time"]}]

    parent_doc, extracted = _extract_decomposed_child_items(doc, entities)

    assert "comments" not in parent_doc
    assert parent_doc["temperature"] == 12
    assert len(extracted) == 2


def test_build_child_insert_payload_routes_unknown_fields_to_overflow() -> None:
    """Unknown/nested child fields should be preserved in child overflow_data JSONB."""
    entity = {
        "source_field": "comments",
        "child_columns": ["text", "time"],
    }
    child_doc = {
        "text": "nice",
        "time": 123,
        "meta": {"sentiment": "positive"},
    }

    built = _build_child_insert_payload(
        parent_table="chiral_data",
        session_id="s1",
        parent_id=10,
        entity=entity,
        child_doc=child_doc,
    )

    assert built is not None
    table_name, payload = built
    assert table_name == "chiral_data_comments"
    assert payload["chiral_data_id"] == 10
    assert payload["session_id"] == "s1"
    assert payload["text"] == "nice"
    assert payload["time"] == "123"
    assert "meta" in payload["overflow_data"]


def test_build_child_insert_payload_coerces_numeric_scalars_for_text_columns() -> None:
    """Numeric child scalar values should be text-coerced for dynamic TEXT columns."""
    entity = {
        "source_field": "comments",
        "child_columns": ["score", "comment_id"],
    }
    child_doc = {
        "score": 0.611,
        "comment_id": 42,
    }

    built = _build_child_insert_payload(
        parent_table="chiral_data",
        session_id="s1",
        parent_id=10,
        entity=entity,
        child_doc=child_doc,
    )

    assert built is not None
    _, payload = built
    assert payload["score"] == "0.611"
    assert payload["comment_id"] == "42"
