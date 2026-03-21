# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""JSON request to SQL/JSONB query translation service."""

from __future__ import annotations

from typing import Any

from chiral.db.query_builder import BuiltQuery, CrudQueryBuilder


def translate_json_request(request: dict[str, Any]) -> BuiltQuery:
    """Translate a user JSON CRUD request into a parameterized SQL query.

    Supported operation values: read, create, update, delete.
    """
    operation = str(request.get("operation", "")).lower()
    table_name = str(request.get("table", "chiral_data"))
    builder = CrudQueryBuilder(table_name=table_name)

    if operation == "read":
        return builder.build_select(
            select_fields=request.get("select", ["*"]),
            filters=request.get("filters", []),
            limit=request.get("limit"),
            offset=request.get("offset"),
        )

    if operation == "create":
        payload = request.get("payload", {})
        if not isinstance(payload, dict):
            msg = "create operation requires object payload"
            raise ValueError(msg)
        return builder.build_insert(payload)

    if operation == "update":
        updates = request.get("updates", {})
        if not isinstance(updates, dict):
            msg = "update operation requires object updates"
            raise ValueError(msg)
        return builder.build_update(updates=updates, filters=request.get("filters", []))

    if operation == "delete":
        return builder.build_delete(filters=request.get("filters", []))

    msg = f"Unsupported operation: {operation}"
    raise ValueError(msg)
