# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Centralized CRUD query builder for SQL and JSONB fields."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
NUMERIC_TEXT_RE = r"^-?\\d+(?:\\.\\d+)?$"


@dataclass(frozen=True)
class BuiltQuery:
    """Built SQL query and bind parameters."""

    sql: str
    params: dict[str, Any]


def _validate_identifier(identifier: str) -> str:
    """Validate SQL identifier for safe interpolation."""
    if not IDENTIFIER_RE.fullmatch(identifier):
        msg = f"Invalid identifier: {identifier}"
        raise ValueError(msg)
    return identifier


class CrudQueryBuilder:
    """Build parameterized SQL for CRUD operations over chiral_data."""

    def __init__(self, table_name: str = "chiral_data") -> None:
        """Initialize builder with target table name."""
        self.table_name = _validate_identifier(table_name)

    def build_select(
        self,
        select_fields: list[str] | None = None,
        filters: list[dict[str, Any]] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> BuiltQuery:
        """Build a SELECT query with SQL and JSONB-aware filters."""
        fields_sql = self._build_select_list(select_fields or ["*"])
        where_sql, params = self._build_where_clause(filters or [])

        sql = f'SELECT {fields_sql} FROM "{self.table_name}"'
        if where_sql:
            sql += f" WHERE {where_sql}"

        if limit is not None:
            sql += " LIMIT :limit"
            params["limit"] = limit

        if offset is not None:
            sql += " OFFSET :offset"
            params["offset"] = offset

        return BuiltQuery(sql=sql, params=params)

    def build_insert(self, payload: dict[str, Any]) -> BuiltQuery:
        """Build an INSERT query from payload fields."""
        if not payload:
            msg = "Insert payload cannot be empty"
            raise ValueError(msg)

        columns: list[str] = []
        binders: list[str] = []
        params: dict[str, Any] = {}

        for key, value in payload.items():
            _validate_identifier(key)
            columns.append(f'"{key}"')
            binders.append(f":{key}")
            params[key] = value

        sql = f'INSERT INTO "{self.table_name}" ({", ".join(columns)}) VALUES ({", ".join(binders)})'
        return BuiltQuery(sql=sql, params=params)

    def build_update(
        self,
        updates: dict[str, Any],
        filters: list[dict[str, Any]] | None = None,
    ) -> BuiltQuery:
        """Build an UPDATE query with optional filters."""
        if not updates:
            msg = "Update payload cannot be empty"
            raise ValueError(msg)

        set_clauses: list[str] = []
        params: dict[str, Any] = {}

        for key, value in updates.items():
            _validate_identifier(key)
            set_clauses.append(f'"{key}" = :set_{key}')
            params[f"set_{key}"] = value

        where_sql, where_params = self._build_where_clause(filters or [])
        params.update(where_params)

        sql = f'UPDATE "{self.table_name}" SET {", ".join(set_clauses)}'
        if where_sql:
            sql += f" WHERE {where_sql}"
        return BuiltQuery(sql=sql, params=params)

    def build_delete(self, filters: list[dict[str, Any]] | None = None) -> BuiltQuery:
        """Build a DELETE query with optional filters."""
        where_sql, params = self._build_where_clause(filters or [])
        sql = f'DELETE FROM "{self.table_name}"'
        if where_sql:
            sql += f" WHERE {where_sql}"
        return BuiltQuery(sql=sql, params=params)

    def _build_select_list(self, select_fields: list[str]) -> str:
        if select_fields == ["*"]:
            return "*"

        select_parts: list[str] = []
        for index, field in enumerate(select_fields):
            if field.startswith("overflow_data."):
                json_key = field.split(".", 1)[1]
                _validate_identifier(json_key)
                alias = f"json_{index}_{json_key}"
                select_parts.append(f"overflow_data->>'{json_key}' AS \"{alias}\"")
            else:
                _validate_identifier(field)
                select_parts.append(f'"{field}"')
        return ", ".join(select_parts)

    def _build_where_clause(self, filters: list[dict[str, Any]]) -> tuple[str, dict[str, Any]]:
        clauses: list[str] = []
        params: dict[str, Any] = {}

        for index, spec in enumerate(filters):
            field = spec.get("field")
            op = str(spec.get("op", "eq")).lower()
            value = spec.get("value")

            if not isinstance(field, str):
                msg = "Filter field must be a string"
                raise TypeError(msg)

            is_jsonb = field.startswith("overflow_data.")
            param_name = f"p_{index}"

            if is_jsonb:
                json_key = field.split(".", 1)[1]
                _validate_identifier(json_key)
                expression = f"overflow_data->>'{json_key}'"
            else:
                _validate_identifier(field)
                expression = f'"{field}"'

            if is_jsonb and op in {"gt", "gte", "lt", "lte"}:
                if not isinstance(value, (int, float)) or isinstance(value, bool):
                    msg = f"JSONB range operation '{op}' requires numeric filter value"
                    raise ValueError(msg)

                numeric_expr = f"({expression})::double precision"
                numeric_guard = f"({expression}) ~ '{NUMERIC_TEXT_RE}'"

                if op == "gt":
                    clauses.append(f"({numeric_guard} AND {numeric_expr} > :{param_name})")
                elif op == "gte":
                    clauses.append(f"({numeric_guard} AND {numeric_expr} >= :{param_name})")
                elif op == "lt":
                    clauses.append(f"({numeric_guard} AND {numeric_expr} < :{param_name})")
                elif op == "lte":
                    clauses.append(f"({numeric_guard} AND {numeric_expr} <= :{param_name})")

                params[param_name] = float(value)
                continue

            if op == "eq":
                clauses.append(f"{expression} = :{param_name}")
                params[param_name] = value
            elif op == "ne":
                clauses.append(f"{expression} != :{param_name}")
                params[param_name] = value
            elif op == "gt":
                clauses.append(f"{expression} > :{param_name}")
                params[param_name] = value
            elif op == "gte":
                clauses.append(f"{expression} >= :{param_name}")
                params[param_name] = value
            elif op == "lt":
                clauses.append(f"{expression} < :{param_name}")
                params[param_name] = value
            elif op == "lte":
                clauses.append(f"{expression} <= :{param_name}")
                params[param_name] = value
            elif op == "contains":
                if not is_jsonb:
                    msg = "contains is only supported for overflow_data.<key> filters"
                    raise ValueError(msg)
                clauses.append(f"overflow_data @> :{param_name}::jsonb")
                params[param_name] = value
            else:
                msg = f"Unsupported filter operation: {op}"
                raise ValueError(msg)

        return " AND ".join(clauses), params
