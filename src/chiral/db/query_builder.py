# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Centralized CRUD query builder for SQL and JSONB fields."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
NUMERIC_TEXT_RE = r"^-?\\d+(?:\\.\\d+)?$"


@dataclass(frozen=True)
class BuiltQuery:
    """Built SQL query and bind parameters."""

    sql: str
    params: dict[str, Any]


@dataclass(frozen=True)
class InferredJoin:
    """Join specification inferred from decomposition metadata."""

    source_field: str
    child_table: str
    parent_fk_column: str
    child_column_types: dict[str, str] | None = None


def _validate_identifier(identifier: str) -> str:
    """Validate SQL identifier for safe interpolation."""
    if not IDENTIFIER_RE.fullmatch(identifier):
        msg = f"Invalid identifier: {identifier}"
        raise ValueError(msg)
    return identifier


class CrudQueryBuilder:
    """Build parameterized SQL for CRUD operations over chiral_data."""

    def __init__(
        self,
        table_name: str = "chiral_data",
        inferred_joins: list[InferredJoin] | None = None,
    ) -> None:
        """Initialize builder with target table name."""
        self.table_name = _validate_identifier(table_name)
        self.inferred_joins = inferred_joins or []
        self._join_by_source_field = {join.source_field: join for join in self.inferred_joins}

    def build_select(
        self,
        select_fields: list[str] | None = None,
        filters: list[dict[str, Any]] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> BuiltQuery:
        """Build a SELECT query with SQL and JSONB-aware filters."""
        joins_sql = self._build_joins_clause()
        fields_sql = self._build_select_list(select_fields or ["*"], use_table_qualification=bool(joins_sql))
        where_sql, params = self._build_where_clause(filters or [], use_table_qualification=bool(joins_sql))

        sql = f'SELECT {fields_sql} FROM "{self.table_name}"'
        if joins_sql:
            sql += f" {joins_sql}"
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

        where_sql, where_params = self._build_where_clause_for_write(filters or [])
        params.update(where_params)

        sql = f'UPDATE "{self.table_name}" SET {", ".join(set_clauses)}'
        if where_sql:
            sql += f" WHERE {where_sql}"
        return BuiltQuery(sql=sql, params=params)

    def build_delete(self, filters: list[dict[str, Any]] | None = None) -> BuiltQuery:
        """Build a DELETE query with optional filters."""
        where_sql, params = self._build_where_clause_for_write(filters or [])
        sql = f'DELETE FROM "{self.table_name}"'
        if where_sql:
            sql += f" WHERE {where_sql}"
        return BuiltQuery(sql=sql, params=params)

    def _build_where_clause_for_write(self, filters: list[dict[str, Any]]) -> tuple[str, dict[str, Any]]:
        clauses: list[str] = []
        params: dict[str, Any] = {}

        base_filters: list[dict[str, Any]] = []
        join_filters_by_prefix: dict[str, list[dict[str, Any]]] = {}

        for spec in filters:
            field = spec.get("field")
            if isinstance(field, str) and "." in field:
                prefix, _ = field.split(".", 1)
                if prefix in self._join_by_source_field:
                    join_filters_by_prefix.setdefault(prefix, []).append(spec)
                    continue
            base_filters.append(spec)

        base_sql, base_params = self._build_where_clause(
            base_filters,
            use_table_qualification=False,
            param_offset=0,
        )
        if base_sql:
            clauses.append(base_sql)
        params.update(base_params)

        param_offset = len(base_filters)
        for prefix, prefix_filters in join_filters_by_prefix.items():
            join = self._join_by_source_field[prefix]
            _validate_identifier(join.child_table)
            _validate_identifier(join.parent_fk_column)

            alias = f"{self._join_alias(prefix)}_w"
            child_where_sql, child_params = self._build_join_where_clause(
                join=join,
                alias=alias,
                source_field=prefix,
                filters=prefix_filters,
                param_offset=param_offset,
            )
            param_offset += len(prefix_filters)

            exists_clause = (
                f'EXISTS (SELECT 1 FROM "{join.child_table}" AS "{alias}" '
                f'WHERE "{alias}"."{join.parent_fk_column}" = "{self.table_name}"."id"'
            )
            if child_where_sql:
                exists_clause += f" AND {child_where_sql}"
            exists_clause += ")"

            clauses.append(exists_clause)
            params.update(child_params)

        return " AND ".join(clauses), params

    def _build_join_where_clause(
        self,
        *,
        join: InferredJoin,
        alias: str,
        source_field: str,
        filters: list[dict[str, Any]],
        param_offset: int,
    ) -> tuple[str, dict[str, Any]]:
        clauses: list[str] = []
        params: dict[str, Any] = {}

        for index, spec in enumerate(filters):
            field = spec.get("field")
            op = str(spec.get("op", "eq")).lower()
            value = spec.get("value")

            if not isinstance(field, str):
                msg = "Filter field must be a string"
                raise TypeError(msg)

            prefix, rest = field.split(".", 1)
            if prefix != source_field:
                msg = f"Unexpected join filter prefix: {prefix}"
                raise ValueError(msg)

            expression, is_jsonb, expected_type = self._resolve_join_where_expression(
                join=join,
                alias=alias,
                field_suffix=rest,
            )
            param_name = f"p_{param_offset + index}"

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
                params[param_name] = self._coerce_filter_value(value, expected_type, op)
            elif op == "ne":
                clauses.append(f"{expression} != :{param_name}")
                params[param_name] = self._coerce_filter_value(value, expected_type, op)
            elif op == "gt":
                clauses.append(f"{expression} > :{param_name}")
                params[param_name] = self._coerce_filter_value(value, expected_type, op)
            elif op == "gte":
                clauses.append(f"{expression} >= :{param_name}")
                params[param_name] = self._coerce_filter_value(value, expected_type, op)
            elif op == "lt":
                clauses.append(f"{expression} < :{param_name}")
                params[param_name] = self._coerce_filter_value(value, expected_type, op)
            elif op == "lte":
                clauses.append(f"{expression} <= :{param_name}")
                params[param_name] = self._coerce_filter_value(value, expected_type, op)
            elif op == "contains":
                if not is_jsonb:
                    msg = "contains is only supported for overflow_data.<key> filters"
                    raise ValueError(msg)
                clauses.append(f'"{alias}"."overflow_data" @> :{param_name}::jsonb')
                params[param_name] = value
            else:
                msg = f"Unsupported filter operation: {op}"
                raise ValueError(msg)

        return " AND ".join(clauses), params

    def _build_select_list(self, select_fields: list[str], *, use_table_qualification: bool) -> str:
        if select_fields == ["*"]:
            return "*"

        select_parts: list[str] = []
        for index, field in enumerate(select_fields):
            if field.startswith("overflow_data."):
                json_key = field.split(".", 1)[1]
                _validate_identifier(json_key)
                alias = f"json_{index}_{json_key}"
                base_expr = self._base_column_expression("overflow_data", use_table_qualification)
                select_parts.append(f"{base_expr}->>'{json_key}' AS \"{alias}\"")
            else:
                expression, alias = self._resolve_select_expression(field, index, use_table_qualification)
                if alias:
                    select_parts.append(f'{expression} AS "{alias}"')
                else:
                    select_parts.append(expression)
        return ", ".join(select_parts)

    def _build_where_clause(
        self,
        filters: list[dict[str, Any]],
        *,
        use_table_qualification: bool,
        param_offset: int = 0,
    ) -> tuple[str, dict[str, Any]]:
        clauses: list[str] = []
        params: dict[str, Any] = {}

        for index, spec in enumerate(filters):
            field = spec.get("field")
            op = str(spec.get("op", "eq")).lower()
            value = spec.get("value")

            if not isinstance(field, str):
                msg = "Filter field must be a string"
                raise TypeError(msg)

            expression, is_jsonb, expected_type = self._resolve_where_expression(field, use_table_qualification)
            param_name = f"p_{param_offset + index}"

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
                params[param_name] = self._coerce_filter_value(value, expected_type, op)
            elif op == "ne":
                clauses.append(f"{expression} != :{param_name}")
                params[param_name] = self._coerce_filter_value(value, expected_type, op)
            elif op == "gt":
                clauses.append(f"{expression} > :{param_name}")
                params[param_name] = self._coerce_filter_value(value, expected_type, op)
            elif op == "gte":
                clauses.append(f"{expression} >= :{param_name}")
                params[param_name] = self._coerce_filter_value(value, expected_type, op)
            elif op == "lt":
                clauses.append(f"{expression} < :{param_name}")
                params[param_name] = self._coerce_filter_value(value, expected_type, op)
            elif op == "lte":
                clauses.append(f"{expression} <= :{param_name}")
                params[param_name] = self._coerce_filter_value(value, expected_type, op)
            elif op == "contains":
                if not is_jsonb:
                    msg = "contains is only supported for overflow_data.<key> filters"
                    raise ValueError(msg)
                if field.startswith("overflow_data."):
                    base_expr = self._base_column_expression("overflow_data", use_table_qualification)
                    clauses.append(f"{base_expr} @> :{param_name}::jsonb")
                else:
                    prefix, rest = field.split(".", 1)
                    if not rest.startswith("overflow_data."):
                        msg = "contains is only supported for overflow_data.<key> filters"
                        raise ValueError(msg)
                    jsonb_column = self._join_column_expression(prefix, "overflow_data")
                    clauses.append(f"{jsonb_column} @> :{param_name}::jsonb")
                params[param_name] = value
            else:
                msg = f"Unsupported filter operation: {op}"
                raise ValueError(msg)

        return " AND ".join(clauses), params

    def _resolve_join_where_expression(
        self,
        *,
        join: InferredJoin,
        alias: str,
        field_suffix: str,
    ) -> tuple[str, bool, str | None]:
        if field_suffix.startswith("overflow_data."):
            json_key = field_suffix.split(".", 1)[1]
            _validate_identifier(json_key)
            return f'"{alias}"."overflow_data"->>\'{json_key}\'', True, None

        _validate_identifier(field_suffix)
        expected_type = None
        if isinstance(join.child_column_types, dict):
            raw_type = join.child_column_types.get(field_suffix)
            if isinstance(raw_type, str):
                expected_type = raw_type
        return f'"{alias}"."{field_suffix}"', False, expected_type

    def _build_joins_clause(self) -> str:
        if not self.inferred_joins:
            return ""

        join_parts: list[str] = []
        for join in self.inferred_joins:
            _validate_identifier(join.source_field)
            _validate_identifier(join.child_table)
            _validate_identifier(join.parent_fk_column)

            alias = self._join_alias(join.source_field)
            join_parts.append(
                f'LEFT JOIN "{join.child_table}" AS "{alias}" '
                f'ON "{alias}"."{join.parent_fk_column}" = "{self.table_name}"."id"'
            )

        return " ".join(join_parts)

    def _resolve_select_expression(
        self,
        field: str,
        index: int,
        use_table_qualification: bool,
    ) -> tuple[str, str | None]:
        if "." not in field:
            if field in self._join_by_source_field:
                alias = self._join_alias(field)
                return f'row_to_json("{alias}")', field
            _validate_identifier(field)
            return self._base_column_expression(field, use_table_qualification), None

        prefix, rest = field.split(".", 1)
        if prefix not in self._join_by_source_field:
            msg = f"Unknown field prefix for join inference: {prefix}"
            raise ValueError(msg)

        if rest.startswith("overflow_data."):
            json_key = rest.split(".", 1)[1]
            _validate_identifier(json_key)
            alias = f"json_{index}_{prefix}_{json_key}"
            return f"{self._join_column_expression(prefix, 'overflow_data')}->>'{json_key}'", alias

        _validate_identifier(rest)
        alias = f"{prefix}_{rest}"
        return self._join_column_expression(prefix, rest), alias

    def _resolve_where_expression(self, field: str, use_table_qualification: bool) -> tuple[str, bool, str | None]:
        if field.startswith("overflow_data."):
            json_key = field.split(".", 1)[1]
            _validate_identifier(json_key)
            base_expr = self._base_column_expression("overflow_data", use_table_qualification)
            return f"{base_expr}->>'{json_key}'", True, None

        if "." not in field:
            _validate_identifier(field)
            return self._base_column_expression(field, use_table_qualification), False, None

        prefix, rest = field.split(".", 1)
        if prefix not in self._join_by_source_field:
            msg = f"Unknown field prefix for join inference: {prefix}"
            raise ValueError(msg)

        if rest.startswith("overflow_data."):
            json_key = rest.split(".", 1)[1]
            _validate_identifier(json_key)
            return f"{self._join_column_expression(prefix, 'overflow_data')}->>'{json_key}'", True, None

        _validate_identifier(rest)
        join = self._join_by_source_field[prefix]
        expected_type = None
        if isinstance(join.child_column_types, dict):
            raw_type = join.child_column_types.get(rest)
            if isinstance(raw_type, str):
                expected_type = raw_type
        return self._join_column_expression(prefix, rest), False, expected_type

    def _raise_value_error(self, message: str) -> None:
        raise ValueError(message)

    def _coerce_filter_value(self, value: Any, expected_type: str | None, op: str) -> Any:
        if expected_type is None or value is None:
            return value

        normalized_type = expected_type.strip().lower()
        result = value

        try:
            if normalized_type == "int":
                if isinstance(value, bool):
                    self._raise_value_error("bool is not a valid int filter value")
                result = int(value)

            elif normalized_type == "float":
                if isinstance(value, bool):
                    self._raise_value_error("bool is not a valid float filter value")
                result = float(value)

            elif normalized_type == "bool":
                if isinstance(value, bool):
                    result = value
                elif isinstance(value, (int, float)) and not isinstance(value, bool):
                    if value in (0, 1):
                        result = bool(value)
                    else:
                        self._raise_value_error("Numeric bool filter value must be 0 or 1")
                elif isinstance(value, str):
                    lowered = value.strip().lower()
                    if lowered in {"true", "1", "yes", "y"}:
                        result = True
                    elif lowered in {"false", "0", "no", "n"}:
                        result = False
                    else:
                        self._raise_value_error("Invalid boolean filter value")
                else:
                    self._raise_value_error("Invalid boolean filter value")

            if normalized_type in {"date", "datetime", "timestamp"}:
                if isinstance(value, datetime):
                    result = value
                elif isinstance(value, str):
                    result = datetime.fromisoformat(value)
                else:
                    self._raise_value_error("Invalid datetime filter value")
        except (TypeError, ValueError) as exc:
            msg = f"Invalid filter value for inferred child type '{normalized_type}' and op '{op}'"
            raise ValueError(msg) from exc

        return result

    def _base_column_expression(self, column: str, use_table_qualification: bool) -> str:
        _validate_identifier(column)
        if use_table_qualification:
            return f'"{self.table_name}"."{column}"'
        if column == "overflow_data":
            return "overflow_data"
        return f'"{column}"'

    def _join_alias(self, source_field: str) -> str:
        return f"j_{source_field}"

    def _join_column_expression(self, source_field: str, column: str) -> str:
        _validate_identifier(column)
        alias = self._join_alias(source_field)
        return f'"{alias}"."{column}"'
