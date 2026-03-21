# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Primary key, foreign key, and uniqueness constraint policies."""

import re
from dataclasses import dataclass, field


@dataclass(frozen=True)
class KeyPolicy:
    """Declarative PK/FK and uniqueness constraint policy."""

    # Uniqueness constraint behavior
    unique_confidence_threshold: float = 1.0

    def should_enforce_unique_constraint(
        self,
        field_unique: bool,
        unique_confidence: float,
        threshold: float | None = None,
    ) -> bool:
        """Determine if a UNIQUE constraint should be enforced on a column.

        Args:
            field_unique: Whether field was inferred as unique from sample.
            unique_confidence: Confidence ratio for uniqueness (0.0 to 1.0).
            threshold: Minimum confidence to enforce constraint. If None, uses policy default.

        Returns:
            True if constraint should be enforced; False otherwise.

        """
        if threshold is None:
            threshold = self.unique_confidence_threshold
        return field_unique and unique_confidence >= threshold


@dataclass(frozen=True)
class TableKeySpec:
    """Specification of a table's primary and foreign key constraints."""

    table_name: str
    primary_key_column: str = "id"
    primary_key_type: str = "SERIAL"
    foreign_keys: list[dict[str, str]] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Validate and normalize spec."""
        if self.foreign_keys is None:
            object.__setattr__(self, "foreign_keys", [])


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def normalize_identifier(raw: str) -> str:
    """Normalize arbitrary text into a SQL-safe identifier-like token."""
    normalized_chars = []
    for char in raw.lower():
        if char.isalnum() or char == "_":
            normalized_chars.append(char)
        else:
            normalized_chars.append("_")

    normalized = "".join(normalized_chars).strip("_")
    if not normalized:
        normalized = "entity"
    if normalized[0].isdigit():
        normalized = f"e_{normalized}"
    return normalized


def build_dynamic_child_table_name(parent_table: str, source_field: str) -> str:
    """Build deterministic child table name for decomposed repeating entities."""
    return f"{normalize_identifier(parent_table)}_{normalize_identifier(source_field)}"


def build_dynamic_child_key_spec(
    *,
    parent_table: str,
    source_field: str,
    parent_pk_column: str = "id",
    parent_pk_type: str = "SERIAL",
    include_session_fk: bool = True,
    session_parent_table: str = "session_metadata",
    session_parent_column: str = "session_id",
) -> TableKeySpec:
    """Create key policy for generated child table.

    Child tables use surrogate PK and mandatory parent FK.
    Optionally include session FK for efficient session-scoped queries.
    """
    child_table = build_dynamic_child_table_name(parent_table, source_field)

    if not IDENTIFIER_RE.fullmatch(parent_table):
        msg = f"Invalid parent table name: {parent_table}"
        raise ValueError(msg)
    if not IDENTIFIER_RE.fullmatch(parent_pk_column):
        msg = f"Invalid parent PK column: {parent_pk_column}"
        raise ValueError(msg)
    if include_session_fk and not IDENTIFIER_RE.fullmatch(session_parent_table):
        msg = f"Invalid session parent table: {session_parent_table}"
        raise ValueError(msg)
    if include_session_fk and not IDENTIFIER_RE.fullmatch(session_parent_column):
        msg = f"Invalid session parent column: {session_parent_column}"
        raise ValueError(msg)

    parent_fk_column = f"{normalize_identifier(parent_table)}_{normalize_identifier(parent_pk_column)}"
    foreign_keys: list[dict[str, str]] = [
        {
            "local_column": parent_fk_column,
            "referenced_table": parent_table,
            "referenced_column": parent_pk_column,
            "on_delete": "CASCADE",
        }
    ]

    if include_session_fk:
        foreign_keys.append(
            {
                "local_column": "session_id",
                "referenced_table": session_parent_table,
                "referenced_column": session_parent_column,
                "on_delete": "CASCADE",
            }
        )

    primary_key_type = "BIGSERIAL" if parent_pk_type.upper() == "BIGSERIAL" else "SERIAL"
    return TableKeySpec(
        table_name=child_table,
        primary_key_column="id",
        primary_key_type=primary_key_type,
        foreign_keys=foreign_keys,
    )


# Canonical key specs for chiral-db tables
CHIRAL_DATA_KEY_SPEC = TableKeySpec(
    table_name="chiral_data",
    primary_key_column="id",
    primary_key_type="SERIAL",
    foreign_keys=[
        {
            "local_column": "session_id",
            "referenced_table": "session_metadata",
            "referenced_column": "session_id",
            "on_delete": "CASCADE",
        }
    ],
)

SESSION_METADATA_KEY_SPEC = TableKeySpec(
    table_name="session_metadata",
    primary_key_column="session_id",
    primary_key_type="TEXT",
)

STAGING_DATA_KEY_SPEC = TableKeySpec(
    table_name="staging_data",
    primary_key_column="id",
    primary_key_type="SERIAL",
    foreign_keys=[
        {
            "local_column": "session_id",
            "referenced_table": "session_metadata",
            "referenced_column": "session_id",
            "on_delete": "CASCADE",
        }
    ],
)


def get_key_spec_for_table(table_name: str) -> TableKeySpec | None:
    """Retrieve canonical key spec for a table name."""
    specs = {
        "chiral_data": CHIRAL_DATA_KEY_SPEC,
        "session_metadata": SESSION_METADATA_KEY_SPEC,
        "staging_data": STAGING_DATA_KEY_SPEC,
    }
    return specs.get(table_name)
