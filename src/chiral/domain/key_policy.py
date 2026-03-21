# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Primary key, foreign key, and uniqueness constraint policies."""

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
