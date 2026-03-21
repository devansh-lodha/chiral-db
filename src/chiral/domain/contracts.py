# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Domain contracts for normalized and JSONB-overflow field categories."""

from dataclasses import dataclass


@dataclass(frozen=True)
class FieldContracts:
    """Canonical labels for field-shape contracts in the ingestion pipeline."""

    stable_scalar: str = "stable_scalar"
    nested_struct: str = "nested_struct"
    drift_prone: str = "drift_prone"
    immutable_required: str = "immutable_required"


FIELD_CONTRACTS = FieldContracts()
