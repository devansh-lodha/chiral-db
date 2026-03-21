# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Configuration settings for the application."""

from functools import lru_cache
from pathlib import Path
from typing import Self

from pydantic import PostgresDsn, computed_field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # PostgreSQL Configuration
    POSTGRES_USER: str = ""
    POSTGRES_PASSWORD: str = ""
    POSTGRES_DB: str = ""
    POSTGRES_PORT: int = 5432
    POSTGRES_HOST: str = "localhost"

    # Routing placeholders for upcoming schema/routing tuning phases
    ROUTING_STABILITY_THRESHOLD: float = 1.0
    ROUTING_TYPE_DRIFT_THRESHOLD: float = 0.0
    ROUTING_TYPE_CONFIDENCE_THRESHOLD: float = 0.8
    ROUTING_NESTING_DEPTH_THRESHOLD: int = 2
    ROUTING_FIELD_STABILITY_RATIO_THRESHOLD: float = 0.75

    # Migration performance settings
    MIGRATION_INSERT_BATCH_SIZE: int = 100
    ENABLE_PERFORMANCE_LOGGING: bool = True

    # Phase 8 observability and guardrails
    ENABLE_STRUCTURED_METRICS_LOGGING: bool = True
    GUARDRAIL_MAX_FIELD_BYTES: int = 65536
    GUARDRAIL_MAX_NESTING_DEPTH: int = 8
    GUARDRAIL_MAX_DRIFT_EVENTS_PER_SESSION: int = 200
    GUARDRAIL_MAX_SAFETY_EVENTS_PER_SESSION: int = 500

    model_config = SettingsConfigDict(
        env_file=Path(__file__).resolve().parent.parent.parent / ".env",
        env_ignore_empty=True,
        extra="ignore",
    )

    @model_validator(mode="after")
    def verify_required_fields(self) -> Self:
        """Ensure all required fields are present and not empty."""
        required_fields = [
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "POSTGRES_DB",
        ]
        missing = [f for f in required_fields if not getattr(self, f)]
        if missing:
            msg = f"Missing required environment variables: {', '.join(missing)}"
            raise ValueError(msg)
        return self

    @computed_field
    @property
    def database_url(self) -> str:
        """Construct the PostgreSQL database URL."""
        return str(
            PostgresDsn.build(
                scheme="postgresql+asyncpg",
                username=self.POSTGRES_USER,
                password=self.POSTGRES_PASSWORD,
                host=self.POSTGRES_HOST,
                port=self.POSTGRES_PORT,
                path=self.POSTGRES_DB,
            )
        )


@lru_cache
def get_settings() -> Settings:
    """Return a cached instance of the Settings class."""
    return Settings()
