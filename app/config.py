"""Runtime configuration, all overridable through environment variables.

Values are read lazily (at pool creation) so test harnesses can point
DATABASE_URL at an isolated schema after importing the package.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    @property
    def database_url(self) -> str:
        return os.getenv(
            "DATABASE_URL",
            "postgresql://node:node@localhost:5439/meterdb",
        )

    @property
    def pool_min_size(self) -> int:
        return int(os.getenv("POOL_MIN_SIZE", "2"))

    @property
    def pool_max_size(self) -> int:
        return int(os.getenv("POOL_MAX_SIZE", "10"))

    @property
    def query_limit_max(self) -> int:
        return int(os.getenv("QUERY_LIMIT_MAX", "1000"))

    @property
    def init_schema(self) -> bool:
        return os.getenv("INIT_SCHEMA", "1") != "0"


settings = Settings()
