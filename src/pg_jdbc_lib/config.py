from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class PgJdbcConfig:
    # JDBC driver jar path (postgresql-xx.jar)
    jar_path: str

    # Connection
    host: str
    port: int = 5432
    database: str = "postgres"

    # Credentials
    user: str = "postgres"
    password: str = ""

    # Defaults for dataset usage
    schema: str = "public"
    table: Optional[str] = None

    # Fetch options
    fetch_size: int = 1000
    default_limit: Optional[int] = None

    # Extra options
    ssl: Optional[bool] = None

    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    def jdbc_props(self) -> dict:
        props = {"user": self.user, "password": self.password}
        if self.ssl is not None:
            props["ssl"] = "true" if self.ssl else "false"
        return props