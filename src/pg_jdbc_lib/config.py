from dataclasses import dataclass

@dataclass(frozen=True)
class PgJdbcConfig:
    jar_path: str
    host: str
    port: int
    database: str
    user: str
    password: str

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"