from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Sequence

import jaydebeapi

from .config import PgJdbcConfig
from .types import Column, RowDict, Rows


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str, what: str) -> str:
    if not name or not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid {what}: {name!r}")
    return name


def _pg_type_to_dku(pg_type: str) -> str:
    """
    Simple Postgres -> Dataiku type mapping
    """
    t = (pg_type or "").lower()

    if t in ("smallint", "integer", "bigint"):
        return "bigint"
    if t in ("numeric", "decimal", "real", "double precision"):
        return "double"
    if t in ("boolean",):
        return "boolean"
    if "timestamp" in t or t in ("date", "time"):
        return "date"
    return "string"


class PgJdbcClient:
    """
    Minimal JDBC client for PostgreSQL via jaydebeapi.
    Dataiku 커스텀 데이터셋에서 쓰기 좋게:
    - infer_schema(): Dataiku 테스트 시 스키마 요청 대응
    - iter_rows(): row streaming
    """

    def __init__(self, cfg: PgJdbcConfig):
        self.cfg = cfg

    def connect(self):
        return jaydebeapi.connect(
            "org.postgresql.Driver",
            self.cfg.jdbc_url(),
            self.cfg.jdbc_props(),
            self.cfg.jar_path,
        )

    # =========================
    # Schema inference for Dataiku
    # =========================
    def infer_schema(self, schema: str | None = None, table: str | None = None) -> List[Dict[str, Any]]:
        """
        Dataiku Custom Python Dataset 테스트/스키마 조회 시 호출될 수 있음.
        schema/table 인자가 없으면 cfg 기본값 사용.
        반환: [{"name": "...", "type": "..."}]
        """
        sch = schema or self.cfg.schema
        tbl = table or self.cfg.table
        if not tbl:
            raise ValueError("infer_schema requires table (either argument or cfg.table)")
        return self.get_dataiku_schema(sch, tbl)

    def get_table_columns(self, schema: str, table: str) -> List[Column]:
        schema = _validate_identifier(schema, "schema")
        table = _validate_identifier(table, "table")

        sql = """
        SELECT
            column_name,
            data_type,
            is_nullable
        FROM information_schema.columns
        WHERE table_schema = ?
          AND table_name = ?
        ORDER BY ordinal_position
        """

        out: List[Column] = []
        with self.connect() as conn:
            curs = conn.cursor()
            try:
                curs.execute(sql, [schema, table])
                for (col_name, data_type, is_nullable) in curs.fetchall():
                    out.append(
                        Column(
                            name=str(col_name),
                            pg_type=str(data_type),
                            nullable=(str(is_nullable).upper() == "YES"),
                        )
                    )
            finally:
                try:
                    curs.close()
                except Exception:
                    pass
        return out

    def get_dataiku_schema(self, schema: str, table: str) -> List[Dict[str, Any]]:
        cols = self.get_table_columns(schema, table)
        return [{"name": c.name, "type": _pg_type_to_dku(c.pg_type)} for c in cols]

    # =========================
    # Data fetch
    # =========================
    def iter_rows(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
        limit: Optional[int] = None,
    ):
        lim = limit if limit is not None else self.cfg.default_limit
        emitted = 0

        with self.connect() as conn:
            curs = conn.cursor()
            try:
                curs.execute(sql, params or [])
                cols = [d[0] for d in (curs.description or [])]

                while True:
                    batch = curs.fetchmany(self.cfg.fetch_size or 1000)
                    if not batch:
                        break
                    for r in batch:
                        yield {cols[i]: r[i] for i in range(len(cols))}
                        emitted += 1
                        if lim is not None and emitted >= lim:
                            return
            finally:
                try:
                    curs.close()
                except Exception:
                    pass

    def fetch_all(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
        limit: Optional[int] = None,
    ) -> Rows:
        return list(self.iter_rows(sql, params=params, limit=limit))