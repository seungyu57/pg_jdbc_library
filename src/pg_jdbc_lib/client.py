from __future__ import annotations
from typing import Dict, Iterator, List, Optional
import jaydebeapi

from .config import PgJdbcConfig


class PgJdbcClient:
    DRIVER = "org.postgresql.Driver"

    def __init__(self, cfg: PgJdbcConfig):
        self.cfg = cfg

    def connect(self):
        return jaydebeapi.connect(
            self.DRIVER,
            self.cfg.jdbc_url,
            [self.cfg.user, self.cfg.password],
            self.cfg.jar_path
        )

    def fetch_columns(self, schema: str, table: str) -> List[str]:
        sql = f'SELECT * FROM "{schema}"."{table}" WHERE 1=0'
        conn = self.connect()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            return [d[0] for d in cur.description] if cur.description else []
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def fetch_rows(self, schema: str, table: str, limit: Optional[int] = None) -> Iterator[Dict]:
        base_sql = f'SELECT * FROM "{schema}"."{table}"'
        sql = base_sql + (f" LIMIT {int(limit)}" if limit else "")

        conn = self.connect()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            colnames = [d[0] for d in cur.description] if cur.description else []
            for row in cur.fetchall():
                yield {colnames[i]: row[i] for i in range(len(colnames))}
        finally:
            try:
                conn.close()
            except Exception:
                pass