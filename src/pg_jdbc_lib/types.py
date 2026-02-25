from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List


RowDict = Dict[str, Any]
Rows = List[RowDict]


@dataclass(frozen=True)
class Column:
    name: str
    pg_type: str
    nullable: bool = True