from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class RunResult:
    run_id: str
    items: List[dict]
    items_count: int
    started_at: str
    finished_at: str
    output_path: Optional[str]
    output_format: Optional[str]
    storage_path: Optional[str]
    counties: Optional[List[str]]
    query: str
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "items": self.items,
            "items_count": self.items_count,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "output_path": self.output_path,
            "output_format": self.output_format,
            "storage_path": self.storage_path,
            "counties": self.counties,
            "query": self.query,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
        }
