from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Iterable

from ..models import RawEvent, TriggerEvent


class TriggerConnector(ABC):
    """A connector produces raw events and can normalize them into trigger events."""

    connector_key: str

    @abstractmethod
    def poll(
        self,
        *,
        county: str,
        now_iso: str,
        limit: int,
    ) -> list[RawEvent]:
        raise NotImplementedError

    @abstractmethod
    def normalize(self, raw: RawEvent, *, now_iso: str) -> TriggerEvent | None:
        raise NotImplementedError


_CONNECTORS: Dict[str, type[TriggerConnector]] = {}


def register_connector(cls: type[TriggerConnector]) -> type[TriggerConnector]:
    key = (getattr(cls, "connector_key", "") or "").strip().lower()
    if not key:
        raise ValueError("connector_key is required")
    _CONNECTORS[key] = cls
    return cls


def get_connector(connector_key: str) -> TriggerConnector:
    key = (connector_key or "").strip().lower()
    cls = _CONNECTORS.get(key)
    if cls is None:
        raise KeyError(f"Unknown connector: {connector_key}")
    return cls()


def list_connectors() -> list[str]:
    return sorted(_CONNECTORS.keys())
