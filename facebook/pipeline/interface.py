from typing import Optional, Callable, Any

from dataclasses import dataclass


@dataclass
class AdsInsights:
    name: str
    level: str
    fields: list[str]
    transform: Callable[[list[dict[str, Any]]], list[dict[str, Any]]]
    schema: list[dict[str, Any]]
    id_key: list[str]
    breakdowns: Optional[str] = None
