from typing import Callable, TypedDict, Optional, Union


class FBAdsInsights(TypedDict):
    name: str
    fields: list[str]
    level: str
    breakdowns: Optional[str]
    transform: Callable[[list[dict]], list[dict]]
    schema: list[dict]
    keys: dict[str, Union[str, list[str]]]
