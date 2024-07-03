from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Protocol, Union

Pathlike = Union[Path, str]


class HasIDAndMetadata(Protocol):
    id: int
    metadata: Optional[str]


class Category(Protocol):
    id: int
    title: str
    metadata: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        ...


class Dictable(Protocol):
    def to_dict(self) -> Dict[str, Any]:
        ...


class LineWriterInterface(Protocol):
    def __call__(self, lines: Iterable[str]) -> None:
        ...


class CSVWriterInterface(Protocol):
    def __call__(self, rows: Iterable[dict[str, Any]]) -> None:
        ...


class CategoriesApi(Protocol):
    def read(self, id: int) -> Category:
        ...

    def iter(self) -> Iterable[Category]:
        ...
