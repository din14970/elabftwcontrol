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

    def to_dict(self) -> Dict[str, Any]: ...


class Dictable(Protocol):
    def to_dict(self) -> Dict[str, Any]: ...


class CategoriesApi(Protocol):
    def read(self, id: int) -> Category: ...

    def iter(self) -> Iterable[Category]: ...


class HasIDAndMetadataAndDictable(HasIDAndMetadata, Dictable):
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
