from pathlib import Path
from typing import Any, Iterable, Optional, Protocol, Union

Pathlike = Union[Path, str]


class HasIDAndMetadata(Protocol):
    id: int
    metadata: Optional[str]


class HasTags(Protocol):
    tags: Optional[str]


class Dictable(Protocol):
    def to_dict(self) -> dict[str, Any]: ...


class Category(Dictable, HasIDAndMetadata):
    title: str


class CategoriesApi(Protocol):
    def read(self, id: int) -> Category: ...

    def iter(self) -> Iterable[Category]: ...


class ElabApiObjInterface(HasIDAndMetadata, Dictable):
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
