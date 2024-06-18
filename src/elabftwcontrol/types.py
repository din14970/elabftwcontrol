from __future__ import annotations

from typing import (
    Any,
    Dict,
    Iterable,
    Literal,
    Optional,
    Protocol,
    Sequence,
    Type,
    TypeVar,
    Union,
)

SingleObjectTypes = Literal["items", "experiments"]
EntityTypes = Union[SingleObjectTypes, Literal["items_types", "experiments_templates"]]
ManifestTypes = Union[EntityTypes, Literal["links"]]


class HasId(Protocol):
    id: Optional[int]


class HasLabel(Protocol):
    _label: str


class StringAble(Protocol):
    def __str__(self) -> str:
        ...


class Dictable(Protocol):
    def to_dict(self) -> Dict[str, Any]:
        ...


class ApiResponseObject(Dictable):
    id: int


T = TypeVar("T", bound=ApiResponseObject, covariant=True)


class EntityRUD(Protocol[T]):
    def read(self, id: int) -> T:
        ...

    def iter(self) -> Iterable[T]:
        ...

    def patch(self, id: int, body: Dict[str, Any]) -> None:
        ...

    def delete(self, id: int) -> None:
        ...


class GroupEntityCreate(Protocol):
    def create(self) -> int:
        ...


class SingleEntityCreate(Protocol):
    def create(self, category_id: int) -> int:
        ...


V = TypeVar("V", bound="ElabObj")


class ElabObj(Protocol):
    """Abstract definition of a concrete elab object"""

    obj_type: EntityTypes
    label: str
    id: Optional[int]
    data: Any
    updatable_fields: Sequence[str]

    def get(self, property: str) -> Any:
        ...

    def to_dict(self) -> Dict[str, Any]:
        ...

    @classmethod
    def from_api_data(
        cls: Type[V],
        label: str,
        data: Dictable,
    ) -> V:
        ...
