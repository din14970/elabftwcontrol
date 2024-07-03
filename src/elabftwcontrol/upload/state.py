from __future__ import annotations

import json
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from pydantic import BaseModel, PrivateAttr

from elabftwcontrol.client import ObjectSyncer
from elabftwcontrol.manifests import Node
from elabftwcontrol.models import (
    WrappedExperiment,
    WrappedExperimentTemplate,
    WrappedItem,
    WrappedItemType,
    WrappedLink,
)
from elabftwcontrol.types import ElabObj, EntityTypes, HasLabel

Pathlike = Union[Path, str]


T = TypeVar("T", bound=HasLabel)
V = TypeVar("V", bound=ElabObj)


if TYPE_CHECKING:
    Obj = ElabObj
else:
    Obj = BaseModel


_pull_collections: Dict[EntityTypes, Type[ElabObj]] = {
    "items_types": WrappedItemType,
    "experiments_templates": WrappedExperimentTemplate,
    "items": WrappedItem,
    "experiments": WrappedExperiment,
}


class ElabState(BaseModel):
    elab_objects: Dict[EntityTypes, Dict[str, Obj]] = {}
    links: Dict[str, WrappedLink] = {}
    _label_map: Dict[EntityTypes, Dict[str, int]] = PrivateAttr(default_factory=dict)

    def label_has_id(
        self,
        entity_type: EntityTypes,
        label: str,
    ) -> bool:
        return label in self._label_map[entity_type]

    def get_object(self, obj_type: EntityTypes, label: str) -> ElabObj:
        category: Dict[str, ElabObj] = self.elab_objects[obj_type]
        return category[label]

    def get_by_node(self, node: Node) -> ElabObj:
        return self.get_object(
            obj_type=cast(EntityTypes, node.obj_type),
            label=node.obj_type,
        )

    def get_nodes(self) -> Set[Node]:
        """Get all (obj_type, label) combinations in the state"""
        return set(Node(obj_type, label) for obj_type, label, _ in self._iter_objects())

    def get_id_from_label(
        self,
        entity_type: EntityTypes,
        label: str,
    ) -> int:
        return self._label_map[entity_type][label]

    def add_item(
        self,
        entity_type: EntityTypes,
        elab_obj: ElabObj,
    ) -> None:
        assert (
            elab_obj.id is not None
        ), "Elab object does not have an ID and so can't be part of the state."
        dictionary = self.elab_objects[entity_type]
        label = elab_obj._label
        assert label not in dictionary, f"Label {label} already exists in the state."
        dictionary[label] = elab_obj
        self._label_map[entity_type][label] = elab_obj.id

    def modify_item(
        self,
        entity_type: EntityTypes,
        elab_obj: ElabObj,
    ) -> None:
        assert (
            elab_obj.id is not None
        ), "Elab object does not have an ID and so can't be part of the state."
        dictionary = self.elab_objects[entity_type]
        label = elab_obj._label
        dictionary[label] = elab_obj
        self._label_map[entity_type][label] = elab_obj.id

    def remove_item(
        self,
        entity_type: EntityTypes,
        label: str,
    ) -> None:
        dictionary = self.elab_objects[entity_type]
        del dictionary[label]
        del self._label_map[entity_type][label]

    @classmethod
    def from_file(cls, filepath: Pathlike) -> ElabState:
        with open(filepath, "r") as f:
            data = json.load(f)
        return cls._init_obj(data)

    @classmethod
    def from_remote(
        cls,
        syncer: ObjectSyncer,
    ) -> ElabState:
        """Create state by pulling the current items eLabFTW"""
        data: Dict[str, Any] = {}
        for var_name, obj_type in _pull_collections.items():
            data[var_name] = cls._list_to_dict(syncer.list_all(obj_type))
        return cls._init_obj(data)

    @classmethod
    def _init_obj(cls, data: Dict[str, Any]) -> ElabState:
        new_obj = cls(**data)
        new_obj._update_label_map()
        return new_obj

    @classmethod
    def _list_to_dict(cls, lst: Sequence[T]) -> Dict[str, T]:
        return {obj._label: obj for obj in lst}

    def _update_label_map(self) -> None:
        label_map: Dict[EntityTypes, Dict[str, int]] = {}
        for pull_collection, label, elab_obj in self._iter_objects():
            if pull_collection not in label_map:
                label_map[pull_collection] = {}
            if elab_obj.id is not None:
                label_map[pull_collection][label] = elab_obj.id
        self._label_map = label_map

    def _iter_objects(self) -> Iterator[Tuple[EntityTypes, str, ElabObj]]:
        """Iterate over all regular objects"""
        for pull_collection in _pull_collections.keys():
            collection = self.elab_objects.get(pull_collection, {})
            for label, elab_obj in collection.items():
                yield pull_collection, label, elab_obj

    def to_string(self) -> str:
        return self.model_dump_json(indent=2, by_alias=True, exclude_none=True)

    def to_file(self, filepath: Pathlike) -> None:
        with open(filepath, "w") as f:
            f.write(self.to_string())
