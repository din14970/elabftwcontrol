from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping, Type

from elabapi_python import Experiment, ExperimentTemplate, Item, ItemsType
from pydantic import BaseModel
from typing_extensions import Self

from elabftwcontrol._logging import logger
from elabftwcontrol.client import ElabftwApi
from elabftwcontrol.core.interfaces import Dictable, HasIDAndMetadata
from elabftwcontrol.core.models import IdNode, NameNode
from elabftwcontrol.core.models import ObjectTypes as ElabObjectType
from elabftwcontrol.core.parsers import MetadataModel, MetadataParser

Pathlike = Path | str


class HasIDAndMetadataAndDictable(HasIDAndMetadata, Dictable):
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...


@dataclass(frozen=True)
class TypedObj:
    type: ElabObjectType
    obj: HasIDAndMetadataAndDictable


_objtype_to_class: Mapping[ElabObjectType, Type[HasIDAndMetadataAndDictable]] = {
    ElabObjectType.ITEM: Item,
    ElabObjectType.EXPERIMENT: Experiment,
    ElabObjectType.ITEMS_TYPE: ItemsType,
    ElabObjectType.EXPERIMENTS_TEMPLATE: ExperimentTemplate,
}

_class_to_objtype: Mapping[Type[HasIDAndMetadataAndDictable], ElabObjectType] = {
    Item: ElabObjectType.ITEM,
    Experiment: ElabObjectType.EXPERIMENT,
    ItemsType: ElabObjectType.ITEMS_TYPE,
    ExperimentTemplate: ElabObjectType.EXPERIMENTS_TEMPLATE,
}


class SerializedObj(BaseModel):
    """For writing and reading obj to JSON file"""

    type: str
    data: dict[str, Any]


class SerializedState(BaseModel):
    """For writing and reading state to JSON file"""

    objects: list[SerializedObj]


@dataclass(frozen=True)
class EnrichedObj:
    type: ElabObjectType
    obj: HasIDAndMetadataAndDictable
    metadata: MetadataModel

    @classmethod
    def from_typed_obj(
        cls,
        typed_obj: TypedObj,
        metadata_parser: Callable[[str | None], MetadataModel],
    ) -> Self:
        parsed_metadata = metadata_parser(typed_obj.obj.metadata)
        return cls(
            type=typed_obj.type,
            obj=typed_obj.obj,
            metadata=parsed_metadata,
        )

    @property
    def id(self) -> int:
        return self.obj.id

    def to_typed_obj(self) -> TypedObj:
        return TypedObj(type=self.type, obj=self.obj)

    def to_dict(self) -> dict[str, Any]:
        return self.to_serialized().model_dump(mode="json")

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        metadata_parser: Callable[[str | None], MetadataModel],
    ) -> Self:
        serialized = SerializedObj(**data)
        return cls.from_serialized(serialized, metadata_parser)

    def to_serialized(self) -> SerializedObj:
        return SerializedObj(
            type=self.type.value,
            data=self.obj.to_dict(),
        )

    @classmethod
    def from_serialized(
        cls,
        serialized: SerializedObj,
        metadata_parser: Callable[[str | None], MetadataModel],
    ) -> Self:
        obj_type = ElabObjectType(serialized.type)
        obj_class = _objtype_to_class[obj_type]
        obj = obj_class(**serialized.data)
        parsed_metadata = metadata_parser(obj.metadata)
        return cls(
            type=obj_type,
            obj=obj,
            metadata=parsed_metadata,
        )

    @property
    def name(self) -> str:
        elabctl_data = self.metadata.elabftwcontrol
        if elabctl_data is not None:
            if self.type.is_template():
                name = elabctl_data.template_name
            else:
                name = elabctl_data.name
        else:
            name = None

        return name or self._default_name

    @property
    def _default_name(self) -> str:
        return f"{self.type.value}_{self.id}"


@dataclass(frozen=True)
class State:
    """The current state of eLab managed by elabftwcontrol"""

    elab_obj: Mapping[IdNode, EnrichedObj]
    name_to_id: Mapping[NameNode, IdNode]
    id_to_name: Mapping[IdNode, NameNode]
    index_to_id: Mapping[int, IdNode]

    def __getitem__(self, index: int) -> EnrichedObj:
        """Purely for convenience for being able to index into the state as a list"""
        id_node = self.index_to_id[index]
        return self.elab_obj[id_node]

    def __len__(self) -> int:
        return len(self.elab_obj)

    def get_all_of_type(self, obj_type: ElabObjectType) -> dict[int, EnrichedObj]:
        return {
            _id: enriched_obj
            for (type, _id), enriched_obj in self.elab_obj.items()
            if type == obj_type
        }

    def get_by_id(
        self,
        obj_type: ElabObjectType,
        id: int,
    ) -> EnrichedObj:
        return self.get_by_id_node(IdNode(obj_type, id))

    def get_by_id_node(
        self,
        node: IdNode,
    ) -> EnrichedObj:
        return self.elab_obj[node]

    def get_by_name(
        self,
        obj_type: ElabObjectType,
        name: str,
    ) -> EnrichedObj:
        return self.get_by_name_node(NameNode(obj_type, name))

    def get_by_name_node(
        self,
        node: NameNode,
    ) -> EnrichedObj:
        type_and_id = self.name_to_id[node]
        return self.elab_obj[type_and_id]

    def get_id(self, obj_type: ElabObjectType, name: str) -> int:
        return self.get_id_from_name_node(NameNode(obj_type, name))

    def get_id_from_name_node(self, node: NameNode) -> int:
        return self.name_to_id[node].id

    def get_name(self, obj_type: ElabObjectType, id: int) -> str:
        return self.get_name_from_id_node(IdNode(obj_type, id))

    def get_name_from_id_node(self, node: IdNode) -> str:
        return self.id_to_name[node].name

    def contains_id(self, obj_type: ElabObjectType, id: int) -> bool:
        return (obj_type, id) in self.elab_obj

    def contains_id_node(self, node: IdNode) -> bool:
        return node in self.elab_obj

    def contains_name(self, obj_type: ElabObjectType, name: str) -> bool:
        return (obj_type, name) in self.name_to_id

    def contains_name_node(self, node: NameNode) -> bool:
        return node in self.name_to_id

    @classmethod
    def from_api(
        cls,
        api: ElabftwApi,
        skip_untracked: bool = True,
    ) -> Self:
        """Get the state from a remote eLabFTW instance via the API"""
        all_obj = cls._pull(api)
        return cls._from_typed_api_objs(all_obj, skip_untracked=skip_untracked)

    @classmethod
    def from_api_objs(
        cls,
        api_objs: Iterable[HasIDAndMetadataAndDictable],
    ) -> Self:
        typed_objs = cls._classify_objects(api_objs)
        return cls._from_typed_api_objs(typed_objs, skip_untracked=False)

    def to_serialized(self) -> SerializedState:
        return SerializedState(
            objects=[obj.to_serialized() for obj in self.elab_obj.values()]
        )

    @classmethod
    def from_serialized(cls, serialized: SerializedState) -> Self:
        meta_parser = MetadataParser()
        api_objs = (
            EnrichedObj.from_serialized(obj, metadata_parser=meta_parser)
            for obj in serialized.objects
        )
        return cls._from_enriched_objs(api_objs)

    def to_file(self, filepath: Pathlike) -> None:
        serialized = self.to_serialized()
        with open(filepath, "w") as f:
            f.write(serialized.model_dump_json(indent=2))

    @classmethod
    def from_file(cls, filepath: Pathlike) -> Self:
        with open(filepath, "r") as f:
            data = json.load(f)
        return cls.from_serialized(SerializedState(**data))

    @classmethod
    def _from_typed_api_objs(
        cls,
        typed_objs: Iterator[TypedObj],
        skip_untracked: bool,
    ) -> Self:
        meta_parser = MetadataParser()
        enriched_objs = cls._enrich_objs(
            typed_objs,
            metadata_parser=meta_parser,
            skip_untracked=skip_untracked,
        )
        return cls._from_enriched_objs(enriched_objs)

    @classmethod
    def _from_enriched_objs(
        cls,
        enriched_objs: Iterable[EnrichedObj],
    ) -> Self:
        elab_obj = {
            IdNode(enriched_obj.type, enriched_obj.id): enriched_obj
            for enriched_obj in enriched_objs
        }
        id_to_name, name_to_id, index_to_id = cls._get_reference_mappings(elab_obj)

        return cls(
            elab_obj=elab_obj,
            name_to_id=name_to_id,
            id_to_name=id_to_name,
            index_to_id=index_to_id,
        )

    @classmethod
    def _get_reference_mappings(
        cls,
        elab_obj: Mapping[IdNode, EnrichedObj],
    ) -> tuple[dict[IdNode, NameNode], dict[NameNode, IdNode], dict[int, IdNode]]:
        name_to_id = {}
        id_to_name = {}
        index_to_id = {}
        for i, (type_and_id, enriched_obj) in enumerate(elab_obj.items()):
            type_and_name = NameNode(enriched_obj.type, enriched_obj.name)
            name_to_id[type_and_name] = type_and_id
            id_to_name[type_and_id] = type_and_name
            index_to_id[i] = type_and_id
        return id_to_name, name_to_id, index_to_id

    @classmethod
    def _pull(cls, api: ElabftwApi) -> Iterator[TypedObj]:
        """Pull all relevant objects from the API"""
        fetch_jobs: list[
            tuple[ElabObjectType, Callable[[], Iterable[HasIDAndMetadataAndDictable]]]
        ] = [
            (ElabObjectType.EXPERIMENT, api.experiments.iter),
            (ElabObjectType.ITEM, api.items.iter),
            (ElabObjectType.ITEMS_TYPE, api.items_types.iter_full),
            (ElabObjectType.EXPERIMENTS_TEMPLATE, api.experiments_templates.iter),
        ]

        for obj_type, getter in fetch_jobs:
            logger.info(f"Pulling {obj_type.value} info...")
            objs = getter()
            for obj in objs:
                yield TypedObj(type=obj_type, obj=obj)

    @classmethod
    def _classify_objects(
        cls,
        api_objs: Iterable[HasIDAndMetadataAndDictable],
    ) -> Iterator[TypedObj]:
        for api_obj in api_objs:
            obj_type = _class_to_objtype.get(type(api_obj))
            if obj_type is None:
                logger.warning("Object %s could not be identified." % api_obj)
                continue
            yield TypedObj(type=obj_type, obj=api_obj)

    @classmethod
    def _enrich_objs(
        cls,
        objects: Iterable[TypedObj],
        metadata_parser: Callable[[str | None], MetadataModel],
        skip_untracked: bool,
    ) -> Iterator[EnrichedObj]:
        for typed_obj in objects:
            obj = typed_obj.obj

            if not obj.metadata and skip_untracked:
                continue

            enriched_obj = cls._process_obj(
                typed_obj=typed_obj,
                metadata_parser=metadata_parser,
            )

            if enriched_obj.metadata.elabftwcontrol is None and skip_untracked:
                continue

            yield enriched_obj

    @classmethod
    def _process_obj(
        cls,
        typed_obj: TypedObj,
        metadata_parser: Callable[[str | None], MetadataModel],
    ) -> EnrichedObj:
        return EnrichedObj.from_typed_obj(
            typed_obj=typed_obj,
            metadata_parser=metadata_parser,
        )


class StateManager:
    def __init__(
        self,
        state: State,
    ) -> None:
        self.state = state
