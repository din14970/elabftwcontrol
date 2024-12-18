from typing import Callable, Generic, Iterable, Mapping, NamedTuple, TypeVar

from typing_extensions import Self

from elabftwcontrol._logging import logger
from elabftwcontrol.client import ElabftwApi
from elabftwcontrol.core.interfaces import HasIDAndMetadata
from elabftwcontrol.core.models import IdNode, NameNode
from elabftwcontrol.core.models import ObjectTypes as ElabObjectType
from elabftwcontrol.core.parsers import MetadataModel, MetadataParser

T = TypeVar("T", bound=HasIDAndMetadata)


class EnrichedObj(Generic[T]):
    def __init__(
        self,
        type: ElabObjectType,
        obj: T,
        metadata: MetadataModel,
    ) -> None:
        self.type = type
        self.obj = obj
        self.metadata = metadata

    @property
    def id(self) -> int:
        return self.obj.id

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


class State(NamedTuple):
    """The current state of eLab managed by elabftwcontrol"""

    elab_obj: Mapping[IdNode, EnrichedObj[HasIDAndMetadata]]
    name_to_id: Mapping[NameNode, IdNode]
    id_to_name: Mapping[IdNode, NameNode]

    def get_all_of_type(
        self, obj_type: ElabObjectType
    ) -> dict[int, EnrichedObj[HasIDAndMetadata]]:
        return {
            _id: enriched_obj
            for (type, _id), enriched_obj in self.elab_obj.items()
            if type == obj_type
        }

    def get_by_id(
        self,
        obj_type: ElabObjectType,
        id: int,
    ) -> EnrichedObj[HasIDAndMetadata]:
        return self.get_by_id_node(IdNode(obj_type, id))

    def get_by_id_node(
        self,
        node: IdNode,
    ) -> EnrichedObj[HasIDAndMetadata]:
        return self.elab_obj[node]

    def get_by_name(
        self,
        obj_type: ElabObjectType,
        name: str,
    ) -> EnrichedObj[HasIDAndMetadata]:
        return self.get_by_name_node(NameNode(obj_type, name))

    def get_by_name_node(
        self,
        node: NameNode,
    ) -> EnrichedObj[HasIDAndMetadata]:
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
    def pull(
        cls,
        api: ElabftwApi,
        skip_untracked: bool = True,
    ) -> Self:
        elab_obj = {}
        meta_parser = MetadataParser()
        fetch_jobs: list[
            tuple[ElabObjectType, Callable[[], Iterable[HasIDAndMetadata]]]
        ] = [
            (ElabObjectType.EXPERIMENT, api.experiments.iter),
            (ElabObjectType.ITEM, api.items.iter),
            (ElabObjectType.ITEMS_TYPE, api.items_types.iter_full),
            (ElabObjectType.EXPERIMENTS_TEMPLATE, api.experiments_templates.iter),
        ]

        for obj_type, getter in fetch_jobs:
            logger.info(f"Pulling {obj_type.value} info...")
            objs = getter()
            retrieved_objs = cls._get_objs(
                objs,
                obj_type=obj_type,
                metadata_parser=meta_parser,
                skip_untracked=skip_untracked,
            )
            elab_obj.update(retrieved_objs)
        logger.info("Done pulling state.")

        name_to_id = {}
        id_to_name = {}
        for type_and_id, enriched_obj in elab_obj.items():
            type_and_name = NameNode(enriched_obj.type, enriched_obj.name)
            name_to_id[type_and_name] = type_and_id
            id_to_name[type_and_id] = type_and_name

        return cls(
            elab_obj=elab_obj,
            name_to_id=name_to_id,
            id_to_name=id_to_name,
        )

    @classmethod
    def _get_objs(
        cls,
        objects: Iterable[T],
        obj_type: ElabObjectType,
        metadata_parser: Callable[[str | None], MetadataModel],
        skip_untracked: bool,
    ) -> dict[IdNode, EnrichedObj[T]]:
        tracked_obj = {}
        for obj in objects:
            if not obj.metadata and skip_untracked:
                continue

            parsed_metadata = metadata_parser(obj.metadata)
            if parsed_metadata.elabftwcontrol is None and skip_untracked:
                continue

            tracked_obj[IdNode(obj_type, obj.id)] = EnrichedObj(
                type=obj_type,
                obj=obj,
                metadata=parsed_metadata,
            )
        return tracked_obj


class StateManager:
    def __init__(
        self,
        state: State,
    ) -> None:
        self.state = state
