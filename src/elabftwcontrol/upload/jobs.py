from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, TypeVar

from pydantic import BaseModel, field_serializer
from typing_extensions import Self

from elabftwcontrol._logging import logger
from elabftwcontrol.client import ElabftwApi
from elabftwcontrol.core.manifests import ManifestIndex, Patch, _Spec
from elabftwcontrol.core.models import Auth, IdNode, MetadataModel, NameNode
from elabftwcontrol.types import ElabObjectType
from elabftwcontrol.upload.diff import Diff
from elabftwcontrol.upload.state import State, EnrichedObj


class JobType(Enum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"


T = TypeVar("T")


class BasePatchBody(BaseModel):
    title: str | None = None
    body: str | None = None
    canread: Auth | None = None
    canwrite: Auth | None = None
    metadata: MetadataModel | None = None

    @field_serializer(
        "canread",
        "canwrite",
        "metadata",
    )
    def serialize_dict_fields(
        self,
        field: BaseModel | None,
        _info: Any,
    ) -> str | None:
        if field is None:
            return None
        return field.model_dump_json(exclude_none=True)


class PatchExperimentsTemplateBody(BasePatchBody): ...


class PatchItemsTypeBody(BasePatchBody):
    color: str | None = None


class PatchExperimentBody(BasePatchBody):
    rating: int | None = None


class PatchItemBody(BasePatchBody):
    rating: int | None = None
    # TODO implement
    # book_can_overlap
    # book_cancel_minutes
    # book_is_cancellable
    # book_max_minutes
    # book_max_slots
    # canbook
    # is_bookable


@dataclass
class WorkEvaluatorState:
    """Keeps track of the objects in eLabFTW during the job.
    Provides easy translation between manifest name nodes and elab state id nodes.
    """

    manifest_index: ManifestIndex
    elab_state: State
    name_to_id: dict[NameNode, IdNode]
    id_to_name: dict[IdNode, NameNode]

    def get_id(self, node: NameNode) -> IdNode | None:
        return self.name_to_id.get(node)

    def get_name(self, node: IdNode) -> NameNode | None:
        return self.id_to_name.get(node)

    def get_state_obj(self, node: IdNode) -> EnrichedObj:
        return self.elab_state[node]

    def get_manifest(self, node: NameNode) -> _Spec:
        return self.manifest_index[node]

    def contains_id(self, node: IdNode) -> bool:
        return node in self.id_to_name

    def contains_name(self, node: NameNode) -> bool:
        return node in self.name_to_id

    def add(self, name: NameNode, id: IdNode) -> None:
        self.name_to_id[name] = id
        self.id_to_name[id] = name

    def remove_by_name(self, name: NameNode) -> None:
        id = self.name_to_id[name]
        del self.id_to_name[id]
        del self.name_to_id[name]

    def remove_by_id(self, id: IdNode) -> None:
        name = self.id_to_name[id]
        del self.name_to_id[name]
        del self.id_to_name[id]

    def get_parent_node_id(self, name: NameNode) -> IdNode | None:
        parent_name = self.manifest_index.parents.get(name)
        if parent_name is None:
            return None
        return self.get_id(parent_name)

    @classmethod
    def new(
        cls,
        elab_state: State,
        manifest_index: ManifestIndex,
    ) -> Self:
        name_to_id = {}
        id_to_name = {}
        for type_and_id, enriched_obj in elab_state.items():
            type_and_name = NameNode(enriched_obj.type, enriched_obj.name)
            name_to_id[type_and_name] = type_and_id
            id_to_name[type_and_id] = type_and_name
        return cls(
            manifest_index=manifest_index,
            elab_state=elab_state,
            name_to_id=name_to_id,
            id_to_name=id_to_name,
        )


class WorkTypes(str, Enum):
    APPLY = "apply"
    DESTROY = "destroy"


@dataclass
class WorkEvaluator:
    """Compares state and manifests and calculates the work plan to be done"""

    job_state: WorkEvaluatorState

    @classmethod
    def new(
        cls,
        manifest_index: ManifestIndex,
        elab_state: State,
    ) -> Self:
        job_state = WorkEvaluatorState.new(
            elab_state=elab_state,
            manifest_index=manifest_index,
        )
        return cls(job_state)

    @property
    def manifest_index(self) -> ManifestIndex:
        return self.job_state.manifest_index

    @property
    def elab_state(self) -> State:
        return self.job_state.elab_state

    def evaluate_apply(self) -> WorkPlan:
        nodes_to_apply = self.manifest_index.get_node_creation_order()
        plan = WorkPlan.new()

        for name_node in nodes_to_apply:
            if not self.job_state.contains_name(name_node):
                new_task = ElabOperation.new_create_obj(
                    name_node=name_node,
                    job_state=self.job_state,
                )
                plan.add_task(new_task)
                new_task = ElabOperation.new_update_obj(
                    name_node=name_node,
                    patch=self.get_create_patch(name_node),
                    job_state=self.job_state,
                )
                plan.add_task(new_task)

            else:
                patch = self.get_diff_patch(name_node)
                if patch:
                    new_task = ElabOperation.new_update_obj(
                        name_node=name_node,
                        patch=patch,
                        job_state=self.job_state,
                    )
                    plan.add_task(new_task)

        return plan

    def get_create_patch(self, name_node: NameNode) -> Patch:
        spec = self.manifest_index[name_node]
        return spec.to_patch(None)

    def get_diff_patch(self, name_node: NameNode) -> Patch:
        id_node = self.job_state.get_id(name_node)
        if id_node is None:
            raise ValueError(f"No matching id for {name_node} found")
        spec = self.manifest_index[name_node]
        state = self.elab_state[id_node]
        return spec.to_patch(state)

    def evaluate_destroy(self) -> WorkPlan:
        nodes_to_delete = self.manifest_index.get_node_deletion_order()
        plan = WorkPlan.new()

        for name_node in nodes_to_delete:
            if self.job_state.contains_name(name_node):
                new_task = ElabOperation.new_delete_obj(
                    name_node=name_node,
                    job_state=self.job_state,
                )
                plan.add_task(new_task)
            else:
                logger.warning(
                    f"The object {name_node} does not exist. Can not delete."
                )

        return plan


class CreationPatch:
    pass


class UpdatePatch:
    pass


@dataclass
class WorkPlan:
    tasks: list[ElabOperation]

    @classmethod
    def new(cls) -> Self:
        return cls([])

    def add_task(self, task: ElabOperation) -> None:
        self.tasks.append(task)


class ElabOperationError(Exception): ...


class DeletionError(ElabOperationError): ...


class CreationError(ElabOperationError): ...


class PatchingError(ElabOperationError): ...


@dataclass(frozen=True)
class ElabOperation:
    name_node: NameNode
    action: Callable[[ElabftwApi], IdNode]
    success_callback: Callable[[IdNode], None] | None
    failure_callback: Callable[[NameNode, Exception], None] | None

    def __call__(self, api: ElabftwApi) -> None:
        try:
            id_node = self.action(api)
        except Exception as e:
            logger.error("Could not execute '%s': %s" % (str(self.action), e))
            if self.failure_callback is not None:
                self.failure_callback(self.name_node, e)
            return

        if self.success_callback is not None:
            self.success_callback(id_node)

    @classmethod
    def new_create_obj(
        cls,
        name_node: NameNode,
        job_state: WorkEvaluatorState,
    ) -> Self:
        obj_type = name_node.kind

        def create_method_factory(api: ElabftwApi) -> CreateMethod:
            match obj_type:
                case ElabObjectType.ITEMS_TYPE:
                    return api.items_types.create
                case ElabObjectType.EXPERIMENTS_TEMPLATE:
                    return api.experiments_templates.create
                case ElabObjectType.EXPERIMENT:
                    return api.experiments.create
                case ElabObjectType.ITEM:

                    def create_item() -> int:
                        parent_node = job_state.get_parent_node_id(name_node)
                        if parent_node is None:
                            raise RuntimeError(
                                f"{name_node} requires a an item type id, which was not found"
                            )
                        category_id = parent_node.id
                        return api.items.create(category_id=category_id)

                    return create_item
                case _:
                    raise ValueError

        def success_callback(id_node: IdNode) -> None:
            pass

        def failure_callback(node: NameNode, error: Exception) -> None:
            raise CreationError(f"{node} failed to be created: {error}")

        action = CreateObj(
            name_node=name_node,
            job_state=job_state,
            create_method_factory=create_method_factory,
        )

        return cls(
            name_node=name_node,
            action=action,
            success_callback=success_callback,
            failure_callback=failure_callback,
        )

    @classmethod
    def new_update_obj(
        cls,
        name_node: NameNode,
        patch: Patch,
        job_state: WorkEvaluatorState,
    ) -> Self:
        action = UpdateObj(
            name_node=name_node,
            job_state=job_state,
            patch=patch,
        )

        def success_callback(id_node: IdNode) -> None:
            pass

        def failure_callback(node: NameNode, error: Exception) -> None:
            raise PatchingError(f"{node} failed to be patched: {error}")

        return cls(
            name_node=name_node,
            action=action,
            success_callback=success_callback,
            failure_callback=failure_callback,
        )

    @classmethod
    def new_delete_obj(
        cls,
        name_node: NameNode,
        job_state: WorkEvaluatorState,
    ) -> Self:
        obj_type = name_node.kind

        def delete_method_factory(api: ElabftwApi) -> DeleteMethod:
            match obj_type:
                case ElabObjectType.ITEMS_TYPE:
                    return api.items_types.delete
                case ElabObjectType.EXPERIMENTS_TEMPLATE:
                    return api.experiments_templates.delete
                case ElabObjectType.EXPERIMENT:
                    return api.experiments.delete
                case ElabObjectType.ITEM:
                    return api.items.delete
                case _:
                    raise ValueError

        action = DeleteObj(
            name_node=name_node,
            job_state=job_state,
            delete_method_factory=delete_method_factory,
        )

        def success_callback(id_node: IdNode) -> None:
            pass

        def failure_callback(node: NameNode, error: Exception) -> None:
            raise RuntimeError(f"{node} failed to be deleted: {error}")

        return cls(
            name_node=name_node,
            action=action,
            success_callback=success_callback,
            failure_callback=failure_callback,
        )


_RED = "\033[31m"
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_RESET = "\033[0m"


CreateMethod = Callable[[], int]


class CreateObj:
    """Command for creating a new object and returning an id"""

    def __init__(
        self,
        name_node: NameNode,
        job_state: WorkEvaluatorState,
        create_method_factory: Callable[[ElabftwApi], CreateMethod],
    ) -> None:
        self.name_node = name_node
        self.job_state = job_state
        self.create_method_factory = create_method_factory

    def __str__(self) -> str:
        return f"{_GREEN}+{_RESET} Creation of new {self.name_node}"

    def __call__(self, api: ElabftwApi) -> IdNode:
        create_method = self.create_method_factory(api)
        id = create_method()
        new_node = IdNode(kind=self.name_node.kind, id=id)
        self.job_state.add(name=self.name_node, id=new_node)
        return new_node


UpdateMethod = Callable[[int, dict[str, Any]], None]


class UpdateObj:
    def __init__(
        self,
        name_node: NameNode,
        patch: Patch,
        job_state: WorkEvaluatorState,
    ) -> None:
        self.name_node = name_node
        self.patch = patch
        self.job_state = job_state

    def __call__(self, api: ElabftwApi) -> IdNode:
        if self.id_node is None:
            raise RuntimeError(f"{self.name_node} does not exist.")

        self.patch(api)
        return self.id_node

    def __str__(self) -> str:
        if self.id_node is None:
            id_text = "ID unknown"
        else:
            id_text = str(self.id_node.id)

        return f"""\
{_YELLOW}~{_RESET} Patching of {self.name_node} ({id_text})

    {self.diff.to_str(indent=4)}
"""

    @property
    def diff(self) -> Diff:
        return self.patch.get_diff()

    @property
    def id_node(self) -> IdNode | None:
        return self.job_state.get_id(self.name_node)


DeleteMethod = Callable[[int], None]


class DeleteObj:
    """Command for deleting an API object and updating the state"""

    def __init__(
        self,
        name_node: NameNode,
        job_state: WorkEvaluatorState,
        delete_method_factory: Callable[[ElabftwApi], DeleteMethod],
    ) -> None:
        self.job_state = job_state
        self.name_node = name_node
        self.delete_method_factory = delete_method_factory

    def __call__(self, api: ElabftwApi) -> IdNode:
        id_node = self.id_node
        if id_node is None:
            raise RuntimeError(f"{self.name_node} does not exist.")

        delete_method = self.delete_method_factory(api)
        delete_method(id_node.id)
        self.job_state.remove_by_id(id_node)
        return id_node

    def __str__(self) -> str:
        if self.id_node is not None:
            node_id = str(self.id_node.id)
        else:
            node_id = "ID unknown"
        return f"{_RED}-{_RESET} Deletion of {self.name_node} ({node_id})"

    @property
    def id_node(self) -> IdNode | None:
        return self.job_state.get_id(self.name_node)
