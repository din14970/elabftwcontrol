from __future__ import annotations

from collections import OrderedDict
from typing import Dict, Iterable, List, Literal, NamedTuple, Sequence

from elabftwcontrol.client import ObjectSyncer
from elabftwcontrol._logging import logger
from elabftwcontrol.graph import DependencyGraph
from elabftwcontrol.manifests import Node, ObjectManifest, StateDefinition
from elabftwcontrol.parsers import ParsedProject
from elabftwcontrol.state import ElabState

DefinitionAction = Literal["create", "update", "check"]
StateAction = Literal["remove"]


class Executor:
    """This class executes plans"""

    def __init__(
        self,
        definition: StateDefinition,
        state: ElabState,
    ) -> None:
        self.definition = definition
        self.state = state

    @classmethod
    def from_parsed_project(cls, parsed_project: ParsedProject) -> Executor:
        return cls(
            definition=parsed_project.state_definition,
            state=parsed_project.state,
        )

    def get_plan(self) -> Plan:
        return Planner(definition=self.definition, state=self.state).plan()

    def apply(self, syncer: ObjectSyncer) -> None:
        plan = self.get_plan()

        node: Node
        action: str

        for node, action in plan.state_node_actions.items():
            elab_obj = self.state.get_by_node(node)
            if action == "remove":
                try:
                    syncer.delete(elab_obj)
                    self.state.remove_item(node.obj_type, node.label)
                except Exception as e:
                    logger.error(f"Could not remove {node}: {e}")
            else:
                logger.error(f"Action {action} not recognized, skipping {node}")

        for node, action in plan.definition_node_actions.items():
            pass


class Plan(NamedTuple):
    """This is the execution plan"""

    definition_node_actions: OrderedDict[Node, DefinitionAction]
    state_node_actions: OrderedDict[Node, StateAction]


class Planner:
    """This class determines the work that needs to be done by comparing manifests and state"""

    def __init__(
        self,
        definition: StateDefinition,
        state: ElabState,
    ) -> None:
        self.definition = definition
        self.state = state

    @classmethod
    def from_parsed_project(cls, parsed_project: ParsedProject) -> Planner:
        return cls(
            definition=parsed_project.state_definition,
            state=parsed_project.state,
        )

    def plan(self) -> Plan:
        state_nodes = self.state.get_nodes()
        ordered_manifests = self.get_ordered_manifests(self.definition.get_manifests())

        definition_node_actions: OrderedDict[Node, DefinitionAction] = OrderedDict()

        for manifest in ordered_manifests:
            comparison_node = Node.from_manifest(manifest)
            if comparison_node in state_nodes:
                elab_obj = self.state.get_by_node(comparison_node)

                assert (
                    elab_obj.id is not None
                ), "Elab objects in state should always have a valid id"

                if manifest.id is None:
                    manifest.id = elab_obj.id

                assert (
                    manifest.id == elab_obj.id
                ), f"Elab object and manifest {comparison_node} have different IDs: {elab_obj.id} vs {manifest.id}"

                # default action is checking, because dependencies may not have been created
                action: Literal["check", "update"] = "check"

                all_dependencies_exist = True
                for dependent_node in manifest.get_dependencies():
                    if dependent_node not in state_nodes:
                        all_dependencies_exist = False
                        break

                if all_dependencies_exist:
                    if manifest.is_different_from(
                        elab_obj=elab_obj, state_definition=self.definition
                    ):
                        action = "update"
                    else:
                        continue

                definition_node_actions[comparison_node] = action
                state_nodes.remove(comparison_node)
            else:
                definition_node_actions[comparison_node] = "create"

        return Plan(
            definition_node_actions=definition_node_actions,
            state_node_actions=self._order_node_removal(state_nodes),
        )

    @classmethod
    def _order_node_removal(
        cls, nodes: Iterable[Node]
    ) -> OrderedDict[Node, StateAction]:
        classified_nodes: Dict[str, List[Node]] = {}

        for node in nodes:
            if node.obj_type not in classified_nodes:
                classified_nodes[node.obj_type] = []

            classified_nodes[node.obj_type].append(node)

        ordered_nodes: OrderedDict[Node, StateAction] = OrderedDict()

        removal_order = [
            "links",
            "experiment",
            "item",
            "experiment_template",
            "items_type",
        ]

        assert (
            set(classified_nodes.keys()) - set(removal_order) == set()
        ), "There are unknown node classes to remove"

        for category in removal_order:
            objects_of_category = classified_nodes.get(category, [])
            for node in objects_of_category:
                ordered_nodes[node] = "remove"

        return ordered_nodes

    @classmethod
    def get_ordered_manifests(
        cls,
        manifests: Sequence[ObjectManifest],
    ) -> List[ObjectManifest]:
        """Manifests in which order they should be created"""
        node_manifest_map = cls._get_object_dict(manifests)
        dependency_graph = cls._build_object_graph(manifests)
        ordered_nodes = dependency_graph.get_ordered_nodes()
        return [node_manifest_map[node] for node in ordered_nodes]

    @classmethod
    def _get_object_dict(
        cls,
        manifests: Sequence[ObjectManifest],
    ) -> Dict[Node, ObjectManifest]:
        return {
            Node.from_manifest(manifest): manifest.copy(deep=True)
            for manifest in manifests
        }

    @classmethod
    def _build_object_graph(
        cls,
        manifests: Sequence[ObjectManifest],
    ) -> DependencyGraph[Node]:
        graph: DependencyGraph[Node] = DependencyGraph()

        # verify uniqueness of nodes and add them to the graph
        for manifest in manifests:
            new_node = Node.from_manifest(manifest)
            if graph.has_node(new_node):
                raise ValueError(
                    f"Node {new_node} is duplicated! Labels on objects should be unique!"
                )
            graph.add_node(new_node)

        # populate the graph with edges
        for manifest in manifests:
            source_node = Node.from_manifest(manifest)
            destination_nodes = manifest.get_dependencies()
            for destination_node in destination_nodes:
                graph.add_edge(source_node, destination_node)

        return graph
