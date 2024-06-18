from __future__ import annotations

from typing import Any, Dict, Generic, List, Optional, Sequence, Set, TypeVar

T = TypeVar("T")


class Edge(Generic[T]):
    source: T
    destination: T


class DependencyGraph(Generic[T]):
    def __init__(
        self,
        edges: Optional[Dict[T, Set[T]]] = None,
        flexible: bool = True,
    ) -> None:
        if edges is None:
            edges = {}
        self.graph: Dict[T, Set[T]] = edges
        if flexible:
            self.add_node = self._flexible_add_node
            self.add_edge = self._flexible_add_edge
            self.get_dependencies = self._flexible_get_dependencies
        else:
            self.add_node = self._rigid_add_node
            self.add_edge = self._rigid_add_edge
            self.get_dependencies = self._rigid_get_dependencies

    def __eq__(self, o: Any) -> bool:
        return self.graph == o.graph

    def __str__(self) -> str:
        to_return = ""
        for node, connected_nodes in self.graph.items():
            to_return += f"-{node}:"
            if not connected_nodes:
                to_return += " /\n"
            else:
                to_return += "\n"
                for connected_node in connected_nodes:
                    to_return += f"   -> {connected_node}\n"

        return to_return

    def has_node(self, node: T) -> bool:
        return node in self.graph

    def _flexible_get_dependencies(self, node: T) -> Set[T]:
        return self.graph.get(node, set())

    def _rigid_get_dependencies(self, node: T) -> Set[T]:
        return self.graph[node]

    def _flexible_add_node(self, node: T) -> None:
        if not self.has_node(node):
            self._add_new_node(node)

    def _rigid_add_node(self, node: T) -> None:
        if self.has_node(node):
            raise ValueError(f"Node {node} already exists!")
        self._add_new_node(node)

    def _add_new_node(self, node: T) -> None:
        self.graph[node] = set()

    def _flexible_add_edge(self, source: T, destination: T) -> None:
        """Add an edge even if the nodes are not yet in the graph."""
        if not self.has_node(source):
            self._add_new_node(source)
        if not self.has_node(destination):
            self._add_new_node(destination)
        self._add_edge(source, destination)

    def _rigid_add_edge(self, source: T, destination: T) -> None:
        if not self.has_node(destination):
            raise ValueError(
                f"Destination {destination} does not exist as node in graph."
            )
        self._add_edge(source, destination)

    def _add_edge(self, source: T, destination: T) -> None:
        self.graph[source].add(destination)

    @classmethod
    def from_list_of_edges(cls, edges: Sequence[Edge]) -> DependencyGraph:
        graph = cls()
        for edge in edges:
            graph._flexible_add_edge(edge.source, edge.destination)
        return graph

    def is_cyclic(self) -> bool:
        """Check whether there are loops in the graph"""
        visited = set()
        recursion_stack = set()

        def depth_first_search(node: T) -> bool:
            visited.add(node)
            recursion_stack.add(node)

            for neighbor in self.graph.get(node, []):
                if neighbor not in visited:
                    if depth_first_search(neighbor):
                        return True
                elif neighbor in recursion_stack:
                    return True

            recursion_stack.remove(node)
            return False

        # Perform DFS for each node in the graph
        for node in self.graph:
            if node not in visited:
                if depth_first_search(node):
                    return True

        return False

    def get_ordered_nodes(self) -> List[T]:
        """Return the order in which nodes must be created"""
        assert not self.is_cyclic(), "Graph contains a cycle! Must be acyclic!"
        visited = set()
        ordering = []

        def depth_first_search(node: T) -> None:
            nonlocal visited, ordering
            visited.add(node)
            for neighbor in self.graph.get(node, []):
                if neighbor not in visited:
                    depth_first_search(neighbor)
            ordering.append(node)

        for node in self.graph:
            if node not in visited:
                depth_first_search(node)

        return ordering
