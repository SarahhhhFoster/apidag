import networkx as nx
from typing import Dict, List, Set, Type, Any
import json
from .node import Node
from .exceptions import DAGValidationError

class DAGGraph:
    def __init__(self):
        self.graph = nx.DiGraph()
        self.nodes: Dict[str, Node] = {}

    def add_node(self, node: Node) -> None:
        self.graph.add_node(node.node_id)
        self.nodes[node.node_id] = node

    def add_edge(self, from_node_id: str, to_node_id: str) -> None:
        self.graph.add_edge(from_node_id, to_node_id)
        self.nodes[from_node_id].downstream_nodes.add(to_node_id)
        self.nodes[to_node_id].upstream_nodes.add(from_node_id)

    def validate(self) -> None:
        if not nx.is_directed_acyclic_graph(self.graph):
            raise DAGValidationError("Graph contains cycles")

        # Validate all nodes have their required inputs
        for node_id in nx.topological_sort(self.graph):
            node = self.nodes[node_id]
            if not all(key in node.inputs for key in getattr(node, 'required_inputs', [])):
                raise DAGValidationError(f"Node {node_id} is missing required inputs")

    def get_ready_nodes(self) -> Set[str]:
        ready_nodes = set()
        for node_id, node in self.nodes.items():
            if not node.is_processed():
                if all(self.nodes[upstream_id].is_processed() 
                      for upstream_id in node.upstream_nodes):
                    ready_nodes.add(node_id)
        return ready_nodes

    def to_dict(self) -> Dict:
        return {
            'nodes': {
                node_id: {
                    'type': type(node).__name__,
                    'inputs': node.inputs,
                    'upstream_nodes': list(node.upstream_nodes)
                } for node_id, node in self.nodes.items()
            }
        }

    def save(self, filepath: str) -> None:
        with open(filepath, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)