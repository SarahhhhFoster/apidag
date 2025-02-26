from typing import Dict, Any
from ..node import Node

class SourceNode(Node):
    """A node that serves as an input source for the DAG"""
    def __init__(self, node_id: str, **kwargs):
        super().__init__(node_id)
        self.outputs = kwargs

    async def process(self) -> Dict[str, Any]:
        """Source nodes just return their initial outputs"""
        return self.outputs