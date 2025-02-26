from typing import Dict, Any, Callable
from ..node import Node

class TransformerNode(Node):
    def __init__(
        self,
        node_id: str,
        transform_function: Callable[[Dict[str, Any]], Dict[str, Any]]
    ):
        super().__init__(node_id)
        self.transform_function = transform_function

    async def process(self) -> Dict[str, Any]:
        return self.transform_function(self.inputs)