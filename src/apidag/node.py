from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import asyncio

class Node(ABC):
    def __init__(self, node_id: str, default_outputs: Optional[Dict[str, Any]] = None):
        self.node_id = node_id
        self.inputs: Dict[str, Any] = {}
        self.outputs: Dict[str, Any] = {}
        self.upstream_nodes: set[str] = set()
        self.downstream_nodes: set[str] = set()
        self._processed = False
        self.default_outputs = default_outputs or {}

    def is_processed(self) -> bool:
        return self._processed

    @abstractmethod
    async def process(self) -> Dict[str, Any]:
        pass

    async def execute(self) -> Dict[str, Any]:
        if not self._processed:
            try:
                self.outputs = await self.process()
            except Exception as e:
                if self.default_outputs:
                    self.outputs = self.default_outputs
                    self._processed = True
                    return self.outputs
                
                # Add context to the exception
                context = self.get_context()
                raise type(e)(
                    f"Error in node {self.node_id}: {str(e)}\nNode context: {context}"
                ) from e
            
            self._processed = True
        return self.outputs

    def get_context(self) -> Dict[str, Any]:
        """Get node context for error reporting"""
        return {
            'node_id': self.node_id,
            'node_type': self.__class__.__name__,
            'inputs': self.inputs,
            'outputs': self.outputs,
            'upstream_nodes': list(self.upstream_nodes),
            'downstream_nodes': list(self.downstream_nodes),
            'processed': self._processed
        }