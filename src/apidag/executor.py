import asyncio
from typing import Dict, Set, Any
import logging
import time
from .graph import DAGGraph

logging.basicConfig(
    level=logging.WARNING,  # Change from INFO to WARNING
    format='%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

class DAGExecutor:
    def __init__(self, graph: DAGGraph, max_workers: int = 20):
        self.graph = graph
        self.max_workers = max_workers
        self.execution_history: Dict[str, Dict] = {}
        self.node_outputs: Dict[str, Dict[str, Any]] = {}
        self._node_lock = asyncio.Lock()
        self._processed_nodes = set()  # Completed nodes
        self._in_progress = {}  # Track {node_id: worker_id} for nodes being processed
        self.start_time = time.time()
        logger.info(f"Initialized executor with {max_workers} workers")

    def _get_elapsed_ms(self) -> float:
        """Get elapsed milliseconds since execution started"""
        return (time.time() - self.start_time) * 1000

    async def worker(self, worker_id: int) -> None:
        while True:
            node_id = None
            async with self._node_lock:
                ready_nodes = self.graph.get_ready_nodes()
                # Filter out processed and in-progress nodes
                available_nodes = ready_nodes - self._processed_nodes - set(self._in_progress.keys())
                if not available_nodes:
                    break
                node_id = available_nodes.pop()
                self._in_progress[node_id] = worker_id  # Claim ownership
                node = self.graph.nodes[node_id]

            if node_id:
                try:
                    logger.info(f"[{self._get_elapsed_ms():.2f}ms] Worker {worker_id}: Processing node {node_id}")
                    
                    node_start = time.time()
                    upstream_outputs = self._get_upstream_outputs(node_id)
                    node.inputs.update(upstream_outputs)

                    outputs = await node.execute()
                    node_duration = (time.time() - node_start) * 1000

                    async with self._node_lock:
                        self.node_outputs[node_id] = outputs
                        self.execution_history[node_id] = {
                            'inputs': node.inputs.copy(),
                            'outputs': outputs.copy(),
                            'start_time': node_start,
                            'duration_ms': node_duration,
                            'worker_id': worker_id
                        }
                        self._processed_nodes.add(node_id)
                        del self._in_progress[node_id]  # Release ownership
                    
                    logger.info(f"[{self._get_elapsed_ms():.2f}ms] Worker {worker_id}: Completed node {node_id} in {node_duration:.2f}ms")
                except Exception as e:
                    async with self._node_lock:
                        del self._in_progress[node_id]  # Release ownership on error
                    raise e

    async def execute(self) -> Dict[str, Dict[str, Any]]:
        self.graph.validate()
        self.start_time = time.time()
        logger.info(f"[0.00ms] Starting DAG execution")
        
        workers = [asyncio.create_task(self.worker(i)) 
                  for i in range(self.max_workers)]
        await asyncio.gather(*workers)
        
        total_duration = self._get_elapsed_ms()
        logger.info(f"[{total_duration:.2f}ms] DAG execution completed")
        return self.execution_history

    def _get_upstream_outputs(self, node_id: str) -> Dict[str, Any]:
        """Gather all upstream outputs with their node prefixes"""
        outputs = {}
        node = self.graph.nodes[node_id]
        
        for upstream_id in node.upstream_nodes:
            if upstream_id in self.node_outputs:
                upstream_outputs = self.node_outputs[upstream_id]
                # Add direct reference format: upstream_node.variable_name
                outputs.update({
                    f"{upstream_id}.{key}": value 
                    for key, value in upstream_outputs.items()
                })
                # Add flat format for backward compatibility
                outputs.update({
                    f"{upstream_id}_{key}": value 
                    for key, value in upstream_outputs.items()
                })
                # Add direct values for immediate consumption
                outputs.update(upstream_outputs)
        
        return outputs

    def get_provenance(self, node_id: str) -> Dict:
        result = {
            node_id: self.execution_history.get(node_id, {})
        }
        
        def gather_upstream(nid: str):
            node = self.graph.nodes[nid]
            for upstream_id in node.upstream_nodes:
                result[upstream_id] = self.execution_history.get(upstream_id, {})
                gather_upstream(upstream_id)
                
        gather_upstream(node_id)
        return result