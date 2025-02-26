import asyncio
from typing import Dict
from apidag.node import Node
from apidag.nodes.source import SourceNode
from apidag.graph import DAGGraph
from apidag.executor import DAGExecutor

class AddNode(Node):
    def __init__(self, node_id: str):
        super().__init__(node_id)

    async def process(self):
        await asyncio.sleep(1)  # Simulate work
        result = sum(self.inputs.values())
        return {'sum': result}

class MultiplyNode(Node):
    async def process(self):
        await asyncio.sleep(1)  # Simulate work
        # Get sums directly from the inputs using the node-prefixed keys
        values = [
            self.inputs.get(f"{node_id}.sum", 0) 
            for node_id in ['add1', 'add2']
        ]
        result = 1
        for value in values:
            result *= value
        return {'product': result}

async def main():
    # Create graph
    dag = DAGGraph()

    # Create source nodes with initial values
    source1 = SourceNode('source1', x=5, y=3)
    source2 = SourceNode('source2', x=2, y=4)
    
    # Create processing nodes
    add1 = AddNode('add1')
    add2 = AddNode('add2')
    mult = MultiplyNode('mult')

    # Add nodes to graph
    dag.add_node(source1)
    dag.add_node(source2)
    dag.add_node(add1)
    dag.add_node(add2)
    dag.add_node(mult)

    # Define edges
    dag.add_edge('source1', 'add1')
    dag.add_edge('source2', 'add2')
    dag.add_edge('add1', 'mult')
    dag.add_edge('add2', 'mult')

    # Execute
    executor = DAGExecutor(dag)
    results = await executor.execute()

    # Print results with provenance
    provenance = executor.get_provenance('mult')
    print("\nExecution Results:")
    print(f"First sum: {results['add1']['outputs']['sum']}")
    print(f"Second sum: {results['add2']['outputs']['sum']}")
    print(f"Final product: {results['mult']['outputs']['product']}")
    
    print("\nProvenance:")
    for node_id, data in provenance.items():
        print(f"{node_id}:")
        print(f"  Inputs: {data.get('inputs', {})}")
        print(f"  Outputs: {data.get('outputs', {})}")

if __name__ == "__main__":
    asyncio.run(main())