import asyncio
from apidag.nodes.http import HTTPNode
from apidag.nodes.source import SourceNode
from apidag.graph import DAGGraph
from apidag.executor import DAGExecutor

async def main():
    dag = DAGGraph()

    # Create source node with initial inputs
    source = SourceNode(
        node_id="input_source",
        user_id="1"
    )

    # Create user fetch node
    user_node = HTTPNode(
        node_id="fetch_user",
        url_template="https://jsonplaceholder.typicode.com/users/${user_id}",
        http_method="GET",
        output_map={
            "username": "$.username",
            "email": "$.email",
            "name": lambda x: f"{x['name']}",
            "user_id": "$.id"  # Propagate the user ID as a string
        }
    )

    # Create posts fetch node
    posts_node = HTTPNode(
        node_id="fetch_posts",
        url_template="https://jsonplaceholder.typicode.com/users/${user_id}/posts",
        http_method="GET",
        output_map={
            "post_titles": "$[*].title",
            "post_count": lambda x: len(x)
        }
    )

    # Add nodes and connect them
    dag.add_node(source)
    dag.add_node(user_node)
    dag.add_node(posts_node)

    # Connect the nodes
    dag.add_edge("input_source", "fetch_user")
    dag.add_edge("fetch_user", "fetch_posts")

    # Execute
    executor = DAGExecutor(dag)
    results = await executor.execute()
    
    # Print results in a more readable format
    for node_id, node_results in results.items():
        print(f"\nNode: {node_id}")
        print("Inputs:", node_results['inputs'])
        print("Outputs:", node_results['outputs'])

if __name__ == "__main__":
    asyncio.run(main())