import asyncio
from typing import Dict
from apidag.nodes.http import HTTPNode
from apidag.nodes.source import SourceNode
from apidag.nodes.transformer import TransformerNode
from apidag.graph import DAGGraph
from apidag.executor import DAGExecutor

def create_comic_flow(comic_id: str) -> DAGGraph:
    dag = DAGGraph()

    # Source node with comic ID
    source = SourceNode(
        node_id="input_source",
        id=comic_id
    )

    # XKCD API node
    xkcd_node = HTTPNode(
        node_id="xkcd_api",
        url_template="https://xkcd.com/${id}/info.0.json",
        http_method="GET",
        output_map={
            "title": "$.safe_title"
        },
        error_handlers={
            404: lambda inputs: {"title": "No title found"}
        },
        default_outputs={"title": "Error fetching comic"},
        retry_count=3,
        retry_delay=2.0
    )

    # Dictionary API node
    dictionary_node = HTTPNode(
        node_id="dictionary_api",
        url_template="https://api.dictionaryapi.dev/api/v2/entries/en/${word}",
        http_method="GET",
        output_map={
            "definitions": "$[0].meanings[*].definitions[*].definition",  # Changed to more specific path
            "raw_response": lambda x: str(x)  # Ensure we can print the response
        },
        error_handlers={
            404: lambda inputs: {
                "definitions": [f"No definition found for '{inputs.get('word')}'"],
                "raw_response": "Not found"
            }
        },
        default_outputs={"definitions": ["Error fetching definition"], "raw_response": None},
        retry_count=3,
        retry_delay=2.0,
        max_concurrent_requests=5
    )

    # Transform XKCD output to dictionary input
    def xkcd_to_dictionary(outputs):
        title = outputs.get("xkcd_api.title", "")
        words = title.split()
        first_word = words[0].lower() if words else ""
        return {"word": first_word}

    transformer = TransformerNode(
        node_id="title_transformer",
        transform_function=xkcd_to_dictionary
    )

    # Add nodes and connections
    dag.add_node(source)
    dag.add_node(xkcd_node)
    dag.add_node(transformer)
    dag.add_node(dictionary_node)

    dag.add_edge("input_source", "xkcd_api")
    dag.add_edge("xkcd_api", "title_transformer")
    dag.add_edge("title_transformer", "dictionary_api")

    return dag

def print_results(results: Dict[str, Dict]):
    comic_id = results["input_source"]["outputs"]["id"]
    comic_title = results["xkcd_api"]["outputs"]["title"]
    word = results["dictionary_api"]["inputs"]["word"]
    definitions = results["dictionary_api"]["outputs"]["definitions"]
    raw_response = results["dictionary_api"]["outputs"]["raw_response"]

    if not isinstance(definitions, list):
        definitions = [definitions]

    if not definitions or definitions == ["Error fetching definition"]:
        print(f"Comic #{comic_id} titled '{comic_title}' has its first word '{word}' undefined.")
    else:
        print(f"Comic #{comic_id} titled '{comic_title}' has its first word '{word}' defined as:")
        for definition in definitions:
            print(f"\t{definition}")

async def process_comic(comic_id: str):
    dag = create_comic_flow(comic_id)
    executor = DAGExecutor(dag)
    results = await executor.execute()
    print_results(results)

async def main():
    comic_ids = range(2630, 2650)  # Back to 20 comics
    tasks = [process_comic(str(comic_id)) for comic_id in comic_ids]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())