<img style="display: block; margin-left: auto; margin-right: auto; width: 30%;" src="img/apidag.png" alt="apidag logo" />

# APIDAG

APIDAG is a Python library for building and executing Directed Acyclic Graphs (DAGs) of API calls and transformations. It allows you to create complex workflows where the output of one API call can be transformed and used as input for subsequent calls.

## Features

- Create DAGs with HTTP API calls and data transformations
- Automatic retry handling for failed requests
- Concurrent execution of independent nodes
- JSONPath-based response parsing
- Error handling with custom handlers
- Rate limiting support
- Detailed execution history

## Installation

```bash
pip install apidag
```

## Example

Here's a simple example that fetches an XKCD comic and looks up the definition of its first word:

```python
from apidag.nodes.http import HTTPNode
from apidag.nodes.source import SourceNode
from apidag.nodes.transformer import TransformerNode
from apidag.graph import DAGGraph
from apidag.executor import DAGExecutor

# Create a DAG
dag = DAGGraph()

# Source node with comic ID
source = SourceNode(
    node_id="input_source",
    id="2630"
)

# XKCD API node
xkcd_node = HTTPNode(
    node_id="xkcd_api",
    url_template="https://xkcd.com/\${id}/info.0.json",
    http_method="GET",
    output_map={
        "title": "$.safe_title"
    }
)

# Dictionary API node
dictionary_node = HTTPNode(
    node_id="dictionary_api",
    url_template="https://api.dictionaryapi.dev/api/v2/entries/en/\${word}",
    http_method="GET",
    output_map={
        "definitions": "$[0].meanings[*].definitions[*].definition"
    }
)

# Transform XKCD title to dictionary input
transformer = TransformerNode(
    node_id="title_transformer",
    transform_function=lambda outputs: {
        "word": outputs["xkcd_api.title"].split()[0].lower()
    }
)

# Add nodes and connections
dag.add_node(source)
dag.add_node(xkcd_node)
dag.add_node(transformer)
dag.add_node(dictionary_node)

dag.add_edge("input_source", "xkcd_api")
dag.add_edge("xkcd_api", "title_transformer")
dag.add_edge("title_transformer", "dictionary_api")

# Execute the DAG
executor = DAGExecutor(dag)
results = await executor.execute()
```

## Node Types

- **HTTPNode**: Makes HTTP requests with templated URLs and bodies
- **SourceNode**: Provides initial input values
- **TransformerNode**: Transforms data between nodes

## Features in Detail

### HTTP Node Configuration
- URL templating with variable substitution
- JSONPath-based response parsing
- Custom error handlers per status code
- Configurable retry behavior
- Rate limiting support

### Execution
- Concurrent execution of independent nodes
- Detailed execution history
- Progress tracking
- Error handling and recovery
