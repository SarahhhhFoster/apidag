import aiohttp
import asyncio
from typing import Dict, Any, Optional, Union, Callable
import re
from string import Template
import jsonpath_ng
from ..node import Node
from ..exceptions import (
    TemplateVariablesMissingError,
    ExcessTemplateVariablesError,
    ResultExtractionError,
    DAGExecutionError
)

class HTTPNode(Node):
    def __init__(
        self,
        node_id: str,
        url_template: str,
        http_method: str,
        body_template: Optional[str] = None,
        value_map: Optional[Dict[str, Any]] = None,
        output_map: Dict[str, Union[str, Callable]] = None,
        error_handlers: Dict[int, Callable] = None,
        default_outputs: Optional[Dict[str, Any]] = None,
        retry_count: int = 3,
        retry_delay: float = 1.0,
        max_concurrent_requests: Optional[int] = None  # Add parameter for concurrent request limiting
    ):
        super().__init__(node_id, default_outputs)
        self.url_template = url_template
        self.http_method = http_method.upper()
        self.body_template = body_template
        self.value_map = value_map or {}
        self.output_map = output_map or {}
        self.error_handlers = error_handlers or {}
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        self._request_semaphore = asyncio.Semaphore(max_concurrent_requests) if max_concurrent_requests else None

    async def process(self) -> Dict[str, Any]:
        values = {**self.value_map, **self.inputs}
        url = self._validate_and_fill_template(self.url_template, values)
        
        body = None
        if self.body_template:
            body = self._validate_and_fill_template(self.body_template, values)

        last_exception = None
        for attempt in range(self.retry_count):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.request(
                        method=self.http_method,
                        url=url,
                        json=body if body else None
                    ) as response:
                        if response.status >= 400:
                            if response.status in self.error_handlers:
                                return self.error_handlers[response.status](values)
                            elif response.status == 429:  # Rate limit
                                retry_after = float(response.headers.get('Retry-After', self.retry_delay))
                                await asyncio.sleep(retry_after)
                                continue
                            raise DAGExecutionError(
                                f"HTTP request failed with status {response.status}\n"
                                f"URL: {url}\n"
                                f"Response: {await response.text()}"
                            )
                        response_data = await response.json()
                        # Store the raw response in the outputs before extraction
                        outputs = self._extract_output(response_data)
                        if "raw_response" in self.output_map:
                            outputs["raw_response"] = response_data
                        return outputs
            
            except aiohttp.ClientError as e:
                last_exception = e
                await asyncio.sleep(self.retry_delay * (attempt + 1))
                continue

        # If we get here, all retries failed
        raise DAGExecutionError(
            f"HTTP request failed after {self.retry_count} attempts.\n"
            f"Last error: {str(last_exception)}\n"
            f"Node context: {self.get_context()}"
        )

    def _get_template_variables(self, template: str) -> set:
        """Extract variables from template"""
        return set(re.findall(r'\$\{([^}]+)\}', template))

    def _get_clean_values(self, values: Dict[str, Any]) -> Dict[str, Any]:
        """Get values without node prefixes, prioritizing direct matches"""
        template_vars = self._get_template_variables(self.url_template)
        if self.body_template:
            template_vars.update(self._get_template_variables(self.body_template))

        clean_values = {}
        # First, try direct matches
        for var in template_vars:
            if var in values:
                clean_values[var] = values[var]
                continue
                
            # Then try namespaced values
            for key, value in values.items():
                if '.' in key and key.split('.')[1] == var:
                    clean_values[var] = value
                    break
                elif '_' in key and key.split('_', 1)[1] == var:
                    clean_values[var] = value
                    break

        return clean_values

    def _validate_and_fill_template(self, template: str, values: Dict[str, Any]) -> str:
        template_vars = self._get_template_variables(template)
        clean_values = self._get_clean_values(values)
        
        # Check for missing variables
        missing_vars = template_vars - set(clean_values.keys())
        if missing_vars:
            raise TemplateVariablesMissingError(
                f"Missing required variables: {missing_vars}"
            )

        return Template(template).safe_substitute(clean_values)

    def _extract_output(self, response_data: Dict[str, Any]) -> Dict[str, Any]:
        results = {}
        
        for output_key, extractor in self.output_map.items():
            if isinstance(extractor, str):
                # Use JSONPath
                jsonpath_expr = jsonpath_ng.parse(extractor)
                matches = [match.value for match in jsonpath_expr.find(response_data)]
                if not matches:
                    raise ResultExtractionError(
                        f"JSONPath '{extractor}' found no matches for output '{output_key}'"
                    )
                results[output_key] = matches[0] if len(matches) == 1 else matches
            elif callable(extractor):
                # Use lambda function
                try:
                    results[output_key] = extractor(response_data)
                except Exception as e:
                    raise ResultExtractionError(
                        f"Lambda extractor failed for output '{output_key}': {str(e)}"
                    )
            else:
                raise ValueError(f"Invalid extractor type for output '{output_key}'")

        return results