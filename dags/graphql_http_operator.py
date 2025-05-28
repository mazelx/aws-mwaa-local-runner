from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.hooks.base import BaseHook
from typing import Dict, Optional, Callable

class GraphQLHttpOperator(BaseOperator):
    """
    Sends a GraphQL POST request in JSON with proper headers via HttpHook.
    """

    template_fields = ("variables",)
    def __init__(
        self,
        *,
        http_conn_id: str,
        endpoint: str,
        query: str,
        variables: Optional[Dict] = None,
        headers: Optional[Dict[str, str]] = None,
        log_response: bool = True,
        keys_check: Optional[dict[str, type]] = None,
        post_process: Optional[Callable] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.query = query
        self.variables = variables or {}
        self.headers = headers or {}
        self.log_response = log_response
        self.keys_check = keys_check
        self.post_process = post_process

    def execute(self, context):
        payload = {
            "query": self.query,
            "variables": self.variables
        }

        self.log.info("📤 GraphQL payload sent:\n%s", payload)
        self.log.info("📤 Headers: %s", self.headers)

        # Explicitly add/overwrite content-type
        conn = BaseHook.get_connection(self.http_conn_id)
        self.headers["Content-Type"] = "application/json"
        self.headers["x-sd-api-key"] = conn.password
        self.headers["X-SPORTSDYNAMICS-SECRET"] = "Nu+R3?k!"

        hook = HttpHook(method="POST", http_conn_id=self.http_conn_id)
        response = hook.run(
            endpoint=self.endpoint,
            json=payload,
            headers=self.headers,
        )

        if self.log_response:
            self.log.info("📥 GraphQL response:\n%s", response.text)

        # JSON content validation if keys_check is provided
        if self.keys_check is not None:
            try:
                response_json = response.json()
            except Exception as e:
                self.log.error("Error decoding JSON: %s", e)
                raise
            if not self._validate_paths(response_json, self.keys_check):
                raise ValueError("keys_check validation failed on GraphQL response.")
            
        if self.post_process:
            if callable(self.post_process):
                return self.post_process(response.json())
            else:
                self.log.error("post_process should be a callable.")
                raise ValueError("post_process should be a callable.")
        
        return response.json()


    def _validate_paths(self, payload: dict, paths_with_types: dict[str, type]) -> bool:
        for path, expected_type in paths_with_types.items():
            current = payload
            for part in path.split("."):
                if not isinstance(current, dict) or part not in current:
                    return False
                current = current[part]
            if not isinstance(current, expected_type):
                return False
        return True