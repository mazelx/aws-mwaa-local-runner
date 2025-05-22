from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.hooks.base import BaseHook
from typing import Dict, Optional, Callable

import json

class GraphQLHttpOperator(BaseOperator):
    """
    Envoie une requête GraphQL POST en JSON avec les bons headers via HttpHook.
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

    def execute(self, context):
        payload = {
            "query": self.query,
            "variables": self.variables
        }

        self.log.info("📤 GraphQL payload envoyé :\n%s", json.dumps(payload, indent=2))
        self.log.info("📤 Headers : %s", self.headers)

        # Ajout/écrasement explicite du content-type
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
            self.log.info("📥 Réponse GraphQL :\n%s", response.text)

        # Vérification du contenu JSON si keys_check est fourni
        if self.keys_check is not None:
            try:
                response_json = response.json()
            except Exception as e:
                self.log.error("Erreur lors du décodage JSON: %s", e)
                raise
            if not self._validate_paths(response_json, self.keys_check):
                raise ValueError("La vérification keys_check a échoué sur la réponse GraphQL.")
        
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