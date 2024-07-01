from dataclasses import dataclass, field
from typing import Any, Type

import fastapi
import requests

from bio_data_harmoniser.core import settings


@dataclass
class AirflowInterface:
    base_url: str
    username: str
    password: str
    exception_cls: Type[fastapi.HTTPException | Exception] = field(
        default_factory=lambda: fastapi.HTTPException
    )

    @classmethod
    def from_settings(
        cls,
        airflow_settings: settings.AirflowSettings = settings.airflow,
        exception_cls: Type[fastapi.HTTPException | Exception] = fastapi.HTTPException,
    ) -> "AirflowInterface":
        return cls(
            base_url=airflow_settings.base_url,
            username=airflow_settings.username,
            password=airflow_settings.password,
            exception_cls=exception_cls,
        )

    @property
    def api_url(self) -> str:
        return f"{self.base_url}/api/v1"

    @property
    def auth(self) -> tuple[str, str]:
        return self.username, self.password

    def get_dag_runs(self, dag_id: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        response = requests.get(
            f"{self.api_url}/dags/{dag_id}/dagRuns",
            auth=self.auth,
            params=params,
        )
        if response.status_code != 200:
            raise self.exception_cls(response.status_code, response.text)
        return response.json()["dag_runs"]

    def create_dag_run(self, dag_id: str, conf: dict[str, Any]) -> str:
        response = requests.post(
            f"{self.api_url}/dags/{dag_id}/dagRuns",
            json={"conf": conf},
            auth=self.auth,
        )
        if response.status_code != 200:
            raise self.exception_cls(response.status_code, response.text)
        return response.json()["dag_run_id"]

    def set_variable(self, name: str, value: Any, description: str | None = None) -> None:
        response = requests.post(
            f"{self.api_url}/variables",
            json={"key": name, "value": value, "description": description},
            auth=self.auth,
        )
        if response.status_code != 200:
            raise self.exception_cls(response.status_code, response.text)

    def get_variable(self, name: str, default: Any = None) -> Any:
        response = requests.get(
            f"{self.api_url}/variables/{name}",
            auth=self.auth,
        )
        if response.status_code == 404:
            return default
        if response.status_code != 200:
            raise self.exception_cls(response.status_code, response.text)
        return response.json()["value"]
