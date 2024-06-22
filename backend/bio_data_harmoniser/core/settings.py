from typing import Literal

import pydantic_settings

LlmProvider = Literal["openai", "anthropic"]


class AirflowSettings(pydantic_settings.BaseSettings):
    base_url: str = "http://localhost:8080"
    username: str = "admin"
    password: str = "admin"

    output_dir: str = "/tmp/airflow"

    class Config:
        env_prefix = "AIRFLOW_"


class FastAPISettings(pydantic_settings.BaseSettings):
    host: str = "0.0.0.0"
    port: int = 80
    upload_dir: str = "/tmp/uploads"

    class Config:
        env_prefix = "FASTAPI_"


class OntologySettings(pydantic_settings.BaseSettings):
    path: str = "data/ontology"

    class Config:
        env_prefix = "ONTOLOGY_"


class LlmSettings(pydantic_settings.BaseSettings):
    provider: LlmProvider = "openai"
    model: str = "gpt-4o"
    embedding_model: str = "mixedbread-ai/mxbai-embed-large-v1"

    class Config:
        env_prefix = "LLM_"


airflow = AirflowSettings()
fastapi = FastAPISettings()
ontology = OntologySettings()
llms = LlmSettings()
