from typing import Literal

import pydantic
import pydantic_settings
from airflow.models import Variable

from bio_data_harmoniser.core import utils

LlmProvider = Literal["openai", "anthropic"]
LlmEmbeddingDevice = Literal["cpu", "cuda"]

LLM_API_KEY_NAME: Literal["llm_api_key"] = "llm_api_key"


class AirflowSettings(pydantic_settings.BaseSettings):
    host: str = "localhost"
    port: int = 8080
    username: str = "admin"
    password: str = "admin"

    output_dir: str = "/tmp/airflow"

    class Config:
        env_prefix = "AIRFLOW_"

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"


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
    provider: LlmProvider = "anthropic"
    model: str = "claude-3-5-sonnet-20240620"
    embedding_model: str = "mixedbread-ai/mxbai-embed-large-v1"
    embedding_device: LlmEmbeddingDevice = "cpu"

    api_key: str | None = None

    class Config:
        env_prefix = "LLM_"

    @pydantic.field_validator("api_key")
    @classmethod
    def validate_api_key(cls, v: str | None) -> str | None:
        if v is not None:
            return v
        with utils.suppress_stdout_stderr():
            # this produces a nasty log message that we don't want
            return Variable.get(LLM_API_KEY_NAME, default_var=None)


airflow = AirflowSettings()
fastapi = FastAPISettings()
ontology = OntologySettings()
llms = LlmSettings()
