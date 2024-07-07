import functools
import textwrap
from dataclasses import dataclass, field

import langchain_anthropic
import langchain_openai
import numpy as np
import pandas as pd
import sentence_transformers
from langchain_core.language_models import BaseLanguageModel
from langchain_core.messages import BaseMessage
from loguru import logger
from sklearn import metrics

from bio_data_harmoniser.core import settings


def clean_prompt_formatting(prompt: str) -> str:
    return textwrap.dedent(prompt).strip()


@functools.cache
def get_llm(
    provider: settings.LlmProvider = settings.llms.provider,
    model_name: str = settings.llms.model,
    api_key: str | None = settings.llms.api_key,
) -> BaseLanguageModel:
    if provider == "anthropic":
        llm = langchain_anthropic.ChatAnthropic(
            model=model_name,
            temperature=0.0,
            anthropic_api_key=api_key,
        )
    elif provider == "openai":
        llm = langchain_openai.ChatOpenAI(
            model_name=model_name,
            temperature=0.0,
            openai_api_key=api_key,
        )
    else:
        raise ValueError(f"Unknown provider {provider}")
    return llm


def wrangle_llm_response(response: str | BaseMessage) -> str:
    if isinstance(response, str):
        return response
    content = response.content
    if isinstance(content, str):
        return content
    if len(content) == 0:
        return ""
    if len(content) > 1:
        logger.warning(f"LLM returned multiple results: {content}")
    if any(not isinstance(r, str) for r in content):
        # in case there are any dicts in the result, we want to know about them
        logger.warning(f"LLM returned unexpected result: {content}")
    # since it is `list[str | dict]`, we need to convert it to a list of strings first
    # (just for safety)
    return "\n".join(map(str, content))


def call_llm(llm: BaseLanguageModel, prompt: str) -> str:
    result = llm.invoke(prompt)
    return wrangle_llm_response(result)


@functools.cache
def get_encoder(
    model_name: str = settings.llms.embedding_model, device: str = "cpu"
) -> sentence_transformers.SentenceTransformer:
    # we pass the device explicitly, because there is a bug when used with Airflow, where it just hangs
    # when it tries to infer the device
    return sentence_transformers.SentenceTransformer(model_name, device=device)


@dataclass
class Index:
    vectors: np.ndarray
    metadata: pd.DataFrame


@dataclass
class DenseRetriever:
    index: Index
    encoder: sentence_transformers.SentenceTransformer = field(
        default_factory=get_encoder
    )

    def retrieve(self, queries: list[str], top_k: int = 10) -> list[pd.DataFrame]:
        embeddings = self.encoder.encode(queries, convert_to_numpy=True)
        distances = metrics.pairwise.cosine_similarity(embeddings, self.index.vectors)
        argsort = np.argsort(-distances, axis=1)
        return [
            self.index.metadata.iloc[a[:top_k]]
            .assign(score=d[a[:top_k]])
            .reset_index(drop=True)
            for a, d in zip(argsort, distances)
        ]
