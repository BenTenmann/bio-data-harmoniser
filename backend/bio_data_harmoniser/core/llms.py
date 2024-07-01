import functools
import textwrap
from dataclasses import dataclass, field

import langchain_anthropic
import langchain_openai
import numpy as np
import pandas as pd
import sentence_transformers
from langchain_core.language_models import BaseLanguageModel
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
