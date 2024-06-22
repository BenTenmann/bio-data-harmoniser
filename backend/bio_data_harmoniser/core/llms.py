import functools
import textwrap
from dataclasses import dataclass, field
from typing import Final, Literal

import langchain_anthropic
import langchain_openai
import numpy as np
import pandas as pd
import sentence_transformers
from langchain_core.language_models import BaseLanguageModel
from sklearn import metrics

Provider = Literal["anthropic", "openai"]

DEFAULT_PROVIDER: Final[Provider] = "openai"
DEFAULT_LLM_MODEL: Final[str] = "gpt-4o"
DEFAULT_ENCODER_MODEL: Final[str] = "mixedbread-ai/mxbai-embed-large-v1"


def clean_prompt_formatting(prompt: str) -> str:
    return textwrap.dedent(prompt).strip()


@functools.cache
def get_llm(provider: Provider = DEFAULT_PROVIDER, model_name: str = DEFAULT_LLM_MODEL) -> BaseLanguageModel:
    if provider == "anthropic":
        llm = langchain_anthropic.ChatAnthropic(
            model=model_name,
            temperature=0.0,
        )
    elif provider == "openai":
        llm = langchain_openai.ChatOpenAI(
            model_name=model_name,
            temperature=0.0,
        )
    else:
        raise ValueError(f"Unknown provider {provider}")
    return llm


@functools.cache
def get_encoder(
    model_name: str = DEFAULT_ENCODER_MODEL, device: str = "cpu"
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
