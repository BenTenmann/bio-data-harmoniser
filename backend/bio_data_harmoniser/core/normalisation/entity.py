import asyncio
import enum
import math
from collections import namedtuple
from dataclasses import dataclass, field

import more_itertools
import numpy as np
import pandas as pd
import tenacity
import tqdm
from langchain_core.language_models import BaseLanguageModel
from loguru import logger

from bio_data_harmoniser.core import ontology, llms, logging, utils


def is_free_text(series: pd.Series, llm: BaseLanguageModel) -> bool:
    # textwrap only works if the string has the same indentation on all lines
    prompt = """
    Are the following example entries written in free text or as identifiers?

    <entries>
    {entries}
    </entries>
    
    Answer either "free text" or "identifiers" and nothing else."""
    response = llms.call_llm(
        llm=llm,
        prompt=llms.clean_prompt_formatting(prompt).format(
            entries="\n".join(
                f"<entry index={i}>{entry}</entry>"
                for i, entry in enumerate(series.dropna().unique()[:10])
            )
        ),
    )
    return response.lower().strip().strip(".").strip("'") == "free text"


def load_dense_retriever_from_entity_types(
    entity_types: list[ontology.EntityType], store: ontology.OntologyStore
) -> llms.DenseRetriever:
    df = store.load(entity_types=entity_types)
    vectors = np.stack(df[store.columns.embedding])
    return llms.DenseRetriever(
        index=llms.Index(
            vectors=vectors, metadata=df.drop(columns=[store.columns.embedding])
        ),
    )


class NormalisationAlgorithm(enum.Enum):
    RETRIEVAL = "retrieval"
    RETRIEVAL_AND_CLASSIFICATION = "retrieval_and_classification"
    AGENTIC_RETRIEVAL_AND_CLASSIFICATION = "agentic_retrieval_and_classification"


def _retrieve(
    series: pd.Series, dense_retriever: llms.DenseRetriever, top_k: int = 10
) -> pd.DataFrame:
    mentions = series.dropna().unique().tolist()
    results = pd.concat(
        [
            result[
                [ontology.OntologyColumns.id, ontology.OntologyColumns.name, "score"]
            ].assign(mention=mention)
            for mention, result in zip(
                mentions, dense_retriever.retrieve(mentions, top_k=top_k)
            )
        ],
        ignore_index=True,
    )
    return results


class _LlmCallException(Exception):
    pass


def _classify(
    retrieved_results: pd.DataFrame, llm: BaseLanguageModel, chunk_size: int = 64
) -> pd.DataFrame:
    prompt = """
    You are given a mention of a scientific entity and a list of potential entities that could matcht mention, from the knowledge base.
    Your task is to select the most relevant entity from the list.
    
    <mention>
    {mention}
    </mention>
    
    <entities>
    {entities}
    </entities>
    
    Answer with the name of the most relevant entity and nothing else.
    """

    Entity = namedtuple("Entity", ["name", "id", "score"])

    @tenacity.retry(
        wait=tenacity.wait_random_exponential(min=1, max=60),
        stop=tenacity.stop_after_attempt(5),
        retry=tenacity.retry_if_exception_type(_LlmCallException),
        reraise=True,
    )
    async def _classify_mention(mention: str, entities: list[Entity]) -> Entity:
        first_entity = entities[0]
        if np.isclose(first_entity.score, 1.0):
            logger.debug("First entity has a score of 1.0, so probably an exact match")
            return first_entity

        try:
            response = await llm.ainvoke(
                llms.clean_prompt_formatting(prompt).format(
                    mention=mention,
                    entities="\n".join(
                        f"<entity>{entity.name}</entity>" for entity in entities
                    ),
                )
            )
        except Exception as exc:
            raise _LlmCallException() from exc
        response = llms.wrangle_llm_response(response)
        selected_entity = {entity.name: entity for entity in entities}.get(response)
        if selected_entity is None:
            logger.warning(f"No entity selected for mention {mention!r}")
            logger.debug(f"LLM response: {response!r}")
            selected_entity = first_entity
        return selected_entity

    async def _classify_chunk(chunk_: list[tuple[str, pd.DataFrame]]) -> pd.DataFrame:
        result = await asyncio.gather(
            *[
                asyncio.create_task(
                    _classify_mention(
                        mention,
                        [
                            Entity(name=name, id=id_, score=score)
                            for name, id_, score in zip(
                                dataframe[ontology.OntologyColumns.name],
                                dataframe[ontology.OntologyColumns.id],
                                dataframe["score"],
                            )
                        ],
                    )
                )
                for mention, dataframe in chunk_
            ]
        )
        return pd.DataFrame(
            [(mention, *res) for (mention, _), res in zip(chunk_, result)],
            columns=[
                "mention",
                ontology.OntologyColumns.name,
                ontology.OntologyColumns.id,
                "score",
            ],
        )

    all_results: list[pd.DataFrame] = []
    for chunk in tqdm.tqdm(
        more_itertools.chunked(retrieved_results.groupby("mention"), n=chunk_size),
        total=math.ceil(len(retrieved_results) / chunk_size),
    ):
        chunk_result = asyncio.run(_classify_chunk(chunk))
        all_results.append(chunk_result)
    return pd.concat(all_results, ignore_index=True)


def _get_available_curie_prefixes(xrefs: pd.Series) -> list[str]:
    return sorted(
        xrefs.str.extract(r"(.+?):").iloc[:, 0].unique()
    )


def _get_curie_prefix_if_needed(series: pd.Series, available_prefixes: list[str], llm: BaseLanguageModel) -> str | None:
    prompt = """
    You are given a list of CURIE prefixes and a list of example identifiers (of a particular type). Your task is to select the appropriate prefix that needs to be added to make them CURIE compliant.
    If no prefix is needed, return an empty string.

    For example, if the provided identifiers are "ENSG00000274572", "ENSG00000274573", "ENSG00000274574", and the provided prefixes are "ENSEMBL", "NCBIGene" and "HGNC", then the output should be "ENSEMBL".
    This is because the identifiers are all in the ENSEMBL format, and the prefix "ENSEMBL" is the one that needs to be added.

    <prefixes>
    {prefixes}
    </prefixes>

    <example_identifiers name="{container_name}">
    {identifiers}
    </example_identifiers>

    Answer with the prefix and nothing else. If no prefix is needed, return an empty string.
    """

    response = llms.call_llm(
        llm=llm,
        prompt=llms.clean_prompt_formatting(prompt).format(
            container_name=series.name,
            prefixes="\n".join(
                [f"<prefix index={i}>{prefix}</prefix" for i, prefix in enumerate(available_prefixes, 1)]
            ),
            identifiers="\n".join(
                [
                    f"<example_identifier index={i}>{identifier}</example_identifier>"
                    for i, identifier in enumerate(series.dropna().unique()[:5], 1)
                ]
            ),
        )
    )
    response = response.strip()
    if response not in available_prefixes:
        logger.warning(f"No prefix selected for series {series.name!r}")
        logger.debug(f"LLM response: {response!r}")
        return
    return response


def _log_mappings(
    result: pd.DataFrame,
    series_name: str,
    entity_types: list[ontology.EntityType],
    mapping_type: logging.MappingType,
) -> None:
    log_session = logging.LoggingSession.get_session()
    etype_names = [etype.name for etype in entity_types]
    mappings = [
        utils.Mapping(
            id=row[ontology.OntologyColumns.id],
            mention=row["mention"],
            types=etype_names,
            name=row[ontology.OntologyColumns.name],
            score=row["score"],
        )
        for row in result.to_dict(orient="records")
    ]
    log_session.log_column_alignment_op(
        column_name=series_name,
        operation=logging.MappingOperation(
            type=mapping_type,
            mappings=mappings,
        )
    )


def _finalise_output(
    results: pd.DataFrame,
    series: pd.Series,
    entity_types: list[ontology.EntityType],
    mapping_type: logging.MappingType,
) -> pd.Series:
    _log_mappings(results, str(series.name), entity_types, mapping_type)
    index = series.index if series.index is not None else pd.RangeIndex(0, len(series))

    original_index_name = index.name
    temp_index_col_name: str | list[str] = "__original_index"
    if isinstance(index, pd.MultiIndex):
        original_index_name = index.names
        temp_index_col_name = [
            f"__original_index_{i}"
            for i in range(len(index.names))
        ]
    potentially_duplicated_ids = (
        results[[ontology.OntologyColumns.id, "mention"]]
        .merge(
            series.to_frame("mention")
            .reset_index(names=temp_index_col_name),
            on="mention",
            how="right"
        )
    )

    deduplicated_ids = (
        # TODO: this is a bit of a hack, but it works for now
        # we need to figure out a better way to do this, maybe using an llm call (with additional context)
        # the additional context is necessary because these types of duplications should only
        # really happen when we are mapping using xrefs, and the xrefs are not guaranteed to be unique
        potentially_duplicated_ids.groupby(temp_index_col_name)
        .first()
        .reset_index()
    )
    ids = (
        deduplicated_ids.set_index(temp_index_col_name)
        .rename_axis(original_index_name)
        [ontology.OntologyColumns.id]
        .astype("string[pyarrow]")
    )
    return ids.loc[index]


@dataclass
class EntityNormaliser:
    types: list[ontology.EntityType]
    store: ontology.OntologyStore
    dense_retriever: llms.DenseRetriever
    llm: BaseLanguageModel = field(default_factory=llms.get_llm)
    algorithm: NormalisationAlgorithm = NormalisationAlgorithm.RETRIEVAL

    def __deepcopy__(self, memo):
        # we need to override the deepcopy because we cannot pickle the store and other objects
        return self

    def normalise(self, series: pd.Series) -> pd.Series:
        if series.dropna().empty:
            logger.debug("Series is empty, returning original series")
            return series
        if is_free_text(series, self.llm):
            logger.debug("Series is free text, using retrieval")
            if self.algorithm == NormalisationAlgorithm.RETRIEVAL:
                logger.debug("Series is free text, using retrieval")
                results = _retrieve(series, self.dense_retriever, top_k=1)
                return _finalise_output(results, series, self.types, mapping_type=logging.MappingType.FREE_TEXT)
            elif self.algorithm == NormalisationAlgorithm.RETRIEVAL_AND_CLASSIFICATION:
                logger.debug("Series is free text, using retrieval and classification")
                results = _retrieve(series, self.dense_retriever, top_k=10)
                results = _classify(results, self.llm)
                return _finalise_output(results, series, self.types, mapping_type=logging.MappingType.FREE_TEXT)
            else:
                raise NotImplementedError(
                    f"Algorithm {self.algorithm} not implemented for free text"
                )
        logger.debug("Series is not free text, using xref mapping")
        xref_mapping = self.store.load_xref_mapping(
            entity_types=self.types,
            additional_columns=[
                ontology.OntologyColumns.name,
            ]
        )
        mapping = (
            series.to_frame("mention")
            .assign(xrefs=lambda df: df["mention"])
        )
        available_prefixes = _get_available_curie_prefixes(xref_mapping[self.store.columns.xrefs])
        if available_prefixes:
            logger.debug(f"Available prefixes for series are {available_prefixes}")
            prefix = _get_curie_prefix_if_needed(
                series, available_prefixes, self.llm
            )
            if prefix is not None:
                logger.debug(f"Adding prefix {prefix} to series")
                mapping["xrefs"] = f"{prefix}:" + series.astype(str)
            else:
                logger.debug("No prefix selected for series")
        out = _finalise_output(
            xref_mapping.merge(mapping, on="xrefs")
            .drop_duplicates()
            .drop(columns=["xrefs"])
            .assign(score=1.0),
            series,
            self.types,
            mapping_type=logging.MappingType.XREF,
        )
        return out

    @classmethod
    def from_entity_types(
        cls,
        entity_types: list[ontology.EntityType],
        store: ontology.OntologyStore | None = None,
        llm: BaseLanguageModel | None = None,
        algorithm: NormalisationAlgorithm = NormalisationAlgorithm.RETRIEVAL,
    ) -> "EntityNormaliser":
        store = store or ontology.OntologyStore.default()
        return cls(
            types=entity_types,
            store=store,
            dense_retriever=load_dense_retriever_from_entity_types(entity_types, store),
            llm=llm or llms.get_llm(),
            algorithm=algorithm,
        )
