import functools
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Literal, Protocol, TypeVar, runtime_checkable

import networkx as nx
import pandas as pd
import pandera as pa
import pandasql
import pydantic
import sqlglot
from langchain_core.language_models import BaseLanguageModel
from loguru import logger

from bio_data_harmoniser.core import data_types as dt
from bio_data_harmoniser.core import llms, logging, ontology, utils
from bio_data_harmoniser.core.logging import ColumnInferenceType
from bio_data_harmoniser.core.extraction import rag

R = TypeVar("R")


@dataclass
class ColumnInferenceSession:
    dataframe: pd.DataFrame
    schema: pa.DataFrameSchema
    llm: BaseLanguageModel
    context: list[str]
    file_path: str


def _get_scientific_notation_substr(s: str) -> str | None:
    match = re.search(r"(\d+(?:\.\d+)?[eE][+\-]?\d+)", s)
    if match is None:
        return
    return match.group(1)


def log_input_output(function: Callable[[str], R]) -> Callable[[str], R]:
    @functools.wraps(function)
    def wrapper(val: str) -> R:
        logger.info(f"Calling function {function.__name__}")
        result = function(val)
        logger.info(f"{val!r} -> {result!r}")
        return result
    return wrapper


def string_to_int(s: str) -> int:
    scientific_notation = _get_scientific_notation_substr(s)
    if scientific_notation is not None:
        return int(float(scientific_notation))
    s = re.sub(r"[^0-9\-]", "", s)
    return int(s)


def _try_to_float(s: str) -> float:
    try:
        return float(s)
    except ValueError:
        return float("nan")


def string_to_float(s: str) -> float:
    scientific_notation = _get_scientific_notation_substr(s)
    if scientific_notation is not None:
        return _try_to_float(scientific_notation)
    s = re.sub(r"[^0-9\-]", "", s)
    return _try_to_float(s)


InferenceFunction = Callable[[ColumnInferenceSession], pd.Series | tuple[pd.Series, Any]]


class ColumnInference(pydantic.BaseModel):
    condition: Callable[[ColumnInferenceSession], bool]
    inference: InferenceFunction
    type: ColumnInferenceType = ColumnInferenceType.DERIVED
    # TODO: clean this up
    # for now, we will add the upstream columns here, to be able to sort the inference order
    upstream_columns: list[str] = pydantic.Field(default_factory=list)

    class Config:
        arbitrary_types_allowed = True

    @classmethod
    def when_has_columns(
        cls,
        column_names: list[str],
        inference: InferenceFunction,
        type: ColumnInferenceType = ColumnInferenceType.DERIVED,
    ) -> "ColumnInference":
        return cls(
            condition=lambda session: set(column_names).issubset(
                session.dataframe.columns
            ),
            inference=inference,
            type=type,
            upstream_columns=column_names,
        )

    @classmethod
    def when_has_context(
        cls,
        inference: InferenceFunction,
        type: ColumnInferenceType = ColumnInferenceType.EXTRACTED,
    ) -> "ColumnInference":
        return cls(
            condition=lambda session: bool(session.context),
            inference=inference,
            type=type,
        )

    @classmethod
    def extract_from_context(
        cls,
        query: str,
        rag_post_processing: Callable[[str], Any] | Literal["int", "float"] = lambda x: x,
    ) -> "ColumnInference":
        if isinstance(rag_post_processing, str):
            rag_post_processing = {
                "int": string_to_int,
                "float": string_to_float,
            }[rag_post_processing]
        rag_post_processing = log_input_output(rag_post_processing)

        def inference(session: ColumnInferenceSession) -> tuple[pd.Series, rag.Response]:
            extractor = rag.RetrievalAugmentedGenerator.from_texts(
                session.context, llm=session.llm
            )
            response = extractor.query(query)
            return (
                session.dataframe.assign(__inferred=rag_post_processing(response.answer))["__inferred"],
                response
            )

        return cls.when_has_context(inference=inference, type=ColumnInferenceType.EXTRACTED)

    @classmethod
    def sql(cls, query: str, output_column: str = "__inferred", table_name: str = "df") -> "ColumnInference":
        parsed_queries = sqlglot.parse(query)
        if len(parsed_queries) != 1:
            raise ValueError("Query must contain exactly one SQL query")
        parsed_query = parsed_queries[0]
        column_names = {
            column.name for column in parsed_query.find_all(sqlglot.exp.Column)
        }
        out_col = {c.alias for c in parsed_query.find_all(sqlglot.exp.Alias)}
        if output_column not in out_col:
            raise ValueError(
                f"Output column {output_column} is not in the query"
            )
        table_names = {
            table.name for table in parsed_query.find_all(sqlglot.exp.Table)
        }
        if table_name not in table_names:
            raise ValueError(
                f"Table {table_name} is not in the query"
            )
        return cls.when_has_columns(
            column_names=sorted(column_names),
            inference=lambda session: pandasql.sqldf(
                query,
                {table_name: session.dataframe}
            )[output_column],
            type=ColumnInferenceType.DERIVED,
        )

    def execute(self, session: ColumnInferenceSession) -> tuple[pd.Series, Any]:
        result = self.inference(session)
        if isinstance(result, pd.Series):
            return result, None
        return result


class ColumnMetadata(pydantic.BaseModel):
    aliases: list[str] = pydantic.Field(default_factory=list)
    column_inferences: list[ColumnInference] = pydantic.Field(default_factory=list)

    class Config:
        arbitrary_types_allowed = True

    @classmethod
    def from_column(cls, column: pa.Column) -> "ColumnMetadata":
        return cls.parse_obj(column.metadata or {})


@runtime_checkable
class Schema(Protocol):
    def create(self) -> pa.DataFrameSchema: ...


def identify_target_schema(
    dataframe: pd.DataFrame,
    schemas: list[pa.DataFrameSchema],
    llm: BaseLanguageModel,
) -> pa.DataFrameSchema | None:
    # when we get a new dataset, we need to identify the data type to match it to a target schema
    # we need to assume that we only have the dataframe available as context
    prompt = """
    Your task is to identify the target schema of the given dataset. You are provided with the head of a dataset and a list of possible schemas. You need to select the most appropriate schema for the dataset.
    
    Here is the list of available schemas:
    
    <schemas>
    {schemas}
    </schemas>
    
    Here is the head of the dataset:
    
    <data_head>
    {data_head}
    </data_head>
    
    Please select the most appropriate target schema from the list above. Provide just the name of the target schema and nothing else. If you are unsure, you can select "Other".
    """

    schema_str = "\n".join(
        [
            f"<schema index={i}>"
            f"\n<name>{schema.name}</name>"
            f"\n<description>{schema.description}</description>"
            f"\n</schema>"
            for i, schema in enumerate(schemas, 1)
        ]
    )
    # TODO: we need to potentially compress the dataframe to avoid blowing the context window of the LLM
    data_head = dataframe.head(3).to_csv(index=False, sep="\t")
    response = llm.predict(
        llms.clean_prompt_formatting(prompt).format(
            schemas=schema_str, data_head=data_head
        )
    )
    return {schema.name.lower(): schema for schema in schemas}.get(
        response.strip().lower()
    )


def get_column_description(column: pa.Column) -> str:
    description = column.description
    if not description:
        dtype = column.dtype
        if isinstance(dtype, dt.EntityType):
            description = "\n".join(
                [
                    ontology.ENTITY_TYPE_DESCRIPTIONS[e_type]
                    for e_type in dtype.types
                    if e_type in ontology.ENTITY_TYPE_DESCRIPTIONS
                ]
            )
    return description or ""


def get_column_name(
    dataframe: pd.DataFrame,
    column: pa.Column,
    llm: BaseLanguageModel,
) -> str | None:
    dataframe = dataframe.head(3).copy()
    original_columns = dataframe.columns
    dataframe.columns = [utils.to_snake_case(x) for x in dataframe.columns]
    column_map = dict(zip(dataframe.columns, original_columns))

    # TODO: we need to tune this prompt to make sure we don't get false positives
    prompt_template = """
    Given a dataframe and a target column, return the name of the column in the dataframe that matches the target column. If there is no match or you are unsure, answer with an empty string.
    Note that the target column may sometimes request a column with IDs, but the dataframe only has a column with free-text values (e.g. the target column is `tissue_id` but the dataframe only has `tissue`). In this case, you should return the name of the column that contains the free-text values (e.g. `tissue`).

    Here are the column names in the dataframe:
    
    <columns>
    {columns}
    </columns>
    
    Here are the first few rows of the dataframe, as XML:
    {df_xml}
    
    Here is the target column:
    
    <target_column>
    {target_column}
    </target_column>
    
    Provide just the name of the column that matches the target column and nothing else. If there is no match or you are unsure, answer with an empty string.
    Remember that if the target column requests IDs, but the dataframe only has free-text values, you should return the name of the column that contains the free-text values.
    """
    df_xml: str = re.sub(" +", " ", dataframe.iloc[:3].to_xml()).split("\n", 1)[1]
    desc = get_column_description(column)
    prompt = llms.clean_prompt_formatting(prompt_template).format(
        columns="\n".join(map("<column>{}</column>".format, dataframe.columns)),
        df_xml=df_xml,
        target_column=f"<name>{column.name}</name>\n<description>{desc}</description>",
    )
    llm_response: str = llm.predict(prompt)
    return column_map.get(llm_response)


def align_dataframe_to_schema(
    dataframe: pd.DataFrame,
    schema: pa.DataFrameSchema,
    llm: BaseLanguageModel | None = None,
    context: list[str] | None = None,
    file_path: str | None = None,
) -> pd.DataFrame:
    logger.info(f"Aligning dataframe to schema ({schema.name})")
    llm = llm or llms.get_llm()
    context = context or []

    log_session = logging.LoggingSession.get_session()
    column_name_mapping: dict[str, str] = {}
    missing_columns: list[pa.Column] = []

    logger.info("Mapping column names to required schema and identifying missing columns")
    column: pa.Column
    for column in schema.columns.values():
        logger.info(f"Looking for column {column.name!r}")
        metadata = ColumnMetadata.from_column(column)
        for alias in metadata.aliases:
            if alias in dataframe.columns:
                logger.info(f"Found column {alias!r} in dataframe for column {column.name!r}")
                column_name_mapping[alias] = column.name
                log_session.log_column_alignment_op(
                    column_name=column.name,
                    operation=logging.RenameOperation(
                        original_name=alias,
                        new_name=column.name,
                    )
                )
                break
        else:
            logger.info(f"No column alias found for column {column.name!r}")
            logger.info(f"Identifying column name for column {column.name!r} using LLM")
            column_name = get_column_name(dataframe, column, llm)
            if column_name is not None:
                logger.info(f"Found column name {column_name!r} for column {column.name!r}")
                column_name_mapping[column_name] = column.name
                log_session.log_column_alignment_op(
                    column_name=column.name,
                    operation=logging.RenameOperation(
                        original_name=column_name,
                        new_name=column.name,
                    )
                )
                continue
            logger.info(f"No column name found for column {column.name!r}")
            missing_columns.append(column)
    dataframe = dataframe.rename(columns=column_name_mapping)
    logger.info(f"Inferring {len(missing_columns)} missing columns")
    for column in _sort_columns_based_on_dependencies(schema, missing_columns, context_available=bool(context)):
        inference_session = ColumnInferenceSession(
            dataframe=dataframe,
            schema=schema,
            llm=llm,
            context=context,
            file_path=file_path or "",
        )
        metadata = ColumnMetadata.from_column(column)
        for inference in metadata.column_inferences:
            if inference.condition(inference_session):
                logger.info(f"Matched column inference for column {column.name!r} of type {inference.type}")
                dataframe[column.name], inference_data = inference.execute(inference_session)
                log_session.log_column_alignment_op(
                    column_name=column.name,
                    operation=logging.InferenceOperation(
                        type=inference.type,
                        data=inference_data,
                    )
                )
                break
        else:
            logger.info(f"No column inference found for column {column.name!r}")
            if column.required and not column.nullable and column.default is None:
                logger.error(f"Column {column.name!r} is required but has no default value")
                raise ValueError(
                    f"Column {column.name} is required but has no default value"
                )
            logger.info(f"Setting column {column.name!r} to default value {column.default!r}")
            dataframe[column.name] = column.default
            log_session.log_column_alignment_op(
                column_name=column.name,
                operation=logging.SetValueOperation(
                    value=column.default,
                )
            )
    return schema.validate(dataframe)


def _sort_columns_based_on_dependencies(
    schema: pa.DataFrameSchema,
    missing_columns: list[pa.Column],
    context_available: bool,
) -> list[pa.Column]:
    logger.info("Sorting column inferences based on dependencies")
    available_node = "<AVAILABLE>"
    graph = nx.DiGraph()
    graph.add_node(available_node)
    missing_column_names = {column.name for column in missing_columns}
    for column in schema.columns.values():
        graph.add_node(column.name)
        metadata = ColumnMetadata.from_column(column)
        for i, inference in enumerate(metadata.column_inferences):
            if inference.type == ColumnInferenceType.EXTRACTED and context_available:
                graph.add_edge(column.name, available_node, priority=i)
                continue
            for dependency in inference.upstream_columns:
                if dependency not in missing_column_names:
                    graph.add_edge(column.name, available_node, priority=i)
                else:
                    graph.add_edge(column.name, dependency, priority=i)

    def get_score(node):
        try:
            return nx.shortest_path_length(graph, node, available_node)
        except nx.NetworkXNoPath:
            return float("inf")

    scored_columns = {
        column_name: get_score(column_name)
        for column_name in missing_column_names
    }
    logger.info(f"Sorting columns based on score: {scored_columns}")
    return [
        schema.columns[column_name]
        for column_name in sorted(scored_columns, key=scored_columns.get)
    ]


def schema(
    name: str,
    description: str,
    columns: dict[str, pa.Column],
) -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        columns,
        name=name,
        description=description,
        strict="filter",
        coerce=True,
    )


def dataset_id_column() -> pa.Column:
    return pa.Column(
        str,
        metadata=ColumnMetadata(
            column_inferences=[
                ColumnInference(
                    condition=lambda _: True,
                    inference=lambda session: session.dataframe.assign(
                        __inferred=Path(session.file_path).stem
                    )["__inferred"],
                ),
            ]
        ).dict(),
    )
