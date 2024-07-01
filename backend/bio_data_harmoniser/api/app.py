import enum
import json
import operator
import re
import shutil
import uuid
from pathlib import Path
from typing import Generic, Type, TypeVar

import deltalake
import pandas as pd
import pandera as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pydantic
from fastapi import FastAPI, HTTPException, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from pandera.engines import pandas_engine

from bio_data_harmoniser.api.airflow import dags, interface
from bio_data_harmoniser.core import data_types, logging, ontology, schemas, settings, utils
from bio_data_harmoniser.core.normalisation import entity as entity_normalisation
from bio_data_harmoniser.core.schemas import base as schema_base
from bio_data_harmoniser.ingestion import pubmed

T = TypeVar("T")

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

airflow = interface.AirflowInterface.from_settings(
    settings.airflow,
    exception_cls=HTTPException,
)


class Status(enum.Enum):
    OK = "OK"
    ERROR = "ERROR"


class Message(pydantic.BaseModel):
    message: str
    status: Status


@app.get("/")
async def root() -> Message:
    return Message(message="bio-data-harmoniser API", status=Status.OK)


def _get_mappings(run_id: str) -> list[utils.Mapping]:
    path = dags.get_mappings_path(
        dags.TaskInstanceInfo(dag_id=dags.DATA_EXTRACTION_DAG_ID, run_id=run_id)
    )
    if not Path(path).exists():
        return []
    return [
        utils.Mapping(**{"mapping_id": ix, **row})
        for ix, row in enumerate(pd.read_json(path, orient="records").to_dict(orient="records"))
    ]


@app.get("/mappings/{run_id}")
async def get_mappings(run_id: str) -> list[utils.Mapping]:
    logger.info(f"Getting mappings for run {run_id}")
    out = _get_mappings(run_id)
    if not out:
        logger.warning(f"No mappings found for run {run_id}")
    logger.info(f"Returning {len(out)} mappings")
    return out


class Entity(pydantic.BaseModel):
    id: str
    name: str
    iri: str


class UpdateMappingParams(pydantic.BaseModel):
    entity: Entity
    mapping: utils.Mapping


@app.post("/mappings/{run_id}")
async def update_mapping(run_id: str, params: UpdateMappingParams) -> Message:
    # TODO: we should really be using a database for this
    # not having ACID is bound to bite us in the future
    logger.info(f"Updating mapping {params.mapping.mapping_id}")
    mapping = params.mapping
    entity = params.entity
    if mapping.mapping_id is None:
        raise HTTPException(status_code=400, detail="Mapping ID is required")
    mappings = _get_mappings(run_id)
    if not mappings:
        raise HTTPException(status_code=404, detail="No mappings found")
    for ix, m in enumerate(mappings):
        if m.mapping_id == mapping.mapping_id:
            logger.info(f"Updating mapping {m.mapping_id} ({mapping.id} -> {entity.id})")
            mappings[ix] = mapping.copy(
                update={
                    "id": entity.id,
                    "name": entity.name,
                }
            )
            break
    else:
        raise HTTPException(status_code=404, detail="Mapping not found")
    logger.info(f"Updating {len(mappings)} mappings")
    (
        pd.DataFrame([mapping.dict() for mapping in mappings])
        .to_json(
            dags.get_mappings_path(
                dags.TaskInstanceInfo(
                    dag_id=dags.DATA_EXTRACTION_DAG_ID,
                    run_id=run_id
                ),
            ),
            orient="records",
        )
    )
    return Message(message="Mappings updated", status=Status.OK)


class GetEntitiesParams(pydantic.BaseModel):
    types: list[ontology.EntityType]
    query: str | None = None
    limit: int | None = None


@app.post("/entities")
async def get_entities(
    params: GetEntitiesParams,
) -> list[Entity]:
    logger.info(f"Getting entities for types {params.types} and query {params.query}")
    store = ontology.OntologyStore.default()
    filter_expression = None
    if params.query is not None:
        filter_expression = pc.match_substring(
            ds.field(ontology.OntologyColumns.name),
            params.query,
            ignore_case=True,
        )
    entities = store.load(
        entity_types=params.types,
        columns=[
            ontology.OntologyColumns.id,
            ontology.OntologyColumns.name,
            ontology.OntologyColumns.iri,
        ],
        filter_expression=filter_expression,
    )
    entities = entities.head(n=params.limit or len(entities))
    logger.info(f"Returning {len(entities)} entities")
    return [
        Entity(
            id=row[ontology.OntologyColumns.id],
            name=row[ontology.OntologyColumns.name],
            iri=row[ontology.OntologyColumns.iri],
        )
        for row in entities.to_dict(orient="records")
    ]


class PaperParams(pydantic.BaseModel):
    url: str


def clean_string(string: str) -> str:
    return re.sub(r"\s+", " ", string).strip()


@app.post("/paper-ingestion/metadata")
async def create_paper_ingestion_metadata(params: PaperParams) -> dags.DataExtractionDagParams:
    # TODO: we should rename this function and generalise the concept of inferring ingestion metadata
    if not pubmed.is_pubmed_url(params.url):
        raise HTTPException(status_code=400, detail="Invalid URL")
    pmc_id = pubmed.get_pubmed_id(params.url)
    paper = pubmed.fetch_pmc_article(pmc_id)
    return dags.DataExtractionDagParams(
        user_id="",
        name=clean_string(paper.title),
        description=clean_string(paper.abstract),
        url=params.url,
    )


class FileUploadResponse(pydantic.BaseModel):
    url: str


@app.post("/ingestion/file-upload")
async def upload_file(file: UploadFile = File(...)) -> FileUploadResponse:
    dest = (Path(settings.fastapi.upload_dir) / uuid.uuid4().hex / file.filename).resolve()
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfileobj(file.file, dest.open("wb"))
    return FileUploadResponse(url=f"file://{dest}")


class IngestionStatus(enum.Enum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    UNKNOWN = "unknown"

    @classmethod
    def from_state(cls, state: str) -> "IngestionStatus":
        if state == "success":
            return IngestionStatus.SUCCESS
        elif state == "failed":
            return IngestionStatus.FAILED
        elif state == "running":
            return IngestionStatus.RUNNING
        return IngestionStatus.UNKNOWN


class Ingestion(pydantic.BaseModel):
    id: str
    params: dags.DataExtractionDagParams
    status: IngestionStatus
    execution_date: int


@app.get("/ingestions/{user_id}")
async def get_ingestions(user_id: str) -> list[Ingestion]:
    runs = airflow.get_dag_runs(dags.DATA_EXTRACTION_DAG_ID)
    if not runs:
        return []
    df: pd.DataFrame = (
        pd.DataFrame(runs)
        .assign(
            user_id=lambda df: df["conf"].apply(operator.itemgetter("user_id")),
            name=lambda df: df["conf"].apply(operator.itemgetter("name")),
            # this mirrors the behaviour of `Date.parse` in JavaScript
            execution_date=lambda df: pd.to_datetime(df["execution_date"], format="mixed").astype(int) // 10 ** 6,
        )
    )
    df = (
        df.loc[df["user_id"] == user_id]
        .groupby("name")
        .apply(lambda df: df.nlargest(1, "execution_date"))
        .assign(status=lambda df: df["state"].apply(IngestionStatus.from_state))
        .rename(columns={"conf": "params", "dag_run_id": "id"})
    )
    return [
        Ingestion.parse_obj(row)
        for row in df.to_dict("records")
    ]


@app.get("/catalog/{run_id}")
async def get_catalog(run_id: str):
    logger.info(f"Getting catalog for run {run_id}")
    table_paths = dags.get_delta_table_paths(
        dags.TaskInstanceInfo(dag_id=dags.DATA_EXTRACTION_DAG_ID, run_id=run_id)
    )
    schemas = [
        {"name": path.name, "schema": json.loads(deltalake.DeltaTable(str(path)).schema().to_json())}
        for path in table_paths
    ]
    return schemas


@app.get("/sql/{query_id}")
def get_sql_results(query_id: str):
    logger.info(f"Getting results for {query_id}")
    path = dags.get_sql_query_results_path(
        dags.TaskInstanceInfo(dag_id=dags.SQL_QUERY_DAG_ID, run_id=query_id),
    )
    if not Path(path).exists():
        raise HTTPException(status_code=404, detail="No results found")
    return json.loads(pd.read_parquet(path).head(n=1000).to_json(orient="split"))


class DataTypeParamOption(pydantic.BaseModel, Generic[T]):
    name: str
    value: T
    description: str | None = None


class DataTypeParam(pydantic.BaseModel, Generic[T]):
    key: str
    name: str
    options: list[DataTypeParamOption[T]]
    allow_multiple: bool = False
    options_ordered: bool = False
    default: str | None = None
    choice: DataTypeParamOption[T] | None = None


class DataType(pydantic.BaseModel):
    key: str
    name: str
    description: str
    parameters: list[DataTypeParam] | None = None

    @classmethod
    def from_type(cls, type_: type, name: str, description: str, parameters: list[DataTypeParam] | None = None) -> "DataType":
        return cls(
            key=type_.__name__,
            name=name,
            description=description,
            parameters=parameters,
        )


DATA_TYPES = [
    DataType.from_type(
        pandas_engine.STRING,
        name="string",
        description="A string",
    ),
    DataType.from_type(
        pandas_engine.INT64,
        name="int64",
        description="An 64-bit signed integer",
    ),
    DataType.from_type(
        pandas_engine.INT32,
        name="int32",
        description="A 32-bit signed integer",
    ),
    DataType.from_type(
        pandas_engine.INT16,
        name="int16",
        description="A 16-bit signed integer",
    ),
    DataType.from_type(
        pandas_engine.INT8,
        name="int8",
        description="An 8-bit signed integer",
    ),
    DataType.from_type(
        pandas_engine.UINT64,
        name="uint64",
        description="An 64-bit unsigned integer",
    ),
    DataType.from_type(
        pandas_engine.UINT32,
        name="uint32",
        description="A 32-bit unsigned integer",
    ),
    DataType.from_type(
        pandas_engine.UINT16,
        name="uint16",
        description="A 16-bit unsigned integer",
    ),
    DataType.from_type(
        pandas_engine.UINT8,
        name="uint8",
        description="An 8-bit unsigned integer",
    ),
    DataType.from_type(
        pandas_engine.FLOAT64,
        name="float64",
        description="A 64-bit floating point number",
    ),
    DataType.from_type(
        pandas_engine.FLOAT32,
        name="float32",
        description="A 32-bit floating point number",
    ),
    DataType.from_type(
        pandas_engine.Decimal,
        name="Decimal",
        description="A decimal number",
    ),
    DataType.from_type(
        pandas_engine.Date,
        name="Date",
        description="A date",
    ),
    DataType.from_type(
        pandas_engine.DateTime,
        name="Date-time",
        description="A datetime",
    ),
    DataType.from_type(
        pandas_engine.BOOL,
        name="boolean",
        description="A boolean value",
    ),
    DataType.from_type(
        data_types.EntityType,
        name="Entity",
        description="An entity",
        parameters=[
            DataTypeParam[ontology.EntityType](
                key="entity_types",
                name="Entity types",
                options=[
                    DataTypeParamOption[ontology.EntityType](
                        name=etype.value,
                        value=etype,
                        description=ontology.ENTITY_TYPE_DESCRIPTIONS[etype],
                    ) for etype in ontology.EntityType
                ],
                allow_multiple=True,
            ),
            DataTypeParam[entity_normalisation.NormalisationAlgorithm](
                key="normalisation_algorithm",
                name="Normalisation algorithm",
                options=[
                    DataTypeParamOption[entity_normalisation.NormalisationAlgorithm](
                        name="Fast",
                        value=entity_normalisation.NormalisationAlgorithm.RETRIEVAL,
                        description="This is the fastest normalisation algorithm, but produces the least accurate results.",
                    ),
                    DataTypeParamOption[entity_normalisation.NormalisationAlgorithm](
                        name="Medium",
                        value=entity_normalisation.NormalisationAlgorithm.RETRIEVAL_AND_CLASSIFICATION,
                        description="This algorithm is slower, but produces more accurate results. It is recommended for most use cases.",
                    ),
                    DataTypeParamOption[entity_normalisation.NormalisationAlgorithm](
                        name="Slow",
                        value=entity_normalisation.NormalisationAlgorithm.AGENTIC_RETRIEVAL_AND_CLASSIFICATION,
                        description="This algorithm is the slowest, but produces the most accurate results. It is recommended for especially tricky use cases.",
                    )
                ],
                options_ordered=True,
                default="Medium",
            )
        ]
    ),
    DataType.from_type(
        data_types.AminoAcidSequenceType,
        name="Amino acid sequence",
        description="An amino acid sequence",
    ),
    DataType.from_type(
        data_types.NucleotideSequenceType,
        name="Nucleotide sequence",
        description="A nucleotide sequence",
    ),
    DataType.from_type(
        data_types.SMILESType,
        name="SMILES",
        description="A SMILES string",
    ),
]
DATA_TYPES_LOOKUP: dict[str, DataType] = {
    datatype.key: datatype for datatype in DATA_TYPES
}


@app.get("/data-types")
def get_data_types() -> list[DataType]:
    # TODO: add user defined data types
    return DATA_TYPES


@app.get("/data-types/{data_type}")
def get_data_type(data_type: str) -> DataType:
    out = DATA_TYPES_LOOKUP.get(data_type)
    if out is None:
        raise HTTPException(status_code=404, detail="Data type not found")
    return out


@app.get("/schemas")
def get_schemas():
    with utils.disable_load_state():
        user_defined_schemas = dags.get_user_defined_schemas()
        logger.info(f"Returning {len(user_defined_schemas)} user-defined schemas")
        default_schemas = [
            scheme.create() for scheme in schemas.schemas
        ]
        logger.info(f"Returning {len(default_schemas)} default schemas")
    return [
        json.loads(schema.to_json()) for schema in user_defined_schemas + default_schemas
    ]


class Column(pydantic.BaseModel):
    name: str
    description: str
    data_type: DataType


class CreateSchemaParams(pydantic.BaseModel):
    name: str
    description: str
    columns: list[Column]


def _get_param(data_type: DataType, key: str, param_value_type: Type[T]) -> DataTypeParam[T] | None:
    out = None
    for param in (data_type.parameters or []):
        if param.key == key:
            out = DataTypeParam[param_value_type].parse_obj(param.dict())
            break
    return out


def _get_data_type(data_type: DataType):
    if data_type.key == data_types.EntityType.__name__:
        etypes_param = _get_param(data_type, "entity_types", ontology.EntityType)
        if etypes_param is None:
            raise HTTPException(status_code=429, detail="Entity type not found")
        if etypes_param.choice is None:
            raise HTTPException(status_code=429, detail="Entity type is not defined")
        etypes = etypes_param.choice.value
        algo = entity_normalisation.NormalisationAlgorithm.RETRIEVAL_AND_CLASSIFICATION
        algo_param = (
            _get_param(data_type, "normalisation_algorithm", entity_normalisation.NormalisationAlgorithm)
        )
        if algo_param is not None and algo_param.choice is not None:
            algo = algo_param.choice.value
        return data_types.EntityType.from_entity_types(
            entity_types=[etypes],
            normalisation_algorithm=algo,
        )
    dtype = getattr(data_types, data_type.key, None)
    if dtype is not None:
        return dtype
    dtype = getattr(pandas_engine, data_type.key, None)
    if dtype is None:
        raise HTTPException(status_code=400, detail="Data type not found")
    return dtype


def _register_schema(schema: pa.DataFrameSchema) -> None:
    conf = dags.SchemaDefinitionParams.from_schema(schema)
    airflow.create_dag_run(dags.SCHEMA_DEFINITION_DAG_ID, conf.dict(by_alias=True))


@app.post("/schemas")
def create_schema(params: CreateSchemaParams):
    with utils.disable_load_state():
        schema = schema_base.schema(
            name=params.name,
            description=params.description,
            columns={
                col.name: pa.Column(
                    _get_data_type(col.data_type),
                    description=col.description,
                    nullable=True,
                )
                for col in params.columns
            }
        )
        _register_schema(schema)
    return Message(message="Schema created", status=Status.OK)


class DataExtractionDag(pydantic.BaseModel):
    nodes: list[logging.LoggedNode]


@app.get("/dag/{run_id}")
def get_dag(run_id: str) -> DataExtractionDag:
    path = dags.folder_path_from_task_instance(
        dags.TaskInstanceInfo(dag_id=dags.DATA_EXTRACTION_DAG_ID, run_id=run_id),
        "logs",
    )
    return DataExtractionDag(
        nodes=[
            logging.LoggedNode.parse_raw(file.read_text())
            for file in Path(path).glob("*.json")
        ]
    )


class LlmSecrets(pydantic.BaseModel):
    llm_api_key: str
    # TODO: add other secrets (e.g. organization ID)


@app.post("/secrets/llm")
def set_llm_secrets(secrets: LlmSecrets) -> Message:
    airflow.set_variable(
        name=settings.LLM_API_KEY_NAME,
        value=secrets.llm_api_key,
    )
    return Message(message="Secrets set", status=Status.OK)


@app.get("/secrets/llm")
def get_llm_secrets() -> LlmSecrets:
    value = airflow.get_variable(settings.LLM_API_KEY_NAME)
    if value is None:
        raise HTTPException(status_code=404, detail="LLM API key not found")
    return LlmSecrets(llm_api_key=value)
