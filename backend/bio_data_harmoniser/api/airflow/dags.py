import itertools
import os
import tarfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

import airflow
import pandas as pd
import pendulum
import pydantic
from airflow.decorators import task, task_group
from airflow.exceptions import AirflowSkipException
from airflow.models import Param, TaskInstance, DagRun
from airflow.utils.trigger_rule import TriggerRule
if TYPE_CHECKING:
    import pandera as pa

from bio_data_harmoniser.core import settings
from bio_data_harmoniser.api.airflow import interface

airflow_interface = interface.AirflowInterface.from_settings(
    settings.airflow,
    exception_cls=Exception,
)


@dataclass
class TaskInstanceInfo:
    dag_id: str
    run_id: str

    @classmethod
    def from_task_instance(cls, task_instance: TaskInstance) -> "TaskInstanceInfo":
        return cls(
            dag_id=task_instance.dag_id,
            run_id=task_instance.run_id,
        )


def folder_path_from_task_instance(
    task_instance: TaskInstanceInfo | TaskInstance, *additional_parts: str
) -> str:
    if isinstance(task_instance, TaskInstance):
        task_instance = TaskInstanceInfo.from_task_instance(task_instance)
    return cast(
        str,
        os.path.join(
            settings.airflow.output_dir,
            f"dag_id={task_instance.dag_id}",
            f"run_id={task_instance.run_id}",
            *additional_parts,
        ),
    )


def get_mappings_path(task_instance: TaskInstanceInfo | TaskInstance) -> str:
    return folder_path_from_task_instance(task_instance, "mappings.json")


def get_processed_data_path(task_instance: TaskInstanceInfo | TaskInstance) -> str:
    return folder_path_from_task_instance(task_instance, "processed_data")


def get_subdirs_in_processed_data_path(task_instance: TaskInstanceInfo | TaskInstance) -> list[Path]:
    from loguru import logger

    subdirs = []
    directory = Path(get_processed_data_path(task_instance))
    if not directory.exists():
        logger.info(f"No processed data found in {directory}")
        return subdirs
    for path in directory.iterdir():
        if path.is_dir():
            subdirs.append(path)
    return subdirs


def get_delta_table_paths(task_instance: TaskInstanceInfo | TaskInstance) -> list[Path]:
    return [
        path for path in get_subdirs_in_processed_data_path(task_instance)
        if (path / "_delta_log").exists()
    ]


SCHEMA_DEFINITION_DAG_ID = "schema_definition"


def get_user_defined_schemas() -> list["pa.DataFrameSchema"]:
    import pandera as pa

    dag_runs = airflow_interface.get_dag_runs(
        SCHEMA_DEFINITION_DAG_ID,
        params={
            "state": ["success"],
            "order_by": "-execution_date",
        },
    )
    seen = set()
    schemas_ = []
    for dag_run in dag_runs:
        record = SchemaDefinitionParams.parse_obj(dag_run["conf"])
        schema_ = pa.DataFrameSchema.from_json(record.schema_)
        if schema_.name in seen:
            continue
        seen.add(schema_.name)
        schemas_.append(schema_)
    return schemas_


class DataExtractionDagParams(pydantic.BaseModel):
    user_id: str = pydantic.Field(
        "test_user", description="The user ID to use for the data extraction"
    )
    name: str = pydantic.Field(
        "", description="The name of the data ingestion pipeline."
    )
    description: str = pydantic.Field(
        "", description="The description of the data ingestion pipeline."
    )
    url: str = pydantic.Field(
        "https://example.com/data.zip", description="The URL/URI of the data to extract."
    )


def pydantic_to_airflow_params(params: pydantic.BaseModel) -> dict[str, Param]:
    schema = params.schema()
    return {
        key: Param(**value) for key, value in schema["properties"].items()
    }


DATA_EXTRACTION_DAG_ID = "data_extraction"
with airflow.DAG(
    dag_id=DATA_EXTRACTION_DAG_ID,
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
    is_paused_upon_creation=False,
    params=pydantic_to_airflow_params(DataExtractionDagParams()),
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(seconds=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": pendulum.duration(minutes=5),
    },
) as data_extraction:

    @task
    def extract_urls(**context) -> list[str]:
        # thoughts
        # - each URL can be one of:
        #   - a URI to a local file
        #   - a URL to a remote file
        #   - a URL to a webpage
        # - the first two cases are easy and are the base cases
        # - the third case is a bit more complicated
        #   - there is a recursive relationship here, as we need to extract the URLs from the webpage
        #   - for each extracted URL, we then need to do the same thing
        # ... basically, we need to build a webscraper
        # we also need to maintain the context accrued across webpages for the metadata extraction later on
        from loguru import logger

        from bio_data_harmoniser.core import logging
        from bio_data_harmoniser.core.extraction import links, pages
        from bio_data_harmoniser.ingestion import pubmed

        url = context["params"]["url"]
        task_instance: TaskInstance = context["task_instance"]
        node_name = "Deciding input type & extracting URLs"
        sess: logging.LoggingSession
        with logging.logging_session(
            node=logging.LoggedNode(
                id="retrieve_urls",
                name=node_name,
                data=logging.NodeMetadata(
                    name=node_name,
                    type=logging.TaskType.RETRIEVE,
                    arguments=[
                        logging.Argument(
                            name="URL/URI",
                            value=url,
                        )
                    ]
                ),
            ),
            output_dir=folder_path_from_task_instance(task_instance, "logs")
        ) as sess:
            if not pubmed.is_pubmed_url(url):
                sess.log_decision(
                    logging.Decision(
                        type=logging.DecisionType.RETRIEVAL_TYPE_IDENTIFIED,
                        content="File URL/URI"
                    )
                )
                return [url]
            sess.log_decision(
                logging.Decision(
                    type=logging.DecisionType.RETRIEVAL_TYPE_IDENTIFIED,
                    content="PubMed paper"
                )
            )
            logger.info(f"Extracting URLs from {url}")

            pmc_id = pubmed.get_pubmed_id(url)
            article = pubmed.fetch_pmc_article(pmc_id)
            task_instance.xcom_push(
                key="context_pieces",
                value=[article.body],
            )

            data_section = article.data_availability_section
            if data_section is None:
                raise ValueError(f"No data availability section found in {url}")
            urls = links.extract_urls_from_text(pages.Page(url=url, content=data_section.body))
            return urls

    @task_group
    def retrieve_and_extract(url: str) -> list[str]:
        @task
        def retrieve(url: str, **context) -> str:
            import sys
            import shutil
            import urllib.parse
            import urllib.request

            from loguru import logger

            from bio_data_harmoniser.core import logging

            task_instance: TaskInstance = context["task_instance"]

            def show_progress(blocknum, blocksize, totalsize):
                """
                Prints the progress of a file download.
                """
                readsofar = blocknum * blocksize
                if totalsize > 0:
                    percent = readsofar * 1e2 / totalsize
                    s = "\r%5.1f%% %*d / %d" % (
                        percent,
                        len(str(totalsize)),
                        readsofar,
                        totalsize,
                    )
                    sys.stdout.write(s)
                    if readsofar >= totalsize:  # near the end
                        sys.stdout.write("\n")
                    if int(percent) % 10 == 0:
                        logger.info(f"Downloaded {percent}%")
                        task_instance.xcom_push(key="download_progress", value=percent)
                else:  # total size is unknown
                    sys.stdout.write("read %d\n" % (readsofar,))

            node_name = "Retrieving data"
            sess: logging.LoggingSession
            with logging.logging_session(
                node=logging.LoggedNode(
                    id=f"retrieve_and_extract.retrieve_{task_instance.map_index}",
                    name=node_name,
                    data=logging.NodeMetadata(
                        name=node_name,
                        type=logging.TaskType.DOWNLOAD,
                        arguments=[
                            logging.Argument(
                                name="URL/URI",
                                value=url,
                            )
                        ]
                    ),
                    upstream_node_ids=["retrieve_urls"]
                ),
                output_dir=folder_path_from_task_instance(task_instance, "logs")
            ) as sess:
                logger.info(f"Retrieving {url}")
                folder_path = folder_path_from_task_instance(task_instance, "raw_data")
                logger.info(f"Saving to {folder_path}")
                os.makedirs(folder_path, exist_ok=True)
                filepath = os.path.join(folder_path, Path(url).name)
                logger.info(f"Saving to {filepath}")
                parsed_url = urllib.parse.urlparse(url)
                if parsed_url.scheme in ("", "file"):
                    sess.log_decision(
                        logging.Decision(
                            type=logging.DecisionType.FILE_COPIED,
                            content=filepath,
                        )
                    )
                    logger.info(f"Copying {parsed_url.path} to {filepath}")
                    shutil.copy(parsed_url.path, filepath)
                    filename = filepath
                else:
                    sess.log_decision(
                        logging.Decision(
                            type=logging.DecisionType.URL_RETRIEVED,
                            content=url,
                        )
                    )
                    logger.info(f"Downloading {url} to {filepath}")
                    filename, _ = urllib.request.urlretrieve(
                        url, filename=filepath, reporthook=show_progress
                    )
                return filename

        @task.branch
        def extraction_type(filename: str, **context) -> str:
            from bio_data_harmoniser.core import logging

            task_instance: TaskInstance = context["task_instance"]
            node_name = "Extraction"
            sess: logging.LoggingSession
            with logging.logging_session(
                node=logging.LoggedNode(
                    id=f"{task_instance.task_id}_{task_instance.map_index}",
                    name=node_name,
                    data=logging.NodeMetadata(
                        name=node_name,
                        type=logging.TaskType.EXTRACT,
                        arguments=[
                            logging.Argument(
                                name="File path",
                                value=filename,
                            )
                        ]
                    ),
                    upstream_node_ids=[
                        f"retrieve_and_extract.retrieve_{task_instance.map_index}",
                    ]
                ),
                output_dir=folder_path_from_task_instance(task_instance, "logs")
            ) as sess:
                selected_task = "retrieve_and_extract.no_extraction"
                log_content = "No extraction"
                if zipfile.is_zipfile(filename):
                    selected_task = "retrieve_and_extract.unzip"
                    log_content = "Unzip"
                if tarfile.is_tarfile(filename):
                    selected_task = "retrieve_and_extract.untar"
                    log_content = "Untar"
                sess.log_decision(
                    logging.Decision(
                        type=logging.DecisionType.EXTRACTION_TYPE_IDENTIFIED,
                        content=log_content,
                    )
                )
                return selected_task

        @task
        def unzip(filename: str, **context) -> None:
            from loguru import logger

            logger.info(f"Unzipping {filename}")
            task_instance: TaskInstance = context["task_instance"]
            path = os.path.join(
                folder_path_from_task_instance(task_instance, "raw_data"),
                Path(filename).stem,
            )
            with zipfile.ZipFile(filename, "r") as zip_ref:
                zip_ref.extractall(path)
            task_instance.xcom_push(key="unzip_path", value=path)

        @task
        def untar(filename: str, **context) -> None:
            from loguru import logger

            logger.info(f"Untarring {filename}")
            task_instance = context["task_instance"]
            path = os.path.join(
                folder_path_from_task_instance(task_instance, "raw_data"),
                Path(filename).stem,
            )
            with tarfile.open(filename, "r") as tar_ref:
                tar_ref.extractall(path)
            task_instance.xcom_push(key="untar_path", value=path)

        @task
        def no_extraction(filename: str, **context) -> None:
            task_instance: TaskInstance = context["task_instance"]
            task_instance.xcom_push(key="no_extraction_path", value=filename)

        @task(
            trigger_rule=TriggerRule.NONE_FAILED,
        )
        def collect_files(**context) -> list[str]:
            task_instance: TaskInstance = context["task_instance"]
            extracted_paths = []
            for key in ["unzip_path", "untar_path", "no_extraction_path"]:
                path = task_instance.xcom_pull(
                    key=key, map_indexes=task_instance.map_index, default=None
                )
                if path is None:
                    continue
                if Path(path).is_dir():
                    all_files = list(map(str, Path(path).rglob("*")))
                else:
                    all_files = [path]
                extracted_paths.extend(all_files)
            return extracted_paths

        file = retrieve(url)
        extraction = extraction_type(file)

        untar_task = untar(file)
        no_extraction_task = no_extraction(file)
        unzip_task = unzip(file)
        collect_files_task = collect_files()

        # the type checker does not understand that this works (confused by the list of tasks)
        (
            extraction
            >> [  # type: ignore
                untar_task,
                no_extraction_task,
                unzip_task,
            ]
            >> collect_files_task
        )
        return collect_files_task

    @task
    def collect_all_files(files: list[list[str]], **context) -> list[str]:
        from bio_data_harmoniser.core import logging

        task_instance: TaskInstance = context["task_instance"]
        dag_run: DagRun = context["dag_run"]
        task_instances = dag_run.get_task_instances()
        node_name = "Pool extracted files"
        with logging.logging_session(
            node=logging.LoggedNode(
                id="collect_all_files",
                name=node_name,
                data=logging.NodeMetadata(
                    name=node_name,
                    type=logging.TaskType.POOL,
                ),
                upstream_node_ids=[
                    f"{ti.task_id}_{ti.map_index}"
                    for ti in task_instances
                    if ti.task_id == "retrieve_and_extract.extraction_type"
                ]
            ),
            output_dir=folder_path_from_task_instance(task_instance, "logs")
        ):
            return list(itertools.chain.from_iterable(files))

    @task
    def process(filename: str, **context) -> None:
        from loguru import logger

        from bio_data_harmoniser.core import utils
        from bio_data_harmoniser.core import llms, logging, schemas
        from bio_data_harmoniser.core.io import core as core_io
        from bio_data_harmoniser.core.schemas import base as schemas_base

        task_instance: TaskInstance = context["task_instance"]
        directory = Path(
            get_processed_data_path(task_instance)
        )
        user_defined_schemas = get_user_defined_schemas()

        node_name = "Processing data"
        sess: logging.LoggingSession
        with logging.logging_session(
            node=logging.LoggedNode(
                id=f"{task_instance.task_id}_{task_instance.map_index}",
                name=node_name,
                data=logging.NodeMetadata(
                    name=node_name,
                    type=logging.TaskType.PROCESS,
                    arguments=[
                        logging.Argument(
                            name="File being processed",
                            value=Path(filename).name,
                        )
                    ]
                ),
                upstream_node_ids=["collect_all_files"]
            ),
            output_dir=folder_path_from_task_instance(task_instance, "logs"),
        ) as sess:
            fmt = core_io.get_format_from_suffix(filename, on_unsupported="ignore")
            if fmt is None:
                logger.warning(f"Skipping {filename} as it is not a supported format")
                raise AirflowSkipException(f"Skipping {filename} as it is not a supported format")

            sess.log_decision(
                logging.Decision(
                    type=logging.DecisionType.FILE_FORMAT_IDENTIFIED,
                    content=fmt.name,
                )
            )
            try:
                logger.info(f"Processing {filename}")
                llm = llms.get_llm()
                dataframe = core_io.read(filename)
                other = schemas.other.create()
                logger.info(f"Identifying target schema for {filename}")
                schema = schemas.identify_target_schema(
                    dataframe,
                    schemas=user_defined_schemas + [schema.create() for schema in schemas.schemas] + [other],
                    llm=llm,
                )
                sess.log_decision(
                    logging.Decision(
                        type=logging.DecisionType.SCHEMA_IDENTIFIED,
                        content=schema.name,
                    )
                )
                logger.info(f"Identified schema: {schema.name}")
                result = schemas.align_dataframe_to_schema(
                    dataframe,
                    # we always want to add the dataset ID column
                    schema=schema.add_columns({"dataset_id": schemas_base.dataset_id_column()}),
                    llm=llm,
                    context=task_instance.xcom_pull(
                        key="context_pieces", default=[]
                    ),
                )
                mappings = pd.DataFrame(
                    [
                        mapping.dict()
                        for mapping in sess.get_logged_mappings()
                    ]
                )
                if not mappings.empty:
                    logger.info(f"Number of mappings: {len(mappings)}")
                    logger.info(
                        f"Pushing mappings to {task_instance.dag_id} task {task_instance.task_id} "
                        f"(map_index={task_instance.map_index})"
                    )
                    task_instance.xcom_push(
                        key="mappings",
                        value=mappings.assign(normalised_score=(mappings["score"] + 1) / 2)
                        .sort_values("normalised_score")
                        .to_json(orient="records"),
                    )
                dataset_id = Path(filename).stem
                result["dataset_id"] = dataset_id
                # next we need to save the results
                # to a Delta table
                output_dir = directory / utils.to_snake_case(schema.name)
                if schema is other:
                    output_dir = (
                        directory / ("other_" + utils.to_snake_case(Path(filename).stem))
                    )
                logger.info(f"Writing output to {output_dir} (partition={dataset_id})")
                result.to_parquet(str(output_dir), partition_cols=["dataset_id"])
            except Exception as e:
                logger.exception(f"Error processing {filename}: {e}")
                logger.error(f"Error processing {filename}: {e}")
                sess.log_decision(
                    logging.Decision(
                        type=logging.DecisionType.UNABLE_TO_PROCESS,
                        content=str(e),
                    )
                )
                raise AirflowSkipException(f"Error processing {filename}: {e}") from e

    @task(
        trigger_rule=TriggerRule.NONE_FAILED,
    )
    def create_tables_and_aggregate_mappings(**context):
        import io
        import json

        import deltalake
        import pandas as pd
        import pyarrow as pa

        from loguru import logger

        task_instance: TaskInstance = context["task_instance"]
        logger.info(f"Converting files to Delta at {get_processed_data_path(task_instance)}")
        table_paths = get_subdirs_in_processed_data_path(task_instance)
        if not table_paths:
            logger.warning("No files to convert to Delta")
        for table in table_paths:
            if table.is_dir():
                logger.info(f"Converting {table.name} to Delta")
                deltalake.convert_to_deltalake(
                    str(table),
                    partition_by=pa.schema(
                        [
                            ("dataset_id", pa.string()),
                        ]
                    ),
                    partition_strategy="hive",
                )
        mappings = []
        for value in task_instance.xcom_pull(task_ids="process", key="mappings", default=[]):
            mappings.append(pd.read_json(io.StringIO(value)))
        if not mappings:
            logger.info("No mappings found")
            task_instance.xcom_push(key="mappings", value=json.dumps([]))
            return
        mappings = (
            pd.concat(mappings, ignore_index=True)
            .groupby(["id", "mention"])
            .agg(
                {
                    "types": "first",
                    "name": "first",
                    "score": "mean",
                    "normalised_score": "mean",
                }
            )
            .reset_index()
        )
        logger.info(f"Number of mappings: {len(mappings)}")
        (
            mappings[["id", "mention", "types", "name", "score", "normalised_score"]]
            .sort_values("normalised_score")
            .to_json(
                get_mappings_path(task_instance),
                orient="records"
            )
        )

    urls = extract_urls()
    retrieve_and_extract_task = retrieve_and_extract.expand(url=urls)
    collect_all_files_task = collect_all_files(retrieve_and_extract_task)
    process_task = process.expand(filename=collect_all_files_task)
    process_task >> create_tables_and_aggregate_mappings()


class SchemaDefinitionParams(pydantic.BaseModel):
    user_id: str = pydantic.Field(
        "", description="The user ID to use for the data extraction."
    )
    schema_: str = pydantic.Field(
        "", description="The JSON encoded `pandera` schema definition.", alias="schema"
    )

    @classmethod
    def from_schema(cls, schema: "pa.DataFrameSchema", user_id: str = "test_user") -> "SchemaDefinitionParams":
        return cls(
            user_id=user_id,
            schema=schema.to_json(),
        )


with airflow.DAG(
    dag_id=SCHEMA_DEFINITION_DAG_ID,
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
    is_paused_upon_creation=False,
    params=pydantic_to_airflow_params(SchemaDefinitionParams()),
) as schema_definition:

    @task
    def register_new_schema():
        from loguru import logger

        logger.info("Registering new schema")
        return "done"

    register_new_schema()


class SqlQueryParams(pydantic.BaseModel):
    data_extraction_run_id: str = pydantic.Field(
        "", description="The run ID of the data extraction task"
    )
    sql_query: str = pydantic.Field(
        "", description="The SQL query to execute"
    )


def get_sql_query_results_path(task_instance: TaskInstanceInfo | TaskInstance) -> str:
    return folder_path_from_task_instance(task_instance, "sql_query_results.parquet")


SQL_QUERY_DAG_ID = "sql_query"
with airflow.DAG(
    dag_id=SQL_QUERY_DAG_ID,
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
    is_paused_upon_creation=False,
    params=pydantic_to_airflow_params(SqlQueryParams()),
) as sql_query:

    @task
    def run_sql_query(**context):
        import polars as pl
        from loguru import logger

        logger.info("Getting available Delta tables")
        tables = get_delta_table_paths(
            TaskInstanceInfo(
                dag_id=DATA_EXTRACTION_DAG_ID,
                run_id=context["params"]["data_extraction_run_id"]
            )
        )
        if not tables:
            raise ValueError("No Delta tables found")
        logger.info("Initialising SQL context")
        sql_context = pl.sql.context.SQLContext(
            **{
                path.stem: pl.scan_delta(str(path))
                for path in tables
            }
        )
        logger.info("Executing SQL query")
        result = sql_context.execute(context["params"]["sql_query"])

        task_instance: TaskInstance = context["task_instance"]
        results_path = get_sql_query_results_path(task_instance)
        Path(results_path).parent.mkdir(parents=True, exist_ok=True)
        result.collect().write_parquet(results_path)

        return results_path

    run_sql_query_task = run_sql_query()
