import functools
import os
import tarfile
import urllib.request
from typing import Annotated, Final, Optional

import deltalake
import pandas as pd
import typer
from loguru import logger
from sklearn import preprocessing

from bio_data_harmoniser.core import llms, ontology

DEFAULT_ONTOLOGY_URL: Final[str] = (
    "https://data.monarchinitiative.org/monarch-kg-dev/latest/monarch-kg.tar.gz"
)
DEFAULT_NODES_FILENAME: Final[str] = "monarch-kg_nodes.tsv"


def main(
    ontology_path: Annotated[
        Optional[str], typer.Option(help="Path to the ontology to download")
    ] = None,
    ontology_url: str = typer.Option(
        DEFAULT_ONTOLOGY_URL, help="URL of the ontology to download"
    ),
    nodes_filename: str = typer.Option(
        DEFAULT_NODES_FILENAME, help="Filename of the ontology nodes file"
    ),
):
    ontology_path = ontology_path or ontology.OntologySettings().path
    logger.info(f"Downloading ontology from {ontology_url}")
    filename, _ = urllib.request.urlretrieve(ontology_url)
    logger.info(f"Downloaded ontology to {filename}")
    with tarfile.open(filename) as tar:
        logger.info("Extracting ontology")
        tar.extract(nodes_filename, path=".")

    def _split_array(s: str) -> list[str]:
        if s == "":
            return []
        return s.split("|")

    logger.info("Loading ontology")
    df = (
        pd.read_csv(
            nodes_filename,
            sep="\t",
            low_memory=False,
            converters={
                "synonym": _split_array,
                "xref": _split_array,
            },
        )
        .rename(
            columns={
                "id": ontology.OntologyColumns.id,
                "name": ontology.OntologyColumns.name,
                "definition": ontology.OntologyColumns.description,
                "category": ontology.OntologyColumns.type,
                "synonym": ontology.OntologyColumns.synonyms,
                "xref": ontology.OntologyColumns.xrefs,
                "iri": ontology.OntologyColumns.iri,
            }
        )
        .assign(
            **{
                "full_name": lambda df: df["full_name"].replace("-", pd.NA),
                ontology.OntologyColumns.type: lambda df: df[
                    ontology.OntologyColumns.type
                ].str.replace("biolink:", ""),
            }
        )
    )
    logger.info(f"Loaded ontology with shape {df.shape}")
    df[ontology.OntologyColumns.name] = df["full_name"].where(
        df["full_name"].notnull(), df[ontology.OntologyColumns.name]
    )
    df = df.loc[
        df[ontology.OntologyColumns.type].isin(
            [entity_type.value for entity_type in ontology.EntityType]
        )
        & df[ontology.OntologyColumns.name].notnull()
    ]
    logger.info(f"Filtered ontology with shape {df.shape}")
    logger.info("Embedding ontology")
    df[ontology.OntologyColumns.embedding] = preprocessing.normalize(
        llms.get_encoder().encode(
            df[ontology.OntologyColumns.name].tolist(),
            convert_to_numpy=True,
            show_progress_bar=True,
            batch_size=256,
        )
    ).tolist()
    df = df[list(ontology.OntologyColumns.to_schema().columns)]
    df = ontology.OntologyColumns.validate(df)
    if not os.path.exists(ontology_path):
        logger.info(f"creating {ontology_path}")
        deltalake.write_deltalake(
            ontology_path,
            df,
            partition_by=[ontology.OntologyColumns.type],
            schema=ontology.OntologyColumns.to_pyarrow_schema(),
        )
        return
    logger.info(f"merging ontology with {ontology_path}")
    dt = deltalake.DeltaTable(ontology_path)
    (
        dt.merge(
            df,
            predicate=" AND ".join(
                [
                    f"source.{col} = target.{col}"
                    for col in [
                        ontology.OntologyColumns.id,
                        ontology.OntologyColumns.type,
                    ]
                ]
            ),
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
