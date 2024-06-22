import pandera as pa

from bio_data_harmoniser.core import data_types as dt
from bio_data_harmoniser.core import ontology
from bio_data_harmoniser.core.normalisation import entity as entity_normalisation
from bio_data_harmoniser.core.schemas import base as schemas


def create() -> pa.DataFrameSchema:
    return schemas.schema(
        name="RNA-seq",
        description="RNA-seq data. This data type is used to store sample-level RNA-seq data.",
        columns={
            "dataset_id": schemas.dataset_id_column(),
            "subject_id": pa.Column(str, nullable=True),
            "sample_id": pa.Column(str, nullable=True),
            "value": pa.Column(float, nullable=True),
            "value_type": pa.Column(
                str,
                nullable=False,
                metadata=schemas.ColumnMetadata(
                    column_inferences=[
                        schemas.ColumnInference.extract_from_context(
                            "What is the measure used to quantify the RNA-seq data? E.g. RPKM, FPKM, TPM, etc."
                        ),
                    ]
                ).dict(),
            ),
            "disease_state": pa.Column(
                dt.EntityType.from_entity_types(
                    entity_types=[ontology.EntityType.Disease],
                    normalisation_algorithm=entity_normalisation.NormalisationAlgorithm.RETRIEVAL_AND_CLASSIFICATION,
                ),
                nullable=True,
                metadata=schemas.ColumnMetadata(
                    column_inferences=[
                        schemas.ColumnInference.extract_from_context(
                            "What is the disease state of the RNA-seq sample?"
                        ),
                    ]
                ).dict(),
            ),
            "tissue": pa.Column(
                dt.EntityType.from_entity_types(
                    entity_types=[ontology.EntityType.GrossAnatomicalStructure],
                    normalisation_algorithm=entity_normalisation.NormalisationAlgorithm.RETRIEVAL_AND_CLASSIFICATION,
                ),
                nullable=True,
                metadata=schemas.ColumnMetadata(
                    column_inferences=[
                        schemas.ColumnInference.extract_from_context(
                            "What tissue are the RNA-seq samples from?"
                        ),
                    ]
                ).dict(),
            ),
            "cell_type": pa.Column(
                dt.EntityType.from_entity_types(
                    entity_types=[ontology.EntityType.Cell],
                    normalisation_algorithm=entity_normalisation.NormalisationAlgorithm.RETRIEVAL_AND_CLASSIFICATION,
                ),
                nullable=True,
                metadata=schemas.ColumnMetadata(
                    column_inferences=[
                        schemas.ColumnInference.extract_from_context(
                            "What cell type are the RNA-seq samples from?"
                        )
                    ]
                ).dict(),
            ),
        },
    )
