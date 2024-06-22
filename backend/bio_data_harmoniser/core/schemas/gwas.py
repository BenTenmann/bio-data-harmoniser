from pathlib import Path

import numpy as np
import pandera as pa

from bio_data_harmoniser.core import data_types as dt
from bio_data_harmoniser.core import ontology
from bio_data_harmoniser.core.normalisation import entity as entity_normalisation
from bio_data_harmoniser.core.schemas import base as schemas


def create() -> pa.DataFrameSchema:
    return schemas.schema(
        name="GWAS",
        description="Genome-wide association study data. This data type is used to store summary statistics from GWAS "
        "studies. The data is typically stored in a tabular format with columns for the chromosome, "
        "position, variant ID, effect allele, non-effect allele, effect size, standard error, p-value, and "
        "effect allele frequency.",
        columns={
            "dataset_id": schemas.dataset_id_column(),
            "trait_id": pa.Column(
                dt.EntityType.from_entity_types(
                    entity_types=[ontology.EntityType.Disease],
                    normalisation_algorithm=entity_normalisation.NormalisationAlgorithm.RETRIEVAL_AND_CLASSIFICATION,
                ),
                metadata=schemas.ColumnMetadata(
                    column_inferences=[
                        schemas.ColumnInference.extract_from_context(
                            "What is the name of the disease that this GWAS study is investigating?"
                        )
                    ]
                ).dict(),
            ),
            "num_samples": pa.Column(
                int,
                metadata=schemas.ColumnMetadata(
                    aliases=[
                        "N",
                        "NMISS",
                        "Nsample",
                        "OBS_CT",
                        "SS",
                        "TotalSampleSize",
                        "n",
                        "num_samples",
                        "sample_size",
                    ],
                    column_inferences=[
                        schemas.ColumnInference.when_has_columns(
                            ["num_cases", "num_controls"],
                            lambda session: session.dataframe["num_cases"]
                            + session.dataframe["num_controls"],
                        ),
                    ],
                ).dict(),
            ),
            "num_cases": pa.Column(
                int,
                metadata=schemas.ColumnMetadata(
                    aliases=["N_CASE", "ncase", "TotalCases"],
                    column_inferences=[
                        schemas.ColumnInference.when_has_columns(
                            ["num_samples", "num_controls"],
                            lambda session: session.dataframe["num_samples"]
                            - session.dataframe["num_controls"],
                        ),
                        schemas.ColumnInference.extract_from_context(
                            "What is the number of cases in this GWAS study?",
                            rag_post_processing="int",
                        ),
                    ],
                ).dict(),
            ),
            "num_controls": pa.Column(
                int,
                metadata=schemas.ColumnMetadata(
                    aliases=["N_CONTROL", "ncontrol", "TotalControls"],
                    column_inferences=[
                        schemas.ColumnInference.when_has_columns(
                            ["num_samples", "num_cases"],
                            lambda session: session.dataframe["num_samples"]
                            - session.dataframe["num_cases"],
                        ),
                        schemas.ColumnInference.extract_from_context(
                            "What is the number of controls in this GWAS study?",
                            rag_post_processing="int",
                        ),
                    ],
                ).dict(),
            ),
            "genome_build": pa.Column(
                # TODO: Add categorical type for aligning data the same way entity types are aligned
                str,
                nullable=True,
                metadata=schemas.ColumnMetadata(
                    aliases=["GenomeBuild"],
                    column_inferences=[
                        schemas.ColumnInference.extract_from_context(
                            "What is the genome build of the GWAS study? Choose one of: GRCh37, GRCh38",
                        )
                    ],
                ).dict(),
            ),
            "variant_id": pa.Column(
                str,
                metadata=schemas.ColumnMetadata(
                    column_inferences=[
                        schemas.ColumnInference.when_has_columns(
                            [
                                "chromosome",
                                "position",
                                "effect_allele",
                                "non_effect_allele",
                            ],
                            lambda session: session.dataframe["chromosome"]
                            .astype(str)
                            .fillna("")
                            + ":"
                            + session.dataframe["position"].astype(str).fillna("")
                            + ":"
                            + session.dataframe["effect_allele"].astype(str).fillna("")
                            + ":"
                            + session.dataframe["non_effect_allele"]
                            .astype(str)
                            .fillna(""),
                        )
                    ]
                ).dict(),
            ),
            "chromosome": pa.Column(
                str,
                metadata=schemas.ColumnMetadata(
                    aliases=[
                        "#CHROM",
                        "0",
                        "CHR",
                        "CHROM",
                        "CHROMOSOME",
                        "Chr",
                        "Chrom",
                        "Chromosome",
                        "chr",
                        "chr_name",
                        "chrom",
                        "chromosome",
                        "hm_chr",
                    ],
                ).dict(),
            ),
            "position": pa.Column(
                int,
                metadata=schemas.ColumnMetadata(
                    aliases=[
                        "3",
                        "BP",
                        "GENPOS",
                        "POS",
                        "Pos",
                        "Position",
                        "base_pair_location",
                        "bp",
                        "bpos",
                        "chr_position",
                        "hm_pos",
                        "pos",
                    ]
                ).dict(),
            ),
            "non_effect_allele": pa.Column(
                str,
                metadata=schemas.ColumnMetadata(
                    aliases=[
                        "4",
                        "A0",
                        "A2",
                        "ALLELE0",
                        "ALLELE2",
                        "ALLELE_0",
                        "ALLELE_2",
                        "Allele0",
                        "Allele1",
                        "Allele_0",
                        "Allele_2",
                        "NEA",
                        "NON_EFFECT_ALLELE",
                        "Ref",
                        "a0",
                        "a2",
                        "allele0",
                        "allele2",
                        "allele_0",
                        "allele_2",
                        "hm_inferOtherAllele",
                        "hm_other_allele",
                        "nea",
                        "non_effect_allele",
                        "other_allele",
                        "ref",
                        "reference",
                        "reference_allele",
                        "REF",
                    ]
                ).dict(),
            ),
            "effect_allele": pa.Column(
                str,
                metadata=schemas.ColumnMetadata(
                    aliases=[
                        "5",
                        "A1",
                        "ALLELE1",
                        "ALLELE_1",
                        "Allele2",
                        "Allele_1",
                        "Alt",
                        "EA",
                        "a1",
                        "allele1",
                        "allele_1",
                        "alt",
                        "alternative",
                        "alternative_allele",
                        "ea",
                        "effect_allele",
                        "hm_effect_allele",
                        "ALT",
                    ]
                ).dict(),
            ),
            "effect_size": pa.Column(
                float,
                metadata=schemas.ColumnMetadata(
                    aliases=[
                        "B",
                        "BETA",
                        "Beta",
                        "ES",
                        "Effect",
                        "b",
                        "beta",
                        "effect_weight",
                        "hm_beta",
                        "EFFECT",
                    ],
                    column_inferences=[
                        schemas.ColumnInference.when_has_columns(
                            ["odds_ratio"],
                            lambda session: np.log(session.dataframe["odds_ratio"]),
                        ),
                    ],
                ).dict(),
            ),
            "odds_ratio": pa.Column(
                float,
                metadata=schemas.ColumnMetadata(
                    aliases=[
                        "OR",
                        "OddsRatio",
                    ],
                    column_inferences=[
                        schemas.ColumnInference.when_has_columns(
                            ["effect_size"],
                            lambda session: np.exp(session.dataframe["effect_size"]),
                        ),
                    ],
                ).dict(),
            ),
            "standard_error": pa.Column(
                float,
                metadata=schemas.ColumnMetadata(
                    aliases=[
                        "LOG(OR)_SE",
                        "SE",
                        "StdErr",
                        "betase",
                        "se",
                        "sebeta",
                        "standard_error",
                    ]
                ).dict(),
            ),
            "p_value": pa.Column(
                float,
                metadata=schemas.ColumnMetadata(
                    aliases=[
                        "P",
                        "P-value",
                        "P-value_association",
                        "P.value",
                        "PVAL",
                        "P_BOLT_LMM",
                        "Pval",
                        "p",
                        "p-value",
                        "p.value",
                        "p_value",
                        "pval",
                    ],
                    column_inferences=[
                        schemas.ColumnInference.when_has_columns(
                            ["negative_log10_p_value"],
                            lambda session: 10
                            ** -session.dataframe["negative_log10_p_value"],
                        ),
                    ],
                ).dict(),
            ),
            "negative_log10_p_value": pa.Column(
                float,
                metadata=schemas.ColumnMetadata(
                    aliases=[
                        "LOG10P",
                        "LOG10_P",
                        "LP",
                        "MLOG10P",
                        "neg_log_10_p_value",
                    ],
                    column_inferences=[
                        schemas.ColumnInference.when_has_columns(
                            ["p_value"],
                            lambda session: -np.log10(session.dataframe["p_value"]),
                        )
                    ],
                ).dict(),
            ),
            "effect_allele_frequency": pa.Column(
                float,
                nullable=True,
                metadata=schemas.ColumnMetadata(
                    aliases=[
                        "A1FREQ",
                        "A1_FREQ",
                        "AF",
                        "AF1",
                        "AF_Allele2",
                        "EAF",
                        "FREQ",
                        "FRQ",
                        "Freq",
                        "Freq1",
                        "Frequency",
                        "Frq",
                        "af",
                        "allelefrequency_effect",
                        "eaf",
                        "effect_allele_frequency",
                        "freq",
                        "frq",
                        "hm_effect_allele_frequency",
                    ]
                ).dict(),
            ),
        }
    )
