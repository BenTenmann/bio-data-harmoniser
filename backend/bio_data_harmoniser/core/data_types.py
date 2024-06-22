import operator
import re
from dataclasses import field, replace
from typing import Any, Optional, Union, Iterable

import pandas as pd
import pandera.dtypes
from langchain_core.language_models import BaseLanguageModel
from pandera import dtypes
from pandera.engines import pandas_engine
from pandera.engines.type_aliases import PandasObject
from pandera.io import pandas_io
from pandera.io.pandas_io import _deserialize_component_stats as _orig_deserialize_component_stats

from bio_data_harmoniser.core import ontology, utils
from bio_data_harmoniser.core.normalisation import entity as entity_normalisation


class _NoOpCheckMixin:
    def check(
        self,
        pandera_dtype: dtypes.DataType,
        data_container: Optional[PandasObject] = None,
    ) -> Union[bool, Iterable[bool]]:
        return True


@pandas_engine.Engine.register_dtype
@pandera.dtypes.immutable(init=True)
class EntityType(_NoOpCheckMixin, pandas_engine.STRING):
    # we inherit from another dataclass which has defaults
    # so since we follow them, we also need to have defaults
    types: list[ontology.EntityType] = field(default_factory=list)
    normaliser: entity_normalisation.EntityNormaliser | None = None

    def __str__(self):
        algo = entity_normalisation.NormalisationAlgorithm.RETRIEVAL.name
        if self.normaliser is not None:
            algo = self.normaliser.algorithm.name
        return (
            "EntityType("
            f"types=[{', '.join(map(operator.attrgetter('name'), self.types))}], "
            f"normalisation_algorithm={algo})"
        )

    def coerce(self, data_container: Any):
        if not (
            isinstance(data_container.dtype, pd.CategoricalDtype)
            or pd.api.types.is_string_dtype(data_container)
        ):
            raise ValueError(f"Cannot coerce {data_container} to {self}")
        assert self.normaliser is not None, f"No normaliser found for {self}"
        out = self.normaliser.normalise(data_container)
        return out

    @classmethod
    def from_entity_types(
        cls,
        entity_types: list[ontology.EntityType],
        normaliser: entity_normalisation.EntityNormaliser | None = None,
        normalisation_algorithm: (
            entity_normalisation.NormalisationAlgorithm
        ) = entity_normalisation.NormalisationAlgorithm.RETRIEVAL,
        llm: BaseLanguageModel | None = None,
    ) -> "EntityType":
        normaliser = (
            normaliser
            or (
                entity_normalisation.EntityNormaliser.from_entity_types(
                    entity_types=entity_types,
                    algorithm=normalisation_algorithm,
                    llm=llm,
                ) if not utils.load_state_is_disabled() else None
            )
        )
        if normaliser is not None and llm is not None:
            normaliser = replace(normaliser, llm=llm)
        if normaliser is not None and normalisation_algorithm is not None:
            normaliser = replace(normaliser, algorithm=normalisation_algorithm)
        return cls(types=entity_types, normaliser=normaliser)

    @classmethod
    def from_parameterized_dtype(cls, equivalent: str) -> "EntityType":
        match = re.search(
            r"EntityType\(types=\[(.+)]",
            equivalent
        )
        if not match:
            raise ValueError(f"Could not parse equivalent {equivalent}")
        types = [
            ontology.EntityType[name.strip()]
            for name in match.group(1).split(", ")
        ]
        algo = entity_normalisation.NormalisationAlgorithm.RETRIEVAL
        match = re.search(
            r"normalisation_algorithm=([A-Z_]+)",
            equivalent
        )
        if match:
            algo = entity_normalisation.NormalisationAlgorithm[match.group(1)]
        return cls.from_entity_types(
            entity_types=types,
            normalisation_algorithm=algo,
        )


@pandas_engine.Engine.register_dtype(
    equivalents=["AminoAcidSequence"]
)
@pandera.dtypes.immutable
class AminoAcidSequenceType(_NoOpCheckMixin, pandas_engine.STRING):
    regex: str = r"^[EIFNSWGAHYTQLVCDPRKMOXU]+$"

    def __str__(self):
        return "AminoAcidSequence"

    def coerce(self, data_container: Any):
        if not pd.api.types.is_string_dtype(data_container):
            raise ValueError(f"Cannot coerce {data_container} to {self}")
        data_container = data_container.str.strip()
        return data_container.where(data_container.str.match(self.regex), pd.NA)


@pandas_engine.Engine.register_dtype(
    equivalents=["NucleotideSequence"]
)
@pandera.dtypes.immutable
class NucleotideSequenceType(_NoOpCheckMixin, pandas_engine.STRING):
    regex: str = r"^[ACGT]+$"

    def __str__(self):
        return "NucleotideSequence"

    def coerce(self, data_container: Any):
        if not pd.api.types.is_string_dtype(data_container):
            raise ValueError(f"Cannot coerce {data_container} to {self}")
        data_container = data_container.str.strip()
        return data_container.where(data_container.str.match(self.regex), pd.NA)


@pandas_engine.Engine.register_dtype(
    equivalents=["SMILES"]
)
@pandera.dtypes.immutable
class SMILESType(_NoOpCheckMixin, pandas_engine.STRING):

    def __str__(self):
        return "SMILES"

    def coerce(self, data_container: Any):
        try:
            from rdkit import Chem
        except (ModuleNotFoundError, ImportError) as e:
            raise RuntimeError(
                "Cannot coerce SMILES to string. Please install rdkit with `pip install rdkit`"
            ) from e
        if not pd.api.types.is_string_dtype(data_container):
            raise ValueError(f"Cannot coerce {data_container} to {self}")
        data_container = data_container.str.strip()
        return pd.Series(
            [
                Chem.CanonSmiles(elem) if Chem.MolFromSmiles(elem) else pd.NA
                for elem in data_container
            ]
        )


def _deserialize_component_stats(serialized_component_stats) -> None:
    dtype = serialized_component_stats.get("dtype")
    if (dtype or "").startswith("EntityType"):
        serialized_component_stats["dtype"] = EntityType.from_parameterized_dtype(
            dtype
        )
    return _orig_deserialize_component_stats(serialized_component_stats)


pandas_io._deserialize_component_stats = _deserialize_component_stats
