import enum
from dataclasses import dataclass
from typing import Final, Type

import deltalake
import pandas as pd
import pandera as pa
import pandera.typing as pat
import pyarrow
import pyarrow.compute as pc

from bio_data_harmoniser.core import settings


class EntityType(str, enum.Enum):
    Gene = "Gene"
    PhenotypicFeature = "PhenotypicFeature"
    BiologicalProcessOrActivity = "BiologicalProcessOrActivity"
    Disease = "Disease"
    Pathway = "Pathway"
    Cell = "Cell"
    GrossAnatomicalStructure = "GrossAnatomicalStructure"
    AnatomicalEntity = "AnatomicalEntity"
    CellularComponent = "CellularComponent"
    MolecularEntity = "MolecularEntity"
    NamedThing = "NamedThing"
    MacromolecularComplex = "MacromolecularComplex"
    Protein = "Protein"
    CellularOrganism = "CellularOrganism"
    Vertebrate = "Vertebrate"
    Virus = "Virus"
    BehavioralFeature = "BehavioralFeature"
    ChemicalEntity = "ChemicalEntity"
    LifeStage = "LifeStage"
    PathologicalProcess = "PathologicalProcess"
    Drug = "Drug"
    SmallMolecule = "SmallMolecule"
    InformationContentEntity = "InformationContentEntity"
    NucleicAcidEntity = "NucleicAcidEntity"
    EvidenceType = "EvidenceType"
    RNAProduct = "RNAProduct"
    Transcript = "Transcript"
    ProcessedMaterial = "ProcessedMaterial"
    EnvironmentalFeature = "EnvironmentalFeature"
    Plant = "Plant"
    OrganismTaxon = "OrganismTaxon"
    Polypeptide = "Polypeptide"


ENTITY_TYPE_DESCRIPTIONS: Final[dict[EntityType, str]] = {
    EntityType.Gene: "A region (or regions) that includes all of the sequence elements necessary to encode a functional transcript. A gene locus may include regulatory regions, transcribed regions and/or other functional sequence regions.",
    EntityType.PhenotypicFeature: "A combination of entity and quality that makes up a phenotyping statement. An observable characteristic of an individual resulting from the interaction of its genotype with its molecular and physical environment.",
    EntityType.BiologicalProcessOrActivity: "Either an individual molecular activity, or a collection of causally connected molecular activities in a biological system.",
    EntityType.Disease: "A disorder of structure or function, especially one that produces specific signs, phenotypes or symptoms or that affects a specific location and is not simply a direct result of physical injury. A disposition to undergo pathological processes that exists in an organism because of one or more disorders in that organism.",
    EntityType.Pathway: "A pathway is a series of chemical reactions that occur in a living organism.",
    EntityType.Cell: "A cell is the basic structural and functional unit of an organism.",
    EntityType.GrossAnatomicalStructure: "A gross anatomical structure is a part of the body (i.e. a tissue, organ, etc.).",
    EntityType.AnatomicalEntity: "An anatomical entity is a part of the body.",
    EntityType.CellularComponent: "A location in or around a cell.",
    EntityType.MolecularEntity: "A molecular entity is a chemical entity composed of individual or covalently bonded atoms.",
    EntityType.NamedThing: "A databased entity or concept/class.",
    EntityType.MacromolecularComplex: "A stable assembly of two or more macromolecules, i.e. proteins, nucleic acids, carbohydrates or lipids, in which at least one component is a protein and the constituent parts function together.",
    EntityType.Protein: "A gene product that is composed of a chain of amino acid sequences and is produced by ribosome-mediated translation of mRNA.",
    EntityType.CellularOrganism: "A cellular organism is an organism that is made up of cells.",
    EntityType.Vertebrate: "A sub-phylum of animals consisting of those having a bony or cartilaginous vertebral column.",
    EntityType.Virus: "A virus is a microorganism that replicates itself as a microRNA and infects the host cell.",
    EntityType.BehavioralFeature: "A phenotypic feature which is behavioral in nature.",
    EntityType.ChemicalEntity: "A chemical entity is a physical entity that pertains to chemistry or biochemistry.",
    EntityType.LifeStage: "A stage of development or growth of an organism, including post-natal adult stages.",
    EntityType.PathologicalProcess: "	A biologic function or a process having an abnormal or deleterious effect at the subcellular, cellular, multicellular, or organismal level.",
    EntityType.Drug: "A substance intended for use in the diagnosis, cure, mitigation, treatment, or prevention of disease.",
    EntityType.SmallMolecule: "A small molecule entity is a molecular entity characterized by availability in small-molecule databases of SMILES, InChI, IUPAC, or other unambiguous representation of its precise chemical structure; for convenience of representation, any valid chemical representation is included, even if it is not strictly molecular (e.g., sodium ion).",
    EntityType.InformationContentEntity: "A piece of information that typically describes some topic of discourse or is used as support.",
    EntityType.NucleicAcidEntity: "A nucleic acid entity is a molecular entity characterized by availability in gene databases of nucleotide-based sequence representations of its precise sequence; for convenience of representation, partial sequences of various kinds are included.",
    EntityType.EvidenceType: "Class of evidence that supports an association.",
    EntityType.RNAProduct: "An RNA product is a product of an RNA molecule.",
    EntityType.Transcript: "An RNA synthesized on a DNA or RNA template by an RNA polymerase.",
    EntityType.ProcessedMaterial: "A chemical entity (often a mixture) processed for consumption for nutritional, medical or technical use. Is a material entity that is created or changed during material processing.",
    EntityType.EnvironmentalFeature: "An environmental feature is a feature of the environment that is stored in a database.",
    EntityType.Plant: "A plant is a living organism that is part of the plant kingdom.",
    EntityType.OrganismTaxon: "A classification of a set of organisms. Example instances: NCBITaxon:9606 (Homo sapiens), NCBITaxon:2 (Bacteria). Can also be used to represent strains or subspecies.",
    EntityType.Polypeptide: "A polypeptide is a molecular entity characterized by availability in protein databases of amino-acid-based sequence representations of its precise primary structure; for convenience of representation, partial sequences of various kinds are included, even if they do not represent a physical molecule.",
}


class OntologyColumns(pa.DataFrameModel):
    id: pat.Series[str]
    name: pat.Series[str]
    description: pat.Series[str] = pa.Field(nullable=True)
    type: pat.Series[str]
    synonyms: pat.Series[list[str]] = pa.Field(nullable=True)
    xrefs: pat.Series[list[str]] = pa.Field(nullable=True)
    iri: pat.Series[str] = pa.Field(nullable=True)
    embedding: pat.Series[list[float]]

    class Config:
        coerce = True

    @classmethod
    def to_pyarrow_schema(cls) -> pyarrow.Schema:
        # TODO: the community is working on implementing this
        # see: https://github.com/unionai-oss/pandera/pull/1047
        # check back when it's merged
        return pyarrow.schema(
            [
                pyarrow.field("id", pyarrow.string(), nullable=False),
                pyarrow.field("name", pyarrow.string(), nullable=False),
                pyarrow.field("description", pyarrow.string(), nullable=True),
                pyarrow.field("type", pyarrow.string(), nullable=False),
                pyarrow.field(
                    "synonyms", pyarrow.list_(pyarrow.string()), nullable=True
                ),
                pyarrow.field("xrefs", pyarrow.list_(pyarrow.string()), nullable=True),
                pyarrow.field("iri", pyarrow.string(), nullable=True),
                pyarrow.field(
                    "embedding", pyarrow.list_(pyarrow.float32()), nullable=True
                ),
            ]
        )


@dataclass
class OntologyStore:
    table: deltalake.DeltaTable
    columns: Type[OntologyColumns] = OntologyColumns

    def load(
        self,
        entity_types: list[EntityType] | None = None,
        columns: list[str] | None = None,
        filter_expression: pc.Expression | None = None,
    ) -> pd.DataFrame:
        partitions = []
        if entity_types is not None:
            partitions.append(
                (
                    self.columns.type,
                    "in",
                    [entity_type.value for entity_type in entity_types],
                )
            )
        if filter_expression is not None:
            table = self.table.to_pyarrow_dataset(partitions=partitions)
            return table.to_table(filter=filter_expression, columns=columns).to_pandas()
        return self.table.to_pandas(partitions=partitions, columns=columns)

    def load_xref_mapping(
        self,
        entity_types: list[EntityType] | None = None,
        additional_columns: list[str] | None = None,
    ) -> pd.DataFrame:
        additional_columns = additional_columns or []
        data = self.load(
            entity_types=entity_types, columns=[self.columns.id, self.columns.xrefs, *additional_columns]
        )
        return (
            data.explode(self.columns.xrefs)
            .dropna(subset=[self.columns.xrefs])
        )

    @classmethod
    def from_path(cls, path: str) -> "OntologyStore":
        return cls(deltalake.DeltaTable(path))

    @classmethod
    def default(cls) -> "OntologyStore":
        return cls.from_path(path=settings.ontology.path)
