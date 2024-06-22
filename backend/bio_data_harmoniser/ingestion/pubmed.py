import re
import xml.etree.ElementTree as ET
from typing import Final

import deltalake
import pandas as pd
import pydantic
import requests
import typer
from loguru import logger

NCBI_URL: Final[str] = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"


def is_pubmed_url(url: str) -> bool:
    return url.startswith("https://www.ncbi.nlm.nih.gov/pmc/articles/")


def get_pubmed_id(url: str) -> str:
    pmc_id = re.search(r".*/(PMC\d+).*", url)
    if pmc_id is None:
        raise ValueError(f"Could not extract PMC ID from {url}")
    pmc_id = pmc_id.group(1)
    return pmc_id


def fetch_pmc_full_text(pmc_id: str) -> str:
    params = {
        "db": "pmc",
        "id": pmc_id,
        "retmode": "xml",  # XML format often includes the full text
    }

    response = requests.get(NCBI_URL, params=params)
    response.raise_for_status()
    return response.text


class PubmedSection(pydantic.BaseModel):
    title: str
    body: str


class PubmedArticle(pydantic.BaseModel):
    title: str
    abstract: str
    body: str
    sections: list[PubmedSection]

    @property
    def data_availability_section(self) -> PubmedSection | None:
        for section in self.sections:
            if re.match(r"data\savailability", section.title, re.IGNORECASE):
                return section


def parse_pmc_xml_detailed(xml_text: str) -> PubmedArticle:
    root = ET.fromstring(xml_text)

    article_details = {"title": "", "abstract": "", "body": "", "sections": []}

    # Extract the article title
    article_title = root.find(".//article-title")
    if article_title is not None:
        article_details["title"] = "".join(article_title.itertext())

    # Extract the abstract text
    abstract = root.find(".//abstract")
    if abstract is not None:
        article_details["abstract"] = "".join(abstract.itertext())

    # Extract the body text (overall)
    body = root.find(".//body")
    if body is not None:
        article_details["body"] = "".join(body.itertext())

        # Extract each section within the body
        sections = body.findall(".//sec")
        for sec in sections:
            section_dict = {"title": "", "body": ""}

            # Extract section title
            title = sec.find(".//title")
            if title is not None:
                section_dict["title"] = "".join(title.itertext())

            # Extract section body (text following the title, within this section)
            # Here we concatenate all text elements in the section excluding the title
            section_text = "".join(
                [
                    text
                    for text in sec.itertext()
                    if text.strip() != section_dict["title"]
                ]
            )
            section_dict["body"] = section_text.strip()

            article_details["sections"].append(section_dict)

    return PubmedArticle(**article_details)


def fetch_pmc_article(pmc_id: str) -> PubmedArticle:
    xml_text = fetch_pmc_full_text(pmc_id)
    return parse_pmc_xml_detailed(xml_text)


def main(
    pmc_ids: list[str] = typer.Argument(..., help="List of PMC IDs to ingest"),
    output_path: str = typer.Option(..., help="Path to the output Delta table"),
):
    logger.info("Fetching PMC articles")
    articles = [fetch_pmc_article(pmc_id) for pmc_id in pmc_ids]
    logger.info("Writing PMC articles to Delta table")
    deltalake.write_deltalake(
        output_path,
        pd.DataFrame(
            [
                {**article.dict(), "pmcid": pmc_id}
                for pmc_id, article in zip(pmc_ids, articles)
            ],
        ),
        partition_by=["pmcid"],
    )
