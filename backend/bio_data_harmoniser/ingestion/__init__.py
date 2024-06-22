import typer

from bio_data_harmoniser.ingestion import ontology, pubmed

app = typer.Typer()
app.command(name="ontology", help="Ingest the ontology")(ontology.main)
app.command(name="pubmed", help="Ingest PubMed articles")(pubmed.main)

__all__ = ["app"]
