import typer

from bio_data_harmoniser import ingestion

app = typer.Typer()
app.add_typer(ingestion.app, name="ingest")


def main():
    app(standalone_mode=False)
