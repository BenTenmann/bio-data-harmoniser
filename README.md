# bio-data-harmoniser

Automatically ingest and harmonise biological data from different sources.

## About

Biomedical data is messy and disharmonised. This tool aims to help researchers ingest and harmonise biological data from
different sources, to make it easier to get insights from the data.

## Usage

In general, we recommend using Docker to run the tool. To do so, you first need to ingest the MONDO KG ontology:

```bash
make ingest_ontology_in_docker
```

Then, you can run the tool:

```bash
make run_in_docker
```

This will start the tool in a Docker container. You can access the tool at `http://0.0.0.0:3000`.

> Note: since the ingestion of the ontology also computes the embeddings for the entities, it can take a while. If you
have a GPU available, we recommend using it by setting the `LLM_EMBEDDINGS_DEVICE` environment variable to `cuda` when
running `make ingest_ontology_in_docker`.

