# bio-data-harmoniser backend

Python backend for the bio-data-harmoniser project.

## Setup

### Install dependencies

```bash
poetry install
```

### Setup Airflow

```bash
./scripts/start-postgres.sh
./scripts/start-redis.sh
USER=postgres ./scripts/setup-airflow.sh
```

### Ingest ontology

```bash
poetry run bio-data-harmoniser ingest ontology
```
