POETRY = poetry run
DATA_DIR = data
ONTOLOGY_DIR = $(DATA_DIR)/ontology

ifneq (,$(wildcard .env))
	include .env
	export
endif

BASE_PATH := $(shell pwd)
ONTOLOGY_PATH := $(BASE_PATH)/backend/data/ontology
PYTHONPATH := $(BASE_PATH):$(PYTHONPATH)
AIRFLOW_HOME := $(BASE_PATH)/backend
UPLOAD_DIR := $(BASE_PATH)/backend/data/uploads
AIRFLOW_OUTPUT_DIR := $(BASE_PATH)/backend/data/airflow
# see: https://stackoverflow.com/a/76405182
NO_PROXY := *
# see: https://stackoverflow.com/a/71525517
OBJC_DISABLE_INITIALIZE_FORK_SAFETY := YES
# see: https://github.com/UKPLab/sentence-transformers/issues/1318#issuecomment-1084731111
OMP_NUM_THREADS := 1

export BASE_PATH
export ONTOLOGY_PATH
export AIRFLOW_HOME
export UPLOAD_DIR
export AIRFLOW_OUTPUT_DIR
export PYTHONPATH
export NO_PROXY
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY
export OMP_NUM_THREADS


ingest-ontology:
	@echo "Ingesting ontology"
	$(POETRY) bio-data-harmoniser ingestion

airflow_webserver:
	cd backend/ && $(POETRY) airflow webserver --port 8080

airflow_scheduler:
	cd backend/ && $(POETRY) airflow scheduler 2>&1 | tee -a scheduler.log

airflow_worker:
	cd backend/ && $(POETRY) airflow celery worker 2>&1 | tee -a celery.log

airflow: airflow_webserver airflow_scheduler airflow_worker
	cd backend/ && $(POETRY) airflow celery stop

backend_dev:
	cd backend/ && $(POETRY) uvicorn bio_data_harmoniser.api.app:app \
		--host 0.0.0.0 \
		--port 80 \
		--reload

frontend_dev:
	cd frontend/ && npm run dev

local: airflow backend_dev frontend_dev

airflow_reset:
	cd backend/ && \
		$(POETRY) airflow db reset -y && \
		$(POETRY) airflow db init && \
		$(POETRY) airflow users create \
			--username admin \
			--firstname admin \
			--lastname admin \
			--email admin@example.com \
			--role Admin && \
		rm -rf $(AIRFLOW_OUTPUT_DIR)/* && \
		rm -rf $(AIRFLOW_HOME)/logs/*
